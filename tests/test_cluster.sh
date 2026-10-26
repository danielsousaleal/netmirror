#!/usr/bin/env bash
# Three-node cluster integration:
#   1. async 2-of-3 quorum write
#   2. continue writing with one node down
#   3. restart the missing node and verify anti-entropy repairs chunks+manifests
#   4. reject a write when only one node remains
#   5. graceful shutdown with ASan/LeakSanitizer diagnostics checked
set -u

NODE_BIN="${NM_NODE_BIN:-}"
CLIENT_BIN="${NM_CLUSTER_CLIENT_BIN:-}"
if [[ -z "$NODE_BIN" || -z "$CLIENT_BIN" ]]; then
    echo "NM_NODE_BIN and NM_CLUSTER_CLIENT_BIN must be set" >&2
    exit 2
fi
if [[ ! -x "$NODE_BIN" || ! -x "$CLIENT_BIN" ]]; then
    echo "node or cluster client binary is not executable" >&2
    exit 2
fi

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/netmirror-cluster-test.XXXXXX")" || exit 1
PID1=""
PID2=""
PID3=""
LOG1="$TMP_ROOT/node1.log"
LOG2="$TMP_ROOT/node2.log"
LOG3="$TMP_ROOT/node3.log"
LOG3_RESTARTED="$TMP_ROOT/node3-restarted.log"
PORT1=""
PORT2=""
PORT3=""

export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1:abort_on_error=1:strict_string_checks=1}"
export UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1:print_stacktrace=1}"

cleanup() {
    local status=$?
    local pid

    for pid in "$PID1" "$PID2" "$PID3"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    if (( status != 0 )); then
        for log in "$LOG1" "$LOG2" "$LOG3" "$LOG3_RESTARTED"; do
            if [[ -f "$log" ]]; then
                echo "--- $log ---" >&2
                tail -160 "$log" >&2
            fi
        done
    fi
    rm -rf -- "$TMP_ROOT"
    exit "$status"
}
trap cleanup EXIT INT TERM

die() {
    echo "FAIL: $*" >&2
    exit 1
}

port_in_use() {
    local port=$1
    (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null
}

choose_ports() {
    local attempt base port
    for attempt in $(seq 1 200); do
        base=$(( 23000 + (RANDOM % 16000) ))
        # Keep the three ports inside the valid TCP range.
        if (( base + 2 > 65535 )); then
            continue
        fi
        local free=1
        for port in "$base" "$((base + 1))" "$((base + 2))"; do
            if port_in_use "$port"; then
                free=0
                break
            fi
        done
        if (( free == 1 )); then
            PORT1=$base
            PORT2=$((base + 1))
            PORT3=$((base + 2))
            return 0
        fi
    done
    return 1
}

peer_string_for() {
    local self=$1
    local peers=""
    if (( self != 1 )); then peers="127.0.0.1:$PORT1"; fi
    if (( self != 2 )); then
        if [[ -n "$peers" ]]; then peers+=","; fi
        peers+="127.0.0.1:$PORT2"
    fi
    if (( self != 3 )); then
        if [[ -n "$peers" ]]; then peers+=","; fi
        peers+="127.0.0.1:$PORT3"
    fi
    echo "$peers"
}

start_node() {
    local index=$1
    local log_file=$2
    local port peers

    case "$index" in
        1) port=$PORT1 ;;
        2) port=$PORT2 ;;
        3) port=$PORT3 ;;
        *) die "invalid node index $index" ;;
    esac
    peers="$(peer_string_for "$index")"
    NM_NODE_ID="cluster-node-$index" \
    NM_BIND_ADDR="127.0.0.1" \
    NM_PORT="$port" \
    NM_DATA_DIR="$TMP_ROOT/node$index-data" \
    NM_PEERS="$peers" \
    NM_LOG_LEVEL="debug" \
    NM_MAX_CONNECTIONS="32" \
    NM_IDLE_TIMEOUT_MS="60000" \
    NM_CONNECT_TIMEOUT_MS="800" \
    NM_RPC_TIMEOUT_MS="4000" \
    NM_HEARTBEAT_INTERVAL_MS="300" \
    NM_HEARTBEAT_TIMEOUT_MS="1500" \
    NM_ANTI_ENTROPY_INTERVAL_MS="1000" \
        "$NODE_BIN" >"$log_file" 2>&1 &
    case "$index" in
        1) PID1=$! ;;
        2) PID2=$! ;;
        3) PID3=$! ;;
    esac
}

wait_tcp() {
    local port=$1
    local attempts=${2:-200}
    for _ in $(seq 1 "$attempts"); do
        if port_in_use "$port"; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

wait_peer_up_count() {
    local log_file=$1
    local wanted=$2
    local attempts=${3:-200}
    local count
    for _ in $(seq 1 "$attempts"); do
        count=$(grep -c 'peer is UP' "$log_file" 2>/dev/null || true)
        if (( count >= wanted )); then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

wait_all_peers_up() {
    local which=$1
    if (( which == 1 )); then
        wait_peer_up_count "$LOG1" 2 240 || die "node1 peers did not become UP"
        wait_peer_up_count "$LOG2" 2 240 || die "node2 peers did not become UP"
        wait_peer_up_count "$LOG3" 2 240 || die "node3 peers did not become UP"
    else
        wait_peer_up_count "$LOG3_RESTARTED" 2 240 ||
            die "restarted node3 peers did not become UP"
    fi
}

verify_retry() {
    local port=$1
    local key=$2
    local size=$3
    local seed=$4
    local attempts=${5:-80}
    local delay=${6:-0.25}
    for _ in $(seq 1 "$attempts"); do
        if "$CLIENT_BIN" verify 127.0.0.1 "$port" "$key" "$size" "$seed" \
            >/dev/null 2>&1; then
            return 0
        fi
        sleep "$delay"
    done
    return 1
}

verify_all_nodes() {
    local key=$1
    local size=$2
    local seed=$3
    verify_retry "$PORT1" "$key" "$size" "$seed" 120 0.25 ||
        die "verification failed on node1 for $key"
    verify_retry "$PORT2" "$key" "$size" "$seed" 120 0.25 ||
        die "verification failed on node2 for $key"
    verify_retry "$PORT3" "$key" "$size" "$seed" 120 0.25 ||
        die "verification failed on node3 for $key"
}

stop_node_gracefully() {
    local pid=$1
    local name=$2
    local log_file=$3
    local status

    kill -TERM "$pid" 2>/dev/null || die "could not stop $name"
    wait "$pid"
    status=$?
    if (( status != 0 )); then
        echo "--- $log_file ---" >&2
        tail -120 "$log_file" >&2
        die "$name exited with status $status"
    fi
    if grep -Eq 'AddressSanitizer|LeakSanitizer|runtime error' "$log_file"; then
        grep -E 'AddressSanitizer|LeakSanitizer|runtime error' "$log_file" >&2
        die "$name produced sanitizer diagnostics"
    fi
}

choose_ports || die "could not reserve three local TCP ports"

start_node 1 "$LOG1"
start_node 2 "$LOG2"
start_node 3 "$LOG3"
wait_tcp "$PORT1" || die "node1 did not listen"
wait_tcp "$PORT2" || die "node2 did not listen"
wait_tcp "$PORT3" || die "node3 did not listen"
wait_all_peers_up 1

KEY1="object-alpha"
SIZE1=$(( (9 * 1024 * 1024) + 123 ))
SEED1=11
"$CLIENT_BIN" write 127.0.0.1 "$PORT1" "$KEY1" "$SIZE1" "$SEED1" ||
    die "initial quorum write failed"
verify_all_nodes "$KEY1" "$SIZE1" "$SEED1"

# One replica crashes. Writes must continue with the remaining 2-of-3.
KILLED_PID3=$PID3
kill -KILL "$KILLED_PID3" 2>/dev/null || die "could not kill node3"
wait "$KILLED_PID3" 2>/dev/null || true
PID3=""
sleep 1.5

KEY2="object-beta"
SIZE2=$(( (5 * 1024 * 1024) + 77 ))
SEED2=29
"$CLIENT_BIN" write 127.0.0.1 "$PORT1" "$KEY2" "$SIZE2" "$SEED2" ||
    die "quorum write with one replica down failed"
verify_retry "$PORT1" "$KEY2" "$SIZE2" "$SEED2" 120 0.25 ||
    die "verification failed on node1 for $KEY2"
verify_retry "$PORT2" "$KEY2" "$SIZE2" "$SEED2" 120 0.25 ||
    die "verification failed on node2 for $KEY2"

# Restart node3. Heartbeats must mark it UP and anti-entropy must copy both the
# missing chunks and the missing manifest from its peers.
start_node 3 "$LOG3_RESTARTED"
wait_tcp "$PORT3" || die "restarted node3 did not listen"
wait_all_peers_up 2
verify_retry "$PORT3" "$KEY2" "$SIZE2" "$SEED2" 160 0.25 ||
    die "anti-entropy did not repair $KEY2 on node3"
verify_retry "$PORT3" "$KEY1" "$SIZE1" "$SEED1" 80 0.25 ||
    die "node3 lost previously replicated $KEY1"

# Stop two replicas gracefully and verify that a lone node cannot form quorum.
stop_node_gracefully "$PID2" "node2" "$LOG2"
PID2=""
stop_node_gracefully "$PID3" "node3" "$LOG3_RESTARTED"
PID3=""
sleep 1.5

FAIL_KEY="object-must-fail"
FAIL_SIZE=$(( 1024 * 1024 ))
FAIL_SEED=71
if FAIL_OUTPUT=$("$CLIENT_BIN" write 127.0.0.1 "$PORT1" "$FAIL_KEY" \
    "$FAIL_SIZE" "$FAIL_SEED" 2>&1); then
    die "write unexpectedly reached quorum with only one node"
fi
if ! grep -Eq 'STORE chunk 0 failed|STORE_OBJECT failed|QUORUM' <<<"$FAIL_OUTPUT"; then
    echo "$FAIL_OUTPUT" >&2
    die "lone-node write failed for an unexpected reason"
fi

stop_node_gracefully "$PID1" "node1" "$LOG1"
PID1=""

echo "test_cluster: quorum, crash/recovery, anti-entropy and lone-node rejection passed"
exit 0
