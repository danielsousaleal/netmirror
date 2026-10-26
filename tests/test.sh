#!/usr/bin/env bash
# End-to-end fire test for netmirror (3 nodes + 1 gateway).
set -u

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
MODE="${MODE:-dev}"

cd "$PROJECT_ROOT" || exit 1
make MODE="$MODE" all || {
    echo "FAIL: make all did not complete" >&2
    exit 1
}

BIN_DIR="$PROJECT_ROOT/build/$MODE/bin"
NODE_BIN="$BIN_DIR/netmirror-node"
GATEWAY_BIN="$BIN_DIR/netmirror-gateway"
CLI_BIN="$BIN_DIR/netmirror-cli"
for binary in "$NODE_BIN" "$GATEWAY_BIN" "$CLI_BIN"; do
    [[ -x "$binary" ]] || {
        echo "FAIL: missing executable $binary" >&2
        exit 1
    }
done

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/netmirror-e2e.XXXXXX")" || exit 1
PID1=""
PID2=""
PID3=""
GATEWAY_PID=""
LOG1="$TMP_ROOT/node1.log"
LOG2="$TMP_ROOT/node2.log"
LOG3="$TMP_ROOT/node3.log"
GATEWAY_LOG="$TMP_ROOT/gateway.log"
PORT1=""
PORT2=""
PORT3=""
PORT_GATEWAY=""
GATEWAY_URL=""

export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1:abort_on_error=1:strict_string_checks=1}"
export UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1:print_stacktrace=1}"

cleanup() {
    local status=$?
    local pid

    for pid in "$GATEWAY_PID" "$PID1" "$PID2" "$PID3"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    if (( status != 0 )); then
        for log in "$LOG1" "$LOG2" "$LOG3" "$GATEWAY_LOG"; do
            if [[ -f "$log" ]]; then
                echo "--- $log ---" >&2
                tail -200 "$log" >&2
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
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

choose_ports() {
    local attempt base port
    for attempt in $(seq 1 250); do
        base=$(( 28000 + (RANDOM % 12000) ))
        if (( base + 3 > 65535 )); then
            continue
        fi
        local free=1
        for port in "$base" "$((base + 1))" "$((base + 2))" "$((base + 3))"; do
            if port_in_use "$port"; then
                free=0
                break
            fi
        done
        if (( free == 1 )); then
            PORT1=$base
            PORT2=$((base + 1))
            PORT3=$((base + 2))
            PORT_GATEWAY=$((base + 3))
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
        [[ -n "$peers" ]] && peers+=","
        peers+="127.0.0.1:$PORT2"
    fi
    if (( self != 3 )); then
        [[ -n "$peers" ]] && peers+=","
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
        *) die "internal invalid node index" ;;
    esac
    peers="$(peer_string_for "$index")"
    NM_NODE_ID="e2e-node-$index" \
    NM_BIND_ADDR="127.0.0.1" \
    NM_PORT="$port" \
    NM_DATA_DIR="$TMP_ROOT/node$index-data" \
    NM_PEERS="$peers" \
    NM_MAX_CONNECTIONS="32" \
    NM_IDLE_TIMEOUT_MS="60000" \
    NM_HEARTBEAT_INTERVAL_MS="300" \
    NM_HEARTBEAT_TIMEOUT_MS="1500" \
    NM_RPC_TIMEOUT_MS="4000" \
    NM_ANTI_ENTROPY_INTERVAL_MS="1000" \
    NM_LOG_LEVEL="info" \
        "$NODE_BIN" >"$log_file" 2>&1 &
    case "$index" in
        1) PID1=$! ;;
        2) PID2=$! ;;
        3) PID3=$! ;;
    esac
}

wait_tcp() {
    local port=$1
    local attempts=${2:-240}
    for _ in $(seq 1 "$attempts"); do
        if port_in_use "$port"; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

wait_peer_up() {
    local log_file=$1
    local wanted=$2
    local attempts=${3:-240}
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

cli_put() {
    local file=$1
    local key="${file##*/}"
    timeout 50s "$CLI_BIN" put "$file" "$GATEWAY_URL" \
        >"$TMP_ROOT/put-$key.json" 2>"$TMP_ROOT/put-$key.err"
}

cli_get() {
    local key=$1
    local output=$2
    timeout 50s "$CLI_BIN" get "$key" "$GATEWAY_URL" "$output" \
        >"$TMP_ROOT/get-$key.out" 2>"$TMP_ROOT/get-$key.err"
}

assert_same_file() {
    local expected=$1
    local actual=$2
    cmp -s "$expected" "$actual" || return 1
    local expected_sha actual_sha
    expected_sha=$(sha256sum "$expected" | awk '{print $1}')
    actual_sha=$(sha256sum "$actual" | awk '{print $1}')
    [[ "$expected_sha" == "$actual_sha" ]]
}

stop_gracefully() {
    local pid=$1
    local name=$2
    local log_file=$3
    local status

    kill -TERM "$pid" 2>/dev/null || return 1
    wait "$pid"
    status=$?
    if (( status != 0 )); then
        echo "--- $log_file ---" >&2
        tail -160 "$log_file" >&2
        return 1
    fi
    if grep -Eq 'AddressSanitizer|LeakSanitizer|runtime error' "$log_file"; then
        grep -E 'AddressSanitizer|LeakSanitizer|runtime error' "$log_file" >&2
        return 1
    fi
    return 0
}

choose_ports || die "could not reserve four local ports"

start_node 1 "$LOG1"
start_node 2 "$LOG2"
start_node 3 "$LOG3"
wait_tcp "$PORT1" || die "node1 did not open its TCP port"
wait_tcp "$PORT2" || die "node2 did not open its TCP port"
wait_tcp "$PORT3" || die "node3 did not open its TCP port"
wait_peer_up "$LOG1" 2 || die "node1 did not see both peers"
wait_peer_up "$LOG2" 2 || die "node2 did not see both peers"
wait_peer_up "$LOG3" 2 || die "node3 did not see both peers"

GATEWAY_URL="http://127.0.0.1:$PORT_GATEWAY"
NM_GATEWAY_BIND="127.0.0.1" \
NM_GATEWAY_PORT="$PORT_GATEWAY" \
NM_NODES="127.0.0.1:$PORT1,127.0.0.1:$PORT2,127.0.0.1:$PORT3" \
NM_GATEWAY_MAX_CONNECTIONS="16" \
NM_HTTP_TIMEOUT_MS="20000" \
NM_NODE_CONNECT_TIMEOUT_MS="600" \
NM_NODE_TIMEOUT_MS="5000" \
NM_LOG_LEVEL="info" \
    "$GATEWAY_BIN" >"$GATEWAY_LOG" 2>&1 &
GATEWAY_PID=$!
wait_tcp "$PORT_GATEWAY" || die "gateway did not listen"

echo "E2E: checking CLI health command"
HEALTH_OUTPUT=$(timeout 20s "$CLI_BIN" health "$GATEWAY_URL") ||
    die "CLI health command failed"
grep -q '"status":"ok"' <<<"$HEALTH_OUTPUT" ||
    die "CLI health output is invalid"

head -c 4096 /dev/urandom >"$TMP_ROOT/small.bin" || die "small fixture"
dd if=/dev/urandom of="$TMP_ROOT/large.bin" bs=1M count=12 status=none ||
    die "large fixture"
: >"$TMP_ROOT/empty.bin" || die "empty fixture"
head -c $((3 * 1024 * 1024)) /dev/urandom >"$TMP_ROOT/durable.bin" ||
    die "durable fixture"
head -c 1024 /dev/urandom >"$TMP_ROOT/quorum-fail.bin" ||
    die "failure fixture"

echo "E2E: uploading small, >10 MiB and empty objects"
cli_put "$TMP_ROOT/small.bin" || die "PUT small.bin failed"
cli_put "$TMP_ROOT/large.bin" || die "PUT large.bin (>10 MiB) failed"
cli_put "$TMP_ROOT/empty.bin" || die "PUT empty.bin failed"

echo "E2E: downloading and comparing SHA-256"
cli_get "small.bin" "$TMP_ROOT/got-small.bin" ||
    die "GET small.bin failed"
cli_get "large.bin" "$TMP_ROOT/got-large.bin" ||
    die "GET large.bin failed"
cli_get "empty.bin" "$TMP_ROOT/got-empty.bin" ||
    die "GET empty.bin failed"
assert_same_file "$TMP_ROOT/small.bin" "$TMP_ROOT/got-small.bin" ||
    die "small.bin integrity mismatch"
assert_same_file "$TMP_ROOT/large.bin" "$TMP_ROOT/got-large.bin" ||
    die "large.bin integrity mismatch"
assert_same_file "$TMP_ROOT/empty.bin" "$TMP_ROOT/got-empty.bin" ||
    die "empty.bin integrity mismatch"

echo "E2E: killing node3 (SIGKILL); gateway must still serve"
KILL3=$PID3
kill -KILL "$KILL3" 2>/dev/null || die "could not kill node3"
wait "$KILL3" 2>/dev/null || true
PID3=""
sleep 1.2
cli_get "large.bin" "$TMP_ROOT/got-large-after-kill.bin" ||
    die "GET large.bin failed with node3 down"
assert_same_file "$TMP_ROOT/large.bin" "$TMP_ROOT/got-large-after-kill.bin" ||
    die "large.bin corrupted with node3 down"

echo "E2E: uploading with two nodes alive, then killing node2"
cli_put "$TMP_ROOT/durable.bin" ||
    die "PUT durable.bin failed with two nodes alive"
cli_get "durable.bin" "$TMP_ROOT/got-durable.bin" ||
    die "GET durable.bin failed with two nodes alive"
assert_same_file "$TMP_ROOT/durable.bin" "$TMP_ROOT/got-durable.bin" ||
    die "durable.bin integrity mismatch"

KILL2=$PID2
kill -KILL "$KILL2" 2>/dev/null || die "could not kill node2"
wait "$KILL2" 2>/dev/null || true
PID2=""
sleep 1.2

echo "E2E: one node left; quorum write must fail"
if timeout 50s "$CLI_BIN" put "$TMP_ROOT/quorum-fail.bin" "$GATEWAY_URL" \
    >"$TMP_ROOT/quorum.out" 2>"$TMP_ROOT/quorum.err"; then
    die "upload unexpectedly succeeded with only one node"
fi
if ! grep -Eq 'HTTP (500|502|503|504)|quorum|QUORUM' \
    "$TMP_ROOT/quorum.err" "$TMP_ROOT/quorum.out"; then
    cat "$TMP_ROOT/quorum.out" "$TMP_ROOT/quorum.err" >&2
    die "lone-node upload failed for an unexpected reason"
fi
echo "E2E: lone-node write rejected by gateway: $(tr '\n' ' ' <"$TMP_ROOT/quorum.err")"

echo "E2E: download must still work with one node"
cli_get "durable.bin" "$TMP_ROOT/got-durable-last.bin" ||
    die "GET durable.bin failed with one node left"
assert_same_file "$TMP_ROOT/durable.bin" "$TMP_ROOT/got-durable-last.bin" ||
    die "durable.bin corrupted with one node left"

echo "E2E: graceful shutdown and sanitizer checks"
stop_gracefully "$GATEWAY_PID" "gateway" "$GATEWAY_LOG" ||
    die "gateway shutdown failed"
GATEWAY_PID=""
stop_gracefully "$PID1" "node1" "$LOG1" ||
    die "node1 shutdown failed"
PID1=""

rm -rf -- "$TMP_ROOT"
TMP_ROOT=""
trap - EXIT INT TERM
echo "test_e2e: PASS (small, 12MiB, empty, node loss, lone-node quorum rejection, clean shutdown)"
exit 0
