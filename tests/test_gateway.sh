#!/usr/bin/env bash
# HTTP gateway integration:
#   - 10 MiB streaming PUT with Expect: 100-continue
#   - GET hash verification
#   - HTTP/1.1 keep-alive connection reuse
#   - health and 404 responses
#   - read/write fallback with one of three nodes dead
#   - graceful shutdown under ASan/UBSan
set -u

NODE_BIN="${NM_NODE_BIN:-}"
GATEWAY_BIN="${NM_GATEWAY_BIN:-}"
if [[ -z "$NODE_BIN" || -z "$GATEWAY_BIN" ]]; then
    echo "NM_NODE_BIN and NM_GATEWAY_BIN must be set" >&2
    exit 2
fi
if [[ ! -x "$NODE_BIN" || ! -x "$GATEWAY_BIN" ]]; then
    echo "node or gateway binary is not executable" >&2
    exit 2
fi
if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required for gateway tests" >&2
    exit 2
fi

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/netmirror-gateway-test.XXXXXX")" || exit 1
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
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

choose_ports() {
    local attempt base port
    for attempt in $(seq 1 250); do
        base=$(( 25000 + (RANDOM % 14000) ))
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
        *) die "invalid node index" ;;
    esac
    peers="$(peer_string_for "$index")"
    NM_NODE_ID="gateway-node-$index" \
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
    local attempts=${2:-200}
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

http_put() {
    local key=$1
    local file=$2
    local headers=$3
    local response_body=$4
    local code
    code=$(curl -sS --http1.1 --max-time 45 -D "$headers" -o "$response_body" \
        -w '%{http_code}' -X PUT -H 'Expect: 100-continue' \
        --data-binary @"$file" "http://127.0.0.1:$PORT_GATEWAY/obj/$key") ||
        die "curl PUT failed for $key"
    echo "$code"
}

http_get() {
    local key=$1
    local output=$2
    local headers=$3
    local code
    code=$(curl -sS --http1.1 --max-time 45 -D "$headers" -o "$output" \
        -w '%{http_code}' "http://127.0.0.1:$PORT_GATEWAY/obj/$key") ||
        die "curl GET failed for $key"
    echo "$code"
}

stop_gracefully() {
    local pid=$1
    local name=$2
    local log_file=$3
    local status

    kill -TERM "$pid" 2>/dev/null || die "cannot stop $name"
    wait "$pid"
    status=$?
    if (( status != 0 )); then
        tail -160 "$log_file" >&2
        die "$name exited with status $status"
    fi
    if grep -Eq 'AddressSanitizer|LeakSanitizer|runtime error' "$log_file"; then
        grep -E 'AddressSanitizer|LeakSanitizer|runtime error' "$log_file" >&2
        die "$name produced sanitizer diagnostics"
    fi
}

choose_ports || die "could not reserve four local TCP ports"

start_node 1 "$LOG1"
start_node 2 "$LOG2"
start_node 3 "$LOG3"
wait_tcp "$PORT1" || die "node1 did not listen"
wait_tcp "$PORT2" || die "node2 did not listen"
wait_tcp "$PORT3" || die "node3 did not listen"
wait_peer_up "$LOG1" 2 || die "node1 peers did not become UP"
wait_peer_up "$LOG2" 2 || die "node2 peers did not become UP"
wait_peer_up "$LOG3" 2 || die "node3 peers did not become UP"

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

dd if=/dev/urandom of="$TMP_ROOT/object.bin" bs=1M count=10 status=none ||
    die "could not create 10 MiB test object"
INPUT_SHA=$(sha256sum "$TMP_ROOT/object.bin" | awk '{print $1}')

CODE=$(http_put "gateway-alpha" "$TMP_ROOT/object.bin" \
    "$TMP_ROOT/put.headers" "$TMP_ROOT/put.body")
[[ "$CODE" == "201" ]] || die "PUT returned $CODE, expected 201"
grep -q '100 Continue' "$TMP_ROOT/put.headers" ||
    die "gateway did not emit 100 Continue"
grep -q "$INPUT_SHA" "$TMP_ROOT/put.body" ||
    die "PUT response does not contain the object hash"

CODE=$(http_get "gateway-alpha" "$TMP_ROOT/get.bin" "$TMP_ROOT/get.headers")
[[ "$CODE" == "200" ]] || die "GET returned $CODE, expected 200"
cmp -s "$TMP_ROOT/object.bin" "$TMP_ROOT/get.bin" ||
    die "downloaded object differs from uploaded object"
GET_SHA=$(sha256sum "$TMP_ROOT/get.bin" | awk '{print $1}')
[[ "$GET_SHA" == "$INPUT_SHA" ]] || die "downloaded SHA-256 mismatch"
grep -qi '^Connection: keep-alive' "$TMP_ROOT/get.headers" ||
    die "GET response did not advertise keep-alive"

# Two transfers in one curl process: the second must reuse the connection.
CONNECTS=$(curl -sS --http1.1 --max-time 45 -o /dev/null -o /dev/null \
    -w '%{num_connects}\n' \
    "http://127.0.0.1:$PORT_GATEWAY/obj/gateway-alpha" \
    "http://127.0.0.1:$PORT_GATEWAY/obj/gateway-alpha") ||
    die "keep-alive curl sequence failed"
LAST_CONNECTS=$(tail -n 1 <<<"$CONNECTS")
[[ "$LAST_CONNECTS" == "0" ]] ||
    die "HTTP connection was not reused (num_connects=$LAST_CONNECTS)"

# Concurrent readers must not interfere with each other. The gateway handles
# each HTTP connection in its own worker while node selection is round-robin.
CONCURRENT_PIDS=()
for index in 1 2 3 4; do
    curl -sS --http1.1 --max-time 45 \
        -o "$TMP_ROOT/concurrent-$index.bin" \
        "http://127.0.0.1:$PORT_GATEWAY/obj/gateway-alpha" &
    CONCURRENT_PIDS+=("$!")
done
for pid in "${CONCURRENT_PIDS[@]}"; do
    wait "$pid" || die "concurrent GET worker failed"
done
for index in 1 2 3 4; do
    cmp -s "$TMP_ROOT/object.bin" "$TMP_ROOT/concurrent-$index.bin" ||
        die "concurrent GET $index returned corrupt data"
done

HEALTH_CODE=$(curl -sS --max-time 10 -o "$TMP_ROOT/health.json" \
    -w '%{http_code}' "http://127.0.0.1:$PORT_GATEWAY/health") ||
    die "health request failed"
[[ "$HEALTH_CODE" == "200" ]] || die "health returned $HEALTH_CODE"
grep -q '"status":"ok"' "$TMP_ROOT/health.json" ||
    die "health body is invalid"

NOT_FOUND_CODE=$(curl -sS --max-time 10 -o /dev/null \
    -w '%{http_code}' "http://127.0.0.1:$PORT_GATEWAY/obj/missing") ||
    die "404 request failed"
[[ "$NOT_FOUND_CODE" == "404" ]] ||
    die "missing object returned $NOT_FOUND_CODE, expected 404"

# Kill a replica. Reads must fall back and writes still reach 2-of-3 quorum.
KILLED_NODE3=$PID3
kill -KILL "$KILLED_NODE3" 2>/dev/null || die "could not kill node3"
wait "$KILLED_NODE3" 2>/dev/null || true
PID3=""
sleep 1.5

CODE=$(http_get "gateway-alpha" "$TMP_ROOT/get-after-down.bin" \
    "$TMP_ROOT/get-after-down.headers")
[[ "$CODE" == "200" ]] || die "GET with one node down returned $CODE"
cmp -s "$TMP_ROOT/object.bin" "$TMP_ROOT/get-after-down.bin" ||
    die "GET with one node down returned corrupt data"

dd if=/dev/urandom of="$TMP_ROOT/object2.bin" bs=1M count=5 status=none ||
    die "could not create second test object"
SECOND_SHA=$(sha256sum "$TMP_ROOT/object2.bin" | awk '{print $1}')
CODE=$(http_put "gateway-beta" "$TMP_ROOT/object2.bin" \
    "$TMP_ROOT/put2.headers" "$TMP_ROOT/put2.body")
[[ "$CODE" == "201" ]] || die "PUT with one node down returned $CODE"
CODE=$(http_get "gateway-beta" "$TMP_ROOT/get2.bin" "$TMP_ROOT/get2.headers")
[[ "$CODE" == "200" ]] || die "GET of new object returned $CODE"
SECOND_GET_SHA=$(sha256sum "$TMP_ROOT/get2.bin" | awk '{print $1}')
[[ "$SECOND_GET_SHA" == "$SECOND_SHA" ]] || die "second object hash mismatch"

stop_gracefully "$GATEWAY_PID" "gateway" "$GATEWAY_LOG"
GATEWAY_PID=""
stop_gracefully "$PID1" "node1" "$LOG1"
PID1=""
stop_gracefully "$PID2" "node2" "$LOG2"
PID2=""

echo "test_gateway: 10MiB streaming, keep-alive, 100-continue, fallback and shutdown passed"
exit 0
