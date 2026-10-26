#!/usr/bin/env bash
# Start one netmirror node and exercise its binary protocol end to end.
set -u

NODE_BIN="${NM_NODE_BIN:-}"
CLIENT_BIN="${NM_TEST_CLIENT_BIN:-}"

if [[ -z "$NODE_BIN" || -z "$CLIENT_BIN" ]]; then
    echo "NM_NODE_BIN and NM_TEST_CLIENT_BIN must be set" >&2
    exit 2
fi
if [[ ! -x "$NODE_BIN" ]]; then
    echo "node binary not executable: $NODE_BIN" >&2
    exit 2
fi
if [[ ! -x "$CLIENT_BIN" ]]; then
    echo "test client not executable: $CLIENT_BIN" >&2
    exit 2
fi

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/netmirror-node-test.XXXXXX")" || exit 1
NODE_PID=""
NODE_LOG="$TMP_ROOT/node.log"

cleanup() {
    local status=$?

    if [[ -n "$NODE_PID" ]] && kill -0 "$NODE_PID" 2>/dev/null; then
        kill -TERM "$NODE_PID" 2>/dev/null || true
        wait "$NODE_PID" 2>/dev/null || true
    fi
    if (( status != 0 )); then
        echo "--- node log ---" >&2
        [[ -f "$NODE_LOG" ]] && cat "$NODE_LOG" >&2
        echo "--- end node log ---" >&2
    fi
    rm -rf -- "$TMP_ROOT"
    exit "$status"
}
trap cleanup EXIT INT TERM

choose_port() {
    local attempt port
    for attempt in $(seq 1 100); do
        port=$(( 20000 + (RANDOM % 20000) ))
        if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            echo "$port"
            return 0
        fi
    done
    return 1
}

PORT="$(choose_port)" || {
    echo "could not find a free TCP port" >&2
    exit 1
}

export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1:abort_on_error=1:strict_string_checks=1}"
export UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1:print_stacktrace=1}"

NM_NODE_ID="stage3-node" \
NM_BIND_ADDR="127.0.0.1" \
NM_PORT="$PORT" \
NM_DATA_DIR="$TMP_ROOT/data" \
NM_MAX_CONNECTIONS="16" \
NM_IDLE_TIMEOUT_MS="10000" \
NM_LOG_LEVEL="debug" \
    "$NODE_BIN" >"$NODE_LOG" 2>&1 &
NODE_PID=$!

READY=0
for _attempt in $(seq 1 200); do
    if ! kill -0 "$NODE_PID" 2>/dev/null; then
        echo "node exited during startup" >&2
        exit 1
    fi
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
        READY=1
        break
    fi
    sleep 0.05
done
if (( READY == 0 )); then
    echo "node did not start listening on port $PORT" >&2
    exit 1
fi

if ! "$CLIENT_BIN" 127.0.0.1 "$PORT"; then
    echo "integration client failed" >&2
    exit 1
fi

kill -TERM "$NODE_PID" 2>/dev/null || true
set +e
wait "$NODE_PID"
NODE_EXIT=$?
set -e
NODE_PID=""

if (( NODE_EXIT != 0 )); then
    echo "node exited with status $NODE_EXIT after SIGTERM" >&2
    exit 1
fi
if grep -Eq 'AddressSanitizer|LeakSanitizer|runtime error' "$NODE_LOG"; then
    echo "sanitizer diagnostics found in node log" >&2
    grep -E 'AddressSanitizer|LeakSanitizer|runtime error' "$NODE_LOG" >&2
    exit 1
fi

echo "test_node: server integration and graceful shutdown passed"
TEST_STATUS=0
exit "$TEST_STATUS"
