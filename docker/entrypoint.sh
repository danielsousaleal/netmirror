#!/bin/sh
# The bind-mounted data directory is owned by the host/root. Fix its ownership
# once per container start, then drop privileges before exec'ing the service.
set -eu

DATA_DIR="${NM_DATA_DIR:-/app/data}"

if [ "$(id -u)" = "0" ]; then
    mkdir -p "$DATA_DIR"
    chown -R 10001:10001 "$DATA_DIR"
    exec setpriv --reuid=10001 --regid=10001 --clear-groups "$@"
fi

exec "$@"
