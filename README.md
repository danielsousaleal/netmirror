# netmirror

**A replicated, content-addressable object store written in pure C11.**
RAID1 over the network, a single HTTP endpoint, and no heavyweight dependencies.

[![Build and test](https://github.com/danielsousaleal/netmirror/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/danielsousaleal/netmirror/actions/workflows/ci.yml)
[![Docker validation](https://img.shields.io/github/actions/workflow/status/danielsousaleal/netmirror/ci.yml?branch=main&label=docker%20validation&logo=docker&logoColor=white)](https://github.com/danielsousaleal/netmirror/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
[![C11](https://img.shields.io/badge/C-11-00599C?logo=c&logoColor=white)](https://en.cppreference.com/w/c/11)

---

**netmirror** is a distributed object store that keeps every object on three
independent nodes and exposes one ordinary HTTP endpoint. Objects are split into
4 MiB chunks, each chunk is named by its SHA-256 digest, and every write must be
acknowledged by at least two of the three nodes before the gateway reports
success. A node can disappear, come back, and repair itself without operator
intervention.

It is a systems-programming portfolio project, not a wrapper around a database.
The storage engine, the binary wire protocol, the event loop, the replication
logic, the heartbeat/anti-entropy machinery, and the HTTP server are all
implemented in C11/POSIX. There is **no Raft**, **no FUSE**, no SQL database, and
no HTTP between storage nodes. The only external runtime dependency is OpenSSL
for SHA-256.

## Architecture

```text
                         +-------------------------------+
   HTTP/1.1  ----------> |        netmirror-gateway      |  :8080
   curl / CLI            |  round-robin + read failover  |
                         +---------------+---------------+
                                         |
                  binary TCP (no HTTP)   |  STORE / FETCH / ACK
             +---------------------------+---------------------------+
             |                           |                           |
             v                           v                           v
    +-----------------+        +-----------------+        +-----------------+
    |     node1       |<------>|     node2       |<------>|     node3       |
    |     :9001       |  PING  |     :9002       |  PING  |     :9003       |
    |  epoll + store  |  PONG  |  epoll + store  |  PONG  |  epoll + store  |
    |     data1/      |        |     data2/      |        |     data3/      |
    +-----------------+        +-----------------+        +-----------------+
             ^                           ^                           ^
             +--------- replication (2-of-3 quorum) --------------+
                        + anti-entropy repair on reconnect
```

- The **gateway** is the only public process. It speaks HTTP/1.1 to clients and
  the custom binary protocol to the nodes.
- The **nodes** own the data. They speak only the binary protocol, run a
  single-threaded level-triggered `epoll` loop, and replicate writes to each
  other in the background.
- **Writes** go through one coordinator node, which persists the chunk locally,
  fans it out as `REPLICA_STORE`, and only ACKs the gateway after a 2-of-3
  quorum.
- **Reads** try nodes in round-robin order and fall back to the next replica on
  failure.

## Quick start

The only requirement is Docker with Compose v2.

```bash
git clone https://github.com/danielsousaleal/netmirror.git
cd netmirror
docker compose up -d --build --wait
```

That starts three storage nodes (ports 9001-9003, internal) and one gateway on
`http://localhost:8080`.

### Upload and download with curl

```bash
curl -fsS http://localhost:8080/health
# {"status":"ok","nodes":3}

curl -fsS -X PUT --data-binary @README.md \
  http://localhost:8080/obj/readme
# {"key":"readme","size":<bytes>,"chunks":1,"sha256":"<64-hex>"}

curl -fsS -o /tmp/readme.md \
  http://localhost:8080/obj/readme

sha256sum README.md /tmp/readme.md   # hashes must match
```

The gateway also accepts `PUT /obj` and `GET /obj` as an alias for the key
`default`. Empty objects are valid.

### Use the native CLI

```bash
make
./build/dev/bin/netmirror-cli put ./README.md http://127.0.0.1:8080 readme
./build/dev/bin/netmirror-cli get readme http://127.0.0.1:8080 /tmp/readme.md
./build/dev/bin/netmirror-cli health http://127.0.0.1:8080
./build/dev/bin/netmirror-cli node-health 127.0.0.1 9001
```

`put` defaults the key to the file basename. `get` defaults the output to
stdout.

### Other ways to run it

```bash
make demo        # docker compose up --build in the foreground
make test        # every unit/integration suite + end-to-end fire test
docker compose down
rm -rf data1 data2 data3   # wipe the bind-mounted node data
```

## HTTP API

| Method | Path          | Success | Description                                      |
| ------ | ------------- | ------- | ------------------------------------------------ |
| `GET`  | `/health`     | `200`   | Gateway liveness as JSON                         |
| `GET`  | `/obj/<key>`  | `200`   | Stream the object; `ETag` and `X-Content-SHA256` |
| `GET`  | `/obj`        | `200`   | Same as `/obj/default`                           |
| `PUT`  | `/obj/<key>`  | `201`   | Stream an object; requires `Content-Length`      |
| `PUT`  | `/obj`        | `201`   | Same as `/obj/default`                           |

Successful `PUT` responses are JSON:

```json
{"key":"readme","size":12345,"chunks":1,"sha256":"6c3d...e91f"}
```

- Keys are 1-127 characters from `[A-Za-z0-9._-]`; `.` and `..` are rejected.
- `Content-Length` is mandatory for `PUT` (`411` otherwise).
- `Expect: 100-continue` is supported.
- `Connection: keep-alive` and bounded HTTP pipelining are supported.
- `Transfer-Encoding: chunked` is **not** supported (`501`).
- Maximum object size is **4 TiB** (1,048,576 chunks of 4 MiB).
- There is no `DELETE`, no object listing endpoint, and no `Range` support.

Errors are JSON and use conventional status codes: `400`, `404`, `405`, `408`,
`411`, `413`, `417`, `431`, `500`, `501`, `502`, `503`, `504`, `505`.

## How it works

### Content-addressable storage

Each object is split into 4 MiB chunks. A chunk is stored under the lowercase
hexadecimal SHA-256 of its content, so identical chunks are naturally
deduplicated within a node:

```text
<data_dir>/
  chunks/
    <sha256-of-chunk>          immutable chunk bytes
  objects/
    <sha256-of-key>.manifest   object metadata and ordered chunk list
```

The manifest is the commit marker. It records the key, total size, update
timestamp, whole-object SHA-256, and the ordered list of chunk digests. The
chunk-hash vector is allocated dynamically from `chunk_count` with
`calloc`/`malloc` and freed with `nm_manifest_free()`; there is no fixed-size
hash array in the struct. Chunks are written first; the manifest is written
last. A crash can therefore leave orphan chunks, but never a manifest that
points to missing data.

Writes are atomic: data goes to a temporary file in the same directory, is
`fsync`'ed, `rename`'d into place, and the parent directory is `fsync`'ed.
Every chunk is hashed before it is written and verified again after it is read.

Manifest wire format (version 1):

| Field         | Size        | Notes                                  |
| ------------- | ----------- | -------------------------------------- |
| magic         | 4 B         | ASCII `NMOB`                           |
| version       | 1 B         | `1`                                    |
| reserved      | 1 B         | `0`                                    |
| key_length    | 2 B, BE     | 1-127                                  |
| key           | n B         | no NUL terminator                      |
| total_size    | 8 B, BE     | object length in bytes                 |
| updated_ms    | 8 B, BE     | Unix epoch milliseconds                |
| chunk_count   | 4 B, BE     | 0-1,048,576                            |
| object_hash   | 32 B        | SHA-256 of the complete object         |
| chunk_hashes  | 32 x c B    | chunk digests in object order          |

### Binary protocol

Every internal message uses a 9-byte header followed by its payload. All
integers are big-endian.

```text
 byte 0      bytes 1..8                 bytes 9..9+len-1
+--------+--------------------------+----------------------------+
| type   | payload length (u64 BE)  | payload                    |
+--------+--------------------------+----------------------------+
   1 B              8 B                       len B
```

| Type                  | Value | Purpose                                      |
| --------------------- | ----- | -------------------------------------------- |
| `PING`                | 0x01  | Heartbeat probe                              |
| `PONG`                | 0x02  | Heartbeat reply                              |
| `STORE`               | 0x03  | Store one chunk                              |
| `FETCH`               | 0x04  | Fetch one chunk                              |
| `ACK`                 | 0x05  | Status + acknowledged type + digest          |
| `ERROR`               | 0x06  | Status + UTF-8 message                       |
| `CHUNK`               | 0x07  | Chunk response                               |
| `STORE_OBJECT`        | 0x08  | Store an object manifest                     |
| `FETCH_OBJECT`        | 0x09  | Fetch an object manifest                     |
| `OBJECT`              | 0x0A  | Manifest response                            |
| `SYNC_REQ`            | 0x0B  | Anti-entropy chunk-catalog page request      |
| `SYNC_RESP`           | 0x0C  | Anti-entropy chunk-catalog page              |
| `LIST_KEYS_REQ`       | 0x0D  | Anti-entropy manifest-key page request       |
| `LIST_KEYS_RESP`      | 0x0E  | Anti-entropy manifest-key page               |
| `REPLICA_STORE`       | 0x0F  | Node-to-node chunk replication               |
| `REPLICA_STORE_OBJECT`| 0x10  | Node-to-node manifest replication            |

`REPLICA_*` frames are applied locally and never fanned out again, which
prevents replication loops. Frames, parser buffers, and outbound queues are all
bounded; oversized input closes the connection instead of growing without
limit.

### Write path (2-of-3 quorum)

1. The gateway parses the HTTP request, enforces `Content-Length`, and selects a
   coordinator node with round-robin.
2. The client body is streamed in 4 MiB pieces. The gateway never buffers the
   whole object in memory.
3. For each chunk, the gateway computes SHA-256 and sends `STORE` to the
   coordinator.
4. The coordinator verifies the digest, persists the chunk atomically, and fans
   it out to every peer that is currently `UP` as `REPLICA_STORE`.
5. The local write counts as the first acknowledgement. The coordinator ACKs
   the gateway only when **2 of 3** replicas have acknowledged.
6. After the last chunk, the gateway sends `STORE_OBJECT` with the manifest,
   which follows the same quorum path.
7. The gateway returns `201 Created` with the object digest. If quorum cannot
   be reached, it returns `503` instead of claiming success.

An acknowledged write is therefore present on at least two nodes. The third
node catches up later through anti-entropy if it was down.

### Read path

1. The gateway selects a node with round-robin and tries to fetch the manifest
   (`FETCH_OBJECT`), falling back to the next node on failure or `NOT_FOUND`.
2. It streams the object chunk by chunk, selecting nodes in round-robin order
   and falling back on failure.
3. Every received chunk is hashed and compared with the digest in the manifest
   before it is sent to the HTTP client.
4. The response carries `Content-Length`, an `ETag` with the object SHA-256, and
   `X-Content-SHA256`.

Reads do not use a quorum. They prefer availability and integrity (per-chunk
verification) over linearizability.

### Heartbeats, reconnects, and anti-entropy

- Each node keeps one persistent non-blocking outbound TCP connection per peer.
- `PING` is sent every **1 s** by default; a matching `PONG` within **3 s**
  marks the peer `UP`. A missed deadline closes the connection and schedules a
  reconnect with exponential backoff from 250 ms to 5 s.
- Every **15 s**, each node runs an anti-entropy pass against every `UP` peer:
  - it pages through the peer's chunk catalog and pulls every missing chunk;
  - it pages through the peer's manifest keys and pulls manifests that are
    missing locally or have a newer `updated_ms`.
- Anti-entropy is one-way per peer (each node pulls what it lacks), so a node
  that returns after downtime repairs itself automatically.

### Concurrency model

- **Storage node:** one thread, one level-triggered `epoll` loop. All sockets
  are non-blocking. Each readiness event performs bounded reads and writes;
  partial frames are retained. Outbound data is queued in bounded buffers, and
  input is paused when a peer's backlog crosses a high-water mark. Disk I/O
  (including `fsync`) is synchronous inside the loop, so a slow disk slows that
  node's event loop.
- **Gateway:** one detached worker thread per accepted HTTP connection, with a
  hard cap (128 by default). A slow storage node delays only the request waiting
  on it; the accept loop and other clients keep moving. Node RPCs use explicit
  connect/read/write timeouts.

## Configuration

CLI flags override environment variables. Storage nodes and the gateway have
independent settings.

### Node

| Flag                          | Environment                     | Default   |
| ----------------------------- | ------------------------------- | --------- |
| `--bind`                      | `NM_BIND_ADDR`                  | `0.0.0.0` |
| `--port`                      | `NM_PORT`                       | `7001`    |
| `--node-id`                   | `NM_NODE_ID`                    | `node`    |
| `--data-dir`                  | `NM_DATA_DIR`                   | `./data`  |
| `--peers`                     | `NM_PEERS`                      | empty     |
| `--max-connections`           | `NM_MAX_CONNECTIONS`            | `64`      |
| `--idle-timeout-ms`           | `NM_IDLE_TIMEOUT_MS`            | `60000`   |
| `--connect-timeout-ms`        | `NM_CONNECT_TIMEOUT_MS`         | `1000`    |
| `--rpc-timeout-ms`            | `NM_RPC_TIMEOUT_MS`             | `5000`    |
| `--heartbeat-interval-ms`     | `NM_HEARTBEAT_INTERVAL_MS`      | `1000`    |
| `--heartbeat-timeout-ms`      | `NM_HEARTBEAT_TIMEOUT_MS`       | `3000`    |
| `--anti-entropy-interval-ms`  | `NM_ANTI_ENTROPY_INTERVAL_MS`   | `15000`   |
| `--log-level`                 | `NM_LOG_LEVEL`                  | `info`    |

`--peers` is a comma-separated `host:port` list that excludes the node itself.
With an empty peer list the node runs standalone and acknowledges writes
locally. `--heartbeat-timeout-ms` must be greater than
`--heartbeat-interval-ms`.

### Gateway

| Flag                          | Environment                     | Default                                        |
| ----------------------------- | ------------------------------- | ---------------------------------------------- |
| `--bind`                      | `NM_GATEWAY_BIND`               | `0.0.0.0`                                      |
| `--port`                      | `NM_GATEWAY_PORT`               | `8080`                                         |
| `--nodes`                     | `NM_NODES`                      | `127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003` |
| `--max-connections`           | `NM_GATEWAY_MAX_CONNECTIONS`    | `128`                                          |
| `--http-timeout-ms`           | `NM_HTTP_TIMEOUT_MS`            | `30000`                                        |
| `--node-connect-timeout-ms`   | `NM_NODE_CONNECT_TIMEOUT_MS`    | `1000`                                         |
| `--node-timeout-ms`           | `NM_NODE_TIMEOUT_MS`            | `10000`                                        |
| `--log-level`                 | `NM_LOG_LEVEL`                  | `info`                                         |

## Failure behavior

| Scenario                     | `PUT`                                      | `GET`                                        |
| ---------------------------- | ------------------------------------------ | -------------------------------------------- |
| All three nodes healthy      | `201`; quorum 2 of 3                       | `200`                                        |
| One node down                | `201`; quorum still reached with two nodes | `200` via a surviving replica                |
| Two nodes down               | `503`; quorum impossible                   | `200` if the surviving node has the object   |
| Node rejoins after downtime  | Writes continue                            | Missing chunks/manifests repair automatically |
| Coordinator fails mid-upload | `502`; the client may retry                | Unaffected                                   |

A `201` means at least two nodes persisted the chunk or manifest. It does not
mean all three have it yet; the third is repaired by anti-entropy.

## Repository layout

| Path                         | Contents                                                        |
| ---------------------------- | --------------------------------------------------------------- |
| `include/`                   | Public contracts: protocol, storage, network, cluster, gateway  |
| `src/`                       | Implementation of the node, gateway, CLI, storage, cluster      |
| `tests/`                     | C unit/integration tests and shell-driven scenario tests        |
| `docker/`                    | Container entrypoint (drops to UID 10001 after fixing data dir) |
| `.github/workflows/ci.yml`   | Build/test job and Docker validation job                        |
| `Dockerfile`                 | Multi-stage Debian Bookworm build and minimal runtime           |
| `docker-compose.yml`         | Three nodes + one gateway with health checks                    |
| `Makefile`                   | `all`, `check`, `test`, `demo`, `clean`, dev/release modes       |

## Build and test

Linux/POSIX build dependencies:

```bash
sudo apt-get install build-essential libssl-dev curl
```

```bash
make                 # development build with ASan + UBSan
make MODE=release    # optimized build without sanitizers
make check           # compile-check the public contract header
make test            # every suite plus the end-to-end fire test
make clean
```

Binaries land in `build/<mode>/bin/`:

- `netmirror-node` — storage and replication node
- `netmirror-gateway` — HTTP/1.1 front end
- `netmirror-cli` — upload, download, health, node-health

### Test suites

| Command             | Coverage                                                        |
| ------------------- | --------------------------------------------------------------- |
| `make test-core`    | SHA-256, chunking, manifests, atomic storage                    |
| `make test-network` | Framing, partial parser, byte order, buffers                    |
| `make test-node`    | Binary protocol server, ACK/ERROR paths, graceful shutdown      |
| `make test-cluster` | Quorum, crash/recovery, anti-entropy, lone-node rejection       |
| `make test-gateway` | 10 MiB streaming, keep-alive, `100-continue`, fallback, shutdown |
| `make test`         | All of the above plus the end-to-end scenario                   |

The end-to-end test uploads small, 12 MiB, and empty objects, verifies their
SHA-256 after download, kills one node and reads again, uploads with two nodes
alive, kills another node and confirms that writes are rejected while reads
still succeed, and checks every process for sanitizer output on shutdown.

### CI

`.github/workflows/ci.yml` runs on pushes and pull requests to `main`:

1. **`build-and-test`** — installs the toolchain, stabilizes ASLR entropy for
   the sanitizer runtime, runs `make check`, builds with ASan/UBSan, and runs
   the full test suite.
2. **`docker-validation`** — runs only after `build-and-test` succeeds, builds
   the runtime image with BuildKit's GitHub Actions cache, starts the Compose
   stack with `--wait`, performs a 2 MiB `PUT`/`GET` byte-for-byte comparison,
   dumps logs on failure, and always tears the stack down.

## Design trade-offs and limitations

netmirror is intentionally small enough to read in one sitting. The following
limitations are deliberate:

- **No TLS and no authentication.** Anyone who can reach the gateway can read or
  overwrite every object. Run it on a trusted network, behind a VPN, or behind a
  reverse proxy that terminates TLS and enforces authentication. Data at rest is
  also unencrypted.
- **No orphan-chunk garbage collection.** A failed multi-chunk upload, or a
  re-upload that replaces a manifest, can leave chunks that no manifest
  references. They cost disk space but cannot corrupt reads. A future GC would
  need to rebuild the live-chunk set from all manifests and coordinate with
  anti-entropy.
- **No `Transfer-Encoding: chunked`.** Uploads must carry `Content-Length`
  (`411` without it, `501` for chunked). This keeps the parser small and makes
  streaming bounds explicit. There is also no `Range`, resume, or multipart
  support.
- **No consensus protocol.** There is no Raft and no leader. Writes use a
  2-of-3 quorum, while reads are served by a single replica with fallback.
  Concurrent writes to the same key can both return `201`; the final value is
  resolved by last-write-wins on the manifest's `updated_ms` during
  anti-entropy. Clock skew can delay convergence, and reads are not
  linearizable.
- **Static membership.** Peers are configured at startup; nodes cannot join or
  leave dynamically. Replication targets every configured peer that is `UP`,
  and the quorum is `floor(N/2)+1` over the configured cluster size.
- **Single-threaded storage nodes.** One slow disk stalls that node's event
  loop. Chunk writes and `fsync` are synchronous by design in v1. The gateway
  uses one worker thread per HTTP connection with a hard connection cap.
- **The `/health` endpoint is shallow.** It reports that the gateway process is
  alive, not that the storage nodes are reachable. Container health checks use
  the binary `node-health` probe; the gateway health check only proves the
  HTTP front end responds.
- **No external listing or deletion.** Keys are known by their writers. There
  is no consistency guarantee across a listing because no listing endpoint
  exists.
- **Large manifests are one frame.** At the 4 TiB ceiling a manifest carries
  1,048,576 digests (~32 MiB) and must fit in a single frame.
  `NM_MAX_FRAME_PAYLOAD` and the per-connection parser buffer are sized for it,
  which increases memory pressure on nodes and requires the manifest RPC to
  finish within the configured timeouts; tune those limits for slower links.
- **Chunk size is part of the storage contract.** `NM_CHUNK_SIZE` is compiled
  in and is not stored in manifests. Changing it changes every chunk boundary
  and digest, so existing manifests become unreadable without a version bump
  and migration.
- **Fixed maximums.** Objects are limited to 4 TiB (1,048,576 chunks of 4 MiB),
  and keys to 127 characters.

## License

MIT. See [LICENSE](LICENSE).
