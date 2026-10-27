/*
 * netmirror - replication node
 *
 * The data plane is a single-threaded, level-triggered epoll server:
 *
 *   - listening, client and peer sockets are non-blocking;
 *   - one bounded recv() is performed per readiness event, so a busy peer
 *     cannot starve the rest of the connections;
 *   - partial frames are retained by nm_frame_parser_t until complete;
 *   - responses are appended to a bounded output buffer and drained through
 *     EPOLLOUT, never through a blocking write;
 *   - input is paused when the output backlog reaches a high-water mark and
 *     resumed after the backlog drains.
 *
 * A client STORE is first persisted locally, then handed to cluster.c, which
 * fans it out as REPLICA_STORE frames. The final ACK is emitted only when the
 * asynchronous 2-of-3 quorum completes. REPLICA_STORE is applied locally and
 * never fanned out again, preventing replication loops.
 *
 * Disk operations are intentionally synchronous in this v1 process. They are
 * bounded to one 4 MiB chunk and contain no network waits, so they cannot
 * deadlock the state machine; they can increase latency while the filesystem
 * performs write/fsync. Isolating disk I/O in a worker is deferred.
 *
 * There is no HTTP parser here: nodes speak only the binary protocol.
 */

#include "blob.h"
#include "cluster.h"
#include "netmirror.h"
#include "network.h"
#include "util.h"

#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <unistd.h>

#define NODE_EPOLL_BATCH 64
#define NODE_EPOLL_TIMEOUT_MS 250
#define NODE_ACCEPT_BATCH 32
#define NODE_OUTPUT_HIGH_WATER ((size_t)NM_CHUNK_SIZE)
#define NODE_OUTPUT_LOW_WATER (NODE_OUTPUT_HIGH_WATER / 2u)
#define NODE_OUTPUT_HARD_LIMIT                                                 \
    (((size_t)NM_CHUNK_SIZE * 2u) + 65536u)
#define NODE_DEFAULT_IDLE_TIMEOUT_MS 60000u

typedef struct node_server node_server_t;
typedef struct node_connection node_connection_t;

typedef enum node_dispatch_result {
    NODE_DISPATCH_KEEP = 0,
    NODE_DISPATCH_CLOSE_AFTER_FLUSH = 1,
    NODE_DISPATCH_FATAL = 2
} node_dispatch_result_t;

struct node_connection {
    int fd;
    node_server_t *server;
    nm_frame_parser_t input;
    nm_outbuf_t output;
    uint64_t last_activity_ms;
    uint32_t interest;
    unsigned int pending_writes;
    bool read_paused;
    bool close_after_flush;
    bool peer_eof;
    bool dead;
    node_connection_t *next;
};

struct node_server {
    nm_store_t store;
    int listen_fd;
    int epoll_fd;
    size_t connection_count;
    size_t max_connections;
    uint32_t idle_timeout_ms;
    char node_id[64];
    nm_cluster_t *cluster;
    node_connection_t *connections;
};

typedef struct node_config {
    char node_id[64];
    char bind_address[NM_MAX_HOST];
    char data_dir[NM_PATH_MAX];
    nm_endpoint_t peers[NM_MAX_PEERS];
    size_t peer_count;
    uint16_t port;
    uint32_t max_connections;
    uint32_t idle_timeout_ms;
    uint32_t connect_timeout_ms;
    uint32_t rpc_timeout_ms;
    uint32_t heartbeat_interval_ms;
    uint32_t heartbeat_timeout_ms;
    uint32_t anti_entropy_interval_ms;
} node_config_t;

typedef struct node_write_op {
    node_connection_t *connection;
    uint8_t ack_type;
    uint8_t digest[NM_SHA256_LEN];
} node_write_op_t;

static volatile sig_atomic_t stop_requested = 0;

static void node_server_destroy(node_server_t *server);
static void node_connection_release(node_connection_t *connection);
static void node_connection_close(node_connection_t *connection);
static bool node_connection_process_frames(node_connection_t *connection);
static bool node_connection_finish_io(node_connection_t *connection);
static void node_accept_connections(node_server_t *server);
static void node_sweep_idle_connections(node_server_t *server);
static void node_write_done(void *opaque, nm_status_t status,
                            uint32_t acknowledgements, uint32_t replicas);

static void handle_shutdown_signal(int signal_number)
{
    (void)signal_number;
    stop_requested = 1;
}

static void log_status(const node_server_t *server, nm_log_level_t level,
                       const char *operation, nm_status_t status)
{
    nm_log(level, "%s: %s failed: %s", server->node_id, operation,
           nm_status_name(status));
}

/* ------------------------------------------------------------------------- */
/* Configuration                                                             */
/* ------------------------------------------------------------------------- */

static bool parse_u32(const char *text, uint32_t minimum, uint32_t maximum,
                      uint32_t *out)
{
    char *end = NULL;
    unsigned long value;

    if (text == NULL || text[0] == '\0' || out == NULL) {
        return false;
    }
    errno = 0;
    value = strtoul(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value < minimum ||
        value > maximum) {
        return false;
    }
    *out = (uint32_t)value;
    return true;
}

static bool read_env_u32(const char *name, uint32_t fallback,
                         uint32_t minimum, uint32_t maximum, uint32_t *out)
{
    const char *value = getenv(name);

    if (value == NULL || value[0] == '\0') {
        *out = fallback;
        return true;
    }
    if (!parse_u32(value, minimum, maximum, out)) {
        nm_log(NM_LOG_ERROR,
               "invalid %s='%s' (expected integer in [%u, %u])", name, value,
               (unsigned int)minimum, (unsigned int)maximum);
        return false;
    }
    return true;
}

static bool read_env_string(const char *name, const char *fallback,
                            char *out, size_t out_size)
{
    const char *value = getenv(name);
    size_t length;

    if (out == NULL || out_size == 0u) {
        return false;
    }
    if (value == NULL || value[0] == '\0') {
        value = fallback;
    }
    length = strlen(value);
    if (length >= out_size) {
        nm_log(NM_LOG_ERROR, "%s is too long (limit %zu bytes)", name,
               out_size - 1u);
        return false;
    }
    memcpy(out, value, length + 1u);
    return true;
}

static bool parse_peer_list(const char *text, nm_endpoint_t *peers,
                            size_t *peer_count)
{
    char buffer[2048];
    char *save = NULL;
    char *token;
    size_t count = 0u;
    size_t length;

    if (text == NULL || peers == NULL || peer_count == NULL) {
        return false;
    }
    memset(peers, 0, sizeof(nm_endpoint_t) * NM_MAX_PEERS);
    *peer_count = 0u;
    if (text[0] == '\0') {
        return true;
    }
    length = strlen(text);
    if (length >= sizeof(buffer)) {
        nm_log(NM_LOG_ERROR, "NM_PEERS is too long");
        return false;
    }
    memcpy(buffer, text, length + 1u);
    token = strtok_r(buffer, ",", &save);
    while (token != NULL) {
        while (*token == ' ' || *token == '\t') {
            ++token;
        }
        char *end = token + strlen(token);
        while (end > token && (end[-1] == ' ' || end[-1] == '\t')) {
            *--end = '\0';
        }
        if (*token == '\0') {
            nm_log(NM_LOG_ERROR, "NM_PEERS contains an empty endpoint");
            return false;
        }
        if (count >= NM_MAX_PEERS) {
            nm_log(NM_LOG_ERROR, "NM_PEERS has more than %u entries",
                   (unsigned int)NM_MAX_PEERS);
            return false;
        }
        if (nm_endpoint_parse(token, &peers[count]) != NM_OK) {
            nm_log(NM_LOG_ERROR, "invalid peer endpoint '%s'", token);
            return false;
        }
        count += 1u;
        token = strtok_r(NULL, ",", &save);
    }
    *peer_count = count;
    return true;
}

static void node_usage(FILE *stream)
{
    (void)fprintf(
        stream,
        "Usage: netmirror-node [options]\n"
        "  --bind <addr>                    bind address (default 0.0.0.0)\n"
        "  --port <port>                    node port (default 7001)\n"
        "  --node-id <id>                   node identifier (default node)\n"
        "  --data-dir <dir>                 persistent data directory\n"
        "  --peers <host:port,...>          replication peers (excludes self)\n"
        "  --max-connections <n>            HTTP/protocol connection cap\n"
        "  --idle-timeout-ms <n>            idle connection timeout\n"
        "  --connect-timeout-ms <n>         peer connect timeout\n"
        "  --rpc-timeout-ms <n>             peer RPC timeout\n"
        "  --heartbeat-interval-ms <n>      PING interval\n"
        "  --heartbeat-timeout-ms <n>       PONG deadline\n"
        "  --anti-entropy-interval-ms <n>   repair interval\n"
        "  --log-level <error|warn|info|debug>\n"
        "  --help                           show this help\n");
}

static bool option_string(char *out, size_t out_size, const char *value)
{
    size_t length;

    if (value == NULL || out == NULL || out_size == 0u) {
        return false;
    }
    length = strlen(value);
    if (length >= out_size) {
        return false;
    }
    memcpy(out, value, length + 1u);
    return true;
}

static bool apply_node_arguments(node_config_t *config, int argc, char **argv)
{
    for (int i = 1; i < argc; ++i) {
        const char *option = argv[i];
        const char *value;
        uint32_t parsed;

        if (strcmp(option, "--help") == 0 || strcmp(option, "-h") == 0) {
            node_usage(stdout);
            exit(EXIT_SUCCESS);
        }
        if (i + 1 >= argc) {
            nm_log(NM_LOG_ERROR, "missing value for %s", option);
            return false;
        }
        value = argv[++i];
        if (strcmp(option, "--bind") == 0) {
            if (!option_string(config->bind_address,
                               sizeof(config->bind_address), value)) {
                return false;
            }
        } else if (strcmp(option, "--node-id") == 0) {
            if (!option_string(config->node_id, sizeof(config->node_id),
                               value)) {
                return false;
            }
        } else if (strcmp(option, "--data-dir") == 0) {
            if (!option_string(config->data_dir, sizeof(config->data_dir),
                               value)) {
                return false;
            }
        } else if (strcmp(option, "--peers") == 0) {
            if (!parse_peer_list(value, config->peers, &config->peer_count)) {
                return false;
            }
        } else if (strcmp(option, "--port") == 0) {
            if (!parse_u32(value, 1u, 65535u, &parsed)) {
                return false;
            }
            config->port = (uint16_t)parsed;
        } else if (strcmp(option, "--max-connections") == 0) {
            if (!parse_u32(value, 1u, 4096u, &config->max_connections)) {
                return false;
            }
        } else if (strcmp(option, "--idle-timeout-ms") == 0) {
            if (!parse_u32(value, 1000u, 86400000u,
                           &config->idle_timeout_ms)) {
                return false;
            }
        } else if (strcmp(option, "--connect-timeout-ms") == 0) {
            if (!parse_u32(value, 100u, 60000u,
                           &config->connect_timeout_ms)) {
                return false;
            }
        } else if (strcmp(option, "--rpc-timeout-ms") == 0) {
            if (!parse_u32(value, 500u, 300000u, &config->rpc_timeout_ms)) {
                return false;
            }
        } else if (strcmp(option, "--heartbeat-interval-ms") == 0) {
            if (!parse_u32(value, 100u, 60000u,
                           &config->heartbeat_interval_ms)) {
                return false;
            }
        } else if (strcmp(option, "--heartbeat-timeout-ms") == 0) {
            if (!parse_u32(value, 300u, 300000u,
                           &config->heartbeat_timeout_ms)) {
                return false;
            }
        } else if (strcmp(option, "--anti-entropy-interval-ms") == 0) {
            if (!parse_u32(value, 500u, 3600000u,
                           &config->anti_entropy_interval_ms)) {
                return false;
            }
        } else if (strcmp(option, "--log-level") == 0) {
            nm_log_set_level(nm_log_level_from_string(value, NM_LOG_INFO));
        } else {
            nm_log(NM_LOG_ERROR, "unknown option '%s'", option);
            return false;
        }
    }
    if (config->heartbeat_timeout_ms <= config->heartbeat_interval_ms) {
        nm_log(NM_LOG_ERROR,
               "--heartbeat-timeout-ms must exceed "
               "--heartbeat-interval-ms");
        return false;
    }
    return true;
}

static bool load_config(node_config_t *config)
{
    uint32_t port = NM_DEFAULT_NODE_PORT;
    uint32_t max_connections = NM_SERVER_MAX_CONNECTIONS;
    uint32_t idle_timeout = NODE_DEFAULT_IDLE_TIMEOUT_MS;
    uint32_t connect_timeout = NM_CLUSTER_DEFAULT_CONNECT_TIMEOUT_MS;
    uint32_t rpc_timeout = NM_CLUSTER_DEFAULT_RPC_TIMEOUT_MS;
    uint32_t heartbeat_interval = NM_CLUSTER_DEFAULT_HEARTBEAT_INTERVAL_MS;
    uint32_t heartbeat_timeout = NM_CLUSTER_DEFAULT_HEARTBEAT_TIMEOUT_MS;
    uint32_t anti_entropy_interval =
        NM_CLUSTER_DEFAULT_ANTI_ENTROPY_INTERVAL_MS;
    char peers_text[2048];
    const char *level_text;
    nm_log_level_t level = NM_LOG_INFO;

    memset(config, 0, sizeof(*config));
    level_text = getenv("NM_LOG_LEVEL");
    if (level_text != NULL && level_text[0] != '\0') {
        level = nm_log_level_from_string(level_text, NM_LOG_INFO);
    }
    nm_log_set_level(level);

    if (!read_env_string("NM_NODE_ID", "node", config->node_id,
                         sizeof(config->node_id)) ||
        !read_env_string("NM_BIND_ADDR", "0.0.0.0", config->bind_address,
                         sizeof(config->bind_address)) ||
        !read_env_string("NM_DATA_DIR", "./data", config->data_dir,
                         sizeof(config->data_dir)) ||
        !read_env_string("NM_PEERS", "", peers_text, sizeof(peers_text)) ||
        !read_env_u32("NM_PORT", NM_DEFAULT_NODE_PORT, 1u, 65535u, &port) ||
        !read_env_u32("NM_MAX_CONNECTIONS", NM_SERVER_MAX_CONNECTIONS, 1u,
                      4096u, &max_connections) ||
        !read_env_u32("NM_IDLE_TIMEOUT_MS", NODE_DEFAULT_IDLE_TIMEOUT_MS,
                      1000u, 86400000u, &idle_timeout) ||
        !read_env_u32("NM_CONNECT_TIMEOUT_MS",
                      NM_CLUSTER_DEFAULT_CONNECT_TIMEOUT_MS, 100u, 60000u,
                      &connect_timeout) ||
        !read_env_u32("NM_RPC_TIMEOUT_MS", NM_CLUSTER_DEFAULT_RPC_TIMEOUT_MS,
                      500u, 300000u, &rpc_timeout) ||
        !read_env_u32("NM_HEARTBEAT_INTERVAL_MS",
                      NM_CLUSTER_DEFAULT_HEARTBEAT_INTERVAL_MS, 100u, 60000u,
                      &heartbeat_interval) ||
        !read_env_u32("NM_HEARTBEAT_TIMEOUT_MS",
                      NM_CLUSTER_DEFAULT_HEARTBEAT_TIMEOUT_MS, 300u, 300000u,
                      &heartbeat_timeout) ||
        !read_env_u32("NM_ANTI_ENTROPY_INTERVAL_MS",
                      NM_CLUSTER_DEFAULT_ANTI_ENTROPY_INTERVAL_MS, 500u,
                      3600000u, &anti_entropy_interval)) {
        return false;
    }
    if (heartbeat_timeout <= heartbeat_interval) {
        nm_log(NM_LOG_ERROR,
               "NM_HEARTBEAT_TIMEOUT_MS must exceed "
               "NM_HEARTBEAT_INTERVAL_MS");
        return false;
    }
    if (!parse_peer_list(peers_text, config->peers, &config->peer_count)) {
        return false;
    }
    config->port = (uint16_t)port;
    config->max_connections = max_connections;
    config->idle_timeout_ms = idle_timeout;
    config->connect_timeout_ms = connect_timeout;
    config->rpc_timeout_ms = rpc_timeout;
    config->heartbeat_interval_ms = heartbeat_interval;
    config->heartbeat_timeout_ms = heartbeat_timeout;
    config->anti_entropy_interval_ms = anti_entropy_interval;
    return true;
}

/* ------------------------------------------------------------------------- */
/* Response construction                                                     */
/* ------------------------------------------------------------------------- */

static bool node_queue_frame(node_connection_t *connection, uint8_t type,
                             const void *payload, uint64_t payload_length)
{
    size_t pending;
    size_t total_length;
    int status;

    if (connection == NULL || connection->dead ||
        connection->close_after_flush) {
        return false;
    }
    if (payload_length > NM_MAX_FRAME_PAYLOAD) {
        return false;
    }
    total_length = NM_FRAME_HEADER_SIZE + (size_t)payload_length;
    pending = nm_outbuf_pending(&connection->output);
    if (pending > NODE_OUTPUT_HARD_LIMIT ||
        total_length > NODE_OUTPUT_HARD_LIMIT - pending) {
        nm_log(NM_LOG_WARN, "%s: output backlog limit reached",
               connection->server->node_id);
        return false;
    }
    status = nm_outbuf_append_frame(&connection->output, type, payload,
                                    payload_length);
    if (status != NM_OK) {
        log_status(connection->server, NM_LOG_ERROR, "queue response", status);
        return false;
    }
    return true;
}

static bool node_send_error(node_connection_t *connection, nm_status_t status,
                            const char *message)
{
    uint8_t *payload = NULL;
    size_t payload_length = 0u;
    bool queued;

    if (nm_wire_error_encode(status, message, &payload, &payload_length) !=
        NM_OK) {
        return false;
    }
    queued = node_queue_frame(connection, NM_MSG_ERROR, payload,
                              (uint64_t)payload_length);
    free(payload);
    return queued;
}

static bool node_send_ack(node_connection_t *connection, nm_status_t status,
                          uint8_t acknowledged_type,
                          const uint8_t digest[NM_SHA256_LEN])
{
    uint8_t payload[NM_ACK_WIRE_SIZE];

    if (nm_wire_ack_encode(status, acknowledged_type, digest, payload) !=
        NM_OK) {
        return false;
    }
    return node_queue_frame(connection, NM_MSG_ACK, payload,
                            NM_ACK_WIRE_SIZE);
}

static node_dispatch_result_t node_fail(node_connection_t *connection,
                                        nm_status_t status,
                                        const char *message)
{
    if (!node_send_error(connection, status, message)) {
        return NODE_DISPATCH_FATAL;
    }
    return NODE_DISPATCH_KEEP;
}

static node_dispatch_result_t node_fail_and_close(node_connection_t *connection,
                                                  nm_status_t status,
                                                  const char *message)
{
    if (!node_send_error(connection, status, message)) {
        return NODE_DISPATCH_FATAL;
    }
    return NODE_DISPATCH_CLOSE_AFTER_FLUSH;
}

/* ------------------------------------------------------------------------- */
/* Asynchronous replication                                                  */
/* ------------------------------------------------------------------------- */

static node_dispatch_result_t node_replicate_chunk_async(
    node_server_t *server, node_connection_t *connection,
    const uint8_t digest[NM_SHA256_LEN], const uint8_t *data, size_t length)
{
    node_write_op_t *operation = calloc(1u, sizeof(*operation));
    int result;

    if (operation == NULL) {
        if (!node_send_ack(connection, NM_ERR_NOMEM, NM_MSG_STORE, digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    operation->connection = connection;
    operation->ack_type = (uint8_t)NM_MSG_STORE;
    memcpy(operation->digest, digest, NM_SHA256_LEN);
    connection->pending_writes += 1u;
    result = nm_cluster_replicate_chunk(server->cluster, digest, data, length,
                                        node_write_done, operation);
    if (result != NM_OK) {
        connection->pending_writes -= 1u;
        free(operation);
        if (!node_send_ack(connection, (nm_status_t)result, NM_MSG_STORE,
                           digest)) {
            return NODE_DISPATCH_FATAL;
        }
    }
    return NODE_DISPATCH_KEEP;
}

static node_dispatch_result_t node_replicate_manifest_async(
    node_server_t *server, node_connection_t *connection,
    const nm_manifest_t *manifest,
    const uint8_t key_digest[NM_SHA256_LEN])
{
    node_write_op_t *operation = calloc(1u, sizeof(*operation));
    int result;

    if (operation == NULL) {
        if (!node_send_ack(connection, NM_ERR_NOMEM, NM_MSG_STORE_OBJECT,
                           key_digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    operation->connection = connection;
    operation->ack_type = (uint8_t)NM_MSG_STORE_OBJECT;
    memcpy(operation->digest, key_digest, NM_SHA256_LEN);
    connection->pending_writes += 1u;
    result = nm_cluster_replicate_manifest(server->cluster, manifest,
                                           node_write_done, operation);
    if (result != NM_OK) {
        connection->pending_writes -= 1u;
        free(operation);
        if (!node_send_ack(connection, (nm_status_t)result,
                           NM_MSG_STORE_OBJECT, key_digest)) {
            return NODE_DISPATCH_FATAL;
        }
    }
    return NODE_DISPATCH_KEEP;
}

/* ------------------------------------------------------------------------- */
/* Request handlers                                                          */
/* ------------------------------------------------------------------------- */

static node_dispatch_result_t handle_ping(node_connection_t *connection,
                                          const nm_frame_t *frame)
{
    if (frame->length != 8u || frame->payload == NULL) {
        return node_fail(connection, NM_ERR_PROTOCOL,
                         "PING payload must be 8 bytes");
    }
    if (!node_queue_frame(connection, NM_MSG_PONG, frame->payload,
                          frame->length)) {
        return NODE_DISPATCH_FATAL;
    }
    return NODE_DISPATCH_KEEP;
}

static bool valid_store_payload(const nm_frame_t *frame)
{
    return frame->payload != NULL && frame->length > NM_SHA256_LEN &&
           frame->length <= (uint64_t)NM_SHA256_LEN + NM_CHUNK_SIZE;
}

static node_dispatch_result_t handle_store(node_server_t *server,
                                           node_connection_t *connection,
                                           const nm_frame_t *frame,
                                           bool replica)
{
    uint8_t digest[NM_SHA256_LEN];
    const uint8_t *chunk_data;
    size_t chunk_length;
    nm_status_t status;

    if (!valid_store_payload(frame)) {
        return node_fail(connection, NM_ERR_PROTOCOL,
                         "invalid STORE payload length");
    }
    memcpy(digest, frame->payload, NM_SHA256_LEN);
    chunk_data = frame->payload + NM_SHA256_LEN;
    chunk_length = (size_t)(frame->length - NM_SHA256_LEN);
    status = nm_store_put_chunk(&server->store, digest, chunk_data,
                                chunk_length);
    if (status != NM_OK) {
        nm_log(NM_LOG_DEBUG, "%s: rejected STORE: %s", server->node_id,
               nm_status_name(status));
    }
    if (replica) {
        uint8_t ack_type = (uint8_t)NM_MSG_REPLICA_STORE;
        if (!node_send_ack(connection, status, ack_type, digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    if (status != NM_OK) {
        if (!node_send_ack(connection, status, NM_MSG_STORE, digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    if (server->cluster == NULL) {
        if (!node_send_ack(connection, NM_OK, NM_MSG_STORE, digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    return node_replicate_chunk_async(server, connection, digest, chunk_data,
                                      chunk_length);
}

static node_dispatch_result_t handle_fetch(node_server_t *server,
                                           node_connection_t *connection,
                                           const nm_frame_t *frame)
{
    uint8_t digest[NM_SHA256_LEN];
    uint8_t *chunk = NULL;
    uint8_t *payload = NULL;
    size_t chunk_length = 0u;
    nm_status_t status;

    if (frame->length != NM_SHA256_LEN || frame->payload == NULL) {
        return node_fail(connection, NM_ERR_PROTOCOL,
                         "FETCH payload must be a 32-byte digest");
    }
    memcpy(digest, frame->payload, NM_SHA256_LEN);
    status = nm_store_get_chunk(&server->store, digest, &chunk, &chunk_length);
    if (status != NM_OK) {
        node_dispatch_result_t result =
            node_fail(connection, status, nm_status_name(status));
        free(chunk);
        return result;
    }
    if (chunk_length > SIZE_MAX - NM_SHA256_LEN) {
        free(chunk);
        return node_fail(connection, NM_ERR_TOO_LARGE, "chunk too large");
    }
    payload = malloc(NM_SHA256_LEN + chunk_length);
    if (payload == NULL) {
        free(chunk);
        return node_fail(connection, NM_ERR_NOMEM, "out of memory");
    }
    memcpy(payload, digest, NM_SHA256_LEN);
    memcpy(payload + NM_SHA256_LEN, chunk, chunk_length);
    if (!node_queue_frame(connection, NM_MSG_CHUNK, payload,
                          (uint64_t)(NM_SHA256_LEN + chunk_length))) {
        free(payload);
        free(chunk);
        return NODE_DISPATCH_FATAL;
    }
    free(payload);
    free(chunk);
    return NODE_DISPATCH_KEEP;
}

static node_dispatch_result_t handle_store_object(node_server_t *server,
                                                  node_connection_t *connection,
                                                  const nm_frame_t *frame,
                                                  bool replica)
{
    nm_manifest_t manifest;
    uint8_t key_digest[NM_SHA256_LEN];
    nm_status_t status;
    uint8_t ack_type = replica ? (uint8_t)NM_MSG_REPLICA_STORE_OBJECT
                               : (uint8_t)NM_MSG_STORE_OBJECT;
    int result;

    memset(&manifest, 0, sizeof(manifest));
    if (frame->length == 0u || frame->payload == NULL) {
        return node_fail(connection, NM_ERR_PROTOCOL,
                         "STORE_OBJECT payload is empty");
    }
    result = nm_manifest_deserialize(frame->payload, (size_t)frame->length,
                                     &manifest);
    if (result != NM_OK) {
        return node_fail(connection, (nm_status_t)result,
                         "invalid object manifest");
    }
    result = nm_manifest_key_hash(manifest.key, key_digest);
    if (result != NM_OK) {
        nm_manifest_free(&manifest);
        return node_fail(connection, (nm_status_t)result,
                         "invalid object key");
    }
    status = nm_store_put_manifest(&server->store, &manifest);
    if (status != NM_OK || replica || server->cluster == NULL) {
        nm_manifest_free(&manifest);
        if (!node_send_ack(connection, status, ack_type, key_digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    node_dispatch_result_t dispatch_result = node_replicate_manifest_async(
        server, connection, &manifest, key_digest);
    nm_manifest_free(&manifest);
    return dispatch_result;
}

static node_dispatch_result_t handle_fetch_object(
    node_server_t *server, node_connection_t *connection,
    const nm_frame_t *frame)
{
    char key[NM_MAX_KEY];
    nm_manifest_t manifest;
    uint8_t *serialized = NULL;
    size_t serialized_length = 0u;
    nm_status_t status;
    int result;

    memset(&manifest, 0, sizeof(manifest));
    if (frame->payload == NULL || frame->length == 0u ||
        frame->length > NM_MAX_KEY_LEN) {
        return node_fail(connection, NM_ERR_PROTOCOL, "invalid object key");
    }
    if (memchr(frame->payload, '\0', (size_t)frame->length) != NULL) {
        return node_fail(connection, NM_ERR_PROTOCOL,
                         "object key contains NUL");
    }
    memcpy(key, frame->payload, (size_t)frame->length);
    key[(size_t)frame->length] = '\0';
    if (!nm_key_is_valid(key)) {
        return node_fail(connection, NM_ERR_PROTOCOL, "invalid object key");
    }

    status = nm_store_get_manifest(&server->store, key, &manifest);
    if (status != NM_OK) {
        return node_fail(connection, status, nm_status_name(status));
    }
    result = nm_manifest_serialize(&manifest, &serialized,
                                   &serialized_length);
    nm_manifest_free(&manifest);
    if (result != NM_OK) {
        return node_fail(connection, (nm_status_t)result,
                         "could not serialize manifest");
    }
    if (!node_queue_frame(connection, NM_MSG_OBJECT, serialized,
                          (uint64_t)serialized_length)) {
        free(serialized);
        return NODE_DISPATCH_FATAL;
    }
    free(serialized);
    return NODE_DISPATCH_KEEP;
}

static node_dispatch_result_t handle_sync(node_server_t *server,
                                          node_connection_t *connection,
                                          const nm_frame_t *frame)
{
    uint8_t *hashes = NULL;
    size_t count = 0u;
    uint8_t *response = NULL;
    size_t response_length;
    uint32_t offset;
    uint32_t requested;
    uint32_t total;
    uint32_t returned;
    size_t available;
    nm_status_t status;
    bool queued;

    if (frame->length != NM_SYNC_REQ_WIRE_SIZE || frame->payload == NULL) {
        return node_fail(connection, NM_ERR_PROTOCOL, "invalid SYNC_REQ");
    }
    offset = nm_get_u32_be(frame->payload);
    requested = nm_get_u32_be(frame->payload + 4u);
    if (requested == 0u) {
        return node_fail(connection, NM_ERR_PROTOCOL,
                         "SYNC_REQ limit must be non-zero");
    }
    if (requested > NM_SYNC_DEFAULT_BATCH) {
        requested = NM_SYNC_DEFAULT_BATCH;
    }
    status = nm_store_list_chunks(&server->store, &hashes, &count);
    if (status != NM_OK) {
        return node_fail(connection, status, "could not list chunks");
    }
    if (count > UINT32_MAX) {
        free(hashes);
        return node_fail(connection, NM_ERR_TOO_LARGE,
                         "chunk index exceeds protocol limits");
    }
    total = (uint32_t)count;
    available = offset < count ? count - (size_t)offset : 0u;
    if (available > requested) {
        available = requested;
    }
    returned = (uint32_t)available;
    response_length = NM_SYNC_RESP_HEADER_SIZE +
                      ((size_t)returned * NM_SHA256_LEN);
    response = malloc(response_length);
    if (response == NULL) {
        free(hashes);
        return node_fail(connection, NM_ERR_NOMEM, "out of memory");
    }
    nm_put_u32_be(response, total);
    nm_put_u32_be(response + 4u, returned);
    if (returned > 0u) {
        memcpy(response + NM_SYNC_RESP_HEADER_SIZE,
               hashes + ((size_t)offset * NM_SHA256_LEN),
               (size_t)returned * NM_SHA256_LEN);
    }
    free(hashes);
    queued = node_queue_frame(connection, NM_MSG_SYNC_RESP, response,
                              (uint64_t)response_length);
    free(response);
    return queued ? NODE_DISPATCH_KEEP : NODE_DISPATCH_FATAL;
}

static node_dispatch_result_t handle_list_keys_common(
    node_server_t *server, node_connection_t *connection,
    const nm_frame_t *frame, bool include_tombstones, uint8_t response_type)
{
    char **keys = NULL;
    size_t count = 0u;
    uint8_t *response = NULL;
    size_t response_length = NM_LIST_KEYS_RESP_HEADER_SIZE;
    uint32_t offset;
    uint32_t requested;
    uint32_t total;
    uint32_t returned;
    size_t available;
    nm_status_t status;
    bool queued;

    if (frame->length != NM_LIST_KEYS_REQ_WIRE_SIZE ||
        frame->payload == NULL) {
        return node_fail(connection, NM_ERR_PROTOCOL,
                         "invalid list request");
    }
    offset = nm_get_u32_be(frame->payload);
    requested = nm_get_u32_be(frame->payload + 4u);
    if (requested == 0u) {
        return node_fail(connection, NM_ERR_PROTOCOL,
                         "list limit must be non-zero");
    }
    if (requested > NM_LIST_KEYS_DEFAULT_BATCH) {
        requested = NM_LIST_KEYS_DEFAULT_BATCH;
    }
    status = include_tombstones
                 ? nm_store_list_keys(&server->store, &keys, &count)
                 : nm_store_list_live_keys(&server->store, &keys, &count);
    if (status != NM_OK) {
        return node_fail(connection, status, "could not list manifests");
    }
    if (count > UINT32_MAX) {
        nm_store_free_keys(keys, count);
        return node_fail(connection, NM_ERR_TOO_LARGE,
                         "manifest index exceeds protocol limits");
    }
    total = (uint32_t)count;
    available = offset < count ? count - (size_t)offset : 0u;
    if (available > requested) {
        available = requested;
    }
    returned = (uint32_t)available;
    for (size_t i = 0u; i < (size_t)returned; ++i) {
        response_length += 2u + strlen(keys[(size_t)offset + i]);
    }
    if (response_length > NM_MAX_FRAME_PAYLOAD) {
        nm_store_free_keys(keys, count);
        return node_fail(connection, NM_ERR_TOO_LARGE,
                         "manifest key page too large");
    }
    response = malloc(response_length);
    if (response == NULL) {
        nm_store_free_keys(keys, count);
        return node_fail(connection, NM_ERR_NOMEM, "out of memory");
    }
    nm_put_u32_be(response, total);
    nm_put_u32_be(response + 4u, returned);
    size_t cursor = NM_LIST_KEYS_RESP_HEADER_SIZE;
    for (size_t i = 0u; i < (size_t)returned; ++i) {
        size_t key_length = strlen(keys[(size_t)offset + i]);
        nm_put_u16_be(response + cursor, (uint16_t)key_length);
        cursor += 2u;
        memcpy(response + cursor, keys[(size_t)offset + i], key_length);
        cursor += key_length;
    }
    nm_store_free_keys(keys, count);
    queued = node_queue_frame(connection, response_type, response,
                              (uint64_t)response_length);
    free(response);
    return queued ? NODE_DISPATCH_KEEP : NODE_DISPATCH_FATAL;
}

static node_dispatch_result_t handle_list_keys(
    node_server_t *server, node_connection_t *connection,
    const nm_frame_t *frame)
{
    return handle_list_keys_common(server, connection, frame, true,
                                   NM_MSG_LIST_KEYS_RESP);
}

static node_dispatch_result_t handle_list_objects(
    node_server_t *server, node_connection_t *connection,
    const nm_frame_t *frame)
{
    return handle_list_keys_common(server, connection, frame, false,
                                   NM_MSG_LIST_OBJECTS_RESP);
}

static bool parse_delete_key(const nm_frame_t *frame, bool replica,
                             char key[NM_MAX_KEY], uint64_t *deleted_ms)
{
    const uint8_t *key_data;
    size_t key_length;
    size_t prefix = replica ? 8u : 0u;

    if (frame->payload == NULL || frame->length <= prefix) {
        return false;
    }
    if (replica) {
        if (deleted_ms == NULL) {
            return false;
        }
        *deleted_ms = nm_get_u64_be(frame->payload);
        if (*deleted_ms == 0u) {
            return false;
        }
    }
    key_length = (size_t)(frame->length - prefix);
    if (key_length == 0u || key_length >= NM_MAX_KEY) {
        return false;
    }
    key_data = frame->payload + prefix;
    if (memchr(key_data, '\0', key_length) != NULL) {
        return false;
    }
    memcpy(key, key_data, key_length);
    key[key_length] = '\0';
    return nm_key_is_valid(key);
}

static node_dispatch_result_t node_replicate_delete_async(
    node_server_t *server, node_connection_t *connection, const char *key,
    uint64_t deleted_ms, const uint8_t key_digest[NM_SHA256_LEN])
{
    node_write_op_t *operation = calloc(1u, sizeof(*operation));
    int result;

    if (operation == NULL) {
        if (!node_send_ack(connection, NM_ERR_NOMEM, NM_MSG_DELETE_OBJECT,
                           key_digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    operation->connection = connection;
    operation->ack_type = (uint8_t)NM_MSG_DELETE_OBJECT;
    memcpy(operation->digest, key_digest, NM_SHA256_LEN);
    connection->pending_writes += 1u;
    result = nm_cluster_replicate_delete(server->cluster, key, deleted_ms,
                                         node_write_done, operation);
    if (result != NM_OK) {
        connection->pending_writes -= 1u;
        free(operation);
        if (!node_send_ack(connection, (nm_status_t)result,
                           NM_MSG_DELETE_OBJECT, key_digest)) {
            return NODE_DISPATCH_FATAL;
        }
    }
    return NODE_DISPATCH_KEEP;
}

static node_dispatch_result_t handle_delete_object(
    node_server_t *server, node_connection_t *connection,
    const nm_frame_t *frame, bool replica)
{
    char key[NM_MAX_KEY];
    uint8_t key_digest[NM_SHA256_LEN];
    uint64_t deleted_ms = 0u;
    nm_status_t status;
    int result;

    if (!parse_delete_key(frame, replica, key, &deleted_ms)) {
        return node_fail(connection, NM_ERR_PROTOCOL, "invalid DELETE payload");
    }
    result = nm_manifest_key_hash(key, key_digest);
    if (result != NM_OK) {
        return node_fail(connection, (nm_status_t)result,
                         "invalid object key");
    }
    if (replica) {
        status = (nm_status_t)nm_store_apply_tombstone(&server->store, key,
                                                       deleted_ms);
        if (!node_send_ack(connection, status, NM_MSG_REPLICA_DELETE_OBJECT,
                           key_digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    status = (nm_status_t)nm_store_delete_manifest(&server->store, key,
                                                   &deleted_ms);
    if (status != NM_OK) {
        if (!node_send_ack(connection, status, NM_MSG_DELETE_OBJECT,
                           key_digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    if (server->cluster == NULL) {
        if (!node_send_ack(connection, NM_OK, NM_MSG_DELETE_OBJECT,
                           key_digest)) {
            return NODE_DISPATCH_FATAL;
        }
        return NODE_DISPATCH_KEEP;
    }
    return node_replicate_delete_async(server, connection, key, deleted_ms,
                                       key_digest);
}

static node_dispatch_result_t handle_stats(node_server_t *server,
                                           node_connection_t *connection,
                                           const nm_frame_t *frame)
{
    struct statvfs vfs;
    const char *root;
    uint8_t *hashes = NULL;
    char **keys = NULL;
    size_t chunk_count = 0u;
    size_t object_count = 0u;
    uint64_t disk_total = 0u;
    uint64_t disk_free = 0u;
    uint8_t payload[NM_STATS_RESP_WIRE_SIZE];
    nm_status_t status;
    bool queued;

    if (frame->length != 0u) {
        return node_fail(connection, NM_ERR_PROTOCOL,
                         "STATS_REQ payload must be empty");
    }

    root = nm_store_root(&server->store);
    if (root != NULL && statvfs(root, &vfs) == 0) {
        disk_total = (uint64_t)vfs.f_blocks * (uint64_t)vfs.f_frsize;
        disk_free = (uint64_t)vfs.f_bavail * (uint64_t)vfs.f_frsize;
    } else {
        nm_log(NM_LOG_WARN, "%s: statvfs failed for %s: %s", server->node_id,
               root != NULL ? root : "(null)", strerror(errno));
    }

    status = (nm_status_t)nm_store_list_chunks(&server->store, &hashes,
                                               &chunk_count);
    if (status != NM_OK) {
        nm_log(NM_LOG_WARN, "%s: stats chunk count failed: %s",
               server->node_id, nm_status_name(status));
        chunk_count = 0u;
    }
    free(hashes);

    status = (nm_status_t)nm_store_list_live_keys(&server->store, &keys,
                                                  &object_count);
    if (status != NM_OK) {
        nm_log(NM_LOG_WARN, "%s: stats object count failed: %s",
               server->node_id, nm_status_name(status));
        object_count = 0u;
    }
    nm_store_free_keys(keys, object_count);

    nm_put_u64_be(payload + NM_STATS_DISK_TOTAL_OFFSET, disk_total);
    nm_put_u64_be(payload + NM_STATS_DISK_FREE_OFFSET, disk_free);
    nm_put_u64_be(payload + NM_STATS_CHUNK_COUNT_OFFSET, (uint64_t)chunk_count);
    nm_put_u64_be(payload + NM_STATS_OBJECT_COUNT_OFFSET,
                  (uint64_t)object_count);
    payload[NM_STATS_STATUS_OFFSET] = (uint8_t)NM_STATS_STATUS_ONLINE;

    queued = node_queue_frame(connection, NM_MSG_STATS_RESP, payload,
                              NM_STATS_RESP_WIRE_SIZE);
    return queued ? NODE_DISPATCH_KEEP : NODE_DISPATCH_FATAL;
}

static node_dispatch_result_t dispatch_frame(node_server_t *server,
                                             node_connection_t *connection,
                                             const nm_frame_t *frame)
{
    switch (frame->type) {
    case NM_MSG_PING:
        return handle_ping(connection, frame);
    case NM_MSG_PONG:
        if (frame->length != 8u) {
            return node_fail(connection, NM_ERR_PROTOCOL,
                             "invalid PONG payload");
        }
        return NODE_DISPATCH_KEEP;
    case NM_MSG_STORE:
        return handle_store(server, connection, frame, false);
    case NM_MSG_REPLICA_STORE:
        return handle_store(server, connection, frame, true);
    case NM_MSG_FETCH:
        return handle_fetch(server, connection, frame);
    case NM_MSG_STORE_OBJECT:
        return handle_store_object(server, connection, frame, false);
    case NM_MSG_REPLICA_STORE_OBJECT:
        return handle_store_object(server, connection, frame, true);
    case NM_MSG_FETCH_OBJECT:
        return handle_fetch_object(server, connection, frame);
    case NM_MSG_DELETE_OBJECT:
        return handle_delete_object(server, connection, frame, false);
    case NM_MSG_REPLICA_DELETE_OBJECT:
        return handle_delete_object(server, connection, frame, true);
    case NM_MSG_SYNC_REQ:
        return handle_sync(server, connection, frame);
    case NM_MSG_LIST_KEYS_REQ:
        return handle_list_keys(server, connection, frame);
    case NM_MSG_LIST_OBJECTS_REQ:
        return handle_list_objects(server, connection, frame);
    case NM_MSG_STATS_REQ:
        return handle_stats(server, connection, frame);
    case NM_MSG_ACK:
    case NM_MSG_ERROR:
    case NM_MSG_CHUNK:
    case NM_MSG_OBJECT:
    case NM_MSG_SYNC_RESP:
    case NM_MSG_LIST_KEYS_RESP:
    case NM_MSG_LIST_OBJECTS_RESP:
    case NM_MSG_STATS_RESP:
        return node_fail_and_close(connection, NM_ERR_PROTOCOL,
                                   "unsupported request type");
    default:
        return node_fail_and_close(connection, NM_ERR_PROTOCOL,
                                   "unknown message type");
    }
}

/* ------------------------------------------------------------------------- */
/* Connection lifecycle and async completion                                 */
/* ------------------------------------------------------------------------- */

static void node_connection_release(node_connection_t *connection)
{
    node_server_t *server;

    if (connection == NULL) {
        return;
    }
    server = connection->server;
    if (server != NULL) {
        node_connection_t **cursor = &server->connections;
        while (*cursor != NULL) {
            if (*cursor == connection) {
                *cursor = connection->next;
                break;
            }
            cursor = &(*cursor)->next;
        }
        if (server->connection_count > 0u) {
            server->connection_count -= 1u;
        }
        nm_log(NM_LOG_DEBUG, "%s: connection released (active=%zu)",
               server->node_id, server->connection_count);
    }
    if (connection->fd >= 0) {
        if (server != NULL && server->epoll_fd >= 0) {
            (void)epoll_ctl(server->epoll_fd, EPOLL_CTL_DEL, connection->fd,
                            NULL);
        }
        nm_close_fd(&connection->fd);
    }
    nm_frame_parser_reset(&connection->input);
    nm_outbuf_free(&connection->output);
    free(connection);
}

static void node_connection_close(node_connection_t *connection)
{
    if (connection == NULL || connection->dead) {
        return;
    }
    connection->dead = true;
    if (connection->fd >= 0) {
        if (connection->server->epoll_fd >= 0) {
            (void)epoll_ctl(connection->server->epoll_fd, EPOLL_CTL_DEL,
                            connection->fd, NULL);
        }
        nm_close_fd(&connection->fd);
    }
    nm_frame_parser_reset(&connection->input);
    nm_outbuf_free(&connection->output);
    nm_log(NM_LOG_DEBUG, "%s: closing client connection (pending_writes=%u)",
           connection->server->node_id, connection->pending_writes);
    if (connection->pending_writes == 0u) {
        node_connection_release(connection);
    }
}

static void node_write_done(void *opaque, nm_status_t status,
                            uint32_t acknowledgements, uint32_t replicas)
{
    node_write_op_t *operation = opaque;
    node_connection_t *connection;
    uint8_t ack_type;
    uint8_t digest[NM_SHA256_LEN];

    if (operation == NULL) {
        return;
    }
    connection = operation->connection;
    ack_type = operation->ack_type;
    memcpy(digest, operation->digest, NM_SHA256_LEN);
    free(operation);
    if (connection->pending_writes > 0u) {
        connection->pending_writes -= 1u;
    }
    nm_log(NM_LOG_DEBUG, "%s: quorum write finished: %s (%u/%u acks)",
           connection->server->node_id, nm_status_name(status),
           (unsigned int)acknowledgements, (unsigned int)replicas);
    if (connection->dead) {
        if (connection->pending_writes == 0u) {
            node_connection_release(connection);
        }
        return;
    }
    if (!node_send_ack(connection, status, ack_type, digest)) {
        node_connection_close(connection);
    }
}

/* ------------------------------------------------------------------------- */
/* Client connection state machine                                           */
/* ------------------------------------------------------------------------- */

static uint32_t node_connection_desired_events(
    const node_connection_t *connection)
{
    uint32_t events = 0u;

    if (!connection->dead && !connection->peer_eof && !connection->read_paused &&
        !connection->close_after_flush) {
        events |= EPOLLIN;
    }
    if (nm_outbuf_pending(&connection->output) > 0u) {
        events |= EPOLLOUT;
    }
    return events;
}

static int node_connection_update_interest(node_connection_t *connection)
{
    struct epoll_event event;
    uint32_t desired = node_connection_desired_events(connection);
    int result;

    if (connection->dead || desired == 0u) {
        return NM_ERR_INVALID;
    }
    if (desired == connection->interest) {
        return NM_OK;
    }
    memset(&event, 0, sizeof(event));
    event.events = desired;
    event.data.fd = connection->fd;
    result = epoll_ctl(connection->server->epoll_fd, EPOLL_CTL_MOD,
                       connection->fd, &event);
    if (result != 0) {
        log_status(connection->server, NM_LOG_ERROR, "epoll_ctl(MOD)",
                   NM_ERR_IO);
        return NM_ERR_IO;
    }
    connection->interest = desired;
    return NM_OK;
}

static bool node_connection_process_frames(node_connection_t *connection)
{
    for (;;) {
        nm_frame_t frame;
        bool complete = false;
        int parse_status;
        node_dispatch_result_t dispatch_status;

        if (connection->dead || connection->close_after_flush ||
            connection->read_paused) {
            break;
        }
        parse_status =
            nm_frame_parser_next(&connection->input, &frame, &complete);
        if (parse_status != NM_OK) {
            nm_status_t error_status =
                parse_status == NM_ERR_NOMEM ? NM_ERR_NOMEM : NM_ERR_PROTOCOL;
            (void)node_send_error(connection, error_status,
                                  error_status == NM_ERR_NOMEM
                                      ? "out of memory parsing frame"
                                      : "invalid frame length");
            connection->close_after_flush = true;
            break;
        }
        if (!complete) {
            break;
        }
        dispatch_status = dispatch_frame(connection->server, connection,
                                         &frame);
        nm_frame_free(&frame);
        if (dispatch_status == NODE_DISPATCH_FATAL) {
            return false;
        }
        if (dispatch_status == NODE_DISPATCH_CLOSE_AFTER_FLUSH) {
            connection->close_after_flush = true;
            break;
        }
        if (nm_outbuf_pending(&connection->output) >=
            NODE_OUTPUT_HIGH_WATER) {
            connection->read_paused = true;
            break;
        }
    }

    if (connection->peer_eof && !connection->read_paused &&
        !connection->close_after_flush) {
        if (connection->input.length > 0u) {
            if (!node_send_error(connection, NM_ERR_PROTOCOL,
                                 "truncated frame at EOF")) {
                return false;
            }
            connection->close_after_flush = true;
        } else {
            connection->close_after_flush = true;
        }
    }
    return true;
}

static bool node_connection_read_once(node_connection_t *connection)
{
    uint8_t buffer[NM_NET_READ_CHUNK];
    ssize_t received;
    int result;

    received = recv(connection->fd, buffer, sizeof(buffer), 0);
    if (received < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return true;
        }
        nm_log(NM_LOG_DEBUG, "%s: recv failed: %s",
               connection->server->node_id, strerror(errno));
        return false;
    }
    if (received == 0) {
        connection->peer_eof = true;
        return node_connection_process_frames(connection);
    }
    connection->last_activity_ms = nm_now_ms();
    result = nm_frame_parser_feed(&connection->input, buffer,
                                  (size_t)received);
    if (result != NM_OK) {
        if (!node_send_error(connection,
                             result == NM_ERR_TOO_LARGE ? NM_ERR_TOO_LARGE
                                                        : NM_ERR_NOMEM,
                             "input frame buffer limit reached")) {
            return false;
        }
        connection->close_after_flush = true;
        return true;
    }
    return node_connection_process_frames(connection);
}

static bool node_connection_finish_io(node_connection_t *connection)
{
    size_t pending;

    if (connection->dead) {
        return false;
    }
    if (connection->read_paused && !connection->close_after_flush &&
        nm_outbuf_pending(&connection->output) <= NODE_OUTPUT_LOW_WATER) {
        connection->read_paused = false;
        if (!node_connection_process_frames(connection)) {
            return false;
        }
    }
    pending = nm_outbuf_pending(&connection->output);
    if (connection->close_after_flush && pending == 0u) {
        return false;
    }
    if (connection->peer_eof && pending == 0u) {
        return false;
    }
    if (node_connection_desired_events(connection) == 0u) {
        return false;
    }
    return node_connection_update_interest(connection) == NM_OK;
}

static bool node_connection_handle_event(node_connection_t *connection,
                                         uint32_t events)
{
    if (connection->dead) {
        return false;
    }
    if ((events & EPOLLERR) != 0u) {
        return false;
    }
    if ((events & EPOLLIN) != 0u) {
        if (!node_connection_read_once(connection)) {
            return false;
        }
    }
    if ((events & EPOLLOUT) != 0u) {
        size_t before = nm_outbuf_pending(&connection->output);
        nm_flush_result_t flush_result =
            nm_outbuf_flush(connection->fd, &connection->output);
        if (flush_result == NM_FLUSH_ERROR) {
            return false;
        }
        if (nm_outbuf_pending(&connection->output) < before) {
            connection->last_activity_ms = nm_now_ms();
        }
    }
    if ((events & EPOLLHUP) != 0u) {
        return false;
    }
    return node_connection_finish_io(connection);
}

/* ------------------------------------------------------------------------- */
/* Accepting, sweeping and service                                           */
/* ------------------------------------------------------------------------- */

static void node_accept_connections(node_server_t *server)
{
    for (unsigned int accepted = 0u; accepted < NODE_ACCEPT_BATCH;
         ++accepted) {
        int fd = nm_tcp_accept(server->listen_fd);
        node_connection_t *connection;
        struct epoll_event event;

        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                nm_log(NM_LOG_WARN, "%s: accept failed: %s", server->node_id,
                       strerror(errno));
            }
            break;
        }
        if (server->connection_count >= server->max_connections) {
            nm_log(NM_LOG_WARN, "%s: connection limit reached; rejecting",
                   server->node_id);
            nm_close_fd(&fd);
            continue;
        }
        connection = calloc(1u, sizeof(*connection));
        if (connection == NULL) {
            nm_log(NM_LOG_ERROR, "%s: out of memory accepting connection",
                   server->node_id);
            nm_close_fd(&fd);
            continue;
        }
        connection->fd = fd;
        connection->server = server;
        connection->last_activity_ms = nm_now_ms();
        connection->interest = EPOLLIN;
        nm_frame_parser_init(&connection->input);
        nm_outbuf_init(&connection->output);
        connection->next = server->connections;
        server->connections = connection;
        server->connection_count += 1u;

        memset(&event, 0, sizeof(event));
        event.events = EPOLLIN;
        event.data.fd = fd;
        if (epoll_ctl(server->epoll_fd, EPOLL_CTL_ADD, fd, &event) != 0) {
            nm_log(NM_LOG_ERROR, "%s: epoll_ctl(ADD) failed: %s",
                   server->node_id, strerror(errno));
            node_connection_close(connection);
            continue;
        }
        nm_log(NM_LOG_DEBUG, "%s: accepted connection (active=%zu)",
               server->node_id, server->connection_count);
    }
}

static void node_sweep_idle_connections(node_server_t *server)
{
    node_connection_t *connection = server->connections;
    uint64_t now = nm_now_ms();

    while (connection != NULL) {
        node_connection_t *next = connection->next;
        uint64_t idle_ms =
            now >= connection->last_activity_ms
                ? now - connection->last_activity_ms
                : 0u;
        if (!connection->dead && idle_ms > server->idle_timeout_ms) {
            nm_log(NM_LOG_DEBUG, "%s: closing idle connection after %llu ms",
                   server->node_id, (unsigned long long)idle_ms);
            node_connection_close(connection);
        }
        connection = next;
    }
}

static node_connection_t *node_find_connection(node_server_t *server, int fd)
{
    for (node_connection_t *connection = server->connections;
         connection != NULL; connection = connection->next) {
        if (connection->fd == fd) {
            return connection;
        }
    }
    return NULL;
}

static void node_connections_service(node_server_t *server)
{
    node_connection_t *connection = server->connections;

    while (connection != NULL) {
        node_connection_t *next = connection->next;
        if (!connection->dead) {
            if (!node_connection_finish_io(connection)) {
                node_connection_close(connection);
            }
        }
        connection = next;
    }
}

/* ------------------------------------------------------------------------- */
/* Server lifecycle                                                          */
/* ------------------------------------------------------------------------- */

static int node_server_init(node_server_t *server, const node_config_t *config)
{
    struct epoll_event listener_event;
    int result;

    memset(server, 0, sizeof(*server));
    server->listen_fd = -1;
    server->epoll_fd = -1;
    server->max_connections = config->max_connections;
    server->idle_timeout_ms = config->idle_timeout_ms;
    memcpy(server->node_id, config->node_id, sizeof(server->node_id));
    server->node_id[sizeof(server->node_id) - 1u] = '\0';

    result = nm_store_init(&server->store, config->data_dir);
    if (result != NM_OK) {
        log_status(server, NM_LOG_ERROR, "initialize store",
                   (nm_status_t)result);
        node_server_destroy(server);
        return EXIT_FAILURE;
    }
    server->listen_fd = nm_tcp_listen(config->bind_address, config->port,
                                      NM_TCP_BACKLOG);
    if (server->listen_fd < 0) {
        nm_log(NM_LOG_ERROR, "%s: cannot listen on %s:%u: %s",
               server->node_id, config->bind_address,
               (unsigned int)config->port, strerror(errno));
        node_server_destroy(server);
        return EXIT_FAILURE;
    }
    server->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (server->epoll_fd < 0) {
        nm_log(NM_LOG_ERROR, "%s: epoll_create1 failed: %s", server->node_id,
               strerror(errno));
        node_server_destroy(server);
        return EXIT_FAILURE;
    }
    memset(&listener_event, 0, sizeof(listener_event));
    listener_event.events = EPOLLIN;
    listener_event.data.fd = server->listen_fd;
    if (epoll_ctl(server->epoll_fd, EPOLL_CTL_ADD, server->listen_fd,
                  &listener_event) != 0) {
        nm_log(NM_LOG_ERROR, "%s: epoll_ctl(listener) failed: %s",
               server->node_id, strerror(errno));
        node_server_destroy(server);
        return EXIT_FAILURE;
    }
    if (config->peer_count > 0u) {
        nm_cluster_options_t options;
        memset(&options, 0, sizeof(options));
        options.epoll_fd = server->epoll_fd;
        options.store = &server->store;
        options.node_id = server->node_id;
        options.peers = config->peers;
        options.peer_count = config->peer_count;
        options.connect_timeout_ms = config->connect_timeout_ms;
        options.rpc_timeout_ms = config->rpc_timeout_ms;
        options.heartbeat_interval_ms = config->heartbeat_interval_ms;
        options.heartbeat_timeout_ms = config->heartbeat_timeout_ms;
        options.anti_entropy_interval_ms = config->anti_entropy_interval_ms;
        options.max_peer_queue_bytes =
            NM_CLUSTER_DEFAULT_MAX_PEER_QUEUE_BYTES;
        options.max_peer_requests = NM_CLUSTER_DEFAULT_MAX_PEER_REQUESTS;
        options.sync_batch = NM_CLUSTER_DEFAULT_SYNC_BATCH;
        result = nm_cluster_init(&server->cluster, &options);
        if (result != NM_OK) {
            log_status(server, NM_LOG_ERROR, "initialize cluster",
                       (nm_status_t)result);
            node_server_destroy(server);
            return EXIT_FAILURE;
        }
    }
    return EXIT_SUCCESS;
}

static void node_server_destroy(node_server_t *server)
{
    node_connection_t *connection = server->connections;

    while (connection != NULL) {
        node_connection_t *next = connection->next;
        node_connection_close(connection);
        connection = next;
    }
    if (server->cluster != NULL) {
        nm_cluster_destroy(server->cluster);
        server->cluster = NULL;
    }
    while (server->connections != NULL) {
        node_connection_release(server->connections);
    }
    nm_close_fd(&server->listen_fd);
    nm_close_fd(&server->epoll_fd);
}

static int node_server_run(node_server_t *server)
{
    struct epoll_event events[NODE_EPOLL_BATCH];
    int exit_code = EXIT_SUCCESS;

    while (!stop_requested) {
        int count;

        node_sweep_idle_connections(server);
        if (server->cluster != NULL) {
            nm_cluster_tick(server->cluster);
        }
        node_connections_service(server);
        count = epoll_wait(server->epoll_fd, events, NODE_EPOLL_BATCH,
                           NODE_EPOLL_TIMEOUT_MS);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            nm_log(NM_LOG_ERROR, "%s: epoll_wait failed: %s", server->node_id,
                   strerror(errno));
            exit_code = EXIT_FAILURE;
            break;
        }
        for (int i = 0; i < count; ++i) {
            int fd = events[i].data.fd;
            if (fd == server->listen_fd) {
                if ((events[i].events & (EPOLLERR | EPOLLHUP)) != 0u) {
                    nm_log(NM_LOG_ERROR, "%s: listener error", server->node_id);
                    exit_code = EXIT_FAILURE;
                    stop_requested = 1;
                    break;
                }
                node_accept_connections(server);
                continue;
            }
            if (server->cluster != NULL &&
                nm_cluster_owns_fd(server->cluster, fd)) {
                nm_cluster_handle_event(server->cluster, fd,
                                        events[i].events);
                continue;
            }
            node_connection_t *connection =
                node_find_connection(server, fd);
            if (connection != NULL && !connection->dead &&
                !node_connection_handle_event(connection,
                                              events[i].events)) {
                node_connection_close(connection);
            }
        }
    }
    return exit_code;
}

static int install_signal_handlers(void)
{
    struct sigaction action;
    struct sigaction ignore;

    memset(&action, 0, sizeof(action));
    action.sa_handler = handle_shutdown_signal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    if (sigaction(SIGINT, &action, NULL) != 0 ||
        sigaction(SIGTERM, &action, NULL) != 0) {
        return -1;
    }
    memset(&ignore, 0, sizeof(ignore));
    ignore.sa_handler = SIG_IGN;
    sigemptyset(&ignore.sa_mask);
    if (sigaction(SIGPIPE, &ignore, NULL) != 0) {
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    node_config_t config;
    node_server_t server;

    if (!load_config(&config) ||
        !apply_node_arguments(&config, argc, argv)) {
        node_usage(stderr);
        return EXIT_FAILURE;
    }
    if (install_signal_handlers() != 0) {
        nm_log(NM_LOG_ERROR, "cannot install signal handlers: %s",
               strerror(errno));
        return EXIT_FAILURE;
    }
    if (node_server_init(&server, &config) != EXIT_SUCCESS) {
        return EXIT_FAILURE;
    }
    nm_log(NM_LOG_INFO,
           "%s: listening on %s:%u (data=%s, max_connections=%u, "
           "idle_timeout=%ums, peers=%zu, quorum=%u)",
           config.node_id, config.bind_address, (unsigned int)config.port,
           config.data_dir, (unsigned int)config.max_connections,
           (unsigned int)config.idle_timeout_ms, config.peer_count,
           (unsigned int)nm_quorum_size((uint32_t)config.peer_count + 1u));

    int result = node_server_run(&server);
    nm_log(NM_LOG_INFO, "%s: shutting down", server.node_id);
    node_server_destroy(&server);
    return result;
}
