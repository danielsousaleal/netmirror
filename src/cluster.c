/*
 * netmirror - peer replication, heartbeat and anti-entropy
 *
 * This module is driven exclusively by the node's single-threaded epoll loop.
 * It never sleeps, never performs a blocking connect, and never waits for an
 * ACK: peer sockets and transaction state machines advance only from
 * nm_cluster_handle_event()/nm_cluster_tick().
 */

#include "cluster.h"

#include "network.h"
#include "util.h"

#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#define NM_CLUSTER_RECONNECT_MIN_MS 250u
#define NM_CLUSTER_RECONNECT_MAX_MS 5000u
#define NM_CLUSTER_SYNC_RETRY_MS 1000u
#define NM_CLUSTER_READ_CHUNK NM_NET_READ_CHUNK
#define NM_CLUSTER_MAX_SYNC_BATCH 128u

typedef struct nm_peer nm_peer_t;
typedef struct nm_peer_rpc nm_peer_rpc_t;
typedef struct nm_cluster_txn nm_cluster_txn_t;

typedef enum nm_rpc_kind {
    NM_RPC_STORE = 0,
    NM_RPC_STORE_OBJECT,
    NM_RPC_DELETE_OBJECT,
    NM_RPC_SYNC_INDEX,
    NM_RPC_SYNC_FETCH,
    NM_RPC_SYNC_LIST_KEYS,
    NM_RPC_SYNC_MANIFEST
} nm_rpc_kind_t;

typedef enum nm_sync_phase {
    NM_SYNC_IDLE = 0,
    NM_SYNC_WAIT_CHUNK_INDEX,
    NM_SYNC_WAIT_CHUNK,
    NM_SYNC_WAIT_KEY_INDEX,
    NM_SYNC_WAIT_MANIFEST
} nm_sync_phase_t;

typedef struct nm_peer_sync {
    bool active;
    uint64_t next_run_ms;
    nm_sync_phase_t phase;
    uint32_t chunk_offset;
    uint32_t chunk_total;
    uint32_t chunk_page_returned;
    uint8_t missing[NM_CLUSTER_MAX_SYNC_BATCH][NM_SHA256_LEN];
    uint32_t missing_count;
    uint32_t missing_pos;
    uint32_t key_offset;
    uint32_t key_total;
    uint32_t key_page_returned;
    char keys[NM_CLUSTER_MAX_SYNC_BATCH][NM_MAX_KEY];
    uint32_t key_count;
    uint32_t key_pos;
} nm_peer_sync_t;

struct nm_peer_rpc {
    nm_rpc_kind_t kind;
    uint8_t expected_type;
    uint8_t ack_type;
    uint8_t digest[NM_SHA256_LEN];
    nm_cluster_txn_t *txn;
    uint64_t deadline_ms;
    nm_peer_rpc_t *next;
};

struct nm_peer {
    nm_cluster_t *cluster;
    nm_endpoint_t endpoint;
    struct sockaddr_storage address;
    socklen_t address_length;
    int fd;
    bool connecting;
    bool connected;
    bool up;
    uint32_t interest;
    uint64_t next_connect_ms;
    uint64_t connect_deadline_ms;
    uint32_t reconnect_delay_ms;
    uint64_t next_ping_ms;
    uint64_t ping_deadline_ms;
    uint64_t ping_nonce;
    bool ping_outstanding;
    nm_frame_parser_t input;
    nm_outbuf_t output;
    nm_peer_rpc_t *rpc_head;
    nm_peer_rpc_t *rpc_tail;
    size_t rpc_count;
    nm_peer_sync_t sync;
};

struct nm_cluster_txn {
    nm_cluster_t *cluster;
    nm_cluster_write_done_fn callback;
    void *opaque;
    uint32_t replicas;
    uint32_t quorum;
    uint32_t acknowledgements;
    uint32_t attempts;
    bool ready;
    bool notified;
    nm_status_t status;
    nm_cluster_txn_t *next;
};

struct nm_cluster {
    int epoll_fd;
    nm_store_t *store;
    char node_id[64];
    nm_peer_t peers[NM_MAX_PEERS];
    size_t peer_count;
    size_t up_peers;
    uint32_t connect_timeout_ms;
    uint32_t rpc_timeout_ms;
    uint32_t heartbeat_interval_ms;
    uint32_t heartbeat_timeout_ms;
    uint32_t anti_entropy_interval_ms;
    size_t max_peer_queue_bytes;
    size_t max_peer_requests;
    uint32_t sync_batch;
    uint64_t nonce_counter;
    bool shutting_down;
    nm_cluster_txn_t *transactions;
};

/* ------------------------------------------------------------------------- */
/* Small helpers                                                             */
/* ------------------------------------------------------------------------- */

static uint64_t min_u64(uint64_t left, uint64_t right)
{
    return left < right ? left : right;
}

static const char *peer_name(const nm_peer_t *peer)
{
    static char name[NM_MAX_HOST + 16u];
    (void)snprintf(name, sizeof(name), "%s:%u", peer->endpoint.host,
                   (unsigned int)peer->endpoint.port);
    return name;
}

static void peer_log(const nm_peer_t *peer, nm_log_level_t level,
                     const char *format, ...)
{
    va_list arguments;
    char message[512];

    va_start(arguments, format);
    (void)vsnprintf(message, sizeof(message), format, arguments);
    va_end(arguments);
    nm_log(level, "%s: peer %s: %s", peer->cluster->node_id, peer_name(peer),
           message);
}

static nm_peer_t *cluster_find_fd(nm_cluster_t *cluster, int fd)
{
    if (cluster == NULL || fd < 0) {
        return NULL;
    }
    for (size_t i = 0u; i < cluster->peer_count; ++i) {
        if (cluster->peers[i].fd == fd) {
            return &cluster->peers[i];
        }
    }
    return NULL;
}

static int peer_update_interest(nm_peer_t *peer)
{
    struct epoll_event event;
    uint32_t desired = 0u;
    int result;

    if (peer == NULL || peer->fd < 0) {
        return NM_ERR_INVALID;
    }
    if (peer->connecting) {
        desired = EPOLLOUT;
    } else if (peer->connected) {
        desired = EPOLLIN;
        if (nm_outbuf_pending(&peer->output) > 0u) {
            desired |= EPOLLOUT;
        }
    }
    if (desired == 0u) {
        return NM_ERR_INVALID;
    }
    if (desired == peer->interest) {
        return NM_OK;
    }
    memset(&event, 0, sizeof(event));
    event.events = desired;
    event.data.fd = peer->fd;
    result = epoll_ctl(peer->cluster->epoll_fd, EPOLL_CTL_MOD, peer->fd,
                       &event);
    if (result != 0) {
        peer_log(peer, NM_LOG_ERROR, "epoll_ctl(MOD) failed: %s",
                 strerror(errno));
        return NM_ERR_IO;
    }
    peer->interest = desired;
    return NM_OK;
}

static bool peer_has_output_room(const nm_peer_t *peer, size_t frame_length)
{
    size_t pending;

    if (frame_length > NM_MAX_FRAME_PAYLOAD + NM_FRAME_HEADER_SIZE) {
        return false;
    }
    pending = nm_outbuf_pending(&peer->output);
    if (pending > peer->cluster->max_peer_queue_bytes) {
        return false;
    }
    return frame_length <= peer->cluster->max_peer_queue_bytes - pending;
}

static void peer_rpc_link(nm_peer_t *peer, nm_peer_rpc_t *rpc)
{
    rpc->next = NULL;
    if (peer->rpc_tail == NULL) {
        peer->rpc_head = rpc;
        peer->rpc_tail = rpc;
    } else {
        peer->rpc_tail->next = rpc;
        peer->rpc_tail = rpc;
    }
    peer->rpc_count += 1u;
}

static nm_peer_rpc_t *peer_rpc_unlink(nm_peer_t *peer)
{
    nm_peer_rpc_t *rpc = peer->rpc_head;

    if (rpc == NULL) {
        return NULL;
    }
    peer->rpc_head = rpc->next;
    if (peer->rpc_head == NULL) {
        peer->rpc_tail = NULL;
    }
    if (peer->rpc_count > 0u) {
        peer->rpc_count -= 1u;
    }
    rpc->next = NULL;
    return rpc;
}

/* ------------------------------------------------------------------------- */
/* Forward declarations                                                      */
/* ------------------------------------------------------------------------- */

static void peer_disconnect(nm_peer_t *peer, const char *reason);
static void transaction_evaluate(nm_cluster_txn_t *transaction);
static void transaction_peer_result(nm_cluster_txn_t *transaction,
                                    nm_status_t status);
static int peer_enqueue_rpc(nm_peer_t *peer, nm_rpc_kind_t kind,
                            uint8_t request_type, uint8_t expected_type,
                            uint8_t ack_type,
                            const uint8_t digest[NM_SHA256_LEN],
                            nm_cluster_txn_t *txn, const void *payload,
                            size_t payload_length)
{
    nm_peer_rpc_t *rpc;
    int result;

    if (peer == NULL || !peer->connected || !peer->up) {
        return NM_ERR_UNAVAILABLE;
    }
    if (payload_length > NM_MAX_FRAME_PAYLOAD ||
        (payload == NULL && payload_length != 0u)) {
        return NM_ERR_INVALID;
    }
    if (peer->rpc_count >= peer->cluster->max_peer_requests) {
        return NM_ERR_BUSY;
    }
    if (!peer_has_output_room(peer,
                              NM_FRAME_HEADER_SIZE + payload_length)) {
        return NM_ERR_BUSY;
    }
    rpc = calloc(1u, sizeof(*rpc));
    if (rpc == NULL) {
        return NM_ERR_NOMEM;
    }
    rpc->kind = kind;
    rpc->expected_type = expected_type;
    rpc->ack_type = ack_type;
    rpc->txn = txn;
    rpc->deadline_ms = nm_now_ms() + peer->cluster->rpc_timeout_ms;
    if (digest != NULL) {
        memcpy(rpc->digest, digest, NM_SHA256_LEN);
    }
    result = nm_outbuf_append_frame(&peer->output, request_type, payload,
                                    (uint64_t)payload_length);
    if (result != NM_OK) {
        free(rpc);
        return result;
    }
    /*
     * Subscribe to EPOLLOUT before linking the request metadata. If the epoll
     * update fails, disconnect() fails only pre-existing RPCs and resets the
     * output buffer; this request has not been counted yet and is freed here.
     */
    if (peer_update_interest(peer) != NM_OK) {
        peer_disconnect(peer, "could not arm EPOLLOUT");
        free(rpc);
        return NM_ERR_IO;
    }
    peer_rpc_link(peer, rpc);
    return NM_OK;
}

/* ------------------------------------------------------------------------- */
/* Heartbeats and connection lifecycle                                       */
/* ------------------------------------------------------------------------- */

static bool peer_send_ping(nm_peer_t *peer)
{
    uint8_t payload[8];
    uint64_t now = nm_now_ms();
    uint64_t nonce = ++peer->cluster->nonce_counter ^ now;
    size_t frame_length = NM_FRAME_HEADER_SIZE + sizeof(payload);

    if (!peer->connected || peer->ping_outstanding) {
        return false;
    }
    if (!peer_has_output_room(peer, frame_length)) {
        peer_log(peer, NM_LOG_WARN, "heartbeat queue limit reached");
        return false;
    }
    nm_put_u64_be(payload, nonce);
    if (nm_outbuf_append_frame(&peer->output, NM_MSG_PING, payload,
                               sizeof(payload)) != NM_OK) {
        return false;
    }
    peer->ping_nonce = nonce;
    peer->ping_outstanding = true;
    peer->ping_deadline_ms = now + peer->cluster->heartbeat_timeout_ms;
    peer->next_ping_ms = now + peer->cluster->heartbeat_interval_ms;
    if (peer_update_interest(peer) != NM_OK) {
        return false;
    }
    return true;
}

static void peer_schedule_reconnect(nm_peer_t *peer, uint64_t now)
{
    uint32_t delay = peer->reconnect_delay_ms;

    if (delay < NM_CLUSTER_RECONNECT_MIN_MS) {
        delay = NM_CLUSTER_RECONNECT_MIN_MS;
    }
    peer->next_connect_ms = now + delay;
    if (peer->reconnect_delay_ms <= NM_CLUSTER_RECONNECT_MAX_MS / 2u) {
        peer->reconnect_delay_ms = peer->reconnect_delay_ms * 2u;
    }
    if (peer->reconnect_delay_ms < NM_CLUSTER_RECONNECT_MIN_MS) {
        peer->reconnect_delay_ms = NM_CLUSTER_RECONNECT_MIN_MS;
    }
    if (peer->reconnect_delay_ms > NM_CLUSTER_RECONNECT_MAX_MS) {
        peer->reconnect_delay_ms = NM_CLUSTER_RECONNECT_MAX_MS;
    }
}

static void peer_sync_abort(nm_peer_t *peer)
{
    if (peer == NULL) {
        return;
    }
    peer->sync.active = false;
    peer->sync.phase = NM_SYNC_IDLE;
    peer->sync.chunk_offset = 0u;
    peer->sync.chunk_total = 0u;
    peer->sync.chunk_page_returned = 0u;
    peer->sync.missing_count = 0u;
    peer->sync.missing_pos = 0u;
    peer->sync.key_offset = 0u;
    peer->sync.key_total = 0u;
    peer->sync.key_page_returned = 0u;
    peer->sync.key_count = 0u;
    peer->sync.key_pos = 0u;
}

static void peer_fail_queued_rpcs(nm_peer_t *peer)
{
    for (;;) {
        nm_peer_rpc_t *rpc = peer_rpc_unlink(peer);
        if (rpc == NULL) {
            break;
        }
        if (rpc->txn != NULL) {
            transaction_peer_result(rpc->txn, NM_ERR_UNAVAILABLE);
        }
        free(rpc);
    }
}

static void peer_disconnect(nm_peer_t *peer, const char *reason)
{
    uint64_t now;
    bool was_up;
    bool was_connected;

    if (peer == NULL) {
        return;
    }
    if (peer->fd < 0) {
        return;
    }
    now = nm_now_ms();
    was_up = peer->up;
    was_connected = peer->connected;
    peer_log(peer, was_connected ? NM_LOG_WARN : NM_LOG_DEBUG,
             "disconnected: %s", reason != NULL ? reason : "connection lost");
    if (peer->cluster->epoll_fd >= 0) {
        (void)epoll_ctl(peer->cluster->epoll_fd, EPOLL_CTL_DEL, peer->fd,
                        NULL);
    }
    nm_close_fd(&peer->fd);
    peer->connecting = false;
    peer->connected = false;
    peer->up = false;
    peer->interest = 0u;
    peer->ping_outstanding = false;
    peer_fail_queued_rpcs(peer);
    nm_frame_parser_reset(&peer->input);
    nm_outbuf_reset(&peer->output);
    if (was_up && peer->cluster->up_peers > 0u) {
        peer->cluster->up_peers -= 1u;
    }
    peer_sync_abort(peer);
    if (!peer->cluster->shutting_down) {
        peer_schedule_reconnect(peer, now);
    }
}

static int peer_register_socket(nm_peer_t *peer, uint32_t events)
{
    struct epoll_event event;

    memset(&event, 0, sizeof(event));
    event.events = events;
    event.data.fd = peer->fd;
    if (epoll_ctl(peer->cluster->epoll_fd, EPOLL_CTL_ADD, peer->fd,
                  &event) != 0) {
        return NM_ERR_IO;
    }
    peer->interest = events;
    return NM_OK;
}

static bool peer_connected(nm_peer_t *peer)
{
    uint64_t now = nm_now_ms();

    peer->connecting = false;
    peer->connected = true;
    peer->up = false;
    peer->connect_deadline_ms = 0u;
    peer->ping_outstanding = false;
    nm_frame_parser_reset(&peer->input);
    nm_outbuf_reset(&peer->output);
    peer->next_ping_ms = now;
    peer_log(peer, NM_LOG_INFO, "outbound connection established");
    if (!peer_send_ping(peer)) {
        peer_disconnect(peer, "could not send initial heartbeat");
        return false;
    }
    if (peer_update_interest(peer) != NM_OK) {
        peer_disconnect(peer, "could not arm epoll interest");
        return false;
    }
    return true;
}

static void peer_start_connect(nm_peer_t *peer)
{
    uint64_t now = nm_now_ms();
    int fd;
    int connect_result;
    int enabled = 1;

    if (peer == NULL || peer->cluster == NULL || peer->fd >= 0 ||
        peer->cluster->shutting_down) {
        return;
    }
    if (peer->address_length == 0u) {
        peer_log(peer, NM_LOG_ERROR, "peer address is unresolved");
        peer_schedule_reconnect(peer, now);
        return;
    }
    fd = socket(peer->address.ss_family, SOCK_STREAM, 0);
    if (fd < 0) {
        peer_log(peer, NM_LOG_WARN, "socket failed: %s", strerror(errno));
        peer_schedule_reconnect(peer, now);
        return;
    }
    if (nm_fd_set_cloexec(fd) != NM_OK ||
        nm_fd_set_nonblocking(fd, true) != NM_OK) {
        nm_close_fd(&fd);
        peer_schedule_reconnect(peer, now);
        return;
    }
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) !=
        0) {
        nm_close_fd(&fd);
        peer_schedule_reconnect(peer, now);
        return;
    }
    peer->fd = fd;
    peer->connecting = true;
    peer->connected = false;
    peer->up = false;
    peer->connect_deadline_ms = now + peer->cluster->connect_timeout_ms;
    peer_log(peer, NM_LOG_DEBUG, "connecting");
    connect_result = connect(fd, (const struct sockaddr *)&peer->address,
                             peer->address_length);
    if (peer_register_socket(peer, EPOLLOUT) != NM_OK) {
        peer_log(peer, NM_LOG_ERROR, "epoll_ctl(ADD) failed: %s",
                 strerror(errno));
        peer_disconnect(peer, "epoll_ctl(ADD) failed");
        return;
    }
    if (connect_result == 0) {
        (void)peer_connected(peer);
    } else if (errno != EINPROGRESS) {
        peer_disconnect(peer, strerror(errno));
    }
}

static bool peer_read_input(nm_peer_t *peer);

static bool peer_handle_event(nm_peer_t *peer, uint32_t events)
{
    if (peer->fd < 0) {
        return false;
    }
    if (peer->connecting) {
        int socket_error = 0;
        socklen_t length = sizeof(socket_error);
        if ((events & (EPOLLERR | EPOLLHUP)) != 0u) {
            peer_disconnect(peer, "connect failed");
            return false;
        }
        if ((events & EPOLLOUT) == 0u) {
            return true;
        }
        if (getsockopt(peer->fd, SOL_SOCKET, SO_ERROR, &socket_error,
                       &length) != 0 ||
            socket_error != 0) {
            peer_disconnect(peer,
                            socket_error != 0 ? strerror(socket_error)
                                              : "getsockopt(SO_ERROR) failed");
            return false;
        }
        return peer_connected(peer);
    }
    if ((events & EPOLLERR) != 0u) {
        peer_disconnect(peer, "socket error");
        return false;
    }
    if ((events & EPOLLIN) != 0u) {
        if (!peer_read_input(peer)) {
            return false;
        }
    }
    if ((events & EPOLLOUT) != 0u) {
        nm_flush_result_t result =
            nm_outbuf_flush(peer->fd, &peer->output);
        if (result == NM_FLUSH_ERROR) {
            peer_disconnect(peer, "send failed");
            return false;
        }
    }
    if ((events & EPOLLHUP) != 0u) {
        peer_disconnect(peer, "hangup");
        return false;
    }
    if (peer->fd >= 0 && peer_update_interest(peer) != NM_OK) {
        peer_disconnect(peer, "epoll update failed");
        return false;
    }
    return peer->fd >= 0;
}

/* ------------------------------------------------------------------------- */
/* Response handling and anti-entropy                                        */
/* ------------------------------------------------------------------------- */

static void peer_sync_fail(nm_peer_t *peer, const char *reason)
{
    if (peer == NULL) {
        return;
    }
    peer_log(peer, NM_LOG_DEBUG, "anti-entropy aborted: %s", reason);
    peer_sync_abort(peer);
    if (!peer->cluster->shutting_down) {
        peer->sync.next_run_ms =
            nm_now_ms() + min_u64(NM_CLUSTER_SYNC_RETRY_MS,
                                  peer->cluster->anti_entropy_interval_ms);
    }
}

static void peer_sync_complete(nm_peer_t *peer)
{
    peer_log(peer, NM_LOG_DEBUG, "anti-entropy pass complete");
    peer_sync_abort(peer);
    peer->sync.next_run_ms =
        nm_now_ms() + peer->cluster->anti_entropy_interval_ms;
}

static bool peer_sync_request_chunk_page(nm_peer_t *peer)
{
    uint8_t payload[NM_SYNC_REQ_WIRE_SIZE];
    int result;

    if (!peer->sync.active || !peer->up) {
        return false;
    }
    nm_put_u32_be(payload, peer->sync.chunk_offset);
    nm_put_u32_be(payload + 4u, peer->cluster->sync_batch);
    peer->sync.phase = NM_SYNC_WAIT_CHUNK_INDEX;
    result = peer_enqueue_rpc(peer, NM_RPC_SYNC_INDEX, NM_MSG_SYNC_REQ,
                              NM_MSG_SYNC_RESP, 0u, NULL, NULL, payload,
                              sizeof(payload));
    return result == NM_OK;
}

static bool peer_sync_request_chunk_data(nm_peer_t *peer)
{
    int result;

    if (!peer->sync.active || !peer->up ||
        peer->sync.missing_pos >= peer->sync.missing_count) {
        return false;
    }
    const uint8_t *hash = peer->sync.missing[peer->sync.missing_pos];
    peer->sync.phase = NM_SYNC_WAIT_CHUNK;
    result = peer_enqueue_rpc(peer, NM_RPC_SYNC_FETCH, NM_MSG_FETCH,
                              NM_MSG_CHUNK, 0u, hash, NULL, hash,
                              NM_SHA256_LEN);
    return result == NM_OK;
}

static bool peer_sync_request_key_page(nm_peer_t *peer)
{
    uint8_t payload[NM_LIST_KEYS_REQ_WIRE_SIZE];
    int result;

    if (!peer->sync.active || !peer->up) {
        return false;
    }
    nm_put_u32_be(payload, peer->sync.key_offset);
    nm_put_u32_be(payload + 4u, peer->cluster->sync_batch);
    peer->sync.phase = NM_SYNC_WAIT_KEY_INDEX;
    result = peer_enqueue_rpc(peer, NM_RPC_SYNC_LIST_KEYS,
                              NM_MSG_LIST_KEYS_REQ, NM_MSG_LIST_KEYS_RESP, 0u,
                              NULL, NULL, payload, sizeof(payload));
    return result == NM_OK;
}

static bool peer_sync_request_manifest(nm_peer_t *peer)
{
    int result;

    if (!peer->sync.active || !peer->up ||
        peer->sync.key_pos >= peer->sync.key_count) {
        return false;
    }
    const char *key = peer->sync.keys[peer->sync.key_pos];
    peer->sync.phase = NM_SYNC_WAIT_MANIFEST;
    result = peer_enqueue_rpc(peer, NM_RPC_SYNC_MANIFEST,
                              NM_MSG_FETCH_OBJECT, NM_MSG_OBJECT, 0u, NULL,
                              NULL, key, strlen(key));
    return result == NM_OK;
}

static void peer_sync_start(nm_peer_t *peer)
{
    if (!peer->up || peer->sync.active) {
        return;
    }
    peer->sync.active = true;
    peer->sync.phase = NM_SYNC_WAIT_CHUNK_INDEX;
    peer->sync.chunk_offset = 0u;
    peer->sync.chunk_total = 0u;
    peer->sync.chunk_page_returned = 0u;
    peer->sync.missing_count = 0u;
    peer->sync.missing_pos = 0u;
    peer->sync.key_offset = 0u;
    peer->sync.key_total = 0u;
    peer->sync.key_page_returned = 0u;
    peer->sync.key_count = 0u;
    peer->sync.key_pos = 0u;
    peer_log(peer, NM_LOG_DEBUG, "starting anti-entropy pass");
    if (!peer_sync_request_chunk_page(peer)) {
        peer_sync_fail(peer, "could not queue chunk index request");
    }
}

static void peer_sync_after_chunk_page(nm_peer_t *peer)
{
    peer->sync.chunk_offset += peer->sync.chunk_page_returned;
    if (peer->sync.chunk_page_returned == 0u ||
        peer->sync.chunk_offset >= peer->sync.chunk_total) {
        peer->sync.key_offset = 0u;
        if (!peer_sync_request_key_page(peer)) {
            peer_sync_fail(peer, "could not queue manifest key request");
        }
    } else if (!peer_sync_request_chunk_page(peer)) {
        peer_sync_fail(peer, "could not queue next chunk index page");
    }
}

static void peer_sync_after_chunk(nm_peer_t *peer)
{
    peer->sync.missing_pos += 1u;
    if (peer->sync.missing_pos < peer->sync.missing_count) {
        if (!peer_sync_request_chunk_data(peer)) {
            peer_sync_fail(peer, "could not queue chunk fetch");
        }
    } else {
        peer_sync_after_chunk_page(peer);
    }
}

static void peer_sync_after_key_page(nm_peer_t *peer)
{
    peer->sync.key_offset += peer->sync.key_page_returned;
    if (peer->sync.key_page_returned == 0u ||
        peer->sync.key_offset >= peer->sync.key_total) {
        peer_sync_complete(peer);
    } else if (!peer_sync_request_key_page(peer)) {
        peer_sync_fail(peer, "could not queue next manifest key page");
    }
}

static void peer_sync_after_manifest(nm_peer_t *peer)
{
    peer->sync.key_pos += 1u;
    if (peer->sync.key_pos < peer->sync.key_count) {
        if (!peer_sync_request_manifest(peer)) {
            peer_sync_fail(peer, "could not queue manifest fetch");
        }
    } else {
        peer_sync_after_key_page(peer);
    }
}

static bool peer_sync_handle_chunk_index(nm_peer_t *peer,
                                         const nm_frame_t *frame)
{
    uint32_t total;
    uint32_t returned;

    if (!peer->sync.active ||
        peer->sync.phase != NM_SYNC_WAIT_CHUNK_INDEX ||
        frame->length < NM_SYNC_RESP_HEADER_SIZE || frame->payload == NULL) {
        return false;
    }
    total = nm_get_u32_be(frame->payload);
    returned = nm_get_u32_be(frame->payload + 4u);
    if (returned > peer->cluster->sync_batch ||
        frame->length != NM_SYNC_RESP_HEADER_SIZE +
                              ((uint64_t)returned * NM_SHA256_LEN)) {
        return false;
    }
    peer->sync.chunk_total = total;
    peer->sync.chunk_page_returned = returned;
    peer->sync.missing_count = 0u;
    peer->sync.missing_pos = 0u;
    for (uint32_t i = 0u; i < returned; ++i) {
        const uint8_t *hash =
            frame->payload + NM_SYNC_RESP_HEADER_SIZE +
            ((size_t)i * NM_SHA256_LEN);
        bool exists = false;
        if (nm_store_has_chunk(peer->cluster->store, hash, &exists) != NM_OK) {
            peer_sync_fail(peer, "local chunk check failed");
            return true;
        }
        if (!exists) {
            memcpy(peer->sync.missing[peer->sync.missing_count], hash,
                   NM_SHA256_LEN);
            peer->sync.missing_count += 1u;
        }
    }
    if (peer->sync.missing_count > 0u) {
        if (!peer_sync_request_chunk_data(peer)) {
            peer_sync_fail(peer, "could not queue chunk fetch");
        }
    } else {
        peer_sync_after_chunk_page(peer);
    }
    return true;
}

static bool peer_sync_handle_chunk(nm_peer_t *peer, const nm_frame_t *frame)
{
    const uint8_t *expected_hash;
    const uint8_t *chunk_hash;
    const uint8_t *chunk_data;
    size_t chunk_length;
    int result;

    if (!peer->sync.active || peer->sync.phase != NM_SYNC_WAIT_CHUNK ||
        peer->sync.missing_pos >= peer->sync.missing_count ||
        frame->payload == NULL ||
        frame->length <= NM_SHA256_LEN ||
        frame->length > (uint64_t)NM_SHA256_LEN + NM_CHUNK_SIZE) {
        return false;
    }
    expected_hash = peer->sync.missing[peer->sync.missing_pos];
    chunk_hash = frame->payload;
    chunk_data = frame->payload + NM_SHA256_LEN;
    chunk_length = (size_t)(frame->length - NM_SHA256_LEN);
    if (memcmp(expected_hash, chunk_hash, NM_SHA256_LEN) != 0) {
        return false;
    }
    result = nm_store_put_chunk(peer->cluster->store, chunk_hash, chunk_data,
                                chunk_length);
    if (result != NM_OK) {
        peer_sync_fail(peer, "could not persist fetched chunk");
        return true;
    }
    peer_sync_after_chunk(peer);
    return true;
}

static bool peer_sync_handle_keys(nm_peer_t *peer, const nm_frame_t *frame)
{
    uint32_t total;
    uint32_t returned;
    size_t cursor = NM_LIST_KEYS_RESP_HEADER_SIZE;

    if (!peer->sync.active || peer->sync.phase != NM_SYNC_WAIT_KEY_INDEX ||
        frame->length < NM_LIST_KEYS_RESP_HEADER_SIZE ||
        frame->payload == NULL) {
        return false;
    }
    total = nm_get_u32_be(frame->payload);
    returned = nm_get_u32_be(frame->payload + 4u);
    if (returned > peer->cluster->sync_batch) {
        return false;
    }
    for (uint32_t i = 0u; i < returned; ++i) {
        uint16_t key_length;
        if (cursor > frame->length || frame->length - cursor < 2u) {
            return false;
        }
        key_length = nm_get_u16_be(frame->payload + cursor);
        cursor += 2u;
        if (key_length == 0u || key_length > NM_MAX_KEY_LEN ||
            cursor > frame->length ||
            frame->length - cursor < (uint64_t)key_length) {
            return false;
        }
        if (memchr(frame->payload + cursor, '\0', key_length) != NULL) {
            return false;
        }
        memcpy(peer->sync.keys[i], frame->payload + cursor, key_length);
        peer->sync.keys[i][key_length] = '\0';
        if (!nm_key_is_valid(peer->sync.keys[i])) {
            return false;
        }
        cursor += key_length;
    }
    if (cursor != frame->length) {
        return false;
    }
    peer->sync.key_total = total;
    peer->sync.key_page_returned = returned;
    peer->sync.key_count = returned;
    peer->sync.key_pos = 0u;
    if (returned == 0u) {
        peer_sync_after_key_page(peer);
    } else if (!peer_sync_request_manifest(peer)) {
        peer_sync_fail(peer, "could not queue first manifest fetch");
    }
    return true;
}

static bool peer_sync_handle_manifest(nm_peer_t *peer,
                                      const nm_frame_t *frame)
{
    nm_manifest_t remote;
    nm_manifest_t local;
    nm_status_t local_status;
    int result;
    bool should_store = false;

    memset(&remote, 0, sizeof(remote));
    memset(&local, 0, sizeof(local));
    if (!peer->sync.active ||
        peer->sync.phase != NM_SYNC_WAIT_MANIFEST ||
        peer->sync.key_pos >= peer->sync.key_count ||
        frame->payload == NULL) {
        return false;
    }
    result = nm_manifest_deserialize(frame->payload, (size_t)frame->length,
                                     &remote);
    if (result != NM_OK) {
        return false;
    }
    if (strcmp(remote.key, peer->sync.keys[peer->sync.key_pos]) != 0) {
        nm_manifest_free(&remote);
        return false;
    }
    local_status = (nm_status_t)nm_store_get_manifest(
        peer->cluster->store, remote.key, &local);
    if (local_status == NM_ERR_NOT_FOUND) {
        should_store = true;
    } else if (local_status != NM_OK) {
        nm_manifest_free(&remote);
        peer_sync_fail(peer, "could not read local manifest");
        return true;
    } else if (remote.updated_ms > local.updated_ms) {
        should_store = true;
    }
    if (should_store) {
        if (remote.deleted) {
            result = nm_store_apply_tombstone(peer->cluster->store, remote.key,
                                              remote.updated_ms);
        } else {
            result = nm_store_put_manifest(peer->cluster->store, &remote);
        }
        if (result != NM_OK) {
            nm_manifest_free(&local);
            nm_manifest_free(&remote);
            peer_sync_fail(peer, "could not persist fetched manifest");
            return true;
        }
    }
    nm_manifest_free(&local);
    nm_manifest_free(&remote);
    peer_sync_after_manifest(peer);
    return true;
}

static void peer_sync_handle_error(nm_peer_t *peer, nm_rpc_kind_t kind,
                                   nm_status_t status)
{
    if (kind == NM_RPC_SYNC_FETCH && status == NM_ERR_NOT_FOUND) {
        peer_sync_after_chunk(peer);
        return;
    }
    if (kind == NM_RPC_SYNC_MANIFEST && status == NM_ERR_NOT_FOUND) {
        peer_sync_after_manifest(peer);
        return;
    }
    peer_sync_fail(peer, nm_status_name(status));
}

static bool peer_handle_response(nm_peer_t *peer, const nm_frame_t *frame)
{
    nm_peer_rpc_t *rpc = peer->rpc_head;
    nm_rpc_kind_t kind;
    uint8_t ack_type;
    uint8_t digest[NM_SHA256_LEN];
    nm_cluster_txn_t *txn;
    nm_status_t status;
    char error_message[NM_MAX_ERROR_MSG + 1u];
    int result;

    if (rpc == NULL) {
        peer_log(peer, NM_LOG_WARN, "unexpected response type 0x%02x",
                 (unsigned int)frame->type);
        return false;
    }
    if (frame->type == NM_MSG_ERROR) {
        result = nm_wire_error_decode(frame->payload, frame->length, &status,
                                      error_message,
                                      sizeof(error_message));
        if (result != NM_OK) {
            return false;
        }
        kind = rpc->kind;
        txn = rpc->txn;
        free(peer_rpc_unlink(peer));
        if (txn != NULL) {
            transaction_peer_result(txn, status);
        } else {
            peer_sync_handle_error(peer, kind, status);
        }
        return true;
    }
    if (frame->type != rpc->expected_type) {
        peer_log(peer, NM_LOG_WARN,
                 "protocol mismatch: expected 0x%02x, got 0x%02x",
                 (unsigned int)rpc->expected_type, (unsigned int)frame->type);
        return false;
    }
    if (rpc->kind == NM_RPC_STORE || rpc->kind == NM_RPC_STORE_OBJECT ||
        rpc->kind == NM_RPC_DELETE_OBJECT) {
        if (nm_wire_ack_decode(frame->payload, frame->length, &status,
                               &ack_type, digest) != NM_OK ||
            ack_type != rpc->ack_type ||
            memcmp(digest, rpc->digest, NM_SHA256_LEN) != 0) {
            return false;
        }
        txn = rpc->txn;
        free(peer_rpc_unlink(peer));
        transaction_peer_result(txn, status);
        return true;
    }
    kind = rpc->kind;
    free(peer_rpc_unlink(peer));
    switch (kind) {
    case NM_RPC_SYNC_INDEX:
        return peer_sync_handle_chunk_index(peer, frame);
    case NM_RPC_SYNC_FETCH:
        return peer_sync_handle_chunk(peer, frame);
    case NM_RPC_SYNC_LIST_KEYS:
        return peer_sync_handle_keys(peer, frame);
    case NM_RPC_SYNC_MANIFEST:
        return peer_sync_handle_manifest(peer, frame);
    default:
        return false;
    }
}

static bool peer_handle_pong(nm_peer_t *peer, const nm_frame_t *frame)
{
    uint64_t nonce;
    uint64_t now;

    if (frame->length != 8u || frame->payload == NULL) {
        return false;
    }
    nonce = nm_get_u64_be(frame->payload);
    if (!peer->ping_outstanding || nonce != peer->ping_nonce) {
        return true;
    }
    now = nm_now_ms();
    peer->ping_outstanding = false;
    peer->next_ping_ms = now + peer->cluster->heartbeat_interval_ms;
    if (!peer->up) {
        peer->up = true;
        peer->reconnect_delay_ms = NM_CLUSTER_RECONNECT_MIN_MS;
        peer->cluster->up_peers += 1u;
        peer->sync.active = false;
        peer->sync.next_run_ms = now;
        peer_log(peer, NM_LOG_INFO, "peer is UP");
    }
    return true;
}

static bool peer_process_frames(nm_peer_t *peer)
{
    for (;;) {
        nm_frame_t frame;
        bool complete = false;
        bool healthy;
        int result;

        memset(&frame, 0, sizeof(frame));
        result = nm_frame_parser_next(&peer->input, &frame, &complete);
        if (result != NM_OK) {
            peer_log(peer, NM_LOG_WARN, "frame parser error: %s",
                     nm_status_name((nm_status_t)result));
            return false;
        }
        if (!complete) {
            return true;
        }
        if (frame.type == NM_MSG_PONG) {
            healthy = peer_handle_pong(peer, &frame);
            nm_frame_free(&frame);
            if (!healthy) {
                return false;
            }
            continue;
        }
        healthy = peer_handle_response(peer, &frame);
        nm_frame_free(&frame);
        if (!healthy || peer->fd < 0) {
            return false;
        }
    }
}

static bool peer_read_input(nm_peer_t *peer)
{
    uint8_t buffer[NM_CLUSTER_READ_CHUNK];
    ssize_t received;
    int result;

    received = recv(peer->fd, buffer, sizeof(buffer), 0);
    if (received < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return true;
        }
        peer_log(peer, NM_LOG_DEBUG, "recv failed: %s", strerror(errno));
        peer_disconnect(peer, "recv failed");
        return false;
    }
    if (received == 0) {
        peer_disconnect(peer, "peer closed connection");
        return false;
    }
    result = nm_frame_parser_feed(&peer->input, buffer, (size_t)received);
    if (result != NM_OK) {
        peer_disconnect(peer, "input frame buffer limit");
        return false;
    }
    if (!peer_process_frames(peer)) {
        if (peer->fd >= 0) {
            peer_disconnect(peer, "protocol violation");
        }
        return false;
    }
    return peer->fd >= 0;
}

/* ------------------------------------------------------------------------- */
/* Quorum transactions                                                       */
/* ------------------------------------------------------------------------- */

static void transaction_evaluate(nm_cluster_txn_t *transaction)
{
    uint32_t possible;

    if (transaction->ready) {
        return;
    }
    if (transaction->acknowledgements >= transaction->quorum) {
        transaction->ready = true;
        transaction->status = NM_OK;
        return;
    }
    possible = transaction->acknowledgements + transaction->attempts;
    if (possible < transaction->quorum) {
        transaction->ready = true;
        transaction->status = NM_ERR_QUORUM;
        return;
    }
    if (transaction->attempts == 0u) {
        transaction->ready = true;
        transaction->status = NM_ERR_QUORUM;
    }
}

static void transaction_peer_result(nm_cluster_txn_t *transaction,
                                    nm_status_t status)
{
    if (transaction->attempts > 0u) {
        transaction->attempts -= 1u;
    }
    if (status == NM_OK) {
        transaction->acknowledgements += 1u;
    }
    transaction_evaluate(transaction);
}

static int cluster_enqueue_replication(nm_cluster_t *cluster,
                                       nm_rpc_kind_t kind,
                                       const uint8_t digest[NM_SHA256_LEN],
                                       const void *payload,
                                       size_t payload_length,
                                       nm_cluster_write_done_fn callback,
                                       void *opaque)
{
    nm_cluster_txn_t *transaction;

    if (cluster == NULL || callback == NULL || digest == NULL ||
        (payload == NULL && payload_length != 0u)) {
        return NM_ERR_INVALID;
    }
    transaction = calloc(1u, sizeof(*transaction));
    if (transaction == NULL) {
        return NM_ERR_NOMEM;
    }
    transaction->cluster = cluster;
    transaction->callback = callback;
    transaction->opaque = opaque;
    transaction->replicas = (uint32_t)cluster->peer_count + 1u;
    transaction->quorum = nm_quorum_size(transaction->replicas);
    transaction->acknowledgements = 1u; /* local write already succeeded */
    for (size_t i = 0u; i < cluster->peer_count; ++i) {
        nm_peer_t *peer = &cluster->peers[i];
        int result;
        if (!peer->up) {
            continue;
        }
        uint8_t message_type;
        switch (kind) {
        case NM_RPC_STORE:
            message_type = (uint8_t)NM_MSG_REPLICA_STORE;
            break;
        case NM_RPC_STORE_OBJECT:
            message_type = (uint8_t)NM_MSG_REPLICA_STORE_OBJECT;
            break;
        case NM_RPC_DELETE_OBJECT:
            message_type = (uint8_t)NM_MSG_REPLICA_DELETE_OBJECT;
            break;
        default:
            free(transaction);
            return NM_ERR_INVALID;
        }
        result = peer_enqueue_rpc(peer, kind, message_type, NM_MSG_ACK,
                                  message_type, digest, transaction, payload,
                                  payload_length);
        if (result == NM_OK) {
            transaction->attempts += 1u;
        }
    }
    transaction_evaluate(transaction);
    transaction->next = cluster->transactions;
    cluster->transactions = transaction;
    return NM_OK;
}

int nm_cluster_replicate_chunk(nm_cluster_t *cluster,
                               const uint8_t hash[NM_SHA256_LEN],
                               const uint8_t *data, size_t length,
                               nm_cluster_write_done_fn callback,
                               void *opaque)
{
    uint8_t *payload;
    size_t payload_length;
    int result;

    if (cluster == NULL || hash == NULL || data == NULL || callback == NULL ||
        length == 0u || length > NM_CHUNK_SIZE) {
        return NM_ERR_INVALID;
    }
    payload_length = NM_SHA256_LEN + length;
    payload = malloc(payload_length);
    if (payload == NULL) {
        return NM_ERR_NOMEM;
    }
    memcpy(payload, hash, NM_SHA256_LEN);
    memcpy(payload + NM_SHA256_LEN, data, length);
    result = cluster_enqueue_replication(cluster, NM_RPC_STORE, hash, payload,
                                         payload_length, callback, opaque);
    free(payload);
    return result;
}

int nm_cluster_replicate_manifest(nm_cluster_t *cluster,
                                  const nm_manifest_t *manifest,
                                  nm_cluster_write_done_fn callback,
                                  void *opaque)
{
    uint8_t key_digest[NM_SHA256_LEN];
    uint8_t *serialized = NULL;
    size_t serialized_length = 0u;
    int result;

    if (cluster == NULL || manifest == NULL || callback == NULL ||
        !nm_key_is_valid(manifest->key)) {
        return NM_ERR_INVALID;
    }
    result = nm_manifest_key_hash(manifest->key, key_digest);
    if (result != NM_OK) {
        return result;
    }
    result = nm_manifest_serialize(manifest, &serialized, &serialized_length);
    if (result != NM_OK) {
        return result;
    }
    result = cluster_enqueue_replication(cluster, NM_RPC_STORE_OBJECT,
                                         key_digest, serialized,
                                         serialized_length, callback, opaque);
    free(serialized);
    return result;
}

int nm_cluster_replicate_delete(nm_cluster_t *cluster, const char *key,
                                uint64_t deleted_ms,
                                nm_cluster_write_done_fn callback,
                                void *opaque)
{
    uint8_t key_digest[NM_SHA256_LEN];
    uint8_t *payload;
    size_t key_length;
    size_t payload_length;
    int result;

    if (cluster == NULL || key == NULL || callback == NULL ||
        !nm_key_is_valid(key) || deleted_ms == 0u) {
        return NM_ERR_INVALID;
    }
    result = nm_manifest_key_hash(key, key_digest);
    if (result != NM_OK) {
        return result;
    }
    key_length = strlen(key);
    payload_length = 8u + key_length;
    payload = malloc(payload_length);
    if (payload == NULL) {
        return NM_ERR_NOMEM;
    }
    nm_put_u64_be(payload, deleted_ms);
    memcpy(payload + 8u, key, key_length);
    result = cluster_enqueue_replication(cluster, NM_RPC_DELETE_OBJECT,
                                         key_digest, payload, payload_length,
                                         callback, opaque);
    free(payload);
    return result;
}

static void cluster_notify_transactions(nm_cluster_t *cluster)
{
    nm_cluster_txn_t **link = &cluster->transactions;

    while (*link != NULL) {
        nm_cluster_txn_t *transaction = *link;

        if (transaction->ready && !transaction->notified) {
            nm_cluster_write_done_fn callback = transaction->callback;
            void *opaque = transaction->opaque;
            nm_status_t status = transaction->status;
            uint32_t acknowledgements = transaction->acknowledgements;
            uint32_t replicas = transaction->replicas;

            transaction->notified = true;
            transaction->callback = NULL;
            transaction->opaque = NULL;
            if (callback != NULL) {
                callback(opaque, status, acknowledgements, replicas);
            }
        }
        if (transaction->notified && transaction->attempts == 0u) {
            *link = transaction->next;
            free(transaction);
        } else {
            link = &transaction->next;
        }
    }
}

/* ------------------------------------------------------------------------- */
/* Public lifecycle and event dispatch                                       */
/* ------------------------------------------------------------------------- */

static int peer_resolve(nm_peer_t *peer)
{
    struct addrinfo hints;
    struct addrinfo *addresses = NULL;
    struct addrinfo *current;
    struct addrinfo *selected = NULL;
    char service[8];
    int written;
    int result = NM_ERR_UNAVAILABLE;

    written = snprintf(service, sizeof(service), "%u",
                       (unsigned int)peer->endpoint.port);
    if (written <= 0 || (size_t)written >= sizeof(service)) {
        return NM_ERR_INVALID;
    }
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    if (getaddrinfo(peer->endpoint.host, service, &hints, &addresses) != 0) {
        return NM_ERR_UNAVAILABLE;
    }
    for (current = addresses; current != NULL; current = current->ai_next) {
        if (current->ai_addrlen <= sizeof(peer->address)) {
            if (selected == NULL || current->ai_family == AF_INET) {
                selected = current;
                if (current->ai_family == AF_INET) {
                    break;
                }
            }
        }
    }
    if (selected != NULL) {
        memcpy(&peer->address, selected->ai_addr, selected->ai_addrlen);
        peer->address_length = (socklen_t)selected->ai_addrlen;
        result = NM_OK;
    }
    freeaddrinfo(addresses);
    return result;
}

int nm_cluster_init(nm_cluster_t **out, const nm_cluster_options_t *options)
{
    nm_cluster_t *cluster;
    size_t node_id_length;

    if (out == NULL) {
        return NM_ERR_INVALID;
    }
    *out = NULL;
    if (options == NULL || options->epoll_fd < 0 || options->store == NULL ||
        options->peer_count == 0u || options->peer_count > NM_MAX_PEERS ||
        options->peers == NULL || options->connect_timeout_ms == 0u ||
        options->rpc_timeout_ms == 0u ||
        options->heartbeat_interval_ms == 0u ||
        options->heartbeat_timeout_ms == 0u ||
        options->anti_entropy_interval_ms == 0u ||
        options->max_peer_requests == 0u ||
        options->max_peer_requests > NM_CLUSTER_MAX_PEER_REQUESTS ||
        options->max_peer_queue_bytes <
            NM_CHUNK_SIZE + NM_FRAME_HEADER_SIZE + NM_SHA256_LEN ||
        options->max_peer_queue_bytes > NM_CLUSTER_MAX_PEER_QUEUE_BYTES) {
        return NM_ERR_INVALID;
    }
    cluster = calloc(1u, sizeof(*cluster));
    if (cluster == NULL) {
        return NM_ERR_NOMEM;
    }
    cluster->epoll_fd = options->epoll_fd;
    cluster->store = options->store;
    cluster->peer_count = options->peer_count;
    cluster->connect_timeout_ms = options->connect_timeout_ms;
    cluster->rpc_timeout_ms = options->rpc_timeout_ms;
    cluster->heartbeat_interval_ms = options->heartbeat_interval_ms;
    cluster->heartbeat_timeout_ms = options->heartbeat_timeout_ms;
    cluster->anti_entropy_interval_ms = options->anti_entropy_interval_ms;
    cluster->max_peer_queue_bytes = options->max_peer_queue_bytes;
    cluster->max_peer_requests = options->max_peer_requests;
    cluster->sync_batch = options->sync_batch == 0u
                              ? NM_CLUSTER_DEFAULT_SYNC_BATCH
                              : options->sync_batch;
    if (cluster->sync_batch > NM_CLUSTER_MAX_SYNC_BATCH) {
        cluster->sync_batch = NM_CLUSTER_MAX_SYNC_BATCH;
    }
    node_id_length = strlen(options->node_id != NULL ? options->node_id
                                                     : "cluster");
    if (node_id_length >= sizeof(cluster->node_id)) {
        free(cluster);
        return NM_ERR_INVALID;
    }
    memcpy(cluster->node_id, options->node_id != NULL ? options->node_id
                                                      : "cluster",
           node_id_length + 1u);

    for (size_t i = 0u; i < cluster->peer_count; ++i) {
        nm_peer_t *peer = &cluster->peers[i];
        peer->cluster = cluster;
        peer->endpoint = options->peers[i];
        peer->fd = -1;
        peer->next_connect_ms = 0u;
        peer->reconnect_delay_ms = NM_CLUSTER_RECONNECT_MIN_MS;
        peer->sync.next_run_ms = 0u;
        nm_frame_parser_init(&peer->input);
        nm_outbuf_init(&peer->output);
        if (peer->endpoint.host[0] == '\0' ||
            peer->endpoint.port == 0u || peer_resolve(peer) != NM_OK) {
            nm_log(NM_LOG_ERROR, "%s: cannot resolve peer %s:%u",
                   cluster->node_id, peer->endpoint.host,
                   (unsigned int)peer->endpoint.port);
            for (size_t j = 0u; j <= i; ++j) {
                nm_frame_parser_reset(&cluster->peers[j].input);
                nm_outbuf_free(&cluster->peers[j].output);
            }
            free(cluster);
            return NM_ERR_UNAVAILABLE;
        }
    }
    nm_log(NM_LOG_INFO, "%s: cluster initialized with %zu peers (quorum %u)",
           cluster->node_id, cluster->peer_count,
           (unsigned int)nm_quorum_size((uint32_t)cluster->peer_count + 1u));
    *out = cluster;
    return NM_OK;
}

void nm_cluster_destroy(nm_cluster_t *cluster)
{
    if (cluster == NULL) {
        return;
    }
    cluster->shutting_down = true;
    for (size_t i = 0u; i < cluster->peer_count; ++i) {
        nm_peer_t *peer = &cluster->peers[i];
        if (peer->fd >= 0) {
            if (cluster->epoll_fd >= 0) {
                (void)epoll_ctl(cluster->epoll_fd, EPOLL_CTL_DEL, peer->fd,
                                NULL);
            }
            nm_close_fd(&peer->fd);
        }
        for (;;) {
            nm_peer_rpc_t *rpc = peer_rpc_unlink(peer);
            if (rpc == NULL) {
                break;
            }
            free(rpc);
        }
        nm_frame_parser_reset(&peer->input);
        nm_outbuf_free(&peer->output);
    }
    for (;;) {
        nm_cluster_txn_t *transaction = cluster->transactions;
        if (transaction == NULL) {
            break;
        }
        cluster->transactions = transaction->next;
        if (!transaction->notified && transaction->callback != NULL) {
            transaction->callback(transaction->opaque, NM_ERR_UNAVAILABLE,
                                  transaction->acknowledgements,
                                  transaction->replicas);
        }
        free(transaction);
    }
    free(cluster);
}

bool nm_cluster_owns_fd(const nm_cluster_t *cluster, int fd)
{
    if (cluster == NULL || fd < 0) {
        return false;
    }
    for (size_t i = 0u; i < cluster->peer_count; ++i) {
        if (cluster->peers[i].fd == fd) {
            return true;
        }
    }
    return false;
}

void nm_cluster_handle_event(nm_cluster_t *cluster, int fd, uint32_t events)
{
    nm_peer_t *peer;

    if (cluster == NULL || cluster->shutting_down) {
        return;
    }
    peer = cluster_find_fd(cluster, fd);
    if (peer == NULL) {
        return;
    }
    (void)peer_handle_event(peer, events);
}

void nm_cluster_tick(nm_cluster_t *cluster)
{
    uint64_t now;

    if (cluster == NULL || cluster->shutting_down) {
        return;
    }
    now = nm_now_ms();
    for (size_t i = 0u; i < cluster->peer_count; ++i) {
        nm_peer_t *peer = &cluster->peers[i];
        if (peer->fd < 0) {
            if (now >= peer->next_connect_ms) {
                peer_start_connect(peer);
            }
            continue;
        }
        if (peer->connecting) {
            if (now >= peer->connect_deadline_ms) {
                peer_disconnect(peer, "connect timeout");
            }
            continue;
        }
        if (!peer->connected) {
            peer_disconnect(peer, "inconsistent peer state");
            continue;
        }
        if (peer->rpc_head != NULL &&
            now >= peer->rpc_head->deadline_ms) {
            peer_disconnect(peer, "RPC timeout");
            continue;
        }
        if (peer->ping_outstanding) {
            if (now >= peer->ping_deadline_ms) {
                peer_disconnect(peer, "heartbeat timeout");
                continue;
            }
        } else if (now >= peer->next_ping_ms) {
            if (!peer_send_ping(peer)) {
                peer_disconnect(peer, "could not send heartbeat");
                continue;
            }
        }
        if (peer->up && !peer->sync.active &&
            now >= peer->sync.next_run_ms) {
            peer_sync_start(peer);
        }
    }
    cluster_notify_transactions(cluster);
}

size_t nm_cluster_peer_count(const nm_cluster_t *cluster)
{
    return cluster != NULL ? cluster->peer_count : 0u;
}

size_t nm_cluster_up_peer_count(const nm_cluster_t *cluster)
{
    return cluster != NULL ? cluster->up_peers : 0u;
}

uint32_t nm_cluster_quorum(const nm_cluster_t *cluster)
{
    if (cluster == NULL) {
        return 1u;
    }
    return nm_quorum_size((uint32_t)cluster->peer_count + 1u);
}
