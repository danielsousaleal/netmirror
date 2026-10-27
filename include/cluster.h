/*
 * netmirror - outbound replication cluster
 *
 * The cluster owns one persistent, non-blocking TCP connection to every peer.
 * Peer sockets are registered in the node's existing epoll instance; this
 * module never blocks and never creates a worker thread. All functions must be
 * called from the node's event-loop thread.
 *
 * WRITE QUORUM
 * ------------
 * A successful local store counts as the first acknowledgement. Callers invoke
 * nm_cluster_replicate_*() only after the local chunk/manifest write succeeded.
 * The module forwards the object to all currently healthy peers and completes
 * the callback when floor(replicas/2)+1 acknowledgements are reached. Thus a
 * three-node cluster requires the local write plus one remote ACK (2 of 3).
 *
 * The callback is also called on failure, exactly once. Callbacks are delivered
 * from nm_cluster_tick(), never from the nm_cluster_replicate_*() call itself.
 *
 * HEARTBEATS
 * ----------
 * PING is sent on each connected peer at the heartbeat interval. A matching
 * PONG marks/keeps the peer UP. Heartbeat deadlines and reconnects are driven
 * by nm_cluster_tick(); no sleep or blocking wait exists.
 *
 * ANTI-ENTROPY
 * ------------
 * When a peer transitions to UP, and periodically afterwards, the node pulls
 * missing chunks via SYNC_REQ/FETCH and missing/newer manifests via the
 * LIST_KEYS/FETCH_OBJECT requests. Requests are interleaved with replication
 * on the same peer connection and are bounded by per-peer queue limits.
 */

#ifndef NETMIRROR_CLUSTER_H
#define NETMIRROR_CLUSTER_H

#include "blob.h"
#include "netmirror.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define NM_CLUSTER_DEFAULT_CONNECT_TIMEOUT_MS 1000u
#define NM_CLUSTER_DEFAULT_RPC_TIMEOUT_MS 5000u
#define NM_CLUSTER_DEFAULT_HEARTBEAT_INTERVAL_MS 1000u
#define NM_CLUSTER_DEFAULT_HEARTBEAT_TIMEOUT_MS 3000u
#define NM_CLUSTER_DEFAULT_ANTI_ENTROPY_INTERVAL_MS 15000u
/*
 * A max-size manifest frame is much larger than a chunk frame. Leave room for
 * one manifest frame plus one chunk frame; the hard cap stays at 64 MiB.
 */
#define NM_CLUSTER_MAX_PEER_QUEUE_BYTES (64u * 1024u * 1024u)
#define NM_CLUSTER_DEFAULT_MAX_PEER_QUEUE_BYTES \
    (NM_MAX_FRAME_PAYLOAD + NM_CHUNK_SIZE + NM_FRAME_HEADER_SIZE)
#define NM_CLUSTER_DEFAULT_MAX_PEER_REQUESTS 8u
#define NM_CLUSTER_DEFAULT_SYNC_BATCH 128u
#define NM_CLUSTER_MAX_PEER_REQUESTS 64u

typedef struct nm_cluster nm_cluster_t;

typedef void (*nm_cluster_write_done_fn)(void *opaque, nm_status_t status,
                                          uint32_t acknowledgements,
                                          uint32_t replicas);

typedef struct nm_cluster_options {
    int epoll_fd;                 /* shared with the node */
    nm_store_t *store;            /* local persistent store */
    const char *node_id;          /* borrowed, copied internally */
    const nm_endpoint_t *peers;   /* replicated peers, excludes self */
    size_t peer_count;
    uint32_t connect_timeout_ms;
    uint32_t rpc_timeout_ms;
    uint32_t heartbeat_interval_ms;
    uint32_t heartbeat_timeout_ms;
    uint32_t anti_entropy_interval_ms;
    size_t max_peer_queue_bytes;
    size_t max_peer_requests;
    uint32_t sync_batch;
} nm_cluster_options_t;

/*
 * Resolve peers and initialize state. No sockets are opened synchronously;
 * connections begin on the first tick. On failure returns a non-NM_OK status
 * and leaves *out NULL.
 */
int nm_cluster_init(nm_cluster_t **out, const nm_cluster_options_t *options);
void nm_cluster_destroy(nm_cluster_t *cluster);

/* True if fd belongs to a peer connection currently registered with epoll. */
bool nm_cluster_owns_fd(const nm_cluster_t *cluster, int fd);

/* Dispatch one epoll event for a peer fd returned by owns_fd(). */
void nm_cluster_handle_event(nm_cluster_t *cluster, int fd, uint32_t events);

/*
 * Drive reconnect backoff, heartbeat emission/timeouts, anti-entropy progress
 * and write-quorum callbacks. Call once per node event-loop iteration.
 */
void nm_cluster_tick(nm_cluster_t *cluster);

size_t nm_cluster_peer_count(const nm_cluster_t *cluster);
size_t nm_cluster_up_peer_count(const nm_cluster_t *cluster);
uint32_t nm_cluster_quorum(const nm_cluster_t *cluster);

/*
 * Forward a locally committed chunk/manifest/deletion to peers. The chunk
 * hash/key must already be present in the local store. Returns NM_OK if a
 * transaction was accepted and the callback will be called asynchronously.
 * Returns an error if no transaction could be started (invalid input/allocation
 * failure); in that case no callback occurs.
 */
int nm_cluster_replicate_chunk(nm_cluster_t *cluster,
                               const uint8_t hash[NM_SHA256_LEN],
                               const uint8_t *data, size_t length,
                               nm_cluster_write_done_fn callback,
                               void *opaque);
int nm_cluster_replicate_manifest(nm_cluster_t *cluster,
                                  const nm_manifest_t *manifest,
                                  nm_cluster_write_done_fn callback,
                                  void *opaque);
int nm_cluster_replicate_delete(nm_cluster_t *cluster, const char *key,
                                uint64_t deleted_ms,
                                nm_cluster_write_done_fn callback,
                                void *opaque);

#ifdef __cplusplus
}
#endif

#endif /* NETMIRROR_CLUSTER_H */
