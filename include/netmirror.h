/*
 * netmirror - replicated content-addressable object store
 *
 * Global protocol contract and project-wide constants.
 *
 * WIRE FORMAT
 * -----------
 * Every message on the internal TCP network is framed as:
 *
 *   +----------+------------------+-------------------+
 *   | type (1) | length (8, BE)   | payload (length)  |
 *   +----------+------------------+-------------------+
 *
 * `type` is one byte (nm_msg_type_t). `length` is an unsigned 64-bit
 * big-endian integer and counts payload bytes only. The header is always
 * exactly NM_FRAME_HEADER_SIZE bytes and is never serialized as a C struct.
 *
 * All multi-byte integers inside payloads are also big-endian. No HTTP is
 * used between nodes.
 *
 * MESSAGE PAYLOADS
 * ----------------
 *   PING            u64 nonce
 *   PONG            u64 nonce echoed from PING
 *   STORE           32-byte chunk digest || raw chunk bytes
 *   ACK             u8 status || u8 acknowledged_type || 32-byte digest
 *   FETCH           32-byte chunk digest
 *   ERROR           u8 status || u16 message_length || UTF-8 message
 *   CHUNK           32-byte chunk digest || raw chunk bytes
 *   STORE_OBJECT    serialized nm_manifest_t (see MANIFEST WIRE FORMAT)
 *   FETCH_OBJECT    object key bytes (no NUL, 1..NM_MAX_KEY_LEN bytes)
 *   OBJECT          serialized nm_manifest_t (see MANIFEST WIRE FORMAT)
 *   SYNC_REQ        u32 offset || u32 limit
 *   SYNC_RESP       u32 total_chunks || u32 returned
 *                   || returned * 32-byte chunk digests
 *   LIST_KEYS_REQ   u32 offset || u32 limit
 *   LIST_KEYS_RESP  u32 total_manifests || u32 returned
 *                   || returned * (u16 key_length || key bytes)
 *   REPLICA_STORE         same payload as STORE, never re-fanned-out
 *   REPLICA_STORE_OBJECT  same payload as STORE_OBJECT, never re-fanned-out
 *
 * LIST_KEYS_* lets anti-entropy discover manifests created while a node was
 * offline. Keys are sorted lexicographically so pagination is deterministic.
 * Limits are clamped by the receiving node.
 *
 * REPLICA_* is the node-to-node write path. A node receiving a client STORE
 * coordinates quorum fan-out; receiving a REPLICA_STORE only applies the
 * write locally and ACKs it. This distinction prevents infinite replication
 * loops. ACKs for replica requests echo the replica message type.
 *
 * For ACK frames, the digest identifies a chunk digest for STORE and the
 * SHA-256 of the object key for STORE_OBJECT. Error messages are advisory;
 * the numeric status is the source of truth.
 *
 * MANIFEST WIRE FORMAT (version 1)
 * --------------------------------
 *   offset  size  field
 *   0       4     magic, ASCII "NMOB" (0x4e4d4f42)
 *   4       1     version (1)
 *   5       1     reserved (0)
 *   6       2     key_length (BE, 1..NM_MAX_KEY_LEN)
 *   8       n     key bytes (UTF-8, no NUL)
 *   8+n     8     total_size (BE)
 *   16+n    8     updated_ms (BE, Unix epoch milliseconds)
 *   24+n    4     chunk_count (BE, 0..NM_MAX_OBJECT_CHUNKS)
 *   28+n    32    SHA-256 of the complete object
 *   60+n    32*c  chunk digests, in object order
 *
 * Chunk i has length min(NM_CHUNK_SIZE, total_size - i*NM_CHUNK_SIZE).
 * A zero-byte object has zero chunks and is still a valid manifest.
 *
 * PERSISTENT LAYOUT
 * -----------------
 *   <data_dir>/chunks/<chunk-sha256-hex>
 *   <data_dir>/objects/<sha256(key)-hex>.manifest
 *
 * Chunks are immutable and named after their content. Manifests are replaced
 * atomically when the same key is uploaded again.
 *
 * WRITE QUORUM
 * ------------
 * The project has 3 replicas and requires 2 acknowledgements. This is the
 * strict majority floor(N/2)+1 implemented by nm_quorum_size(). Applying the
 * alternate expression ceil(N/2)+1 literally to N=3 would require all 3
 * replicas and contradict the explicit 2-of-3 rule, so the explicit rule and
 * the majority formula prevail.
 */

#ifndef NETMIRROR_H
#define NETMIRROR_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define NM_PROJECT_NAME "netmirror"
#define NM_PROTOCOL_VERSION 1u

/* Chunks are 4 MiB. Object limits are constants to make wire sizing exact. */
#define NM_CHUNK_SIZE (4u * 1024u * 1024u)
#define NM_MAX_OBJECT_CHUNKS 4096u
#define NM_MAX_OBJECT_SIZE ((uint64_t)NM_CHUNK_SIZE * NM_MAX_OBJECT_CHUNKS)

#define NM_SHA256_LEN 32u
#define NM_SHA256_HEX_LEN (NM_SHA256_LEN * 2u)

#define NM_MAX_KEY_LEN 127u
#define NM_MAX_KEY (NM_MAX_KEY_LEN + 1u)

#define NM_MAX_HOST_LEN 253u
#define NM_MAX_HOST (NM_MAX_HOST_LEN + 1u)
#define NM_MAX_PEERS 16u

#define NM_MAX_ERROR_MSG 160u
#define NM_MAX_ERROR_WIRE (1u + 2u + NM_MAX_ERROR_MSG)

/* 9-byte framing header: 1-byte type + 8-byte big-endian payload length. */
#define NM_FRAME_HEADER_SIZE 9u
#define NM_MAX_FRAME_PAYLOAD (NM_CHUNK_SIZE + 4096u)

/* Manifest constants. Keep these in sync with the format documented above. */
#define NM_MANIFEST_MAGIC 0x4E4D4F42u
#define NM_MANIFEST_VERSION 1u
#define NM_MANIFEST_FIXED_HEADER_SIZE 8u
#define NM_MANIFEST_FIXED_TRAILER_SIZE 52u
#define NM_MAX_MANIFEST_SIZE                                                   \
    (NM_MANIFEST_FIXED_HEADER_SIZE + NM_MAX_KEY_LEN +                          \
     NM_MANIFEST_FIXED_TRAILER_SIZE +                                          \
     (NM_MAX_OBJECT_CHUNKS * NM_SHA256_LEN))

/* Exact ACK payload layout. */
#define NM_ACK_WIRE_SIZE (1u + 1u + NM_SHA256_LEN)
#define NM_ACK_STATUS_OFFSET 0u
#define NM_ACK_TYPE_OFFSET 1u
#define NM_ACK_DIGEST_OFFSET 2u

/* Exact ERROR payload prefix. The trailing message is not NUL-terminated. */
#define NM_ERROR_STATUS_OFFSET 0u
#define NM_ERROR_LENGTH_OFFSET 1u
#define NM_ERROR_MESSAGE_OFFSET 3u

/* Exact SYNC payload layouts. */
#define NM_SYNC_REQ_WIRE_SIZE 8u
#define NM_SYNC_RESP_HEADER_SIZE 8u
#define NM_SYNC_DEFAULT_BATCH 512u

/* Manifest-key pagination used by anti-entropy. */
#define NM_LIST_KEYS_REQ_WIRE_SIZE 8u
#define NM_LIST_KEYS_RESP_HEADER_SIZE 8u
#define NM_LIST_KEYS_DEFAULT_BATCH 128u

/* Operational defaults; every value can be overridden by configuration. */
#define NM_DEFAULT_NODE_PORT 7001u
#define NM_DEFAULT_GATEWAY_PORT 8080u
#define NM_DEFAULT_REPLICATION_FACTOR 3u
#define NM_DEFAULT_CONNECT_TIMEOUT_MS 500
#define NM_DEFAULT_RPC_TIMEOUT_MS 5000
#define NM_DEFAULT_HTTP_TIMEOUT_MS 30000
#define NM_DEFAULT_HEARTBEAT_INTERVAL_MS 2000
#define NM_DEFAULT_PEER_TIMEOUT_MS 1500
#define NM_DEFAULT_ANTI_ENTROPY_INTERVAL_MS 15000
#define NM_DEFAULT_SYNC_BATCH NM_SYNC_DEFAULT_BATCH
#define NM_MAX_HTTP_CONNECTIONS 128u
#define NM_MAX_HTTP_HEADER_SIZE 16384u
#define NM_HTTP_SPOOL_DIR "/tmp"

/*
 * Message types. Values are part of the on-wire protocol; never renumber an
 * existing value.
 */
typedef enum nm_msg_type {
    NM_MSG_PING = 0x01,
    NM_MSG_PONG = 0x02,
    NM_MSG_STORE = 0x03,
    NM_MSG_FETCH = 0x04,
    NM_MSG_ACK = 0x05,
    NM_MSG_ERROR = 0x06,
    NM_MSG_CHUNK = 0x07,
    NM_MSG_STORE_OBJECT = 0x08,
    NM_MSG_FETCH_OBJECT = 0x09,
    NM_MSG_OBJECT = 0x0A,
    NM_MSG_SYNC_REQ = 0x0B,
    NM_MSG_SYNC_RESP = 0x0C,
    NM_MSG_LIST_KEYS_REQ = 0x0D,
    NM_MSG_LIST_KEYS_RESP = 0x0E,
    NM_MSG_REPLICA_STORE = 0x0F,
    NM_MSG_REPLICA_STORE_OBJECT = 0x10
} nm_msg_type_t;

#define NM_MSG_MIN NM_MSG_PING
#define NM_MSG_MAX NM_MSG_REPLICA_STORE_OBJECT

/*
 * Status values. NM_OK is the only success value. Values travel on the wire
 * as one byte, so this enum must never exceed 255 members.
 */
typedef enum nm_status {
    NM_OK = 0,
    NM_ERR_INVALID = 1,
    NM_ERR_NOMEM = 2,
    NM_ERR_IO = 3,
    NM_ERR_NOT_FOUND = 4,
    NM_ERR_HASH_MISMATCH = 5,
    NM_ERR_PROTOCOL = 6,
    NM_ERR_TIMEOUT = 7,
    NM_ERR_QUORUM = 8,
    NM_ERR_TOO_LARGE = 9,
    NM_ERR_BUSY = 10,
    NM_ERR_UNAVAILABLE = 11,
    NM_ERR_HTTP = 12,
    NM_ERR_SYSTEM = 13
} nm_status_t;

/*
 * A decoded frame. `payload` is heap-allocated by the receive path and must be
 * freed by its owner; it is NULL when length == 0. This struct is never sent
 * over the wire as-is, so its native padding is irrelevant.
 */
typedef struct nm_frame {
    uint8_t type;
    uint64_t length;
    uint8_t *payload;
} nm_frame_t;

/* A TCP endpoint. `host` is always NUL-terminated on construction/parsing. */
typedef struct nm_endpoint {
    char host[NM_MAX_HOST];
    uint16_t port;
} nm_endpoint_t;

/*
 * Deserialized object manifest. `key` is NUL-terminated; `chunk_hashes`
 * holds exactly chunk_count SHA-256 digests in object order and may be NULL
 * only when chunk_count is zero. Use the manifest helpers in blob.h to
 * allocate and free it.
 */
typedef struct nm_manifest {
    char key[NM_MAX_KEY];
    uint64_t total_size;
    uint64_t updated_ms;
    uint32_t chunk_count;
    uint8_t object_hash[NM_SHA256_LEN];
    uint8_t *chunk_hashes;
} nm_manifest_t;

/*
 * Strict-majority write quorum: floor(replicas / 2) + 1.
 *
 * With the project's three nodes: 2 ACKs. A caller must additionally refuse
 * to report success when the number of replicas attempted is below this
 * value, even if every attempted node acknowledged.
 */
static inline uint32_t nm_quorum_size(uint32_t replicas)
{
    return (replicas / 2u) + 1u;
}

_Static_assert(NM_FRAME_HEADER_SIZE == 9u, "frame header must be 9 bytes");
_Static_assert(NM_CHUNK_SIZE == 4194304u, "chunk size must be exactly 4 MiB");
_Static_assert(NM_MAX_MANIFEST_SIZE <= NM_MAX_FRAME_PAYLOAD,
               "a manifest must fit in one frame payload");
_Static_assert((NM_SHA256_LEN * 8u) == 256u, "SHA-256 is 256 bits");
_Static_assert(NM_MAX_KEY >= 2u, "object keys need at least one character");

#ifdef __cplusplus
}
#endif

#endif /* NETMIRROR_H */
