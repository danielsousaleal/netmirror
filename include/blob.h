/*
 * netmirror - local content-addressable storage
 *
 * Public API for hashing, chunk persistence, object manifests and local file
 * ingestion. This layer contains no sockets and no cluster logic.
 *
 * PERSISTENCE RULES
 * -----------------
 *   <root>/chunks/<sha256-hex>               immutable chunk data
 *   <root>/objects/<sha256(key)-hex>.manifest  mutable object metadata
 *
 * Every file is written to a hidden temporary file in its final directory,
 * fsync'ed, then atomically installed with rename(2). Chunk writes and reads
 * verify the SHA-256 digest; the digest is both the file name and the
 * integrity contract. The manifest is the commit marker for an object: it is
 * written only after all referenced chunks were stored successfully.
 *
 * A failed multi-chunk ingest can leave unreferenced chunks behind. They are
 * harmless in a content-addressed store and may be reclaimed by a future GC.
 */

#ifndef NETMIRROR_BLOB_H
#define NETMIRROR_BLOB_H

#include "netmirror.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Internal path buffer size. Paths longer than this are rejected cleanly. */
#define NM_PATH_MAX 4096u

/*
 * Initialized local store. Treat the fields as read-only after
 * nm_store_init(); callers should use the accessors and storage functions.
 */
typedef struct nm_store {
    char root[NM_PATH_MAX];
    char chunks_dir[NM_PATH_MAX];
    char objects_dir[NM_PATH_MAX];
} nm_store_t;

/* ------------------------------------------------------------------------- */
/* SHA-256 and object keys                                                   */
/* ------------------------------------------------------------------------- */

/*
 * Compute SHA-256 over `length` bytes. `data` may be NULL only when length is
 * zero. Returns NM_OK on success.
 */
int nm_sha256_buf(const void *data, size_t length,
                  uint8_t out[NM_SHA256_LEN]);

/* Lowercase hexadecimal encoding; output has NM_SHA256_HEX_LEN chars + NUL. */
void nm_sha256_to_hex(const uint8_t hash[NM_SHA256_LEN],
                      char out[NM_SHA256_HEX_LEN + 1u]);

/*
 * Decode exactly 64 hexadecimal characters (upper- or lowercase). Returns
 * NM_OK on success, NM_ERR_INVALID for malformed input.
 */
int nm_sha256_from_hex(const char *hex,
                       uint8_t out[NM_SHA256_LEN]);

/*
 * An object key is 1..127 ASCII characters from [A-Za-z0-9._-]. The special
 * names "." and ".." are rejected. Keys never become raw path components, but
 * the restricted alphabet keeps gateway routing unambiguous.
 */
bool nm_key_is_valid(const char *key);

/* SHA-256 of the key bytes, used as the storage handle and ACK digest. */
int nm_manifest_key_hash(const char *key, uint8_t out[NM_SHA256_LEN]);

/* ------------------------------------------------------------------------- */
/* Local store lifecycle                                                     */
/* ------------------------------------------------------------------------- */

/*
 * Create the root/chunks/objects directory tree. Idempotent. The path is
 * copied into the store and must fit NM_PATH_MAX.
 */
int nm_store_init(nm_store_t *store, const char *data_dir);

const char *nm_store_root(const nm_store_t *store);
const char *nm_store_chunks_dir(const nm_store_t *store);
const char *nm_store_objects_dir(const nm_store_t *store);

/* ------------------------------------------------------------------------- */
/* Chunk storage                                                             */
/* ------------------------------------------------------------------------- */

/*
 * Verify that data hashes to `hash`, then atomically write it. Length must be
 * in 1..NM_CHUNK_SIZE. A mismatched digest returns NM_ERR_HASH_MISMATCH and
 * writes nothing.
 */
int nm_store_put_chunk(nm_store_t *store,
                       const uint8_t hash[NM_SHA256_LEN],
                       const void *data, size_t length);

/*
 * Read and verify one chunk. On success returns NM_OK and a malloc'ed buffer
 * in *data with its size in *length; the caller owns the buffer.
 */
int nm_store_get_chunk(nm_store_t *store,
                       const uint8_t hash[NM_SHA256_LEN],
                       uint8_t **data, size_t *length);

/*
 * Report whether a chunk file exists. Returns NM_OK and sets *exists, or a
 * non-NM_OK status for a filesystem error. Malformed files (non-regular or
 * wrong size) count as absent.
 */
int nm_store_has_chunk(const nm_store_t *store,
                       const uint8_t hash[NM_SHA256_LEN],
                       bool *exists);

/*
 * List chunk digests currently present, sorted ascending. On success returns
 * NM_OK and a malloc'ed array in *hashes with *count entries. The caller owns
 * the array; for an empty store *hashes is NULL and *count is zero.
 */
int nm_store_list_chunks(const nm_store_t *store,
                         uint8_t **hashes, size_t *count);

/* ------------------------------------------------------------------------- */
/* Object manifest                                                           */
/* ------------------------------------------------------------------------- */

/*
 * Initialize a manifest and allocate its dynamically sized chunk hash array
 * (chunk_count * NM_SHA256_LEN bytes). total_size must be
 * <= NM_MAX_OBJECT_SIZE; chunk_count is derived (zero for an empty object).
 * object_hash starts as zero bytes and updated_ms is set to now. The caller
 * fills object_hash and each chunk hash, then validates/persists the manifest.
 * Validation rejects the all-zero object_hash placeholder, so an incomplete
 * manifest cannot be committed.
 */
int nm_manifest_init(nm_manifest_t *manifest, const char *key,
                     uint64_t total_size);

/*
 * Release dynamic members and clear the struct. Safe on a zeroed or already
 * freed manifest. `chunk_hashes` may be NULL.
 */
void nm_manifest_free(nm_manifest_t *manifest);

/*
 * Check key, bounds, total_size/chunk_count consistency and dynamic storage.
 * Returns NM_OK or NM_ERR_INVALID.
 */
int nm_manifest_validate(const nm_manifest_t *manifest);

/* Length of chunk `index` for a valid manifest; 0 if index is out of range. */
size_t nm_manifest_chunk_length(const nm_manifest_t *manifest, uint32_t index);

/*
 * Serialize/deserialize the versioned wire format documented in
 * netmirror.h. Serialization allocates *out; the caller owns it. Deserialization
 * allocates the manifest's chunk array; free it with nm_manifest_free().
 *
 * `out` must be zero-initialized (or previously freed) because deserialization
 * replaces it on success. Malformed input returns NM_ERR_PROTOCOL and leaves
 * `out` untouched.
 */
int nm_manifest_serialize(const nm_manifest_t *manifest,
                          uint8_t **out, size_t *out_length);
int nm_manifest_deserialize(const uint8_t *data, size_t length,
                            nm_manifest_t *out);

/*
 * Persist/load a manifest using sha256(key) as the file handle. Loading also
 * verifies that the manifest's embedded key matches the requested key.
 *
 * `out` must be zero-initialized (or previously freed): on success it receives
 * an allocated chunk array that the caller must release with
 * nm_manifest_free(). On failure `out` is left untouched.
 */
int nm_store_put_manifest(nm_store_t *store,
                          const nm_manifest_t *manifest);
int nm_store_get_manifest(nm_store_t *store, const char *key,
                          nm_manifest_t *out);

/*
 * List all valid object keys, sorted lexicographically for deterministic
 * anti-entropy pagination. On success returns NM_OK and a malloc'ed array of
 * malloc'ed NUL-terminated strings. The caller releases the whole result with
 * nm_store_free_keys(). For an empty store *keys is NULL and *count is zero.
 * Structurally invalid manifest files are skipped.
 */
int nm_store_list_keys(const nm_store_t *store, char ***keys, size_t *count);
void nm_store_free_keys(char **keys, size_t count);

/* ------------------------------------------------------------------------- */
/* File ingestion                                                            */
/* ------------------------------------------------------------------------- */

/*
 * Read `source_path` with bounded fread loops, split it into 4 MiB chunks,
 * store every chunk locally, then store the manifest as the commit marker.
 *
 * `key` identifies the object. If `out_manifest` is non-NULL it receives the
 * resulting manifest on success and must be freed by the caller; it must be
 * zero-initialized. On failure, already-stored chunks are left behind (no
 * manifest is committed).
 */
int nm_store_put_file(nm_store_t *store, const char *source_path,
                      const char *key, nm_manifest_t *out_manifest);

#ifdef __cplusplus
}
#endif

#endif /* NETMIRROR_BLOB_H */
