/*
 * netmirror - local content-addressable storage implementation
 *
 * Design notes:
 *  - No global mutable state; nm_store_t instances may be used concurrently
 *    as long as each thread does not reinitialize the same instance.
 *  - Every persistent object is installed with rename(2), so readers observe
 *    either the old complete file or the new complete file, never a partial.
 *  - Chunk integrity is checked before a write and after every read.
 *  - Every read/write loop handles EINTR and short transfers.
 */

#include "blob.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <openssl/evp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define NM_TMP_PATTERN ".nm-tmp-XXXXXX"

static const char NM_HEX_DIGITS[] = "0123456789abcdef";

/* ------------------------------------------------------------------------- */
/* Small helpers                                                             */
/* ------------------------------------------------------------------------- */

static void put_u16_be(uint8_t *out, uint16_t value)
{
    out[0] = (uint8_t)((value >> 8u) & 0xffu);
    out[1] = (uint8_t)(value & 0xffu);
}

static void put_u32_be(uint8_t *out, uint32_t value)
{
    for (unsigned int i = 0; i < 4u; ++i) {
        out[3u - i] = (uint8_t)(value & 0xffu);
        value >>= 8u;
    }
}

static void put_u64_be(uint8_t *out, uint64_t value)
{
    for (unsigned int i = 0; i < 8u; ++i) {
        out[7u - i] = (uint8_t)(value & 0xffu);
        value >>= 8u;
    }
}

static uint16_t get_u16_be(const uint8_t *in)
{
    return (uint16_t)(((uint16_t)in[0] << 8u) | (uint16_t)in[1]);
}

static uint32_t get_u32_be(const uint8_t *in)
{
    uint32_t value = 0;
    for (unsigned int i = 0; i < 4u; ++i) {
        value = (value << 8u) | (uint32_t)in[i];
    }
    return value;
}

static uint64_t get_u64_be(const uint8_t *in)
{
    uint64_t value = 0;
    for (unsigned int i = 0; i < 8u; ++i) {
        value = (value << 8u) | (uint64_t)in[i];
    }
    return value;
}

static int current_time_ms(uint64_t *out)
{
    struct timespec ts;

    if (out == NULL) {
        return NM_ERR_INVALID;
    }
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return NM_ERR_SYSTEM;
    }
    if (ts.tv_sec < 0) {
        return NM_ERR_SYSTEM;
    }
    *out = (uint64_t)ts.tv_sec * 1000u +
           (uint64_t)(ts.tv_nsec / 1000000L);
    return NM_OK;
}

static int join_path(char out[NM_PATH_MAX], const char *directory,
                     const char *name)
{
    int written;

    if (out == NULL || directory == NULL || name == NULL) {
        return NM_ERR_INVALID;
    }
    written = snprintf(out, NM_PATH_MAX, "%s/%s", directory, name);
    if (written < 0 || (size_t)written >= NM_PATH_MAX) {
        return NM_ERR_INVALID;
    }
    return NM_OK;
}

static int ensure_directory(const char *path)
{
    struct stat st;

    if (path == NULL || path[0] == '\0') {
        return NM_ERR_INVALID;
    }
    if (mkdir(path, 0750) == 0) {
        return NM_OK;
    }
    if (errno != EEXIST) {
        return NM_ERR_IO;
    }
    if (stat(path, &st) != 0) {
        return NM_ERR_IO;
    }
    if (!S_ISDIR(st.st_mode)) {
        return NM_ERR_IO;
    }
    return NM_OK;
}

static int mkdir_p(const char *path)
{
    char buffer[NM_PATH_MAX];
    size_t length;

    if (path == NULL || path[0] == '\0') {
        return NM_ERR_INVALID;
    }
    length = strnlen(path, NM_PATH_MAX);
    if (length == 0u || length >= NM_PATH_MAX) {
        return NM_ERR_INVALID;
    }
    memcpy(buffer, path, length + 1u);
    while (length > 1u && buffer[length - 1u] == '/') {
        buffer[--length] = '\0';
    }
    for (char *cursor = buffer + 1; *cursor != '\0'; ++cursor) {
        if (*cursor == '/') {
            *cursor = '\0';
            if (ensure_directory(buffer) != NM_OK) {
                return NM_ERR_IO;
            }
            *cursor = '/';
        }
    }
    return ensure_directory(buffer);
}

static bool store_is_initialized(const nm_store_t *store)
{
    return store != NULL && store->root[0] != '\0' &&
           store->chunks_dir[0] != '\0' && store->objects_dir[0] != '\0';
}

static int write_all_fd(int fd, const void *data, size_t length)
{
    const uint8_t *cursor = data;
    size_t remaining = length;

    if (fd < 0 || (data == NULL && length != 0u)) {
        return NM_ERR_INVALID;
    }
    while (remaining > 0u) {
        ssize_t written = write(fd, cursor, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return NM_ERR_IO;
        }
        if (written == 0) {
            return NM_ERR_IO;
        }
        cursor += (size_t)written;
        remaining -= (size_t)written;
    }
    return NM_OK;
}

static int read_exact_fd(int fd, void *data, size_t length)
{
    uint8_t *cursor = data;
    size_t remaining = length;

    if (fd < 0 || (data == NULL && length != 0u)) {
        return NM_ERR_INVALID;
    }
    while (remaining > 0u) {
        ssize_t received = read(fd, cursor, remaining);
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            return NM_ERR_IO;
        }
        if (received == 0) {
            return NM_ERR_IO;
        }
        cursor += (size_t)received;
        remaining -= (size_t)received;
    }
    return NM_OK;
}

static int fsync_directory(const char *directory)
{
    int fd;
    int result = NM_OK;

    fd = open(directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        return NM_ERR_IO;
    }
    if (fsync(fd) != 0) {
        result = NM_ERR_IO;
    }
    if (close(fd) != 0 && result == NM_OK) {
        result = NM_ERR_IO;
    }
    return result;
}

/*
 * Write a complete file atomically. The temporary file lives in the same
 * directory as the final path, guaranteeing that rename(2) cannot cross
 * filesystems. After rename, the parent directory is fsync'ed so the link is
 * durable as well as atomic.
 */
static int atomic_write_file(const char *directory, const char *final_path,
                             const void *data, size_t length)
{
    char temporary[NM_PATH_MAX];
    int fd = -1;
    int written;
    bool renamed = false;
    int result = NM_ERR_IO;

    if (directory == NULL || final_path == NULL ||
        (data == NULL && length != 0u)) {
        return NM_ERR_INVALID;
    }
    written = snprintf(temporary, sizeof(temporary), "%s/%s", directory,
                       NM_TMP_PATTERN);
    if (written < 0 || (size_t)written >= sizeof(temporary)) {
        return NM_ERR_INVALID;
    }

    fd = mkstemp(temporary);
    if (fd < 0) {
        return NM_ERR_IO;
    }

    int fd_flags = fcntl(fd, F_GETFD);
    if (fd_flags < 0 || fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) < 0) {
        goto cleanup;
    }
    if (write_all_fd(fd, data, length) != NM_OK) {
        goto cleanup;
    }
    if (fsync(fd) != 0) {
        goto cleanup;
    }
    if (close(fd) != 0) {
        fd = -1;
        goto cleanup;
    }
    fd = -1;
    if (rename(temporary, final_path) != 0) {
        goto cleanup;
    }
    renamed = true;

    result = fsync_directory(directory);
    if (result != NM_OK) {
        return result;
    }
    return NM_OK;

cleanup:
    if (fd >= 0) {
        (void)close(fd);
    }
    if (!renamed) {
        (void)unlink(temporary);
    }
    return result;
}

static int chunk_path(const nm_store_t *store,
                      const uint8_t hash[NM_SHA256_LEN],
                      char out[NM_PATH_MAX])
{
    char name[NM_SHA256_HEX_LEN + 1u];

    if (!store_is_initialized(store) || hash == NULL) {
        return NM_ERR_INVALID;
    }
    nm_sha256_to_hex(hash, name);
    return join_path(out, store->chunks_dir, name);
}

static int manifest_path(const nm_store_t *store, const char *key,
                         char out[NM_PATH_MAX])
{
    uint8_t key_hash[NM_SHA256_LEN];
    char name[NM_SHA256_HEX_LEN + sizeof(".manifest")];
    int result;
    int written;

    if (!store_is_initialized(store) || !nm_key_is_valid(key)) {
        return NM_ERR_INVALID;
    }
    result = nm_manifest_key_hash(key, key_hash);
    if (result != NM_OK) {
        return result;
    }
    nm_sha256_to_hex(key_hash, name);
    /*
     * nm_sha256_to_hex() only writes up to index 64. The rest of `name` is
     * initialized by the explicit extension copy below.
     */
    written = snprintf(name + NM_SHA256_HEX_LEN,
                       sizeof(name) - NM_SHA256_HEX_LEN, ".manifest");
    if (written < 0 ||
        (size_t)written >= sizeof(name) - NM_SHA256_HEX_LEN) {
        return NM_ERR_INVALID;
    }
    return join_path(out, store->objects_dir, name);
}

static int hash_vector_append(uint8_t **hashes, size_t *count, size_t *capacity,
                              const uint8_t hash[NM_SHA256_LEN])
{
    size_t new_capacity;
    uint8_t *new_hashes;

    if (hashes == NULL || count == NULL || capacity == NULL || hash == NULL) {
        return NM_ERR_INVALID;
    }
    if (*count == *capacity) {
        if (*capacity == 0u) {
            new_capacity = 8u;
        } else {
            if (*capacity > (SIZE_MAX / 2u)) {
                return NM_ERR_NOMEM;
            }
            new_capacity = *capacity * 2u;
        }
        if (new_capacity > (SIZE_MAX / NM_SHA256_LEN)) {
            return NM_ERR_NOMEM;
        }
        new_hashes = realloc(*hashes, new_capacity * NM_SHA256_LEN);
        if (new_hashes == NULL) {
            return NM_ERR_NOMEM;
        }
        *hashes = new_hashes;
        *capacity = new_capacity;
    }
    memcpy(*hashes + (*count * NM_SHA256_LEN), hash, NM_SHA256_LEN);
    *count += 1u;
    return NM_OK;
}

static int compare_hashes(const void *left, const void *right)
{
    return memcmp(left, right, NM_SHA256_LEN);
}

static bool hash_is_all_zero(const uint8_t hash[NM_SHA256_LEN])
{
    uint8_t accumulator = 0u;

    for (size_t i = 0u; i < NM_SHA256_LEN; ++i) {
        accumulator |= hash[i];
    }
    return accumulator == 0u;
}

/* ------------------------------------------------------------------------- */
/* SHA-256 and keys                                                          */
/* ------------------------------------------------------------------------- */

int nm_sha256_buf(const void *data, size_t length,
                  uint8_t out[NM_SHA256_LEN])
{
    EVP_MD_CTX *context;
    unsigned int digest_length = 0u;
    int result = NM_ERR_SYSTEM;

    if (out == NULL || (data == NULL && length != 0u)) {
        return NM_ERR_INVALID;
    }
    context = EVP_MD_CTX_new();
    if (context == NULL) {
        return NM_ERR_NOMEM;
    }
    if (EVP_DigestInit_ex(context, EVP_sha256(), NULL) != 1) {
        goto cleanup;
    }
    if (length > 0u && EVP_DigestUpdate(context, data, length) != 1) {
        goto cleanup;
    }
    if (EVP_DigestFinal_ex(context, out, &digest_length) != 1 ||
        digest_length != NM_SHA256_LEN) {
        goto cleanup;
    }
    result = NM_OK;

cleanup:
    EVP_MD_CTX_free(context);
    return result;
}

void nm_sha256_to_hex(const uint8_t hash[NM_SHA256_LEN],
                      char out[NM_SHA256_HEX_LEN + 1u])
{
    if (out == NULL || hash == NULL) {
        return;
    }
    for (size_t i = 0u; i < NM_SHA256_LEN; ++i) {
        out[2u * i] = NM_HEX_DIGITS[(hash[i] >> 4u) & 0x0fu];
        out[(2u * i) + 1u] = NM_HEX_DIGITS[hash[i] & 0x0fu];
    }
    out[NM_SHA256_HEX_LEN] = '\0';
}

static int hex_value(unsigned char character)
{
    if (character >= (unsigned char)'0' &&
        character <= (unsigned char)'9') {
        return (int)(character - (unsigned char)'0');
    }
    if (character >= (unsigned char)'a' &&
        character <= (unsigned char)'f') {
        return (int)(character - (unsigned char)'a') + 10;
    }
    if (character >= (unsigned char)'A' &&
        character <= (unsigned char)'F') {
        return (int)(character - (unsigned char)'A') + 10;
    }
    return -1;
}

int nm_sha256_from_hex(const char *hex, uint8_t out[NM_SHA256_LEN])
{
    uint8_t decoded[NM_SHA256_LEN];

    if (hex == NULL || out == NULL) {
        return NM_ERR_INVALID;
    }
    if (strnlen(hex, NM_SHA256_HEX_LEN + 1u) != NM_SHA256_HEX_LEN) {
        return NM_ERR_INVALID;
    }
    for (size_t i = 0u; i < NM_SHA256_LEN; ++i) {
        int high = hex_value((unsigned char)hex[2u * i]);
        int low = hex_value((unsigned char)hex[(2u * i) + 1u]);
        if (high < 0 || low < 0) {
            return NM_ERR_INVALID;
        }
        decoded[i] = (uint8_t)((high << 4) | low);
    }
    memcpy(out, decoded, sizeof(decoded));
    return NM_OK;
}

bool nm_key_is_valid(const char *key)
{
    size_t length;

    if (key == NULL) {
        return false;
    }
    length = strnlen(key, NM_MAX_KEY);
    if (length == 0u || length >= NM_MAX_KEY) {
        return false;
    }
    if (strcmp(key, ".") == 0 || strcmp(key, "..") == 0) {
        return false;
    }
    for (size_t i = 0u; i < length; ++i) {
        unsigned char character = (unsigned char)key[i];
        bool alpha = (character >= (unsigned char)'a' &&
                      character <= (unsigned char)'z') ||
                     (character >= (unsigned char)'A' &&
                      character <= (unsigned char)'Z');
        bool digit =
            character >= (unsigned char)'0' && character <= (unsigned char)'9';
        bool punctuation = character == (unsigned char)'.' ||
                           character == (unsigned char)'_' ||
                           character == (unsigned char)'-';
        if (!alpha && !digit && !punctuation) {
            return false;
        }
    }
    return true;
}

int nm_manifest_key_hash(const char *key, uint8_t out[NM_SHA256_LEN])
{
    size_t length;

    if (!nm_key_is_valid(key) || out == NULL) {
        return NM_ERR_INVALID;
    }
    length = strlen(key);
    return nm_sha256_buf(key, length, out);
}

/* ------------------------------------------------------------------------- */
/* Store lifecycle                                                           */
/* ------------------------------------------------------------------------- */

int nm_store_init(nm_store_t *store, const char *data_dir)
{
    char root[NM_PATH_MAX];
    char chunks[NM_PATH_MAX];
    char objects[NM_PATH_MAX];
    size_t length;

    if (store == NULL) {
        return NM_ERR_INVALID;
    }
    memset(store, 0, sizeof(*store));
    if (data_dir == NULL || data_dir[0] == '\0') {
        return NM_ERR_INVALID;
    }
    length = strnlen(data_dir, NM_PATH_MAX);
    if (length == 0u || length >= NM_PATH_MAX) {
        return NM_ERR_INVALID;
    }
    memcpy(root, data_dir, length + 1u);
    while (length > 1u && root[length - 1u] == '/') {
        root[--length] = '\0';
    }
    if (mkdir_p(root) != NM_OK) {
        return NM_ERR_IO;
    }
    if (join_path(chunks, root, "chunks") != NM_OK ||
        join_path(objects, root, "objects") != NM_OK) {
        return NM_ERR_INVALID;
    }
    if (ensure_directory(chunks) != NM_OK ||
        ensure_directory(objects) != NM_OK) {
        return NM_ERR_IO;
    }
    memcpy(store->root, root, strlen(root) + 1u);
    memcpy(store->chunks_dir, chunks, strlen(chunks) + 1u);
    memcpy(store->objects_dir, objects, strlen(objects) + 1u);
    return NM_OK;
}

const char *nm_store_root(const nm_store_t *store)
{
    return store != NULL ? store->root : NULL;
}

const char *nm_store_chunks_dir(const nm_store_t *store)
{
    return store != NULL ? store->chunks_dir : NULL;
}

const char *nm_store_objects_dir(const nm_store_t *store)
{
    return store != NULL ? store->objects_dir : NULL;
}

/* ------------------------------------------------------------------------- */
/* Chunk storage                                                             */
/* ------------------------------------------------------------------------- */

int nm_store_put_chunk(nm_store_t *store,
                       const uint8_t hash[NM_SHA256_LEN],
                       const void *data, size_t length)
{
    uint8_t actual[NM_SHA256_LEN];
    char path[NM_PATH_MAX];
    int result;

    if (!store_is_initialized(store) || hash == NULL || data == NULL) {
        return NM_ERR_INVALID;
    }
    if (length == 0u) {
        return NM_ERR_INVALID;
    }
    if (length > NM_CHUNK_SIZE) {
        return NM_ERR_TOO_LARGE;
    }
    result = nm_sha256_buf(data, length, actual);
    if (result != NM_OK) {
        return result;
    }
    if (memcmp(actual, hash, NM_SHA256_LEN) != 0) {
        return NM_ERR_HASH_MISMATCH;
    }
    result = chunk_path(store, hash, path);
    if (result != NM_OK) {
        return result;
    }
    return atomic_write_file(store->chunks_dir, path, data, length);
}

int nm_store_get_chunk(nm_store_t *store,
                       const uint8_t hash[NM_SHA256_LEN],
                       uint8_t **data, size_t *length)
{
    char path[NM_PATH_MAX];
    struct stat st;
    uint8_t *buffer = NULL;
    uint8_t actual[NM_SHA256_LEN];
    size_t file_length;
    int fd = -1;
    int result;

    if (data == NULL || length == NULL) {
        return NM_ERR_INVALID;
    }
    *data = NULL;
    *length = 0u;
    if (!store_is_initialized(store) || hash == NULL) {
        return NM_ERR_INVALID;
    }
    result = chunk_path(store, hash, path);
    if (result != NM_OK) {
        return result;
    }

    fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return errno == ENOENT ? NM_ERR_NOT_FOUND : NM_ERR_IO;
    }
    if (fstat(fd, &st) != 0) {
        result = NM_ERR_IO;
        goto cleanup;
    }
    if (!S_ISREG(st.st_mode) || st.st_size <= 0 ||
        (uint64_t)st.st_size > NM_CHUNK_SIZE) {
        result = NM_ERR_IO;
        goto cleanup;
    }
    file_length = (size_t)st.st_size;
    buffer = malloc(file_length);
    if (buffer == NULL) {
        result = NM_ERR_NOMEM;
        goto cleanup;
    }
    if (read_exact_fd(fd, buffer, file_length) != NM_OK) {
        result = NM_ERR_IO;
        goto cleanup;
    }
    result = nm_sha256_buf(buffer, file_length, actual);
    if (result != NM_OK) {
        goto cleanup;
    }
    if (memcmp(actual, hash, NM_SHA256_LEN) != 0) {
        result = NM_ERR_HASH_MISMATCH;
        goto cleanup;
    }
    if (close(fd) != 0) {
        fd = -1;
        result = NM_ERR_IO;
        goto cleanup;
    }
    fd = -1;
    *data = buffer;
    *length = file_length;
    return NM_OK;

cleanup:
    if (fd >= 0) {
        (void)close(fd);
    }
    free(buffer);
    return result;
}

int nm_store_has_chunk(const nm_store_t *store,
                       const uint8_t hash[NM_SHA256_LEN], bool *exists)
{
    char path[NM_PATH_MAX];
    struct stat st;
    int result;

    if (exists == NULL) {
        return NM_ERR_INVALID;
    }
    *exists = false;
    if (!store_is_initialized(store) || hash == NULL) {
        return NM_ERR_INVALID;
    }
    result = chunk_path(store, hash, path);
    if (result != NM_OK) {
        return result;
    }
    if (lstat(path, &st) != 0) {
        return errno == ENOENT ? NM_OK : NM_ERR_IO;
    }
    if (S_ISREG(st.st_mode) && st.st_size > 0 &&
        (uint64_t)st.st_size <= NM_CHUNK_SIZE) {
        *exists = true;
    }
    return NM_OK;
}

int nm_store_list_chunks(const nm_store_t *store, uint8_t **hashes,
                         size_t *count)
{
    DIR *directory = NULL;
    uint8_t *result_hashes = NULL;
    size_t result_count = 0u;
    size_t capacity = 0u;
    int result = NM_OK;

    if (hashes == NULL || count == NULL) {
        return NM_ERR_INVALID;
    }
    *hashes = NULL;
    *count = 0u;
    if (!store_is_initialized(store)) {
        return NM_ERR_INVALID;
    }

    directory = opendir(store->chunks_dir);
    if (directory == NULL) {
        return NM_ERR_IO;
    }

    for (;;) {
        struct dirent *entry;
        struct stat st;
        uint8_t hash[NM_SHA256_LEN];

        errno = 0;
        entry = readdir(directory);
        if (entry == NULL) {
            if (errno != 0) {
                result = NM_ERR_IO;
            }
            break;
        }
        if (entry->d_name[0] == '.' &&
            (entry->d_name[1] == '\0' ||
             (entry->d_name[1] == '.' && entry->d_name[2] == '\0'))) {
            continue;
        }
        if (strlen(entry->d_name) != NM_SHA256_HEX_LEN) {
            continue;
        }
        if (nm_sha256_from_hex(entry->d_name, hash) != NM_OK) {
            continue;
        }
        if (fstatat(dirfd(directory), entry->d_name, &st,
                    AT_SYMLINK_NOFOLLOW) != 0) {
            if (errno == ENOENT) {
                continue;
            }
            result = NM_ERR_IO;
            break;
        }
        if (!S_ISREG(st.st_mode) || st.st_size <= 0 ||
            (uint64_t)st.st_size > NM_CHUNK_SIZE) {
            continue;
        }
        result = hash_vector_append(&result_hashes, &result_count, &capacity,
                                    hash);
        if (result != NM_OK) {
            break;
        }
    }

    if (closedir(directory) != 0 && result == NM_OK) {
        result = NM_ERR_IO;
    }
    directory = NULL;
    if (result != NM_OK) {
        free(result_hashes);
        return result;
    }

    if (result_count > 1u) {
        qsort(result_hashes, result_count, NM_SHA256_LEN, compare_hashes);
        size_t unique = 1u;
        for (size_t i = 1u; i < result_count; ++i) {
            const uint8_t *candidate =
                result_hashes + (i * NM_SHA256_LEN);
            const uint8_t *previous =
                result_hashes + ((unique - 1u) * NM_SHA256_LEN);
            if (memcmp(candidate, previous, NM_SHA256_LEN) != 0) {
                if (unique != i) {
                    memmove(result_hashes + (unique * NM_SHA256_LEN),
                            candidate, NM_SHA256_LEN);
                }
                unique += 1u;
            }
        }
        result_count = unique;
    }

    *hashes = result_hashes;
    *count = result_count;
    return NM_OK;
}

/* ------------------------------------------------------------------------- */
/* Manifest                                                                  */
/* ------------------------------------------------------------------------- */

static int expected_chunk_count(uint64_t total_size, uint32_t *out)
{
    uint64_t chunks;

    if (out == NULL) {
        return NM_ERR_INVALID;
    }
    if (total_size > NM_MAX_OBJECT_SIZE) {
        return NM_ERR_TOO_LARGE;
    }
    if (total_size == 0u) {
        chunks = 0u;
    } else {
        chunks = (total_size + (uint64_t)NM_CHUNK_SIZE - 1u) /
                 (uint64_t)NM_CHUNK_SIZE;
    }
    if (chunks > NM_MAX_OBJECT_CHUNKS) {
        return NM_ERR_TOO_LARGE;
    }
    *out = (uint32_t)chunks;
    return NM_OK;
}

int nm_manifest_init(nm_manifest_t *manifest, const char *key,
                     uint64_t total_size)
{
    uint32_t expected = 0u;
    uint64_t now_ms = 0u;
    size_t key_length;
    uint8_t *hashes = NULL;
    int result;

    if (manifest == NULL || !nm_key_is_valid(key)) {
        return NM_ERR_INVALID;
    }
    memset(manifest, 0, sizeof(*manifest));
    result = expected_chunk_count(total_size, &expected);
    if (result != NM_OK) {
        return result;
    }
    result = current_time_ms(&now_ms);
    if (result != NM_OK) {
        return result;
    }
    if (expected > 0u) {
        hashes = calloc((size_t)expected, NM_SHA256_LEN);
        if (hashes == NULL) {
            return NM_ERR_NOMEM;
        }
    }
    key_length = strlen(key);
    memcpy(manifest->key, key, key_length + 1u);
    manifest->total_size = total_size;
    manifest->updated_ms = now_ms;
    manifest->chunk_count = expected;
    manifest->chunk_hashes = hashes;
    return NM_OK;
}

void nm_manifest_free(nm_manifest_t *manifest)
{
    if (manifest == NULL) {
        return;
    }
    free(manifest->chunk_hashes);
    memset(manifest, 0, sizeof(*manifest));
}

int nm_manifest_validate(const nm_manifest_t *manifest)
{
    uint32_t expected = 0u;
    size_t key_length;

    if (manifest == NULL) {
        return NM_ERR_INVALID;
    }
    key_length = strnlen(manifest->key, NM_MAX_KEY);
    if (key_length >= NM_MAX_KEY) {
        return NM_ERR_INVALID;
    }
    if (!nm_key_is_valid(manifest->key)) {
        return NM_ERR_INVALID;
    }
    if (expected_chunk_count(manifest->total_size, &expected) != NM_OK) {
        return NM_ERR_INVALID;
    }
    if (manifest->chunk_count != expected) {
        return NM_ERR_INVALID;
    }
    if (manifest->chunk_count > 0u && manifest->chunk_hashes == NULL) {
        return NM_ERR_INVALID;
    }
    /*
     * A real SHA-256 is never all-zero for any known input. Reject a
     * placeholder manifest before it can be persisted as committed state.
     */
    if (hash_is_all_zero(manifest->object_hash)) {
        return NM_ERR_INVALID;
    }
    return NM_OK;
}

size_t nm_manifest_chunk_length(const nm_manifest_t *manifest, uint32_t index)
{
    uint64_t offset;
    uint64_t remaining;

    if (manifest == NULL || index >= manifest->chunk_count) {
        return 0u;
    }
    offset = (uint64_t)index * (uint64_t)NM_CHUNK_SIZE;
    if (offset >= manifest->total_size) {
        return 0u;
    }
    remaining = manifest->total_size - offset;
    return remaining < (uint64_t)NM_CHUNK_SIZE ? (size_t)remaining
                                               : (size_t)NM_CHUNK_SIZE;
}

int nm_manifest_serialize(const nm_manifest_t *manifest, uint8_t **out,
                          size_t *out_length)
{
    size_t key_length;
    size_t total_length;
    size_t offset;
    uint8_t *buffer;

    if (out == NULL || out_length == NULL) {
        return NM_ERR_INVALID;
    }
    *out = NULL;
    *out_length = 0u;
    if (nm_manifest_validate(manifest) != NM_OK) {
        return NM_ERR_INVALID;
    }
    key_length = strlen(manifest->key);
    total_length = NM_MANIFEST_FIXED_HEADER_SIZE + key_length +
                   NM_MANIFEST_FIXED_TRAILER_SIZE +
                   ((size_t)manifest->chunk_count * NM_SHA256_LEN);
    if (total_length > NM_MAX_MANIFEST_SIZE) {
        return NM_ERR_TOO_LARGE;
    }
    buffer = malloc(total_length);
    if (buffer == NULL) {
        return NM_ERR_NOMEM;
    }

    put_u32_be(buffer, NM_MANIFEST_MAGIC);
    buffer[4] = (uint8_t)NM_MANIFEST_VERSION;
    buffer[5] = 0u;
    put_u16_be(buffer + 6, (uint16_t)key_length);
    memcpy(buffer + 8, manifest->key, key_length);

    offset = 8u + key_length;
    put_u64_be(buffer + offset, manifest->total_size);
    offset += 8u;
    put_u64_be(buffer + offset, manifest->updated_ms);
    offset += 8u;
    put_u32_be(buffer + offset, manifest->chunk_count);
    offset += 4u;
    memcpy(buffer + offset, manifest->object_hash, NM_SHA256_LEN);
    offset += NM_SHA256_LEN;
    if (manifest->chunk_count > 0u) {
        memcpy(buffer + offset, manifest->chunk_hashes,
               (size_t)manifest->chunk_count * NM_SHA256_LEN);
        offset += (size_t)manifest->chunk_count * NM_SHA256_LEN;
    }
    if (offset != total_length) {
        free(buffer);
        return NM_ERR_SYSTEM;
    }

    *out = buffer;
    *out_length = total_length;
    return NM_OK;
}

int nm_manifest_deserialize(const uint8_t *data, size_t length,
                            nm_manifest_t *out)
{
    nm_manifest_t parsed;
    uint32_t version;
    uint32_t reserved;
    uint16_t key_length;
    uint32_t chunk_count;
    uint32_t expected;
    uint64_t total_size;
    uint64_t updated_ms;
    size_t expected_length;
    uint8_t *hashes = NULL;

    if (data == NULL || out == NULL) {
        return NM_ERR_INVALID;
    }
    memset(&parsed, 0, sizeof(parsed));
    if (length < NM_MANIFEST_FIXED_HEADER_SIZE) {
        return NM_ERR_PROTOCOL;
    }
    if (get_u32_be(data) != NM_MANIFEST_MAGIC) {
        return NM_ERR_PROTOCOL;
    }
    version = data[4];
    reserved = data[5];
    key_length = get_u16_be(data + 6);
    if (version != NM_MANIFEST_VERSION || reserved != 0u ||
        key_length == 0u || key_length > NM_MAX_KEY_LEN) {
        return NM_ERR_PROTOCOL;
    }
    expected_length = NM_MANIFEST_FIXED_HEADER_SIZE + (size_t)key_length +
                      NM_MANIFEST_FIXED_TRAILER_SIZE;
    if (length < expected_length) {
        return NM_ERR_PROTOCOL;
    }
    total_size = get_u64_be(data + 8u + key_length);
    updated_ms = get_u64_be(data + 16u + key_length);
    chunk_count = get_u32_be(data + 24u + key_length);
    if (expected_chunk_count(total_size, &expected) != NM_OK ||
        chunk_count != expected) {
        return NM_ERR_PROTOCOL;
    }
    if ((size_t)chunk_count > (SIZE_MAX - expected_length) / NM_SHA256_LEN) {
        return NM_ERR_PROTOCOL;
    }
    expected_length += (size_t)chunk_count * NM_SHA256_LEN;
    if (length != expected_length) {
        return NM_ERR_PROTOCOL;
    }
    if (memchr(data + NM_MANIFEST_FIXED_HEADER_SIZE, '\0',
               (size_t)key_length) != NULL) {
        return NM_ERR_PROTOCOL;
    }
    memcpy(parsed.key, data + NM_MANIFEST_FIXED_HEADER_SIZE,
           (size_t)key_length);
    parsed.key[key_length] = '\0';
    if (!nm_key_is_valid(parsed.key)) {
        return NM_ERR_PROTOCOL;
    }
    if (hash_is_all_zero(data + 28u + key_length)) {
        return NM_ERR_PROTOCOL;
    }
    memcpy(parsed.object_hash, data + 28u + key_length, NM_SHA256_LEN);
    if (chunk_count > 0u) {
        hashes = malloc((size_t)chunk_count * NM_SHA256_LEN);
        if (hashes == NULL) {
            return NM_ERR_NOMEM;
        }
        memcpy(hashes, data + 60u + key_length,
               (size_t)chunk_count * NM_SHA256_LEN);
    }
    parsed.total_size = total_size;
    parsed.updated_ms = updated_ms;
    parsed.chunk_count = chunk_count;
    parsed.chunk_hashes = hashes;
    *out = parsed;
    return NM_OK;
}

int nm_store_put_manifest(nm_store_t *store,
                          const nm_manifest_t *manifest)
{
    char path[NM_PATH_MAX];
    uint8_t *serialized = NULL;
    size_t serialized_length = 0u;
    int result;

    if (!store_is_initialized(store) || manifest == NULL) {
        return NM_ERR_INVALID;
    }
    result = nm_manifest_validate(manifest);
    if (result != NM_OK) {
        return result;
    }
    result = manifest_path(store, manifest->key, path);
    if (result != NM_OK) {
        return result;
    }
    result = nm_manifest_serialize(manifest, &serialized, &serialized_length);
    if (result != NM_OK) {
        return result;
    }
    result = atomic_write_file(store->objects_dir, path, serialized,
                               serialized_length);
    free(serialized);
    return result;
}

static int load_manifest_file(const char *path, nm_manifest_t *out)
{
    struct stat st;
    uint8_t *serialized = NULL;
    nm_manifest_t decoded;
    size_t serialized_length;
    int fd;
    int result;

    memset(&decoded, 0, sizeof(decoded));
    fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return errno == ENOENT ? NM_ERR_NOT_FOUND : NM_ERR_IO;
    }
    if (fstat(fd, &st) != 0) {
        result = NM_ERR_IO;
        goto cleanup;
    }
    if (!S_ISREG(st.st_mode) || st.st_size <= 0 ||
        (uint64_t)st.st_size > NM_MAX_MANIFEST_SIZE) {
        result = NM_ERR_PROTOCOL;
        goto cleanup;
    }
    serialized_length = (size_t)st.st_size;
    serialized = malloc(serialized_length);
    if (serialized == NULL) {
        result = NM_ERR_NOMEM;
        goto cleanup;
    }
    if (read_exact_fd(fd, serialized, serialized_length) != NM_OK) {
        result = NM_ERR_IO;
        goto cleanup;
    }
    if (close(fd) != 0) {
        fd = -1;
        result = NM_ERR_IO;
        goto cleanup;
    }
    fd = -1;
    result = nm_manifest_deserialize(serialized, serialized_length, &decoded);
    if (result != NM_OK) {
        goto cleanup;
    }
    free(serialized);
    *out = decoded;
    return NM_OK;

cleanup:
    if (fd >= 0) {
        (void)close(fd);
    }
    free(serialized);
    return result;
}

int nm_store_get_manifest(nm_store_t *store, const char *key,
                          nm_manifest_t *out)
{
    char path[NM_PATH_MAX];
    nm_manifest_t decoded;
    int result;

    memset(&decoded, 0, sizeof(decoded));
    if (!store_is_initialized(store) || !nm_key_is_valid(key) || out == NULL) {
        return NM_ERR_INVALID;
    }
    result = manifest_path(store, key, path);
    if (result != NM_OK) {
        return result;
    }
    result = load_manifest_file(path, &decoded);
    if (result != NM_OK) {
        return result;
    }
    if (strcmp(decoded.key, key) != 0) {
        nm_manifest_free(&decoded);
        return NM_ERR_PROTOCOL;
    }
    *out = decoded;
    return NM_OK;
}

static int compare_strings(const void *left, const void *right)
{
    const char *const *left_string = left;
    const char *const *right_string = right;
    return strcmp(*left_string, *right_string);
}

static int key_vector_append(char ***keys, size_t *count, size_t *capacity,
                             const char *key)
{
    char *copy;
    char **new_keys;
    size_t new_capacity;

    if (keys == NULL || count == NULL || capacity == NULL || key == NULL) {
        return NM_ERR_INVALID;
    }
    if (*count == *capacity) {
        if (*capacity == 0u) {
            new_capacity = 8u;
        } else {
            if (*capacity > SIZE_MAX / 2u) {
                return NM_ERR_NOMEM;
            }
            new_capacity = *capacity * 2u;
        }
        if (new_capacity > SIZE_MAX / sizeof(char *)) {
            return NM_ERR_NOMEM;
        }
        new_keys = realloc(*keys, new_capacity * sizeof(char *));
        if (new_keys == NULL) {
            return NM_ERR_NOMEM;
        }
        *keys = new_keys;
        *capacity = new_capacity;
    }
    size_t key_length = strlen(key);
    copy = malloc(key_length + 1u);
    if (copy == NULL) {
        return NM_ERR_NOMEM;
    }
    memcpy(copy, key, key_length + 1u);
    (*keys)[*count] = copy;
    *count += 1u;
    return NM_OK;
}

int nm_store_list_keys(const nm_store_t *store, char ***keys, size_t *count)
{
    DIR *directory = NULL;
    char **result_keys = NULL;
    size_t result_count = 0u;
    size_t result_capacity = 0u;
    int result = NM_OK;

    if (keys == NULL || count == NULL) {
        return NM_ERR_INVALID;
    }
    *keys = NULL;
    *count = 0u;
    if (!store_is_initialized(store)) {
        return NM_ERR_INVALID;
    }
    directory = opendir(store->objects_dir);
    if (directory == NULL) {
        return NM_ERR_IO;
    }

    for (;;) {
        struct dirent *entry;
        struct stat st;
        char path[NM_PATH_MAX];
        char hash_text[NM_SHA256_HEX_LEN + 1u];
        uint8_t ignored_hash[NM_SHA256_LEN];
        nm_manifest_t manifest;
        size_t name_length;

        memset(&manifest, 0, sizeof(manifest));
        errno = 0;
        entry = readdir(directory);
        if (entry == NULL) {
            if (errno != 0) {
                result = NM_ERR_IO;
            }
            break;
        }
        name_length = strlen(entry->d_name);
        if (name_length != NM_SHA256_HEX_LEN + strlen(".manifest") ||
            strcmp(entry->d_name + NM_SHA256_HEX_LEN, ".manifest") != 0) {
            continue;
        }
        memcpy(hash_text, entry->d_name, NM_SHA256_HEX_LEN);
        hash_text[NM_SHA256_HEX_LEN] = '\0';
        if (nm_sha256_from_hex(hash_text, ignored_hash) != NM_OK) {
            continue;
        }
        if (fstatat(dirfd(directory), entry->d_name, &st,
                    AT_SYMLINK_NOFOLLOW) != 0 ||
            !S_ISREG(st.st_mode)) {
            continue;
        }
        if (join_path(path, store->objects_dir, entry->d_name) != NM_OK) {
            result = NM_ERR_IO;
            break;
        }
        if (load_manifest_file(path, &manifest) != NM_OK) {
            /* Ignore unreadable/corrupt manifest files during discovery. */
            continue;
        }
        result = key_vector_append(&result_keys, &result_count,
                                   &result_capacity, manifest.key);
        nm_manifest_free(&manifest);
        if (result != NM_OK) {
            break;
        }
    }

    if (closedir(directory) != 0 && result == NM_OK) {
        result = NM_ERR_IO;
    }
    directory = NULL;
    if (result != NM_OK) {
        nm_store_free_keys(result_keys, result_count);
        return result;
    }
    if (result_count > 1u) {
        qsort(result_keys, result_count, sizeof(char *), compare_strings);
    }
    *keys = result_keys;
    *count = result_count;
    return NM_OK;
}

void nm_store_free_keys(char **keys, size_t count)
{
    if (keys == NULL) {
        return;
    }
    for (size_t i = 0u; i < count; ++i) {
        free(keys[i]);
    }
    free(keys);
}

/* ------------------------------------------------------------------------- */
/* File ingestion                                                            */
/* ------------------------------------------------------------------------- */

int nm_store_put_file(nm_store_t *store, const char *source_path,
                      const char *key, nm_manifest_t *out_manifest)
{
    FILE *file = NULL;
    EVP_MD_CTX *object_digest = NULL;
    uint8_t *buffer = NULL;
    uint8_t *hashes = NULL;
    size_t hash_count = 0u;
    size_t hash_capacity = 0u;
    uint64_t total_size = 0u;
    uint8_t object_hash[NM_SHA256_LEN];
    nm_manifest_t manifest;
    bool manifest_allocated = false;
    int result = NM_OK;

    memset(&manifest, 0, sizeof(manifest));
    if (!store_is_initialized(store) || source_path == NULL ||
        source_path[0] == '\0' || !nm_key_is_valid(key)) {
        return NM_ERR_INVALID;
    }

    file = fopen(source_path, "rb");
    if (file == NULL) {
        return errno == ENOENT ? NM_ERR_NOT_FOUND : NM_ERR_IO;
    }
    buffer = malloc(NM_CHUNK_SIZE);
    if (buffer == NULL) {
        result = NM_ERR_NOMEM;
        goto cleanup;
    }
    object_digest = EVP_MD_CTX_new();
    if (object_digest == NULL) {
        result = NM_ERR_NOMEM;
        goto cleanup;
    }
    if (EVP_DigestInit_ex(object_digest, EVP_sha256(), NULL) != 1) {
        result = NM_ERR_SYSTEM;
        goto cleanup;
    }

    for (;;) {
        size_t filled = 0u;
        uint8_t chunk_hash[NM_SHA256_LEN];

        while (filled < NM_CHUNK_SIZE) {
            size_t received =
                fread(buffer + filled, 1u, NM_CHUNK_SIZE - filled, file);
            if (received > 0u) {
                filled += received;
            }
            if (received == 0u) {
                if (ferror(file)) {
                    result = NM_ERR_IO;
                    goto cleanup;
                }
                if (feof(file)) {
                    break;
                }
                /* No progress without EOF or error: refuse to spin. */
                result = NM_ERR_IO;
                goto cleanup;
            }
        }

        if (filled == 0u) {
            break;
        }
        if ((uint64_t)filled > NM_MAX_OBJECT_SIZE - total_size) {
            result = NM_ERR_TOO_LARGE;
            goto cleanup;
        }
        if (hash_count >= NM_MAX_OBJECT_CHUNKS) {
            result = NM_ERR_TOO_LARGE;
            goto cleanup;
        }
        if (EVP_DigestUpdate(object_digest, buffer, filled) != 1) {
            result = NM_ERR_SYSTEM;
            goto cleanup;
        }
        result = nm_sha256_buf(buffer, filled, chunk_hash);
        if (result != NM_OK) {
            goto cleanup;
        }
        result = hash_vector_append(&hashes, &hash_count, &hash_capacity,
                                    chunk_hash);
        if (result != NM_OK) {
            goto cleanup;
        }
        result = nm_store_put_chunk(store, chunk_hash, buffer, filled);
        if (result != NM_OK) {
            goto cleanup;
        }
        total_size += (uint64_t)filled;
        if (feof(file)) {
            break;
        }
    }

    if (fclose(file) != 0) {
        file = NULL;
        result = NM_ERR_IO;
        goto cleanup;
    }
    file = NULL;

    unsigned int digest_length = 0u;
    if (EVP_DigestFinal_ex(object_digest, object_hash, &digest_length) != 1 ||
        digest_length != NM_SHA256_LEN) {
        result = NM_ERR_SYSTEM;
        goto cleanup;
    }
    EVP_MD_CTX_free(object_digest);
    object_digest = NULL;

    result = nm_manifest_init(&manifest, key, total_size);
    if (result != NM_OK) {
        goto cleanup;
    }
    manifest_allocated = true;
    if (manifest.chunk_count != hash_count) {
        result = NM_ERR_SYSTEM;
        goto cleanup;
    }
    memcpy(manifest.object_hash, object_hash, NM_SHA256_LEN);
    if (hash_count > 0u) {
        memcpy(manifest.chunk_hashes, hashes, hash_count * NM_SHA256_LEN);
    }
    result = nm_store_put_manifest(store, &manifest);
    if (result != NM_OK) {
        goto cleanup;
    }

    if (out_manifest != NULL) {
        *out_manifest = manifest;
        memset(&manifest, 0, sizeof(manifest));
        manifest_allocated = false;
    }
    result = NM_OK;

cleanup:
    if (object_digest != NULL) {
        EVP_MD_CTX_free(object_digest);
    }
    if (file != NULL) {
        (void)fclose(file);
    }
    if (manifest_allocated) {
        nm_manifest_free(&manifest);
    }
    free(buffer);
    free(hashes);
    return result;
}
