/*
 * Unit tests for the local storage core.
 *
 * Built and run by `make test-core` using the sanitizer-enabled dev flags.
 * The test owns its temporary directory and removes it before returning.
 */

#include "blob.h"

#include <dirent.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static unsigned long checks_run;
static unsigned long checks_failed;

static void check_true_impl(int condition, const char *expression, int line)
{
    checks_run += 1u;
    if (!condition) {
        checks_failed += 1u;
        fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, line, expression);
    }
}

static void check_status_impl(int status, nm_status_t expected,
                              const char *expression, int line)
{
    checks_run += 1u;
    if (status != (int)expected) {
        checks_failed += 1u;
        fprintf(stderr, "FAIL %s:%d: %s returned %d, expected %d\n", __FILE__,
                line, expression, status, (int)expected);
    }
}

#define CHECK(condition) check_true_impl((condition) ? 1 : 0, #condition, __LINE__)
#define CHECK_STATUS(expression, expected)                                     \
    check_status_impl((expression), (expected), #expression, __LINE__)

static void fill_pattern(uint8_t *data, size_t length, unsigned int seed)
{
    uint32_t state = 0x9e3779b9u ^ seed;

    for (size_t i = 0u; i < length; ++i) {
        state = (state * 1664525u) + 1013904223u;
        data[i] = (uint8_t)((state >> 24u) & 0xffu);
    }
}

static bool write_file_content(const char *path, const uint8_t *data,
                               size_t length)
{
    FILE *file = fopen(path, "wb");
    size_t offset = 0u;

    if (file == NULL) {
        return false;
    }
    while (offset < length) {
        size_t request = length - offset;
        if (request > 65537u) {
            request = 65537u;
        }
        size_t written = fwrite(data + offset, 1u, request, file);
        if (written != request) {
            (void)fclose(file);
            return false;
        }
        offset += written;
    }
    if (fclose(file) != 0) {
        return false;
    }
    return true;
}

static void remove_tree(const char *path)
{
    struct stat st;
    DIR *directory;
    struct dirent *entry;

    if (lstat(path, &st) != 0) {
        return;
    }
    if (!S_ISDIR(st.st_mode)) {
        (void)unlink(path);
        return;
    }
    directory = opendir(path);
    if (directory != NULL) {
        while ((entry = readdir(directory)) != NULL) {
            char child[NM_PATH_MAX];
            int written;

            if (strcmp(entry->d_name, ".") == 0 ||
                strcmp(entry->d_name, "..") == 0) {
                continue;
            }
            written = snprintf(child, sizeof(child), "%s/%s", path,
                               entry->d_name);
            if (written > 0 && (size_t)written < sizeof(child)) {
                remove_tree(child);
            }
        }
        (void)closedir(directory);
    }
    (void)rmdir(path);
}

static bool directory_has_prefix(const char *path, const char *prefix)
{
    DIR *directory = opendir(path);
    struct dirent *entry;
    bool found = false;

    if (directory == NULL) {
        return true;
    }
    while ((entry = readdir(directory)) != NULL) {
        if (strncmp(entry->d_name, prefix, strlen(prefix)) == 0) {
            found = true;
            break;
        }
    }
    (void)closedir(directory);
    return found;
}

static void chunk_file_path(const nm_store_t *store,
                            const uint8_t hash[NM_SHA256_LEN],
                            char out[NM_PATH_MAX])
{
    char hex[NM_SHA256_HEX_LEN + 1u];

    nm_sha256_to_hex(hash, hex);
    (void)snprintf(out, NM_PATH_MAX, "%s/%s", nm_store_chunks_dir(store), hex);
}

static bool manifest_equal(const nm_manifest_t *left,
                           const nm_manifest_t *right)
{
    if (strcmp(left->key, right->key) != 0 ||
        left->total_size != right->total_size ||
        left->updated_ms != right->updated_ms ||
        left->chunk_count != right->chunk_count ||
        memcmp(left->object_hash, right->object_hash, NM_SHA256_LEN) != 0) {
        return false;
    }
    if (left->chunk_count > 0u) {
        if (left->chunk_hashes == NULL || right->chunk_hashes == NULL) {
            return false;
        }
        return memcmp(left->chunk_hashes, right->chunk_hashes,
                      (size_t)left->chunk_count * NM_SHA256_LEN) == 0;
    }
    return true;
}

static void test_hashing_and_keys(void)
{
    static const char abc_hex[] =
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    static const char empty_hex[] =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    uint8_t digest[NM_SHA256_LEN];
    uint8_t decoded[NM_SHA256_LEN];
    char hex[NM_SHA256_HEX_LEN + 1u];
    char invalid_hex[NM_SHA256_HEX_LEN + 1u];
    char long_key[129];

    CHECK_STATUS(nm_sha256_buf("abc", 3u, digest), NM_OK);
    nm_sha256_to_hex(digest, hex);
    CHECK(strcmp(hex, abc_hex) == 0);
    CHECK_STATUS(nm_sha256_from_hex(abc_hex, decoded), NM_OK);
    CHECK(memcmp(digest, decoded, NM_SHA256_LEN) == 0);

    CHECK_STATUS(nm_sha256_buf(NULL, 0u, digest), NM_OK);
    nm_sha256_to_hex(digest, hex);
    CHECK(strcmp(hex, empty_hex) == 0);
    CHECK_STATUS(nm_sha256_from_hex(empty_hex, decoded), NM_OK);
    CHECK(memcmp(digest, decoded, NM_SHA256_LEN) == 0);

    CHECK_STATUS(nm_sha256_from_hex("abcdef", decoded), NM_ERR_INVALID);
    memset(invalid_hex, '0', NM_SHA256_HEX_LEN);
    invalid_hex[0] = 'z';
    invalid_hex[NM_SHA256_HEX_LEN] = '\0';
    CHECK_STATUS(nm_sha256_from_hex(invalid_hex, decoded), NM_ERR_INVALID);

    CHECK(nm_key_is_valid("a"));
    CHECK(nm_key_is_valid("object-1_test.v2"));
    CHECK(!nm_key_is_valid(NULL));
    CHECK(!nm_key_is_valid(""));
    CHECK(!nm_key_is_valid("."));
    CHECK(!nm_key_is_valid(".."));
    CHECK(!nm_key_is_valid("with/slash"));
    CHECK(!nm_key_is_valid("with space"));
    CHECK(!nm_key_is_valid("unicode-\xc3\xa9"));

    memset(long_key, 'k', 127u);
    long_key[127] = '\0';
    CHECK(nm_key_is_valid(long_key));
    memset(long_key, 'k', 128u);
    long_key[128] = '\0';
    CHECK(!nm_key_is_valid(long_key));
    CHECK_STATUS(nm_manifest_key_hash("key", digest), NM_OK);
    CHECK_STATUS(nm_manifest_key_hash("bad/key", decoded), NM_ERR_INVALID);
}

static void test_chunk_store(nm_store_t *store)
{
    uint8_t first_data[1000];
    uint8_t second_data[777];
    uint8_t first_hash[NM_SHA256_LEN];
    uint8_t second_hash[NM_SHA256_LEN];
    uint8_t missing_hash[NM_SHA256_LEN];
    bool exists = true;
    uint8_t *output = NULL;
    size_t output_length = 0u;
    uint8_t *listed = NULL;
    size_t listed_count = 0u;
    uint8_t *oversized = NULL;

    fill_pattern(first_data, sizeof(first_data), 1u);
    fill_pattern(second_data, sizeof(second_data), 2u);
    CHECK_STATUS(nm_sha256_buf(first_data, sizeof(first_data), first_hash),
                 NM_OK);
    CHECK_STATUS(nm_sha256_buf(second_data, sizeof(second_data), second_hash),
                 NM_OK);
    CHECK_STATUS(nm_sha256_buf("missing", 7u, missing_hash), NM_OK);

    CHECK_STATUS(nm_store_has_chunk(store, first_hash, &exists), NM_OK);
    CHECK(!exists);
    CHECK_STATUS(nm_store_put_chunk(store, first_hash, first_data,
                                    sizeof(first_data)),
                 NM_OK);
    CHECK_STATUS(nm_store_has_chunk(store, first_hash, &exists), NM_OK);
    CHECK(exists);
    CHECK_STATUS(nm_store_get_chunk(store, first_hash, &output,
                                    &output_length),
                 NM_OK);
    CHECK(output_length == sizeof(first_data));
    CHECK(output != NULL);
    if (output != NULL) {
        CHECK(memcmp(output, first_data, sizeof(first_data)) == 0);
    }
    free(output);
    output = NULL;

    CHECK_STATUS(nm_store_put_chunk(store, second_hash, second_data,
                                    sizeof(second_data)),
                 NM_OK);
    CHECK_STATUS(nm_store_put_chunk(store, first_hash, first_data,
                                    sizeof(first_data)),
                 NM_OK);

    CHECK_STATUS(nm_store_get_chunk(store, missing_hash, &output,
                                    &output_length),
                 NM_ERR_NOT_FOUND);
    CHECK(output == NULL);
    CHECK(output_length == 0u);

    memset(missing_hash, 0, sizeof(missing_hash));
    CHECK_STATUS(nm_store_put_chunk(store, missing_hash, first_data,
                                    sizeof(first_data)),
                 NM_ERR_HASH_MISMATCH);
    CHECK_STATUS(nm_store_put_chunk(store, first_hash, first_data, 0u),
                 NM_ERR_INVALID);

    oversized = malloc(NM_CHUNK_SIZE + 1u);
    CHECK(oversized != NULL);
    if (oversized != NULL) {
        memset(oversized, 0, NM_CHUNK_SIZE + 1u);
        CHECK_STATUS(nm_store_put_chunk(store, first_hash, oversized,
                                        NM_CHUNK_SIZE + 1u),
                     NM_ERR_TOO_LARGE);
        free(oversized);
    }

    CHECK_STATUS(nm_store_list_chunks(store, &listed, &listed_count), NM_OK);
    CHECK(listed_count == 2u);
    if (listed_count == 2u) {
        CHECK(memcmp(listed, listed + NM_SHA256_LEN, NM_SHA256_LEN) < 0);
        CHECK((memcmp(listed, first_hash, NM_SHA256_LEN) == 0 &&
               memcmp(listed + NM_SHA256_LEN, second_hash, NM_SHA256_LEN) == 0) ||
              (memcmp(listed, second_hash, NM_SHA256_LEN) == 0 &&
               memcmp(listed + NM_SHA256_LEN, first_hash, NM_SHA256_LEN) ==
                   0));
    }
    free(listed);
    listed = NULL;

    CHECK_STATUS(nm_store_get_chunk(store, second_hash, &output,
                                    &output_length),
                 NM_OK);
    free(output);
    output = NULL;

    /*
     * Corrupt an installed chunk with same-length garbage: reads must reject
     * it rather than returning unauthenticated data.
     */
    {
        char path[NM_PATH_MAX];
        uint8_t corrupt[1000];

        fill_pattern(corrupt, sizeof(corrupt), 3u);
        chunk_file_path(store, first_hash, path);
        CHECK(write_file_content(path, corrupt, sizeof(corrupt)));
        CHECK_STATUS(nm_store_get_chunk(store, first_hash, &output,
                                        &output_length),
                     NM_ERR_HASH_MISMATCH);
        CHECK(output == NULL);
    }

    CHECK(!directory_has_prefix(nm_store_chunks_dir(store), ".nm-tmp-"));
    CHECK(!directory_has_prefix(nm_store_objects_dir(store), ".nm-tmp-"));
}

static void test_manifest_roundtrip(nm_store_t *store)
{
    const char *key = "manifest-roundtrip";
    const uint64_t total_size =
        ((uint64_t)NM_CHUNK_SIZE * 3u) + 1234u;
    nm_manifest_t manifest;
    nm_manifest_t decoded;
    nm_manifest_t loaded;
    uint8_t *serialized = NULL;
    size_t serialized_length = 0u;
    uint8_t *broken = NULL;
    uint8_t *extended = NULL;
    size_t key_length = strlen(key);

    memset(&manifest, 0, sizeof(manifest));
    memset(&decoded, 0, sizeof(decoded));
    memset(&loaded, 0, sizeof(loaded));

    CHECK_STATUS(nm_manifest_init(&manifest, key, total_size), NM_OK);
    CHECK(manifest.chunk_count == 4u);
    CHECK(manifest.total_size == total_size);
    CHECK(manifest.updated_ms > 0u);
    if (manifest.chunk_count == 4u && manifest.chunk_hashes != NULL) {
        for (uint32_t i = 0u; i < manifest.chunk_count; ++i) {
            char seed[32];
            int written = snprintf(seed, sizeof(seed), "chunk-%u", i);
            CHECK(written > 0 && (size_t)written < sizeof(seed));
            CHECK_STATUS(nm_sha256_buf(seed, (size_t)written,
                                       manifest.chunk_hashes +
                                           ((size_t)i * NM_SHA256_LEN)),
                         NM_OK);
        }
        CHECK_STATUS(nm_sha256_buf("object digits", 13u,
                                   manifest.object_hash),
                     NM_OK);
    }
    CHECK_STATUS(nm_manifest_validate(&manifest), NM_OK);
    CHECK(nm_manifest_chunk_length(&manifest, 0u) == NM_CHUNK_SIZE);
    CHECK(nm_manifest_chunk_length(&manifest, 2u) == NM_CHUNK_SIZE);
    CHECK(nm_manifest_chunk_length(&manifest, 3u) == 1234u);
    CHECK(nm_manifest_chunk_length(&manifest, 4u) == 0u);

    {
        uint8_t saved_object_hash[NM_SHA256_LEN];

        memcpy(saved_object_hash, manifest.object_hash, NM_SHA256_LEN);
        memset(manifest.object_hash, 0, NM_SHA256_LEN);
        CHECK_STATUS(nm_manifest_validate(&manifest), NM_ERR_INVALID);
        memcpy(manifest.object_hash, saved_object_hash, NM_SHA256_LEN);
    }

    CHECK_STATUS(nm_manifest_serialize(&manifest, &serialized,
                                       &serialized_length),
                 NM_OK);
    CHECK(serialized != NULL);
    CHECK(serialized_length ==
          NM_MANIFEST_FIXED_HEADER_SIZE + key_length +
              NM_MANIFEST_FIXED_TRAILER_SIZE +
              ((size_t)manifest.chunk_count * NM_SHA256_LEN));
    if (serialized != NULL) {
        CHECK(memcmp(serialized, "NMOB", 4u) == 0);
        CHECK(serialized[4] == (uint8_t)NM_MANIFEST_VERSION);
        CHECK(serialized[5] == 0u);
        CHECK(serialized[6] == 0u);
        CHECK(serialized[7] == (uint8_t)key_length);
        CHECK(memcmp(serialized + 8u, key, key_length) == 0);
        CHECK(memcmp(serialized + 28u + key_length, manifest.object_hash,
                     NM_SHA256_LEN) == 0);
        CHECK(memcmp(serialized + 60u + key_length, manifest.chunk_hashes,
                     (size_t)manifest.chunk_count * NM_SHA256_LEN) == 0);
    }

    CHECK_STATUS(nm_manifest_deserialize(serialized, serialized_length,
                                         &decoded),
                 NM_OK);
    CHECK(manifest_equal(&manifest, &decoded));
    CHECK_STATUS(nm_manifest_validate(&decoded), NM_OK);
    nm_manifest_free(&decoded);

    CHECK_STATUS(nm_store_put_manifest(store, &manifest), NM_OK);
    CHECK_STATUS(nm_store_get_manifest(store, key, &loaded), NM_OK);
    CHECK(manifest_equal(&manifest, &loaded));
    CHECK_STATUS(nm_store_get_manifest(store, "does-not-exist", &loaded),
                 NM_ERR_NOT_FOUND);

    if (serialized != NULL && serialized_length > 0u) {
        broken = malloc(serialized_length);
        extended = malloc(serialized_length + 1u);
        CHECK(broken != NULL);
        CHECK(extended != NULL);
        if (broken != NULL) {
            memcpy(broken, serialized, serialized_length);
            broken[0] ^= 0xffu;
            memset(&decoded, 0, sizeof(decoded));
            CHECK_STATUS(nm_manifest_deserialize(broken, serialized_length,
                                                 &decoded),
                         NM_ERR_PROTOCOL);
            nm_manifest_free(&decoded);

            memcpy(broken, serialized, serialized_length);
            broken[4] = (uint8_t)(NM_MANIFEST_VERSION + 1u);
            memset(&decoded, 0, sizeof(decoded));
            CHECK_STATUS(nm_manifest_deserialize(broken, serialized_length,
                                                 &decoded),
                         NM_ERR_PROTOCOL);
            nm_manifest_free(&decoded);

            memcpy(broken, serialized, serialized_length);
            broken[24u + key_length + 3u] = 0x7fu;
            memset(&decoded, 0, sizeof(decoded));
            CHECK_STATUS(nm_manifest_deserialize(broken, serialized_length,
                                                 &decoded),
                         NM_ERR_PROTOCOL);
            nm_manifest_free(&decoded);

            memcpy(broken, serialized, serialized_length);
            memset(broken + 28u + key_length, 0, NM_SHA256_LEN);
            memset(&decoded, 0, sizeof(decoded));
            CHECK_STATUS(nm_manifest_deserialize(broken, serialized_length,
                                                 &decoded),
                         NM_ERR_PROTOCOL);
            nm_manifest_free(&decoded);

            memset(&decoded, 0, sizeof(decoded));
            CHECK_STATUS(nm_manifest_deserialize(serialized,
                                                 serialized_length - 1u,
                                                 &decoded),
                         NM_ERR_PROTOCOL);
            nm_manifest_free(&decoded);
        }
        if (extended != NULL) {
            memcpy(extended, serialized, serialized_length);
            extended[serialized_length] = 0u;
            memset(&decoded, 0, sizeof(decoded));
            CHECK_STATUS(nm_manifest_deserialize(extended,
                                                 serialized_length + 1u,
                                                 &decoded),
                         NM_ERR_PROTOCOL);
            nm_manifest_free(&decoded);
        }
        free(broken);
        free(extended);
    }

    nm_manifest_free(&loaded);
    nm_manifest_free(&decoded);
    nm_manifest_free(&manifest);
    free(serialized);

    CHECK(!directory_has_prefix(nm_store_objects_dir(store), ".nm-tmp-"));
}

static void test_large_manifest_roundtrip(void)
{
    char key[NM_MAX_KEY];
    const uint64_t total_size = (uint64_t)NM_MAX_CHUNKS * NM_CHUNK_SIZE;
    nm_manifest_t manifest;
    nm_manifest_t decoded;
    uint8_t *serialized = NULL;
    size_t serialized_length = 0u;
    size_t expected_length;

    memset(key, 'k', NM_MAX_KEY_LEN);
    key[NM_MAX_KEY_LEN] = '\0';
    memset(&manifest, 0, sizeof(manifest));
    memset(&decoded, 0, sizeof(decoded));

    CHECK(NM_MAX_OBJECT_SIZE == total_size);
    CHECK(NM_MAX_CHUNKS <= UINT32_MAX);

    CHECK_STATUS(nm_manifest_init(&manifest, key, total_size), NM_OK);
    CHECK(manifest.chunk_count == NM_MAX_CHUNKS);
    CHECK(manifest.chunk_hashes != NULL);
    if (manifest.chunk_hashes != NULL) {
        for (uint32_t i = 0u; i < manifest.chunk_count; ++i) {
            uint8_t *hash =
                manifest.chunk_hashes + ((size_t)i * NM_SHA256_LEN);
            memset(hash, (int)(i & 0xffu), NM_SHA256_LEN);
            hash[1] = (uint8_t)((i >> 8u) & 0xffu);
            hash[2] = (uint8_t)((i >> 16u) & 0xffu);
        }
    }
    memset(manifest.object_hash, 0x5a, NM_SHA256_LEN);
    CHECK_STATUS(nm_manifest_validate(&manifest), NM_OK);

    expected_length = NM_MANIFEST_FIXED_HEADER_SIZE + strlen(key) +
                      NM_MANIFEST_FIXED_TRAILER_SIZE +
                      ((size_t)NM_MAX_CHUNKS * NM_SHA256_LEN);
    CHECK(expected_length == NM_MAX_MANIFEST_SIZE);
    CHECK_STATUS(nm_manifest_serialize(&manifest, &serialized,
                                       &serialized_length),
                 NM_OK);
    CHECK(serialized != NULL);
    CHECK(serialized_length == expected_length);
    CHECK(serialized_length == NM_MAX_MANIFEST_SIZE);
    CHECK(NM_MAX_MANIFEST_SIZE <= NM_MAX_FRAME_PAYLOAD);

    if (serialized != NULL) {
        CHECK_STATUS(nm_manifest_deserialize(serialized, serialized_length,
                                             &decoded),
                     NM_OK);
        CHECK(manifest_equal(&manifest, &decoded));
        CHECK(decoded.chunk_hashes != NULL);
        if (decoded.chunk_hashes != NULL) {
            CHECK(memcmp(decoded.chunk_hashes, manifest.chunk_hashes,
                         (size_t)NM_MAX_CHUNKS * NM_SHA256_LEN) == 0);
        }
    }

    nm_manifest_free(&decoded);
    nm_manifest_free(&manifest);
    free(serialized);

    CHECK_STATUS(nm_manifest_init(&manifest, "over-limit", total_size + 1u),
                 NM_ERR_TOO_LARGE);
    nm_manifest_free(&manifest);
}

static void test_empty_and_invalid_manifests(nm_store_t *store)
{
    static const char empty_hex[] =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    nm_manifest_t empty;
    nm_manifest_t loaded;
    uint8_t empty_hash[NM_SHA256_LEN];
    uint8_t *serialized = NULL;
    size_t serialized_length = 0u;

    memset(&empty, 0, sizeof(empty));
    memset(&loaded, 0, sizeof(loaded));
    CHECK_STATUS(nm_sha256_from_hex(empty_hex, empty_hash), NM_OK);
    CHECK_STATUS(nm_manifest_init(&empty, "empty-object", 0u), NM_OK);
    CHECK(empty.chunk_count == 0u);
    CHECK(empty.chunk_hashes == NULL);
    memcpy(empty.object_hash, empty_hash, NM_SHA256_LEN);
    CHECK_STATUS(nm_manifest_validate(&empty), NM_OK);
    CHECK_STATUS(nm_manifest_serialize(&empty, &serialized,
                                       &serialized_length),
                 NM_OK);
    CHECK_STATUS(nm_manifest_deserialize(serialized, serialized_length,
                                         &loaded),
                 NM_OK);
    CHECK(loaded.chunk_count == 0u);
    CHECK(loaded.chunk_hashes == NULL);
    CHECK(memcmp(loaded.object_hash, empty_hash, NM_SHA256_LEN) == 0);
    CHECK_STATUS(nm_store_put_manifest(store, &empty), NM_OK);
    nm_manifest_free(&loaded);
    CHECK_STATUS(nm_store_get_manifest(store, "empty-object", &loaded), NM_OK);
    CHECK(loaded.total_size == 0u);
    CHECK(loaded.chunk_count == 0u);
    nm_manifest_free(&loaded);
    nm_manifest_free(&empty);
    free(serialized);

    memset(&empty, 0, sizeof(empty));
    CHECK(nm_manifest_init(&empty, "too-big", NM_MAX_OBJECT_SIZE + 1u) !=
          NM_OK);
    nm_manifest_free(&empty);
    CHECK(nm_manifest_init(&empty, "bad/key", 1u) != NM_OK);
    CHECK(nm_manifest_init(&empty, "negative-size", UINT64_MAX) != NM_OK);
}

static void test_file_ingest(nm_store_t *store, const char *temporary_root)
{
    const size_t file_size = (NM_CHUNK_SIZE * 2u) + 777u;
    const char *key = "big-object";
    uint8_t *content = NULL;
    uint8_t expected_object_hash[NM_SHA256_LEN];
    uint8_t expected_chunk_hash[NM_SHA256_LEN];
    nm_manifest_t manifest;
    nm_manifest_t loaded;
    uint8_t *listed = NULL;
    size_t listed_count = 0u;
    char source_path[NM_PATH_MAX];
    char empty_path[NM_PATH_MAX];
    int written;

    memset(&manifest, 0, sizeof(manifest));
    memset(&loaded, 0, sizeof(loaded));
    content = malloc(file_size);
    CHECK(content != NULL);
    if (content == NULL) {
        return;
    }
    fill_pattern(content, file_size, 9u);
    CHECK_STATUS(nm_sha256_buf(content, file_size, expected_object_hash),
                 NM_OK);

    written = snprintf(source_path, sizeof(source_path), "%s/object.bin",
                       temporary_root);
    CHECK(written > 0 && (size_t)written < sizeof(source_path));
    CHECK(write_file_content(source_path, content, file_size));

    CHECK_STATUS(nm_store_put_file(store, source_path, key, &manifest), NM_OK);
    CHECK(manifest.total_size == file_size);
    CHECK(manifest.chunk_count == 3u);
    CHECK(manifest.updated_ms > 0u);
    CHECK(memcmp(manifest.object_hash, expected_object_hash, NM_SHA256_LEN) ==
          0);

    for (uint32_t i = 0u; i < manifest.chunk_count; ++i) {
        size_t offset = (size_t)i * NM_CHUNK_SIZE;
        size_t length = file_size - offset;
        uint8_t *read_back = NULL;
        size_t read_length = 0u;

        if (length > NM_CHUNK_SIZE) {
            length = NM_CHUNK_SIZE;
        }
        CHECK_STATUS(nm_sha256_buf(content + offset, length,
                                   expected_chunk_hash),
                     NM_OK);
        CHECK(manifest.chunk_hashes != NULL);
        if (manifest.chunk_hashes == NULL) {
            break;
        }
        CHECK(memcmp(manifest.chunk_hashes + ((size_t)i * NM_SHA256_LEN),
                     expected_chunk_hash, NM_SHA256_LEN) == 0);
        CHECK_STATUS(nm_store_get_chunk(
                         store,
                         manifest.chunk_hashes + ((size_t)i * NM_SHA256_LEN),
                         &read_back, &read_length),
                     NM_OK);
        CHECK(read_length == length);
        if (read_back != NULL) {
            CHECK(memcmp(read_back, content + offset, length) == 0);
        }
        free(read_back);
    }

    CHECK_STATUS(nm_store_get_manifest(store, key, &loaded), NM_OK);
    CHECK(manifest_equal(&manifest, &loaded));
    CHECK_STATUS(nm_store_list_chunks(store, &listed, &listed_count), NM_OK);
    CHECK(listed_count == 3u);
    free(listed);
    free(content);
    content = NULL;
    nm_manifest_free(&loaded);

    written = snprintf(empty_path, sizeof(empty_path), "%s/empty.bin",
                       temporary_root);
    CHECK(written > 0 && (size_t)written < sizeof(empty_path));
    CHECK(write_file_content(empty_path, NULL, 0u));
    CHECK_STATUS(nm_store_put_file(store, empty_path, "empty-file", &loaded),
                 NM_OK);
    CHECK(loaded.total_size == 0u);
    CHECK(loaded.chunk_count == 0u);
    CHECK(loaded.chunk_hashes == NULL);
    nm_manifest_free(&loaded);
    free(content);
    content = NULL;

    CHECK(nm_store_put_file(store, source_path, "bad key", NULL) ==
          NM_ERR_INVALID);
    CHECK(nm_store_put_file(store, "/nonexistent/netmirror/file", "missing",
                            NULL) == NM_ERR_NOT_FOUND);

    CHECK(!directory_has_prefix(nm_store_chunks_dir(store), ".nm-tmp-"));
    CHECK(!directory_has_prefix(nm_store_objects_dir(store), ".nm-tmp-"));
    nm_manifest_free(&manifest);
}

static void test_key_listing(nm_store_t *store)
{
    char **keys = NULL;
    size_t count = 0u;
    size_t matching = 0u;

    CHECK_STATUS(nm_store_list_keys(store, &keys, &count), NM_OK);
    CHECK(count >= 2u);
    for (size_t i = 0u; i < count; ++i) {
        if (i > 0u) {
            CHECK(strcmp(keys[i - 1u], keys[i]) <= 0);
        }
        if (strcmp(keys[i], "big-object") == 0 ||
            strcmp(keys[i], "empty-file") == 0) {
            matching += 1u;
        }
    }
    CHECK(matching == 2u);
    nm_store_free_keys(keys, count);
}

int main(void)
{
    char temporary[NM_PATH_MAX];
    char store_one_path[NM_PATH_MAX];
    char store_two_path[NM_PATH_MAX];
    const char *tmp_base = getenv("TMPDIR");
    nm_store_t store_one;
    nm_store_t store_two;
    int written;

    if (tmp_base == NULL || tmp_base[0] == '\0') {
        tmp_base = "/tmp";
    }
    written = snprintf(temporary, sizeof(temporary),
                       "%s/netmirror-blob-XXXXXX", tmp_base);
    if (written <= 0 || (size_t)written >= sizeof(temporary) ||
        mkdtemp(temporary) == NULL) {
        perror("mkdtemp");
        return EXIT_FAILURE;
    }

    test_hashing_and_keys();

    written = snprintf(store_one_path, sizeof(store_one_path), "%s/store-one",
                       temporary);
    CHECK(written > 0 && (size_t)written < sizeof(store_one_path));
    written = snprintf(store_two_path, sizeof(store_two_path), "%s/store-two",
                       temporary);
    CHECK(written > 0 && (size_t)written < sizeof(store_two_path));

    memset(&store_one, 0, sizeof(store_one));
    memset(&store_two, 0, sizeof(store_two));
    CHECK_STATUS(nm_store_init(&store_one, store_one_path), NM_OK);
    CHECK(nm_store_root(&store_one) != NULL);
    CHECK(nm_store_chunks_dir(&store_one) != NULL);
    CHECK(access(nm_store_root(&store_one), F_OK) == 0);
    CHECK_STATUS(nm_store_init(&store_two, store_two_path), NM_OK);

    test_chunk_store(&store_one);
    test_manifest_roundtrip(&store_one);
    test_large_manifest_roundtrip();
    test_empty_and_invalid_manifests(&store_one);
    test_file_ingest(&store_two, temporary);
    test_key_listing(&store_two);

    printf("test_blob: %lu checks, %lu failure(s)\n", checks_run,
           checks_failed);
    remove_tree(temporary);
    return checks_failed == 0u ? EXIT_SUCCESS : EXIT_FAILURE;
}
