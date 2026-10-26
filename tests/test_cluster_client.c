/*
 * Cluster integration client.
 *
 * Commands:
 *   write  <host> <port> <key> <size> <seed>
 *   verify <host> <port> <key> <size> <seed>
 *
 * The deterministic payload is derived from `seed`. write drives one object
 * through the node's asynchronous quorum path (one STORE per 4 MiB chunk plus
 * STORE_OBJECT). verify checks the manifest and every chunk on a given node.
 */

#include "blob.h"
#include "netmirror.h"
#include "network.h"
#include "util.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define CLIENT_TIMEOUT_MS 15000

typedef struct test_object {
    char key[NM_MAX_KEY];
    size_t size;
    uint32_t seed;
    uint8_t *data;
    nm_manifest_t manifest;
} test_object_t;

static void fill_pattern(uint8_t *data, size_t length, uint32_t seed)
{
    uint32_t state = 0x9e3779b9u ^ seed;

    for (size_t i = 0u; i < length; ++i) {
        state = (state * 1664525u) + 1013904223u;
        data[i] = (uint8_t)((state >> 24u) & 0xffu);
    }
}

static void object_free(test_object_t *object)
{
    if (object == NULL) {
        return;
    }
    nm_manifest_free(&object->manifest);
    free(object->data);
    memset(object, 0, sizeof(*object));
}

static bool object_build(test_object_t *object, const char *key, size_t size,
                         uint32_t seed)
{
    int result;

    memset(object, 0, sizeof(*object));
    if (!nm_key_is_valid(key) || size == 0u || size > NM_MAX_OBJECT_SIZE) {
        return false;
    }
    snprintf(object->key, sizeof(object->key), "%s", key);
    object->size = size;
    object->seed = seed;
    object->data = malloc(size);
    if (object->data == NULL) {
        return false;
    }
    fill_pattern(object->data, size, seed);
    result = nm_manifest_init(&object->manifest, key, size);
    if (result != NM_OK) {
        object_free(object);
        return false;
    }
    result = nm_sha256_buf(object->data, size, object->manifest.object_hash);
    if (result != NM_OK) {
        object_free(object);
        return false;
    }
    for (uint32_t i = 0u; i < object->manifest.chunk_count; ++i) {
        size_t offset = (size_t)i * NM_CHUNK_SIZE;
        size_t length = size - offset;
        if (length > NM_CHUNK_SIZE) {
            length = NM_CHUNK_SIZE;
        }
        result = nm_sha256_buf(object->data + offset, length,
                               object->manifest.chunk_hashes +
                                   ((size_t)i * NM_SHA256_LEN));
        if (result != NM_OK) {
            object_free(object);
            return false;
        }
    }
    return true;
}

static int connect_node(const char *host, uint16_t port)
{
    int fd = nm_tcp_connect(host, port, CLIENT_TIMEOUT_MS);
    if (fd < 0) {
        fprintf(stderr, "cannot connect to %s:%u\n", host,
                (unsigned int)port);
    }
    return fd;
}

static bool receive_frame(int fd, nm_frame_t *frame)
{
    int result = nm_frame_recv(fd, frame);
    if (result != NM_OK) {
        fprintf(stderr, "receive failed: %s\n", nm_status_name((nm_status_t)result));
        return false;
    }
    return true;
}

static bool decode_error_and_print(const nm_frame_t *frame,
                                   nm_status_t *status)
{
    char message[256];
    int result = nm_wire_error_decode(frame->payload, frame->length, status,
                                      message, sizeof(message));
    if (result != NM_OK) {
        fprintf(stderr, "malformed ERROR frame\n");
        return false;
    }
    fprintf(stderr, "server error: %s (%s)\n", nm_status_name(*status),
            message);
    return true;
}

static bool expect_ack(int fd, nm_msg_type_t request_type,
                       const uint8_t digest[NM_SHA256_LEN],
                       nm_status_t *status_out, bool *success)
{
    nm_frame_t frame;
    uint8_t ack_type = 0u;
    uint8_t ack_digest[NM_SHA256_LEN];
    nm_status_t status = NM_ERR_SYSTEM;

    memset(&frame, 0, sizeof(frame));
    if (!receive_frame(fd, &frame)) {
        return false;
    }
    if (frame.type == NM_MSG_ERROR) {
        bool decoded = decode_error_and_print(&frame, &status);
        nm_frame_free(&frame);
        if (!decoded) {
            return false;
        }
        *status_out = status;
        *success = false;
        return true;
    }
    if (frame.type != NM_MSG_ACK) {
        nm_frame_free(&frame);
        fprintf(stderr, "expected ACK, got message type 0x%02x\n",
                (unsigned int)frame.type);
        return false;
    }
    if (nm_wire_ack_decode(frame.payload, frame.length, &status, &ack_type,
                           ack_digest) != NM_OK) {
        nm_frame_free(&frame);
        fprintf(stderr, "malformed ACK\n");
        return false;
    }
    nm_frame_free(&frame);
    if (ack_type != (uint8_t)request_type ||
        memcmp(ack_digest, digest, NM_SHA256_LEN) != 0) {
        fprintf(stderr, "ACK does not match request\n");
        return false;
    }
    *status_out = status;
    *success = status == NM_OK;
    return true;
}

static bool command_write(const char *host, uint16_t port,
                          const test_object_t *object)
{
    int fd = -1;
    bool result = false;
    uint8_t *frame_payload = NULL;
    uint8_t *manifest_wire = NULL;
    size_t manifest_length = 0u;
    uint8_t key_digest[NM_SHA256_LEN];

    fd = connect_node(host, port);
    if (fd < 0) {
        return false;
    }
    frame_payload = malloc(NM_SHA256_LEN + NM_CHUNK_SIZE);
    if (frame_payload == NULL) {
        goto cleanup;
    }
    for (uint32_t i = 0u; i < object->manifest.chunk_count; ++i) {
        size_t offset = (size_t)i * NM_CHUNK_SIZE;
        size_t length = object->size - offset;
        nm_status_t status = NM_ERR_SYSTEM;
        bool success = false;

        if (length > NM_CHUNK_SIZE) {
            length = NM_CHUNK_SIZE;
        }
        memcpy(frame_payload,
               object->manifest.chunk_hashes + ((size_t)i * NM_SHA256_LEN),
               NM_SHA256_LEN);
        memcpy(frame_payload + NM_SHA256_LEN, object->data + offset, length);
        if (nm_frame_send(fd, NM_MSG_STORE, frame_payload,
                          (uint64_t)(NM_SHA256_LEN + length)) != NM_OK) {
            fprintf(stderr, "could not send STORE chunk %u\n", i);
            goto cleanup;
        }
        if (!expect_ack(fd, NM_MSG_STORE,
                        object->manifest.chunk_hashes +
                            ((size_t)i * NM_SHA256_LEN),
                        &status, &success)) {
            goto cleanup;
        }
        if (!success) {
            fprintf(stderr, "STORE chunk %u failed: %s\n", i,
                    nm_status_name(status));
            goto cleanup;
        }
    }
    if (nm_manifest_key_hash(object->key, key_digest) != NM_OK ||
        nm_manifest_serialize(&object->manifest, &manifest_wire,
                              &manifest_length) != NM_OK) {
        fprintf(stderr, "could not serialize manifest\n");
        goto cleanup;
    }
    if (nm_frame_send(fd, NM_MSG_STORE_OBJECT, manifest_wire,
                      (uint64_t)manifest_length) != NM_OK) {
        fprintf(stderr, "could not send STORE_OBJECT\n");
        goto cleanup;
    }
    nm_status_t manifest_status = NM_ERR_SYSTEM;
    bool manifest_success = false;
    if (!expect_ack(fd, NM_MSG_STORE_OBJECT, key_digest, &manifest_status,
                    &manifest_success)) {
        goto cleanup;
    }
    if (!manifest_success) {
        fprintf(stderr, "STORE_OBJECT failed: %s\n",
                nm_status_name(manifest_status));
        goto cleanup;
    }
    result = true;

cleanup:
    nm_close_fd(&fd);
    free(frame_payload);
    free(manifest_wire);
    return result;
}

static bool command_verify(const char *host, uint16_t port,
                           const test_object_t *object)
{
    int fd = -1;
    bool result = false;
    nm_manifest_t remote;
    nm_frame_t frame;
    uint8_t digest[NM_SHA256_LEN];

    memset(&remote, 0, sizeof(remote));
    memset(&frame, 0, sizeof(frame));
    fd = connect_node(host, port);
    if (fd < 0) {
        return false;
    }
    if (nm_frame_send(fd, NM_MSG_FETCH_OBJECT, object->key,
                      strlen(object->key)) != NM_OK) {
        fprintf(stderr, "could not send FETCH_OBJECT\n");
        goto cleanup;
    }
    if (!receive_frame(fd, &frame)) {
        goto cleanup;
    }
    if (frame.type != NM_MSG_OBJECT) {
        fprintf(stderr, "expected OBJECT, got 0x%02x\n",
                (unsigned int)frame.type);
        goto cleanup;
    }
    if (nm_manifest_deserialize(frame.payload, frame.length, &remote) !=
        NM_OK) {
        fprintf(stderr, "invalid manifest from server\n");
        goto cleanup;
    }
    nm_frame_free(&frame);
    memset(&frame, 0, sizeof(frame));
    if (strcmp(remote.key, object->key) != 0 ||
        remote.total_size != object->size ||
        remote.chunk_count != object->manifest.chunk_count ||
        memcmp(remote.object_hash, object->manifest.object_hash,
               NM_SHA256_LEN) != 0) {
        fprintf(stderr, "manifest metadata mismatch\n");
        goto cleanup;
    }
    for (uint32_t i = 0u; i < remote.chunk_count; ++i) {
        size_t offset = (size_t)i * NM_CHUNK_SIZE;
        size_t length = object->size - offset;

        if (length > NM_CHUNK_SIZE) {
            length = NM_CHUNK_SIZE;
        }
        memcpy(digest, remote.chunk_hashes + ((size_t)i * NM_SHA256_LEN),
               NM_SHA256_LEN);
        if (memcmp(digest,
                   object->manifest.chunk_hashes +
                       ((size_t)i * NM_SHA256_LEN),
                   NM_SHA256_LEN) != 0) {
            fprintf(stderr, "manifest chunk digest mismatch at %u\n", i);
            goto cleanup;
        }
        if (nm_frame_send(fd, NM_MSG_FETCH, digest, NM_SHA256_LEN) != NM_OK) {
            fprintf(stderr, "could not send FETCH\n");
            goto cleanup;
        }
        if (!receive_frame(fd, &frame)) {
            goto cleanup;
        }
        if (frame.type != NM_MSG_CHUNK ||
            frame.length != (uint64_t)(NM_SHA256_LEN + length) ||
            frame.payload == NULL ||
            memcmp(frame.payload, digest, NM_SHA256_LEN) != 0 ||
            memcmp(frame.payload + NM_SHA256_LEN, object->data + offset,
                   length) != 0) {
            fprintf(stderr, "chunk %u mismatch\n", i);
            goto cleanup;
        }
        nm_frame_free(&frame);
        memset(&frame, 0, sizeof(frame));
    }
    result = true;

cleanup:
    nm_manifest_free(&remote);
    nm_frame_free(&frame);
    nm_close_fd(&fd);
    return result;
}

static bool parse_size(const char *text, size_t *out)
{
    unsigned long long value;
    char *end = NULL;

    errno = 0;
    value = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value == 0u ||
        value > NM_MAX_OBJECT_SIZE || value > SIZE_MAX) {
        return false;
    }
    *out = (size_t)value;
    return true;
}

static bool parse_seed(const char *text, uint32_t *out)
{
    unsigned long value;
    char *end = NULL;

    errno = 0;
    value = strtoul(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value > UINT32_MAX) {
        return false;
    }
    *out = (uint32_t)value;
    return true;
}

int main(int argc, char **argv)
{
    size_t size;
    uint32_t seed;
    unsigned long port_value;
    char *end = NULL;
    test_object_t object;
    bool success;

    if (argc != 7) {
        fprintf(stderr,
                "usage: %s <write|verify> <host> <port> <key> <size> "
                "<seed>\n",
                argv[0]);
        return EXIT_FAILURE;
    }
    errno = 0;
    port_value = strtoul(argv[3], &end, 10);
    if (errno != 0 || end == argv[3] || *end != '\0' || port_value == 0u ||
        port_value > 65535u || !parse_size(argv[5], &size) ||
        !parse_seed(argv[6], &seed)) {
        fprintf(stderr, "invalid port, size or seed\n");
        return EXIT_FAILURE;
    }
    if (!object_build(&object, argv[4], size, seed)) {
        fprintf(stderr, "could not construct deterministic object\n");
        return EXIT_FAILURE;
    }
    if (strcmp(argv[1], "write") == 0) {
        success = command_write(argv[2], (uint16_t)port_value, &object);
    } else if (strcmp(argv[1], "verify") == 0) {
        success = command_verify(argv[2], (uint16_t)port_value, &object);
    } else {
        fprintf(stderr, "unknown command: %s\n", argv[1]);
        success = false;
    }
    object_free(&object);
    if (!success) {
        return EXIT_FAILURE;
    }
    printf("cluster %s OK: %s on %s:%s\n", argv[1], argv[4], argv[2],
           argv[3]);
    return EXIT_SUCCESS;
}
