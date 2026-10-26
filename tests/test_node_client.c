/*
 * Integration test client for the binary node server.
 *
 * Invoked by tests/test_node.sh after the node is accepting connections.
 * Exercises partial frames, partial writes through a 4 MiB store/fetch,
 * chunk integrity, manifests, sync pages and malformed input.
 */

#include "blob.h"
#include "netmirror.h"
#include "network.h"
#include "util.h"

#include <errno.h>
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define CLIENT_TIMEOUT_MS 10000
#define EXPECTED_NO_PACKET_MS 200

static bool fail(const char *message)
{
    fprintf(stderr, "FAIL: %s\n", message);
    return false;
}

static bool check_status(int actual, nm_status_t expected, const char *message)
{
    if (actual != (int)expected) {
        fprintf(stderr, "FAIL: %s (got %d, expected %d)\n", message, actual,
                (int)expected);
        return false;
    }
    return true;
}

static bool wait_without_response(int fd)
{
    struct pollfd descriptor;
    int result;

    descriptor.fd = fd;
    descriptor.events = POLLIN;
    descriptor.revents = 0;
    result = poll(&descriptor, 1, EXPECTED_NO_PACKET_MS);
    if (result == 0) {
        return true;
    }
    if (result < 0) {
        perror("poll");
        return false;
    }
    return fail("server responded before the frame was complete");
}

static bool client_ping(int fd)
{
    static const uint8_t nonce[8] = {'p', 'i', 'n', 'g', 'p', 'o', 'n', 'g'};
    nm_frame_t response;
    int status;

    memset(&response, 0, sizeof(response));
    status = nm_frame_send(fd, NM_MSG_PING, nonce, sizeof(nonce));
    if (!check_status(status, NM_OK, "send PING")) {
        return false;
    }
    status = nm_frame_recv(fd, &response);
    if (!check_status(status, NM_OK, "receive PONG")) {
        return false;
    }
    if (response.type != NM_MSG_PONG || response.length != sizeof(nonce) ||
        response.payload == NULL ||
        memcmp(response.payload, nonce, sizeof(nonce)) != 0) {
        nm_frame_free(&response);
        return fail("PONG did not echo the PING nonce");
    }
    nm_frame_free(&response);
    return true;
}

static bool client_pipelined_pings(int fd)
{
    static const uint8_t first[8] = {'p', 'i', 'p', 'e', '0', '0', '0', '1'};
    static const uint8_t second[8] = {'p', 'i', 'p', 'e', '0', '0', '0', '2'};
    const uint8_t *nonces[2] = {first, second};
    int status;

    for (size_t i = 0u; i < 2u; ++i) {
        status = nm_frame_send(fd, NM_MSG_PING, nonces[i], 8u);
        if (!check_status(status, NM_OK, "send pipelined PING")) {
            return false;
        }
    }
    for (size_t i = 0u; i < 2u; ++i) {
        nm_frame_t response;
        memset(&response, 0, sizeof(response));
        status = nm_frame_recv(fd, &response);
        if (!check_status(status, NM_OK, "receive pipelined PONG")) {
            return false;
        }
        if (response.type != NM_MSG_PONG || response.length != 8u ||
            response.payload == NULL ||
            memcmp(response.payload, nonces[i], 8u) != 0) {
            nm_frame_free(&response);
            return fail("pipelined PONG mismatch");
        }
        nm_frame_free(&response);
    }
    return true;
}

static bool expect_ack(int fd, nm_msg_type_t operation,
                       const uint8_t digest[NM_SHA256_LEN],
                       nm_status_t expected_status)
{
    nm_frame_t response;
    nm_status_t status = NM_ERR_SYSTEM;
    uint8_t acknowledged_type = 0u;
    uint8_t response_digest[NM_SHA256_LEN];
    int result;

    memset(&response, 0, sizeof(response));
    result = nm_frame_recv(fd, &response);
    if (!check_status(result, NM_OK, "receive ACK")) {
        return false;
    }
    if (response.type != NM_MSG_ACK) {
        nm_frame_free(&response);
        return fail("expected ACK frame");
    }
    result = nm_wire_ack_decode(response.payload, response.length, &status,
                                &acknowledged_type, response_digest);
    nm_frame_free(&response);
    if (!check_status(result, NM_OK, "decode ACK")) {
        return false;
    }
    if (status != expected_status) {
        fprintf(stderr, "FAIL: ACK status %s, expected %s\n",
                nm_status_name(status), nm_status_name(expected_status));
        return false;
    }
    if (acknowledged_type != (uint8_t)operation) {
        return fail("ACK operation type mismatch");
    }
    if (memcmp(digest, response_digest, NM_SHA256_LEN) != 0) {
        return fail("ACK digest mismatch");
    }
    return true;
}

static bool expect_error(int fd, nm_status_t expected_status)
{
    nm_frame_t response;
    nm_status_t status = NM_ERR_SYSTEM;
    char message[256];
    int result;

    memset(&response, 0, sizeof(response));
    result = nm_frame_recv(fd, &response);
    if (!check_status(result, NM_OK, "receive ERROR")) {
        return false;
    }
    if (response.type != NM_MSG_ERROR) {
        nm_frame_free(&response);
        return fail("expected ERROR frame");
    }
    result = nm_wire_error_decode(response.payload, response.length, &status,
                                  message, sizeof(message));
    nm_frame_free(&response);
    if (!check_status(result, NM_OK, "decode ERROR payload")) {
        return false;
    }
    if (status != expected_status) {
        fprintf(stderr, "FAIL: ERROR status %s, expected %s (%s)\n",
                nm_status_name(status), nm_status_name(expected_status),
                message);
        return false;
    }
    return true;
}

static bool store_chunk(int fd, const uint8_t digest[NM_SHA256_LEN],
                        const uint8_t *data, size_t length,
                        nm_status_t expected_status)
{
    uint8_t *payload;
    int status;

    payload = malloc(NM_SHA256_LEN + length);
    if (payload == NULL) {
        return fail("out of memory building STORE");
    }
    memcpy(payload, digest, NM_SHA256_LEN);
    memcpy(payload + NM_SHA256_LEN, data, length);
    status = nm_frame_send(fd, NM_MSG_STORE, payload,
                           (uint64_t)(NM_SHA256_LEN + length));
    free(payload);
    if (!check_status(status, NM_OK, "send STORE")) {
        return false;
    }
    return expect_ack(fd, NM_MSG_STORE, digest, expected_status);
}

static bool fetch_chunk(int fd, const uint8_t digest[NM_SHA256_LEN],
                        const uint8_t *expected, size_t expected_length)
{
    nm_frame_t response;
    int status;

    memset(&response, 0, sizeof(response));
    status = nm_frame_send(fd, NM_MSG_FETCH, digest, NM_SHA256_LEN);
    if (!check_status(status, NM_OK, "send FETCH")) {
        return false;
    }
    status = nm_frame_recv(fd, &response);
    if (!check_status(status, NM_OK, "receive CHUNK")) {
        return false;
    }
    if (response.type != NM_MSG_CHUNK ||
        response.length != NM_SHA256_LEN + expected_length ||
        response.payload == NULL) {
        nm_frame_free(&response);
        return fail("CHUNK framing mismatch");
    }
    if (memcmp(response.payload, digest, NM_SHA256_LEN) != 0 ||
        memcmp(response.payload + NM_SHA256_LEN, expected, expected_length) !=
            0) {
        nm_frame_free(&response);
        return fail("CHUNK content mismatch");
    }
    nm_frame_free(&response);
    return true;
}

static bool test_partial_store_and_fetch(int fd)
{
    uint8_t *chunk = NULL;
    uint8_t digest[NM_SHA256_LEN];
    uint8_t *store_payload = NULL;
    uint8_t *encoded = NULL;
    size_t encoded_length = 0u;
    size_t split_points[3];
    size_t cursor = 0u;
    int status = NM_OK;

    chunk = malloc(NM_CHUNK_SIZE);
    store_payload = malloc(NM_SHA256_LEN + NM_CHUNK_SIZE);
    if (chunk == NULL || store_payload == NULL) {
        fail("out of memory constructing 4 MiB chunk");
        free(chunk);
        free(store_payload);
        return false;
    }
    for (size_t i = 0u; i < NM_CHUNK_SIZE; ++i) {
        chunk[i] = (uint8_t)(((i * 131u) + 17u) & 0xffu);
    }
    status = nm_sha256_buf(chunk, NM_CHUNK_SIZE, digest);
    if (!check_status(status, NM_OK, "hash 4 MiB chunk")) {
        free(chunk);
        free(store_payload);
        return false;
    }
    memcpy(store_payload, digest, NM_SHA256_LEN);
    memcpy(store_payload + NM_SHA256_LEN, chunk, NM_CHUNK_SIZE);
    status = nm_frame_encode(
        NM_MSG_STORE, store_payload,
        (uint64_t)(NM_SHA256_LEN + NM_CHUNK_SIZE), &encoded, &encoded_length);
    free(store_payload);
    if (!check_status(status, NM_OK, "encode STORE frame")) {
        free(chunk);
        return false;
    }

    split_points[0] = 5u;
    split_points[1] = NM_FRAME_HEADER_SIZE;
    split_points[2] = 1u + NM_FRAME_HEADER_SIZE + 1024u;
    for (size_t i = 0u; i < 3u; ++i) {
        if (split_points[i] <= cursor) {
            continue;
        }
        status = nm_write_all(fd, encoded + cursor, split_points[i] - cursor);
        if (!check_status(status, NM_OK, "send partial STORE")) {
            free(chunk);
            free(encoded);
            return false;
        }
        cursor = split_points[i];
        if (!wait_without_response(fd)) {
            free(chunk);
            free(encoded);
            return false;
        }
    }
    status = nm_write_all(fd, encoded + cursor, encoded_length - cursor);
    free(encoded);
    if (!check_status(status, NM_OK, "send STORE tail")) {
        free(chunk);
        return false;
    }
    if (!expect_ack(fd, NM_MSG_STORE, digest, NM_OK)) {
        free(chunk);
        return false;
    }
    if (!fetch_chunk(fd, digest, chunk, NM_CHUNK_SIZE)) {
        free(chunk);
        return false;
    }
    free(chunk);
    return true;
}

static bool test_manifest(int fd, const uint8_t chunk_digest[NM_SHA256_LEN])
{
    const char *key = "stage3-object";
    nm_manifest_t manifest;
    nm_manifest_t loaded;
    uint8_t *wire = NULL;
    size_t wire_length = 0u;
    uint8_t key_digest[NM_SHA256_LEN];
    nm_frame_t response;
    int status;

    memset(&manifest, 0, sizeof(manifest));
    memset(&loaded, 0, sizeof(loaded));
    memset(&response, 0, sizeof(response));

    status = nm_manifest_init(&manifest, key, NM_CHUNK_SIZE);
    if (!check_status(status, NM_OK, "initialize manifest")) {
        return false;
    }
    memcpy(manifest.object_hash, chunk_digest, NM_SHA256_LEN);
    memcpy(manifest.chunk_hashes, chunk_digest, NM_SHA256_LEN);
    status = nm_manifest_serialize(&manifest, &wire, &wire_length);
    if (!check_status(status, NM_OK, "serialize manifest")) {
        nm_manifest_free(&manifest);
        return false;
    }
    status = nm_manifest_key_hash(key, key_digest);
    if (!check_status(status, NM_OK, "hash object key")) {
        nm_manifest_free(&manifest);
        free(wire);
        return false;
    }
    status = nm_frame_send(fd, NM_MSG_STORE_OBJECT, wire,
                           (uint64_t)wire_length);
    free(wire);
    if (!check_status(status, NM_OK, "send STORE_OBJECT")) {
        nm_manifest_free(&manifest);
        return false;
    }
    if (!expect_ack(fd, NM_MSG_STORE_OBJECT, key_digest, NM_OK)) {
        nm_manifest_free(&manifest);
        return false;
    }

    status = nm_frame_send(fd, NM_MSG_FETCH_OBJECT, key, strlen(key));
    if (!check_status(status, NM_OK, "send FETCH_OBJECT")) {
        nm_manifest_free(&manifest);
        return false;
    }
    status = nm_frame_recv(fd, &response);
    if (!check_status(status, NM_OK, "receive OBJECT")) {
        nm_manifest_free(&manifest);
        return false;
    }
    if (response.type != NM_MSG_OBJECT) {
        nm_frame_free(&response);
        nm_manifest_free(&manifest);
        return fail("expected OBJECT frame");
    }
    status = nm_manifest_deserialize(response.payload, response.length,
                                     &loaded);
    nm_frame_free(&response);
    if (!check_status(status, NM_OK, "decode OBJECT manifest")) {
        nm_manifest_free(&manifest);
        return false;
    }
    bool equal = strcmp(loaded.key, key) == 0 &&
                 loaded.total_size == NM_CHUNK_SIZE &&
                 loaded.chunk_count == 1u &&
                 memcmp(loaded.object_hash, chunk_digest, NM_SHA256_LEN) == 0 &&
                 loaded.chunk_hashes != NULL &&
                 memcmp(loaded.chunk_hashes, chunk_digest, NM_SHA256_LEN) == 0;
    nm_manifest_free(&loaded);
    nm_manifest_free(&manifest);
    return equal ? true : fail("OBJECT manifest round-trip mismatch");
}

static bool test_sync(int fd, const uint8_t chunk_digest[NM_SHA256_LEN])
{
    uint8_t request[NM_SYNC_REQ_WIRE_SIZE];
    uint8_t *hashes = NULL;
    size_t count = 0u;
    nm_frame_t response;
    uint32_t total;
    uint32_t returned;
    bool found = false;
    int status;

    memset(&response, 0, sizeof(response));
    nm_put_u32_be(request, 0u);
    nm_put_u32_be(request + 4u, NM_SYNC_DEFAULT_BATCH);
    status = nm_frame_send(fd, NM_MSG_SYNC_REQ, request, sizeof(request));
    if (!check_status(status, NM_OK, "send SYNC_REQ")) {
        return false;
    }
    status = nm_frame_recv(fd, &response);
    if (!check_status(status, NM_OK, "receive SYNC_RESP")) {
        return false;
    }
    if (response.type != NM_MSG_SYNC_RESP ||
        response.length < NM_SYNC_RESP_HEADER_SIZE ||
        response.payload == NULL) {
        nm_frame_free(&response);
        return fail("invalid SYNC_RESP frame");
    }
    total = nm_get_u32_be(response.payload);
    returned = nm_get_u32_be(response.payload + 4u);
    if (response.length != NM_SYNC_RESP_HEADER_SIZE +
                              ((uint64_t)returned * NM_SHA256_LEN) ||
        returned > total) {
        nm_frame_free(&response);
        return fail("inconsistent SYNC_RESP lengths");
    }
    if (returned > 0u) {
        hashes = malloc((size_t)returned * NM_SHA256_LEN);
        if (hashes == NULL) {
            nm_frame_free(&response);
            return fail("out of memory copying SYNC_RESP");
        }
        memcpy(hashes, response.payload + NM_SYNC_RESP_HEADER_SIZE,
               (size_t)returned * NM_SHA256_LEN);
        count = returned;
    }
    nm_frame_free(&response);
    for (size_t i = 0u; i < count; ++i) {
        if (memcmp(hashes + (i * NM_SHA256_LEN), chunk_digest,
                   NM_SHA256_LEN) == 0) {
            found = true;
            break;
        }
    }
    free(hashes);
    if (!found) {
        return fail("stored chunk missing from sync index");
    }
    return true;
}

static bool test_bad_hash_and_missing(int fd)
{
    static const uint8_t garbage[] = "this is not the expected content";
    uint8_t expected_digest[NM_SHA256_LEN];
    uint8_t missing_digest[NM_SHA256_LEN];
    int status;

    status = nm_sha256_buf("expected content", strlen("expected content"),
                           expected_digest);
    if (!check_status(status, NM_OK, "hash expected content")) {
        return false;
    }
    if (!store_chunk(fd, expected_digest, garbage, sizeof(garbage) - 1u,
                     NM_ERR_HASH_MISMATCH)) {
        return false;
    }
    if (!client_ping(fd)) {
        return false;
    }

    status = nm_sha256_buf("definitely missing chunk",
                           strlen("definitely missing chunk"), missing_digest);
    if (!check_status(status, NM_OK, "hash missing chunk")) {
        return false;
    }
    status = nm_frame_send(fd, NM_MSG_FETCH, missing_digest,
                           NM_SHA256_LEN);
    if (!check_status(status, NM_OK, "send missing FETCH")) {
        return false;
    }
    return expect_error(fd, NM_ERR_NOT_FOUND);
}

static bool test_slow_client_does_not_block(const char *host, uint16_t port)
{
    static const uint8_t nonce[8] = {'s', 'l', 'o', 'w', 'c', 'l', 'i', 't'};
    int slow_fd = -1;
    int fast_fd = -1;
    uint8_t *encoded = NULL;
    size_t encoded_length = 0u;
    nm_frame_t response;
    bool result = false;
    int status;

    slow_fd = nm_tcp_connect(host, port, CLIENT_TIMEOUT_MS);
    fast_fd = nm_tcp_connect(host, port, CLIENT_TIMEOUT_MS);
    if (slow_fd < 0 || fast_fd < 0) {
        fail("could not open two concurrent connections");
        goto cleanup;
    }
    status = nm_frame_encode(NM_MSG_PING, nonce, sizeof(nonce), &encoded,
                             &encoded_length);
    if (!check_status(status, NM_OK, "encode partial PING")) {
        goto cleanup;
    }
    status = nm_write_all(slow_fd, encoded, 5u);
    if (!check_status(status, NM_OK, "send partial PING header")) {
        goto cleanup;
    }

    /* A stalled connection must not prevent service on another connection. */
    if (!client_ping(fast_fd)) {
        goto cleanup;
    }

    status = nm_write_all(slow_fd, encoded + 5u, encoded_length - 5u);
    if (!check_status(status, NM_OK, "send remaining partial PING")) {
        goto cleanup;
    }
    memset(&response, 0, sizeof(response));
    status = nm_frame_recv(slow_fd, &response);
    if (!check_status(status, NM_OK, "receive resumed PONG")) {
        goto cleanup;
    }
    if (response.type != NM_MSG_PONG || response.length != sizeof(nonce) ||
        response.payload == NULL ||
        memcmp(response.payload, nonce, sizeof(nonce)) != 0) {
        nm_frame_free(&response);
        fail("resumed PONG mismatch");
        goto cleanup;
    }
    nm_frame_free(&response);
    result = true;

cleanup:
    free(encoded);
    nm_close_fd(&slow_fd);
    nm_close_fd(&fast_fd);
    return result;
}

static bool test_oversized_header(const char *host, uint16_t port)
{
    int fd;
    uint8_t header[NM_FRAME_HEADER_SIZE];
    nm_frame_t response;
    nm_status_t status = NM_ERR_SYSTEM;
    char message[128];
    int result;

    fd = nm_tcp_connect(host, port, CLIENT_TIMEOUT_MS);
    if (fd < 0) {
        return fail("could not connect for oversized-frame test");
    }
    nm_frame_header_encode(header, NM_MSG_FETCH,
                           NM_MAX_FRAME_PAYLOAD + 1u);
    result = nm_write_all(fd, header, sizeof(header));
    if (!check_status(result, NM_OK, "send oversized header")) {
        nm_close_fd(&fd);
        return false;
    }
    memset(&response, 0, sizeof(response));
    result = nm_frame_recv(fd, &response);
    if (!check_status(result, NM_OK, "receive oversized ERROR")) {
        nm_close_fd(&fd);
        return false;
    }
    if (response.type != NM_MSG_ERROR) {
        nm_frame_free(&response);
        nm_close_fd(&fd);
        return fail("expected ERROR for oversized frame");
    }
    result = nm_wire_error_decode(response.payload, response.length, &status,
                                  message, sizeof(message));
    nm_frame_free(&response);
    nm_close_fd(&fd);
    if (!check_status(result, NM_OK, "decode oversized ERROR")) {
        return false;
    }
    return status == NM_ERR_PROTOCOL ||
                   status == NM_ERR_TOO_LARGE
               ? true
               : fail("unexpected status for oversized frame");
}

static bool run_tests(const char *host, uint16_t port)
{
    int fd = -1;
    int probe_fd = -1;
    bool result = false;

    fd = nm_tcp_connect(host, port, CLIENT_TIMEOUT_MS);
    if (fd < 0) {
        return fail("could not connect to node");
    }
    if (!client_ping(fd)) {
        goto cleanup;
    }
    if (!client_pipelined_pings(fd)) {
        goto cleanup;
    }
    if (!test_partial_store_and_fetch(fd)) {
        goto cleanup;
    }
    if (!test_bad_hash_and_missing(fd)) {
        goto cleanup;
    }

    /*
     * Re-derive the 4 MiB chunk digest for the manifest and sync checks. The
     * STORE test proved it is retrievable; hashing it again keeps helpers
     * independent.
     */
    uint8_t *content = malloc(NM_CHUNK_SIZE);
    if (content == NULL) {
        fail("out of memory reconstructing chunk");
        goto cleanup;
    }
    for (size_t i = 0u; i < NM_CHUNK_SIZE; ++i) {
        content[i] = (uint8_t)(((i * 131u) + 17u) & 0xffu);
    }
    uint8_t digest[NM_SHA256_LEN];
    if (!check_status(nm_sha256_buf(content, NM_CHUNK_SIZE, digest), NM_OK,
                      "recompute chunk digest")) {
        free(content);
        goto cleanup;
    }
    free(content);

    if (!test_manifest(fd, digest)) {
        goto cleanup;
    }
    if (!test_sync(fd, digest)) {
        goto cleanup;
    }
    if (!client_ping(fd)) {
        goto cleanup;
    }
    if (!test_oversized_header(host, port)) {
        goto cleanup;
    }
    if (!test_slow_client_does_not_block(host, port)) {
        goto cleanup;
    }
    probe_fd = nm_tcp_connect(host, port, CLIENT_TIMEOUT_MS);
    if (probe_fd < 0) {
        fail("could not reconnect after malformed-frame close");
        goto cleanup;
    }
    if (!client_ping(probe_fd)) {
        goto cleanup;
    }
    result = true;

cleanup:
    nm_close_fd(&probe_fd);
    nm_close_fd(&fd);
    return result;
}

int main(int argc, char **argv)
{
    char *end = NULL;
    unsigned long port_value;

    if (argc != 3) {
        fprintf(stderr, "usage: %s <host> <port>\n", argv[0]);
        return EXIT_FAILURE;
    }
    port_value = strtoul(argv[2], &end, 10);
    if (errno != 0 || end == argv[2] || *end != '\0' || port_value == 0u ||
        port_value > 65535u) {
        fprintf(stderr, "invalid port: %s\n", argv[2]);
        return EXIT_FAILURE;
    }
    if (!run_tests(argv[1], (uint16_t)port_value)) {
        return EXIT_FAILURE;
    }
    printf("test_node_client: all integration checks passed\n");
    return EXIT_SUCCESS;
}
