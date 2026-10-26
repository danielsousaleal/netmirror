/*
 * Unit tests for framing, the partial-frame state machine, output buffering
 * and wire payload helpers. Server behavior is covered by test_node.sh.
 */

#include "netmirror.h"
#include "network.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
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

static void test_byte_order(void)
{
    uint8_t bytes[8];
    uint8_t header[NM_FRAME_HEADER_SIZE];
    uint8_t type = 0u;
    uint64_t length = 0u;

    nm_put_u16_be(bytes, 0x1234u);
    CHECK(bytes[0] == 0x12u && bytes[1] == 0x34u);
    CHECK(nm_get_u16_be(bytes) == 0x1234u);
    nm_put_u32_be(bytes, 0x89abcdefu);
    CHECK(bytes[0] == 0x89u && bytes[3] == 0xefu);
    CHECK(nm_get_u32_be(bytes) == 0x89abcdefu);
    nm_put_u64_be(bytes, UINT64_C(0x0123456789abcdef));
    CHECK(bytes[0] == 0x01u && bytes[7] == 0xefu);
    CHECK(nm_get_u64_be(bytes) == UINT64_C(0x0123456789abcdef));

    CHECK_STATUS(nm_frame_header_encode(header, 0xa5u,
                                        UINT64_C(0x0102030405060708)),
                 NM_OK);
    CHECK(header[0] == 0xa5u);
    CHECK(header[1] == 0x01u);
    CHECK(header[8] == 0x08u);
    CHECK_STATUS(nm_frame_header_decode(header, &type, &length), NM_OK);
    CHECK(type == 0xa5u);
    CHECK(length == UINT64_C(0x0102030405060708));
}

static void test_endpoint_parse(void)
{
    nm_endpoint_t endpoint;

    CHECK_STATUS(nm_endpoint_parse("node1:7001", &endpoint), NM_OK);
    CHECK(strcmp(endpoint.host, "node1") == 0);
    CHECK(endpoint.port == 7001u);
    CHECK_STATUS(nm_endpoint_parse("[::1]:8080", &endpoint), NM_OK);
    CHECK(strcmp(endpoint.host, "::1") == 0);
    CHECK(endpoint.port == 8080u);
    CHECK(nm_endpoint_parse("missing-port", &endpoint) != NM_OK);
    CHECK(nm_endpoint_parse("host:0", &endpoint) != NM_OK);
    CHECK(nm_endpoint_parse("host:65536", &endpoint) != NM_OK);
    CHECK(nm_endpoint_parse(":7001", &endpoint) != NM_OK);
    CHECK(nm_endpoint_parse("a:b:7001", &endpoint) != NM_OK);
    CHECK(nm_endpoint_parse("[::1]7001", &endpoint) != NM_OK);
}

static void test_full_frame(void)
{
    static const uint8_t payload[] = {1u, 2u, 3u, 0u, 255u};
    uint8_t *encoded = NULL;
    size_t encoded_length = 0u;
    uint8_t type = 0u;
    uint64_t payload_length = 0u;

    CHECK_STATUS(nm_frame_encode(NM_MSG_PING, payload, sizeof(payload),
                                 &encoded, &encoded_length),
                 NM_OK);
    CHECK(encoded != NULL);
    CHECK(encoded_length == NM_FRAME_HEADER_SIZE + sizeof(payload));
    if (encoded != NULL) {
        CHECK(encoded[0] == (uint8_t)NM_MSG_PING);
        CHECK_STATUS(nm_frame_header_decode(encoded, &type, &payload_length),
                     NM_OK);
        CHECK(type == (uint8_t)NM_MSG_PING);
        CHECK(payload_length == sizeof(payload));
        CHECK(memcmp(encoded + NM_FRAME_HEADER_SIZE, payload,
                     sizeof(payload)) == 0);
    }
    free(encoded);
    encoded = NULL;
    encoded_length = 0u;

    CHECK_STATUS(nm_frame_encode(NM_MSG_PING, NULL, 0u, &encoded,
                                 &encoded_length),
                 NM_OK);
    CHECK(encoded_length == NM_FRAME_HEADER_SIZE);
    free(encoded);

    encoded = NULL;
    CHECK_STATUS(nm_frame_encode(NM_MSG_STORE, payload,
                                 NM_MAX_FRAME_PAYLOAD + 1u, &encoded,
                                 &encoded_length),
                 NM_ERR_TOO_LARGE);
    CHECK(encoded == NULL);
    CHECK_STATUS(nm_frame_encode(NM_MSG_STORE, NULL, 4u, &encoded,
                                 &encoded_length),
                 NM_ERR_INVALID);
}

static void test_partial_parser(void)
{
    static const uint8_t payload[] = {'h', 'e', 'l', 'l', 'o'};
    nm_frame_parser_t parser;
    nm_frame_t frame;
    bool complete = false;
    uint8_t *encoded = NULL;
    size_t encoded_length = 0u;
    uint8_t oversized_header[NM_FRAME_HEADER_SIZE];

    CHECK_STATUS(nm_frame_encode(0x42u, payload, sizeof(payload), &encoded,
                                 &encoded_length),
                 NM_OK);
    if (encoded == NULL) {
        return;
    }
    nm_frame_parser_init(&parser);
    memset(&frame, 0, sizeof(frame));

    CHECK_STATUS(nm_frame_parser_feed(&parser, encoded, 5u), NM_OK);
    CHECK_STATUS(nm_frame_parser_next(&parser, &frame, &complete), NM_OK);
    CHECK(!complete);
    CHECK(parser.length == 5u);

    CHECK_STATUS(nm_frame_parser_feed(&parser, encoded + 5u, 4u), NM_OK);
    CHECK_STATUS(nm_frame_parser_next(&parser, &frame, &complete), NM_OK);
    CHECK(!complete);
    CHECK(parser.length == NM_FRAME_HEADER_SIZE);

    CHECK_STATUS(nm_frame_parser_feed(&parser, encoded + 9u, 2u), NM_OK);
    CHECK_STATUS(nm_frame_parser_next(&parser, &frame, &complete), NM_OK);
    CHECK(!complete);
    CHECK(parser.length == NM_FRAME_HEADER_SIZE + 2u);

    CHECK_STATUS(nm_frame_parser_feed(&parser, encoded + 11u,
                                      encoded_length - 11u),
                 NM_OK);
    CHECK_STATUS(nm_frame_parser_next(&parser, &frame, &complete), NM_OK);
    CHECK(complete);
    CHECK(frame.type == 0x42u);
    CHECK(frame.length == sizeof(payload));
    CHECK(frame.payload != NULL);
    if (frame.payload != NULL) {
        CHECK(memcmp(frame.payload, payload, sizeof(payload)) == 0);
    }
    nm_frame_free(&frame);
    CHECK(parser.length == 0u);
    free(encoded);
    encoded = NULL;

    /* Two frames in the same feed: dispatch must return them one at a time. */
    CHECK_STATUS(nm_frame_encode(NM_MSG_PING, "12345678", 8u, &encoded,
                                 &encoded_length),
                 NM_OK);
    if (encoded != NULL) {
        CHECK_STATUS(nm_frame_parser_feed(&parser, encoded, encoded_length),
                     NM_OK);
        CHECK_STATUS(nm_frame_parser_feed(&parser, encoded, encoded_length),
                     NM_OK);
        for (unsigned int i = 0u; i < 2u; ++i) {
            CHECK_STATUS(nm_frame_parser_next(&parser, &frame, &complete),
                         NM_OK);
            CHECK(complete);
            CHECK(frame.type == (uint8_t)NM_MSG_PING);
            CHECK(frame.length == 8u);
            CHECK(frame.payload != NULL);
            if (frame.payload != NULL) {
                CHECK(memcmp(frame.payload, "12345678", 8u) == 0);
            }
            nm_frame_free(&frame);
        }
        CHECK(parser.length == 0u);
        free(encoded);
        encoded = NULL;
    }

    /* A header claiming more than the protocol limit is rejected at once. */
    nm_frame_header_encode(oversized_header, NM_MSG_STORE,
                           NM_MAX_FRAME_PAYLOAD + 1u);
    CHECK_STATUS(nm_frame_parser_feed(&parser, oversized_header,
                                      sizeof(oversized_header)),
                 NM_OK);
    CHECK_STATUS(nm_frame_parser_next(&parser, &frame, &complete),
                 NM_ERR_TOO_LARGE);
    CHECK(!complete);

    nm_frame_header_encode(oversized_header, NM_MSG_PING, 0u);
    nm_frame_parser_reset(&parser);
    nm_frame_parser_init(&parser);
    CHECK_STATUS(nm_frame_parser_feed(&parser, oversized_header,
                                      sizeof(oversized_header)),
                 NM_OK);
    CHECK_STATUS(nm_frame_parser_next(&parser, &frame, &complete), NM_OK);
    CHECK(complete);
    CHECK(frame.type == (uint8_t)NM_MSG_PING);
    CHECK(frame.length == 0u);
    CHECK(frame.payload == NULL);
    nm_frame_free(&frame);

    nm_frame_parser_reset(&parser);
}

static void test_wire_helpers(void)
{
    uint8_t digest[NM_SHA256_LEN];
    uint8_t ack[NM_ACK_WIRE_SIZE];
    uint8_t decoded_digest[NM_SHA256_LEN];
    uint8_t decoded_type = 0u;
    nm_status_t status = NM_ERR_SYSTEM;
    uint8_t *error_payload = NULL;
    size_t error_length = 0u;
    char message[64];

    for (size_t i = 0u; i < NM_SHA256_LEN; ++i) {
        digest[i] = (uint8_t)(i + 1u);
    }
    CHECK_STATUS(nm_wire_ack_encode(NM_ERR_QUORUM, NM_MSG_STORE, digest, ack),
                 NM_OK);
    CHECK_STATUS(nm_wire_ack_decode(ack, sizeof(ack), &status, &decoded_type,
                                    decoded_digest),
                 NM_OK);
    CHECK(status == NM_ERR_QUORUM);
    CHECK(decoded_type == (uint8_t)NM_MSG_STORE);
    CHECK(memcmp(digest, decoded_digest, NM_SHA256_LEN) == 0);
    CHECK_STATUS(nm_wire_ack_decode(ack, sizeof(ack) - 1u, &status,
                                    &decoded_type, decoded_digest),
                 NM_ERR_PROTOCOL);

    CHECK_STATUS(nm_wire_error_encode(NM_ERR_NOT_FOUND, "chunk not found",
                                      &error_payload, &error_length),
                 NM_OK);
    CHECK(error_length == 3u + strlen("chunk not found"));
    memset(message, 0, sizeof(message));
    CHECK_STATUS(nm_wire_error_decode(error_payload, error_length, &status,
                                      message, sizeof(message)),
                 NM_OK);
    CHECK(status == NM_ERR_NOT_FOUND);
    CHECK(strcmp(message, "chunk not found") == 0);
    if (error_length > 0u) {
        error_payload[error_length - 1u] ^= 0x01u;
    }
    CHECK_STATUS(nm_wire_error_decode(error_payload, error_length, &status,
                                      message, sizeof(message)),
                 NM_OK);
    CHECK(strcmp(message, "chunk not foune") == 0);
    CHECK_STATUS(nm_wire_error_decode(error_payload, error_length - 1u,
                                      &status, message, sizeof(message)),
                 NM_ERR_PROTOCOL);
    free(error_payload);
}

static void test_outbuf(void)
{
    nm_outbuf_t buffer;
    int sockets[2] = {-1, -1};
    uint8_t received[64];
    nm_frame_t frame;
    uint8_t *large_payload = NULL;
    nm_flush_result_t flush_result;

    nm_outbuf_init(&buffer);
    CHECK(nm_outbuf_pending(&buffer) == 0u);
    CHECK_STATUS(nm_outbuf_append(&buffer, "hello", 5u), NM_OK);
    CHECK(nm_outbuf_pending(&buffer) == 5u);
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
        perror("socketpair");
        checks_failed += 1u;
        return;
    }
    flush_result = nm_outbuf_flush(sockets[0], &buffer);
    CHECK(flush_result == NM_FLUSH_DONE);
    CHECK(nm_outbuf_pending(&buffer) == 0u);
    CHECK(nm_read_exact(sockets[1], received, 5u) == NM_OK);
    CHECK(memcmp(received, "hello", 5u) == 0);

    CHECK_STATUS(nm_outbuf_append_frame(&buffer, NM_MSG_PONG, "12345678",
                                        8u),
                 NM_OK);
    CHECK(nm_outbuf_pending(&buffer) == 17u);
    flush_result = nm_outbuf_flush(sockets[0], &buffer);
    CHECK(flush_result == NM_FLUSH_DONE);
    memset(&frame, 0, sizeof(frame));
    CHECK_STATUS(nm_frame_recv(sockets[1], &frame), NM_OK);
    CHECK(frame.type == (uint8_t)NM_MSG_PONG);
    CHECK(frame.length == 8u);
    nm_frame_free(&frame);

    CHECK_STATUS(nm_set_socket_timeouts(sockets[1], 100, 100), NM_OK);
    memset(&frame, 0, sizeof(frame));
    CHECK_STATUS(nm_frame_recv(sockets[1], &frame), NM_ERR_TIMEOUT);
    nm_frame_free(&frame);

    close(sockets[0]);
    close(sockets[1]);
    nm_outbuf_free(&buffer);
    nm_outbuf_init(&buffer);

    /* Force a partial flush with a tiny send buffer and a non-blocking peer. */
    sockets[0] = -1;
    sockets[1] = -1;
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
        perror("socketpair");
        checks_failed += 1u;
        return;
    }
    int send_buffer = 4096;
    (void)setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &send_buffer,
                     sizeof(send_buffer));
    CHECK_STATUS(nm_fd_set_nonblocking(sockets[0], true), NM_OK);
    large_payload = malloc(512u * 1024u);
    CHECK(large_payload != NULL);
    if (large_payload != NULL) {
        memset(large_payload, 0x5a, 512u * 1024u);
        CHECK_STATUS(nm_outbuf_append(&buffer, large_payload, 512u * 1024u),
                     NM_OK);
        flush_result = nm_outbuf_flush(sockets[0], &buffer);
        CHECK(flush_result == NM_FLUSH_PARTIAL);
        CHECK(nm_outbuf_pending(&buffer) > 0u);
    }
    free(large_payload);
    close(sockets[0]);
    close(sockets[1]);
    nm_outbuf_free(&buffer);
}

int main(void)
{
    test_byte_order();
    test_endpoint_parse();
    test_full_frame();
    test_partial_parser();
    test_wire_helpers();
    test_outbuf();

    printf("test_network: %lu checks, %lu failure(s)\n", checks_run,
           checks_failed);
    return checks_failed == 0u ? EXIT_SUCCESS : EXIT_FAILURE;
}
