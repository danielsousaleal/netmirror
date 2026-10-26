#include "network.h"

#include "util.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

/* ------------------------------------------------------------------------- */
/* Byte order                                                                */
/* ------------------------------------------------------------------------- */

void nm_put_u16_be(uint8_t *out, uint16_t value)
{
    if (out == NULL) {
        return;
    }
    out[0] = (uint8_t)((value >> 8u) & 0xffu);
    out[1] = (uint8_t)(value & 0xffu);
}

void nm_put_u32_be(uint8_t *out, uint32_t value)
{
    if (out == NULL) {
        return;
    }
    for (unsigned int i = 0u; i < 4u; ++i) {
        out[3u - i] = (uint8_t)(value & 0xffu);
        value >>= 8u;
    }
}

void nm_put_u64_be(uint8_t *out, uint64_t value)
{
    if (out == NULL) {
        return;
    }
    for (unsigned int i = 0u; i < 8u; ++i) {
        out[7u - i] = (uint8_t)(value & 0xffu);
        value >>= 8u;
    }
}

uint16_t nm_get_u16_be(const uint8_t *in)
{
    if (in == NULL) {
        return 0u;
    }
    return (uint16_t)(((uint16_t)in[0] << 8u) | (uint16_t)in[1]);
}

uint32_t nm_get_u32_be(const uint8_t *in)
{
    uint32_t value = 0u;

    if (in == NULL) {
        return 0u;
    }
    for (unsigned int i = 0u; i < 4u; ++i) {
        value = (value << 8u) | (uint32_t)in[i];
    }
    return value;
}

uint64_t nm_get_u64_be(const uint8_t *in)
{
    uint64_t value = 0u;

    if (in == NULL) {
        return 0u;
    }
    for (unsigned int i = 0u; i < 8u; ++i) {
        value = (value << 8u) | (uint64_t)in[i];
    }
    return value;
}

/* ------------------------------------------------------------------------- */
/* Framing                                                                   */
/* ------------------------------------------------------------------------- */

int nm_frame_header_encode(uint8_t header[NM_FRAME_HEADER_SIZE], uint8_t type,
                           uint64_t payload_length)
{
    if (header == NULL) {
        return NM_ERR_INVALID;
    }
    header[0] = type;
    nm_put_u64_be(header + 1u, payload_length);
    return NM_OK;
}

int nm_frame_header_decode(const uint8_t header[NM_FRAME_HEADER_SIZE],
                           uint8_t *type, uint64_t *payload_length)
{
    if (header == NULL || type == NULL || payload_length == NULL) {
        return NM_ERR_INVALID;
    }
    *type = header[0];
    *payload_length = nm_get_u64_be(header + 1u);
    return NM_OK;
}

int nm_frame_encode(uint8_t type, const void *payload,
                    uint64_t payload_length, uint8_t **out,
                    size_t *out_length)
{
    uint8_t *frame;
    size_t total_length;

    if (out == NULL || out_length == NULL) {
        return NM_ERR_INVALID;
    }
    *out = NULL;
    *out_length = 0u;
    if (payload_length > NM_MAX_FRAME_PAYLOAD) {
        return NM_ERR_TOO_LARGE;
    }
    if (payload == NULL && payload_length != 0u) {
        return NM_ERR_INVALID;
    }
    total_length = NM_FRAME_HEADER_SIZE + (size_t)payload_length;
    frame = malloc(total_length);
    if (frame == NULL) {
        return NM_ERR_NOMEM;
    }
    if (nm_frame_header_encode(frame, type, payload_length) != NM_OK) {
        free(frame);
        return NM_ERR_INVALID;
    }
    if (payload_length > 0u) {
        memcpy(frame + NM_FRAME_HEADER_SIZE, payload, (size_t)payload_length);
    }
    *out = frame;
    *out_length = total_length;
    return NM_OK;
}

void nm_frame_free(nm_frame_t *frame)
{
    if (frame == NULL) {
        return;
    }
    free(frame->payload);
    frame->payload = NULL;
    frame->length = 0u;
    frame->type = 0u;
}

/* ------------------------------------------------------------------------- */
/* Partial-frame parser                                                      */
/* ------------------------------------------------------------------------- */

void nm_frame_parser_init(nm_frame_parser_t *parser)
{
    if (parser == NULL) {
        return;
    }
    parser->buffer = NULL;
    parser->length = 0u;
    parser->capacity = 0u;
}

void nm_frame_parser_reset(nm_frame_parser_t *parser)
{
    if (parser == NULL) {
        return;
    }
    free(parser->buffer);
    parser->buffer = NULL;
    parser->length = 0u;
    parser->capacity = 0u;
}

static int parser_reserve(nm_frame_parser_t *parser, size_t extra)
{
    size_t needed;
    size_t new_capacity;
    uint8_t *new_buffer;

    if (parser == NULL || (extra > 0u && parser->buffer == NULL &&
                           parser->length != 0u)) {
        return NM_ERR_INVALID;
    }
    if (parser->length > NM_FRAME_PARSER_MAX_BUFFER ||
        extra > NM_FRAME_PARSER_MAX_BUFFER - parser->length) {
        return NM_ERR_TOO_LARGE;
    }
    needed = parser->length + extra;
    if (needed <= parser->capacity) {
        return NM_OK;
    }
    new_capacity = parser->capacity == 0u ? NM_OUTBUF_INITIAL_CAPACITY
                                          : parser->capacity;
    while (new_capacity < needed) {
        if (new_capacity > NM_FRAME_PARSER_MAX_BUFFER / 2u) {
            new_capacity = NM_FRAME_PARSER_MAX_BUFFER;
            break;
        }
        new_capacity *= 2u;
    }
    if (new_capacity < needed) {
        new_capacity = needed;
    }
    new_buffer = realloc(parser->buffer, new_capacity);
    if (new_buffer == NULL) {
        return NM_ERR_NOMEM;
    }
    parser->buffer = new_buffer;
    parser->capacity = new_capacity;
    return NM_OK;
}

int nm_frame_parser_feed(nm_frame_parser_t *parser, const void *data,
                         size_t length)
{
    int result;

    if (parser == NULL || (data == NULL && length != 0u)) {
        return NM_ERR_INVALID;
    }
    if (length == 0u) {
        return NM_OK;
    }
    result = parser_reserve(parser, length);
    if (result != NM_OK) {
        return result;
    }
    memcpy(parser->buffer + parser->length, data, length);
    parser->length += length;
    return NM_OK;
}

static void parser_consume(nm_frame_parser_t *parser, size_t length)
{
    if (length >= parser->length) {
        parser->length = 0u;
    } else {
        memmove(parser->buffer, parser->buffer + length,
                parser->length - length);
        parser->length -= length;
    }
    if (parser->length == 0u &&
        parser->capacity > NM_FRAME_PARSER_RETAIN_BYTES) {
        free(parser->buffer);
        parser->buffer = NULL;
        parser->capacity = 0u;
    }
}

int nm_frame_parser_next(nm_frame_parser_t *parser, nm_frame_t *frame,
                         bool *complete)
{
    uint8_t type = 0u;
    uint64_t payload_length = 0u;
    size_t total_length;
    uint8_t *payload = NULL;

    if (parser == NULL || frame == NULL || complete == NULL) {
        return NM_ERR_INVALID;
    }
    *complete = false;
    if (parser->length < NM_FRAME_HEADER_SIZE) {
        return NM_OK;
    }
    if (nm_frame_header_decode(parser->buffer, &type, &payload_length) !=
        NM_OK) {
        return NM_ERR_PROTOCOL;
    }
    if (payload_length > NM_MAX_FRAME_PAYLOAD) {
        return NM_ERR_TOO_LARGE;
    }
    total_length = NM_FRAME_HEADER_SIZE + (size_t)payload_length;
    if (parser->length < total_length) {
        return NM_OK;
    }
    if (payload_length > 0u) {
        payload = malloc((size_t)payload_length);
        if (payload == NULL) {
            return NM_ERR_NOMEM;
        }
        memcpy(payload, parser->buffer + NM_FRAME_HEADER_SIZE,
               (size_t)payload_length);
    }
    parser_consume(parser, total_length);
    frame->type = type;
    frame->length = payload_length;
    frame->payload = payload;
    *complete = true;
    return NM_OK;
}

/* ------------------------------------------------------------------------- */
/* Blocking frame I/O                                                        */
/* ------------------------------------------------------------------------- */

int nm_write_all(int fd, const void *data, size_t length)
{
    const uint8_t *cursor = data;
    size_t remaining = length;

    if (fd < 0 || (data == NULL && length != 0u)) {
        return NM_ERR_INVALID;
    }
    while (remaining > 0u) {
        ssize_t written = send(fd, cursor, remaining, MSG_NOSIGNAL);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return NM_ERR_TIMEOUT;
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

int nm_read_exact(int fd, void *data, size_t length)
{
    uint8_t *cursor = data;
    size_t remaining = length;

    if (fd < 0 || (data == NULL && length != 0u)) {
        return NM_ERR_INVALID;
    }
    while (remaining > 0u) {
        ssize_t received = recv(fd, cursor, remaining, 0);
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return NM_ERR_TIMEOUT;
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

int nm_frame_send(int fd, uint8_t type, const void *payload,
                  uint64_t payload_length)
{
    uint8_t header[NM_FRAME_HEADER_SIZE];
    int result;

    if (payload_length > NM_MAX_FRAME_PAYLOAD ||
        (payload == NULL && payload_length != 0u)) {
        return NM_ERR_INVALID;
    }
    result = nm_frame_header_encode(header, type, payload_length);
    if (result != NM_OK) {
        return result;
    }
    result = nm_write_all(fd, header, sizeof(header));
    if (result != NM_OK) {
        return result;
    }
    if (payload_length > 0u) {
        return nm_write_all(fd, payload, (size_t)payload_length);
    }
    return NM_OK;
}

int nm_frame_recv(int fd, nm_frame_t *frame)
{
    uint8_t header[NM_FRAME_HEADER_SIZE];
    uint8_t type = 0u;
    uint64_t payload_length = 0u;
    uint8_t *payload = NULL;
    int result;

    if (frame == NULL) {
        return NM_ERR_INVALID;
    }
    frame->type = 0u;
    frame->length = 0u;
    frame->payload = NULL;
    result = nm_read_exact(fd, header, sizeof(header));
    if (result != NM_OK) {
        return result;
    }
    if (nm_frame_header_decode(header, &type, &payload_length) != NM_OK) {
        return NM_ERR_PROTOCOL;
    }
    if (payload_length > NM_MAX_FRAME_PAYLOAD) {
        return NM_ERR_TOO_LARGE;
    }
    if (payload_length > 0u) {
        payload = malloc((size_t)payload_length);
        if (payload == NULL) {
            return NM_ERR_NOMEM;
        }
        result = nm_read_exact(fd, payload, (size_t)payload_length);
        if (result != NM_OK) {
            free(payload);
            return result;
        }
    }
    frame->type = type;
    frame->length = payload_length;
    frame->payload = payload;
    return NM_OK;
}

/* ------------------------------------------------------------------------- */
/* Wire payload helpers                                                      */
/* ------------------------------------------------------------------------- */

int nm_wire_ack_encode(nm_status_t status, uint8_t acknowledged_type,
                       const uint8_t digest[NM_SHA256_LEN],
                       uint8_t out[NM_ACK_WIRE_SIZE])
{
    if (digest == NULL || out == NULL) {
        return NM_ERR_INVALID;
    }
    out[NM_ACK_STATUS_OFFSET] = (uint8_t)status;
    out[NM_ACK_TYPE_OFFSET] = acknowledged_type;
    memcpy(out + NM_ACK_DIGEST_OFFSET, digest, NM_SHA256_LEN);
    return NM_OK;
}

int nm_wire_ack_decode(const uint8_t *payload, uint64_t payload_length,
                       nm_status_t *status, uint8_t *acknowledged_type,
                       uint8_t digest[NM_SHA256_LEN])
{
    if (payload == NULL || status == NULL || acknowledged_type == NULL ||
        digest == NULL) {
        return NM_ERR_INVALID;
    }
    if (payload_length != NM_ACK_WIRE_SIZE) {
        return NM_ERR_PROTOCOL;
    }
    *status = (nm_status_t)payload[NM_ACK_STATUS_OFFSET];
    *acknowledged_type = payload[NM_ACK_TYPE_OFFSET];
    memcpy(digest, payload + NM_ACK_DIGEST_OFFSET, NM_SHA256_LEN);
    return NM_OK;
}

int nm_wire_error_encode(nm_status_t status, const char *message,
                         uint8_t **out, size_t *out_length)
{
    size_t message_length = 0u;
    size_t total_length;
    uint8_t *payload;

    if (out == NULL || out_length == NULL) {
        return NM_ERR_INVALID;
    }
    *out = NULL;
    *out_length = 0u;
    if (message != NULL) {
        message_length = strnlen(message, NM_MAX_ERROR_MSG);
    }
    total_length = 3u + message_length;
    payload = malloc(total_length);
    if (payload == NULL) {
        return NM_ERR_NOMEM;
    }
    payload[NM_ERROR_STATUS_OFFSET] = (uint8_t)status;
    nm_put_u16_be(payload + NM_ERROR_LENGTH_OFFSET, (uint16_t)message_length);
    if (message_length > 0u) {
        memcpy(payload + NM_ERROR_MESSAGE_OFFSET, message, message_length);
    }
    *out = payload;
    *out_length = total_length;
    return NM_OK;
}

int nm_wire_error_decode(const uint8_t *payload, uint64_t payload_length,
                         nm_status_t *status, char *message,
                         size_t message_capacity)
{
    uint16_t message_length;

    if (payload == NULL || status == NULL || message == NULL ||
        message_capacity == 0u) {
        return NM_ERR_INVALID;
    }
    message[0] = '\0';
    if (payload_length < 3u || payload_length > NM_MAX_ERROR_WIRE) {
        return NM_ERR_PROTOCOL;
    }
    message_length = nm_get_u16_be(payload + NM_ERROR_LENGTH_OFFSET);
    if ((uint64_t)message_length != payload_length - 3u) {
        return NM_ERR_PROTOCOL;
    }
    *status = (nm_status_t)payload[NM_ERROR_STATUS_OFFSET];
    size_t copy_length = message_length;
    if (copy_length >= message_capacity) {
        copy_length = message_capacity - 1u;
    }
    if (copy_length > 0u) {
        memcpy(message, payload + NM_ERROR_MESSAGE_OFFSET, copy_length);
    }
    message[copy_length] = '\0';
    return NM_OK;
}

/* ------------------------------------------------------------------------- */
/* Output buffer                                                             */
/* ------------------------------------------------------------------------- */

void nm_outbuf_init(nm_outbuf_t *buffer)
{
    if (buffer == NULL) {
        return;
    }
    buffer->data = NULL;
    buffer->start = 0u;
    buffer->length = 0u;
    buffer->capacity = 0u;
}

void nm_outbuf_reset(nm_outbuf_t *buffer)
{
    if (buffer == NULL) {
        return;
    }
    free(buffer->data);
    nm_outbuf_init(buffer);
}

void nm_outbuf_free(nm_outbuf_t *buffer)
{
    nm_outbuf_reset(buffer);
}

size_t nm_outbuf_pending(const nm_outbuf_t *buffer)
{
    if (buffer == NULL || buffer->length < buffer->start) {
        return 0u;
    }
    return buffer->length - buffer->start;
}

static int outbuf_make_room(nm_outbuf_t *buffer, size_t extra,
                            uint8_t **destination)
{
    size_t pending;
    size_t needed;
    size_t new_capacity;
    uint8_t *new_data;

    if (buffer == NULL || destination == NULL) {
        return NM_ERR_INVALID;
    }
    if (buffer->length < buffer->start) {
        return NM_ERR_INVALID;
    }
    pending = buffer->length - buffer->start;
    if (extra > SIZE_MAX - pending) {
        return NM_ERR_NOMEM;
    }
    if (buffer->capacity >= buffer->length &&
        buffer->capacity - buffer->length >= extra) {
        *destination = buffer->data + buffer->length;
        return NM_OK;
    }
    if (buffer->start > 0u) {
        memmove(buffer->data, buffer->data + buffer->start, pending);
        buffer->start = 0u;
        buffer->length = pending;
        if (buffer->capacity - buffer->length >= extra) {
            *destination = buffer->data + buffer->length;
            return NM_OK;
        }
    }
    needed = buffer->length + extra;
    new_capacity = buffer->capacity == 0u ? NM_OUTBUF_INITIAL_CAPACITY
                                          : buffer->capacity;
    while (new_capacity < needed) {
        if (new_capacity > SIZE_MAX / 2u) {
            new_capacity = needed;
            break;
        }
        new_capacity *= 2u;
    }
    new_data = realloc(buffer->data, new_capacity);
    if (new_data == NULL) {
        return NM_ERR_NOMEM;
    }
    buffer->data = new_data;
    buffer->capacity = new_capacity;
    *destination = buffer->data + buffer->length;
    return NM_OK;
}

int nm_outbuf_append(nm_outbuf_t *buffer, const void *data, size_t length)
{
    uint8_t *destination;
    int result;

    if (buffer == NULL || (data == NULL && length != 0u)) {
        return NM_ERR_INVALID;
    }
    if (length == 0u) {
        return NM_OK;
    }
    result = outbuf_make_room(buffer, length, &destination);
    if (result != NM_OK) {
        return result;
    }
    memcpy(destination, data, length);
    buffer->length += length;
    return NM_OK;
}

int nm_outbuf_append_frame(nm_outbuf_t *buffer, uint8_t type,
                           const void *payload, uint64_t payload_length)
{
    uint8_t header[NM_FRAME_HEADER_SIZE];
    uint8_t *destination;
    size_t total_length;
    int result;

    if (buffer == NULL ||
        (payload == NULL && payload_length != 0u)) {
        return NM_ERR_INVALID;
    }
    if (payload_length > NM_MAX_FRAME_PAYLOAD) {
        return NM_ERR_TOO_LARGE;
    }
    total_length = NM_FRAME_HEADER_SIZE + (size_t)payload_length;
    result = outbuf_make_room(buffer, total_length, &destination);
    if (result != NM_OK) {
        return result;
    }
    nm_frame_header_encode(header, type, payload_length);
    memcpy(destination, header, sizeof(header));
    if (payload_length > 0u) {
        memcpy(destination + NM_FRAME_HEADER_SIZE, payload,
               (size_t)payload_length);
    }
    buffer->length += total_length;
    return NM_OK;
}

nm_flush_result_t nm_outbuf_flush(int fd, nm_outbuf_t *buffer)
{
    if (fd < 0 || buffer == NULL) {
        return NM_FLUSH_ERROR;
    }
    while (buffer->start < buffer->length) {
        size_t remaining = buffer->length - buffer->start;
        ssize_t written =
            send(fd, buffer->data + buffer->start, remaining, MSG_NOSIGNAL);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return NM_FLUSH_PARTIAL;
            }
            return NM_FLUSH_ERROR;
        }
        if (written == 0) {
            return NM_FLUSH_ERROR;
        }
        buffer->start += (size_t)written;
    }
    buffer->start = 0u;
    buffer->length = 0u;
    return NM_FLUSH_DONE;
}

/* ------------------------------------------------------------------------- */
/* POSIX sockets                                                             */
/* ------------------------------------------------------------------------- */

int nm_endpoint_parse(const char *text, nm_endpoint_t *out)
{
    const char *host_start;
    const char *host_end;
    const char *port_start;
    char *end = NULL;
    unsigned long port;
    size_t host_length;

    if (text == NULL || out == NULL) {
        return NM_ERR_INVALID;
    }
    memset(out, 0, sizeof(*out));
    if (text[0] == '[') {
        host_start = text + 1;
        host_end = strchr(host_start, ']');
        if (host_end == NULL || host_end[1] != ':') {
            return NM_ERR_INVALID;
        }
        port_start = host_end + 2;
    } else {
        const char *colon = strrchr(text, ':');
        if (colon == NULL || colon == text || strchr(text, ':') != colon) {
            return NM_ERR_INVALID;
        }
        host_start = text;
        host_end = colon;
        port_start = colon + 1;
    }
    if (*port_start == '\0') {
        return NM_ERR_INVALID;
    }
    errno = 0;
    port = strtoul(port_start, &end, 10);
    if (errno != 0 || end == port_start || *end != '\0' || port == 0u ||
        port > 65535u) {
        return NM_ERR_INVALID;
    }
    host_length = (size_t)(host_end - host_start);
    if (host_length == 0u || host_length >= NM_MAX_HOST) {
        return NM_ERR_INVALID;
    }
    memcpy(out->host, host_start, host_length);
    out->host[host_length] = '\0';
    out->port = (uint16_t)port;
    return NM_OK;
}

int nm_fd_set_cloexec(int fd)
{
    int flags;

    if (fd < 0) {
        return NM_ERR_INVALID;
    }
    flags = fcntl(fd, F_GETFD);
    if (flags < 0) {
        return NM_ERR_IO;
    }
    if (fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) {
        return NM_ERR_IO;
    }
    return NM_OK;
}

int nm_fd_set_nonblocking(int fd, bool enabled)
{
    int flags;

    if (fd < 0) {
        return NM_ERR_INVALID;
    }
    flags = fcntl(fd, F_GETFL);
    if (flags < 0) {
        return NM_ERR_IO;
    }
    if (enabled) {
        flags |= O_NONBLOCK;
    } else {
        flags &= ~O_NONBLOCK;
    }
    if (fcntl(fd, F_SETFL, flags) < 0) {
        return NM_ERR_IO;
    }
    return NM_OK;
}

int nm_set_socket_timeouts(int fd, int receive_ms, int send_ms)
{
    struct timeval timeout;

    if (fd < 0 || receive_ms < 0 || send_ms < 0) {
        return NM_ERR_INVALID;
    }
    timeout.tv_sec = receive_ms / 1000;
    timeout.tv_usec = (receive_ms % 1000) * 1000;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout)) != 0) {
        return NM_ERR_IO;
    }
    timeout.tv_sec = send_ms / 1000;
    timeout.tv_usec = (send_ms % 1000) * 1000;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                   sizeof(timeout)) != 0) {
        return NM_ERR_IO;
    }
    return NM_OK;
}

void nm_close_fd(int *fd)
{
    if (fd == NULL || *fd < 0) {
        return;
    }
    int closing = *fd;
    *fd = -1;
    if (close(closing) != 0) {
        /* The descriptor is gone from this process even if close() failed. */
    }
}

static int configure_connected_socket(int fd, int timeout_ms)
{
    int result;

    result = nm_fd_set_nonblocking(fd, false);
    if (result != NM_OK) {
        return result;
    }
    result = nm_set_socket_timeouts(fd, timeout_ms, timeout_ms);
    if (result != NM_OK) {
        return result;
    }
    int enabled = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                   sizeof(enabled)) != 0) {
        return NM_ERR_IO;
    }
    return NM_OK;
}

int nm_tcp_connect(const char *host, uint16_t port, int timeout_ms)
{
    struct addrinfo hints;
    struct addrinfo *addresses = NULL;
    struct addrinfo *current;
    char service[8];
    int written;
    int result_status = NM_ERR_UNAVAILABLE;
    uint64_t deadline;
    int address_error = 0;

    if (host == NULL || host[0] == '\0' || port == 0u || timeout_ms <= 0) {
        return NM_ERR_INVALID;
    }
    written = snprintf(service, sizeof(service), "%u", (unsigned int)port);
    if (written <= 0 || (size_t)written >= sizeof(service)) {
        return NM_ERR_INVALID;
    }
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    address_error = getaddrinfo(host, service, &hints, &addresses);
    if (address_error != 0) {
        return NM_ERR_UNAVAILABLE;
    }
    deadline = nm_now_ms() + (uint64_t)timeout_ms;

    for (current = addresses; current != NULL; current = current->ai_next) {
        int fd;
        int connect_result;
        int socket_error = 0;
        socklen_t socket_error_length = sizeof(socket_error);
        bool connected = false;
        bool timed_out = false;

        fd = socket(current->ai_family, current->ai_socktype,
                    current->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (nm_fd_set_cloexec(fd) != NM_OK ||
            nm_fd_set_nonblocking(fd, true) != NM_OK) {
            nm_close_fd(&fd);
            continue;
        }
        connect_result =
            connect(fd, current->ai_addr, current->ai_addrlen);
        if (connect_result == 0) {
            connected = true;
        } else if (errno == EINPROGRESS) {
            for (;;) {
                uint64_t now = nm_now_ms();
                uint64_t remaining_ms =
                    now < deadline ? deadline - now : 0u;
                struct pollfd poll_fd;
                int poll_result;
                int poll_timeout;

                if (remaining_ms == 0u) {
                    timed_out = true;
                    break;
                }
                poll_timeout = remaining_ms > (uint64_t)INT_MAX
                                   ? INT_MAX
                                   : (int)remaining_ms;
                poll_fd.fd = fd;
                poll_fd.events = POLLOUT;
                poll_fd.revents = 0;
                poll_result = poll(&poll_fd, 1, poll_timeout);
                if (poll_result < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    break;
                }
                if (poll_result == 0) {
                    timed_out = true;
                    break;
                }
                if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error,
                               &socket_error_length) != 0) {
                    break;
                }
                if (socket_error != 0) {
                    break;
                }
                connected = true;
                break;
            }
        }
        if (!connected) {
            if (timed_out) {
                result_status = NM_ERR_TIMEOUT;
            }
            nm_close_fd(&fd);
            continue;
        }
        if (configure_connected_socket(fd, timeout_ms) != NM_OK) {
            nm_close_fd(&fd);
            result_status = NM_ERR_IO;
            continue;
        }
        freeaddrinfo(addresses);
        return fd;
    }

    freeaddrinfo(addresses);
    return result_status;
}

int nm_tcp_listen(const char *bind_address, uint16_t port, int backlog)
{
    struct addrinfo hints;
    struct addrinfo *addresses = NULL;
    struct addrinfo *current;
    char service[8];
    int written;

    if (backlog <= 0) {
        backlog = NM_TCP_BACKLOG;
    }
    written = snprintf(service, sizeof(service), "%u", (unsigned int)port);
    if (written <= 0 || (size_t)written >= sizeof(service)) {
        return NM_ERR_INVALID;
    }
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_PASSIVE;
    const char *node = (bind_address != NULL && bind_address[0] != '\0')
                           ? bind_address
                           : NULL;
    if (getaddrinfo(node, service, &hints, &addresses) != 0) {
        return NM_ERR_UNAVAILABLE;
    }
    for (current = addresses; current != NULL; current = current->ai_next) {
        int fd;
        int enabled = 1;

        fd = socket(current->ai_family, current->ai_socktype,
                    current->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (nm_fd_set_cloexec(fd) != NM_OK) {
            nm_close_fd(&fd);
            continue;
        }
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled,
                       sizeof(enabled)) != 0) {
            nm_close_fd(&fd);
            continue;
        }
        if (bind(fd, current->ai_addr, current->ai_addrlen) != 0) {
            nm_close_fd(&fd);
            continue;
        }
        if (listen(fd, backlog) != 0) {
            nm_close_fd(&fd);
            continue;
        }
        if (nm_fd_set_nonblocking(fd, true) != NM_OK) {
            nm_close_fd(&fd);
            continue;
        }
        freeaddrinfo(addresses);
        return fd;
    }
    freeaddrinfo(addresses);
    return NM_ERR_IO;
}

int nm_tcp_accept(int listening_fd)
{
    int fd;
    int result;
    int enabled = 1;

    if (listening_fd < 0) {
        errno = EINVAL;
        return -1;
    }
    fd = accept(listening_fd, NULL, NULL);
    if (fd < 0) {
        return -1;
    }
    result = nm_fd_set_cloexec(fd);
    if (result == NM_OK) {
        result = nm_fd_set_nonblocking(fd, true);
    }
    if (result == NM_OK &&
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                   sizeof(enabled)) != 0) {
        result = NM_ERR_IO;
    }
    if (result != NM_OK) {
        nm_close_fd(&fd);
        errno = EIO;
        return -1;
    }
    return fd;
}
