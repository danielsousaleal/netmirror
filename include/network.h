/*
 * netmirror - internal binary transport
 *
 * This module owns the on-wire framing, partial-frame parser, bounded output
 * buffer and POSIX socket helpers. Nodes and gateways use it; no HTTP is
 * accepted here.
 *
 * PARTIAL FRAMES
 * --------------
 * TCP is a byte stream: one recv() may return a fraction of a frame, several
 * frames, or only part of the 9-byte header. nm_frame_parser_t is an explicit
 * buffering state machine:
 *
 *   1. retain bytes until the complete 9-byte header is available;
 *   2. decode the declared payload length and validate the protocol maximum;
 *   3. retain the payload until exactly `length` bytes have arrived;
 *   4. return one owned nm_frame_t and consume those bytes;
 *   5. repeat if more frames are already buffered.
 *
 * An incomplete frame is never dispatched. The parser keeps at most one
 * maximum-sized frame plus one read chunk in memory. The server's read loop is
 * level-triggered epoll, so it may safely read one bounded chunk per event and
 * return to other connections.
 *
 * PARTIAL WRITES
 * --------------
 * Sockets are non-blocking on the server. nm_outbuf_t owns unsent bytes and
 * nm_outbuf_flush() reports partial progress. The server subscribes to
 * EPOLLOUT while data is pending and never performs a blocking send.
 */

#ifndef NETMIRROR_NETWORK_H
#define NETMIRROR_NETWORK_H

#include "netmirror.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Read granularity for one level-triggered epoll event. */
#define NM_NET_READ_CHUNK 65536u
#define NM_TCP_BACKLOG 128
#define NM_TCP_DEFAULT_TIMEOUT_MS 5000
#define NM_SERVER_MAX_CONNECTIONS 64u

/* Parser may hold a max frame plus at most one extra read chunk. */
#define NM_FRAME_PARSER_MAX_BUFFER                                             \
    (NM_FRAME_HEADER_SIZE + NM_MAX_FRAME_PAYLOAD + NM_NET_READ_CHUNK)

/* Release a parser's storage after a frame if it grew beyond this size. */
#define NM_FRAME_PARSER_RETAIN_BYTES 65536u

#define NM_OUTBUF_INITIAL_CAPACITY 4096u

/* ------------------------------------------------------------------------- */
/* Byte-order helpers (all protocol integers are big-endian)                 */
/* ------------------------------------------------------------------------- */

void nm_put_u16_be(uint8_t *out, uint16_t value);
void nm_put_u32_be(uint8_t *out, uint32_t value);
void nm_put_u64_be(uint8_t *out, uint64_t value);
uint16_t nm_get_u16_be(const uint8_t *in);
uint32_t nm_get_u32_be(const uint8_t *in);
uint64_t nm_get_u64_be(const uint8_t *in);

/* ------------------------------------------------------------------------- */
/* Framing                                                                   */
/* ------------------------------------------------------------------------- */

/*
 * Encode/decode only the 9-byte header. The header encoder accepts any u64
 * length so tests and fault-injection tools can exercise parser rejection; the
 * parser and nm_frame_encode() enforce NM_MAX_FRAME_PAYLOAD.
 */
int nm_frame_header_encode(uint8_t header[NM_FRAME_HEADER_SIZE], uint8_t type,
                           uint64_t payload_length);
int nm_frame_header_decode(const uint8_t header[NM_FRAME_HEADER_SIZE],
                           uint8_t *type, uint64_t *payload_length);

/*
 * Allocate and serialize one complete frame. The caller owns *out and must
 * free it. Payload may be NULL only when payload_length is zero.
 */
int nm_frame_encode(uint8_t type, const void *payload,
                    uint64_t payload_length, uint8_t **out,
                    size_t *out_length);

/* Free a frame returned by nm_frame_recv() or nm_frame_parser_next(). */
void nm_frame_free(nm_frame_t *frame);

/*
 * Copy-free ownership for decoded frames: parser_next() allocates payload.
 * These helpers are the only supported way to release frames.
 */

typedef struct nm_frame_parser {
    uint8_t *buffer;
    size_t length;
    size_t capacity;
} nm_frame_parser_t;

void nm_frame_parser_init(nm_frame_parser_t *parser);
void nm_frame_parser_reset(nm_frame_parser_t *parser);

/*
 * Append bytes received from a socket. Returns NM_ERR_TOO_LARGE if the input
 * would exceed NM_FRAME_PARSER_MAX_BUFFER. Does not parse.
 */
int nm_frame_parser_feed(nm_frame_parser_t *parser, const void *data,
                         size_t length);

/*
 * Try to decode exactly one frame. On success:
 *   - returns NM_OK;
 *   - sets *complete = true and fills `frame` with an allocated payload, or
 *   - sets *complete = false when more bytes are required.
 *
 * A declared payload length above NM_MAX_FRAME_PAYLOAD returns
 * NM_ERR_TOO_LARGE. The caller must then treat the connection as corrupt.
 * `frame` is not modified unless a complete frame is returned.
 */
int nm_frame_parser_next(nm_frame_parser_t *parser, nm_frame_t *frame,
                         bool *complete);

/*
 * Blocking frame helpers for client RPC paths. Sockets must have suitable
 * SO_RCVTIMEO/SO_SNDTIMEO values (nm_tcp_connect does this). They read/write
 * exactly one frame and translate timeouts to NM_ERR_TIMEOUT.
 */
int nm_frame_send(int fd, uint8_t type, const void *payload,
                  uint64_t payload_length);
int nm_frame_recv(int fd, nm_frame_t *frame);

/* Exact I/O loops with EINTR handling and short-write handling. */
int nm_write_all(int fd, const void *data, size_t length);
int nm_read_exact(int fd, void *data, size_t length);

/* ------------------------------------------------------------------------- */
/* Wire payload helpers                                                      */
/* ------------------------------------------------------------------------- */

int nm_wire_ack_encode(nm_status_t status, uint8_t acknowledged_type,
                       const uint8_t digest[NM_SHA256_LEN],
                       uint8_t out[NM_ACK_WIRE_SIZE]);
int nm_wire_ack_decode(const uint8_t *payload, uint64_t payload_length,
                       nm_status_t *status, uint8_t *acknowledged_type,
                       uint8_t digest[NM_SHA256_LEN]);

/*
 * Error payloads are [u8 status][u16 message_length BE][message bytes].
 * Encoding truncates the message to NM_MAX_ERROR_MSG. Decoding truncates the
 * output string to message_capacity, always NUL-terminating it.
 */
int nm_wire_error_encode(nm_status_t status, const char *message,
                         uint8_t **out, size_t *out_length);
int nm_wire_error_decode(const uint8_t *payload, uint64_t payload_length,
                         nm_status_t *status, char *message,
                         size_t message_capacity);

/* ------------------------------------------------------------------------- */
/* Non-blocking output buffer                                                */
/* ------------------------------------------------------------------------- */

typedef enum nm_flush_result {
    NM_FLUSH_ERROR = -1,
    NM_FLUSH_DONE = 0,
    NM_FLUSH_PARTIAL = 1
} nm_flush_result_t;

typedef struct nm_outbuf {
    uint8_t *data;
    size_t start;
    size_t length;
    size_t capacity;
} nm_outbuf_t;

void nm_outbuf_init(nm_outbuf_t *buffer);
void nm_outbuf_reset(nm_outbuf_t *buffer);
void nm_outbuf_free(nm_outbuf_t *buffer);
size_t nm_outbuf_pending(const nm_outbuf_t *buffer);

int nm_outbuf_append(nm_outbuf_t *buffer, const void *data, size_t length);
int nm_outbuf_append_frame(nm_outbuf_t *buffer, uint8_t type,
                           const void *payload, uint64_t payload_length);

/*
 * Send as much as the non-blocking socket accepts. EAGAIN/EWOULDBLOCK is
 * NM_FLUSH_PARTIAL, not an error. A completely drained buffer is reset.
 */
nm_flush_result_t nm_outbuf_flush(int fd, nm_outbuf_t *buffer);

/* ------------------------------------------------------------------------- */
/* POSIX sockets                                                             */
/* ------------------------------------------------------------------------- */

/* Parse "host:port" or "[ipv6-address]:port". Port must be 1..65535. */
int nm_endpoint_parse(const char *text, nm_endpoint_t *out);

int nm_fd_set_cloexec(int fd);
int nm_fd_set_nonblocking(int fd, bool enabled);
int nm_set_socket_timeouts(int fd, int receive_ms, int send_ms);
void nm_close_fd(int *fd);

/*
 * Connect with a bounded total timeout. On success returns a blocking socket
 * with receive/send timeouts set to timeout_ms. Performs name resolution with
 * getaddrinfo(), so Docker service names work.
 */
int nm_tcp_connect(const char *host, uint16_t port, int timeout_ms);

/*
 * Return a non-blocking, close-on-exec listening socket. `bind_address` may be
 * NULL or empty for the wildcard address. Only the first address returned by
 * getaddrinfo() that can bind is used.
 */
int nm_tcp_listen(const char *bind_address, uint16_t port, int backlog);

/*
 * Accept one connection and configure it non-blocking, close-on-exec and
 * TCP_NODELAY. Returns -1 with errno set; EAGAIN means no more connections.
 */
int nm_tcp_accept(int listening_fd);

#ifdef __cplusplus
}
#endif

#endif /* NETMIRROR_NETWORK_H */
