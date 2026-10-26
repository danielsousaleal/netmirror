/*
 * netmirror - external HTTP gateway
 *
 * The gateway is intentionally a thin HTTP/1.1 front end over the binary node
 * protocol. Writes are sent to exactly one coordinator node; that node performs
 * the asynchronous 2-of-3 replication and only then ACKs the gateway. Reads use
 * round-robin node selection with per-request fallback.
 *
 * The HTTP side is connection-oriented: one detached worker handles each keep-
 * alive client connection, so a slow node only delays the request waiting on
 * that node while other clients keep being served.
 */

#ifndef NETMIRROR_GATEWAY_H
#define NETMIRROR_GATEWAY_H

#include "netmirror.h"
#include "network.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define NM_GATEWAY_DEFAULT_PORT 8080u
#define NM_GATEWAY_DEFAULT_MAX_CONNECTIONS 128u
#define NM_GATEWAY_DEFAULT_NODE_CONNECT_TIMEOUT_MS 1000u
#define NM_GATEWAY_DEFAULT_NODE_IO_TIMEOUT_MS 10000u
#define NM_GATEWAY_DEFAULT_HTTP_TIMEOUT_MS 30000u
#define NM_GATEWAY_MAX_HEADER_BYTES 16384u
#define NM_GATEWAY_INPUT_BUFFER_BYTES 32768u
#define NM_GATEWAY_MAX_TARGET_BYTES 512u

typedef struct nm_gateway_options {
    char bind_address[NM_MAX_HOST];
    uint16_t port;
    nm_endpoint_t nodes[NM_MAX_PEERS];
    size_t node_count;
    size_t max_connections;
    int http_timeout_ms;
    int node_connect_timeout_ms;
    int node_io_timeout_ms;
} nm_gateway_options_t;

/*
 * Run the HTTP gateway until SIGINT/SIGTERM. Returns 0 on graceful shutdown.
 * The options struct must remain valid for the duration of the call; it is
 * copied internally.
 */
int nm_gateway_run(const nm_gateway_options_t *options);

#ifdef __cplusplus
}
#endif

#endif /* NETMIRROR_GATEWAY_H */
