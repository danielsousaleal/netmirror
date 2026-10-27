/*
 * netmirror - HTTP/1.1 gateway
 *
 * Design:
 *   - one detached worker per accepted HTTP connection;
 *   - the accept loop never waits on a storage node;
 *   - outbound node sockets are blocking only inside the request's worker and
 *     always carry connect/read/write timeouts;
 *   - writes go to one coordinator; the node performs asynchronous 2-of-3
 *     quorum replication;
 *   - request bodies are streamed in 4 MiB chunks and never buffered whole;
 *   - GET responses stream one verified 4 MiB chunk at a time to the client;
 *   - HTTP/1.1 keep-alive and bounded pipelining are supported through a
 *     per-connection input buffer that preserves bytes after Content-Length.
 */

#include "gateway.h"

#include "blob.h"
#include "network.h"
#include "util.h"

#include <errno.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define GW_MAX_HEADER_COUNT 100u
#define GW_MAX_TARGET (NM_GATEWAY_MAX_TARGET_BYTES + 1u)
#define GW_HTTP_INPUT_SIZE NM_GATEWAY_INPUT_BUFFER_BYTES
#define GW_HEADER_SCRATCH (NM_GATEWAY_MAX_HEADER_BYTES + 1u)
#define GW_MAX_LIST_KEYS 65536u
#define GW_STATS_TIMEOUT_MS 2000u
#define GW_ACTIVITY_MAX 50u
#define GW_ACTIVITY_TYPE_MAX 16u
#define GW_ACTIVITY_MESSAGE_MAX 192u

typedef struct gw_shared gw_shared_t;
typedef struct gw_client gw_client_t;

typedef enum gw_method {
    GW_METHOD_GET = 0,
    GW_METHOD_PUT,
    GW_METHOD_DELETE,
    GW_METHOD_OPTIONS
} gw_method_t;

typedef struct gw_request {
    gw_method_t method;
    bool keep_alive;
    bool expect_continue;
    bool has_content_length;
    bool is_health;
    bool is_list;
    bool is_cluster_stats;
    bool is_activity;
    uint64_t content_length;
    char key[NM_MAX_KEY];
} gw_request_t;

typedef struct gw_key_list {
    char **keys;
    size_t count;
    size_t capacity;
} gw_key_list_t;

typedef struct gw_activity_event {
    uint64_t id;
    uint64_t timestamp_ms;
    char type[GW_ACTIVITY_TYPE_MAX];
    char message[GW_ACTIVITY_MESSAGE_MAX];
} gw_activity_event_t;

struct gw_client {
    int fd;
    gw_shared_t *shared;
    uint8_t input[GW_HTTP_INPUT_SIZE];
    size_t input_start;
    size_t input_end;
    gw_client_t *next;
};

struct gw_shared {
    nm_gateway_options_t options;
    int listen_fd;
    pthread_mutex_t lock;
    pthread_cond_t condition;
    gw_client_t *clients;
    size_t active_clients;
    atomic_bool stopping;
    atomic_uint_fast32_t round_robin;
    pthread_mutex_t activity_lock;
    gw_activity_event_t activity[GW_ACTIVITY_MAX];
    size_t activity_count;
    size_t activity_next;
    uint64_t activity_last_id;
    bool node_status_known[NM_MAX_PEERS];
    bool node_online[NM_MAX_PEERS];
};

typedef struct gw_node_stats {
    char id[NM_MAX_HOST + 16u];
    bool online;
    uint64_t disk_total;
    uint64_t disk_free;
    uint64_t chunk_count;
    uint64_t object_count;
} gw_node_stats_t;

typedef struct gw_stats_request {
    const gw_shared_t *shared;
    size_t index;
    gw_node_stats_t *result;
} gw_stats_request_t;

static volatile sig_atomic_t stop_requested = 0;

static void handle_shutdown_signal(int signal_number)
{
    (void)signal_number;
    stop_requested = 1;
}

/* ------------------------------------------------------------------------- */
/* Configuration                                                             */
/* ------------------------------------------------------------------------- */

static bool parse_u32(const char *text, uint32_t minimum, uint32_t maximum,
                      uint32_t *out)
{
    unsigned long value;
    char *end = NULL;

    if (text == NULL || text[0] == '\0' || out == NULL) {
        return false;
    }
    errno = 0;
    value = strtoul(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value < minimum ||
        value > maximum) {
        return false;
    }
    *out = (uint32_t)value;
    return true;
}

static bool read_env_u32(const char *name, uint32_t fallback,
                         uint32_t minimum, uint32_t maximum, uint32_t *out)
{
    const char *value = getenv(name);
    if (value == NULL || value[0] == '\0') {
        *out = fallback;
        return true;
    }
    if (!parse_u32(value, minimum, maximum, out)) {
        nm_log(NM_LOG_ERROR,
               "invalid %s='%s' (expected integer in [%u, %u])", name, value,
               (unsigned int)minimum, (unsigned int)maximum);
        return false;
    }
    return true;
}

static bool read_env_string(const char *name, const char *fallback,
                            char *out, size_t out_size)
{
    const char *value = getenv(name);
    size_t length;

    if (out == NULL || out_size == 0u) {
        return false;
    }
    if (value == NULL || value[0] == '\0') {
        value = fallback;
    }
    length = strlen(value);
    if (length >= out_size) {
        nm_log(NM_LOG_ERROR, "%s is too long (limit %zu bytes)", name,
               out_size - 1u);
        return false;
    }
    memcpy(out, value, length + 1u);
    return true;
}

static bool parse_node_list(const char *text, nm_endpoint_t *nodes,
                            size_t *node_count)
{
    char buffer[2048];
    char *save = NULL;
    char *token;
    size_t count = 0u;
    size_t length;

    if (text == NULL || nodes == NULL || node_count == NULL) {
        return false;
    }
    memset(nodes, 0, sizeof(nm_endpoint_t) * NM_MAX_PEERS);
    *node_count = 0u;
    if (text[0] == '\0') {
        return false;
    }
    length = strlen(text);
    if (length >= sizeof(buffer)) {
        return false;
    }
    memcpy(buffer, text, length + 1u);
    token = strtok_r(buffer, ",", &save);
    while (token != NULL) {
        while (*token == ' ' || *token == '\t') {
            ++token;
        }
        char *end = token + strlen(token);
        while (end > token && (end[-1] == ' ' || end[-1] == '\t')) {
            *--end = '\0';
        }
        if (*token == '\0' || count >= NM_MAX_PEERS ||
            nm_endpoint_parse(token, &nodes[count]) != NM_OK) {
            return false;
        }
        count += 1u;
        token = strtok_r(NULL, ",", &save);
    }
    *node_count = count;
    return count > 0u;
}

static void gateway_usage(FILE *stream)
{
    (void)fprintf(
        stream,
        "Usage: netmirror-gateway [options]\n"
        "  --bind <addr>                  bind address (default 0.0.0.0)\n"
        "  --port <port>                  HTTP port (default 8080)\n"
        "  --nodes <host:port,...>        storage nodes\n"
        "  --max-connections <n>          concurrent HTTP connection cap\n"
        "  --http-timeout-ms <n>          HTTP client I/O timeout\n"
        "  --node-connect-timeout-ms <n>  storage node connect timeout\n"
        "  --node-timeout-ms <n>          storage node RPC timeout\n"
        "  --log-level <error|warn|info|debug>\n"
        "  --help                         show this help\n");
}

static bool option_string(char *out, size_t out_size, const char *value)
{
    size_t length;

    if (value == NULL || out == NULL || out_size == 0u) {
        return false;
    }
    length = strlen(value);
    if (length >= out_size) {
        return false;
    }
    memcpy(out, value, length + 1u);
    return true;
}

static bool apply_gateway_arguments(nm_gateway_options_t *options, int argc,
                                    char **argv)
{
    for (int i = 1; i < argc; ++i) {
        const char *option = argv[i];
        const char *value;
        uint32_t parsed;

        if (strcmp(option, "--help") == 0 || strcmp(option, "-h") == 0) {
            gateway_usage(stdout);
            exit(EXIT_SUCCESS);
        }
        if (i + 1 >= argc) {
            nm_log(NM_LOG_ERROR, "missing value for %s", option);
            return false;
        }
        value = argv[++i];
        if (strcmp(option, "--bind") == 0) {
            if (!option_string(options->bind_address,
                               sizeof(options->bind_address), value)) {
                return false;
            }
        } else if (strcmp(option, "--nodes") == 0) {
            if (!parse_node_list(value, options->nodes,
                                 &options->node_count)) {
                return false;
            }
        } else if (strcmp(option, "--port") == 0) {
            if (!parse_u32(value, 1u, 65535u, &parsed)) {
                return false;
            }
            options->port = (uint16_t)parsed;
        } else if (strcmp(option, "--max-connections") == 0) {
            if (!parse_u32(value, 1u, 4096u, &parsed)) {
                return false;
            }
            options->max_connections = parsed;
        } else if (strcmp(option, "--http-timeout-ms") == 0) {
            if (!parse_u32(value, 1000u, 300000u, &parsed)) {
                return false;
            }
            options->http_timeout_ms = (int)parsed;
        } else if (strcmp(option, "--node-connect-timeout-ms") == 0) {
            if (!parse_u32(value, 100u, 60000u, &parsed)) {
                return false;
            }
            options->node_connect_timeout_ms = (int)parsed;
        } else if (strcmp(option, "--node-timeout-ms") == 0) {
            if (!parse_u32(value, 500u, 300000u, &parsed)) {
                return false;
            }
            options->node_io_timeout_ms = (int)parsed;
        } else if (strcmp(option, "--log-level") == 0) {
            nm_log_set_level(nm_log_level_from_string(value, NM_LOG_INFO));
        } else {
            nm_log(NM_LOG_ERROR, "unknown option '%s'", option);
            return false;
        }
    }
    return options->node_count > 0u;
}

static bool load_options(nm_gateway_options_t *options)
{
    uint32_t port = NM_GATEWAY_DEFAULT_PORT;
    uint32_t max_connections = NM_GATEWAY_DEFAULT_MAX_CONNECTIONS;
    uint32_t http_timeout = NM_GATEWAY_DEFAULT_HTTP_TIMEOUT_MS;
    uint32_t connect_timeout = NM_GATEWAY_DEFAULT_NODE_CONNECT_TIMEOUT_MS;
    uint32_t node_timeout = NM_GATEWAY_DEFAULT_NODE_IO_TIMEOUT_MS;
    char nodes_text[2048];
    const char *level_text;
    nm_log_level_t level = NM_LOG_INFO;

    memset(options, 0, sizeof(*options));
    level_text = getenv("NM_LOG_LEVEL");
    if (level_text != NULL && level_text[0] != '\0') {
        level = nm_log_level_from_string(level_text, NM_LOG_INFO);
    }
    nm_log_set_level(level);

    if (!read_env_string("NM_GATEWAY_BIND", "0.0.0.0", options->bind_address,
                         sizeof(options->bind_address)) ||
        !read_env_string("NM_NODES",
                         "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003",
                         nodes_text, sizeof(nodes_text)) ||
        !read_env_u32("NM_GATEWAY_PORT", NM_GATEWAY_DEFAULT_PORT, 1u, 65535u,
                      &port) ||
        !read_env_u32("NM_GATEWAY_MAX_CONNECTIONS",
                      NM_GATEWAY_DEFAULT_MAX_CONNECTIONS, 1u, 4096u,
                      &max_connections) ||
        !read_env_u32("NM_HTTP_TIMEOUT_MS",
                      NM_GATEWAY_DEFAULT_HTTP_TIMEOUT_MS, 1000u, 300000u,
                      &http_timeout) ||
        !read_env_u32("NM_NODE_CONNECT_TIMEOUT_MS",
                      NM_GATEWAY_DEFAULT_NODE_CONNECT_TIMEOUT_MS, 100u, 60000u,
                      &connect_timeout) ||
        !read_env_u32("NM_NODE_TIMEOUT_MS",
                      NM_GATEWAY_DEFAULT_NODE_IO_TIMEOUT_MS, 500u, 300000u,
                      &node_timeout) ||
        !parse_node_list(nodes_text, options->nodes, &options->node_count)) {
        nm_log(NM_LOG_ERROR, "invalid gateway configuration");
        return false;
    }
    options->port = (uint16_t)port;
    options->max_connections = max_connections;
    options->http_timeout_ms = (int)http_timeout;
    options->node_connect_timeout_ms = (int)connect_timeout;
    options->node_io_timeout_ms = (int)node_timeout;
    return true;
}

/* ------------------------------------------------------------------------- */
/* HTTP low-level helpers                                                    */
/* ------------------------------------------------------------------------- */

static const char *http_reason(int status)
{
    switch (status) {
    case 100:
        return "Continue";
    case 200:
        return "OK";
    case 201:
        return "Created";
    case 204:
        return "No Content";
    case 400:
        return "Bad Request";
    case 404:
        return "Not Found";
    case 405:
        return "Method Not Allowed";
    case 408:
        return "Request Timeout";
    case 411:
        return "Length Required";
    case 413:
        return "Payload Too Large";
    case 417:
        return "Expectation Failed";
    case 431:
        return "Request Header Fields Too Large";
    case 500:
        return "Internal Server Error";
    case 501:
        return "Not Implemented";
    case 502:
        return "Bad Gateway";
    case 503:
        return "Service Unavailable";
    case 504:
        return "Gateway Timeout";
    case 505:
        return "HTTP Version Not Supported";
    default:
        return "Error";
    }
}

static bool send_all(int fd, const void *data, size_t length)
{
    return nm_write_all(fd, data, length) == NM_OK;
}

static bool send_response_head(gw_client_t *client, int status,
                               const char *content_type,
                               uint64_t content_length, bool keep_alive,
                               const char *extra_headers)
{
    char header[2048];
    int written;

    written = snprintf(
        header, sizeof(header),
        "HTTP/1.1 %d %s\r\n"
        "Server: netmirror-gateway\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: GET, PUT, DELETE, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Content-Type\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %llu\r\n"
        "Connection: %s\r\n"
        "Cache-Control: no-store\r\n"
        "%s"
        "\r\n",
        status, http_reason(status), content_type,
        (unsigned long long)content_length,
        keep_alive ? "keep-alive" : "close",
        extra_headers != NULL ? extra_headers : "");
    if (written < 0 || (size_t)written >= sizeof(header)) {
        return false;
    }
    return send_all(client->fd, header, (size_t)written);
}

static bool send_error(gw_client_t *client, int status)
{
    char body[192];
    const char *extra =
        status == 405 ? "Allow: GET, PUT, DELETE, OPTIONS\r\n" : "";
    int body_length;

    body_length = snprintf(body, sizeof(body),
                           "{\"error\":%d,\"message\":\"%s\"}\n", status,
                           http_reason(status));
    if (body_length < 0 || (size_t)body_length >= sizeof(body)) {
        return false;
    }
    if (!send_response_head(client, status, "application/json",
                            (uint64_t)body_length, false, extra)) {
        return false;
    }
    return send_all(client->fd, body, (size_t)body_length);
}

static bool send_continue(gw_client_t *client)
{
    static const char response[] = "HTTP/1.1 100 Continue\r\n\r\n";
    return send_all(client->fd, response, sizeof(response) - 1u);
}

/* ------------------------------------------------------------------------- */
/* HTTP request parsing                                                      */
/* ------------------------------------------------------------------------- */

static void trim_ows(char **start)
{
    char *value = *start;
    while (*value == ' ' || *value == '\t') {
        ++value;
    }
    char *end = value + strlen(value);
    while (end > value && (end[-1] == ' ' || end[-1] == '\t')) {
        *--end = '\0';
    }
    *start = value;
}

static bool header_name_valid(const char *name)
{
    if (*name == '\0') {
        return false;
    }
    for (const unsigned char *cursor = (const unsigned char *)name;
         *cursor != '\0'; ++cursor) {
        unsigned char c = *cursor;
        bool alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
        bool digit = c >= '0' && c <= '9';
        bool punctuation = strchr("!#$%&'*+-.^_`|~", (int)c) != NULL;
        if (!alpha && !digit && !punctuation) {
            return false;
        }
    }
    return true;
}

static bool parse_content_length(const char *value, uint64_t *out)
{
    uint64_t length = 0u;

    if (value == NULL || *value == '\0' || out == NULL) {
        return false;
    }
    for (const unsigned char *cursor = (const unsigned char *)value;
         *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') {
            return false;
        }
        uint64_t digit = (uint64_t)(*cursor - '0');
        if (length > (UINT64_MAX - digit) / 10u) {
            return false;
        }
        length = (length * 10u) + digit;
    }
    *out = length;
    return true;
}

static int connection_mode(const char *value, bool *close_seen,
                           bool *keep_alive_seen)
{
    char buffer[512];
    char *save = NULL;
    char *token;

    if (strlen(value) >= sizeof(buffer)) {
        return 400;
    }
    memcpy(buffer, value, strlen(value) + 1u);
    token = strtok_r(buffer, ",", &save);
    while (token != NULL) {
        char *trimmed = token;
        trim_ows(&trimmed);
        if (strcasecmp(trimmed, "close") == 0) {
            *close_seen = true;
        } else if (strcasecmp(trimmed, "keep-alive") == 0) {
            *keep_alive_seen = true;
        }
        token = strtok_r(NULL, ",", &save);
    }
    return 0;
}

static int resolve_target(const char *target, gw_request_t *request)
{
    if (target == NULL || target[0] != '/' ||
        strlen(target) >= NM_GATEWAY_MAX_TARGET_BYTES ||
        strchr(target, '?') != NULL || strchr(target, '#') != NULL ||
        strchr(target, '%') != NULL) {
        return 400;
    }
    if (strcmp(target, "/health") == 0) {
        request->is_health = true;
        return 0;
    }
    if (strcmp(target, "/cluster/stats") == 0) {
        request->is_cluster_stats = true;
        return 0;
    }
    if (strcmp(target, "/cluster/activity") == 0) {
        request->is_activity = true;
        return 0;
    }
    if (strcmp(target, "/objects") == 0) {
        request->is_list = true;
        return 0;
    }
    if (strcmp(target, "/obj") == 0) {
        memcpy(request->key, "default", sizeof("default"));
        return 0;
    }
    if (strncmp(target, "/obj/", 5u) != 0) {
        return 404;
    }
    const char *key = target + 5u;
    if (*key == '\0' || strchr(key, '/') != NULL ||
        !nm_key_is_valid(key)) {
        return 404;
    }
    memcpy(request->key, key, strlen(key) + 1u);
    return 0;
}

static int parse_http_request(char *header, gw_request_t *request)
{
    char *cursor = header;
    bool first_line = true;
    unsigned int header_count = 0u;
    bool close_seen = false;
    bool keep_alive_seen = false;
    bool transfer_encoding = false;
    bool version_http11 = false;
    bool expect_seen = false;
    const char *method_text = NULL;
    char *target = NULL;
    const char *version = NULL;

    memset(request, 0, sizeof(*request));
    while (*cursor != '\0') {
        char *line_end = strstr(cursor, "\r\n");
        char *line = cursor;
        if (line_end != NULL) {
            *line_end = '\0';
            cursor = line_end + 2;
        } else {
            cursor += strlen(cursor);
        }
        if (strchr(line, '\r') != NULL || strchr(line, '\n') != NULL) {
            return 400;
        }
        if (first_line) {
            char *space1 = strchr(line, ' ');
            if (space1 == NULL) {
                return 400;
            }
            *space1 = '\0';
            method_text = line;
            target = space1 + 1;
            char *space2 = strchr(target, ' ');
            if (space2 == NULL) {
                return 400;
            }
            *space2 = '\0';
            version = space2 + 1;
            if (*method_text == '\0' || *target == '\0' || *version == '\0' ||
                strchr(version, ' ') != NULL || strchr(target, '\t') != NULL) {
                return 400;
            }
            first_line = false;
        } else {
            if (*line == '\0' || *line == ' ' || *line == '\t') {
                return 400;
            }
            header_count += 1u;
            if (header_count > GW_MAX_HEADER_COUNT) {
                return 431;
            }
            char *colon = strchr(line, ':');
            if (colon == NULL) {
                return 400;
            }
            *colon = '\0';
            if (!header_name_valid(line)) {
                return 400;
            }
            char *value = colon + 1;
            trim_ows(&value);
            if (strcasecmp(line, "Content-Length") == 0) {
                uint64_t length;
                if (request->has_content_length ||
                    !parse_content_length(value, &length)) {
                    return 400;
                }
                if (length > NM_MAX_OBJECT_SIZE) {
                    return 413;
                }
                request->has_content_length = true;
                request->content_length = length;
            } else if (strcasecmp(line, "Transfer-Encoding") == 0) {
                transfer_encoding = true;
            } else if (strcasecmp(line, "Connection") == 0) {
                int status = connection_mode(value, &close_seen,
                                             &keep_alive_seen);
                if (status != 0) {
                    return status;
                }
            } else if (strcasecmp(line, "Expect") == 0) {
                if (strcasecmp(value, "100-continue") != 0) {
                    return 417;
                }
                expect_seen = true;
            }
        }
    }
    if (method_text == NULL || target == NULL || version == NULL) {
        return 400;
    }
    if (strcmp(version, "HTTP/1.1") == 0) {
        version_http11 = true;
    } else if (strcmp(version, "HTTP/1.0") != 0) {
        return 505;
    }
    if (transfer_encoding) {
        return 501;
    }
    if (strcmp(method_text, "GET") == 0) {
        request->method = GW_METHOD_GET;
    } else if (strcmp(method_text, "PUT") == 0) {
        request->method = GW_METHOD_PUT;
    } else if (strcmp(method_text, "DELETE") == 0) {
        request->method = GW_METHOD_DELETE;
    } else if (strcmp(method_text, "OPTIONS") == 0) {
        request->method = GW_METHOD_OPTIONS;
    } else {
        return 405;
    }
    request->keep_alive =
        version_http11 ? !close_seen : keep_alive_seen;
    if (close_seen) {
        request->keep_alive = false;
    }
    if (request->method == GW_METHOD_OPTIONS) {
        if (target[0] != '/' ||
            strlen(target) >= NM_GATEWAY_MAX_TARGET_BYTES) {
            return 400;
        }
        if (expect_seen) {
            return 417;
        }
        if (request->has_content_length && request->content_length != 0u) {
            return 400;
        }
        request->expect_continue = false;
        return 0;
    }
    int status = resolve_target(target, request);
    if (status != 0) {
        return status;
    }
    request->expect_continue =
        expect_seen && request->method == GW_METHOD_PUT;

    if (request->is_health) {
        if (request->method != GW_METHOD_GET) {
            return 405;
        }
        if (request->has_content_length && request->content_length != 0u) {
            return 400;
        }
        request->expect_continue = false;
        return 0;
    }
    if (request->is_cluster_stats) {
        if (request->method != GW_METHOD_GET) {
            return 405;
        }
        if (request->has_content_length && request->content_length != 0u) {
            return 400;
        }
        if (expect_seen) {
            return 417;
        }
        request->expect_continue = false;
        return 0;
    }
    if (request->is_activity) {
        if (request->method != GW_METHOD_GET) {
            return 405;
        }
        if (request->has_content_length && request->content_length != 0u) {
            return 400;
        }
        if (expect_seen) {
            return 417;
        }
        request->expect_continue = false;
        return 0;
    }
    if (request->is_list) {
        if (request->method != GW_METHOD_GET) {
            return 405;
        }
        if (request->has_content_length && request->content_length != 0u) {
            return 400;
        }
        if (expect_seen) {
            return 417;
        }
        request->expect_continue = false;
        return 0;
    }
    if (request->method == GW_METHOD_PUT) {
        if (!request->has_content_length) {
            return 411;
        }
        return 0;
    }
    if (request->method == GW_METHOD_DELETE) {
        if (request->has_content_length && request->content_length != 0u) {
            return 400;
        }
        if (expect_seen) {
            return 417;
        }
        request->expect_continue = false;
        return 0;
    }
    if (expect_seen) {
        return 417;
    }
    if (request->has_content_length && request->content_length != 0u) {
        return 400;
    }
    return 0;
}

/* ------------------------------------------------------------------------- */
/* Buffered HTTP input                                                       */
/* ------------------------------------------------------------------------- */

static void input_compact(gw_client_t *client)
{
    if (client->input_start == 0u) {
        return;
    }
    size_t available = client->input_end - client->input_start;
    if (available > 0u) {
        memmove(client->input, client->input + client->input_start, available);
    }
    client->input_start = 0u;
    client->input_end = available;
}

static int read_http_header(gw_client_t *client, char *header, size_t capacity,
                            bool *peer_closed)
{
    *peer_closed = false;
    for (;;) {
        size_t available = client->input_end - client->input_start;
        const uint8_t *data = client->input + client->input_start;
        size_t found = SIZE_MAX;
        for (size_t i = 0u; i + 3u < available; ++i) {
            if (data[i] == '\r' && data[i + 1u] == '\n' &&
                data[i + 2u] == '\r' && data[i + 3u] == '\n') {
                found = i;
                break;
            }
        }
        if (found != SIZE_MAX) {
            if (found > NM_GATEWAY_MAX_HEADER_BYTES ||
                found + 1u > capacity) {
                return 431;
            }
            memcpy(header, data, found);
            header[found] = '\0';
            client->input_start += found + 4u;
            if (client->input_start == client->input_end) {
                client->input_start = 0u;
                client->input_end = 0u;
            }
            return 0;
        }
        if (available > NM_GATEWAY_MAX_HEADER_BYTES) {
            return 431;
        }
        if (client->input_end == sizeof(client->input)) {
            if (client->input_start > 0u) {
                input_compact(client);
            } else {
                return 431;
            }
        }
        ssize_t received = recv(client->fd,
                                client->input + client->input_end,
                                sizeof(client->input) - client->input_end, 0);
        if (received > 0) {
            client->input_end += (size_t)received;
            continue;
        }
        if (received == 0) {
            if (available == 0u) {
                *peer_closed = true;
                return 0;
            }
            return 400;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (available == 0u) {
                *peer_closed = true;
                return 0;
            }
            return 408;
        }
        *peer_closed = true;
        return 0;
    }
}

static int read_body_exact(gw_client_t *client, uint8_t *destination,
                           size_t length)
{
    size_t copied = client->input_end - client->input_start;
    if (copied > length) {
        copied = length;
    }
    if (copied > 0u) {
        memcpy(destination, client->input + client->input_start, copied);
        client->input_start += copied;
        if (client->input_start == client->input_end) {
            client->input_start = 0u;
            client->input_end = 0u;
        }
    }
    size_t filled = copied;
    while (filled < length) {
        ssize_t received = recv(client->fd, destination + filled,
                                length - filled, 0);
        if (received > 0) {
            filled += (size_t)received;
            continue;
        }
        if (received == 0) {
            return 400;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 408;
        }
        return 400;
    }
    return 0;
}

/* ------------------------------------------------------------------------- */
/* Node RPC helpers                                                          */
/* ------------------------------------------------------------------------- */

static size_t node_start(const gw_shared_t *shared)
{
    uint32_t index =
        atomic_fetch_add_explicit(&((gw_shared_t *)shared)->round_robin, 1u,
                                  memory_order_relaxed);
    return (size_t)(index % shared->options.node_count);
}

static int connect_node(const gw_shared_t *shared, size_t index)
{
    int fd;

    if (index >= shared->options.node_count) {
        return -1;
    }
    fd = nm_tcp_connect(shared->options.nodes[index].host,
                        shared->options.nodes[index].port,
                        shared->options.node_connect_timeout_ms);
    if (fd < 0) {
        return -1;
    }
    if (nm_fd_set_nonblocking(fd, false) != NM_OK ||
        nm_set_socket_timeouts(fd, shared->options.node_io_timeout_ms,
                               shared->options.node_io_timeout_ms) != NM_OK) {
        nm_close_fd(&fd);
        return -1;
    }
    return fd;
}

static int connect_node_with_timeout(const gw_shared_t *shared, size_t index,
                                     int timeout_ms)
{
    int fd;

    if (shared == NULL || timeout_ms <= 0 ||
        index >= shared->options.node_count) {
        return -1;
    }
    fd = nm_tcp_connect(shared->options.nodes[index].host,
                        shared->options.nodes[index].port, timeout_ms);
    if (fd < 0) {
        return -1;
    }
    if (nm_fd_set_nonblocking(fd, false) != NM_OK ||
        nm_set_socket_timeouts(fd, timeout_ms, timeout_ms) != NM_OK) {
        nm_close_fd(&fd);
        return -1;
    }
    return fd;
}

static bool decode_error(const nm_frame_t *frame, nm_status_t *status)
{
    char message[NM_MAX_ERROR_MSG + 1u];
    if (frame == NULL || status == NULL || frame->payload == NULL) {
        return false;
    }
    return nm_wire_error_decode(frame->payload, frame->length, status,
                                message, sizeof(message)) == NM_OK;
}

static bool receive_ack(int fd, uint8_t expected_type,
                        const uint8_t digest[NM_SHA256_LEN],
                        nm_status_t *status)
{
    nm_frame_t frame;
    uint8_t acknowledged_type = 0u;
    uint8_t response_digest[NM_SHA256_LEN];
    int result;

    memset(&frame, 0, sizeof(frame));
    result = nm_frame_recv(fd, &frame);
    if (result != NM_OK) {
        return false;
    }
    if (frame.type == NM_MSG_ERROR) {
        bool decoded = decode_error(&frame, status);
        nm_frame_free(&frame);
        return decoded;
    }
    if (frame.type != NM_MSG_ACK) {
        nm_frame_free(&frame);
        return false;
    }
    result = nm_wire_ack_decode(frame.payload, frame.length, status,
                                &acknowledged_type, response_digest);
    nm_frame_free(&frame);
    if (result != NM_OK || acknowledged_type != expected_type ||
        memcmp(response_digest, digest, NM_SHA256_LEN) != 0) {
        return false;
    }
    return true;
}

static bool store_chunk_on_node(int fd, const uint8_t digest[NM_SHA256_LEN],
                                const uint8_t *data, size_t length,
                                nm_status_t *status)
{
    uint8_t header[NM_FRAME_HEADER_SIZE];

    if (nm_frame_header_encode(header, NM_MSG_STORE,
                               NM_SHA256_LEN + (uint64_t)length) != NM_OK ||
        !send_all(fd, header, sizeof(header)) ||
        !send_all(fd, digest, NM_SHA256_LEN) ||
        !send_all(fd, data, length)) {
        return false;
    }
    return receive_ack(fd, NM_MSG_STORE, digest, status);
}

static bool store_manifest_on_node(int fd, const nm_manifest_t *manifest,
                                   const uint8_t key_digest[NM_SHA256_LEN],
                                   nm_status_t *status)
{
    uint8_t *serialized = NULL;
    size_t serialized_length = 0u;
    int result;
    bool success;

    result = nm_manifest_serialize(manifest, &serialized, &serialized_length);
    if (result != NM_OK) {
        return false;
    }
    result = nm_frame_send(fd, NM_MSG_STORE_OBJECT, serialized,
                           (uint64_t)serialized_length);
    free(serialized);
    if (result != NM_OK) {
        return false;
    }
    success = receive_ack(fd, NM_MSG_STORE_OBJECT, key_digest, status);
    return success;
}

static nm_status_t fetch_manifest_from_nodes(gw_shared_t *shared,
                                             const char *key,
                                             nm_manifest_t *manifest)
{
    size_t start = node_start(shared);
    nm_manifest_t best;
    bool saw_manifest = false;
    bool saw_not_found = false;

    memset(&best, 0, sizeof(best));
    for (size_t attempt = 0u; attempt < shared->options.node_count; ++attempt) {
        size_t index = (start + attempt) % shared->options.node_count;
        int fd = connect_node(shared, index);
        nm_frame_t frame;
        nm_status_t status = NM_ERR_UNAVAILABLE;
        int result;

        if (fd < 0) {
            continue;
        }
        memset(&frame, 0, sizeof(frame));
        if (nm_frame_send(fd, NM_MSG_FETCH_OBJECT, key, strlen(key)) !=
            NM_OK) {
            nm_close_fd(&fd);
            continue;
        }
        result = nm_frame_recv(fd, &frame);
        if (result != NM_OK) {
            nm_close_fd(&fd);
            continue;
        }
        if (frame.type == NM_MSG_ERROR) {
            if (decode_error(&frame, &status) &&
                status == NM_ERR_NOT_FOUND) {
                saw_not_found = true;
            }
            nm_frame_free(&frame);
            nm_close_fd(&fd);
            continue;
        }
        if (frame.type == NM_MSG_OBJECT) {
            nm_manifest_t candidate;
            memset(&candidate, 0, sizeof(candidate));
            result = nm_manifest_deserialize(frame.payload, frame.length,
                                             &candidate);
            nm_frame_free(&frame);
            nm_close_fd(&fd);
            if (result != NM_OK || strcmp(candidate.key, key) != 0) {
                nm_manifest_free(&candidate);
                continue;
            }
            if (!saw_manifest ||
                candidate.updated_ms > best.updated_ms ||
                (candidate.updated_ms == best.updated_ms &&
                 candidate.deleted && !best.deleted)) {
                nm_manifest_free(&best);
                best = candidate;
                saw_manifest = true;
            } else {
                nm_manifest_free(&candidate);
            }
            continue;
        }
        nm_frame_free(&frame);
        nm_close_fd(&fd);
    }
    if (saw_manifest) {
        *manifest = best;
        return NM_OK;
    }
    nm_manifest_free(&best);
    return saw_not_found ? NM_ERR_NOT_FOUND : NM_ERR_UNAVAILABLE;
}

static nm_status_t fetch_chunk_from_nodes(
    gw_shared_t *shared, const uint8_t digest[NM_SHA256_LEN],
    size_t expected_length, uint8_t **data_out)
{
    size_t start = node_start(shared);
    uint8_t *result_data = NULL;

    if (data_out == NULL || expected_length == 0u ||
        expected_length > NM_CHUNK_SIZE) {
        return NM_ERR_INVALID;
    }
    *data_out = NULL;
    for (size_t attempt = 0u; attempt < shared->options.node_count; ++attempt) {
        size_t index = (start + attempt) % shared->options.node_count;
        int fd = connect_node(shared, index);
        nm_frame_t frame;
        int result;

        if (fd < 0) {
            continue;
        }
        memset(&frame, 0, sizeof(frame));
        if (nm_frame_send(fd, NM_MSG_FETCH, digest, NM_SHA256_LEN) != NM_OK) {
            nm_close_fd(&fd);
            continue;
        }
        result = nm_frame_recv(fd, &frame);
        if (result != NM_OK) {
            nm_close_fd(&fd);
            continue;
        }
        nm_close_fd(&fd);
        if (frame.type != NM_MSG_CHUNK || frame.payload == NULL ||
            frame.length != (uint64_t)(NM_SHA256_LEN + expected_length) ||
            memcmp(frame.payload, digest, NM_SHA256_LEN) != 0) {
            nm_frame_free(&frame);
            continue;
        }
        uint8_t actual[NM_SHA256_LEN];
        result = nm_sha256_buf(frame.payload + NM_SHA256_LEN,
                               expected_length, actual);
        if (result != NM_OK ||
            memcmp(actual, digest, NM_SHA256_LEN) != 0) {
            nm_frame_free(&frame);
            continue;
        }
        result_data = malloc(expected_length);
        if (result_data == NULL) {
            nm_frame_free(&frame);
            return NM_ERR_NOMEM;
        }
        memcpy(result_data, frame.payload + NM_SHA256_LEN, expected_length);
        nm_frame_free(&frame);
        *data_out = result_data;
        return NM_OK;
    }
    return NM_ERR_UNAVAILABLE;
}

static int coordinator_connect(gw_shared_t *shared)
{
    size_t start = node_start(shared);

    for (size_t attempt = 0u; attempt < shared->options.node_count; ++attempt) {
        size_t index = (start + attempt) % shared->options.node_count;
        int fd = connect_node(shared, index);
        if (fd >= 0) {
            return fd;
        }
    }
    return -1;
}

static int http_status_for(nm_status_t status)
{
    switch (status) {
    case NM_ERR_NOT_FOUND:
        return 404;
    case NM_ERR_TOO_LARGE:
        return 413;
    case NM_ERR_QUORUM:
    case NM_ERR_UNAVAILABLE:
    case NM_ERR_BUSY:
        return 503;
    case NM_ERR_TIMEOUT:
        return 504;
    default:
        return 500;
    }
}

static void gw_key_list_free(gw_key_list_t *list)
{
    if (list == NULL) {
        return;
    }
    for (size_t i = 0u; i < list->count; ++i) {
        free(list->keys[i]);
    }
    free(list->keys);
    memset(list, 0, sizeof(*list));
}

static int gw_key_list_append(gw_key_list_t *list, const char *key)
{
    char *copy;
    char **new_keys;
    size_t new_capacity;
    size_t key_length;

    if (list == NULL || key == NULL) {
        return NM_ERR_INVALID;
    }
    if (list->count >= GW_MAX_LIST_KEYS) {
        return NM_ERR_TOO_LARGE;
    }
    if (list->count == list->capacity) {
        new_capacity = list->capacity == 0u ? 32u : list->capacity * 2u;
        if (new_capacity > GW_MAX_LIST_KEYS) {
            new_capacity = GW_MAX_LIST_KEYS;
        }
        new_keys = realloc(list->keys, new_capacity * sizeof(*new_keys));
        if (new_keys == NULL) {
            return NM_ERR_NOMEM;
        }
        list->keys = new_keys;
        list->capacity = new_capacity;
    }
    key_length = strlen(key);
    copy = malloc(key_length + 1u);
    if (copy == NULL) {
        return NM_ERR_NOMEM;
    }
    memcpy(copy, key, key_length + 1u);
    list->keys[list->count] = copy;
    list->count += 1u;
    return NM_OK;
}

static int build_object_list_json(const gw_key_list_t *list, char **out,
                                  size_t *out_length)
{
    static const char prefix[] = "{\"objects\":[";
    size_t length = (sizeof(prefix) - 1u) + 2u + 1u;
    size_t cursor;
    char *json;

    if (list == NULL || out == NULL || out_length == NULL) {
        return NM_ERR_INVALID;
    }
    *out = NULL;
    *out_length = 0u;
    for (size_t i = 0u; i < list->count; ++i) {
        size_t key_length = strlen(list->keys[i]);
        size_t extra = key_length + 2u + (i == 0u ? 0u : 1u);
        if (length > SIZE_MAX - extra) {
            return NM_ERR_TOO_LARGE;
        }
        length += extra;
    }
    json = malloc(length);
    if (json == NULL) {
        return NM_ERR_NOMEM;
    }
    memcpy(json, prefix, sizeof(prefix) - 1u);
    cursor = sizeof(prefix) - 1u;
    for (size_t i = 0u; i < list->count; ++i) {
        size_t key_length = strlen(list->keys[i]);
        if (i > 0u) {
            json[cursor++] = ',';
        }
        json[cursor++] = '"';
        memcpy(json + cursor, list->keys[i], key_length);
        cursor += key_length;
        json[cursor++] = '"';
    }
    json[cursor++] = ']';
    json[cursor++] = '}';
    json[cursor] = '\0';
    if (cursor + 1u != length) {
        free(json);
        return NM_ERR_SYSTEM;
    }
    *out = json;
    *out_length = cursor;
    return NM_OK;
}

static nm_status_t fetch_object_list_from_node(gw_shared_t *shared,
                                               size_t index,
                                               gw_key_list_t *list)
{
    int fd;
    uint32_t offset = 0u;
    nm_status_t result = NM_ERR_UNAVAILABLE;

    if (shared == NULL || list == NULL) {
        return NM_ERR_INVALID;
    }
    fd = connect_node(shared, index);
    if (fd < 0) {
        return NM_ERR_UNAVAILABLE;
    }
    for (;;) {
        uint8_t request[NM_LIST_KEYS_REQ_WIRE_SIZE];
        nm_frame_t frame;
        uint32_t total;
        uint32_t returned;
        size_t cursor;
        int status;

        memset(&frame, 0, sizeof(frame));
        nm_put_u32_be(request, offset);
        nm_put_u32_be(request + 4u, NM_LIST_KEYS_DEFAULT_BATCH);
        status = nm_frame_send(fd, NM_MSG_LIST_OBJECTS_REQ, request,
                               sizeof(request));
        if (status != NM_OK) {
            result = NM_ERR_UNAVAILABLE;
            break;
        }
        status = nm_frame_recv(fd, &frame);
        if (status != NM_OK) {
            result = NM_ERR_UNAVAILABLE;
            break;
        }
        if (frame.type == NM_MSG_ERROR) {
            if (!decode_error(&frame, &result)) {
                result = NM_ERR_PROTOCOL;
            }
            nm_frame_free(&frame);
            break;
        }
        if (frame.type != NM_MSG_LIST_OBJECTS_RESP || frame.payload == NULL ||
            frame.length < NM_LIST_KEYS_RESP_HEADER_SIZE) {
            nm_frame_free(&frame);
            result = NM_ERR_PROTOCOL;
            break;
        }
        total = nm_get_u32_be(frame.payload);
        returned = nm_get_u32_be(frame.payload + 4u);
        if (returned > NM_LIST_KEYS_DEFAULT_BATCH) {
            nm_frame_free(&frame);
            result = NM_ERR_PROTOCOL;
            break;
        }
        result = NM_OK;
        cursor = NM_LIST_KEYS_RESP_HEADER_SIZE;
        for (uint32_t i = 0u; i < returned; ++i) {
            char key[NM_MAX_KEY];
            uint16_t key_length;

            if (cursor > frame.length || frame.length - cursor < 2u) {
                result = NM_ERR_PROTOCOL;
                break;
            }
            key_length = nm_get_u16_be(frame.payload + cursor);
            cursor += 2u;
            if (key_length == 0u || key_length >= NM_MAX_KEY ||
                cursor > frame.length ||
                frame.length - cursor < (uint64_t)key_length) {
                result = NM_ERR_PROTOCOL;
                break;
            }
            if (memchr(frame.payload + cursor, '\0', key_length) != NULL) {
                result = NM_ERR_PROTOCOL;
                break;
            }
            memcpy(key, frame.payload + cursor, key_length);
            key[key_length] = '\0';
            cursor += key_length;
            if (!nm_key_is_valid(key)) {
                result = NM_ERR_PROTOCOL;
                break;
            }
            status = gw_key_list_append(list, key);
            if (status != NM_OK) {
                result = (nm_status_t)status;
                break;
            }
        }
        if (result == NM_OK && cursor != frame.length) {
            result = NM_ERR_PROTOCOL;
        }
        nm_frame_free(&frame);
        if (result != NM_OK) {
            break;
        }
        if (returned == 0u) {
            result = offset >= total ? NM_OK : NM_ERR_PROTOCOL;
            break;
        }
        if (offset > UINT32_MAX - returned) {
            result = NM_ERR_PROTOCOL;
            break;
        }
        offset += returned;
        if (offset >= total) {
            result = NM_OK;
            break;
        }
        if (list->count >= GW_MAX_LIST_KEYS) {
            result = NM_ERR_TOO_LARGE;
            break;
        }
    }
    nm_close_fd(&fd);
    return result;
}

static nm_status_t fetch_object_list(gw_shared_t *shared, gw_key_list_t *list)
{
    size_t start;
    nm_status_t result = NM_ERR_UNAVAILABLE;

    if (shared == NULL || list == NULL) {
        return NM_ERR_INVALID;
    }
    start = node_start(shared);
    for (size_t attempt = 0u; attempt < shared->options.node_count; ++attempt) {
        size_t index = (start + attempt) % shared->options.node_count;
        gw_key_list_free(list);
        result = fetch_object_list_from_node(shared, index, list);
        if (result == NM_OK || result == NM_ERR_TOO_LARGE) {
            return result;
        }
    }
    gw_key_list_free(list);
    return result;
}

static nm_status_t delete_object_on_nodes(gw_shared_t *shared,
                                          const char *key)
{
    uint8_t digest[NM_SHA256_LEN];
    size_t start;
    nm_status_t result = NM_ERR_UNAVAILABLE;

    if (shared == NULL || key == NULL ||
        nm_manifest_key_hash(key, digest) != NM_OK) {
        return NM_ERR_INVALID;
    }
    start = node_start(shared);
    for (size_t attempt = 0u; attempt < shared->options.node_count; ++attempt) {
        size_t index = (start + attempt) % shared->options.node_count;
        int fd = connect_node(shared, index);
        nm_status_t status = NM_ERR_UNAVAILABLE;
        bool acknowledged;

        if (fd < 0) {
            result = NM_ERR_UNAVAILABLE;
            continue;
        }
        if (nm_frame_send(fd, NM_MSG_DELETE_OBJECT, key, strlen(key)) !=
            NM_OK) {
            nm_close_fd(&fd);
            result = NM_ERR_UNAVAILABLE;
            continue;
        }
        acknowledged = receive_ack(fd, NM_MSG_DELETE_OBJECT, digest, &status);
        nm_close_fd(&fd);
        if (!acknowledged) {
            result = NM_ERR_UNAVAILABLE;
            continue;
        }
        if (status == NM_OK) {
            return NM_OK;
        }
        if (status == NM_ERR_NOT_FOUND) {
            result = NM_ERR_NOT_FOUND;
            continue;
        }
        return status;
    }
    return result;
}

static void *stats_query_thread(void *argument)
{
    gw_stats_request_t *request = argument;
    nm_frame_t frame;
    int fd;

    if (request == NULL || request->shared == NULL ||
        request->result == NULL) {
        return NULL;
    }
    fd = connect_node_with_timeout(request->shared, request->index,
                                   GW_STATS_TIMEOUT_MS);
    if (fd < 0) {
        return NULL;
    }
    memset(&frame, 0, sizeof(frame));
    if (nm_frame_send(fd, NM_MSG_STATS_REQ, NULL, 0u) != NM_OK) {
        nm_close_fd(&fd);
        return NULL;
    }
    if (nm_frame_recv(fd, &frame) != NM_OK) {
        nm_close_fd(&fd);
        return NULL;
    }
    nm_close_fd(&fd);
    if (frame.type != NM_MSG_STATS_RESP || frame.payload == NULL ||
        frame.length != NM_STATS_RESP_WIRE_SIZE) {
        nm_frame_free(&frame);
        return NULL;
    }
    if (frame.payload[NM_STATS_STATUS_OFFSET] ==
        (uint8_t)NM_STATS_STATUS_ONLINE) {
        request->result->online = true;
        request->result->disk_total =
            nm_get_u64_be(frame.payload + NM_STATS_DISK_TOTAL_OFFSET);
        request->result->disk_free =
            nm_get_u64_be(frame.payload + NM_STATS_DISK_FREE_OFFSET);
        request->result->chunk_count =
            nm_get_u64_be(frame.payload + NM_STATS_CHUNK_COUNT_OFFSET);
        request->result->object_count =
            nm_get_u64_be(frame.payload + NM_STATS_OBJECT_COUNT_OFFSET);
    }
    nm_frame_free(&frame);
    return NULL;
}

static size_t collect_cluster_stats(gw_shared_t *shared,
                                    gw_node_stats_t *results,
                                    pthread_t *threads,
                                    gw_stats_request_t *requests,
                                    bool *started)
{
    size_t count;

    if (shared == NULL || results == NULL || threads == NULL ||
        requests == NULL || started == NULL) {
        return 0u;
    }
    count = shared->options.node_count;
    for (size_t i = 0u; i < count; ++i) {
        int written;

        memset(&results[i], 0, sizeof(results[i]));
        written = snprintf(results[i].id, sizeof(results[i].id), "%s:%u",
                           shared->options.nodes[i].host,
                           (unsigned int)shared->options.nodes[i].port);
        if (written < 0 || (size_t)written >= sizeof(results[i].id)) {
            (void)snprintf(results[i].id, sizeof(results[i].id), "node-%zu",
                           i + 1u);
        }
        requests[i].shared = shared;
        requests[i].index = i;
        requests[i].result = &results[i];
        started[i] = pthread_create(&threads[i], NULL, stats_query_thread,
                                    &requests[i]) == 0;
    }
    for (size_t i = 0u; i < count; ++i) {
        if (started[i]) {
            (void)pthread_join(threads[i], NULL);
        }
    }
    return count;
}

static int build_cluster_stats_json(const gw_node_stats_t *nodes,
                                    size_t count, char **out,
                                    size_t *out_length)
{
    size_t capacity;
    size_t used = 0u;
    uint64_t total_objects = 0u;
    uint64_t total_chunks = 0u;
    char *json;
    int written;

    if (nodes == NULL || out == NULL || out_length == NULL) {
        return NM_ERR_INVALID;
    }
    *out = NULL;
    *out_length = 0u;
    capacity = 256u + (count * (NM_MAX_HOST + 192u));
    json = malloc(capacity);
    if (json == NULL) {
        return NM_ERR_NOMEM;
    }
    written = snprintf(json, capacity, "{\"nodes\":[");
    if (written < 0 || (size_t)written >= capacity) {
        free(json);
        return NM_ERR_TOO_LARGE;
    }
    used = (size_t)written;
    for (size_t i = 0u; i < count; ++i) {
        uint64_t objects = nodes[i].online ? nodes[i].object_count : 0u;
        uint64_t chunks = nodes[i].online ? nodes[i].chunk_count : 0u;

        if (objects > total_objects) {
            total_objects = objects;
        }
        if (chunks > total_chunks) {
            total_chunks = chunks;
        }
        written = snprintf(
            json + used, capacity - used,
            "%s{\"id\":\"%s\",\"status\":\"%s\",\"disk_total\":%llu,"
            "\"disk_free\":%llu,\"objects\":%llu}",
            i == 0u ? "" : ",", nodes[i].id,
            nodes[i].online ? "online" : "offline",
            (unsigned long long)(nodes[i].online ? nodes[i].disk_total : 0u),
            (unsigned long long)(nodes[i].online ? nodes[i].disk_free : 0u),
            (unsigned long long)objects);
        if (written < 0 || (size_t)written >= capacity - used) {
            free(json);
            return NM_ERR_TOO_LARGE;
        }
        used += (size_t)written;
    }
    written = snprintf(
        json + used, capacity - used,
        "],\"total_objects\":%llu,\"total_chunks\":%llu}",
        (unsigned long long)total_objects,
        (unsigned long long)total_chunks);
    if (written < 0 || (size_t)written >= capacity - used) {
        free(json);
        return NM_ERR_TOO_LARGE;
    }
    used += (size_t)written;
    *out = json;
    *out_length = used;
    return NM_OK;
}

static uint64_t gateway_wall_ms(void)
{
    struct timespec timestamp;

    if (clock_gettime(CLOCK_REALTIME, &timestamp) != 0 ||
        timestamp.tv_sec < 0) {
        return 0u;
    }
    return (uint64_t)timestamp.tv_sec * 1000u +
           (uint64_t)(timestamp.tv_nsec / 1000000L);
}

static void gateway_activity_append_locked(gw_shared_t *shared,
                                           const char *type,
                                           const char *message)
{
    gw_activity_event_t *event;

    if (shared == NULL || type == NULL || message == NULL) {
        return;
    }
    event = &shared->activity[shared->activity_next];
    shared->activity_last_id += 1u;
    event->id = shared->activity_last_id;
    event->timestamp_ms = gateway_wall_ms();
    (void)snprintf(event->type, sizeof(event->type), "%s", type);
    (void)snprintf(event->message, sizeof(event->message), "%s", message);
    shared->activity_next =
        (shared->activity_next + 1u) % GW_ACTIVITY_MAX;
    if (shared->activity_count < GW_ACTIVITY_MAX) {
        shared->activity_count += 1u;
    }
}

static void gateway_record_activity(gw_shared_t *shared, const char *type,
                                    const char *message)
{
    if (shared == NULL) {
        return;
    }
    pthread_mutex_lock(&shared->activity_lock);
    gateway_activity_append_locked(shared, type, message);
    pthread_mutex_unlock(&shared->activity_lock);
}

static void gateway_note_node_status(gw_shared_t *shared,
                                     const gw_node_stats_t *results,
                                     size_t count)
{
    char message[GW_ACTIVITY_MESSAGE_MAX];

    if (shared == NULL || results == NULL) {
        return;
    }
    if (count > NM_MAX_PEERS) {
        count = NM_MAX_PEERS;
    }
    pthread_mutex_lock(&shared->activity_lock);
    for (size_t i = 0u; i < count; ++i) {
        bool online = results[i].online;

        if (shared->node_status_known[i] &&
            shared->node_online[i] != online) {
            (void)snprintf(message, sizeof(message), "Node %.128s is %s",
                           results[i].id, online ? "online" : "offline");
            gateway_activity_append_locked(
                shared, online ? "node_online" : "node_offline", message);
        }
        shared->node_status_known[i] = true;
        shared->node_online[i] = online;
    }
    pthread_mutex_unlock(&shared->activity_lock);
}

static int build_activity_json(gw_shared_t *shared, char **out,
                               size_t *out_length)
{
    size_t capacity;
    size_t used = 0u;
    char *json;
    int written;

    if (shared == NULL || out == NULL || out_length == NULL) {
        return NM_ERR_INVALID;
    }
    *out = NULL;
    *out_length = 0u;
    pthread_mutex_lock(&shared->activity_lock);
    capacity = 32u + shared->activity_count *
                         (GW_ACTIVITY_MESSAGE_MAX + 128u);
    json = malloc(capacity);
    if (json == NULL) {
        pthread_mutex_unlock(&shared->activity_lock);
        return NM_ERR_NOMEM;
    }
    written = snprintf(json, capacity, "{\"events\":[");
    if (written < 0 || (size_t)written >= capacity) {
        free(json);
        pthread_mutex_unlock(&shared->activity_lock);
        return NM_ERR_TOO_LARGE;
    }
    used = (size_t)written;
    for (size_t n = 0u; n < shared->activity_count; ++n) {
        size_t index =
            (shared->activity_next + GW_ACTIVITY_MAX - 1u - n) %
            GW_ACTIVITY_MAX;
        const gw_activity_event_t *event = &shared->activity[index];

        written = snprintf(
            json + used, capacity - used,
            "%s{\"id\":%llu,\"type\":\"%s\",\"message\":\"%s\","
            "\"timestamp\":%llu}",
            n == 0u ? "" : ",", (unsigned long long)event->id, event->type,
            event->message, (unsigned long long)event->timestamp_ms);
        if (written < 0 || (size_t)written >= capacity - used) {
            free(json);
            pthread_mutex_unlock(&shared->activity_lock);
            return NM_ERR_TOO_LARGE;
        }
        used += (size_t)written;
    }
    written = snprintf(json + used, capacity - used, "]}");
    if (written < 0 || (size_t)written >= capacity - used) {
        free(json);
        pthread_mutex_unlock(&shared->activity_lock);
        return NM_ERR_TOO_LARGE;
    }
    used += (size_t)written;
    pthread_mutex_unlock(&shared->activity_lock);
    *out = json;
    *out_length = used;
    return NM_OK;
}

static bool handle_activity(gw_shared_t *shared, gw_client_t *client,
                            const gw_request_t *request)
{
    char *json = NULL;
    size_t json_length = 0u;
    bool keep_alive;
    bool sent;
    int result;

    if (shared == NULL || client == NULL || request == NULL) {
        return false;
    }
    keep_alive = request->keep_alive && !atomic_load(&shared->stopping);
    result = build_activity_json(shared, &json, &json_length);
    if (result != NM_OK) {
        (void)send_error(client, 500);
        return false;
    }
    sent = send_response_head(client, 200, "application/json", json_length,
                              keep_alive, "");
    if (sent) {
        sent = send_all(client->fd, json, json_length);
    }
    free(json);
    return sent && keep_alive;
}

static bool handle_cluster_stats(gw_shared_t *shared, gw_client_t *client,
                                 const gw_request_t *request)
{
    gw_node_stats_t results[NM_MAX_PEERS];
    gw_stats_request_t requests[NM_MAX_PEERS];
    pthread_t threads[NM_MAX_PEERS];
    bool started[NM_MAX_PEERS];
    char *json = NULL;
    size_t json_length = 0u;
    size_t count;
    bool keep_alive;
    bool sent;
    int result;

    if (shared == NULL || client == NULL || request == NULL) {
        return false;
    }
    keep_alive = request->keep_alive && !atomic_load(&shared->stopping);
    memset(results, 0, sizeof(results));
    memset(requests, 0, sizeof(requests));
    memset(threads, 0, sizeof(threads));
    memset(started, 0, sizeof(started));

    count = collect_cluster_stats(shared, results, threads, requests, started);
    gateway_note_node_status(shared, results, count);
    result = build_cluster_stats_json(results, count, &json, &json_length);
    if (result != NM_OK) {
        (void)send_error(client, 500);
        return false;
    }
    sent = send_response_head(client, 200, "application/json", json_length,
                              keep_alive, "");
    if (sent) {
        sent = send_all(client->fd, json, json_length);
    }
    free(json);
    return sent && keep_alive;
}

/* ------------------------------------------------------------------------- */
/* Request handlers                                                          */
/* ------------------------------------------------------------------------- */

static bool handle_health(gw_shared_t *shared, gw_client_t *client,
                          const gw_request_t *request)
{
    char body[128];
    bool keep_alive = request->keep_alive && !atomic_load(&shared->stopping);
    int length = snprintf(body, sizeof(body),
                          "{\"status\":\"ok\",\"nodes\":%zu}\n",
                          shared->options.node_count);

    if (length < 0 || (size_t)length >= sizeof(body)) {
        return false;
    }
    if (!send_response_head(client, 200, "application/json", (uint64_t)length,
                            keep_alive, "")) {
        return false;
    }
    if (!send_all(client->fd, body, (size_t)length)) {
        return false;
    }
    return keep_alive;
}

static bool handle_get(gw_shared_t *shared, gw_client_t *client,
                       const gw_request_t *request)
{
    nm_manifest_t manifest;
    bool keep_alive = request->keep_alive && !atomic_load(&shared->stopping);
    char response_headers[512];
    char etag_hex[NM_SHA256_HEX_LEN + 1u];
    nm_status_t status;
    int header_length;

    memset(&manifest, 0, sizeof(manifest));
    status = fetch_manifest_from_nodes(shared, request->key, &manifest);
    if (status != NM_OK) {
        int http_status = http_status_for(status);
        (void)send_error(client, http_status);
        return false;
    }
    if (manifest.deleted) {
        nm_manifest_free(&manifest);
        (void)send_error(client, 404);
        return false;
    }
    if (nm_manifest_validate(&manifest) != NM_OK) {
        nm_manifest_free(&manifest);
        (void)send_error(client, 500);
        return false;
    }
    nm_sha256_to_hex(manifest.object_hash, etag_hex);
    header_length = snprintf(
        response_headers, sizeof(response_headers),
        "ETag: \"%s\"\r\nX-Content-SHA256: %s\r\n"
        "Content-Disposition: attachment; filename=\"%s\"\r\n",
        etag_hex, etag_hex, request->key);
    if (header_length < 0 ||
        (size_t)header_length >= sizeof(response_headers)) {
        nm_manifest_free(&manifest);
        (void)send_error(client, 500);
        return false;
    }
    if (!send_response_head(client, 200, "application/octet-stream",
                            manifest.total_size, keep_alive,
                            response_headers)) {
        nm_manifest_free(&manifest);
        return false;
    }
    for (uint32_t i = 0u; i < manifest.chunk_count; ++i) {
        size_t chunk_length = nm_manifest_chunk_length(&manifest, i);
        const uint8_t *digest = manifest.chunk_hashes +
                                ((size_t)i * NM_SHA256_LEN);
        uint8_t *data = NULL;

        status = fetch_chunk_from_nodes(shared, digest, chunk_length, &data);
        if (status != NM_OK) {
            nm_manifest_free(&manifest);
            nm_log(NM_LOG_WARN,
                   "gateway: GET key '%s' chunk %u unavailable: %s",
                   request->key, i, nm_status_name(status));
            return false;
        }
        if (!send_all(client->fd, data, chunk_length)) {
            free(data);
            nm_manifest_free(&manifest);
            return false;
        }
        free(data);
    }
    {
        char activity[GW_ACTIVITY_MESSAGE_MAX];
        (void)snprintf(activity, sizeof(activity),
                       "Object %s downloaded (%llu bytes)", request->key,
                       (unsigned long long)manifest.total_size);
        gateway_record_activity(shared, "download", activity);
    }
    nm_manifest_free(&manifest);
    return keep_alive;
}

static bool handle_put(gw_shared_t *shared, gw_client_t *client,
                       const gw_request_t *request)
{
    nm_manifest_t manifest;
    EVP_MD_CTX *object_digest = NULL;
    uint8_t *chunk_buffer = NULL;
    uint8_t key_digest[NM_SHA256_LEN];
    uint8_t object_hash[NM_SHA256_LEN];
    char body[512];
    char etag_headers[160];
    char sha_hex[NM_SHA256_HEX_LEN + 1u];
    uint64_t remaining = request->content_length;
    uint32_t chunk_index = 0u;
    int node_fd = -1;
    bool keep_alive = request->keep_alive && !atomic_load(&shared->stopping);
    bool success = false;
    nm_status_t node_status = NM_ERR_SYSTEM;

    memset(&manifest, 0, sizeof(manifest));
    if (nm_manifest_init(&manifest, request->key, request->content_length) !=
            NM_OK ||
        nm_manifest_key_hash(request->key, key_digest) != NM_OK) {
        (void)send_error(client, 500);
        return false;
    }
    node_fd = coordinator_connect(shared);
    if (node_fd < 0) {
        nm_manifest_free(&manifest);
        (void)send_error(client, 503);
        return false;
    }
    if (request->content_length > 0u) {
        chunk_buffer = malloc(NM_CHUNK_SIZE);
        if (chunk_buffer == NULL) {
            nm_close_fd(&node_fd);
            nm_manifest_free(&manifest);
            (void)send_error(client, 500);
            return false;
        }
    }
    object_digest = EVP_MD_CTX_new();
    if (object_digest == NULL ||
        EVP_DigestInit_ex(object_digest, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(object_digest);
        nm_close_fd(&node_fd);
        free(chunk_buffer);
        nm_manifest_free(&manifest);
        (void)send_error(client, 500);
        return false;
    }
    if (request->expect_continue) {
        if (!send_continue(client)) {
            goto cleanup;
        }
    }

    while (remaining > 0u) {
        size_t chunk_length =
            remaining > NM_CHUNK_SIZE ? (size_t)NM_CHUNK_SIZE
                                      : (size_t)remaining;
        uint8_t *digest = manifest.chunk_hashes +
                          ((size_t)chunk_index * NM_SHA256_LEN);
        int read_status = read_body_exact(client, chunk_buffer, chunk_length);
        if (read_status != 0) {
            (void)send_error(client, read_status);
            goto cleanup;
        }
        if (nm_sha256_buf(chunk_buffer, chunk_length, digest) != NM_OK ||
            EVP_DigestUpdate(object_digest, chunk_buffer, chunk_length) != 1) {
            (void)send_error(client, 500);
            goto cleanup;
        }
        if (!store_chunk_on_node(node_fd, digest, chunk_buffer, chunk_length,
                                 &node_status)) {
            nm_log(NM_LOG_WARN,
                   "gateway: coordinator failed while storing chunk %u",
                   chunk_index);
            (void)send_error(client, 502);
            goto cleanup;
        }
        if (node_status != NM_OK) {
            nm_log(NM_LOG_WARN,
                   "gateway: quorum STORE failed for chunk %u: %s",
                   chunk_index, nm_status_name(node_status));
            (void)send_error(client, http_status_for(node_status));
            goto cleanup;
        }
        remaining -= chunk_length;
        chunk_index += 1u;
    }

    unsigned int digest_length = 0u;
    if (EVP_DigestFinal_ex(object_digest, object_hash, &digest_length) != 1 ||
        digest_length != NM_SHA256_LEN ||
        chunk_index != manifest.chunk_count) {
        (void)send_error(client, 500);
        goto cleanup;
    }
    memcpy(manifest.object_hash, object_hash, NM_SHA256_LEN);
    if (!store_manifest_on_node(node_fd, &manifest, key_digest,
                                &node_status)) {
        (void)send_error(client, 502);
        goto cleanup;
    }
    if (node_status != NM_OK) {
        (void)send_error(client, http_status_for(node_status));
        goto cleanup;
    }
    nm_close_fd(&node_fd);
    {
        char activity[GW_ACTIVITY_MESSAGE_MAX];
        (void)snprintf(activity, sizeof(activity),
                       "Object %s uploaded (%llu bytes)", request->key,
                       (unsigned long long)request->content_length);
        gateway_record_activity(shared, "upload", activity);
    }
    nm_sha256_to_hex(object_hash, sha_hex);
    (void)snprintf(etag_headers, sizeof(etag_headers),
                   "ETag: \"%s\"\r\n", sha_hex);
    int body_length = snprintf(
        body, sizeof(body),
        "{\"key\":\"%s\",\"size\":%llu,\"chunks\":%u,\"sha256\":\"%s\"}\n",
        request->key, (unsigned long long)request->content_length,
        chunk_index, sha_hex);
    if (body_length < 0 || (size_t)body_length >= sizeof(body) ||
        !send_response_head(client, 201, "application/json",
                            (uint64_t)body_length, keep_alive,
                            etag_headers) ||
        !send_all(client->fd, body, (size_t)body_length)) {
        goto cleanup;
    }
    success = true;

cleanup:
    if (node_fd >= 0) {
        nm_close_fd(&node_fd);
    }
    EVP_MD_CTX_free(object_digest);
    free(chunk_buffer);
    nm_manifest_free(&manifest);
    return success && keep_alive;
}

static bool handle_options(gw_shared_t *shared, gw_client_t *client,
                           const gw_request_t *request)
{
    bool keep_alive = request->keep_alive && !atomic_load(&shared->stopping);

    if (!send_response_head(client, 204, "text/plain", 0u, keep_alive,
                            "Allow: GET, PUT, DELETE, OPTIONS\r\n")) {
        return false;
    }
    return keep_alive;
}

static bool handle_list_objects(gw_shared_t *shared, gw_client_t *client,
                                const gw_request_t *request)
{
    gw_key_list_t list;
    char *json = NULL;
    size_t json_length = 0u;
    nm_status_t status;
    bool keep_alive = request->keep_alive && !atomic_load(&shared->stopping);
    bool sent;

    memset(&list, 0, sizeof(list));
    status = fetch_object_list(shared, &list);
    if (status != NM_OK) {
        gw_key_list_free(&list);
        (void)send_error(client, http_status_for(status));
        return false;
    }
    status = (nm_status_t)build_object_list_json(&list, &json, &json_length);
    gw_key_list_free(&list);
    if (status != NM_OK) {
        (void)send_error(client, http_status_for(status));
        return false;
    }
    sent = send_response_head(client, 200, "application/json", json_length,
                              keep_alive, "");
    if (sent) {
        sent = send_all(client->fd, json, json_length);
    }
    free(json);
    return sent && keep_alive;
}

static bool handle_delete(gw_shared_t *shared, gw_client_t *client,
                          const gw_request_t *request)
{
    bool keep_alive = request->keep_alive && !atomic_load(&shared->stopping);
    nm_status_t status = delete_object_on_nodes(shared, request->key);

    if (status != NM_OK) {
        (void)send_error(client, http_status_for(status));
        return false;
    }
    {
        char activity[GW_ACTIVITY_MESSAGE_MAX];
        (void)snprintf(activity, sizeof(activity), "Object %s deleted",
                       request->key);
        gateway_record_activity(shared, "delete", activity);
    }
    if (!send_response_head(client, 204, "text/plain", 0u, keep_alive, "")) {
        return false;
    }
    return keep_alive;
}

/* ------------------------------------------------------------------------- */
/* Client worker                                                             */
/* ------------------------------------------------------------------------- */

static void client_cleanup(gw_client_t *client)
{
    gw_shared_t *shared;
    gw_client_t **link;

    if (client == NULL) {
        return;
    }
    shared = client->shared;
    pthread_mutex_lock(&shared->lock);
    link = &shared->clients;
    while (*link != NULL) {
        if (*link == client) {
            *link = client->next;
            break;
        }
        link = &(*link)->next;
    }
    pthread_mutex_unlock(&shared->lock);

    nm_close_fd(&client->fd);
    free(client);

    pthread_mutex_lock(&shared->lock);
    if (shared->active_clients > 0u) {
        shared->active_clients -= 1u;
    }
    pthread_cond_broadcast(&shared->condition);
    pthread_mutex_unlock(&shared->lock);
}

static void *client_worker(void *argument)
{
    gw_client_t *client = argument;
    gw_shared_t *shared = client->shared;

    for (;;) {
        char header[GW_HEADER_SCRATCH];
        bool peer_closed = false;
        int status = read_http_header(client, header, sizeof(header),
                                      &peer_closed);
        if (peer_closed) {
            break;
        }
        if (status != 0) {
            (void)send_error(client, status);
            break;
        }
        gw_request_t request;
        status = parse_http_request(header, &request);
        if (status != 0) {
            (void)send_error(client, status);
            break;
        }
        bool keep_alive;
        if (request.method == GW_METHOD_OPTIONS) {
            keep_alive = handle_options(shared, client, &request);
        } else if (request.is_health) {
            keep_alive = handle_health(shared, client, &request);
        } else if (request.is_cluster_stats) {
            keep_alive = handle_cluster_stats(shared, client, &request);
        } else if (request.is_activity) {
            keep_alive = handle_activity(shared, client, &request);
        } else if (request.is_list) {
            keep_alive = handle_list_objects(shared, client, &request);
        } else if (request.method == GW_METHOD_GET) {
            keep_alive = handle_get(shared, client, &request);
        } else if (request.method == GW_METHOD_DELETE) {
            keep_alive = handle_delete(shared, client, &request);
        } else {
            keep_alive = handle_put(shared, client, &request);
        }
        if (!keep_alive) {
            break;
        }
    }
    client_cleanup(client);
    return NULL;
}

/* ------------------------------------------------------------------------- */
/* Gateway lifecycle                                                         */
/* ------------------------------------------------------------------------- */

static int install_signal_handlers(void)
{
    struct sigaction action;
    struct sigaction ignore;

    memset(&action, 0, sizeof(action));
    action.sa_handler = handle_shutdown_signal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    if (sigaction(SIGINT, &action, NULL) != 0 ||
        sigaction(SIGTERM, &action, NULL) != 0) {
        return -1;
    }
    memset(&ignore, 0, sizeof(ignore));
    ignore.sa_handler = SIG_IGN;
    sigemptyset(&ignore.sa_mask);
    if (sigaction(SIGPIPE, &ignore, NULL) != 0) {
        return -1;
    }
    return 0;
}

static bool configure_client_socket(gw_shared_t *shared, int fd)
{
    if (nm_fd_set_nonblocking(fd, false) != NM_OK ||
        nm_set_socket_timeouts(fd, shared->options.http_timeout_ms,
                               shared->options.http_timeout_ms) != NM_OK) {
        return false;
    }
    return true;
}

static void reject_connection(int fd)
{
    static const char response[] =
        "HTTP/1.1 503 Service Unavailable\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: GET, PUT, DELETE, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Content-Type\r\n"
        "Content-Length: 0\r\n"
        "Connection: close\r\n"
        "\r\n";
    (void)send_all(fd, response, sizeof(response) - 1u);
    nm_close_fd(&fd);
}

int nm_gateway_run(const nm_gateway_options_t *options)
{
    gw_shared_t shared;
    int result = EXIT_FAILURE;
    bool accept_failed = false;

    if (options == NULL || options->node_count == 0u ||
        options->node_count > NM_MAX_PEERS || options->max_connections == 0u ||
        options->http_timeout_ms <= 0 ||
        options->node_connect_timeout_ms <= 0 ||
        options->node_io_timeout_ms <= 0) {
        return EXIT_FAILURE;
    }
    memset(&shared, 0, sizeof(shared));
    shared.options = *options;
    shared.listen_fd = -1;
    atomic_init(&shared.stopping, false);
    atomic_init(&shared.round_robin, 0u);
    if (pthread_mutex_init(&shared.lock, NULL) != 0) {
        return EXIT_FAILURE;
    }
    if (pthread_mutex_init(&shared.activity_lock, NULL) != 0) {
        pthread_mutex_destroy(&shared.lock);
        return EXIT_FAILURE;
    }
    if (pthread_cond_init(&shared.condition, NULL) != 0) {
        pthread_mutex_destroy(&shared.activity_lock);
        pthread_mutex_destroy(&shared.lock);
        return EXIT_FAILURE;
    }
    gateway_record_activity(&shared, "gateway_started", "Gateway started");
    if (install_signal_handlers() != 0) {
        nm_log(NM_LOG_ERROR, "gateway: cannot install signal handlers: %s",
               strerror(errno));
        goto cleanup_sync;
    }
    shared.listen_fd = nm_tcp_listen(shared.options.bind_address,
                                     shared.options.port, NM_TCP_BACKLOG);
    if (shared.listen_fd < 0) {
        nm_log(NM_LOG_ERROR, "gateway: cannot listen on %s:%u: %s",
               shared.options.bind_address,
               (unsigned int)shared.options.port, strerror(errno));
        goto cleanup_sync;
    }
    if (nm_fd_set_nonblocking(shared.listen_fd, false) != NM_OK) {
        goto cleanup_sync;
    }
    nm_log(NM_LOG_INFO,
           "gateway: listening on %s:%u with %zu nodes, max_connections=%zu",
           shared.options.bind_address, (unsigned int)shared.options.port,
           shared.options.node_count, shared.options.max_connections);

    while (!stop_requested) {
        int fd = nm_tcp_accept(shared.listen_fd);
        gw_client_t *client;
        pthread_t thread;

        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            nm_log(NM_LOG_ERROR, "gateway: accept failed: %s",
                   strerror(errno));
            accept_failed = true;
            break;
        }
        if (!configure_client_socket(&shared, fd)) {
            nm_close_fd(&fd);
            continue;
        }
        pthread_mutex_lock(&shared.lock);
        bool reject =
            shared.active_clients >= shared.options.max_connections ||
            atomic_load(&shared.stopping);
        pthread_mutex_unlock(&shared.lock);
        if (reject) {
            reject_connection(fd);
            continue;
        }
        client = calloc(1u, sizeof(*client));
        if (client == NULL) {
            reject_connection(fd);
            continue;
        }
        client->fd = fd;
        client->shared = &shared;

        pthread_mutex_lock(&shared.lock);
        client->next = shared.clients;
        shared.clients = client;
        shared.active_clients += 1u;
        pthread_mutex_unlock(&shared.lock);

        if (pthread_create(&thread, NULL, client_worker, client) != 0) {
            nm_log(NM_LOG_ERROR, "gateway: pthread_create failed");
            pthread_mutex_lock(&shared.lock);
            gw_client_t **link = &shared.clients;
            while (*link != NULL && *link != client) {
                link = &(*link)->next;
            }
            if (*link == client) {
                *link = client->next;
            }
            if (shared.active_clients > 0u) {
                shared.active_clients -= 1u;
            }
            pthread_mutex_unlock(&shared.lock);
            nm_close_fd(&fd);
            free(client);
            continue;
        }
        (void)pthread_detach(thread);
    }

    nm_close_fd(&shared.listen_fd);
    atomic_store(&shared.stopping, true);
    pthread_mutex_lock(&shared.lock);
    for (gw_client_t *client = shared.clients; client != NULL;
         client = client->next) {
        (void)shutdown(client->fd, SHUT_RDWR);
    }
    while (shared.active_clients > 0u) {
        pthread_cond_wait(&shared.condition, &shared.lock);
    }
    pthread_mutex_unlock(&shared.lock);
    nm_log(NM_LOG_INFO, "gateway: shut down");
    result = accept_failed ? EXIT_FAILURE : EXIT_SUCCESS;

cleanup_sync:
    nm_close_fd(&shared.listen_fd);
    pthread_cond_destroy(&shared.condition);
    pthread_mutex_destroy(&shared.activity_lock);
    pthread_mutex_destroy(&shared.lock);
    return result;
}

int main(int argc, char **argv)
{
    nm_gateway_options_t options;

    if (!load_options(&options) ||
        !apply_gateway_arguments(&options, argc, argv)) {
        gateway_usage(stderr);
        return EXIT_FAILURE;
    }
    if (nm_gateway_run(&options) != EXIT_SUCCESS) {
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
