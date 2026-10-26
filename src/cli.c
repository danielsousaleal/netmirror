/*
 * netmirror - command-line HTTP client
 *
 * Commands:
 *   netmirror put <file> <url> [key]     key defaults to the file basename
 *   netmirror get <key> <url> [output]   output defaults to stdout
 *   netmirror health <url>
 *
 * The CLI is intentionally independent from curl. Uploads are streamed from
 * disk with Content-Length and Expect: 100-continue; downloads are streamed to
 * the destination. Only http:// URLs are supported.
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
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define CLI_TIMEOUT_MS 30000
#define CLI_HEADER_MAX 16384u
#define CLI_INPUT_SIZE 32768u
#define CLI_COPY_BUFFER 262144u
#define CLI_DEFAULT_PORT 8080u

typedef struct cli_url {
    char host[NM_MAX_HOST];
    uint16_t port;
} cli_url_t;

typedef struct cli_connection {
    int fd;
    uint8_t input[CLI_INPUT_SIZE];
    size_t input_start;
    size_t input_end;
} cli_connection_t;

typedef struct cli_response_head {
    int status;
    bool has_content_length;
    uint64_t content_length;
} cli_response_head_t;

static void usage(FILE *stream)
{
    (void)fprintf(
        stream,
        "usage:\n"
        "  netmirror put <file> <url> [key]\n"
        "  netmirror get <key> <url> [output]\n"
        "  netmirror health <url>\n"
        "  netmirror node-health <host> <port>\n"
        "\n"
        "url is an http://host[:port] gateway root, for example\n"
        "  http://127.0.0.1:8080\n");
}

static bool parse_url(const char *text, cli_url_t *url)
{
    char authority[NM_MAX_HOST + 16u];
    const char *rest;
    const char *slash;
    size_t length;
    char endpoint_text[NM_MAX_HOST + 32u];

    if (text == NULL || url == NULL ||
        strncasecmp(text, "http://", 7u) != 0) {
        return false;
    }
    rest = text + 7u;
    slash = strchr(rest, '/');
    if (slash != NULL) {
        if (slash[1] != '\0') {
            return false;
        }
        length = (size_t)(slash - rest);
    } else {
        length = strlen(rest);
    }
    if (length == 0u || length >= sizeof(authority)) {
        return false;
    }
    memcpy(authority, rest, length);
    authority[length] = '\0';

    bool needs_default_port = false;
    if (authority[0] == '[') {
        char *closing = strchr(authority, ']');
        if (closing == NULL || (closing[1] != '\0' && closing[1] != ':')) {
            return false;
        }
        needs_default_port = closing[1] == '\0';
    } else {
        needs_default_port = strchr(authority, ':') == NULL;
    }
    char default_port[16];
    (void)snprintf(default_port, sizeof(default_port), ":%u",
                   (unsigned int)CLI_DEFAULT_PORT);
    int written = snprintf(endpoint_text, sizeof(endpoint_text), "%s%s",
                           authority,
                           needs_default_port ? default_port : "");
    if (written < 0 || (size_t)written >= sizeof(endpoint_text)) {
        return false;
    }
    nm_endpoint_t endpoint;
    if (nm_endpoint_parse(endpoint_text, &endpoint) != NM_OK) {
        return false;
    }
    memcpy(url->host, endpoint.host, strlen(endpoint.host) + 1u);
    url->port = endpoint.port;
    return true;
}

static bool derive_key(const char *path, char key[NM_MAX_KEY])
{
    const char *base = strrchr(path, '/');
    base = base == NULL ? path : base + 1;
    if (!nm_key_is_valid(base)) {
        (void)fprintf(stderr,
                      "netmirror: invalid key '%s'; use letters, digits, '.', "
                      "'_' or '-' (max 127 chars)\n",
                      base);
        return false;
    }
    memcpy(key, base, strlen(base) + 1u);
    return true;
}

static int connect_gateway(const cli_url_t *url)
{
    int fd = nm_tcp_connect(url->host, url->port, CLI_TIMEOUT_MS);
    if (fd < 0) {
        (void)fprintf(stderr, "netmirror: cannot connect to %s:%u\n",
                      url->host, (unsigned int)url->port);
        return -1;
    }
    return fd;
}

static int send_header_block(cli_connection_t *connection, const char *header)
{
    return nm_write_all(connection->fd, header, strlen(header)) == NM_OK ? 0
                                                                         : -1;
}

static int fill_input(cli_connection_t *connection)
{
    if (connection->input_end == sizeof(connection->input) &&
        connection->input_start > 0u) {
        size_t available = connection->input_end - connection->input_start;
        memmove(connection->input, connection->input + connection->input_start,
                available);
        connection->input_start = 0u;
        connection->input_end = available;
    }
    ssize_t received =
        recv(connection->fd, connection->input + connection->input_end,
             sizeof(connection->input) - connection->input_end, 0);
    if (received > 0) {
        connection->input_end += (size_t)received;
        return 1;
    }
    if (received == 0) {
        return 0;
    }
    if (errno == EINTR) {
        return 1;
    }
    return -1;
}

static int read_response_head(cli_connection_t *connection,
                              cli_response_head_t *head)
{
    char header[CLI_HEADER_MAX + 1u];

    for (;;) {
        size_t available = connection->input_end - connection->input_start;
        const uint8_t *data = connection->input + connection->input_start;
        size_t delimiter = SIZE_MAX;

        for (size_t i = 0u; i + 3u < available; ++i) {
            if (data[i] == '\r' && data[i + 1u] == '\n' &&
                data[i + 2u] == '\r' && data[i + 3u] == '\n') {
                delimiter = i;
                break;
            }
        }
        if (delimiter != SIZE_MAX) {
            if (delimiter > CLI_HEADER_MAX) {
                return -1;
            }
            memcpy(header, data, delimiter);
            header[delimiter] = '\0';
            connection->input_start += delimiter + 4u;
            if (connection->input_start == connection->input_end) {
                connection->input_start = 0u;
                connection->input_end = 0u;
            }
            break;
        }
        if (available > CLI_HEADER_MAX ||
            connection->input_end == sizeof(connection->input)) {
            return -1;
        }
        int result = fill_input(connection);
        if (result <= 0) {
            return -1;
        }
    }

    memset(head, 0, sizeof(*head));
    head->content_length = 0u;
    char *cursor = header;
    bool first = true;
    while (*cursor != '\0') {
        char *line_end = strstr(cursor, "\r\n");
        char *line = cursor;
        if (line_end != NULL) {
            *line_end = '\0';
            cursor = line_end + 2;
        } else {
            cursor += strlen(cursor);
        }
        if (first) {
            if (strncmp(line, "HTTP/", 5u) != 0) {
                return -1;
            }
            char *space = strchr(line, ' ');
            if (space == NULL) {
                return -1;
            }
            char *end = NULL;
            long status = strtol(space + 1, &end, 10);
            if (end == space + 1 || status < 100 || status > 999) {
                return -1;
            }
            head->status = (int)status;
            first = false;
            continue;
        }
        char *colon = strchr(line, ':');
        if (colon == NULL) {
            continue;
        }
        *colon = '\0';
        char *value = colon + 1;
        while (*value == ' ' || *value == '\t') {
            ++value;
        }
        char *value_end = value + strlen(value);
        while (value_end > value &&
               (value_end[-1] == ' ' || value_end[-1] == '\t')) {
            *--value_end = '\0';
        }
        if (strcasecmp(line, "Content-Length") == 0) {
            if (head->has_content_length) {
                return -1;
            }
            uint64_t length = 0u;
            if (*value == '\0') {
                return -1;
            }
            for (const unsigned char *digit = (const unsigned char *)value;
                 *digit != '\0'; ++digit) {
                if (*digit < '0' || *digit > '9') {
                    return -1;
                }
                uint64_t number = (uint64_t)(*digit - '0');
                if (length > (UINT64_MAX - number) / 10u) {
                    return -1;
                }
                length = (length * 10u) + number;
            }
            head->has_content_length = true;
            head->content_length = length;
        }
    }
    if (first) {
        return -1;
    }
    return 0;
}

static int copy_body(cli_connection_t *connection, uint64_t length, FILE *out)
{
    uint8_t buffer[CLI_COPY_BUFFER];
    uint64_t remaining = length;

    size_t buffered = connection->input_end - connection->input_start;
    if ((uint64_t)buffered > remaining) {
        buffered = (size_t)remaining;
    }
    if (buffered > 0u) {
        if (fwrite(connection->input + connection->input_start, 1u, buffered,
                   out) != buffered) {
            return -1;
        }
        connection->input_start += buffered;
        remaining -= buffered;
        if (connection->input_start == connection->input_end) {
            connection->input_start = 0u;
            connection->input_end = 0u;
        }
    }
    while (remaining > 0u) {
        size_t request = remaining > sizeof(buffer) ? sizeof(buffer)
                                                    : (size_t)remaining;
        ssize_t received = recv(connection->fd, buffer, request, 0);
        if (received > 0) {
            size_t amount = (size_t)received;
            if (fwrite(buffer, 1u, amount, out) != amount) {
                return -1;
            }
            remaining -= amount;
            continue;
        }
        if (received == 0) {
            return -1;
        }
        if (errno == EINTR) {
            continue;
        }
        return -1;
    }
    return 0;
}

static int read_and_emit_head(cli_connection_t *connection,
                              cli_response_head_t *head)
{
    if (read_response_head(connection, head) != 0) {
        (void)fprintf(stderr, "netmirror: malformed HTTP response\n");
        return -1;
    }
    return 0;
}

static int emit_response_body(cli_connection_t *connection,
                              const cli_response_head_t *head, FILE *out,
                              bool require_length)
{
    if (!head->has_content_length) {
        if (require_length && head->status != 100) {
            (void)fprintf(stderr,
                          "netmirror: response without Content-Length\n");
            return -1;
        }
        return 0;
    }
    return copy_body(connection, head->content_length, out);
}

static int command_put(const char *path, const char *url_text,
                       const char *key_override)
{
    cli_url_t url;
    cli_connection_t connection;
    cli_response_head_t head;
    FILE *file = NULL;
    struct stat st;
    char key[NM_MAX_KEY];
    char header[1024];
    uint8_t *buffer = NULL;
    int result = EXIT_FAILURE;

    if (!parse_url(url_text, &url)) {
        (void)fprintf(stderr, "netmirror: invalid gateway URL '%s'\n",
                      url_text);
        return EXIT_FAILURE;
    }
    if (key_override != NULL) {
        if (!nm_key_is_valid(key_override)) {
            (void)fprintf(stderr, "netmirror: invalid key '%s'\n",
                          key_override);
            return EXIT_FAILURE;
        }
        memcpy(key, key_override, strlen(key_override) + 1u);
    } else if (!derive_key(path, key)) {
        return EXIT_FAILURE;
    }
    file = fopen(path, "rb");
    if (file == NULL) {
        (void)fprintf(stderr, "netmirror: cannot open '%s': %s\n", path,
                      strerror(errno));
        return EXIT_FAILURE;
    }
    if (fstat(fileno(file), &st) != 0 || !S_ISREG(st.st_mode)) {
        (void)fprintf(stderr, "netmirror: '%s' is not a regular file\n", path);
        goto cleanup;
    }
    if ((uint64_t)st.st_size > NM_MAX_OBJECT_SIZE) {
        (void)fprintf(stderr, "netmirror: object exceeds 16 GiB limit\n");
        goto cleanup;
    }
    buffer = malloc(CLI_COPY_BUFFER);
    if (buffer == NULL) {
        (void)fprintf(stderr, "netmirror: out of memory\n");
        goto cleanup;
    }
    memset(&connection, 0, sizeof(connection));
    connection.fd = connect_gateway(&url);
    if (connection.fd < 0) {
        goto cleanup;
    }
    int written = snprintf(
        header, sizeof(header),
        "PUT /obj/%s HTTP/1.1\r\n"
        "Host: %s:%u\r\n"
        "Content-Length: %llu\r\n"
        "Expect: 100-continue\r\n"
        "Content-Type: application/octet-stream\r\n"
        "Connection: close\r\n"
        "\r\n",
        key, url.host, (unsigned int)url.port,
        (unsigned long long)st.st_size);
    if (written < 0 || (size_t)written >= sizeof(header) ||
        send_header_block(&connection, header) != 0) {
        goto cleanup_connection;
    }
    if (read_and_emit_head(&connection, &head) != 0) {
        goto cleanup_connection;
    }
    if (head.status == 100) {
        uint64_t remaining = (uint64_t)st.st_size;
        while (remaining > 0u) {
            size_t request = remaining > CLI_COPY_BUFFER
                                 ? CLI_COPY_BUFFER
                                 : (size_t)remaining;
            size_t got = fread(buffer, 1u, request, file);
            if (got == 0u) {
                if (ferror(file)) {
                    (void)fprintf(stderr, "netmirror: read error\n");
                    goto cleanup_connection;
                }
                (void)fprintf(stderr, "netmirror: file changed while reading\n");
                goto cleanup_connection;
            }
            if (nm_write_all(connection.fd, buffer, got) != NM_OK) {
                (void)fprintf(stderr, "netmirror: upload interrupted\n");
                goto cleanup_connection;
            }
            remaining -= got;
        }
        if (read_and_emit_head(&connection, &head) != 0) {
            goto cleanup_connection;
        }
    }
    if (head.status >= 200 && head.status < 300) {
        if (emit_response_body(&connection, &head, stdout, true) != 0) {
            goto cleanup_connection;
        }
        result = EXIT_SUCCESS;
    } else {
        (void)fprintf(stderr, "netmirror: HTTP %d\n", head.status);
        (void)emit_response_body(&connection, &head, stderr, true);
    }

cleanup_connection:
    nm_close_fd(&connection.fd);
cleanup:
    free(buffer);
    if (file != NULL) {
        (void)fclose(file);
    }
    return result;
}

static int command_get(const char *key, const char *url_text,
                       const char *output_path)
{
    cli_url_t url;
    cli_connection_t connection;
    cli_response_head_t head;
    FILE *output = stdout;
    char header[1024];
    int result = EXIT_FAILURE;

    if (!nm_key_is_valid(key) || !parse_url(url_text, &url)) {
        (void)fprintf(stderr, "netmirror: invalid key or URL\n");
        return EXIT_FAILURE;
    }
    if (output_path != NULL) {
        output = fopen(output_path, "wb");
        if (output == NULL) {
            (void)fprintf(stderr, "netmirror: cannot create '%s': %s\n",
                          output_path, strerror(errno));
            return EXIT_FAILURE;
        }
    }
    memset(&connection, 0, sizeof(connection));
    connection.fd = connect_gateway(&url);
    if (connection.fd < 0) {
        goto cleanup;
    }
    int written = snprintf(header, sizeof(header),
                           "GET /obj/%s HTTP/1.1\r\n"
                           "Host: %s:%u\r\n"
                           "Connection: close\r\n"
                           "\r\n",
                           key, url.host, (unsigned int)url.port);
    if (written < 0 || (size_t)written >= sizeof(header) ||
        send_header_block(&connection, header) != 0) {
        goto cleanup_connection;
    }
    if (read_and_emit_head(&connection, &head) != 0) {
        goto cleanup_connection;
    }
    if (head.status >= 200 && head.status < 300) {
        if (emit_response_body(&connection, &head, output, true) != 0) {
            goto cleanup_connection;
        }
        result = EXIT_SUCCESS;
    } else {
        (void)fprintf(stderr, "netmirror: HTTP %d\n", head.status);
        (void)emit_response_body(&connection, &head, stderr, true);
    }

cleanup_connection:
    nm_close_fd(&connection.fd);
cleanup:
    if (output_path != NULL && output != NULL) {
        if (fclose(output) != 0 && result == EXIT_SUCCESS) {
            result = EXIT_FAILURE;
        }
    }
    return result;
}

static int command_node_health(const char *host, const char *port_text)
{
    char *end = NULL;
    unsigned long port_value;
    uint8_t nonce_payload[8];
    uint8_t verified_payload[8];
    nm_frame_t response;
    int fd = -1;
    int result = EXIT_FAILURE;

    errno = 0;
    port_value = strtoul(port_text, &end, 10);
    if (errno != 0 || end == port_text || *end != '\0' ||
        port_value == 0u || port_value > 65535u) {
        (void)fprintf(stderr, "netmirror: invalid node port\n");
        return EXIT_FAILURE;
    }
    fd = nm_tcp_connect(host, (uint16_t)port_value, 3000);
    if (fd < 0) {
        (void)fprintf(stderr, "netmirror: cannot connect to %s:%lu\n", host,
                      port_value);
        return EXIT_FAILURE;
    }
    nm_put_u64_be(nonce_payload, nm_now_ms() ^ UINT64_C(0x6e65746d6972726f));
    if (nm_frame_send(fd, NM_MSG_PING, nonce_payload,
                      sizeof(nonce_payload)) != NM_OK) {
        goto cleanup;
    }
    memset(&response, 0, sizeof(response));
    if (nm_frame_recv(fd, &response) != NM_OK) {
        goto cleanup;
    }
    if (response.type != NM_MSG_PONG ||
        response.length != sizeof(verified_payload) ||
        response.payload == NULL ||
        memcmp(response.payload, nonce_payload, sizeof(nonce_payload)) != 0) {
        (void)fprintf(stderr, "netmirror: invalid PONG from node\n");
        nm_frame_free(&response);
        goto cleanup;
    }
    memcpy(verified_payload, response.payload, sizeof(verified_payload));
    nm_frame_free(&response);
    (void)fprintf(stdout, "node OK (nonce=%llu)\n",
                  (unsigned long long)nm_get_u64_be(verified_payload));
    result = EXIT_SUCCESS;

cleanup:
    nm_close_fd(&fd);
    return result;
}

static int command_health(const char *url_text)
{
    cli_url_t url;
    cli_connection_t connection;
    cli_response_head_t head;
    char header[1024];
    int result = EXIT_FAILURE;

    if (!parse_url(url_text, &url)) {
        (void)fprintf(stderr, "netmirror: invalid gateway URL\n");
        return EXIT_FAILURE;
    }
    memset(&connection, 0, sizeof(connection));
    connection.fd = connect_gateway(&url);
    if (connection.fd < 0) {
        return EXIT_FAILURE;
    }
    int written = snprintf(header, sizeof(header),
                           "GET /health HTTP/1.1\r\n"
                           "Host: %s:%u\r\n"
                           "Connection: close\r\n"
                           "\r\n",
                           url.host, (unsigned int)url.port);
    if (written < 0 || (size_t)written >= sizeof(header) ||
        send_header_block(&connection, header) != 0) {
        goto cleanup;
    }
    if (read_and_emit_head(&connection, &head) != 0) {
        goto cleanup;
    }
    if (head.status >= 200 && head.status < 300) {
        if (emit_response_body(&connection, &head, stdout, true) != 0) {
            goto cleanup;
        }
        result = EXIT_SUCCESS;
    } else {
        (void)fprintf(stderr, "netmirror: HTTP %d\n", head.status);
        (void)emit_response_body(&connection, &head, stderr, true);
    }

cleanup:
    nm_close_fd(&connection.fd);
    return result;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        usage(stderr);
        return EXIT_FAILURE;
    }
    if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0 ||
        strcmp(argv[1], "help") == 0) {
        usage(stdout);
        return EXIT_SUCCESS;
    }
    if (strcmp(argv[1], "put") == 0) {
        if (argc != 4 && argc != 5) {
            usage(stderr);
            return EXIT_FAILURE;
        }
        return command_put(argv[2], argv[3], argc == 5 ? argv[4] : NULL);
    }
    if (strcmp(argv[1], "get") == 0) {
        if (argc != 4 && argc != 5) {
            usage(stderr);
            return EXIT_FAILURE;
        }
        return command_get(argv[2], argv[3], argc == 5 ? argv[4] : NULL);
    }
    if (strcmp(argv[1], "health") == 0) {
        if (argc != 3) {
            usage(stderr);
            return EXIT_FAILURE;
        }
        return command_health(argv[2]);
    }
    if (strcmp(argv[1], "node-health") == 0) {
        if (argc != 4) {
            usage(stderr);
            return EXIT_FAILURE;
        }
        return command_node_health(argv[2], argv[3]);
    }
    usage(stderr);
    return EXIT_FAILURE;
}
