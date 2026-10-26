#include "util.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static nm_log_level_t global_log_level = NM_LOG_INFO;

static const char *const LEVEL_NAMES[] = {"ERROR", "WARN", "INFO", "DEBUG"};

nm_log_level_t nm_log_level_from_string(const char *value,
                                        nm_log_level_t fallback)
{
    if (value == NULL) {
        return fallback;
    }
    if (strcmp(value, "error") == 0) {
        return NM_LOG_ERROR;
    }
    if (strcmp(value, "warn") == 0 || strcmp(value, "warning") == 0) {
        return NM_LOG_WARN;
    }
    if (strcmp(value, "info") == 0) {
        return NM_LOG_INFO;
    }
    if (strcmp(value, "debug") == 0) {
        return NM_LOG_DEBUG;
    }
    return fallback;
}

void nm_log_set_level(nm_log_level_t level)
{
    if (level >= NM_LOG_ERROR && level <= NM_LOG_DEBUG) {
        global_log_level = level;
    }
}

nm_log_level_t nm_log_get_level(void)
{
    return global_log_level;
}

void nm_log(nm_log_level_t level, const char *format, ...)
{
    va_list arguments;
    struct timespec timestamp;
    struct tm broken_down;
    char time_buffer[32] = "unknown-time";

    if (level < NM_LOG_ERROR || level > NM_LOG_DEBUG ||
        level > global_log_level || format == NULL) {
        return;
    }

    if (clock_gettime(CLOCK_REALTIME, &timestamp) == 0 &&
        gmtime_r(&timestamp.tv_sec, &broken_down) != NULL &&
        strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%dT%H:%M:%S",
                 &broken_down) > 0u) {
        size_t used = strlen(time_buffer);
        (void)snprintf(time_buffer + used, sizeof(time_buffer) - used,
                       ".%03ldZ", timestamp.tv_nsec / 1000000L);
    }

    flockfile(stderr);
    (void)fprintf(stderr, "%s %-5s ", time_buffer,
                  LEVEL_NAMES[(int)level]);
    va_start(arguments, format);
    (void)vfprintf(stderr, format, arguments);
    va_end(arguments);
    (void)fputc('\n', stderr);
    (void)fflush(stderr);
    funlockfile(stderr);
}

uint64_t nm_now_ms(void)
{
    struct timespec timestamp;

    if (clock_gettime(CLOCK_MONOTONIC, &timestamp) != 0 ||
        timestamp.tv_sec < 0) {
        return 0u;
    }
    return (uint64_t)timestamp.tv_sec * 1000u +
           (uint64_t)(timestamp.tv_nsec / 1000000L);
}

const char *nm_status_name(nm_status_t status)
{
    switch (status) {
    case NM_OK:
        return "OK";
    case NM_ERR_INVALID:
        return "INVALID";
    case NM_ERR_NOMEM:
        return "NOMEM";
    case NM_ERR_IO:
        return "IO";
    case NM_ERR_NOT_FOUND:
        return "NOT_FOUND";
    case NM_ERR_HASH_MISMATCH:
        return "HASH_MISMATCH";
    case NM_ERR_PROTOCOL:
        return "PROTOCOL";
    case NM_ERR_TIMEOUT:
        return "TIMEOUT";
    case NM_ERR_QUORUM:
        return "QUORUM";
    case NM_ERR_TOO_LARGE:
        return "TOO_LARGE";
    case NM_ERR_BUSY:
        return "BUSY";
    case NM_ERR_UNAVAILABLE:
        return "UNAVAILABLE";
    case NM_ERR_HTTP:
        return "HTTP";
    case NM_ERR_SYSTEM:
        return "SYSTEM";
    default:
        return "UNKNOWN";
    }
}
