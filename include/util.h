/*
 * netmirror - small process-wide utilities
 *
 * Logging and clock helpers have no protocol semantics. They are isolated from
 * the storage and network layers so those modules stay independently testable.
 */

#ifndef NETMIRROR_UTIL_H
#define NETMIRROR_UTIL_H

#include "netmirror.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum nm_log_level {
    NM_LOG_ERROR = 0,
    NM_LOG_WARN = 1,
    NM_LOG_INFO = 2,
    NM_LOG_DEBUG = 3
} nm_log_level_t;

/* Parse "error", "warn", "info" or "debug"; returns fallback otherwise. */
nm_log_level_t nm_log_level_from_string(const char *value,
                                        nm_log_level_t fallback);

/* Process-wide level; initialized to NM_LOG_INFO. */
void nm_log_set_level(nm_log_level_t level);
nm_log_level_t nm_log_get_level(void);

/*
 * Thread-safe line-oriented diagnostic logger. The timestamp is UTC and the
 * function flushes stderr while holding the stdio file lock.
 */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((format(printf, 2, 3)))
#endif
void nm_log(nm_log_level_t level, const char *format, ...);

/* Monotonic milliseconds. Returns 0 if the clock is unavailable. */
uint64_t nm_now_ms(void);

const char *nm_status_name(nm_status_t status);

#ifdef __cplusplus
}
#endif

#endif /* NETMIRROR_UTIL_H */
