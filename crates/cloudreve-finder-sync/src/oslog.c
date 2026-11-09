// NSLog from a sandboxed process reaches the unified log redacted
// ("<private>"); os_log with a public specifier does not. The macro needs
// a C compiler (it builds its argument buffer at compile time), hence this
// file instead of a Rust declaration.
#include <os/log.h>

void cloudreve_finder_sync_log(const char *message) {
    os_log(OS_LOG_DEFAULT, "%{public}s", message);
}
