#ifndef POLARIS_ZIG_CORE_H
#define POLARIS_ZIG_CORE_H
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct { uint8_t *ptr; size_t len; } polaris_buffer;
typedef struct polaris_cursor polaris_cursor;
typedef struct polaris_stream polaris_stream;

/* Inputs are borrowed UTF-8 JSON. Outputs are owned JSON {ok: ...} or
 * {error: {kind, message, ...}}. Release outputs with polaris_v1_free. */
polaris_buffer polaris_v1_validate(const uint8_t *, size_t);
polaris_buffer polaris_v1_request(const uint8_t *, size_t);
void polaris_v1_free(polaris_buffer);

polaris_cursor *polaris_v1_cursor_create(void);
bool polaris_v1_cursor_remember(polaris_cursor *, const uint8_t *, size_t);
void polaris_v1_cursor_destroy(polaris_cursor *);

polaris_buffer polaris_v1_stream_create(const uint8_t *, size_t, polaris_stream **);
polaris_buffer polaris_v1_stream_next(polaris_stream *);
/* One reader per stream. Cancel wakes the reader. Join it before destroy. */
void polaris_v1_stream_cancel(polaris_stream *);
void polaris_v1_stream_destroy(polaris_stream *);

#ifdef __cplusplus
}
#endif
#endif
