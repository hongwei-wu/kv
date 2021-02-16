#ifndef __KV_H__
#define __KV_H__
#include <stdint.h>
#include <stdbool.h>
#include "define.h"

typedef struct __kv_file kv_file;

kv_file* kv_open(const char* name);
int      kv_close(kv_file* kv);
int      kv_put(kv_file* kv, int64_t key, int64_t value);
int      kv_del(kv_file* kv, int64_t key);
int      kv_get(kv_file* kv, int64_t key, int64_t* value);
int      kv_next(kv_file* kv, int64_t sk, int64_t* key, int64_t* value);
void     kv_range(kv_file* kv, int64_t min, int64_t max, void* ptr, void (*callback)(void* ptr, int64_t key, int64_t value));
int      kv_clear(kv_file *kv);

// for test
kv_page* kv_page_create(kv_file* kv, uint16_t type);
void kv_page_set(kv_file* kv, kv_page* p, int64_t key, int64_t value);
void kv_iterate(kv_file*kv, void* ptr, void(*f)(void* ptr, uint16_t page, int64_t key, int64_t value));
void kv_print(kv_file* kv);

#endif//__KV_H__
