#ifndef __KV_PAGE_CACHE_H__
#define __KV_PAGE_CACHE_H__
#include <stdio.h>
#include <stdbool.h>
#include "define.h"

typedef struct __kv_page_cache kv_page_cache;

kv_page_cache* cache_create(uint32_t pages, uint32_t cache_pages, FILE* f, size_t offset);
void  cache_destroy(kv_page_cache* cache);
kv_page* cache_get_page(kv_page_cache *cache, uint32_t page);
void cache_set_page_num(kv_page_cache* cache, uint32_t pages);
void cache_set_page_dirty(kv_page_cache* cache, uint32_t page);
bool cache_flush_dirty(kv_page_cache*cache, bool force);

#endif//__KV_PAGE_CACHE_H__