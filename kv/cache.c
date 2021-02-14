#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "cache.h"
#include "cache_list.h"
#include "log.h"

#define HASH_SLOT 1021

typedef struct __kv_page_cache_item{
    struct cache_list list;
    struct cache_list hash_list;
    uint8_t           dirty;
    kv_page           *page;
}kv_page_cache_item;

typedef struct __kv_page_cache_item_hash {
	struct cache_list slots[HASH_SLOT];
}kv_page_cache_item_hash;

typedef struct __kv_page_cache{
    uint32_t pages;
    uint32_t cache_pages;
    size_t   offset;
    FILE     *f;
    uint8_t  *buf;
    kv_page_cache_item *items;
    struct cache_list free_list;
    struct cache_list       read_list;
    kv_page_cache_item_hash read_hash;
    struct cache_list       dirty_list;
    kv_page_cache_item_hash dirty_hash;
    uint32_t dirty;
}kv_page_cache;

void cache_init_hash(kv_page_cache_item_hash* h){
    for(int i=0; i<HASH_SLOT; ++i){
        list_init(&h->slots[i]);
    }
}

kv_page_cache_item* cache_find_item(kv_page_cache_item_hash* h, uint32_t page){
    struct cache_list* head = &h->slots[page % HASH_SLOT];
    for (struct cache_list* l = list_first(head); l != list_sentinel(head); l = list_next(l)) {
        kv_page_cache_item *item = list_data(l, kv_page_cache_item, hash_list);
        if(item->page->page == page){
            return item;
        }
    }
    return NULL;
}

void add_item_to_hash(kv_page_cache_item_hash* h, struct cache_list *l, kv_page_cache_item* item){
    list_insert_head(l, &item->list);

    struct cache_list* head = &h->slots[item->page->page % HASH_SLOT];
    list_insert_head(head, &item->hash_list);
}

void del_item_from_hash(kv_page_cache_item* item){
    list_remove(&item->list);
    list_remove(&item->hash_list);
}

kv_page_cache_item* remove_tail_from_read_list(kv_page_cache* cache){
    if(list_empty(&cache->read_list)){
        FATAL("cache read list is empty")
    }

    struct cache_list* l = list_last(&cache->read_list);
    kv_page_cache_item *item = list_data(l, kv_page_cache_item, list);
    del_item_from_hash(item);
    return item;
}

kv_page_cache* cache_create(uint32_t pages, uint32_t cache_pages, FILE* f, size_t offset) {
    kv_page_cache* c = (kv_page_cache*)malloc(sizeof(kv_page_cache));
    c->pages  = pages;
    c->cache_pages = cache_pages;
    c->offset = offset;
    c->f      = f;
    c->buf    = (uint8_t*)malloc(KV_PAGE_SIZE * cache_pages);
    memset(c->buf, 0, KV_PAGE_SIZE * cache_pages);
    c->items  = (kv_page_cache_item *)malloc(sizeof(kv_page_cache_item) * cache_pages);
    c->dirty  = 0;

    list_init(&c->free_list);
    list_init(&c->read_list);
    cache_init_hash(&c->read_hash);
    list_init(&c->dirty_list);
    cache_init_hash(&c->dirty_hash);

    for(int i=0; i<cache_pages; ++i){
        kv_page_cache_item* item = c->items + i;
        list_init(&item->list);
        list_init(&item->hash_list);
        item->page = (kv_page*)(c->buf + KV_PAGE_SIZE * i);
        item->dirty = 0;

        list_insert_tail(&c->free_list, &item->list);
    }
    return c;
}

void cache_load_page_from_file(kv_page_cache *c, uint32_t page, void *buf){
    fseek(c->f, c->offset + KV_PAGE_SIZE * page , SEEK_SET);
    size_t ret = fread(buf, KV_PAGE_SIZE, 1, c->f);
    if(ret <= 0){
        FATAL("load page %d from file error: %d", page, errno)
    }
}

void cache_flush_page_to_file(kv_page_cache* c, uint32_t page, void *buf){
    fseek(c->f, c->offset + KV_PAGE_SIZE * page , SEEK_SET);
    size_t ret = fwrite(buf, KV_PAGE_SIZE, 1, c->f);
    if(ret <= 0){
        FATAL("flush page %d from file error: %d", page, errno)
    }
}

kv_page* cache_get_page(kv_page_cache *cache, uint32_t page) {
    if(page >= cache->pages || page <= 0){
        FATAL("invalid pages: %d, total pages: %d", page, cache->pages)
    }

    kv_page_cache_item *item = cache_find_item(&cache->dirty_hash, page);
    if(item != NULL){
        return item->page;
    }

    item = cache_find_item(&cache->read_hash, page);
    if(item != NULL){
        return item->page;
    }

    if(list_empty(&cache->free_list)){
        item = remove_tail_from_read_list(cache);
        list_insert_head(&cache->free_list, &item->list);
    }

    if(!list_empty(&cache->free_list)){
        struct cache_list *l = list_first(&cache->free_list);
        list_remove(l);

        kv_page_cache_item *item = list_data(l, kv_page_cache_item, list);
        cache_load_page_from_file(cache, page, item->page);
        if(item->page->page != page){
            FATAL("invalid page load from file: %d %d", item->page->page, page);
        }

        add_item_to_hash(&cache->read_hash, &cache->read_list, item);
        return item->page;
    }

    FATAL("cache page %ld cache out of memory", page)
}

void cache_set_page_num(kv_page_cache* cache, uint32_t pages){
    cache->pages = pages;
}

void cache_set_page_dirty(kv_page_cache* cache, uint32_t page){
    //DEBUG("dirty page: %d", page)
    kv_page_cache_item *item = cache_find_item(&cache->dirty_hash, page);
    if(item != NULL){
        return;
    }

    item = cache_find_item(&cache->read_hash, page);
    if(item != NULL){
        item->dirty = 1;
        cache->dirty += 1;
        del_item_from_hash(item);
        add_item_to_hash(&cache->dirty_hash, &cache->dirty_list, item);
    }
}

bool cache_flush_dirty(kv_page_cache*cache, bool force){
    if(!force && cache->dirty < cache->cache_pages/2){
        return false;
    }

    for (struct cache_list* l = list_first(&cache->dirty_list); l != list_sentinel(&cache->dirty_list);) {
        kv_page_cache_item *item = list_data(l, kv_page_cache_item, list);
        l = list_next(l);

        del_item_from_hash(item);
        item->dirty = 0;
        cache_flush_page_to_file(cache, item->page->page, item->page);

        list_insert_head(&cache->free_list, &item->list);
    }
    cache->dirty = 0;
    return true;
}
