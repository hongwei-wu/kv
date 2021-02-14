#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <signal.h>
#include "kv.h"
#include "cache.h"
#include "log.h"

#pragma pack(1)
struct __kv_file{
    uint32_t magic;
    uint32_t root;
    uint32_t free;
    uint32_t page_num;
    kv_page_cache* cache;
    FILE* f;
    uint8_t buf[KV_PAGE_SIZE];
};
#pragma pack()

kv_page* kv_page_at(kv_file* kv, uint32_t page);
kv_page* kv_find_leaf_page(kv_file* kv, kv_page* p, int64_t key);
uint16_t kv_find_child_index(kv_page* p, int64_t key);
void kv_page_merge_if_need(kv_file* kv, kv_page* p);
void kv_page_del(kv_file* kv, kv_page* p, int64_t key);
void kv_page_split_if_need(kv_file* kv, kv_page* p);
uint16_t kv_page_find_insert_index(kv_page* p, int64_t key);
int  kv_initialize(const char* name);
void kv_dirty_page(kv_file* kv, uint32_t page);
void kv_dirty_flush(kv_file* kv, bool force);
void kv_set_signal_handler(kv_file* kv);
kv_file *_kv_for_signal = NULL;

kv_file* kv_open(const char* name){
    int ret = kv_initialize(name);
    if( ret != 0){
        FATAL("initialize kv failed with errno: %d", ret)
    }

    kv_file* kv = (kv_file*)malloc(sizeof(kv_file));
#ifndef _WIN32
    FILE *f = fopen(name, "r+");
#else
    FILE *f = fopen(name, "rb+");
#endif
    if(f == NULL){
        FATAL("open kv failed with errno: %d", errno)
    }

    if (fread(kv, offsetof(kv_file, cache), 1, f) <= 0){
        FATAL("read kv header failed with errno: %d", errno)
    }
    kv->f = f;
    kv->cache = cache_create(kv->page_num, 1024, f, offsetof(kv_file, cache));
    kv_set_signal_handler(kv);

    return kv;
}

int kv_initialize(const char* name){
    if(!access(name, 0)){
        return 0;
    }
#ifndef _WIN32
    FILE* f = fopen(name, "w+");
#else
    FILE* f = fopen(name, "wb+");
#endif
    if(f == NULL){
        return errno;
    }

    kv_file kv = {.magic = KV_MAGIC, .root = NULL_PAGE, .free=NULL_PAGE, .page_num=0};
    if(fwrite(&kv, offsetof(struct __kv_file, cache), 1, f) <= 0){
        return errno;
    }
    if(fclose(f) != 0){
        return errno;
    }
    return 0;
}

int kv_close(kv_file* kv){
    if(kv == NULL){
        return CODE_INVALID_PARAMETER;
    }
    kv_dirty_flush(kv, true);
    fclose(kv->f);
    return 0;
}

int kv_put(kv_file* kv, int64_t key, int64_t value){
    if(kv->root == NULL_PAGE){
        kv_page *new = kv_page_create(kv, KV_PAGE_DATA);
        kv->root = new->page;
    }

    kv_page* leaf = kv_find_leaf_page(kv, kv_page_at(kv, kv->root), key);
    kv_page_set(kv, leaf, key, value);
    kv_page_split_if_need(kv, leaf);

    // flush dirty
    kv_dirty_flush(kv, false);
    return 0;
}

int kv_del(kv_file* kv, int64_t key){
    if(kv->root == NULL_PAGE){
        return 0;
    }
    kv_page* leaf = kv_find_leaf_page(kv, kv_page_at(kv, kv->root), key);
    kv_page_del(kv, leaf, key);
    kv_page_merge_if_need(kv, leaf);

    //
    kv_dirty_flush(kv, false);
    return 0;
}

int kv_get(kv_file* kv, int64_t key, int64_t* value){
    if(kv->root == NULL_PAGE){
        return CODE_KEY_NOT_EXIST;
    }

    kv_page* leaf  = kv_find_leaf_page(kv, kv_page_at(kv, kv->root), key);
    uint16_t index = kv_page_find_insert_index(leaf, key);
    kv_record* records = KV_PAGE_RECORDS(leaf);
    if(index >= leaf->record_num || records[index].key != key){
        return CODE_KEY_NOT_EXIST;
    }
    *value = records[index+1].value;
    return 0;
}

int kv_next(kv_file* kv, int64_t sk, int64_t* key, int64_t* value){
    kv_page* leaf  = kv_find_leaf_page(kv, kv_page_at(kv, kv->root), sk);
    uint16_t index = kv_page_find_insert_index(leaf, sk);
    kv_record* records = KV_PAGE_RECORDS(leaf);

    if(index < leaf->record_num && records[index].key == sk){
        ++index;
    }

    if(index >= leaf->record_num && leaf->next_page == NULL_PAGE){
        return CODE_KEY_NOT_EXIST;
    }else if(index >= leaf->record_num && leaf->next_page != NULL_PAGE){
        leaf = kv_page_at(kv, leaf->next_page);
        index = 0;
    }

    records = KV_PAGE_RECORDS(leaf);
    *key   = records[index].key;
    *value = records[index+1].value;
    return 0;
}

void kv_range(kv_file* kv, int64_t min, int64_t max, void* ptr, void (*callback)(void*, int64_t, int64_t)){
    kv_page* leaf  = kv_find_leaf_page(kv, kv_page_at(kv, kv->root), min);
    uint16_t index = kv_page_find_insert_index(leaf, min);
    kv_record* records = KV_PAGE_RECORDS(leaf);
    if(index >= leaf->record_num){
        return;
    }

    for(;;){
        if(records[index].key >= max){
            break;
        }
        callback(ptr, records[index].key, records[index+1].value);
        ++index;
        if(index >= leaf->record_num){
            if(leaf->next_page == NULL_PAGE){
                break;
            }
            leaf = kv_page_at(kv, leaf->next_page);
            index = 0;
        }
    }
}

void kv_iterate(kv_file*kv, void* ptr, void(*f)(void*, uint16_t, int64_t, int64_t)){
    kv_record *records;
    kv_page* p = kv_page_at(kv, kv->root);
    for(; p->type != KV_PAGE_DATA;){
        records = KV_PAGE_RECORDS(p);
        p = kv_page_at(kv, records[0].value);
    }

    uint32_t page = p->page;
    for(; page != NULL_PAGE;){
        p = kv_page_at(kv, page);
        records = KV_PAGE_RECORDS(p);
        for(uint16_t i=0; i<p->record_num; ++i){
            f(ptr, page, records[i].key, records[i+1].value);
        }
        page = p->next_page;
    }
}

void kv_page_set(kv_file*kv, kv_page* p, int64_t key, int64_t value){
    kv_record* records = KV_PAGE_RECORDS(p);
    uint16_t index = kv_page_find_insert_index(p, key);

    if(index >= p->record_num || records[index].key != key){
        for(uint16_t i=p->record_num; i>index; --i){
            records[i].key = records[i-1].key;
            records[i+1].value = records[i].value;
        }
        records[index].key   = key;
        records[index+1].value = value;
        p->record_num += 1;
    }else if(records[index].key == key){
        if(records[index+1].value == value){
            return;
        }
        records[index+1].value = value;
    }
    kv_dirty_page(kv, p->page);
}

kv_page* kv_split_page(kv_file* kv, kv_page* p){
    kv_record* records = KV_PAGE_RECORDS(p);
    uint16_t mid = KV_ORDER / 2;
    int64_t  mid_key = records[mid].key;
    kv_page* new = kv_page_create(kv, p->type);
    kv_record* new_records = KV_PAGE_RECORDS(new);
    kv_page* parent = NULL;
    if(p->parent != NULL_PAGE){
        parent = kv_page_at(kv, p->parent);
        kv_record* parent_records = KV_PAGE_RECORDS(parent);
        uint16_t index = kv_find_child_index(parent, records[0].key);
        for(uint16_t i=parent->record_num; i>index; --i){
            parent_records[i].key = parent_records[i-1].key;
            parent_records[i+1].value = parent_records[i].value;
        }

        parent_records[index].key   = mid_key;
        parent_records[index+1].value = new->page;
        new->parent = parent->page;
        parent->record_num += 1;
    }else{
        parent = kv_page_create(kv, KV_PAGE_NODE);
        kv_record* parent_records = KV_PAGE_RECORDS(parent);
        uint16_t index = 0;
        parent_records[index].key = mid_key;
        parent_records[index+1].value = new->page;
        new->parent = parent->page;
        parent->record_num += 1;

        parent_records[index].value = p->page;
        p->parent = parent->page;
    }

    if(p->type == KV_PAGE_DATA){
        for(uint16_t i=mid; i<p->record_num; ++i){
            new_records[i-mid].key = records[i].key;
            new_records[i-mid+1].value = records[i+1].value;
        }
        new->record_num = p->record_num - mid;
        p->record_num = mid;
        new->next_page= p->next_page;
        p->next_page = new->page;
    }else{
        for(uint16_t i=mid+1; i<p->record_num+1; ++i){
            new_records[i-mid-1].key   = records[i].key;
            new_records[i-mid-1].value = records[i].value;
            kv_page* child = kv_page_at(kv, records[i].value);
            child->parent = new->page;
        }
        new->record_num = p->record_num - mid - 1;
        p->record_num = mid;
    }

    kv_dirty_page(kv, p->page);
    kv_dirty_page(kv, new->page);
    kv_dirty_page(kv, parent->page);
    return parent;
}

void kv_page_split_if_need(kv_file* kv, kv_page* p){
    if(p->record_num < KV_ORDER) {
        return;
    }

    bool root = (p->parent == NULL_PAGE);
    kv_page* parent = kv_split_page(kv, p);
    if(root){
        kv->root = parent->page;
    }
    kv_page_split_if_need(kv, parent);
}

uint16_t kv_page_find_insert_index(kv_page* p, int64_t key){
    kv_record* records = KV_PAGE_RECORDS(p);
    if(p->record_num <= 0 || records[p->record_num-1].key < key){
        return p->record_num;
    }

    uint16_t left = 0, right = p->record_num -1;
    for(; left < right; ){
        uint16_t mid = (left + right) / 2;
        if(records[mid].key < key){
            left = mid + 1;
        }else{
            right = mid;
        }
    }
    return left;
}

void kv_extend_file(kv_file* kv, uint16_t num){
    fseek(kv->f, 0, SEEK_END);
    kv_page *p = (kv_page*)kv->buf;
    for(uint16_t i=0; i<num; ++i){
        p->page      = kv->page_num + i;
        p->parent    = NULL_PAGE;
        p->type      = 0;
        p->record_num=0;
        if(i == num-1){
            p->next_page = kv->free;
        }else{
            p->next_page = p->page + 1;
        }

        if(fwrite(p, KV_PAGE_SIZE, 1, kv->f) <= 0){
            FATAL("extend kv file errno: %d", errno)
        }
    }

    if(kv->page_num <= 0){
        kv->free = 1;
    }else{
        kv->free = kv->page_num;
    }
    kv->page_num += num;
    cache_set_page_num(kv->cache, kv->page_num);
}

kv_page* kv_page_create(kv_file* kv, uint16_t type){
    if(kv->free == NULL_PAGE){
        kv_extend_file(kv, 1024);
    }

    kv_page *p = (kv_page*)cache_get_page(kv->cache, kv->free);
    kv->free      = p->next_page;
    p->parent     = NULL_PAGE;
    p->type       = type;
    p->next_page  = NULL_PAGE;
    p->record_num = 0;

    if(p->page == NULL_PAGE){
        FATAL("INVALID PAGE")
    }

    //INFO("create page %d", p->page)

    return p;
}

kv_page* kv_page_at(kv_file* kv, uint32_t page){
    return cache_get_page(kv->cache, page);
}

uint16_t kv_recursive_find_child_index(kv_page* p, uint16_t left, uint16_t right, int64_t key){
    kv_record* records = KV_PAGE_RECORDS(p);
    uint16_t mid = (left + right) / 2;

    if(right - left <= 1){
        return left + 1;
    }

    if(records[mid].key > key){
        return kv_recursive_find_child_index(p, left, mid, key);
    }else{
        return kv_recursive_find_child_index(p, mid, right, key);
    }
}

uint16_t kv_find_child_index(kv_page* p, int64_t key){
    kv_record* records = KV_PAGE_RECORDS(p);
    if(records[p->record_num-1].key <= key){
        return p->record_num;
    }else if(records[0].key > key){
        return 0;
    }
    return kv_recursive_find_child_index(p, 0, p->record_num-1, key);
}

kv_page* kv_find_leaf_page(kv_file* kv, kv_page* p, int64_t key){
    if(p->type == KV_PAGE_DATA){
        return p;
    }

    // find child
    kv_record* records = KV_PAGE_RECORDS(p);
    uint16_t index = kv_find_child_index(p, key);
    kv_page* child = kv_page_at(kv, records[index].value);
    return kv_find_leaf_page(kv, child, key);
}

void kv_page_replace_min(kv_file* kv, kv_page* p, int64_t key){
    kv_record* records = KV_PAGE_RECORDS(p);
    int64_t  min = records[0].key;
    uint32_t parent_page = p->parent;
    for( ;parent_page != NULL_PAGE; ){
        kv_page* parent = kv_page_at(kv, parent_page);
        kv_record* parent_records = KV_PAGE_RECORDS(parent);
        uint16_t index = kv_find_child_index(parent, key);
        if(index != 0 && parent_records[index-1].key == key){
            parent_records[index-1].key = min;
            kv_dirty_page(kv, parent->page);
        }
        parent_page = parent->parent;
    }
}

void kv_page_del(kv_file* kv, kv_page* p, int64_t key){
    kv_record* records = KV_PAGE_RECORDS(p);
    uint16_t   index = kv_page_find_insert_index(p, key);
    if(index >= p->record_num || records[index].key != key){
        return;
    }

    for(uint16_t i=index; i<p->record_num-1; ++i){
        records[i].key = records[i+1].key;
        records[i+1].value = records[i+2].value;
    }
    p->record_num -= 1;

    if(p->parent != NULL_PAGE && index == 0){
        kv_page_replace_min(kv, p, key);
    }
    kv_dirty_page(kv, p->page);
}

bool kv_page_should_get_record_from_left(kv_file*kv, kv_page* p){
    kv_page*   parent = kv_page_at(kv, p->parent);
    kv_record* parent_records = KV_PAGE_RECORDS(parent);
    kv_record* records = KV_PAGE_RECORDS(p);
    uint16_t index  = kv_find_child_index(parent, records[0].key);
    if(index > 0){
        kv_page* sibling = kv_page_at(kv, parent_records[index-1].value);
        return sibling->record_num > KV_MIN_RECORDS;
    }
    return false;
}

bool kv_page_should_get_record_from_right(kv_file*kv, kv_page* p){
    kv_page*   parent = kv_page_at(kv, p->parent);
    kv_record* parent_records = KV_PAGE_RECORDS(parent);
    kv_record* records = KV_PAGE_RECORDS(p);
    uint16_t index  = kv_find_child_index(parent, records[0].key);
    if(index < parent->record_num){
        kv_page* sibling = kv_page_at(kv, parent_records[index+1].value);
        return sibling->record_num > KV_MIN_RECORDS;
    }
    return false;
}

void kv_page_get_record_from_left(kv_file* kv, kv_page* p){
    kv_page*   parent = kv_page_at(kv, p->parent);
    kv_record* parent_records = KV_PAGE_RECORDS(parent);
    kv_record* records = KV_PAGE_RECORDS(p);

    for(uint16_t i=p->record_num; i>0; --i){
        records[i].key     = records[i-1].key;
        records[i+1].value = records[i].value;
    }
    p->record_num += 1;

    uint16_t index  = kv_find_child_index(parent, records[0].key);
    kv_page*   sibling = kv_page_at(kv, parent_records[index-1].value);
    kv_record* sibling_records = KV_PAGE_RECORDS(sibling);
    if(p->type == KV_PAGE_DATA){
        records[0].key   = sibling_records[sibling->record_num-1].key;
        records[0].value = sibling_records[sibling->record_num].value;
        parent_records[index-1].key = records[0].key;
    }else{
        records[0].key = parent_records[index-1].key;
        parent_records[index-1].key = sibling_records[sibling->record_num-1].key;
        records[0].value = sibling_records[sibling->record_num].value;
        sibling_records[sibling->record_num].value = NULL_PAGE;
        kv_page* child = kv_page_at(kv, records[0].value);
        child->parent  = p->page;
    }
    sibling->record_num -= 1;

    kv_dirty_page(kv, p->page);
    kv_dirty_page(kv, sibling->page);
    kv_dirty_page(kv, parent->page);
}

void kv_page_get_record_from_right(kv_file* kv, kv_page* p){
    kv_page*   parent = kv_page_at(kv, p->parent);
    kv_record* parent_records = KV_PAGE_RECORDS(parent);
    kv_record* records = KV_PAGE_RECORDS(p);

    p->record_num += 1;
    uint16_t index  = kv_find_child_index(parent, records[0].key);
    kv_page*   sibling = kv_page_at(kv, parent_records[index+1].value);
    kv_record* sibling_records = KV_PAGE_RECORDS(sibling);
    if(p->type == KV_PAGE_DATA){
        records[p->record_num-1].key = sibling_records[0].key;
        records[p->record_num].value = sibling_records[1].value;
        parent_records[index].key = sibling_records[1].key;
    }else{
        records[p->record_num-1].key = parent_records[index].key;
        parent_records[index].key = sibling_records[0].key;
        records[p->record_num].value = sibling_records[0].value;
        kv_page* child = kv_page_at(kv, records[p->record_num].value);
        child->parent  = p->page;
    }

    for(uint16_t i=1; i<sibling->record_num; ++i){
        sibling_records[i-1].key   = sibling_records[i].key;
        sibling_records[i-1].value = sibling_records[i].value;
    }
    sibling_records[sibling->record_num-1].value = sibling_records[sibling->record_num].value;
    sibling->record_num -= 1;

    kv_dirty_page(kv, p->page);
    kv_dirty_page(kv, sibling->page);
    kv_dirty_page(kv, parent->page);
}

kv_page* kv_page_merge_sibling(kv_file* kv, kv_page* left, kv_page*right){
    kv_page*   parent = kv_page_at(kv, left->parent);
    kv_record* parent_records = KV_PAGE_RECORDS(parent);
    kv_record* left_records   = KV_PAGE_RECORDS(left);
    kv_record* right_records  = KV_PAGE_RECORDS(right);
    uint16_t   index = kv_find_child_index(parent, left_records[0].key);

    if(left->type == KV_PAGE_DATA){
        for(uint16_t i=0; i<right->record_num; ++i){
            left_records[left->record_num+i].key = right_records[i].key;
            left_records[left->record_num+i+1].value = right_records[i+1].value;
        }
        left->record_num += right->record_num;
        left->next_page   = right->next_page;
    }else{
        left_records[left->record_num].key = parent_records[index].key;
        left_records[left->record_num+1].value = right_records[0].value;
        kv_page* child = kv_page_at(kv, right_records[0].value);
        child->parent = left->page;
        left->record_num += 1;
        for(uint16_t i=0; i<right->record_num; ++i){
            left_records[left->record_num+i].key = right_records[i].key;
            left_records[left->record_num+i+1].value = right_records[i+1].value;
            child = kv_page_at(kv, right_records[i+1].value);
            child->parent = left->page;
        }
        left->record_num += right->record_num;
    }

    for(uint16_t i=index; i<parent->record_num-1; ++i){
        parent_records[i].key     = parent_records[i+1].key;
        parent_records[i+1].value = parent_records[i+2].value;
    }
    parent->record_num -= 1;

    kv_dirty_page(kv, left->page);
    kv_dirty_page(kv, parent->page);

    // free right page

    return parent;
}

void kv_page_merge_if_need(kv_file* kv, kv_page* p){
    if(p->record_num >= KV_MIN_RECORDS){
        return;
    }

    kv_record* records = KV_PAGE_RECORDS(p);
    if(p->parent == NULL_PAGE && p->record_num == 0){
        kv->root = records[0].value;
        if(kv->root != NULL_PAGE){
            kv_page* root = kv_page_at(kv, kv->root);
            root->parent = NULL_PAGE;
        }
        // free page p

    }else if(p->parent != NULL_PAGE){
        if(kv_page_should_get_record_from_left(kv, p)){
            kv_page_get_record_from_left(kv, p);
        }else if(kv_page_should_get_record_from_right(kv, p)){
            kv_page_get_record_from_right(kv, p);
        }else{
            kv_page*   parent = kv_page_at(kv, p->parent);
            kv_record* parent_records = KV_PAGE_RECORDS(parent);
            uint16_t index  = kv_find_child_index(parent, records[0].key);
            kv_page*   sibling = NULL;
            if(index < parent->record_num){
                sibling = kv_page_at(kv, parent_records[index+1].value);
                parent = kv_page_merge_sibling(kv, p, sibling);
            }else{
                sibling = kv_page_at(kv, parent_records[index-1].value);
                parent  = kv_page_merge_sibling(kv, sibling, p);
            }
            kv_page_merge_if_need(kv, parent);
        }
    }
}

void kv_print_pages(kv_file* kv, kv_page** pages, uint16_t page_num, uint16_t level){
    char fmt[16] = {0};
    uint16_t begin = 60 - level * 10;
    sprintf(fmt, "%%%ds", begin);
    printf(fmt, " ");

    kv_page* child[16] = {0};
    uint16_t child_num = 0;

    for(uint16_t n=0; n<page_num; ++n) {
        kv_page* p = pages[n];
        kv_record *records = KV_PAGE_RECORDS(p);

        printf("    <%d>", p->page);
        if (p->type == KV_PAGE_DATA) {
            for (uint16_t i = 0; i < p->record_num; ++i) {
                printf("(%ld)%ld", records[i].value, records[i].key);
            }
            printf("(%ld)", records[p->record_num].value);
        } else {
            for (uint16_t i = 0; i < p->record_num; ++i) {
                printf("(%ld)%ld", records[i].value, records[i].key);
                child[child_num] = kv_page_at(kv, records[i].value);
                child_num += 1;
            }
            printf("(%ld)", records[p->record_num].value);
            child[child_num] = kv_page_at(kv, records[p->record_num].value);
            child_num += 1;
        }
    }
    printf("\n");
    if(child_num > 0){
        kv_print_pages(kv, child, child_num, level+1);
    }
}

void kv_print(kv_file* kv){
    if(kv->root == NULL_PAGE){
        return;
    }

    kv_page* pages[1] = {kv_page_at(kv, kv->root)};
    kv_print_pages(kv, pages, 1, 0);
}

void kv_dirty_page(kv_file* kv, uint32_t page){
    cache_set_page_dirty(kv->cache, page);
}

void kv_dirty_flush(kv_file* kv, bool force){
    if(!cache_flush_dirty(kv->cache, force)){
        return;
    }

    fseek(kv->f, 0, SEEK_SET);
    if(fwrite(kv, offsetof(struct __kv_file, cache), 1, kv->f) <= 0){
        FATAL("write kv header errno: %d", errno)
    }
    fflush(kv->f);
}

void signal_handler(int sig){
    if(!_kv_for_signal){
        return;
    }

    INFO("catch signal %d", sig);
    kv_dirty_flush(_kv_for_signal, true);
}

void kv_set_signal_handler(kv_file* kv) {
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);
    _kv_for_signal = kv;
}
