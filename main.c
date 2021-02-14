#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "log.h"
#include "kv.h"

struct iterate_context{
    int64_t key;
    int64_t value;
    int64_t num;
};
void iterate(void* ptr, uint16_t page, int64_t key, int64_t value);
void range(void* ptr, int64_t key, int64_t value);
#define MIN_NUM 1
#define MAX_NUM 1000000

int main() {
    SET_LOG_LEVEL(LEVEL_DEBUG)
    INFO("order of b+ tree: %d", KV_ORDER);

    kv_file* kv = kv_open("./test.kdb");
    for(int64_t i=MIN_NUM; i <=MAX_NUM; ++i){
        kv_put(kv, i, i*2);
    }

    int64_t key = MIN_NUM;
    int64_t val = 0;
    kv_get(kv, key, &val);
    DEBUG("kv_get key: %ld value: %ld", key, val);

    kv_next(kv, (MIN_NUM+MAX_NUM)/2, &key, &val);
    DEBUG("kv_next key: %ld value: %ld", key, val);

    //kv_range(kv, MIN_NUM, MIN_NUM+100, NULL, range);

    // iterate
    /*
    struct iterate_context *ctx = (struct iterate_context*)malloc(sizeof(struct iterate_context));
    ctx->num = 0;
    kv_iterate(kv, ctx, iterate);
    */

    kv_close(kv);
#ifndef _WIN32
    //system("rm test.kdb");
#else
    //system("del test.kdb");
#endif

    return 0;
}

void range(void* ptr, int64_t key, int64_t value){
    DEBUG("kv_range key: %ld value: %ld", key, value);
}

void iterate(void* ptr, uint16_t page, int64_t key, int64_t value){
    struct iterate_context* ctx = (struct iterate_context*)ptr;
    if(ctx->num <= 0){
        ctx->key   = key;
        ctx->value = value;
    }else if (ctx->key >= key){
        ERROR("invalid key sequence");
    }
    ctx->num+=1;
}
