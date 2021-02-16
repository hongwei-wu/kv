#include <unistd.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include "log.h"
#include "kv.h"

#define KV_NAME "test.kdb"
#define MIN(a, b) (a) <= (b) ? (a): (b)

void cmd_help();
void cmd_get(kv_file *kv, const char* k);
void cmd_put(kv_file *kv, const char* key_value);
void cmd_del(kv_file *kv, const char* k);
void cmd_list(kv_file *kv);
void cmd_insert_batch(kv_file *kv, const char* n);
void cmd_clear(kv_file *kv);
void cmd_verify(kv_file *kv);

int main(int argc, char** argv) {
    static struct option long_options[] = {
            {"help", no_argument,       NULL, 'h'},
            {"get",  required_argument, NULL, 'g'},
            {"put",  required_argument, NULL, 'p'},
            {"del",  required_argument, NULL, 'd'},
            {"list", optional_argument, NULL, 'l'},
            {"ins",  required_argument, NULL, 'i'},
            {"clr",  no_argument,       NULL, 'c'},
            {"ver",  no_argument,       NULL, 'v'},
            {0,      0,                 0,     0 }
    };

    SET_LOG_LEVEL(LEVEL_DEBUG)
    kv_file *kv = kv_open(KV_NAME);

    int opt;
    int option_index = 0;

    while((opt=getopt_long(argc, argv,  "", long_options, &option_index)) != -1){
        switch (opt) {
            case 'h':
                cmd_help();
                break;
            case 'g':
                cmd_get(kv, optarg);
                break;
            case 'p':
                cmd_put(kv, optarg);
                break;
            case 'd':
                cmd_del(kv, optarg);
                break;
            case 'l':
                cmd_list(kv);
                break;
            case 'i':
                cmd_insert_batch(kv, optarg);
                break;
            case 'c':
                cmd_clear(kv);
                break;
            case 'v':
                cmd_verify(kv);
                break;
            default:
                break;
        }
    }

    kv_close(kv);
    return 0;
}

void cmd_help() {
    printf("kv help                -- show help\r\n"
           "kv get <key>           -- get key\r\n"
           "kv put <key:value>     -- put key\r\n"
           "kv del <key>           -- delete key\r\n"
           "kv list                -- list all keys\r\n"
           "kv ins <num>           -- insert key in batch\r\n"
           "kv clr                 -- clear all record\r\n"
           "kv ver                 -- verify all records\r\n");
}

int64_t str2int64(const char* str){
    return strtoll(str, NULL, 10);
}

void cmd_get(kv_file *kv, const char* k) {
    int64_t val = 0;
    int64_t key = str2int64(k);
    int ret = kv_get(kv, key, &val);
    if( ret ){
        printf("get key=%ld error=%d\r\n", key, ret);
    }else{
        printf("get key=%ld val=%ld\r\n", key, val);
    }
}


void cmd_put(kv_file *kv, const char* key_value) {
    char buf[32] = {0};
    strcpy(buf, key_value);
    char *sep = strstr(buf, ":");
    if(sep == NULL){
        printf("kv put invalid kv pair\r\n");
        return;
    }

    *sep = 0;
    int64_t key = str2int64(buf);
    int64_t val = str2int64(++sep);
    int ret = kv_put(kv, key, val);
    if(ret){
        printf("put key=%ld val=%ld error=%d\r\n", key, val, ret);
    }else{
        printf("put key=%ld val=%ld\r\n", key, val);
    }
}

void cmd_del(kv_file *kv, const char* k){
    int64_t key = str2int64(k);
    int ret = kv_del(kv, key);
    if(ret){
        printf("del key=%ld error=%d\r\n", key, ret);
    }else{
        printf("del key=%ld succeed\r\n", key);
    }
}

void iterate(void* ptr, uint16_t page, int64_t key, int64_t value);
void cmd_list(kv_file *kv){
    kv_iterate(kv, NULL, iterate);
}

int64_t get_timestamp_usec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec*1000000 + tv.tv_usec);
}

int64_t gen_key(int64_t now, int64_t seq){
    return now + seq;
}

int64_t gen_value(int64_t key){
    return key;
}

void cmd_insert_batch(kv_file *kv, const char *n){
    int64_t num = str2int64(n);
    int64_t now = get_timestamp_usec();
    int64_t key, val;
    int64_t sec = now / 1000000;
    for(int64_t i=0; i<num; ++i){
        key = gen_key(sec, i);
        val = gen_value(key);
        int ret = kv_put(kv, key, val);
        if (ret){
            printf("batch put error:%d\r\n", ret);
            return;
        }
    }

    int64_t total = get_timestamp_usec() - now;
    int64_t tpr = total / num;
    printf("batch total time: %ld usec\r\n", total);
    printf("batch time per record: %lld usec\r\n", tpr);
}

void cmd_clear(kv_file* kv){
    int ret = kv_clear(kv);
    if(ret){
        printf("clear error=%d\r\n", ret);
    }else{
        printf("clear succeed\r\n");
    }
}

struct ctx_verify {
    int64_t total;
    int64_t valid;
};

void verify(void* ptr, uint16_t page, int64_t key, int64_t val);

void cmd_verify(kv_file *kv) {
    struct ctx_verify ctx={.total = 0, .valid=0};
    kv_iterate(kv, &ctx, verify);
    printf("verify total: %ld valid: %ld invalid: %ld\r\n", ctx.total, ctx.valid, ctx.total-ctx.valid);
}

void iterate(void* ptr, uint16_t page, int64_t key, int64_t val){
    printf("list key=%ld val=%ld\r\n", key, val);
}

void verify(void* ptr, uint16_t page, int64_t key, int64_t val){
    struct ctx_verify* ctx = (struct ctx_verify*)ptr;
    int64_t n = gen_value(key);
    if(val == n){
        ++ctx->valid;
    }
    ++ctx->total;
}