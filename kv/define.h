#ifndef __KV_DEFINE_H__
#define __KV_DEFINE_H__
#include <stdint.h>
#include <stddef.h>

#pragma pack(1)
typedef struct __kv_page{
    uint32_t page;
    uint32_t parent;
    uint32_t next_page;
    uint16_t type;
    uint16_t record_num;
    // record array
}kv_page;

typedef struct __kv_record{
    int64_t  key;
    int64_t  value;
}kv_record;
#pragma pack()

#define KV_MAGIC 0xefefefef
#define KV_PAGE_SIZE 4*1024

#define KV_ORDER ((KV_PAGE_SIZE - sizeof(kv_page)) / sizeof(kv_record) - 2)
//#define KV_ORDER 5
#define KV_MIN_RECORDS ((KV_ORDER+1)/2 - 1)

#define KV_PAGE_NODE 1
#define KV_PAGE_DATA 2

#define KV_PAGE_RECORDS(__P__)((kv_record*) (((uint8_t*)(__P__)) +(offsetof(kv_page, record_num) + sizeof(uint16_t))))
#define NULL_PAGE 0

// error
#define CODE_SUCCEED 0
#define CODE_INVALID_PARAMETER 1
#define CODE_KEY_NOT_EXIST 2

#endif//__KV_DEFINE_H__
