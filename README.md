# kv

## 介绍
基于B+树实现的kv数据库（key、value只支持int64类型数据）。

## 功能特性
* 数据按4k大小分页
* 缓存上限4M（1024页）
* 按需保存内存中的脏数据

## USAGE
```shell
kv help                -- show help
kv get <key>           -- get key
kv put <key:value>     -- put key
kv del <key>           -- delete key
kv list                -- list all keys
kv ins <num>           -- insert key in batch
kv clr                 -- clear all record
kv ver                 -- verify all records
```

### api
```c
kv_file* kv_open(const char* name);
int      kv_close(kv_file* kv);
int      kv_put(kv_file* kv, int64_t key, int64_t value);
int      kv_del(kv_file* kv, int64_t key);
int      kv_get(kv_file* kv, int64_t key, int64_t* value);
int      kv_next(kv_file* kv, int64_t sk, int64_t* key, int64_t* value);
void     kv_range(kv_file* kv, int64_t min, int64_t max, void* ptr, void (*callback)(void* ptr, int64_t key, int64_t value));
```

## TODO LIST
* 数据完整性校验
* WAL日志
* 优化页内数据拷贝操作
