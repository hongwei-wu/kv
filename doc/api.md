# API

* kv_open 创建或者打开已有的kv数据库
```c
kv_file* kv_open(const char* name);
```

* kv_close 关闭kv数据库
```c
int kv_close(kv_file* kv);
```

* kv_put 保存key、val键值对
```c
int kv_put(kv_file* kv, int64_t key, int64_t value);
```
* kv_del 删除键值对
```c
int kv_del(kv_file* kv, int64_t key);
```

* kv_get 获取key对应的值
```c
int kv_get(kv_file* kv, int64_t key, int64_t* value);
```

* kv_next 获取第一个key大于sk的键值对
```c
int kv_next(kv_file* kv, int64_t sk, int64_t* key, int64_t* value);
```

* kv_range 遍历[min, max)范围内的键值对
```c
void kv_range(kv_file* kv, int64_t min, int64_t max, void* ptr, void (*callback)(void* ptr, int64_t key, int64_t value));
```

* kv_clear 清除所有键值对数据
```c
int kv_clear(kv_file *kv);
```

* kv_iterate 遍历所有键值对
```c
void kv_iterate(kv_file *kv, void* ptr, void(*f)(void* ptr, uint16_t page, int64_t key, int64_t value));
```
