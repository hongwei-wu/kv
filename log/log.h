#ifndef _KV_LOG_H_
#define _KV_LOG_H_
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#define LEVEL_DEBUG 5
#define LEVEL_INFO  4
#define LEVEL_WARN  3
#define LEVEL_ERROR 2
#define LEVEL_FATAL 1

#define PRINT(__LEVEL__, __LEVEL_TAG__, __FD__, __FMT__, ...) \
    if (__LEVEL__ <= log_level_) {                            \
        time_t t = time(NULL); \
        char tmp[16]; \
        strftime(tmp, sizeof(tmp), "%H:%M:%S", localtime(&t)); \
        fprintf(__FD__, "[%s] [%s] [%s:%d] ", tmp, __LEVEL_TAG__, __func__, __LINE__); \
        fprintf(__FD__, __FMT__, ##__VA_ARGS__); \
        fprintf(__FD__, "\n");\
    }

#define DEBUG(fmt, ...) PRINT(LEVEL_DEBUG, "D", stdout, fmt, ##__VA_ARGS__)
#define INFO(fmt, ...)  PRINT(LEVEL_INFO, "I", stdout, fmt, ##__VA_ARGS__)
#define WARN(fmt, ...)  PRINT(LEVEL_WARN, "W", stdout, fmt, ##__VA_ARGS__)
#define ERROR(fmt, ...) PRINT(LEVEL_ERROR, "E", stderr, fmt, ##__VA_ARGS__)
#define FATAL(fmt, ...) fflush(stderr); fflush(stdout); PRINT(LEVEL_FATAL,"F", stderr, fmt, ##__VA_ARGS__); fflush(stderr); exit(-1);

extern int log_level_;
#define SET_LOG_LEVEL(__level__) log_level_ = (__level__);

#endif//_KV_LOG_H_