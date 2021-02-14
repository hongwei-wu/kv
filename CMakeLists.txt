cmake_minimum_required(VERSION 3.16)
project(kv C)

set(CMAKE_C_STANDARD 99)

include_directories("./kv" "./log")

add_executable(kv main.c log/log.c kv/kv.c kv/cache.c)