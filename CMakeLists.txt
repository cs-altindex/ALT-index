cmake_minimum_required(VERSION 3.16)
project(ALT_index)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-O3 -pg -march=core-avx2")


find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)
find_package (Threads)

include_directories(include)

add_executable(ALT_index main.cpp)

target_link_libraries(ALT_index ${TbbLib} ${JemallocLib} ${CMAKE_THREAD_LIBS_INIT})