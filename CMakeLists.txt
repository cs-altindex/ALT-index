cmake_minimum_required(VERSION 3.16)
project(ALT_index)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-O3 -pg -march=core-avx2")

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)
find_package (Threads)
find_package(OpenMP)

include_directories(include)
include_directories(include/OptimizedART)

add_executable(ALT_index examples/base.cpp)
add_executable(multithread examples/multithreaded.cpp)

target_link_libraries(ALT_index ${TbbLib} ${JemallocLib} ${CMAKE_THREAD_LIBS_INIT} OpenMP::OpenMP_CXX)
target_link_libraries(multithread ${TbbLib} ${JemallocLib} ${CMAKE_THREAD_LIBS_INIT} OpenMP::OpenMP_CXX)