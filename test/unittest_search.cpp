//
// Created by yyx on 2023/12/10.
//

#include "utils.h"
#include <iostream>
#include <vector>
#include "omp.h"
#include <chrono>

using namespace std;

#define DATA_NUMBER 1000

int main(){
    vector<uint64_t> data;

    int data_num = DATA_NUMBER;
    //reserve for data
    data.reserve(data_num);

    for(uint64_t i = 0 ; i < data_num ; i++){
        data[i] = i;
    }
    auto start = std::chrono::high_resolution_clock::now();

    //test search scheme
#pragma omp parallel for schedule(static, 8)
    for (uint64_t i = 0; i < data_num; i++) {
        uint64_t pos = random() % data_num;
        binary_search(&data[0], data_num, pos);
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::nanoseconds elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    std::cout << "Binary search elapsed time: " << elapsed.count() << " nanoseconds." << std::endl;

    start = std::chrono::high_resolution_clock::now();

    //test search scheme
#pragma omp parallel for schedule(static, 8)
    for (uint64_t i = 0; i < data_num; i++) {
        uint64_t pos = random() % data_num;
        hybrid_binary_search(&data[0], data_num, pos);
    }

    end = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    std::cout << "Optimized Binary search elapsed time: " << elapsed.count() << " nanoseconds." << std::endl;

    //test search scheme
#pragma omp parallel for schedule(static, 8)
    for (uint64_t i = 0; i < data_num; i++) {
        uint64_t pos = random() % data_num;
        avx_linear_search(&data[0], data_num, pos);
    }

    end = std::chrono::high_resolution_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    std::cout << "AVX linear search elapsed time: " << elapsed.count() << " nanoseconds." << std::endl;



    return 0;
}
