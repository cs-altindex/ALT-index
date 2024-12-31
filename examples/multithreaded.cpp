//
// Created by yyx on 2023/10/1.
//

#include <iostream>
#include "omp.h"
#include "alt_index.h"
#include <unordered_set>

using namespace std;

int cmp(pair<uint64_t , uint64_t > a, pair<uint64_t , uint64_t > b){
    return a.first < b.first;
}

int main() {
    alt_index::AltIndex<uint64_t , uint64_t> alt;

    int key_num = 200000;
    int init_num = 100000;

    unordered_set<uint64_t> data;
    while(data.size() < key_num){
        data.insert(rand() % 1000000000);
    }

    pair<uint64_t , uint64_t > *keys = new pair<uint64_t , uint64_t >[key_num];
    int i = 0;
    for (auto it = data.begin() ; it != data.end() ; it++,i++) {
        keys[i].first = *it;
        keys[i].second = i;
        // std::cout << keys[i].first<<" "<<keys[i].second<<std::endl;
    }
    sort(keys, keys + init_num, cmp);
    alt.bulkLoad(keys,init_num);

    // omp_set_num_threads(12);

// #pragma omp parallel for schedule(static, 12)
    for (i = init_num; i < key_num; i++) {
        bool exist = false;
        keys[i]={keys[i].first,i};
        std::cout << "insert key:" << keys[i].first<<", insert value: "<<keys[i].second<<std::endl;
        alt.insert(keys[i].first,keys[i].second);
    }
// #pragma omp parallel for schedule(static, 12)
    for (int i = 0; i < key_num; i++) {
        bool exist = false;
        std::cout<<"key: " << keys[i].first << " value at "<< i <<": "<<alt.find(keys[i].first,exist)<<std::endl;
        // alt.find(keys[i].first,exist);
        // if(!exist){
        //     std::cout<<"index: " << i << " key:"<<keys[i].first<<" not found!"<<std::endl;
        // }
    }

    return 0;
}
