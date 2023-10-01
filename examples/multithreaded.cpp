//
// Created by admin on 2023/10/1.
//

#include <iostream>
#include "omp.h"
#include "alt_index.h"

using namespace std;

int main() {
    alt_index::AltIndex<uint64_t , uint64_t> alt;

    int key_num = 1000;
    pair<uint64_t , uint64_t > *keys = new pair<uint64_t , uint64_t >[key_num];
    for (int i = 0; i < 1000; i++) {
        keys[i]={i,i};
    }
    alt.bulkLoad(keys,1000);

    omp_set_num_threads(12);

#pragma omp parallel for schedule(static, 12)
    for (int i = 1000; i < 2000; i++) {
        alt.insert(i,i);
    }
#pragma omp parallel for schedule(static, 12)
    for (int i = 0; i < 2000; i++) {
        bool exist = false;
        std::cout<<"value at "<<i<<": "<<alt.find(i,exist)<<std::endl;
    }

    return 0;
}
