#include <iostream>
#include "alt_index.h"
#include "sys/time.h"
#include <sstream>
#include <fstream>
#include <random>

using namespace std;

template<class T>
long long load_binary_data(T *&data, long long length, const std::string &file_path) {
    // open key file
    std::ifstream is(file_path.c_str(), std::ios::binary | std::ios::in);
    if (!is.is_open()) {
        return 0;
    }

//    std::cout << file_path << std::endl;

    // read the number of keys
    T max_size;
    is.read(reinterpret_cast<char*>(&max_size), sizeof(T));

//    std::cout << max_size << std::endl;

    // create array
    if(length < 0 || length > max_size) length = max_size;
    data = new T[length];

    // read keys
    is.read(reinterpret_cast<char *>(data), std::streamsize(length * sizeof(T)));
    is.close();
    return length;
}

int main() {
    alt_index::AltIndex<uint64_t , uint64_t> index;
    bool exist = true;
    long long miss_cnt = 0;
    long long found_cnt = 0;
    uint64_t value;

    int table_size = 20000;
    int init_table_size = 10000;
    int write_table_size = 10000;

    struct timeval start, end;

    uint64_t* keys = new uint64_t[table_size];

//    //real-world dataset
//    string data_path = "../datasets/covid";
//    load_binary_data(keys, table_size, data_path);

    //synthetic data
    vector<int> temp;
    for (int i = 0; i < table_size; ++i){
        keys[i] = i;
    }
    random_shuffle(keys,keys+table_size);

    vector<pair<uint64_t , uint64_t>> data;
    data.reserve(init_table_size);
    std::random_shuffle(keys,keys+table_size);
    sort(keys, keys + init_table_size);

    for (int i = 0; i < init_table_size; i++) {
        data.push_back({keys[i], 12345678});
         cout << keys[i] << endl;
    }


    //bulk load data
    index.bulkLoad(data.data(), data.size());

    //test insert
    for (int i = init_table_size; i < init_table_size + write_table_size ; i++) {
        index.insert(keys[i], 12345678);
        // cout << keys[i] << endl;
    }

    std::cout <<"number of gpl model: " << index.nodes.size() << std::endl;
//    std::cout << index.buffer->memory_consumption() << std::endl;
    //test find
    for(int i = 0 ; i < table_size ; i++) {
        index.find(keys[i],exist);
        if (exist) {
            found_cnt++;
        } else {
            miss_cnt++;
        }
    }
    std::cout << "number of keys found: " << found_cnt << std::endl;
    std::cout << "number of keys not found: " << miss_cnt << std::endl;
//    uint64_t start_range,end_range;
//    start_range = 1344795473723449349;
//    end_range = 1344907383454146561;
//    std::cout <<  index.rangeQuery(start_range,end_range) << std::endl;


//     test update
//    for (int i = 0; i < init_table_size; i++) {
//        index.update(keys[i], 23456781);
        // cout << keys[i] << endl;
//    }


    return 0;

}
