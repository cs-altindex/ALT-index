//
// Created by yyx on 2023/3/17.
//

#ifndef ALT_INDEX_UTILS_H
#define ALT_INDEX_UTILS_H

#define FORCEINLINE __attribute__((always_inline)) inline

#include <utility>

// power of 2 at most x, undefined for x == 0
FORCEINLINE uint32_t bsr(uint32_t x) {
return 31 - __builtin_clz(x);
}

template<typename key_type>
static int binary_search(const key_type *arr, const int n, const key_type key) {
    if(n == 1)
        return 0;
    intptr_t pos = -1;
    intptr_t logstep = bsr(n - 1);
    intptr_t step = intptr_t(1) << logstep;

    pos = (arr[pos + n - step] < key ? pos + n - step : pos);
    step >>= 1;

    while (step > 0) {
        pos = (arr[pos + step] < key ? pos + step : pos);
        step >>= 1;
        __builtin_prefetch(&arr[pos + step], 0, 0);
//        __builtin_prefetch(&arr[pos], 0, 0);
    }
    pos += 1;

    return (int) (arr[pos] > key ? pos - 1 : pos);
}


template<typename key_type, typename value_type>
static void loadKey(TID tid, Key &key) {
    // Store the key of the tuple into the key vector
    // Implementation is database specific

    std::pair<key_type, value_type> * valPtr = reinterpret_cast<std::pair<key_type, value_type> *>(tid);
    key_type reversed = swap_endian(valPtr->first);
    key.set(reinterpret_cast<char *>(&reversed), sizeof(valPtr->first));
    // key.setKeyLen(sizeof(valPtr->first));
    // reinterpret_cast<KEY_TYPE *>(&key[0])[0] = swap_endian(valPtr->first);
}

#endif //ALT_INDEX_UTILS_H
