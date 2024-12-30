//
// Created by yyx on 2023/3/17.
//

#ifndef ALT_INDEX_UTILS_H
#define ALT_INDEX_UTILS_H

#define FORCEINLINE __attribute__((always_inline)) inline

#include <utility>
#include <vector>
#include <iostream>
#include <immintrin.h>

// power of 2 at most x, undefined for x == 0
FORCEINLINE uint32_t bsr(uint32_t x) {
    return 31 - __builtin_clz(x);
}

template<typename key_type>
static int binary_search(const key_type *arr, const int n, const key_type& key) {
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
        // __builtin_prefetch(&arr[pos + step], 0, 0);
    }
    pos += 1;

    return (int) (arr[pos] > key ? pos - 1 : pos);
}

template <typename key_type>
static int hybrid_binary_search(const key_type *arr, const int n, const key_type& target) {
    uint64_t left = 0;
    uint64_t right = n - 1;
    constexpr int LINEAR_SEARCH_THRESHOLD = 8;

    while (left <= right) {
        if (right - left < LINEAR_SEARCH_THRESHOLD) {
            // switch to linear search
            for (uint64_t i = right; i >= left; i--) {
                if (arr[i] <= target) return static_cast<int>(i);
            }
            break;
        }

        uint64_t mid = left + ((right - left) >> 1);

        if (arr[mid] == target) return static_cast<int>(mid);
        else if (arr[mid] < target) {
            left = mid;
        }
        else {
            right = mid - 1;
        }
    }

    return static_cast<int>(left);
}

template <typename key_type>
int avx_linear_search(const key_type* array, const int n, const key_type& target) {
    __m256i target_vec = _mm256_set1_epi64x(target); 
    int i = 0;

    for (; i <= n - 4; i += 4) {
        __m256i array_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&array[i])); 
        __m256i cmp_mask = _mm256_cmpeq_epi64(array_vec, target_vec); 
        int mask = _mm256_movemask_epi8(cmp_mask); 

        if (mask != 0) { 
            int index = __builtin_ctz(mask) / 8; 
            return i + index;
        }
    }

    for (; i < n; ++i) {
        if (array[i] == target) {
            return i;
        }
    }

    return n - 1; 
}

#endif //ALT_INDEX_UTILS_H
