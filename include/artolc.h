//
// Created by yyx on 2023/3/27.
//

#ifndef ALT_INDEX_ARTOLC_H
#define ALT_INDEX_ARTOLC_H

#include "OptimizedART/Tree.h"
#include "OptimizedART/Tree.cpp"
#include <utility>

namespace alt_index
{

    template <class key_type, class value_type>
    class artInterface
    {
    public:
        artInterface()
        {
            index = new ART_OLC::Tree(loadKey);
            auto init_pair = new std::pair<key_type, value_type>(~0ull, 0);
            loadKey(reinterpret_cast<uint64_t>(&init_pair), max_key);
        }

        ~artInterface()
        {
            delete index;
        }

        void bulkLoad(std::pair<key_type, value_type> *data, size_t nums)
        {
            for (size_t i = 0; i < nums; i++)
            {
                put(data[i].first, data[i].second);
            }
        }

        bool put(key_type key, value_type value)
        {
            thread_local static auto tid = index->getThreadInfo();

            auto temp = new std::pair<key_type, value_type>(key, value);
            Key k;
            key_type reserved = swap_endian(key);
            k.set(reinterpret_cast<char *>(&reserved), sizeof(key));
            index->insert(k, reinterpret_cast<TID>(temp), tid);
            return true;
        }

        bool fastPut(key_type key, value_type value, int &fast_pointer_index)
        {
            thread_local static auto tid = index->getThreadInfo();

            auto temp = new std::pair<key_type, value_type>(key, value);
            Key k;
            key_type reserved = swap_endian(key);
            k.set(reinterpret_cast<char *>(&reserved), sizeof(key));
            index->fast_insert(k, reinterpret_cast<TID>(temp), fast_pointer_index, tid);
            return true;
        }

        bool get(key_type key, value_type &value)
        {
            thread_local static auto tid = index->getThreadInfo();
            bool ok = false;

            Key k;
            key_type reserved = swap_endian(key);
            k.set(reinterpret_cast<char *>(&reserved), sizeof(key));
            auto value_ptr = reinterpret_cast<std::pair<key_type, value_type> *>(index->lookup(k, tid));
            if (value_ptr)
            {
                value = value_ptr->second;
                ok = true;
            }

            return ok;
        }

        bool fastGet(key_type key, value_type &value, int &fast_pointer_index)
        {
            thread_local static auto tid = index->getThreadInfo();
            bool ok = false;

            Key k;
            key_type reserved = swap_endian(key);
            k.set(reinterpret_cast<char *>(&reserved), sizeof(key));
            auto value_ptr = reinterpret_cast<std::pair<key_type, value_type> *>(index->fast_lookup(k, fast_pointer_index, tid));
            if (value_ptr)
            {
                value = value_ptr->second;
                ok = true;
            }

            return ok;
        }

        bool update(key_type key, value_type value)
        {
            thread_local static auto tid = index->getThreadInfo();

            auto temp = new std::pair<key_type, value_type>(key, value);
            Key k;
            key_type reserved = swap_endian(key);
            k.set(reinterpret_cast<char *>(&reserved), sizeof(key));
            return index->update(k, reinterpret_cast<TID>(temp), tid);
        }

        bool remove(key_type key)
        {
            thread_local static auto tid = index->getThreadInfo();
            bool ok = false;

            Key k;
            key_type reserved = swap_endian(key);
            k.set(reinterpret_cast<char *>(&reserved), sizeof(key));
            index->remove(k, tid);
            return ok;
        }

        bool fast_remove(key_type key, int &fast_pointer_index)
        {
            thread_local static auto tid = index->getThreadInfo();
            bool ok = false;

            Key k;
            key_type reserved = swap_endian(key);
            k.set(reinterpret_cast<char *>(&reserved), sizeof(key));
            index->fast_remove(k, fast_pointer_index, tid);
            return ok;
        }

        size_t scan(key_type key_low_bound, key_type key_upper_bound, size_t key_num)
        {
            thread_local static auto t = index->getThreadInfo();
            Key k_start;
            k_start.setKeyLen(sizeof(key_low_bound));
            reinterpret_cast<key_type *>(&k_start[0])[0] = swap_endian(key_low_bound);

            Key k_end;
            k_end.setKeyLen(sizeof(key_upper_bound));
            reinterpret_cast<key_type *>(&k_end[0])[0] = swap_endian(key_upper_bound);

            TID results[key_num];
            size_t resultCount;
            Key continueKey;
            index->fast_lookupRange(k_start, k_end, continueKey, results, key_num, resultCount, t);
            return resultCount;
        }

        void build_fast_pointer(key_type key1, key_type key2, int &ret)
        {
            thread_local static auto tid = index->getThreadInfo();

            Key k1, k2;
            key_type reserved1 = swap_endian(key1);
            key_type reserved2 = swap_endian(key2);
            k1.set(reinterpret_cast<char *>(&reserved1), sizeof(key1));
            k2.set(reinterpret_cast<char *>(&reserved2), sizeof(key2));
            index->build_fast_pointer(k1, k2, ret, tid);
        }

        void makeFastRoot()
        {
            index->makeFastRoot();
        }

        ART_OLC::N *get_root()
        {
            return index->root;
        }

        // init the key load function
        static void loadKey(TID tid, Key &key)
        {
            std::pair<key_type, value_type> *value_ptr = reinterpret_cast<std::pair<key_type, value_type> *>(tid);
            key_type reserved = swap_endian(value_ptr->first);
            key.set(reinterpret_cast<char *>(&reserved), sizeof(value_ptr->first));
        }

        long long memory_consumption() const
        {
            return index->size() + index->memoryFastPointer();
        }

        void get_fast_pointer(std::vector<uint64_t>& res){
            index->getFastPointer(res);
        }

    public:
        Key max_key;          // upper bound of the key for scan
        ART_OLC::Tree *index; // art pointer

        inline static uint32_t swap_endian(uint32_t i)
        {
            return __builtin_bswap32(i);
        }
        inline static uint64_t swap_endian(uint64_t i)
        {
            return __builtin_bswap64(i);
        }
    };

}

#endif // ALT_INDEX_ARTOLC_H
