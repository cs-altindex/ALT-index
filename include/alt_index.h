//
// Created by yyx on 2023/3/8.
//

#ifndef ALT_INDEX_ALT_INDEX_H
#define ALT_INDEX_ALT_INDEX_H

#include "linear_model.h"
#include "artolc.h"
#include "utils.h"
#include <stdint.h>
#include <math.h>
#include <limits>
#include <cstdio>
#include <stack>
#include <vector>
#include <cstring>
#include <sstream>
#include <map>

#define RT_ASSERT(expr) \
{ \
    if (!(expr)) { \
        fprintf(stderr, "RT_ASSERT Error at %s:%d, `%s`\n", __FILE__, __LINE__, #expr); \
        exit(0); \
    } \
}

#define PREALLOC_NODE_NUMS 10000000
#define EPSILON 4096
#define VECTOR_RESERVE_NUMS 10000000
#define ARR_GAPS 5

#define USE_FAST_POINTER true

typedef uint8_t bitmap_t;
#define BITMAP_WIDTH (sizeof(bitmap_t) * 8)
#define BITMAP_SIZE(num_items) (((num_items) + BITMAP_WIDTH - 1) / BITMAP_WIDTH)
#define BITMAP_GET(bitmap, pos) (((bitmap)[(pos) / BITMAP_WIDTH] >> ((pos) % BITMAP_WIDTH)) & 1)
#define BITMAP_SET(bitmap, pos) ((bitmap)[(pos) / BITMAP_WIDTH] |= 1 << ((pos) % BITMAP_WIDTH))
#define BITMAP_CLEAR(bitmap, pos) ((bitmap)[(pos) / BITMAP_WIDTH] &= ~bitmap_t(1 << ((pos) % BITMAP_WIDTH)))

namespace alt_index{

    template<class key_type, class value_type>
    class alt_node{

        struct Node;

        struct Item{
            union{
                struct{
                    key_type key;
                    value_type value;
                } data;
            } comp;

            std::atomic <uint64_t> typeVersionLockObsolete;

            bool isLocked(uint64_t version) {
                return ((version & 0b10) == 0b10);
            }

            bool isLocked() {
                return ((this->typeVersionLockObsolete.load() & 0b10) == 0b10);
            }

            void writeLockOrRestart(bool &needRestart) {
                uint64_t version;
                version = readLockOrRestart(needRestart);
                if (needRestart) return;

                upgradeToWriteLockOrRestart(version, needRestart);
            }

            void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
                if (this->typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10)) {
                    version = version + 0b10;
                } else {
                    _mm_pause();
                    needRestart = true;
                }
            }

            void writeUnlock() {
                this->typeVersionLockObsolete.fetch_add(0b10);
            }


            void checkOrRestart(uint64_t startRead, bool &needRestart) const {
                readUnlockOrRestart(startRead, needRestart);
            }

            uint64_t readLockOrRestart(bool &needRestart) {
                uint64_t version;
                version = this->typeVersionLockObsolete.load();
                if (isLocked(version) || isObsolete(version)) {
                    _mm_pause();
                    needRestart = true;
                }
                return version;
            }

            void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
                needRestart = (startRead != this->typeVersionLockObsolete.load());
            }


            void labelObsolete() {
                this->typeVersionLockObsolete.store((this->typeVersionLockObsolete.load() | 1));
            }

            bool isObsolete(uint64_t version) {
                return (version & 1) == 1;
            }

            bool isObsolete() {
                return (this->typeVersionLockObsolete.load() & 1) == 1;
            }
        };

        struct Node{
            bool is_two;
            int build_size;
            int size;
            int num_inserts, num_insert_to_data;
            int num_items;
            LinearModel<key_type> model;
            Item* items;
            int fast_pointer_index;
            bitmap_t* none_bitmap;
        };


        struct Segment{
            key_type first_key;
            int nums;
            double slope1;
            double slope2;
        };


        Node* root;
        std::stack<Node*> node_pool;

        std::allocator<Node> node_allocator;
        Node* new_nodes(int n)
        {
            Node* p = node_allocator.allocate(n);
            RT_ASSERT(p != NULL && p != (Node*)(-1));
            return p;
        }
        void delete_nodes(Node* p, int n)
        {
            node_allocator.deallocate(p, n);
        }

        std::allocator<Item> item_allocator;
        Item* new_items(int n)
        {
            Item* p = item_allocator.allocate(n);
            for(int i = 0 ; i < n ; i++){
                p[i].typeVersionLockObsolete = 0b100;
            }
            RT_ASSERT(p != NULL && p != (Item*)(-1));
            return p;
        }
        void delete_items(Item* p, int n)
        {
            item_allocator.deallocate(p, n);
        }

        std::allocator<bitmap_t> bitmap_allocator;
        bitmap_t* new_bitmap(int n)
        {
            bitmap_t* p = bitmap_allocator.allocate(n);
            RT_ASSERT(p != NULL && p != (bitmap_t*)(-1));
            return p;
        }

        void delete_bitmap(bitmap_t* p, int n)
        {
            bitmap_allocator.deallocate(p, n);
        }

        inline int expected_position(Node* node, const key_type& key) const {
            int ret = node->model.predict(key);
            int last_slot = node->num_items - 1;

            if (ret < 0) {
                return 0;
            }

            return (ret > last_slot) ? last_slot : ret;
        }

    public:
        typedef std::pair<key_type, value_type> bulk;

        alt_node(){
            Node* node = nullptr;
            for(int i = 0 ; i < PREALLOC_NODE_NUMS ; i++){
                node = new_nodes(1);
                node->is_two = 1;
                node->build_size = 2;
                node->size = 2;
                node->num_inserts = node->num_insert_to_data = 0;
                node->fast_pointer_index = 0;
                node_pool.push(node);
            }
            root = build_tree_none();

            node_keys.reserve(VECTOR_RESERVE_NUMS);
            nodes.reserve(VECTOR_RESERVE_NUMS);

            nodes_num = 0;
            node_keys_num = 0;
            buffer_num = 0;

            //init the art
            buffer = new art_interface<key_type, value_type>();

        }

        ~alt_node(){
            for(int i = 0 ; i < nodes_num ; i++){
                Node* temp = &nodes[i];
                destroy_alt_node(temp);
            }
        }

        bool insert(const bulk& kv){
            return insert(kv.first, kv.second);
        }

        bool insert(const key_type& key, const value_type& value){
            restart:
            bool needRestart = false;

            size_t node_pos = binary_search(&node_keys[0], node_keys_num, key);
            bool ok = true;

            if(node_pos >= nodes_num) {
                node_pos = nodes_num - 1;
            }

            Node* node = &nodes[node_pos];
            int predict_pos = expected_position(node, key);
            //read lock
            auto v = node->items[predict_pos].readLockOrRestart(needRestart);
            if(needRestart) goto restart;

            if(BITMAP_GET(node->none_bitmap, predict_pos)){
                //write lock
                node->items[predict_pos].upgradeToWriteLockOrRestart(v, needRestart);
                if(needRestart){
                    node->items[predict_pos].writeUnlock();
                    goto restart;
                }

                BITMAP_CLEAR(node->none_bitmap, predict_pos);
                node->items[predict_pos].comp.data.key = key;
                node->items[predict_pos].comp.data.value = value;
                node->items[predict_pos].writeUnlock();
            }
            else{
//                buffer[key] = value;
                if(USE_FAST_POINTER){
                    if(node_pos < nodes_num - 1) {
                        buffer->fast_put(key, value, node->fast_pointer_index);
                        buffer_num++;
                    }
                    else {
                        buffer->put(key, value);
                        buffer_num++;
                    }
                }
                else {
                    buffer->put(key, value);
                    buffer_num++;
                }
            }

            return ok;
        }


        value_type find(const key_type& key, bool& exist){
            restart:
            bool needRestart = false;

            size_t node_pos = binary_search(&node_keys[0], node_keys_num, key);
            if(node_pos >= nodes_num){
                node_pos = nodes_num - 1;
            }

            Node* node = &nodes[node_pos];
            int key_pos = expected_position(node, key);
            //read lock
            auto v = node->items[key_pos].readLockOrRestart(needRestart);
            if(needRestart) goto restart;

            if(BITMAP_GET(node->none_bitmap, key_pos)){
                exist = false;
                return static_cast<value_type>(0);
            }
            else{
                //key exist in node
                if(node->items[key_pos].comp.data.key == key){
                    node->items[key_pos].readUnlockOrRestart(v, needRestart);
                    if(needRestart) goto restart;

                    exist = true;
                    return node->items[key_pos].comp.data.value;
                }
                else{
                    value_type ret_val;

                    //if the fast pointer is invalid
                    if(USE_FAST_POINTER){
                        if(node_pos < nodes_num - 1){
                            if (buffer->fast_get(key, ret_val, node->fast_pointer_index)) {
                                exist = true;
                                return ret_val;
                            } else {
                                exist = false;
                                return static_cast<value_type>(0);
                            }
                        }
                        else{
                            if(buffer->get(key, ret_val)){
                                exist = true;
                                return ret_val;
                            }
                            else{
                                exist = false;
                                return static_cast<value_type>(0);
                            }
                        }
                    }
                        //if the fast pointer is valid
                    else {

                        if(buffer->get(key, ret_val)){
                            exist = true;
                            return ret_val;
                        }
                        else{
                            exist = false;
                            return static_cast<value_type>(0);
                        }
                    }
                }
            }
            return static_cast<value_type>(0);
        }

        bool update(const key_type& key, const value_type& value){
            restart:
            bool needRestart = false;

            size_t node_pos = binary_search(&node_keys[0], node_keys_num, key);
            bool ok = true;

            if(node_pos >= nodes_num) {
                node_pos = nodes_num - 1;
            }

            Node* node = &nodes[node_pos];
            int predict_pos = expected_position(node, key);
            //read lock
            auto v = node->items[predict_pos].readLockOrRestart(needRestart);
            if(needRestart) goto restart;

            //data doesn't exist in learned index
            if(BITMAP_GET(node->none_bitmap, predict_pos)){
                ok = false;
                return ok;
            }
            else{
                //write lock
                node->items[predict_pos].upgradeToWriteLockOrRestart(v, needRestart);
                if(needRestart){
                    node->items[predict_pos].writeUnlock();
                    goto restart;
                }

                if(node->items[predict_pos].comp.data.key == key){
                    node->items[predict_pos].comp.data.value = value;
                    node->items[predict_pos].writeUnlock();
                }
                else{
                    node->items[predict_pos].writeUnlock();
                    ok = buffer->update(key, value);
                }
            }
            return ok;
        }

        bool remove(const key_type& key){
            restart:
            bool needRestart = false;

            size_t node_pos = binary_search(&node_keys[0], node_keys_num, key);
            bool ok = true;

            if(node_pos >= nodes_num) {
                node_pos = nodes_num - 1;
            }

            Node* node = &nodes[node_pos];
            int predict_pos = expected_position(node, key);
            //read lock
            auto v = node->items[predict_pos].readLockOrRestart(needRestart);
            if(needRestart) goto restart;

            //data doesn't exist
            if(BITMAP_GET(node->none_bitmap, predict_pos)){
                return ok;
            }
            else{
                //write lock
                node->items[predict_pos].upgradeToWriteLockOrRestart(v, needRestart);
                if(needRestart){
                    node->items[predict_pos].writeUnlock();
                    goto restart;
                }

                if(node->items[predict_pos].comp.data.key == key){
                    BITMAP_SET(node->none_bitmap, predict_pos);
                    node->items[predict_pos].writeUnlock();
                }
                else{
                    node->items[predict_pos].writeUnlock();
                    if(USE_FAST_POINTER)
                        ok = buffer->fast_remove(key);
                    else
                        ok = buffer->remove(key);
                    if(ok)
                        buffer_num--;
                }
            }
            return ok;
        }

        int scan(const key_type& start, const key_type& end){
            int result_count = 0;

            int start_node_pos = binary_search(&node_keys[0], node_keys_num, start);
            int end_node_pos = binary_search(&node_keys[0], node_keys_num, end);

            Node* start_node = nodes[start_node_pos];
            Node* end_node = nodes[end_node_pos];

            int start_pos = expected_position(start_node, start);
            int end_pos = expected_position(end_node, end);

            while(start_node != end_node){
                while(start_pos < start_node->num_items){
                    if(!BITMAP_GET(start_node, start_pos)) {
                        result_count++;
                    }
                    start_pos++;
                }
                start_node_pos++;
                start_node = nodes[start_node_pos];
                start_pos = 0;
            }

            while(start_pos <= end_pos){
                if(!BITMAP_GET(start_node, start_pos)) {
                    result_count++;
                }
                start_pos++;
            }

            result_count += buffer->scan(start, end);
            return result_count;
        }


        void destroy_alt_node(Node* node){
            if(node->is_two){
                node->size = 2;
                node->num_inserts = node->num_insert_to_data = 0;
                node->none_bitmap = nullptr;
                node_pool.push(node);
            }else{
                delete_items(node->items, node->num_items);
                const int bitmap_size = BITMAP_SIZE(node->num_items);
                delete_bitmap(node->none_bitmap, bitmap_size);
//                    delete_nodes(node, 1);
            }
        }

        Node* build_tree_none(){
            Node* node = new_nodes(1);
            node->is_two = 0;
            node->build_size = 0;
            node->size = 0;
            node->num_inserts = node->num_insert_to_data = 0;
            node->num_items = 1;
            node->model.a = node->model.b = 0;
            return node;
        }



        void bulk_load(const bulk* kv, const int& num_keys){
            if(num_keys == 0){
                destroy_alt_node(root);
                root = build_tree_none();
                return ;
            }
            else if(num_keys == 1){
                destroy_alt_node(root);
                root = build_tree_none();
                return ;
            }
            //check array is mono
            for(int i = 1 ; i < num_keys ; i++){
                RT_ASSERT(kv[i].first > kv[i - 1].first);
            }

            key_type* keys = new key_type[num_keys];
            value_type* values = new value_type[num_keys];
            for(int i = 0 ; i < num_keys ; i++){
                keys[i] = kv[i].first;
                values[i] = kv[i].second;
            }

            int used_index = 0;
            int remain_nums = num_keys;
            //make segment partition
            while(remain_nums > 0){
                Segment segment;
                make_segment_partition(keys + used_index, remain_nums, segment, num_keys/500);
                nodes.push_back(*build_node_bulk(keys + used_index, values, segment));
                nodes_num++;
                used_index += segment.nums;
                remain_nums -= segment.nums;
                node_keys.push_back(segment.first_key);
                node_keys_num++;
            }
//            node_keys.push_back(std::numeric_limits<key_type>::max());
            if(USE_FAST_POINTER)
                make_fast_pointer();
            delete[] keys;
            delete[] values;
        }

        //return the first index of next segment
        void make_segment_partition(key_type* key_arr, int key_nums, Segment &ret, int epsilon){

            if(key_nums == 1){
                ret.first_key = key_arr[0];
                ret.nums = 1;
                ret.slope1 = 0.0;
                ret.slope2 = 0.0;
                return ;
            }

            if(key_nums == 2){
                ret.first_key = key_arr[0];
                ret.nums = 2;
                ret.slope1 = ret.slope2 = 1.0 / (key_arr[1] - key_arr[0]);
                return ;
            }

            double error1 = 0;     //record first function error
            double error2 = 0;     //record second function error
            double slope1 = 1.0 / (key_arr[1] - key_arr[0]);
            double slope2 = slope1;
            int cur_pos = 2;

            double new_slope = (cur_pos - 0.0) / (key_arr[cur_pos] - key_arr[0]);

            error1 = slope1 * (key_arr[cur_pos] - key_arr[0]) - cur_pos;
            error2 = cur_pos - slope2 * (key_arr[cur_pos] - key_arr[0]);

            // my segment partition algorithm
            while(cur_pos < key_nums){

                if(std::max(error1, error2) > epsilon){
                    break;
                }
                cur_pos++;

                if(slope1 < new_slope)
                    slope1 = new_slope;
                if(slope2 > new_slope)
                    slope2 = new_slope;

                new_slope = (cur_pos - 0.0) / (key_arr[cur_pos] - key_arr[0]);

                error1 = slope1 * (key_arr[cur_pos] - key_arr[0]) - cur_pos;
                error2 = cur_pos - slope2 * (key_arr[cur_pos] - key_arr[0]);
            }

//            printf("%llf %llf %d\n",slope1, slope2, cur_pos);
            ret.first_key = key_arr[0];
            ret.nums = cur_pos;
            ret.slope1 = slope1;
            ret.slope2 = slope2;
            return ;
        }

        //bulk loading a node, each node corresponds to a segment
        Node* build_node_bulk(key_type* keys, value_type* values, const Segment& segment){
            Node* node;
            //first find in node_pool
            if(node_pool.empty()){
                node = new_nodes(1);
            }
            else{
                node = node_pool.top();
                node_pool.pop();
            }

            const int size = segment.nums;

            node->is_two = 0;
            node->build_size = size;
            node->size = size;
            node->num_inserts = node->num_insert_to_data = 0;


            //init a and b
            node->model.a = ((segment.slope1 + segment.slope2) / 2) * (1.0 + ARR_GAPS);
//            node->model.a = segment.slope1 * (1.0 + ARR_GAPS);
            node->model.b = 0.0 - node->model.a * segment.first_key;
            RT_ASSERT(isfinite(node->model.a));
            RT_ASSERT(isfinite(node->model.b));

            node->num_items = size * (1.0 + ARR_GAPS);
            node->items = new_items(node->num_items);
            const int bitmap_size = BITMAP_SIZE(node->num_items);
            node->none_bitmap = new_bitmap(bitmap_size);
            memset(node->none_bitmap, 0xff, sizeof(bitmap_t) * bitmap_size);

            node->fast_pointer_index = 0;

            for(int index = 0 ; index < size; index++){
                int predict_pos = expected_position(node, keys[index]);
                //slot is empty
                if(BITMAP_GET(node->none_bitmap, predict_pos)){
                    BITMAP_CLEAR(node->none_bitmap, predict_pos);
                    node->items[predict_pos].comp.data.key = keys[index];
                    node->items[predict_pos].comp.data.value = values[index];
                    node->num_insert_to_data++;
                }
                else{
//                     buffer[keys[index]] = values[index];
                    buffer->put(keys[index], values[index]);
                    buffer_num++;
                }
                node->num_inserts++;
            }
            return node;
        }

        void make_fast_pointer(){
            int ret;
            for(int i = 0 ; i < nodes_num - 1 ; i++){
                buffer->build_fast_pointer(node_keys[i], node_keys[i + 1], ret);
                nodes[i].fast_pointer_index = ret;
//                flag < ret.second ? (flag = ret.second) : 0 ;
            }
            nodes[nodes_num - 1].fast_pointer_index = 0;
        }

        int memory_consumption() const{
            int size = 0;

            for(int i = 0 ; i < nodes.size() ; i++){
//                size += sizeof(nodes[i]);
                size += sizeof(*(nodes[i].none_bitmap));
                size += sizeof(nodes[i].model);
                size += sizeof(nodes[i].none_bitmap);
                size += sizeof(nodes[i].fast_pointer_index);
                size += sizeof(key_type);
//                for(int i = 0 ; i < nodes[i].num_items ; i++){
//                    size += sizeof(Item);
//                }
            }
//            size += buffer->memory_consumption();
            return size;
        }

        void print_fast_pointer(){
            std::vector<uint64_t> res;
            buffer->get_fast_pointer(res);
            long double save_path_num = 0.0;
            for(auto node:nodes){
                save_path_num += res[node.fast_pointer_index] * (node.num_inserts - node.num_insert_to_data);
            }
            std::cout << "average reduced path length:" << save_path_num / buffer_num << std::endl;
            std::cout << "fast buffer size:" << res.size() << std::endl;
        }


    public:
        std::vector<Node> nodes;       // store model nodes
        std::vector<key_type> node_keys;    //through node_keys to locate model index
//        std::map<key_type, value_type> buffer;  //conflict data will be put into buffer
        art_interface<key_type, value_type>* buffer;

        long long nodes_num;
        long long node_keys_num;
        long long buffer_num;



    };

}



#endif //ALT_INDEX_ALT_INDEX_H
