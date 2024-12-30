//
// Created by yyx on 2023/3/8.
//

#ifndef ALT_INDEX_ALT_INDEX_H
#define ALT_INDEX_ALT_INDEX_H

#include "linear_model.h"
#include "artolc.h"
#include "utils.h"
#include "gpl.h"
#include <stdint.h>
#include <math.h>
#include <limits>
#include <cstdio>
#include <stack>
#include <vector>
#include <cstring>
#include <map>

#define RT_ASSERT(expr)                                                                     \
    {                                                                                       \
        if (!(expr))                                                                        \
        {                                                                                   \
            fprintf(stderr, "RT_ASSERT Error at %s:%d, `%s`\n", __FILE__, __LINE__, #expr); \
            exit(0);                                                                        \
        }                                                                                   \
    }

#define PREALLOC_NODE_NUMS 1000000
#define VECTOR_RESERVE_NUMS 10000000
#define ARR_GAPS 2

#define USE_FAST_POINTER true
#define USE_DYNAMIC_RETRAIN true

#define USE_STATISTIC false

// #define ENABLE_DEBUG

typedef uint8_t bitmap_t;
#define BITMAP_WIDTH (sizeof(bitmap_t) * 8)
#define BITMAP_SIZE(numItems) (((numItems) + BITMAP_WIDTH - 1) / BITMAP_WIDTH)
#define BITMAP_GET(bitmap, pos) (((bitmap)[(pos) / BITMAP_WIDTH] >> ((pos) % BITMAP_WIDTH)) & 1)
#define BITMAP_SET(bitmap, pos) ((bitmap)[(pos) / BITMAP_WIDTH] |= 1 << ((pos) % BITMAP_WIDTH))
#define BITMAP_CLEAR(bitmap, pos) ((bitmap)[(pos) / BITMAP_WIDTH] &= ~bitmap_t(1 << ((pos) % BITMAP_WIDTH)))

namespace alt_index
{

    template <class KeyType, class ValueType>
    class AltIndex
    {

        // key-value pairs
        struct Item
        {
            union
            {
                struct
                {
                    KeyType key;
                    ValueType value;
                } data;
            } components;

            // atomic variable for item concurrency
            std::atomic<uint64_t> typeVersionLockObsolete;

            // Check if the item is locked based on the version
            bool isLocked(uint64_t version)
            {
                bool locked = ((version & 0b10) == 0b10);
#ifdef ENABLE_DEBUG
                std::cout << "Item is " << (locked ? "locked" : "not locked") << " based on version " << version << std::endl;
#endif
                return locked;
            }

            // Check if the item is locked using the current version
            bool isLocked()
            {
                uint64_t currentVersion = this->typeVersionLockObsolete.load();
                bool locked = ((currentVersion & 0b10) == 0b10);
#ifdef ENABLE_DEBUG
                std::cout << "Item is " << (locked ? "locked" : "not locked") << " based on current version " << currentVersion << std::endl;
#endif
                return locked;
            }

            // Acquire a write lock or restart if necessary
            void writeLockOrRestart(bool &needRestart)
            {
                uint64_t version;
                version = readLockOrRestart(needRestart);
                if (needRestart)
                {
#ifdef ENABLE_DEBUG
                    std::cout << "Restart needed after read lock attempt." << std::endl;
#endif
                    return;
                }

                upgradeToWriteLockOrRestart(version, needRestart);
            }

            // Upgrade to a write lock or restart if necessary
            void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart)
            {
                uint64_t expected = version;
                uint64_t desired = version + 0b10;
                if (this->typeVersionLockObsolete.compare_exchange_strong(expected, desired))
                {
                    version = desired;
#ifdef ENABLE_DEBUG
                    std::cout << "Successfully upgraded to write lock with version " << version << std::endl;
#endif
                }
                else
                {
                    _mm_pause();
                    needRestart = true;
#ifdef ENABLE_DEBUG
                    std::cout << "Failed to upgrade to write lock, restart needed." << std::endl;
#endif
                }
            }

            // Release the write lock
            void writeUnlock()
            {
                this->typeVersionLockObsolete.fetch_add(0b10);
#ifdef ENABLE_DEBUG
                std::cout << "Write lock released." << std::endl;
#endif
            }

            // Check for restart based on the start read version
            void checkOrRestart(uint64_t startRead, bool &needRestart) const
            {
                readUnlockOrRestart(startRead, needRestart);
            }

            // Acquire a read lock or restart if necessary
            uint64_t readLockOrRestart(bool &needRestart)
            {
                uint64_t version;
                version = this->typeVersionLockObsolete.load();
                if (isLocked(version) || isObsolete(version))
                {
                    _mm_pause();
                    needRestart = true;
#ifdef ENABLE_DEBUG
                    std::cout << "Restart needed after read lock attempt due to lock or obsolescence." << std::endl;
#endif
                }
                else
                {
#ifdef ENABLE_DEBUG
                    std::cout << "Read lock acquired with version " << version << std::endl;
#endif
                }
                return version;
            }

            // Release the read lock or restart if necessary
            void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const
            {
                needRestart = (startRead != this->typeVersionLockObsolete.load());
                if (needRestart)
                {
#ifdef ENABLE_DEBUG
                    std::cout << "Restart needed after read unlock attempt due to version mismatch." << std::endl;
#endif
                }
                else
                {
#ifdef ENABLE_DEBUG
                    std::cout << "Read lock released successfully." << std::endl;
#endif
                }
            }

            // Mark the item as obsolete
            void labelObsolete()
            {
                this->typeVersionLockObsolete.store((this->typeVersionLockObsolete.load() | 1));
#ifdef ENABLE_DEBUG
                std::cout << "Item marked as obsolete." << std::endl;
#endif
            }

            // Check if the item is obsolete based on the version
            bool isObsolete(uint64_t version)
            {
                bool obsolete = (version & 1) == 1;
#ifdef ENABLE_DEBUG
                std::cout << "Item is " << (obsolete ? "obsolete" : "not obsolete") << " based on version " << version << std::endl;
#endif
                return obsolete;
            }

            // Check if the item is obsolete using the current version
            bool isObsolete()
            {
                uint64_t currentVersion = this->typeVersionLockObsolete.load();
                bool obsolete = (currentVersion & 1) == 1;
#ifdef ENABLE_DEBUG
                std::cout << "Item is " << (obsolete ? "obsolete" : "not obsolete") << " based on current version " << currentVersion << std::endl;
#endif
                return obsolete;
            }
        };

        // GPL model
        struct Node
        {
            int size;                     // Current size
            volatile int numInserts;      // Number of inserts
            volatile int numInsertToData; // Number of inserts to item array
            int numItems;                 // Number of items
            LinearModel<KeyType> model;   // Model information
            Item *items;                  // Pointer to items
            int fastPointerIndex;         // Fast pointer index
            bitmap_t *noneBitmap;         // Bitmap pointer
            Node *expandNode;             // Pointer to expanded node
            bool expand;                  // Flag for expansion
            bool allocateExpand;          // Flag to allocate and expand
        };

        Node *root;                  // for initial node
        std::stack<Node *> nodePool; // pre allocated node and

        // allocate space for nodes
        std::allocator<Node> nodeAllocator;
        Node *new_nodes(int n)
        {
            Node *p = nodeAllocator.allocate(n);
            RT_ASSERT(p != NULL && p != (Node *)(-1));
            return p;
        }
        // deallocate space foe nodes
        void delete_nodes(Node *p, int n)
        {
            nodeAllocator.deallocate(p, n);
        }

        // allocate space for items
        std::allocator<Item> itemAllocator;
        Item *new_items(int n)
        {
            Item *p = itemAllocator.allocate(n);
            for (int i = 0; i < n; i++)
            {
                p[i].typeVersionLockObsolete = 0b100;
            }
            RT_ASSERT(p != NULL && p != (Item *)(-1));
            return p;
        }

        // deallocate space for items
        void delete_items(Item *p, int n)
        {
            itemAllocator.deallocate(p, n);
        }

        // allocate space for bitmap
        std::allocator<bitmap_t> bitmapAllocator;
        bitmap_t *new_bitmap(int n)
        {
            bitmap_t *p = bitmapAllocator.allocate(n);
            RT_ASSERT(p != NULL && p != (bitmap_t *)(-1));
            return p;
        }

        // deallocate space for bitmap
        void delete_bitmap(bitmap_t *p, int n)
        {
            bitmapAllocator.deallocate(p, n);
        }

        // calculate the expected position within a GPL model
        inline int expected_position(Node *node, const KeyType &key) const
        {
            int ret = node->model.predict(key);
            int last_slot = node->numItems - 1;

            if (ret < 0)
            {
                return 0;
            }

            return (ret > last_slot) ? last_slot : ret;
        }

    public:
        // bulk load data in pairs
        typedef std::pair<KeyType, ValueType> payload;

        /**
         * @brief Constructor to initialize the index structure.
         */
        AltIndex()
        {
            Node *node = nullptr;
            for (int i = 0; i < PREALLOC_NODE_NUMS; i++)
            {
                node = new_nodes(1);
                node->numInserts = node->numInsertToData = 0;
                node->fastPointerIndex = 0;
                node->expandNode = nullptr;
                node->expand = false;
                node->allocateExpand = false;
                nodePool.push(node);
            }
            root = build_tree_none();

            node_keys.reserve(VECTOR_RESERVE_NUMS);
            nodes.reserve(VECTOR_RESERVE_NUMS);

            nodes_num = 0;
            node_keys_num = 0;
            buffer_num = 0;

            // init the art
            buffer = new artInterface<KeyType, ValueType>();
        }

        /**
         * @brief Destructor to release memory.
         */
        ~AltIndex()
        {
            for (int i = 0; i < nodes_num; i++)
            {
                Node *temp = nodes[i];
                destroyNode(temp);
            }
        }

        /**
         * @brief Insert a key-value pair into the index.
         * @param kv Key-value pair
         * @return True if insertion is successful, false otherwise.
         */
        bool insert(const payload &kv)
        {
            return insert(kv.first, kv.second);
        }

        /**
         * @brief Insert a key-value pair into the index.
         * @param key Key
         * @param value Value
         * @return True if insertion is successful, false otherwise.
         */
        bool insert(const KeyType &key, const ValueType &value)
        {
            size_t node_pos = binary_search(&node_keys[0], node_keys_num, key);
            bool ok = false;

            if (node_pos >= nodes_num)
            {
                node_pos = nodes_num - 1;
            }

            Node *node = nodes[node_pos];

            int predict_pos = expected_position(node, key);

        restart:
            // read lock
            bool needRestart = false;
            auto v = node->items[predict_pos].readLockOrRestart(needRestart);
            if (needRestart)
                goto restart;

            // insert to dynamic retrain buffer
            if (USE_DYNAMIC_RETRAIN && node->expand)
            {
                // std::cout << "haha1" << std::endl;
                if (BITMAP_GET(node->noneBitmap, predict_pos))
                {
                    // std::cout << "haha2" << std::endl;
                    // if(key == 349517445){
                    //     std::cout << "insert to expand node" << key << std::endl;
                    // }
                    insertToExpand(node->expandNode, node_pos, key, value);
                }
                else
                {
                    // std::cout << "haha3" << std::endl;
                    //first evict data at old position, and insert new data to expand node
                    // if(key == 349517445){
                    //     std::cout << "evict and insert" << key << std::endl;
                    // }
                    evictData(node, node_pos, predict_pos);
                    BITMAP_SET(node->noneBitmap, predict_pos);
                    // if(node_pos == 143 && predict_pos <= 3){
                    //     std::cout << "warning key: " << key << "predict pos: " << predict_pos << std::endl;
                    // }
                    insertToExpand(node->expandNode, node_pos, key, value);
                }
                node->numInserts++;
                ok = true;
            }
            else
            {   // no expand
                if (BITMAP_GET(node->noneBitmap, predict_pos))
                {
                    // write lock
                    node->items[predict_pos].upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart)
                    {
                        //                        node->items[predict_pos].writeUnlock();
                        goto restart;
                    }
                    BITMAP_CLEAR(node->noneBitmap, predict_pos);
                    node->items[predict_pos].components.data.key = key;
                    node->items[predict_pos].components.data.value = value;
                    node->items[predict_pos].writeUnlock();
                    // if(key == 349517445){
                    //     std::cout << "insert to gpl model" << key << std::endl;
                    // }
                }
                else
                {
                    if (USE_DYNAMIC_RETRAIN)
                    {
                        if (node->items[predict_pos].components.data.key == 0)
                        {
                            node->items[predict_pos].upgradeToWriteLockOrRestart(v, needRestart);
                            if (needRestart)
                            {
                                goto restart;
                            }
                            BITMAP_CLEAR(node->noneBitmap, predict_pos);
                            node->items[predict_pos].components.data.key = key;
                            node->items[predict_pos].components.data.value = value;
                            node->items[predict_pos].writeUnlock();
                            // if(key == 349517445){
                            //     std::cout << "insert to gpl model sparse slots" << key << std::endl;
                            // }
                        }
                    }
                    // if(key == 349517445){
                    //     std::cout << "node position: " << node_pos << " insert to art" << key << " index :" << predict_pos << std::endl;
                    // }
                    insertToBuffer(node_pos, key, value, node);
                }
                node->numInserts++;
                ok = true;
            }

            if (USE_DYNAMIC_RETRAIN)
            {
                // dynamic retrain
                if (node->numInserts > node->numItems && node->expand == false)
                {
                    if (node->allocateExpand == false && node->expandNode == nullptr)
                    {
                        node->allocateExpand = true;
                        Node *expandNode = new_nodes(1);
                        //                        memccpy(node, expandNode, sizeof(node));

                        expandNode->numInserts = expandNode->numInsertToData = 0;
                        expandNode->numItems = node->numItems * 2;
                        expandNode->expandNode = nullptr;
                        expandNode->expand = false;

                        expandNode->items = new_items(expandNode->numItems);
                        const int bitmap_size = BITMAP_SIZE(expandNode->numItems);
                        expandNode->noneBitmap = new_bitmap(bitmap_size);
                        memset(expandNode->noneBitmap, 0xff, sizeof(bitmap_t) * bitmap_size);
                        expandNode->model.a = node->model.a * 2.0;
                        expandNode->model.b = 0.0 - node_keys[node_pos] * expandNode->model.a;
                        expandNode->fastPointerIndex = node->fastPointerIndex;

                        node->expandNode = expandNode;
                        node->expand = true;
                        node->allocateExpand = false;
                        if (node_pos == nodes.size() - 1)
                        { // last gpl model
                            Node *newNode = new_nodes(1);
                            newNode->numItems = node->numItems;
                            newNode->numInserts = 0;
                            newNode->expandNode = nullptr;
                            newNode->expand = false;
                            newNode->items = new_items(expandNode->numItems);
                            const int bitmap_size = BITMAP_SIZE(expandNode->numItems);
                            newNode->noneBitmap = new_bitmap(bitmap_size);
                            memset(expandNode->noneBitmap, 0xff, sizeof(bitmap_t) * bitmap_size);
                            newNode->model.a = node->model.a;
                            newNode->model.b = node->model.b;
                            newNode->fastPointerIndex = node->fastPointerIndex;
                            nodes.emplace_back(newNode);

                            int old_first_key = node_keys[node_keys.size() - 1];
                            int new_first_key = old_first_key + node->numItems / node->model.a;
                            node_keys.emplace_back(new_first_key);
                            node_keys_num++;
                            buffer->build_fast_pointer(old_first_key, new_first_key, node->fastPointerIndex);
                        }
                        //    std::cout << "trigger dynamic retraining:" << node_keys[node_pos] << std::endl;
                    }
                }
                // force evict data
                if (node->numInserts > 2 * node->numItems && node->expand == true)
                {
                    node->numInserts = 0;
                    for (int i = 0; i < node->numItems; i++)
                    {
                        if(!BITMAP_GET(node->noneBitmap, i)){
                            // std::cout << "haha4" << std::endl;
                                evictData(node, node_pos, i);
                                // BITMAP_SET(node->noneBitmap, i);
                        }
                    }
                    // update pointer
                    nodes[node_pos] = nodes[node_pos]->expandNode;
                    // std::cout << "finish expansion" << node_pos << std::endl;
                }
            }
            return ok;
        }

        /**
         * @brief Find the value associated with a key in the index.
         * @param key Key
         * @param exist Flag to store whether the key exists
         * @return If the key exists, return its corresponding value; otherwise, return the default value.
         */
        ValueType find(const KeyType &key, bool &exist)
        {

            size_t node_pos = binary_search(&node_keys[0], node_keys_num, key);
            if (node_pos >= nodes_num)
            {
                node_pos = nodes_num - 1;
            }

            Node *node = nodes[node_pos];
            int key_pos = expected_position(node, key);
        // read lock
        restart:
            bool needRestart = false;
            auto v = node->items[key_pos].readLockOrRestart(needRestart);
            if (needRestart)
                goto restart;
            if (BITMAP_GET(node->noneBitmap, key_pos))
            {
                if (USE_DYNAMIC_RETRAIN && node->expand)
                {
                    return searchInExpand(node->expandNode, exist, node_pos, key);
                }
                else
                {
                    if(key == 349517445){
                        std::cout << "node position: " << node_pos << " not found " << key << "position:" << key_pos << std::endl;
                    }
                    exist = false;
                    return static_cast<ValueType>(0);
                }
            }
            else
            {
                // key exist in node
                if (node->items[key_pos].components.data.key == key)
                {
                    node->items[key_pos].readUnlockOrRestart(v, needRestart);
                    if (needRestart)
                        goto restart;

                    exist = true;
                    return node->items[key_pos].components.data.value;
                }
                else
                {
                    if (USE_DYNAMIC_RETRAIN)
                    {
                        if (node->expand){
                            return searchInExpand(node->expandNode, exist, node_pos, key);
                        }

                        ValueType ret = searchInBuffer(node_pos, key, exist, node);
                        if (exist && node->items[key_pos].components.data.key == 0)
                        {
                            node->items[key_pos].components.data.value = ret;
                            node->items[key_pos].components.data.key = key;
                        }
                        return ret;
                    }

                    return searchInBuffer(node_pos, key, exist, node);
                }
            }
            return static_cast<ValueType>(0);
        }

        /**
         * @brief Update the value associated with a key in the index.
         * @param key Key
         * @param value New value
         * @return True if the update is successful, false otherwise.
         */
        bool update(const KeyType &key, const ValueType &value)
        {
            size_t node_pos = binary_search(&node_keys[0], node_keys_num, key);
            if (node_pos >= nodes_num)
            {
                node_pos = nodes_num - 1;
            }

            Node *node = nodes[node_pos];
            int key_pos = expected_position(node, key);
        // read lock
        restart:
            bool needRestart = false;
            auto v = node->items[key_pos].readLockOrRestart(needRestart);
            if (needRestart)
                goto restart;
            if (BITMAP_GET(node->noneBitmap, key_pos))
            {
                if (USE_DYNAMIC_RETRAIN && node->expand)
                {
                    return updateInExpand(node->expandNode, node_pos, key, value);
                }
                else
                {
                    return false;
                }
            }
            else
            {
                // key exist in node
                if (node->items[key_pos].components.data.key == key)
                {
                    node->items[key_pos].readUnlockOrRestart(v, needRestart);
                    if (needRestart)
                        goto restart;

                    return true;
                }
                else
                {
                    if (USE_DYNAMIC_RETRAIN && node->expand)
                    {
                        return updateInExpand(node->expandNode, node_pos, key, value);
                    }

                    return buffer->update(key, value);
                }
            }
            return true;
        }

        /**
         * @brief Remove a key-value pair from the index.
         * @param key Key
         * @return True if removal is successful, false otherwise.
         */
        bool remove(const KeyType &key)
        {
            size_t node_pos = binary_search(&node_keys[0], node_keys_num, key);
            bool ok = true;

            if (node_pos >= nodes_num)
            {
                node_pos = nodes_num - 1;
            }

            Node *node = nodes[node_pos];
            int predict_pos = expected_position(node, key);

        restart:
            bool needRestart = false;
            // read lock
            auto v = node->items[predict_pos].readLockOrRestart(needRestart);
            if (needRestart)
                goto restart;

            // data doesn't exist
            if (BITMAP_GET(node->noneBitmap, predict_pos))
            {
                return ok;
            }
            else
            {
                // write lock
                node->items[predict_pos].upgradeToWriteLockOrRestart(v, needRestart);
                if (needRestart)
                {
                    //                    node->items[predict_pos].writeUnlock();
                    goto restart;
                }

                if (node->items[predict_pos].components.data.key == key)
                {
                    node->items[predict_pos].components.data.key = 0;
                    node->items[predict_pos].writeUnlock();
                }
                else
                {
                    node->items[predict_pos].writeUnlock();
                    if (USE_FAST_POINTER)
                        ok = buffer->fast_remove(key, node->fastPointerIndex);
                    else
                        ok = buffer->remove(key);
                    if (USE_STATISTIC)
                    {
                        if (ok)
                            buffer_num--;
                    }
                }
            }
            return ok;
        }

        /**
         * @brief Perform a range query within the given range and return the number of key-value pairs.
         * @param start Start key of the range
         * @param end End key of the range
         * @return The number of key-value pairs within the specified range.
         */
        int rangeQuery(std::pair<KeyType, ValueType> *results, const KeyType &start, int len)
        {
            int result_count = 0;

            int node_pos = binary_search(&node_keys[0], node_keys_num, start);

            if (node_pos == -1)
                node_pos = 0;

            Node *cur_node = nodes[node_pos];
            int cur_pos = expected_position(cur_node, start);

            while (result_count < len)
            {
                if (cur_pos > cur_node->numItems)
                {
                    node_pos++;
                    if (node_pos >= nodes_num)
                        break;
                    cur_node = nodes[node_pos];
                    cur_pos = 0;
                }
                if (!BITMAP_GET(cur_node->noneBitmap, cur_pos))
                {
                    results[result_count] = {cur_node->items[cur_pos].components.data.key, cur_node->items[cur_pos].components.data.value};
                    result_count++;
                }
                cur_pos++;
            }

            result_count += buffer->scan(start, results[result_count - 1].first, len);
            return result_count >= len ? len : result_count;
        }

        // evict the data of the evict_pos of node
        void evictData(Node *node, const int &node_pos, const int &evict_pos)
        {
            // check expand node bitmap and evict the data
            if(node->items[evict_pos].components.data.key != 0){
                if(node->items[evict_pos].components.data.key == 349517445){
                    std::cout << "evict to expand node" << node->items[evict_pos].components.data.key << std::endl;
                }
                insertToExpand(node->expandNode, node_pos, node->items[evict_pos].components.data.key, node->items[evict_pos].components.data.value);
            }
            BITMAP_CLEAR(node->expandNode->noneBitmap, evict_pos * 2);
            BITMAP_CLEAR(node->expandNode->noneBitmap, evict_pos * 2 + 1);
        }

        void insertToExpand(Node *expandNode, const int &node_pos, const KeyType &key, const ValueType &value)
        {
            int expand_pos = expected_position(expandNode, key);
            bool needRestart = false;

        restart:
            // read lock
            auto v = expandNode->items[expand_pos].readLockOrRestart(needRestart);
            if (needRestart)
                goto restart;
            if (BITMAP_GET(expandNode->noneBitmap, expand_pos))
            {
                expandNode->items[expand_pos].upgradeToWriteLockOrRestart(v, needRestart);
                if (needRestart)
                {
                    //                    expandNode->items[expand_pos].writeUnlock();
                    goto restart;
                }

                BITMAP_CLEAR(expandNode->noneBitmap, expand_pos);
                expandNode->items[expand_pos].components.data.key = key;
                expandNode->items[expand_pos].components.data.value = value;
                expandNode->items[expand_pos].writeUnlock();
                if(key == 349517445){
                    std::cout << "evict and insert to expand node" << key << std::endl;
                }
            }
            else
            {
                // std::cout << "haha5" << std::endl;
                if(expandNode->items[expand_pos].components.data.key == 0){
                    expandNode->items[expand_pos].upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart)
                    {
                        //                    expandNode->items[expand_pos].writeUnlock();
                        goto restart;
                    }

                    expandNode->items[expand_pos].components.data.key = key;
                    expandNode->items[expand_pos].components.data.value = value;
                    expandNode->items[expand_pos].writeUnlock();
                }
                else{
                    insertToBuffer(node_pos, key, value, expandNode);
                }
            }
            expandNode->numInserts++;
        }

        void insertToBuffer(const int &node_pos, const KeyType &key, const ValueType &value, Node *node)
        {
            // insert to buffer
            if (USE_FAST_POINTER)
            {
                if (node_pos < nodes_num - 1)
                {
                    // std::cout << "haha6" << std::endl;
                    // std::cout << node_pos << " " << key << std::endl;
                    buffer->fastPut(key, value, node->fastPointerIndex);
                    if (USE_STATISTIC)
                    {
                        buffer_num++;
                    }
                }
                else
                {
                    // std::cout << "haha7" << std::endl;
                    buffer->put(key, value);
                    if (USE_STATISTIC)
                    {
                        buffer_num++;
                    }
                }
            }
            else
            {
                buffer->put(key, value);
                if (USE_STATISTIC)
                {
                    buffer_num++;
                }
            }
        }

        ValueType searchInExpand(Node *expandNode, bool &exist, const int &node_pos, const KeyType &key)
        {
        restart:
            bool needRestart = false;
            int expand_pos = expected_position(expandNode, key);
            auto v = expandNode->items[expand_pos].readLockOrRestart(needRestart);
            if (needRestart)
                goto restart;

            if (BITMAP_GET(expandNode->noneBitmap, expand_pos))
            {
                return searchInBuffer(node_pos, key, exist, expandNode);
            }
            else
            {
                // slot is occupied
                if (expandNode->items[expand_pos].components.data.key == key)
                {
                    expandNode->items[expand_pos].readUnlockOrRestart(v, needRestart);
                    if (needRestart)
                        goto restart;

                    exist = true;
                    return expandNode->items[expand_pos].components.data.value;
                }
                else
                {
                    ValueType ret_val;
                    // if the fast pointer is invalid
                    ret_val = searchInBuffer(node_pos, key, exist, expandNode);
                    return ret_val;
                }
            }
        }

        ValueType searchInBuffer(const int &node_pos, const KeyType &key, bool &exist, Node *node)
        {
            ValueType ret_val;
            if (USE_FAST_POINTER)
            {
                if (node_pos < nodes_num - 1)
                {
                    if (buffer->fastGet(key, ret_val, node->fastPointerIndex))
                    {
                        exist = true;
                        return ret_val;
                    }
                    else
                    {
                        exist = false;
                        return static_cast<ValueType>(0);
                    }
                }
                else
                {
                    if (buffer->get(key, ret_val))
                    {
                        exist = true;
                        return ret_val;
                    }
                    else
                    {
                        exist = false;
                        return static_cast<ValueType>(0);
                    }
                }
            }
            // if the fast pointer is valid
            else
            {

                if (buffer->get(key, ret_val))
                {
                    exist = true;
                    return ret_val;
                }
                else
                {
                    exist = false;
                    return static_cast<ValueType>(0);
                }
            }
        }

        bool updateInExpand(Node *expandNode, const int &node_pos, const KeyType &key, const ValueType &value)
        {
        restart:
            bool needRestart = false;
            int expand_pos = expected_position(expandNode, key);
            auto v = expandNode->items[expand_pos].readLockOrRestart(needRestart);
            if (needRestart)
                goto restart;

            if (BITMAP_GET(expandNode->noneBitmap, expand_pos))
            {
                return buffer->update(key, value);
            }
            else
            {
                // slot is occupied
                if (expandNode->items[expand_pos].components.data.key == key)
                {
                    expandNode->items[expand_pos].upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart)
                        goto restart;
                    expandNode->items[expand_pos].components.data.value = value;
                    return true;
                }
                else
                {
                    return buffer->update(key, value);
                }
            }
        }

        /**
         * @brief Destroy a node and reclaim memory.
         * @param node The node to be destroyed.
         */
        void destroyNode(Node *node)
        {
            if (node == nullptr)
                return;

            delete_items(node->items, node->numItems);
            const int bitmap_size = BITMAP_SIZE(node->numItems);
            delete_bitmap(node->noneBitmap, bitmap_size);
        }

        /**
         * @brief Build an empty node.
         * @return The newly created node.
         */
        Node *build_tree_none()
        {
            Node *node = new_nodes(1);
            node->numInserts = node->numInsertToData = 0;
            node->numItems = 1;
            node->model.a = node->model.b = 0;
            return node;
        }

        /**
         * @brief Bulk load data to build the index.
         * @param kv Array of key-value pairs
         * @param num_keys Number of data points
         */
        void bulkLoad(const payload *kv, const int &num_keys)
        {
            if (num_keys == 0 || num_keys == 1)
            {
                return;
            }

            // check array is mono
            for (int i = 1; i < num_keys; i++)
            {
                RT_ASSERT(kv[i].first > kv[i - 1].first);
            }

            KeyType *keys = new KeyType[num_keys];
            ValueType *values = new ValueType[num_keys];
            for (int i = 0; i < num_keys; i++)
            {
                keys[i] = kv[i].first;
                values[i] = kv[i].second;
            }

            int used_index = 0;
            int remain_nums = num_keys;
            // make segment partition
            while (remain_nums > 0)
            {
                Segment segment;
                segmentPartition(keys + used_index, remain_nums, segment, num_keys / 1000);
                nodes.push_back(bulkLoadNode(keys + used_index, values, segment));
                nodes_num++;
                used_index += segment.numItems;
                remain_nums -= segment.numItems;
                node_keys.push_back(segment.firstKey);
                node_keys_num++;
            }
            if (USE_FAST_POINTER)
                buildFastPointer();

            delete[] keys;
            delete[] values;
        }

        // bulk loading a node, each node corresponds to a segment
        Node *bulkLoadNode(KeyType *keys, ValueType *values, const Segment &segment)
        {
            Node *node;
            // first find in node pool
            if (nodePool.empty())
            {
                node = new_nodes(1);
            }
            else
            {
                node = nodePool.top();
                nodePool.pop();
            }

            const int size = segment.numItems;

            node->numInserts = node->numInsertToData = 0;

            // init a and b
            node->model.a = ((segment.upperSlope + segment.lowerSlope) / 2) * (1.0 + ARR_GAPS);
            //            node->model.a = segment.upperSlope * (1.0 + ARR_GAPS);
            node->model.b = 0.0 - node->model.a * segment.firstKey;
            RT_ASSERT(isfinite(node->model.a));
            RT_ASSERT(isfinite(node->model.b));

            node->numItems = size * (1.0 + ARR_GAPS);
            node->items = new_items(node->numItems);
            const int bitmap_size = BITMAP_SIZE(node->numItems);
            node->noneBitmap = new_bitmap(bitmap_size);
            memset(node->noneBitmap, 0xff, sizeof(bitmap_t) * bitmap_size);

            node->fastPointerIndex = 0;

            for (int index = 0; index < size; index++)
            {
                int predict_pos = expected_position(node, keys[index]);
                // slot is empty
                if (BITMAP_GET(node->noneBitmap, predict_pos))
                {
                    BITMAP_CLEAR(node->noneBitmap, predict_pos);
                    node->items[predict_pos].components.data.key = keys[index];
                    node->items[predict_pos].components.data.value = values[index];
                    node->numInsertToData++;
                }
                else
                {
                    //                     buffer[keys[index]] = values[index];
                    buffer->put(keys[index], values[index]);
                    if (USE_STATISTIC)
                    {
                        buffer_num++;
                    }
                }
                node->numInserts++;
            }
            return node;
        }

        void buildFastPointer()
        {
            int ret;
            for (int i = 0; i < nodes_num - 1; i++)
            {
                buffer->build_fast_pointer(node_keys[i], node_keys[i + 1], ret);
                nodes[i]->fastPointerIndex = ret;
                //                flag < ret.second ? (flag = ret.second) : 0 ;
            }
            nodes[nodes_num - 1]->fastPointerIndex = 0;
        }

        /**
         * @brief Calculate the memory consumption of the index structure.
         * @return Memory consumption size.
         */
        long long memoryConsumption() const
        {
            long long size = 0;

            for (int i = 0; i < nodes.size(); i++)
            {
                //                size += sizeof(Node);
                //                size += sizeof(*(nodes[i]->noneBitmap));
                size += sizeof(nodes[i]->noneBitmap);
                //                size += sizeof(KeyType);
                //                for(int i = 0 ; i < nodes[i]->numItems ; i++){
                //                    size += sizeof(Item);
                //                }
                size += sizeof(Item) * nodes[i]->numItems;
            }
            size += buffer->memory_consumption();
            return size;
        }

        /**
         * @brief Print information about fast pointers.
         */
        void printFastPointer()
        {
            std::vector<uint64_t> res;
            buffer->get_fast_pointer(res);
            long double save_path_num = 0.0;
            for (auto node : nodes)
            {
                save_path_num += res[node->fastPointerIndex] * (node->numInserts - node->numInsertToData);
            }
            std::cout << "average reduced path length:" << save_path_num / buffer_num << std::endl;
            std::cout << "fast buffer size:" << res.size() << std::endl;
        }

    public:
        std::vector<Node *> nodes;      // store model nodes
        std::vector<KeyType> node_keys; // through node_keys to locate model index
        //        std::map<KeyType, ValueType> buffer;  //conflict data will be put into buffer
        artInterface<KeyType, ValueType> *buffer;

        long long nodes_num;
        long long node_keys_num;
        long long buffer_num;
    };

}

#endif // ALT_INDEX_ALT_INDEX_H
