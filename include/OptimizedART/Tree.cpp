//
// Created by florian on 18.11.15.
//
#include <assert.h>
#include <algorithm>
#include <functional>
#include "Tree.h"
#include "N.cpp"
#include "Key.h"
#include "Epoche.h"


namespace ART_OLC {

    Tree::Tree(LoadKeyFunction loadKey) : root(new N256( nullptr, 0, 0)), loadKey(loadKey) {

    }

    Tree::~Tree() {
        N::deleteChildren(root);
        N::deleteNode(root);
    }

    ThreadInfo Tree::getThreadInfo() {
        return ThreadInfo(this->epoche);
    }

    TID Tree::lookup(const Key &k, ThreadInfo &threadEpocheInfo) const {
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);
        restart:
        bool needRestart = false;

        N *node;
        N *parentNode = nullptr;
        uint64_t v;
        uint32_t level = root->getMatchLevel();
        bool optimisticPrefixMatch = false;

        node = root;
        v = node->readLockOrRestart(needRestart);
        if (needRestart) goto restart;
        while (true) {
            switch (checkPrefix(node, k, level)) { // increases level, verify the prefix
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return 0;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match:
                    if (k.getKeyLen() <= level) {
                        return 0;
                    }
                    parentNode = node;
                    node = N::getChild(k[level], parentNode);
                    parentNode->checkOrRestart(v,needRestart);
                    if (needRestart) goto restart;

                    if (node == nullptr) {
                        return 0;
                    }
                    if (N::isLeaf(node)) {
                        parentNode->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;

                        TID tid = N::getLeaf(node);
                        if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
                            return checkKey(tid, k);
                        }
                        return tid;
                    }
                    level++;
            }
            uint64_t nv = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            parentNode->readUnlockOrRestart(v, needRestart);
            if (needRestart) goto restart;
            v = nv;
        }
    }

    TID Tree::fast_lookup(const Key &k, int& fastPointerIndex, ThreadInfo &threadEpocheInfo) const{
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);
        restart:
        bool needRestart = false;

        N *parentNode = nullptr;
        FastPointerBuffer::FastPointerItem pointer = fastPointerBuffer.getFastPointer(fastPointerIndex);
        N *cur_node = pointer.fast_pointer;


        uint64_t v;
        uint32_t level = cur_node->getMatchLevel();
        bool optimisticPrefixMatch = false;

        v = cur_node->readLockOrRestart(needRestart);
        if (needRestart) goto restart;

        while (true) {
            switch (checkPrefix(cur_node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    cur_node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return 0;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match:
                    if (k.getKeyLen() <= level) {
                        return 0;
                    }
                    parentNode = cur_node;
                    cur_node = N::getChild(k[level], parentNode);
                    parentNode->checkOrRestart(v,needRestart);
                    if (needRestart) goto restart;

                    if (cur_node == nullptr) {
                        return 0;
                    }
                    if (N::isLeaf(cur_node)) {
                        parentNode->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;

                        TID tid = N::getLeaf(cur_node);
                        if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
                            return checkKey(tid, k);
                        }
                        return tid;
                    }
                    level++;
            }
            uint64_t nv = cur_node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            parentNode->readUnlockOrRestart(v, needRestart);
            if (needRestart) goto restart;
            v = nv;
        }
    }

    bool Tree::lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[],
                           std::size_t resultSize, std::size_t &resultsFound, ThreadInfo &threadEpocheInfo) const {
        for (uint32_t i = 0; i < std::min(start.getKeyLen(), end.getKeyLen()); ++i) {
            if (start[i] > end[i]) {
                resultsFound = 0;
                return false;
            } else if (start[i] < end[i]) {
                break;
            }
        }
        EpocheGuard epocheGuard(threadEpocheInfo);
        TID toContinue = 0;
        std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue, &copy](const N *node) {
            if (N::isLeaf(node)) {
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
                    return;
                }
                result[resultsFound] = N::getLeaf(node);
                resultsFound++;
            } else {
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const N *n = std::get<1>(children[i]);
                    copy(n);
                    if (toContinue != 0) {
                        break;
                    }
                }
            }
        };
        std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findStart = [&copy, &start, &findStart, &toContinue, this](
                N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
            if (N::isLeaf(node)) {
                copy(node);
                return;
            }
            uint64_t v;
            PCCompareResults prefixResult;

            {
                readAgain:
                bool needRestart = false;
                v = node->readLockOrRestart(needRestart);
                if (needRestart) goto readAgain;

                prefixResult = checkPrefixCompare(node, start, 0, level, loadKey, needRestart);
                if (needRestart) goto readAgain;

                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) {
                    readParentAgain:
                    vp = parentNode->readLockOrRestart(needRestart);
                    if (needRestart) goto readParentAgain;

                    node = N::getChild(nodeK, parentNode);

                    parentNode->readUnlockOrRestart(vp, needRestart);
                    if (needRestart) goto readParentAgain;

                    if (node == nullptr) {
                        return;
                    }
                    if (N::isLeaf(node)) {
                        copy(node);
                        return;
                    }
                    goto readAgain;
                }
                node->readUnlockOrRestart(v, needRestart);
                if (needRestart) goto readAgain;
            }

            switch (prefixResult) {
                case PCCompareResults::Bigger:
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren(node, startLevel, 255, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        N *n = std::get<1>(children[i]);
                        if (k == startLevel) {
                            findStart(n, k, level + 1, node, v);
                        } else if (k > startLevel) {
                            copy(n);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Smaller:
                    break;
            }
        };
        std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findEnd = [&copy, &end, &toContinue, &findEnd, this](
                N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
            if (N::isLeaf(node)) {
                return;
            }
            uint64_t v;
            PCCompareResults prefixResult;
            {
                readAgain:
                bool needRestart = false;
                v = node->readLockOrRestart(needRestart);
                if (needRestart) goto readAgain;

                prefixResult = checkPrefixCompare(node, end, 255, level, loadKey, needRestart);
                if (needRestart) goto readAgain;

                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) {
                    readParentAgain:
                    vp = parentNode->readLockOrRestart(needRestart);
                    if (needRestart) goto readParentAgain;

                    node = N::getChild(nodeK, parentNode);

                    parentNode->readUnlockOrRestart(vp, needRestart);
                    if (needRestart) goto readParentAgain;

                    if (node == nullptr) {
                        return;
                    }
                    if (N::isLeaf(node)) {
                        return;
                    }
                    goto readAgain;
                }
                node->readUnlockOrRestart(v, needRestart);
                if (needRestart) goto readAgain;
            }
            switch (prefixResult) {
                case PCCompareResults::Smaller:
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren(node, 0, endLevel, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        N *n = std::get<1>(children[i]);
                        if (k == endLevel) {
                            findEnd(n, k, level + 1, node, v);
                        } else if (k < endLevel) {
                            copy(n);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Bigger:
                    break;
            }
        };

        restart:
        bool needRestart = false;

        resultsFound = 0;

        uint32_t level = 0;
        N *node = nullptr;
        N *nextNode = root;
        N *parentNode;
        uint64_t v = 0;
        uint64_t vp;

        while (true) {
            parentNode = node;
            vp = v;
            node = nextNode;
            PCEqualsResults prefixResult;   //check prefix result

            v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;
            prefixResult = checkPrefixEquals(node, level, start, end, loadKey, needRestart);
            if (needRestart) goto restart;
            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) goto restart;
            }
            node->readUnlockOrRestart(v, needRestart);
            if (needRestart) goto restart;
            switch (prefixResult) {
                case PCEqualsResults::NoMatch: {
                    return false;
                }
                case PCEqualsResults::Contained: {
                    copy(node);
                    break;
                }
                case PCEqualsResults::BothMatch: {
                    uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                    uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
                    if (startLevel != endLevel) {
                        std::tuple<uint8_t, N *> children[256];
                        uint32_t childrenCount = 0;
                        v = N::getChildren(node, startLevel, endLevel, children, childrenCount);
                        for (uint32_t i = 0; i < childrenCount; ++i) {
                            const uint8_t k = std::get<0>(children[i]);
                            N *n = std::get<1>(children[i]);
                            if (k == startLevel) {
                                findStart(n, k, level + 1, node, v);
                            } else if (k > startLevel && k < endLevel) {
                                copy(n);
                            } else if (k == endLevel) {
                                findEnd(n, k, level + 1, node, v);
                            }
                            if (toContinue) {
                                break;
                            }
                        }
                    } else {
                        nextNode = N::getChild(startLevel, node);
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        level++;
                        continue;
                    }
                    break;
                }
            }
            break;
        }
        if (toContinue != 0) {
            loadKey(toContinue, continueKey);
            return true;
        } else {
            return false;
        }
    }

    bool Tree::fast_lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[],
                           std::size_t resultSize, std::size_t &resultsFound, ThreadInfo &threadEpocheInfo) const {
        for (uint32_t i = 0; i < std::min(start.getKeyLen(), end.getKeyLen()); ++i) {
            if (start[i] > end[i]) {
                resultsFound = 0;
                return false;
            } else if (start[i] < end[i]) {
                break;
            }
        }
        EpocheGuard epocheGuard(threadEpocheInfo);
        TID toContinue = 0;
        std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound, &toContinue, &copy](const N *node) {
            if (N::isLeaf(node)) {
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
                    return;
                }
                result[resultsFound] = N::getLeaf(node);
                resultsFound++;
            } else {
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const N *n = std::get<1>(children[i]);
                    copy(n);
                    if (toContinue != 0) {
                        break;
                    }
                }
            }
        };
        std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findStart = [&copy, &start, &findStart, &toContinue, this](
                N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
            if (N::isLeaf(node)) {
                copy(node);
                return;
            }
            uint64_t v;
            PCCompareResults prefixResult;

            {
                readAgain:
                bool needRestart = false;
                v = node->readLockOrRestart(needRestart);
                if (needRestart) goto readAgain;

                prefixResult = checkPrefixCompare(node, start, 0, level, loadKey, needRestart);
                if (needRestart) goto readAgain;

                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) {
                    readParentAgain:
                    vp = parentNode->readLockOrRestart(needRestart);
                    if (needRestart) goto readParentAgain;

                    node = N::getChild(nodeK, parentNode);

                    parentNode->readUnlockOrRestart(vp, needRestart);
                    if (needRestart) goto readParentAgain;

                    if (node == nullptr) {
                        return;
                    }
                    if (N::isLeaf(node)) {
                        copy(node);
                        return;
                    }
                    goto readAgain;
                }
                node->readUnlockOrRestart(v, needRestart);
                if (needRestart) goto readAgain;
            }

            switch (prefixResult) {
                case PCCompareResults::Bigger:
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren(node, startLevel, 255, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        N *n = std::get<1>(children[i]);
                        if (k == startLevel) {
                            findStart(n, k, level + 1, node, v);
                        } else if (k > startLevel) {
                            copy(n);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Smaller:
                    break;
            }
        };
        std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findEnd = [&copy, &end, &toContinue, &findEnd, this](
                N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
            if (N::isLeaf(node)) {
                return;
            }
            uint64_t v;
            PCCompareResults prefixResult;
            {
                readAgain:
                bool needRestart = false;
                v = node->readLockOrRestart(needRestart);
                if (needRestart) goto readAgain;

                prefixResult = checkPrefixCompare(node, end, 255, level, loadKey, needRestart);
                if (needRestart) goto readAgain;

                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) {
                    readParentAgain:
                    vp = parentNode->readLockOrRestart(needRestart);
                    if (needRestart) goto readParentAgain;

                    node = N::getChild(nodeK, parentNode);

                    parentNode->readUnlockOrRestart(vp, needRestart);
                    if (needRestart) goto readParentAgain;

                    if (node == nullptr) {
                        return;
                    }
                    if (N::isLeaf(node)) {
                        return;
                    }
                    goto readAgain;
                }
                node->readUnlockOrRestart(v, needRestart);
                if (needRestart) goto readAgain;
            }
            switch (prefixResult) {
                case PCCompareResults::Smaller:
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren(node, 0, endLevel, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        N *n = std::get<1>(children[i]);
                        if (k == endLevel) {
                            findEnd(n, k, level + 1, node, v);
                        } else if (k < endLevel) {
                            copy(n);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Bigger:
                    break;
            }
        };

        restart:
        bool needRestart = false;

        resultsFound = 0;


        int ret = 0;
        N *node = build_fast_pointer_range(start,end,ret);
        N *nextNode = root;
        N *parentNode;
        uint64_t v = 0;
        uint64_t vp;
        uint32_t level = node->getMatchLevel();

        uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
        uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
        std::tuple<uint8_t, N *> children[256];
        uint32_t childrenCount = 0;
        v = N::getChildren(node, startLevel, endLevel, children, childrenCount);
        for (uint32_t i = 0; i < childrenCount; ++i) {
            const uint8_t k = std::get<0>(children[i]);
            N *n = std::get<1>(children[i]);
            if (k == startLevel) {
                findStart(n, k, level + 1, node, v);
            } else if (k > startLevel && k < endLevel) {
                copy(n);
            } else if (k == endLevel) {
                findEnd(n, k, level + 1, node, v);
            }
            if (toContinue) {
                break;
            }
        }

        if (toContinue != 0) {
            loadKey(toContinue, continueKey);
            return true;
        } else {
            return false;
        }
    }

    TID Tree::checkKey(const TID tid, const Key &k) const {
        Key kt;
        this->loadKey(tid, kt);
        if (k == kt) {
            return tid;
        }
        return 0;
    }

    void Tree::insert(const Key &k, TID tid, ThreadInfo &epocheInfo) {
        EpocheGuard EpocheGuard(epocheInfo);
        restart:
        bool needRestart = false;

        N *node = nullptr;  //current node
        N *nextNode = root; //next node
        N *parentNode = nullptr;    //parent node
        uint8_t parentKey, nodeKey = 0;     //parent and node key
        uint64_t parentVersion = 0;     //optimistic lock
        uint32_t level = 0;     //index of key

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            auto res = checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                              this->loadKey, needRestart); // increases level get the prefix
            if (needRestart) goto restart;
            switch (res) {
                case CheckPrefixPessimisticResult::NoMatch: {   //a new node will be inserted
                    parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                    if (needRestart) goto restart;

                    node->upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart) {
                        parentNode->writeUnlock();
                        goto restart;
                    }
                    uint32_t node_prefix = node->getPrefixLength();
                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    auto newNode = new N4(node->getPrefix(), nextLevel - level, node->getMatchLevel());

                    // 2)  add node and (tid, *k) as children
                    newNode->insert(k[nextLevel], N::setLeaf(tid));
                    newNode->insert(nonMatchingKey, node);

                    // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                    N::change(parentNode, parentKey, newNode);
                    parentNode->writeUnlock();

                    // 4) update prefix of node, unlock
                    node->setPrefix(remainingPrefix,
                                    node->getPrefixLength() - ((nextLevel - level) + 1));

                    node->setMatchLevel(level + node_prefix - node->getPrefixLength());

                    node->writeUnlock();

                    return;
                }
                case CheckPrefixPessimisticResult::Match:
                    break;
            }
            level = nextLevel;
            nodeKey = k[level];
            nextNode = N::getChild(nodeKey, node);
            node->checkOrRestart(v,needRestart);
            if (needRestart) goto restart;

            if (nextNode == nullptr) {
                N* new_node = N::insertAndUnlock(node, v, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(tid), needRestart, epocheInfo);
                if (needRestart) goto restart;

                if(new_node != nullptr){
                    retry:
                    auto v = node->readLockOrRestart(needRestart);
                    if(needRestart) goto retry;

                    int pointer_index = fastPointerBuffer.getFastPointerIndex(node);

                    node->upgradeToWriteLockOrRestart(v, needRestart);
                    if(needRestart) goto retry;

                    fastPointerBuffer.updateFastPointerWithIndex(pointer_index, new_node);
                    node->writeUnlock();

                    epocheInfo.getEpoche().markNodeForDeletion(node, epocheInfo);
                }

                return;
            }

            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(parentVersion, needRestart);
                if (needRestart) goto restart;
            }

            if (N::isLeaf(nextNode)) {
                node->upgradeToWriteLockOrRestart(v, needRestart);
                if (needRestart) goto restart;

                Key key;
                loadKey(N::getLeaf(nextNode), key);

                level++;
                uint32_t prefixLength = 0;
                while (key[level + prefixLength] == k[level + prefixLength]) {
                    prefixLength++;
                }

                auto n4 = new N4(&k[level], prefixLength, level);
                assert(n4->getMatchLevel() != 0);
                n4->insert(k[level + prefixLength], N::setLeaf(tid));
                n4->insert(key[level + prefixLength], nextNode);
                N::change(node, k[level - 1], n4);
                node->writeUnlock();

                return;
            }
            level++;
            parentVersion = v;
        }

    }

    void Tree::fast_insert(const Key &k, TID tid, int& fastPointerIndex, ThreadInfo &epocheInfo) {
        EpocheGuard EpocheGuard(epocheInfo);
        restart:
        bool needRestart = false;

        FastPointerBuffer::FastPointerItem pointer = fastPointerBuffer.getFastPointer(fastPointerIndex);
        N *node = nullptr;  //current node
        N *nextNode = root; //next node
        N *parentNode = nullptr;    //parent node
        uint8_t parentKey, nodeKey = 0;     //parent and node key
        uint64_t parentVersion = 0;     //optimistic lock
        uint32_t level = 0;     //index of key

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            auto res = checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                              this->loadKey, needRestart); // increases level get the prefix
            if (needRestart) goto restart;
            switch (res) {
                case CheckPrefixPessimisticResult::NoMatch: {   //a new node will be inserted

                    parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                    if (needRestart) goto restart;

                    node->upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart) {
                        parentNode->writeUnlock();
                        goto restart;
                    }
                    uint32_t node_prefix = node->getPrefixLength();
                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    auto newNode = new N4(node->getPrefix(), nextLevel - level, node->getMatchLevel());

                    // 2)  add node and (tid, *k) as children
                    newNode->insert(k[nextLevel], N::setLeaf(tid));
                    newNode->insert(nonMatchingKey, node);

                    // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                    N::change(parentNode, parentKey, newNode);
                    parentNode->writeUnlock();

                    // 4) update prefix of node, unlock
                    node->setPrefix(remainingPrefix,
                                    node->getPrefixLength() - ((nextLevel - level) + 1));

                    if(node == pointer.fast_pointer) {
                        fastPointerIndex = fastPointerBuffer.insertFastPointer(newNode);
                    }

                    node->setMatchLevel(level + node_prefix - node->getPrefixLength());

                    node->writeUnlock();

                    return;
                }
                case CheckPrefixPessimisticResult::Match:
                    break;
            }
            level = nextLevel;
            nodeKey = k[level];
            nextNode = N::getChild(nodeKey, node);
            node->checkOrRestart(v,needRestart);
            if (needRestart) goto restart;

            if (nextNode == nullptr) {
                N* new_node = N::insertAndUnlock(node, v, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(tid), needRestart, epocheInfo);

                if (needRestart) goto restart;

                if(new_node != nullptr){
                    retry:
                    auto v = node->readLockOrRestart(needRestart);
                    if(needRestart) goto retry;

                    int pointer_index = fastPointerBuffer.getFastPointerIndex(node);

                    node->upgradeToWriteLockOrRestart(v, needRestart);
                    if(needRestart) goto retry;

                    fastPointerBuffer.updateFastPointerWithIndex(pointer_index, new_node);
                    node->writeUnlock();

                    epocheInfo.getEpoche().markNodeForDeletion(node, epocheInfo);
                }

                return;
            }

            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(parentVersion, needRestart);
                if (needRestart) goto restart;
            }

            if (N::isLeaf(nextNode)) {
                node->upgradeToWriteLockOrRestart(v, needRestart);
                if (needRestart) goto restart;

                Key key;
                loadKey(N::getLeaf(nextNode), key);

                level++;
                uint32_t prefixLength = 0;
                while (key[level + prefixLength] == k[level + prefixLength]) {
                    prefixLength++;
                }

                auto n4 = new N4(&k[level], prefixLength, level);
                n4->insert(k[level + prefixLength], N::setLeaf(tid));
                n4->insert(key[level + prefixLength], nextNode);
                N::change(node, k[level - 1], n4);
                node->writeUnlock();

                return;
            }
            level++;
            parentVersion = v;
        }

    }

    bool Tree::update(const Key &k, TID tid, ThreadInfo &threadEpocheInfo) {
        EpocheGuard epocheGuard(threadEpocheInfo);
        restart:
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;
        bool optimisticPrefixMatch = false;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return false;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                    optimisticPrefixMatch = true;
                case CheckPrefixResult::Match: {
                    nodeKey = k[level];
                    nextNode = N::getChild(nodeKey, node);

                    node->checkOrRestart(v, needRestart);
                    if (needRestart) goto restart;

                    if (nextNode == nullptr) {
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        return false;
                    }
                    if (N::isLeaf(nextNode)) {
                        if (k.getKeyLen() <= level) {
                            return false;
                        }

                        if(parentNode != nullptr) {
                            parentNode->readUnlockOrRestart(parentVersion, needRestart);
                            if (needRestart) goto restart;
                        }

                        node->upgradeToWriteLockOrRestart(v, needRestart);
                        if (needRestart) goto restart;

                        TID old_tid = N::getLeaf(nextNode);
                        if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
                            if (checkKey(old_tid, k) == old_tid) {
                                N::change(node, k[level], N::setLeaf(tid));
                                node->writeUnlock();
                                return true;
                            }
                        }
                        N::change(node, k[level], N::setLeaf(tid));
                        node->writeUnlock();
                        return true;
                    }
                    level++;
                    parentVersion = v;
                }
            }
        }
    }

    void Tree::remove(const Key &k, ThreadInfo &threadInfo) {
        EpocheGuard epocheGuard(threadInfo);
        restart:
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    nodeKey = k[level];
                    nextNode = N::getChild(nodeKey, node);

                    node->checkOrRestart(v, needRestart);
                    if (needRestart) goto restart;

                    if (nextNode == nullptr) {
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        return;
                    }
                    if (N::isLeaf(nextNode)) {
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && parentNode != nullptr) {
                            parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                            if (needRestart) goto restart;

                            node->upgradeToWriteLockOrRestart(v, needRestart);
                            if (needRestart) {
                                parentNode->writeUnlock();
                                goto restart;
                            }
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {
                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            } else {
                                secondNodeN->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    parentNode->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);
                                parentNode->writeUnlock();

                                secondNodeN->addPrefixBefore(node, secondNodeK);
                                secondNodeN->writeUnlock();

                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            }
                        } else {
                            N::removeAndUnlock(node, v, k[level], parentNode, parentVersion, parentKey, needRestart, threadInfo);
                            if (needRestart) goto restart;
                        }
                        return;
                    }
                    level++;
                    parentVersion = v;
                }
            }
        }
    }

    void Tree::fast_remove(const Key &k, int& fastPointerIndex, ART::ThreadInfo &epocheInfo) {
        EpocheGuard epocheGuard(epocheInfo);
        restart:
        bool needRestart = false;

        FastPointerBuffer::FastPointerItem pointer = fastPointerBuffer.getFastPointer(fastPointerIndex);
        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    return;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    nodeKey = k[level];
                    nextNode = N::getChild(nodeKey, node);

                    node->checkOrRestart(v, needRestart);
                    if (needRestart) goto restart;

                    if (nextNode == nullptr) {
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        return;
                    }
                    if (N::isLeaf(nextNode)) {
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && parentNode != nullptr) {
                            parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                            if (needRestart) goto restart;

                            node->upgradeToWriteLockOrRestart(v, needRestart);
                            if (needRestart) {
                                parentNode->writeUnlock();
                                goto restart;
                            }
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {
                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);

                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, epocheInfo);
                            } else {
                                secondNodeN->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    parentNode->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);
                                parentNode->writeUnlock();

                                secondNodeN->addPrefixBefore(node, secondNodeK);
                                secondNodeN->writeUnlock();

                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, epocheInfo);
                            }
                        } else {
                            N* new_node = N::removeAndUnlock(node, v, k[level], parentNode, parentVersion, parentKey, needRestart, epocheInfo);

                            if (needRestart) goto restart;

                            if(new_node != nullptr){
                                retry:
                                auto v = node->readLockOrRestart(needRestart);
                                if(needRestart) goto retry;

                                int pointer_index = fastPointerBuffer.getFastPointerIndex(node);

                                node->upgradeToWriteLockOrRestart(v, needRestart);
                                if(needRestart) goto retry;

                                fastPointerBuffer.updateFastPointerWithIndex(pointer_index, new_node);
                                node->writeUnlock();

                                epocheInfo.getEpoche().markNodeForDeletion(node, epocheInfo);
                            }

                            return;
                        }
                        return;
                    }
                    level++;
                    parentVersion = v;
                }
            }
        }
    }

    void Tree::build_fast_pointer(const Key &k1, const Key &k2, int &ret, ART::ThreadInfo &threadEpocheInfo){
        EpocheGuardReadonly epocheGuard(threadEpocheInfo);

        restart:
        bool needRestart = false;

        N *node1;
        N *node2;
        N *parentNode = root;
        //version for olc
        uint64_t v1;
        uint32_t level1 = 0;
        uint32_t level2 = 0;

        uint32_t match_level = 0;


        node1 = root;
        node2 = root;

        v1 = node1->readLockOrRestart(needRestart);

        CheckPrefixResult checkPrefix1;
        CheckPrefixResult checkPrefix2;

        if (needRestart) goto restart;
        while (true) {

            match_level = level1;

            checkPrefix1 = checkPrefix(node1, k1, level1);
            checkPrefix2 = checkPrefix(node2, k2, level2);

            //have the same return value
            if(checkPrefix1 == checkPrefix2){
                if(checkPrefix1 == CheckPrefixResult::Match){
                    if(k1.getKeyLen() <= level1 || k2.getKeyLen() <= level2){
//                        ret_node = node1;
//                        ret.second = match_level;
                        ret = fastPointerBuffer.insertFastPointer(node1);
                        assert(node1->getMatchLevel() == match_level);
                        return ;
                    }
                    parentNode = node1;

                    node1 = N::getChild(k1[level1], parentNode);
                    node2 = N::getChild(k2[level2], parentNode);
                    parentNode->checkOrRestart(v1, needRestart);

                    if(needRestart) goto restart;
                    if(node1 == nullptr || node2 == nullptr){
//                        ret_node = parentNode;
//                        ret.second = match_level;
                        ret = fastPointerBuffer.insertFastPointer(parentNode);
                        assert(parentNode->getMatchLevel() == match_level);
                        return ;
                    }

                    //return parent node
                    if(node1 != node2){
//                        ret_node = parentNode;
//                        ret.second = match_level;
                        ret = fastPointerBuffer.insertFastPointer(parentNode);
                        assert(parentNode->getMatchLevel() == match_level);
                        return ;
                    }

                    if (N::isLeaf(node1) || N::isLeaf(node2)) {
                        parentNode->readUnlockOrRestart(v1, needRestart);
                        if (needRestart) goto restart;

//                        ret_node = parentNode;
//                        ret.second = match_level;
                        ret = fastPointerBuffer.insertFastPointer(parentNode);
                        assert(parentNode->getMatchLevel() == match_level);
                        return ;
                    }

                    level1++;
                    level2++;
                }
                else{
                    break;
                }
            }
            else{
                break;
            }

            uint64_t nv1 = node1->readLockOrRestart(needRestart);
            if(needRestart) goto restart;

            parentNode->readUnlockOrRestart(v1, needRestart);
            if(needRestart) goto restart;
            v1 = nv1;

        }
//        ret_node = node1;
//        ret.second = match_level;
        ret = fastPointerBuffer.insertFastPointer(node1);
        assert(node1->getMatchLevel() == match_level);
        return ;

    }

    N *Tree::build_fast_pointer_range(const Key &k1, const Key &k2, int &ret) const {

        restart:
        bool needRestart = false;

        N *node1;
        N *node2;
        N *parentNode = root;
        //version for olc
        uint64_t v1;
        uint32_t level1 = 0;
        uint32_t level2 = 0;

        uint32_t match_level = 0;


        node1 = root;
        node2 = root;

        v1 = node1->readLockOrRestart(needRestart);

        CheckPrefixResult checkPrefix1;
        CheckPrefixResult checkPrefix2;

        if (needRestart) goto restart;
        while (true) {

            match_level = level1;

            checkPrefix1 = checkPrefix(node1, k1, level1);
            checkPrefix2 = checkPrefix(node2, k2, level2);

            //have the same return value
            if(checkPrefix1 == checkPrefix2){
                if(checkPrefix1 == CheckPrefixResult::Match){
                    if(k1.getKeyLen() <= level1 || k2.getKeyLen() <= level2){
                        assert(node1->getMatchLevel() == match_level);
                        return node1;
                    }
                    parentNode = node1;

                    node1 = N::getChild(k1[level1], parentNode);
                    node2 = N::getChild(k2[level2], parentNode);
                    parentNode->checkOrRestart(v1, needRestart);

                    if(needRestart) goto restart;
                    if(node1 == nullptr || node2 == nullptr){
                        assert(parentNode->getMatchLevel() == match_level);
                        return parentNode;
                    }

                    //return parent node
                    if(node1 != node2){
                        assert(parentNode->getMatchLevel() == match_level);
                        return parentNode;
                    }

                    if (N::isLeaf(node1) || N::isLeaf(node2)) {
                        parentNode->readUnlockOrRestart(v1, needRestart);
                        if (needRestart) goto restart;
                        assert(parentNode->getMatchLevel() == match_level);
                        return parentNode;
                    }

                    level1++;
                    level2++;
                }
                else{
                    break;
                }
            }
            else{
                break;
            }

            uint64_t nv1 = node1->readLockOrRestart(needRestart);
            if(needRestart) goto restart;

            parentNode->readUnlockOrRestart(v1, needRestart);
            if(needRestart) goto restart;
            v1 = nv1;

        }
        assert(node1->getMatchLevel() == match_level);
        return node1;

    }

    inline typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
        if (n->hasPrefix()) {
            if (k.getKeyLen() <= level + n->getPrefixLength()) {
                return CheckPrefixResult::NoMatch;
            }
            for (uint32_t i = 0; i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
                if (n->getPrefix()[i] != k[level]) {
                    return CheckPrefixResult::NoMatch;
                }
                ++level;
            }
            if (n->getPrefixLength() > maxStoredPrefixLength) {
                level = level + (n->getPrefixLength() - maxStoredPrefixLength);
                return CheckPrefixResult::OptimisticMatch;
            }
        }
        return CheckPrefixResult::Match;
    }

    typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                             uint8_t &nonMatchingKey,
                                                                             Prefix &nonMatchingPrefix,
                                                                             LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            uint32_t prevLevel = level;
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {       //overflow goto next layer
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return CheckPrefixPessimisticResult::Match;
                    loadKey(anyTID, kt);
                }
                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey != k[level]) {
                    nonMatchingKey = curKey;
                    if (n->getPrefixLength() > maxStoredPrefixLength) {
                        if (i < maxStoredPrefixLength) {
                            auto anyTID = N::getAnyChildTid(n, needRestart);
                            if (needRestart) return CheckPrefixPessimisticResult::Match;
                            loadKey(anyTID, kt);
                        }
                        memcpy(nonMatchingPrefix, &kt[0] + level + 1, std::min((n->getPrefixLength() - (level - prevLevel) - 1),
                                                                               maxStoredPrefixLength));
                    } else {
                        memcpy(nonMatchingPrefix, n->getPrefix() + i + 1, n->getPrefixLength() - i - 1);
                    }
                    return CheckPrefixPessimisticResult::NoMatch;
                }
                ++level;
            }
        }
        return CheckPrefixPessimisticResult::Match;
    }

    typename Tree::PCCompareResults Tree::checkPrefixCompare(const N *n, const Key &k, uint8_t fillKey, uint32_t &level,
                                                             LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return PCCompareResults::Equal;
                    loadKey(anyTID, kt);
                }
                uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : fillKey;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey < kLevel) {
                    return PCCompareResults::Smaller;
                } else if (curKey > kLevel) {
                    return PCCompareResults::Bigger;
                }
                ++level;
            }
        }
        return PCCompareResults::Equal;
    }

    typename Tree::PCEqualsResults Tree::checkPrefixEquals(const N *n, uint32_t &level, const Key &start, const Key &end,
                                                           LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return PCEqualsResults::BothMatch;
                    loadKey(anyTID, kt);
                }
                uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey > startLevel && curKey < endLevel) {
                    return PCEqualsResults::Contained;
                } else if (curKey < startLevel || curKey > endLevel) {
                    return PCEqualsResults::NoMatch;
                }
                ++level;
            }
        }
        return PCEqualsResults::BothMatch;
    }

    long Tree::size() {
        auto size = N::size(root);
        return size + sizeof(root);
    }

    void Tree::makeFastRoot(){
        fastPointerBuffer.insertFastPointer(root);
    }
}
