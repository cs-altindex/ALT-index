//
// Created by florian on 18.11.15.
//

#ifndef ART_OPTIMISTICLOCK_COUPLING_N_H
#define ART_OPTIMISTICLOCK_COUPLING_N_H
#include "N.h"
#include "FastPointerBuffer.h"


using namespace ART;

namespace ART_OLC {

    class Tree {
    public:
        using LoadKeyFunction = void (*)(TID tid, Key &key);

        N *const root;

    private:

        TID checkKey(const TID tid, const Key &k) const;

        LoadKeyFunction loadKey;

        Epoche epoche{128};

        FastPointerBuffer fastPointerBuffer;

    public:
        enum class CheckPrefixResult : uint8_t {
            Match,
            NoMatch,
            OptimisticMatch
        };

        enum class CheckPrefixPessimisticResult : uint8_t {
            Match,
            NoMatch,
        };

        enum class PCCompareResults : uint8_t {
            Smaller,
            Equal,
            Bigger,
        };
        enum class PCEqualsResults : uint8_t {
            BothMatch,
            Contained,
            NoMatch
        };
        static CheckPrefixResult checkPrefix(N* n, const Key &k, uint32_t &level);

        static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                   uint8_t &nonMatchingKey,
                                                                   Prefix &nonMatchingPrefix,
                                                                   LoadKeyFunction loadKey, bool &needRestart);

        static PCCompareResults checkPrefixCompare(const N* n, const Key &k, uint8_t fillKey, uint32_t &level, LoadKeyFunction loadKey, bool &needRestart);

        static PCEqualsResults checkPrefixEquals(const N* n, uint32_t &level, const Key &start, const Key &end, LoadKeyFunction loadKey, bool &needRestart);

    public:

        Tree(LoadKeyFunction loadKey);

        Tree(const Tree &) = delete;

        Tree(Tree &&t) : root(t.root), loadKey(t.loadKey) { }

        ~Tree();

        ThreadInfo getThreadInfo();

        TID lookup(const Key &k, ThreadInfo &threadEpocheInfo) const;

        TID fast_lookup(const Key &k, int& fastPointerIndex, ThreadInfo &threadEpocheInfo) const;

        bool lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[], std::size_t resultLen,
                         std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

        bool fast_lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[],
                                    std::size_t resultSize, std::size_t &resultsFound, ThreadInfo &threadEpocheInfo) const;

        void insert(const Key &k, TID tid, ThreadInfo &epocheInfo);

        void fast_insert(const Key &k, TID tid, int& fastPointerIndex, ThreadInfo &epocheInfo);

        bool update(const Key &k, TID tid, ThreadInfo &threadEpocheInfo);

        void remove(const Key &k, ThreadInfo &epocheInfo);

        void fast_remove(const Key &k, int& fastPointerIndex, ThreadInfo &epocheInfo);

        void build_fast_pointer(const Key &k1, const Key &k2, int& ret, ThreadInfo &threadEpocheInfo);     //get the last node which has two keys' prefix

        N * build_fast_pointer_range(const Key &k1, const Key &k2, int& ret) const;

        void clear_stat();

        long size();

        void makeFastRoot();

        void getFastPointer(std::vector<uint64_t>& res){
            return fastPointerBuffer.pointerSavedPath(res);
        }

        long long memoryFastPointer(){
            return fastPointerBuffer.size() * sizeof(N*);
        }
    };
}

#endif //ART_OPTIMISTICLOCK_COUPLING_N_H
