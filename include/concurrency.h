//
// Created by yyx on 2023/3/30.
//

#ifndef ALT_INDEX_CONCURRENCY_H
#define ALT_INDEX_CONCURRENCY_H

#pragma once

#include <atomic>
#include <array>
#include <immintrin.h>
#include <sched.h>
// #include <tbb/queuing_rw_mutex.h>
// #include "tbb/spin_rw_mutex.h"
 #include "tbb/spin_mutex.h"
// #include "tbb/mutex.h"
// #include "tbb/reader_writer_lock.h"
// #include "tbb/cache_aligned_allocator.h"
// #include "tbb/enumerable_thread_specific.h"

namespace alt_index
{

    template <typename T>
    void atomic_add(std::atomic<T> &target, T operand)
    {
        while (true)
        {
            auto expected = target.load();
            auto desired = expected + operand;
            if (target.compare_exchange_strong(expected, desired))
            {
                break;
            }
        }
    }

    class spin_lock
    {
    private:
        std::atomic_bool lock_;

    public:
        spin_lock()
        {
            lock_.store(false);
        }
        ~spin_lock()
        {
            lock_.store(false);
        }
        void lock()
        {
            while (lock_.exchange(true))
            {
            }
        }

        bool try_lock()
        {
            return !lock_.exchange(true);
        }

        void unlock()
        {
            while (!lock_.exchange(false))
            {
            }
        }

        bool test()
        {
            return lock_.load();
        }

        void wait()
        {
            while (lock_.load())
            {
            };
        }
    };

    void yield(int count)
    {
        if (count > 3)
            sched_yield();
        else
            _mm_pause();
    }

    struct OptLock
    {
        std::atomic<uint64_t> typeVersionLockObsolete{0b100};

        OptLock() = default;
        OptLock(const OptLock &other)
        {
            typeVersionLockObsolete = 0b100;
        }

        uint64_t get_version_number()
        {
            return typeVersionLockObsolete.load();
        }

        bool isLocked(uint64_t version)
        {
            return ((version & 0b10) == 0b10);
        }

        bool isLocked()
        {
            return ((typeVersionLockObsolete.load() & 0b10) == 0b10);
        }

        void writeLockOrRestart(bool &needRestart)
        {
            uint64_t version;
            version = readLockOrRestart(needRestart);
            if (needRestart)
                return;

            upgradeToWriteLockOrRestart(version, needRestart);
        }

        void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart)
        {
            if (typeVersionLockObsolete.compare_exchange_strong(version, version + 0b10))
            {
                version = version + 0b10;
            }
            else
            {
                _mm_pause();
                needRestart = true;
            }
        }

        void writeUnlock()
        {
            typeVersionLockObsolete.fetch_add(0b10);
        }

        void checkOrRestart(uint64_t startRead, bool &needRestart) const
        {
            readUnlockOrRestart(startRead, needRestart);
        }

        uint64_t readLockOrRestart(bool &needRestart)
        {
            uint64_t version;
            version = typeVersionLockObsolete.load();
            if (isLocked(version) || isObsolete(version))
            {
                _mm_pause();
                needRestart = true;
            }
            return version;
        }

        void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const
        {
            needRestart = (startRead != typeVersionLockObsolete.load());
        }

        void writeUnlockObsolete()
        {
            typeVersionLockObsolete.fetch_add(0b11);
        }

        void labelObsolete()
        {
            typeVersionLockObsolete.store((typeVersionLockObsolete.load() | 1));
        }

        bool isObsolete(uint64_t version)
        {
            return (version & 1) == 1;
        }

        bool isObsolete()
        {
            return (typeVersionLockObsolete.load() & 1) == 1;
        }
    };
}

#endif // ALT_INDEX_CONCURRENCY_H
