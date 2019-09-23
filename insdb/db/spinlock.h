/*
 * Original Code Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of the original source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 *
 * Modifications made 2019
 * Modifications Copyright (c) 2019, Samsung Electronics.
 *
 * Architect    : Heekwon Park(heekwon.p@samsung.com), Yangseok Ki(yangseok.ki@samsung.com)
 * Authors      : Heekwon Park, Ilgu Hong, Hobin Lee
 *
 * This modified version is distributed under a BSD-style license that can be
 * found in the LICENSE.insdb file  
 *                    
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  
 */


#ifndef STORAGE_INSDB_DB_SPINLOCK_H_
#define STORAGE_INSDB_DB_SPINLOCK_H_

#include <stdlib.h>
#include <pthread.h>
#include <mutex>
#include <boost/atomic.hpp>
#include "insdb/kv_trace.h"
#include "port/port.h"
/** @ Spinlock class */
/**
  Include boost spinlock to protect SkipList from multiple-writer.
  This spinlock does not allow recursive locking.
  Please, maek sure code not to try multiple locking.
  */
namespace insdb {

#if 0
    class Spinlock {
        private:
            typedef enum {Locked = false, Unlocked = true} LockState;
            std::atomic<bool> state_;
        public:
            Spinlock() : state_(Unlocked){}
            ~Spinlock(){ if(state_ == Locked) abort();}
            void Lock()
            {
                while (state_.exchange(Locked, std::memory_order_seq_cst) == Locked) {
                    /* busy-wait */
                }
            }
            int TryLock()
            {
                if(state_.exchange(Locked, std::memory_order_seq_cst) == Locked) 
                    return -1;
                return 0;
            }
            void Unlock()
            {
                state_.store(Unlocked, std::memory_order_seq_cst);
            }
    };
#else
    class Spinlock {
        private:
            pthread_spinlock_t lock_;
        public:
            Spinlock() { pthread_spin_init(&lock_, PTHREAD_PROCESS_SHARED); }
            ~Spinlock() { pthread_spin_destroy(&lock_); }
            void Lock(){ pthread_spin_lock(&lock_); }
            int TryLock(){ return pthread_spin_trylock(&lock_); }
            void Unlock(){ pthread_spin_unlock(&lock_); }
    };

#endif

}  // namespace insdb

#endif  // STORAGE_INSDB_DB_SPINLOCK_H_
