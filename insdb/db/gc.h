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

#ifndef STORAGE_INSDB_DB_GARBAGE_COLLECTOR_H_
#define STORAGE_INSDB_DB_GARBAGE_COLLECTOR_H_

#include "pthread.h"
namespace insdb {

#define MASK(n)             ((1ULL <<(n)) - 1)
#define MOD_SCALE(x, b)     ((x) & MASK(b))
    /**
      <Per thread structure>
      For lockfree skiplist, removed internodes will be kept in FreeList until
      circular list is full.

      circular list
      head
      tail
      scale // size of circular list
      scale_down_threshold // move out threshold.
      count // number of entry
      set // flag to indicate this is occupied by thread.
      */
    struct FreeList {
        uint32_t head;
        uint32_t tail;
        uint32_t count;
        void **x;
    };

    struct GC_FreeList {
        GC_FreeList *next;
        uint32_t set;
        uint32_t scale;
        uint32_t scale_down_threshold;
        FreeList *node_free_list;
        FreeList *ukey_free_list;
    };

    /**
      <Garbage Collector>
      Each thread has own context to keep and track removed internal node. It is essential feature
      in Lockfree SkipList implemntation.

      Constructor receives scale parameter, which is index of power of 2. eq, 10 means 1024 entries
      whill be keep and tracking in garbage collector.
      */
    class GCInfo {
        public:
            explicit GCInfo(uint32_t scale) : scale_(scale), gc_free_list_(NULL) { InitGCInfo();} 
            ~GCInfo();
            void AddItemNode(void *item);
            void AddItemUserKey(void *item);
            bool CleanupGCInfo();
        private:
            GC_FreeList * GetGC();
            static void DestroyGC(void *arg);
            bool InitGCInfo();
            uint32_t scale_;
            GC_FreeList *gc_free_list_;
            pthread_key_t GC_key_;
    }; //GCInfo class

} //namespace insdb
#endif //STORAGE_INSDB_DB_GARBAGE_COLLECTOR_H_
