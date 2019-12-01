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


#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/skldata.h"
#include "db/skiplist2.h"
#include <set>
#include "insdb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/mutexlock.h"
#include "port/port.h"

namespace insdb {

    typedef Slice Key;


    struct MyComparator {
        int operator()(const Key& a, const Key& b) const {
            if (a.size() == 0 || b.size() == 0) {
                if (a.size() == 0 && b.size() == 0) return 0;
                if (a.size() == 0) return -1;
                if (b.size() == 0) return 1;
            }
            return a.compare(b);
        }
    };

    class MyData : public SKLData<Key, MyComparator> {
        private:
            void *node_;
        public:
            Key key_;
            char key_str[32];
            uint64_t val_;
            MyData(Key key, uint64_t val): val_(val) {
                memcpy(key_str, key.data(), key.size());
                key_ = Slice(key_str, key.size());
            }
            void SetNode(void * internalnode) {
                node_ = internalnode;
            }
            void *GetNode() {
                return node_;
            }
            Key GetKeySlice() { return key_; }
            bool CompareKey(MyComparator comp, const Key& key, int& delta) { delta = comp(key_, key); return true;  }
    };


    typedef SkipList2<MyData*, Key, MyComparator> Sk_list;

    class Skiptest2 { };

    TEST(Skiptest2, Insert_search) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        Sk_list table(cmp);
        uint32_t insert = 0, insert_fail = 0;
        int key_count = 1000000;

        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            Slice key(key_str, 16);
            if (table.Insert(key, new MyData(key, i + 1000))) insert++;
            else insert_fail++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "insert result s(%lu) f(%lu) micros(%lu) insert(%d) insert_fail(%d)\n",
                start_time, finish_time, microsec, insert, insert_fail);  


        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        uint32_t found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            Slice key(key_str, 16);
            if (table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d)\n",
                start_time, finish_time, microsec, found, not_found);  
 
    }
}  // namespace insdb
int main(int argc, char** argv) {
    return insdb::test::RunAllTests();
}
