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
#include <vector>
#include <random>
#include <algorithm>
#include "db/skldata.h"
#include "db/skiplist.h"
#include "db/insdb_internal.h"
#include <set>
#include "insdb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/mutexlock.h"
#include "port/port.h"
#include <folly/ConcurrentSkipList.h>
#include <folly/concurrency/ConcurrentHashMap.h>
namespace insdb {
    using namespace folly;
    typedef Slice Key;

    struct MyComparator {
        int operator()(const Key& a, const Key& b) const {
            return a.compare(b);
        }
    };

    class MyData : public SKLData<Key, MyComparator> {
        private:
            void *node_;
        public:
            std::string ukey_;
            Slice key_slice_;
            uint64_t val_;
            MyData(Key key, uint64_t val): ukey_(key.data(), key.size()), key_slice_(ukey_), val_(val) {

            }
            void SetNode(void * internalnode) {
                node_ = internalnode;
            }
            void *GetNode() {
                return node_;
            }
            Key GetKeySlice() { return key_slice_; }
            bool CompareKey(MyComparator comp, const Key &key, int &delta) { delta = comp(GetKeySlice(), key);  return true; }
    };

    struct MyComparatorExt {
        bool operator()(MyData* a, MyData* b) const {
            return (a->GetKeySlice().compare(b->GetKeySlice()) < 0);
        }
    };
    const uint32_t TEST_32_HASH_START = 2166136261UL;
    struct MyHash {
        uint32_t operator() (Slice a) const {
            const signed char* s = reinterpret_cast<const signed char*>(a.data());
            uint32_t hash = TEST_32_HASH_START;
            uint32_t n = a.size();
            for (uint32_t i = 0; i < n; i++) {
                hash += (hash << 1) + (hash <<4) + (hash<<7) + (hash<< 8) + (hash<<24);
                hash ^= s[i];
            }
            return hash;
        }
    };

    typedef SkipList<MyData*, Key, MyComparator> Sk_list;
    typedef ConcurrentSkipList<MyData*, MyComparatorExt> SkipListType;
    std::vector<MyData*> g_DataList;

    class Skiptest { };

    struct ReadWriteArg{
        Sk_list *list;
        int start_key;
        int key_count;
        int hit;
        int miss;
        int insert;
        int insert_fail;
        int remove;
        int remove_already;
        int remove_invalid;
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
    };



    void WriteFunction(void *arg) {
        ReadWriteArg *rw_arg = reinterpret_cast<ReadWriteArg *>(arg);
        uint64_t wkey = rw_arg->start_key;
        for (int i = 0; i < rw_arg->key_count; i++, wkey++) {
            if (rw_arg->list->Insert(g_DataList[wkey]->GetKeySlice(), g_DataList[wkey])) rw_arg->insert++;
            else rw_arg->insert_fail++;
        }
    }


    void ReadFunction(void *arg) {
        ReadWriteArg *rw_arg = reinterpret_cast<ReadWriteArg *>(arg);
        uint64_t  wkey = rw_arg->start_key;
        MyData *result = NULL;
        for (int i = 0; i < rw_arg->key_count; i++, wkey++) {
            if (result =(MyData *)rw_arg->list->Search(g_DataList[wkey]->GetKeySlice())) {
                //ASSERT_EQ(g_DataList[wkey], result);
                //rw_arg->hit++;
            } else {
                //rw_arg->miss++;
            }
        }
    }

    void RemoveFunction(void *arg) {
        ReadWriteArg *rw_arg = reinterpret_cast<ReadWriteArg *>(arg);
        uint64_t wkey = rw_arg->start_key;
        MyData *data = NULL;
        for (int i = 0; i < rw_arg->key_count; i++, wkey++) {
            data = (MyData *)rw_arg->list->Remove(g_DataList[wkey]->GetKeySlice());
            if (data && data == g_DataList[wkey]) {
                //rw_arg->remove++;
            } else if (data) {
                //rw_arg->remove_invalid++;
            } else {
                //rw_arg->remove_already++;
            }
        }
    }

    void InitData(uint64_t count) {
        char buff[32];
        MyData *data = NULL;
        for (uint32_t i = 0; i < g_DataList.size(); i++)
            delete g_DataList[i];
        g_DataList.clear();
        g_DataList.resize(count);
        for (uint64_t i = 0; i < g_DataList.size(); i++) {
            sprintf(buff, "%016lx", i);
            buff[17] = 0;
            Slice key(buff, 16);
            g_DataList[i] = new MyData(key, i + 1);
        }
        //std::shuffle(g_DataList.begin(), g_DataList.end(), std::mtl199937{});
    }

    // single thread benchmarks
    TEST(Skiptest,  Folly_test) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        const int max_keys = 1000000;
        auto skipList = SkipListType::create(12);
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < max_keys; ++i) {
            skipList.add(g_DataList[i]);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nfolly SInser  workload %u total write execution time(%lu)microsec\n", max_keys,  microsec);

        start_time = Env::Default()->NowMicros();
        auto iter = skipList.begin();
        for (int i = 0; i < max_keys; ++i) {
            ASSERT_EQ(g_DataList[i], *iter);
            iter++;
            if (iter == skipList.end()) {
                iter = skipList.begin();
            }
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nfolly Iterate  workload %u total write execution time(%lu)microsec\n", max_keys,  microsec);

        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < max_keys; ++i) {
            skipList.contains(g_DataList[i]);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nfolly Find  workload %u total write execution time(%lu)microsec\n", max_keys,  microsec);
    }

    TEST(Skiptest, SInser_And_SRead) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 1000000;
        const int w_thr_count = 1;
        const int r_thr_count = 1;
        GCInfo gcinfo(16);
        Sk_list table(cmp, &gcinfo);

        /** write with multiple thread */
        ReadWriteArg *rw_arg = new ReadWriteArg[w_thr_count];
        TestThreadArg *t_arg = new TestThreadArg[w_thr_count];

        TestSharedState shared;
        shared.total = w_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < w_thr_count; i++) {
            rw_arg[i].start_key = 0;
            rw_arg[i].key_count = 1000000;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = WriteFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        WriteFunction((void *)&rw_arg[0]);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInser  workload %u total write execution time(%lu)microsec\n", max_keys,  microsec);
#if 0
        for (int i = 0; i < w_thr_count; i++) {
            fprintf(stderr, "write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
#endif
        delete rw_arg;
        delete t_arg;

        /** read with multiple thread */
        rw_arg = new ReadWriteArg[r_thr_count];
        t_arg = new TestThreadArg[r_thr_count];
        shared.total = r_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;

        for (int i = 0; i < r_thr_count; i++) {
            rw_arg[i].start_key = 0;
            rw_arg[i].key_count = 1000000;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = ReadFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        ReadFunction((void *)&rw_arg[0]);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSRead  workload %u total read execution time(%lu)microsec\n", max_keys,  microsec);
#if 0
        for (int i = 0; i < r_thr_count; i++) {
            fprintf(stderr, "read result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert); 
        }
#endif
        start_time = Env::Default()->NowMicros();
        MyData* Data = table.First();
        for (int i = 0; i < max_keys && Data; i++) {
            ASSERT_EQ(g_DataList[i], Data);
            Data = table.Next(Data);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSIter  workload %u total read execution time(%lu)microsec\n", max_keys,  microsec);

        delete rw_arg;
        delete t_arg;
    }

    TEST(Skiptest, ConcurrentHashMap_MyData) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        //MyComparator cmp;
        const int max_keys = 1000000;
        ConcurrentHashMap<Slice, MyData*, MyHash> hashmap(1000000);
        start_time = Env::Default()->NowMicros();
        for(int i = 0; i < max_keys; i++) {
            hashmap.insert(g_DataList[i]->GetKeySlice(), g_DataList[i]);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nHashMap_int insert  workload %u total read execution time(%lu)microsec\n", max_keys,  microsec);



        start_time = Env::Default()->NowMicros();
        for(int i = 0; i < max_keys; i++) {
            auto it = hashmap.find(g_DataList[i]->GetKeySlice());
            if (it->second != g_DataList[i]) {
                fprintf(stderr, "fail to find %dth key\n", i);
            }
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nHashMap_int find  workload %u total read execution time(%lu)microsec\n", max_keys,  microsec);

        start_time = Env::Default()->NowMicros();
        for(int i = 0; i < max_keys; i++) {
            hashmap.erase(g_DataList[i]->GetKeySlice());
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nHashMap_int erase  workload %u total read execution time(%lu)microsec\n", max_keys,  microsec);

    }


    TEST(Skiptest, ConcurrentHashMap_int) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        //MyComparator cmp;
        const int max_keys = 1000000;
        ConcurrentHashMap<int, int> hashmap(1000000);
        start_time = Env::Default()->NowMicros();
        for(int i = 0; i < max_keys; i++) {
            hashmap.insert(i, i);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nHashMap_int insert  workload %u total read execution time(%lu)microsec\n", max_keys,  microsec);



        start_time = Env::Default()->NowMicros();
        for(int i = 0; i < max_keys; i++) {
            hashmap.find(i);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nHashMap_int find  workload %u total read execution time(%lu)microsec\n", max_keys,  microsec);

        start_time = Env::Default()->NowMicros();
        for(int i = 0; i < max_keys; i++) {
            hashmap.erase(i);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nHashMap_int erase  workload %u total read execution time(%lu)microsec\n", max_keys,  microsec);
    }

    TEST(Skiptest, ConcurrentHashMap_uint64) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        //MyComparator cmp;
        const uint64_t max_keys = 1000000;
        ConcurrentHashMap<uint64_t, uint64_t> hashmap(1000000);
        start_time = Env::Default()->NowMicros();
        for(uint64_t i = 0; i < max_keys; i++) {
            hashmap.insert(i, i);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nHashMap_int insert  workload %lu total read execution time(%lu)microsec\n", max_keys,  microsec);



        start_time = Env::Default()->NowMicros();
        for(uint64_t i = 0; i < max_keys; i++) {
            hashmap.find(i);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nHashMap_int find  workload %lu total read execution time(%lu)microsec\n", max_keys,  microsec);

        start_time = Env::Default()->NowMicros();
        for(uint64_t i = 0; i < max_keys; i++) {
            hashmap.erase(i);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nHashMap_int erase  workload %lu total read execution time(%lu)microsec\n", max_keys,  microsec);
    }


#if 0
    TEST(Skiptest, MInsert_And_MRead) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 1000000;
        const int w_thr_count = 5;
        const int r_thr_count = 10;
        GCInfo gcinfo(16);
        Sk_list table(cmp, &gcinfo);
        InitData(max_keys);

        /** write with multiple thread */
        ReadWriteArg *rw_arg = new ReadWriteArg[w_thr_count];
        TestThreadArg *t_arg = new TestThreadArg[w_thr_count];

        TestSharedState shared;
        shared.total = w_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < w_thr_count; i++) {
            rw_arg[i].start_key = i * 200000;
            rw_arg[i].key_count = 200000;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = WriteFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, w_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert workload %u total write execution time(%lu) microsec\n", max_keys, microsec);
        for (int i = 0; i < w_thr_count; i++) {
            fprintf(stderr, "write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
        delete rw_arg;
        delete t_arg;

        /** read with multiple thread */
        rw_arg = new ReadWriteArg[r_thr_count];
        t_arg = new TestThreadArg[r_thr_count];
        shared.total = r_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;

        for (int i = 0; i < r_thr_count; i++) {
            rw_arg[i].start_key = (i % 10) * 100000 ;
            rw_arg[i].key_count = 100000;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = ReadFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, r_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMRead workload %u totalread execution time(%lu)\n microsec", max_keys,  microsec);
        for (int i = 0; i < r_thr_count; i++) {
            fprintf(stderr, "read result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert); 
        }
        delete rw_arg;
        delete t_arg;
    }
    TEST(Skiptest, MInsert_And_MRead_With_MSkipList) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 18;
        const int r_thr_count = 18;

        GCInfo gcinfo(16);
        Sk_list table1(cmp, &gcinfo);
        Sk_list table2(cmp, &gcinfo);
        Sk_list table3(cmp, &gcinfo);


        /** write with multiple thread */
        ReadWriteArg *rw_arg = new ReadWriteArg[w_thr_count];
        TestThreadArg *t_arg = new TestThreadArg[w_thr_count];

        TestSharedState shared;
        shared.total = w_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < w_thr_count; i++) {
            if (i < 6) { 
                rw_arg[i].list = &table1;
            } else if (i < 12) {
                rw_arg[i].list = &table2;
            } else {
                rw_arg[i].list = &table3;
            }
            rw_arg[i].start_key = (i % 6)  * 100000;
            rw_arg[i].key_count = 100000;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = WriteFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, w_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_With_MSkipList total write execution time(%lu)\n", microsec);
        for (int i = 0; i < w_thr_count; i++) {
            fprintf(stderr, "write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
        delete rw_arg;
        delete t_arg;

        /** read with multiple thread */
        rw_arg = new ReadWriteArg[r_thr_count];
        t_arg = new TestThreadArg[r_thr_count];
        shared.total = r_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;

        for (int i = 0; i < r_thr_count; i++) {
            if (i < 6) { 
                rw_arg[i].list = &table1;
            } else if (i < 12) {
                rw_arg[i].list = &table2;
            } else {
                rw_arg[i].list = &table3;
            }
            rw_arg[i].start_key = (i % 6) * 100000 ;
            rw_arg[i].key_count = 100000;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = ReadFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, r_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_With_MSkipList total read execution time(%lu)\n", microsec);
        for (int i = 0; i < r_thr_count; i++) {
            fprintf(stderr, "read result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert); 
        }
        delete rw_arg;
        delete t_arg;
    }







    TEST(Skiptest, C_MInsert_MRead) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 6;
        const int r_thr_count = 18;
        const int thr_count = w_thr_count + r_thr_count;
        GCInfo gcinfo(16);
        Sk_list table(cmp, &gcinfo);

        ReadWriteArg *rw_arg = new ReadWriteArg[thr_count];
        TestThreadArg *t_arg = new TestThreadArg[thr_count];

        TestSharedState shared;
        shared.total = thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < thr_count; i++) {
            if (i < w_thr_count) {
                rw_arg[i].start_key = i * 100000;
                rw_arg[i].key_count = 100000;
                t_arg[i].func = WriteFunction;
            } else {
                rw_arg[i].start_key = (i % 6) * 100000 ;
                rw_arg[i].key_count = 100000;
                t_arg[i].func = ReadFunction;
            }
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].thread_id = i;
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nC_MInsert_MRead total read/write execution time(%lu)\n", microsec);
        for (int i = 0; i < thr_count; i++) {
            fprintf(stderr, "read/write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
        delete rw_arg;
        delete t_arg;
    }


    TEST(Skiptest, C_MInsert_MRead_With_MSkipList) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 18;
        const int r_thr_count = 18;
        const int thr_count = w_thr_count + r_thr_count;
        GCInfo gcinfo(16);
        Sk_list table1(cmp, &gcinfo);
        Sk_list table2(cmp, &gcinfo);
        Sk_list table3(cmp, &gcinfo);

        ReadWriteArg *rw_arg = new ReadWriteArg[thr_count];
        TestThreadArg *t_arg = new TestThreadArg[thr_count];

        TestSharedState shared;
        shared.total = thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < thr_count; i++) {
            if (i < w_thr_count) {
                rw_arg[i].start_key = ((i / 3 ) % 6) * 100000;
                rw_arg[i].key_count = 100000;
                t_arg[i].func = WriteFunction;
            } else {
                rw_arg[i].start_key = ((i / 3) % 6) * 100000 ;
                rw_arg[i].key_count = 100000;
                t_arg[i].func = ReadFunction;
            }
            if ((i % 3) == 0) {
                rw_arg[i].list = &table1;
            } else if ((i % 3) == 1) {
                rw_arg[i].list = &table2;
            } else {
                rw_arg[i].list = &table3;
            }

            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].thread_id = i;
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nC_MInsert_MRead_With_MSkipList total read/write execution time(%lu)\n", microsec);
        for (int i = 0; i < thr_count; i++) {
            fprintf(stderr, "read/write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
        delete rw_arg;
        delete t_arg;
    }

    TEST(Skiptest, SInsert_And_SRead_And_SRemove) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 1;
        const int r_thr_count = 1;
        const int rm_thr_count = 1;
        GCInfo gcinfo(16);
        Sk_list table(cmp, &gcinfo);


        /** write with multiple thread */
        ReadWriteArg *rw_arg = new ReadWriteArg[w_thr_count];
        TestThreadArg *t_arg = new TestThreadArg[w_thr_count];

        TestSharedState shared;
        shared.total = w_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < w_thr_count; i++) {
            rw_arg[i].start_key = 0;
            rw_arg[i].key_count = 600000;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = WriteFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, w_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInsert_And_SRead_And_SRemove total write execution time(%lu)\n", microsec);
        for (int i = 0; i < w_thr_count; i++) {
            fprintf(stderr, "write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
        delete rw_arg;
        delete t_arg;

        /** read with multiple thread */
        rw_arg = new ReadWriteArg[r_thr_count];
        t_arg = new TestThreadArg[r_thr_count];
        shared.total = r_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;
        for (int i = 0; i < r_thr_count; i++) {
            rw_arg[i].start_key = 0;
            rw_arg[i].key_count = 600000;
            t_arg[i].func = ReadFunction;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, r_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInsert_And_SRead_And_SRemove total read execution time(%lu)\n", microsec);
        for (int i = 0; i < r_thr_count; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
        delete rw_arg;
        delete t_arg;

        rw_arg = new ReadWriteArg[rm_thr_count];
        t_arg = new TestThreadArg[rm_thr_count];
        shared.total = rm_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;
        for (int i = 0; i < rm_thr_count; i++) {
            rw_arg[i].start_key = 0;
            rw_arg[i].key_count = 600000;
            t_arg[i].func = RemoveFunction;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, rm_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInsert_And_SRead_And_SRemove total remove execution time(%lu)\n", microsec);
        for (int i = 0; i < rm_thr_count; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
        delete rw_arg;
        delete t_arg;
    }

    TEST(Skiptest, MInsert_And_MRead_And_MRemove) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 6;
        const int r_thr_count = 6;
        const int rm_thr_count = 6;
        GCInfo gcinfo(16);
        Sk_list table(cmp, &gcinfo);


        /** write with multiple thread */
        ReadWriteArg *rw_arg = new ReadWriteArg[w_thr_count];
        TestThreadArg *t_arg = new TestThreadArg[w_thr_count];

        TestSharedState shared;
        shared.total = w_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < w_thr_count; i++) {
            rw_arg[i].start_key = (i % 6) * 100000;
            rw_arg[i].key_count = 100000;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = WriteFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, w_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_And_MRemove total write execution time(%lu)\n", microsec);
        for (int i = 0; i < w_thr_count; i++) {
            fprintf(stderr, "write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
        delete rw_arg;
        delete t_arg;

        /** read with multiple thread */
        rw_arg = new ReadWriteArg[r_thr_count];
        t_arg = new TestThreadArg[r_thr_count];
        shared.total = r_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;
        for (int i = 0; i < r_thr_count; i++) {
            rw_arg[i].start_key = (i % 6) * 100000;
            rw_arg[i].key_count = 100000;
            t_arg[i].func = ReadFunction;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, r_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_And_MRemove total read execution time(%lu)\n", microsec);
        for (int i = 0; i < r_thr_count ; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
        delete rw_arg;
        delete t_arg;

        rw_arg = new ReadWriteArg[rm_thr_count];
        t_arg = new TestThreadArg[rm_thr_count];
        shared.total = rm_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;
        for (int i = 0; i < rm_thr_count; i++) {
            rw_arg[i].start_key = (i % 6) * 100000;
            rw_arg[i].key_count = 100000;
            t_arg[i].func = RemoveFunction;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, rm_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_And_MRemove total remove execution time(%lu)\n", microsec);
        for (int i = 0; i < rm_thr_count; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
        delete rw_arg;
        delete t_arg;
    }



    TEST(Skiptest, MInsert_And_MRead_And_MRemove_With_MSkipList) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 18;
        const int r_thr_count = 18;
        const int rm_thr_count = 18;
        GCInfo gcinfo(16);
        Sk_list table1(cmp, &gcinfo);
        Sk_list table2(cmp, &gcinfo);
        Sk_list table3(cmp, &gcinfo);



        /** write with multiple thread */
        ReadWriteArg *rw_arg = new ReadWriteArg[w_thr_count];
        TestThreadArg *t_arg = new TestThreadArg[w_thr_count];

        TestSharedState shared;
        shared.total = w_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < w_thr_count; i++) {
            if ((i % 3) == 0) {
                rw_arg[i].list = &table1;
            } else if ((i % 3) == 1) {
                rw_arg[i].list = &table2;
            } else {
                rw_arg[i].list = &table3;
            }
            rw_arg[i].start_key = ((i / 3 ) % 6) * 100000;
            rw_arg[i].key_count = 100000;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = WriteFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, w_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_And_MRemove_With_MSkipList total write execution time(%lu)\n", microsec);
        for (int i = 0; i < w_thr_count; i++) {
            fprintf(stderr, "write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
        delete rw_arg;
        delete t_arg;

        /** read with multiple thread */
        rw_arg = new ReadWriteArg[r_thr_count];
        t_arg = new TestThreadArg[r_thr_count];
        shared.total = r_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;
        for (int i = 0; i < r_thr_count; i++) {
            t_arg[i].func = ReadFunction;
            if ((i % 3) == 0) {
                rw_arg[i].list = &table1;
            } else if ((i % 3) == 1) {
                rw_arg[i].list = &table2;
            } else {
                rw_arg[i].list = &table3;
            }
            rw_arg[i].start_key = ((i / 3 ) % 6) * 100000;
            rw_arg[i].key_count = 100000;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, r_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_And_MRemove_With_MSkipList total read execution time(%lu)\n", microsec);
        for (int i = 0; i < r_thr_count; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
        delete rw_arg;
        delete t_arg;


        rw_arg = new ReadWriteArg[rm_thr_count];
        t_arg = new TestThreadArg[rm_thr_count];
        shared.total = rm_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;
        for (int i = 0; i < rm_thr_count; i++) {
            t_arg[i].func = RemoveFunction;
            if ((i % 3) == 0) {
                rw_arg[i].list = &table1;
            } else if ((i % 3) == 1) {
                rw_arg[i].list = &table2;
            } else {
                rw_arg[i].list = &table3;
            }
            rw_arg[i].start_key = ((i / 3 ) % 6) * 100000;
            rw_arg[i].key_count = 100000;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, rm_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_And_MRemove_With_MSkipList total remove execution time(%lu)\n", microsec);
        for (int i = 0; i < rm_thr_count; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
        delete rw_arg;
        delete t_arg;
    }



    TEST(Skiptest, MInsert_And_MRead_MRemove) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 6;
        const int r_thr_count = 18;
        const int rm_thr_count = 6;
        GCInfo gcinfo(16);
        Sk_list table(cmp, &gcinfo);


        /** write with multiple thread */
        ReadWriteArg *rw_arg = new ReadWriteArg[w_thr_count];
        TestThreadArg *t_arg = new TestThreadArg[w_thr_count];

        TestSharedState shared;
        shared.total = w_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < w_thr_count; i++) {
            rw_arg[i].start_key = i * 100000;
            rw_arg[i].key_count = 100000;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = WriteFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, w_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_MRemove total write execution time(%lu)\n", microsec);
        for (int i = 0; i < w_thr_count; i++) {
            fprintf(stderr, "write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
        delete rw_arg;
        delete t_arg;

        /** read with multiple thread */
        int total_thr_count = r_thr_count + rm_thr_count;
        rw_arg = new ReadWriteArg[total_thr_count];
        t_arg = new TestThreadArg[total_thr_count];
        shared.total = total_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;
        for (int i = 0; i < total_thr_count; i++) {
            if (i < r_thr_count) {
                rw_arg[i].start_key = (i % 6) * 100000 ;
                rw_arg[i].key_count = 100000;
                t_arg[i].func = ReadFunction;
            } else {
                rw_arg[i].start_key = (i % 6) * 100000 ;
                rw_arg[i].key_count = 10000;
                t_arg[i].func = RemoveFunction;
            }
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, total_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_MRemove total read/remove execution time(%lu)\n", microsec);
        for (int i = 0; i < r_thr_count + rm_thr_count; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
        delete rw_arg;
        delete t_arg;
    }

    TEST(Skiptest, MInsert_And_MRead_MRemove_With_MSkipList) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 18;
        const int r_thr_count = 18;
        const int rm_thr_count = 18;
        GCInfo gcinfo(16);
        Sk_list table1(cmp, &gcinfo);
        Sk_list table2(cmp, &gcinfo);
        Sk_list table3(cmp, &gcinfo);



        /** write with multiple thread */
        ReadWriteArg *rw_arg = new ReadWriteArg[w_thr_count];
        TestThreadArg *t_arg = new TestThreadArg[w_thr_count];

        TestSharedState shared;
        shared.total = w_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < w_thr_count; i++) {
            if ((i % 3) == 0) {
                rw_arg[i].list = &table1;
            } else if ((i % 3) == 1) {
                rw_arg[i].list = &table2;
            } else {
                rw_arg[i].list = &table3;
            }
            rw_arg[i].start_key = ((i / 3 ) % 6) * 100000;
            rw_arg[i].key_count = 100000;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].func = WriteFunction;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, w_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_MRemove_With_MSkipList total write execution time(%lu)\n", microsec);
        for (int i = 0; i < w_thr_count; i++) {
            fprintf(stderr, "write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
        delete rw_arg;
        delete t_arg;

        /** read with multiple thread */
        int total_thr_count = r_thr_count + rm_thr_count;
        int rm_start = 0;
        rw_arg = new ReadWriteArg[total_thr_count];
        t_arg = new TestThreadArg[total_thr_count];
        shared.total = total_thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;
        for (int i = 0; i < total_thr_count; i++) {
            if (i < r_thr_count) {
                t_arg[i].func = ReadFunction;
                rw_arg[i].key_count = 100000;
            } else {
                t_arg[i].func = RemoveFunction;
                rw_arg[i].key_count = 10000;
            }
            if ((i % 3) == 0) {
                rw_arg[i].list = &table1;
            } else if ((i % 3) == 1) {
                rw_arg[i].list = &table2;
            } else {
                rw_arg[i].list = &table3;
            }
            rw_arg[i].start_key = ((i / 3 ) % 6) * 100000;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].thread_id = i;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, total_thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nMInsert_And_MRead_MRemove_With_MSkipList total read/remove execution time(%lu)\n", microsec);
        for (int i = 0; i < r_thr_count + rm_thr_count; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
        delete rw_arg;
        delete t_arg;
    }

    TEST(Skiptest, C_MInsert_MRead_MRemove) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 6;
        const int r_thr_count = 18;
        const int rm_thr_count = 6;
        const int thr_count = w_thr_count + r_thr_count + rm_thr_count;
        GCInfo gcinfo(16);
        Sk_list table(cmp, &gcinfo);

        ReadWriteArg *rw_arg = new ReadWriteArg[thr_count];
        TestThreadArg *t_arg = new TestThreadArg[thr_count];

        TestSharedState shared;
        shared.total = thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < thr_count; i++) {
            if (i < w_thr_count) {
                t_arg[i].func = WriteFunction;
                rw_arg[i].key_count = 100000;
            } else if (i < r_thr_count) {
                t_arg[i].func = ReadFunction;
                rw_arg[i].key_count = 100000;
            } else {
                t_arg[i].func = RemoveFunction;
                rw_arg[i].key_count = 10000;
            }
            rw_arg[i].start_key = (i % 6) * 100000;
            rw_arg[i].list = &table;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].thread_id = i;
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nC_MInsert_MRead_MRemove total read/write/remove execution time(%lu)\n", microsec);
        for (int i = 0; i < thr_count; i++) {
            fprintf(stderr, "read/write/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
        delete rw_arg;
        delete t_arg;
    }

    TEST(Skiptest, C_MInsert_MREad_MRemove_With_MSkipList) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const int max_keys = 600000;
        const int w_thr_count = 18;
        const int r_thr_count = 18;
        const int rm_thr_count = 18;
        const int thr_count = w_thr_count + r_thr_count + rm_thr_count;
        GCInfo gcinfo(16);
        Sk_list table1(cmp, &gcinfo);
        Sk_list table2(cmp, &gcinfo);
        Sk_list table3(cmp, &gcinfo);

        ReadWriteArg *rw_arg = new ReadWriteArg[thr_count];
        TestThreadArg *t_arg = new TestThreadArg[thr_count];

        TestSharedState shared;
        shared.total = thr_count;
        shared.num_done = 0;
        shared.num_initialized = 0;
        shared.start = false;


        for (int i = 0; i < thr_count; i++) {
            if ((i % 3) == 0) {
                rw_arg[i].list = &table1;
            } else if ((i % 3) == 1) {
                rw_arg[i].list = &table2;
            } else {
                rw_arg[i].list = &table3;
            }

            if (i < w_thr_count) {
                t_arg[i].func = WriteFunction;
                rw_arg[i].key_count = 100000;
            } else if (i < w_thr_count + r_thr_count) {
                t_arg[i].func = ReadFunction;
                rw_arg[i].key_count = 100000;
            } else {
                t_arg[i].func = RemoveFunction;
                rw_arg[i].key_count = 10000;
            }
            rw_arg[i].start_key = ((i / 3 ) % 6) * 100000;
            rw_arg[i].start_time = 0;
            rw_arg[i].finish_time = 0;
            rw_arg[i].microsec = 0;
            rw_arg[i].hit = 0;
            rw_arg[i].miss = 0;
            rw_arg[i].insert = 0;
            rw_arg[i].insert_fail = 0;
            rw_arg[i].remove = 0;
            rw_arg[i].remove_already = 0;
            rw_arg[i].remove_invalid = 0;
            t_arg[i].arg = reinterpret_cast<void*>(&rw_arg[i]);
            t_arg[i].thread_id = i;
            t_arg[i].shared = &shared;
        }
        start_time = Env::Default()->NowMicros();
        TestLaunch(&shared, t_arg, thr_count);
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nC_MInsert_MREad_MRemove_With_MSkipList total read/write/remove execution time(%lu)\n", microsec);
        for (int i = 0; i < thr_count; i++) {
            fprintf(stderr, "read/write/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }

        // Forward iteration test
        {
            MyData *data = NULL;
            std::set<Key> keys;
            data = (MyData *)table1.First();

            // Compare against model iterator
            for (; data; data = (MyData *)table1.Next(data)) {
                if(!keys.insert(table1.GetKeySlice(data)).second) {
                    ASSERT_TRUE(0);
                }
                //fprintf(stderr, "iter key %lu\n", iter.key());

            }
            // Backward iteration test
            data = (MyData *)table1.Last();

            // Compare against model iterator
            for (std::set<Key>::reverse_iterator model_iter = keys.rbegin();
                    model_iter != keys.rend();
                    ++model_iter) {
                ASSERT_EQ(*model_iter, table1.GetKeySlice(data));
                //fprintf(stderr, "iter key %lu\n", iter.key());
                data = (MyData *)table1.Prev(data);
            }
        }

        {
            MyData *data = NULL;
            std::set<Key> keys;
            data = (MyData *)table2.First();

            // Compare against model iterator
            for (; data; data = (MyData *)table2.Next(data)) {
                if(!keys.insert(table2.GetKeySlice(data)).second) {
                    ASSERT_TRUE(0);
                }
                //fprintf(stderr, "iter key %lu\n", iter.key());

            }
            // Backward iteration test
            data = (MyData *)table2.Last();

            // Compare against model iterator
            for (std::set<Key>::reverse_iterator model_iter = keys.rbegin();
                    model_iter != keys.rend();
                    ++model_iter) {
                ASSERT_EQ(*model_iter, table2.GetKeySlice(data));
                //fprintf(stderr, "iter key %lu\n", iter.key());
                data = (MyData *)table2.Prev(data);
            }
        }

        {
            MyData *data = NULL;
            std::set<Key> keys;
            data = (MyData *)table3.First();

            // Compare against model iterator
            for (; data; data = (MyData *)table3.Next(data)) {
                if(!keys.insert(*(table3.GetKeySlice(data)().second) {
                            ASSERT_TRUE(0);
                            }
                            //fprintf(stderr, "iter key %lu\n", iter.key());

                            }
                            // Backward iteration test
                            data = (MyData *)table3.Last();

                            // Compare against model iterator
                            for (std::set<Key>::reverse_iterator model_iter = keys.rbegin();
                                model_iter != keys.rend();
                                ++model_iter) {
                            ASSERT_EQ(*model_iter, table3.GetKeySlice(data));
                            //fprintf(stderr, "iter key %lu\n", iter.key());
                            data = (MyData *)table3.Prev(data);
                            }
                            }
                            MyData *cur = (MyData *)table1.SearchGreaterOrEqual(1);
                            if (cur) {
                            do { 
                                //if (!(cur->val_ % 100)) fprintf(stderr, "Next(1) %lu\n", cur->val_);
                            } while ((cur = (MyData *)table1.Next(cur)) != NULL);
                            }

                            cur = (MyData *)table1.SearchLessThanEqual(600000);
                            if (cur) {
                                do { 
                                    //if (!(cur->val_ % 100)) fprintf(stderr, "Prev(600000) %lu\n", cur->val_);
                                } while ((cur = (MyData *)table1.Prev(cur)) != NULL);
                            }

                            delete rw_arg;
                            delete t_arg;
    }




    TEST(Skiptest, Empty) {
        MyComparator cmp;
        GCInfo gcinfo(16);
        SkipList<MyData*, Key, MyComparator> list(cmp, &gcinfo);
        ASSERT_TRUE(!list.Contains(10));

        MyData *data = NULL;
        ASSERT_TRUE(!list.First());
        ASSERT_TRUE(!list.Search(100));
        ASSERT_TRUE(!list.Last());
    }

    TEST(Skiptest, InsertAndLookupData) {
        const int N = 2000;
        MyComparator cmp;
        MyData *result = NULL;
        GCInfo gcinfo(16);
        SkipList<MyData*, Key, MyComparator> list(cmp, &gcinfo);
        for (int i = 0; i < N; i++) {
            list.Insert(i*2, new MyData(i*2, i+1));
        }
        for (int i = 0; i < N; i++) {
            if (result = (MyData *)list.Search(i*2)) {
                ASSERT_EQ(i + 1, result->val_);
            } else {
                ASSERT_TRUE(0);
            }

            if (result = (MyData *)list.Search(i*2 + 1)) {
                ASSERT_TRUE(0);
            }

            if (result = (MyData *)list.SearchLessThanEqual(i*2 + 1)) {
                ASSERT_EQ(i + 1, result->val_);
            } else {
                ASSERT_TRUE(0);
            }
        }
    }


    TEST(Skiptest, InsertAndRemove) {
        const int N = 2000;
        MyComparator cmp;
        MyData *result = NULL;
        GCInfo gcinfo(16);
        SkipList<MyData*, Key, MyComparator> list(cmp, &gcinfo);
        for (int i = 0; i < N; i++) {
            list.Insert(i*2, new MyData(i*2, i+1));
        }
        for (int i = 0; i < N; i++) {
            if (result = (MyData *)list.Search(i*2)) {
                ASSERT_EQ(i + 1, result->val_);
            } else {
                ASSERT_TRUE(0);
            }

            result = (MyData *)list.Remove(i*2);
            if (result && result->val_ != (i + 1)) {
                ASSERT_TRUE(0);
            }

            if (result =(MyData *)list.Search(i*2)) {
                ASSERT_TRUE(0);
            }

        }
    }



    TEST(Skiptest, InsertAndLookup) {
        const int N = 2000;
        const int R = 5000;
        Random rnd(1000);
        std::set<Key> keys;
        MyComparator cmp;
        GCInfo gcinfo(16);
        SkipList<MyData*, Key, MyComparator> list(cmp, &gcinfo);
        MyData* data = NULL;
        for (int i = 0; i < N; i++) {
            Key key = rnd.Next() % R;
            if (keys.insert(key).second) {
                list.Insert(key, new MyData(key, i+1));
            }
        }

        for (int i = 0; i < R; i++) {
            if (list.Contains(i)) {
                ASSERT_EQ(keys.count(i), 1);
            } else {
                ASSERT_EQ(keys.count(i), 0);
            }
        }

        // Simple iterator tests
        {
            data = (MyData *)list.First();
            ASSERT_TRUE(data);

            data = (MyData *)list.Search(list.GetKeySlice(data));
            ASSERT_TRUE(data);
            ASSERT_EQ(*(keys.begin()), list.GetKeySlice(data));


            data = (MyData *)list.First();
            ASSERT_TRUE(data);
            ASSERT_EQ(*(keys.begin()), list.GetKeySlice(data));

            data = (MyData *)list.Last();
            ASSERT_TRUE(data);
            ASSERT_EQ(*(keys.rbegin()), list.GetKeySlice(data));
        }

        // Forward iteration test
        for (int i = 0; i < R; i++) {
            data = (MyData *)list.Search(i); 

            // Compare against model iterator
            if (data) {
                std::set<Key>::iterator model_iter = keys.lower_bound(i);
                for (int j = 0; j < 3; j++) {
                    if (model_iter == keys.end()) {
                        ASSERT_TRUE(!data);
                        break;
                    } else {
                        ASSERT_TRUE(data);
                        ASSERT_EQ(*model_iter, list.GetKeySlice(data));
                        ++model_iter;
                        data = (MyData *)list.Next(data);
                    }
                }
            }
        }

        // Backward iteration test
        {
            data = (MyData *)list.Last();

            // Compare against model iterator
            for (std::set<Key>::reverse_iterator model_iter = keys.rbegin();
                    model_iter != keys.rend();
                    ++model_iter) {
                ASSERT_TRUE(data);
                ASSERT_EQ(*model_iter, list.GetKeySlice(data));
                data = (MyData *)list.Prev(data);
            }
            ASSERT_TRUE(!data);
        }
    }
#endif
}  // namespace insdb
int main(int argc, char** argv) {
    insdb::InitData(1000000);
    return insdb::test::RunAllTests();
}
