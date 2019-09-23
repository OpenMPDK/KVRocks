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

    typedef uint64_t Key;


    struct MyComparator {
        int operator()(const Key& a, const Key& b) const {
            if (a < b) {
                return -1;
            } else if (a > b) {
                return +1;
            } else {
                return 0;
            }
        }
    };

    class MyData : public SKLData<Key, MyComparator> {
        private:
            void *node_;
        public:
            Key key_;
            uint64_t val_;
            MyData(Key key, uint64_t val): key_(key), val_(val) {}
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
        rw_arg->start_time = Env::Default()->NowMicros();
        Key wkey = rw_arg->start_key;
        for (int i = 0; i < rw_arg->key_count; i++, wkey++) {
            if (rw_arg->list->Insert(wkey, new MyData(wkey, wkey+1))) rw_arg->insert++;
            else rw_arg->insert_fail++;
        }
        rw_arg->finish_time = Env::Default()->NowMicros();
        rw_arg->microsec = (rw_arg->finish_time - rw_arg->start_time);
    }


    void ReadFunction(void *arg) {
        ReadWriteArg *rw_arg = reinterpret_cast<ReadWriteArg *>(arg);
        rw_arg->start_time = Env::Default()->NowMicros();
        Key wkey = rw_arg->start_key;
        MyData *result = NULL;
        for (int i = 0; i < rw_arg->key_count; i++, wkey++) {
            if (result =(MyData *)rw_arg->list->Search(wkey)) {
                ASSERT_EQ(wkey+1, result->val_);
                rw_arg->hit++;
            } else {
                rw_arg->miss++;
            }
        }
        rw_arg->finish_time = Env::Default()->NowMicros();
        rw_arg->microsec = (rw_arg->finish_time - rw_arg->start_time);
    }


    void RemoveFunction(void *arg) {
        ReadWriteArg *rw_arg = reinterpret_cast<ReadWriteArg *>(arg);
        rw_arg->start_time = Env::Default()->NowMicros();
        Key wkey = rw_arg->start_key;
        MyData *data = NULL;
        for (int i = 0; i < rw_arg->key_count; i++, wkey++) {
            data = (MyData *)rw_arg->list->Remove(wkey);
            if (data && data->val_ == (wkey + 1)) {
                rw_arg->remove++;
            } else if (data) {
                rw_arg->remove_invalid++;
            } else {
                rw_arg->remove_already++;
            }
        }
        rw_arg->finish_time = Env::Default()->NowMicros();
        rw_arg->microsec = (rw_arg->finish_time - rw_arg->start_time);
    }

    TEST(Skiptest2, SInsert_And_SRead_And_SRemove) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        //const int max_keys = 600000;
        const int w_thr_count = 1;
        const int r_thr_count = 1;
        const int rm_thr_count = 1;
        Sk_list table(cmp);


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
#if 1
        for (int i = 0; i < w_thr_count; i++) {
            fprintf(stderr, "write result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec, rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail); 
        }
#endif
        delete [] rw_arg;
        delete [] t_arg;

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
#if 1
        for (int i = 0; i < r_thr_count; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
#endif
        delete [] rw_arg;
        delete [] t_arg;

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
#if 1
        for (int i = 0; i < rm_thr_count; i++) {
            fprintf(stderr, "read/remove result [%d] :list(%p)  s(%lu) f(%lu) micros(%lu) hit(%d) miss(%d) insert(%d) insert_fail(%d) remove(%d) remove_already(%d) remove_invalid(%d)\n",
                    i, rw_arg[i].list, rw_arg[i].start_time, rw_arg[i].finish_time, rw_arg[i].microsec,
                    rw_arg[i].hit, rw_arg[i].miss, rw_arg[i].insert, rw_arg[i].insert_fail, rw_arg[i].remove, rw_arg[i].remove_already, rw_arg[i].remove_invalid); 
        }
#endif
        delete [] rw_arg;
        delete [] t_arg;
    }

    TEST(Skiptest2, SInsert_SSplit) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const uint64_t max_keys = 1000000;
        Sk_list table(cmp);

        start_time = Env::Default()->NowMicros();
        Key wkey = 0;
        for (uint64_t i = 0; i < max_keys; i++, wkey++)
            table.Insert(wkey, new MyData(wkey, wkey+1));
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total write execution time(%lu)\n", microsec);

        start_time = Env::Default()->NowMicros();
        SLSplitCtx m_ctx;
        Sk_list t_table(cmp);
        t_table.InitSplitTarget(&m_ctx);
        MyData* mk = table.First(&m_ctx);
        while(mk) {
            Key mk_key = mk->GetKeySlice();
            if (mk_key % 2) {
                table.Remove(&m_ctx);
                t_table.Append(&m_ctx);
            }
            mk = table.Next(nullptr, &m_ctx);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total split execution time(%lu)\n", microsec);
        

        start_time = Env::Default()->NowMicros();
        for (uint64_t i = 0; i < max_keys; i++) {
            if ((i % 2) == 0)  {
                if (!table.Search(i)) fprintf(stderr, "not found key %ld from original table\n", i);
            }
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total search original table execution time(%lu)\n", microsec);

        start_time = Env::Default()->NowMicros();
        mk = table.First(&m_ctx);
        while(mk) {
            MyData *next_mk = table.Next(mk);
            table.Remove(mk->GetKeySlice());
            mk = next_mk;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total remove original table execution time(%lu)\n", microsec);


        start_time = Env::Default()->NowMicros();
        for (uint64_t i = 0; i < max_keys; i++) {
            if (i % 2) {
                if (!t_table.Search(i)) fprintf(stderr, "not found key %ld from split table\n", i);
            }
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total search split table execution time(%lu)\n", microsec);

        start_time = Env::Default()->NowMicros();
        mk = t_table.First(&m_ctx);
        while(mk) {
            MyData *next_mk = t_table.Next(mk);
            t_table.Remove(mk->GetKeySlice());
            mk = next_mk;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total remove split table execution time(%lu)\n", microsec);



    }

    TEST(Skiptest2, SInsert_SSplit_With_SearchGreaterOrEqual) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const uint64_t max_keys = 1000000;
        Sk_list table(cmp);

        start_time = Env::Default()->NowMicros();
        Key wkey = 0;
        for (uint64_t i = 0; i < max_keys; i++, wkey++)
            table.Insert(wkey, new MyData(wkey, wkey+1));
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total write execution time(%lu)\n", microsec);

        start_time = Env::Default()->NowMicros();
        SLSplitCtx m_ctx;
        Sk_list t_table(cmp);
        t_table.InitSplitTarget(&m_ctx);
        Key start_key = 500000;
        MyData* mk = table.SearchGreaterOrEqual(start_key, &m_ctx);
        while(mk) {
            table.Remove(&m_ctx);
            t_table.Append(&m_ctx);
            mk = table.Next(nullptr, &m_ctx);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total split execution time(%lu)\n", microsec);
        

        start_time = Env::Default()->NowMicros();
        for (uint64_t i = 0; i < 499999; i++) {
            if (!table.Search(i)) fprintf(stderr, "not found key %ld from original table\n", i);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total search original table execution time(%lu)\n", microsec);

        start_time = Env::Default()->NowMicros();
        mk = table.First(&m_ctx);
        while(mk) {
            MyData *next_mk = table.Next(mk);
            table.Remove(mk->GetKeySlice());
            mk = next_mk;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total remove original table execution time(%lu)\n", microsec);


        start_time = Env::Default()->NowMicros();
        for (uint64_t i = 500000; i < max_keys; i++) {
            if (!t_table.Search(i)) fprintf(stderr, "not found key %ld from split table\n", i);
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total search split table execution time(%lu)\n", microsec);

        start_time = Env::Default()->NowMicros();
        mk = t_table.First(&m_ctx);
        while(mk) {
            MyData *next_mk = t_table.Next(mk);
            t_table.Remove(mk->GetKeySlice());
            mk = next_mk;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total remove split table execution time(%lu)\n", microsec);
    }




    TEST(Skiptest2, SInsert_SSplit_orig) {
        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        MyComparator cmp;
        const uint64_t max_keys = 1000000;
        Sk_list table(cmp);

        start_time = Env::Default()->NowMicros();
        Key wkey = 0;
        for (uint64_t i = 0; i < max_keys; i++, wkey++)
            table.Insert(wkey, new MyData(wkey, wkey+1));
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total write execution time(%lu)\n", microsec);

        start_time = Env::Default()->NowMicros();
        SLSplitCtx m_ctx;
        Sk_list t_table(cmp);
        t_table.InitSplitTarget(&m_ctx);
        MyData* mk = table.First();
        MyData* next_mk = nullptr;
        while(mk) {
            next_mk = table.Next(mk);
            Key mk_key = mk->GetKeySlice();
            if (mk_key % 2) {
                table.Remove(mk_key);
                t_table.Insert(mk->GetKeySlice(), mk);
            }
            mk = next_mk;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total split execution time(%lu)\n", microsec);
        

        start_time = Env::Default()->NowMicros();
        for (uint64_t i = 0; i < max_keys; i++) {
            if ((i % 2) == 0)  {
                if (!table.Search(i)) fprintf(stderr, "not found key %ld from original table\n", i);
            }
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total search original table execution time(%lu)\n", microsec);

        start_time = Env::Default()->NowMicros();
        mk = table.First(&m_ctx);
        while(mk) {
            MyData *next_mk = table.Next(mk);
            table.Remove(mk->GetKeySlice());
            mk = next_mk;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total remove original table execution time(%lu)\n", microsec);


        start_time = Env::Default()->NowMicros();
        for (uint64_t i = 0; i < max_keys; i++) {
            if (i % 2) {
                if (!t_table.Search(i)) fprintf(stderr, "not found key %ld from split table\n", i);
            }
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total search split table execution time(%lu)\n", microsec);

        start_time = Env::Default()->NowMicros();
        mk = t_table.First(&m_ctx);
        while(mk) {
            MyData *next_mk = t_table.Next(mk);
            t_table.Remove(mk->GetKeySlice());
            mk = next_mk;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        fprintf(stderr, "\nSInset_SSplit total remove split table execution time(%lu)\n", microsec);
    }


}  // namespace insdb
int main(int argc, char** argv) {
    return insdb::test::RunAllTests();
}
