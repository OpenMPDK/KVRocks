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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <set>
#include "insdb/env.h"
#include "insdb/slice.h"
#include "port/port.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace insdb {

static const int kReqCount = 100000;

class EnvPosixTest {
 public:
  Env* env_;
  EnvPosixTest() : env_(Env::Default()) { }
};
#if 0
class InSDBKeyCompare {
    public:
    bool operator() (const InSDBKey &lhs, const InSDBKey &rhs) const {
        if (lhs.type != rhs.type) return lhs.type < rhs.type;
        if (lhs.split_seq != rhs.split_seq) return lhs.split_seq < rhs.split_seq;
        if (lhs.seq != rhs.seq) return lhs.seq < rhs.seq;

    }
};
#endif

TEST(EnvPosixTest, ReadWriteDelete) {
    char buffer[4096];
    int i = 0;
    int size = 0;
    uint64_t start_time = 0, end_time = 0, microsec = 0;
    double iops = 0;
    Status s;
    InSDBKey key;
    std::vector<std::string> path{"/dev/nvme0n1"};
    ASSERT_OK(env_->Open(path));
    memset(&key, 0, sizeof(InSDBKey));
    memset(buffer, 0, 4096);

#if 1
    // Multicommand test
    bool async = true;
    static int step = 100;
    static int value_size = 100;
    InSDBKey blocking_key = { 0 };

test_again:
    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        std::vector<InSDBKey> key_vec;
        std::vector<char*> buf_vec;
        std::vector<int> size_vec;
        int this_step = step<=(kReqCount-i)?step:(kReqCount-i);
        for(int j=0; j<this_step; j++, i++) {
            key.db_id = 0x19191234;
            key.seq = i;
            snprintf(buffer, value_size, "value%d", i);
            key_vec.push_back(key);
            buf_vec.push_back(buffer);
            size_vec.push_back(value_size);
        }
        ASSERT_OK(env_->MultiPut(key_vec, buf_vec, size_vec, blocking_key, async));
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/((double)microsec/kReqCount);
    fprintf(stderr, "MultiPut %s execution time(%lu) : numIo(%d) : IOps(%lf)\n", async?"async":"sync", microsec, kReqCount, iops);

    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount;) {
        std::vector<InSDBKey> key_vec;
        int this_step = step<=(kReqCount-i)?step:(kReqCount-i);
        for(int j=0; j<this_step; j++, i++) {
            key.db_id = 0x19191234;
            key.seq = i;
            key_vec.push_back(key);
        }
        std::vector<Env::DevStatus> status;
        ASSERT_OK(env_->MultiDel(key_vec, blocking_key, async, status));
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/((double)microsec/kReqCount);
    fprintf(stderr, "MultiDel %s execution time(%lu) : numIo(%d) : IOps(%lf)\n", async?"async":"sync", microsec, kReqCount, iops);

    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; ) {
        std::vector<InSDBKey> key_vec;
        std::vector<char*> buf_vec;
        std::vector<int> size_vec;
        int this_step = step<=(kReqCount-i)?step:(kReqCount-i);
        memset(buffer, 0, size);
        for(int j=0; j<this_step; j++, i++) {
            key.db_id = 0x19191234;
            key.seq = i;
            key_vec.push_back(key);
            buf_vec.push_back(buffer);
            size_vec.push_back(value_size);
        }
        std::vector<Env::DevStatus> status;
        ASSERT_OK(env_->MultiGet(key_vec, buf_vec, size_vec, blocking_key, async, 0, status));
        if(!async) {
            for(int j=0; j<this_step; j++) {
                ASSERT_TRUE(status[j] == Env::KeyNotExist);
            }
        }
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/((double)microsec/kReqCount);
    fprintf(stderr, "MultiGet %s execution time(%lu) : numIo(%d) : IOps(%lf)\n", async?"async":"sync", microsec, kReqCount, iops);

    // Make sure keys do not exist using single GET
    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        size = 4096;
        memset(buffer, 0, size);
        ASSERT_TRUE(!env_->Get(key, buffer, &size).ok());
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "Get non-existing keys execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);

    if(async) {
        async=false;
        goto test_again;
    }
#endif

    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        snprintf(buffer, 4096, "value%d", i);
        Slice value(buffer, 4096);
        ASSERT_OK(env_->Put(key, value));
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "Put execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);


#if 0

    char iter_buff[1024*32];
    unsigned char iter_handle = 0;
    int iter_buff_size = 1024*32;
    uint32_t iter_index = 0;
    InSDBKey *key_array = NULL;
    int key_count = 0;
    std::set<InSDBKey, InSDBKeyCompare> keyslist;
    key.db_id = 0x19191234;
    key.seq = 99;
    char* p = (char *)&key;
    printf("key sequence example: ");
    for (i = 0; i < 16; i++) printf("%02x.", p[i]);
    printf("\n");

    for (unsigned char j = 0; j < 16; j++) {
        iter_handle = j;
        env_->IterReq(0xffffffff, key.db_id, &iter_handle, false);
    }
    iter_handle = 0;
    start_time = env_->NowMicros();
    ASSERT_OK(env_->IterReq(0xffffffff, key.db_id, &iter_handle, true));
    while(1) {
        iter_buff_size = 1024*32;
        s = env_->IterRead(iter_buff, &iter_buff_size, iter_handle);
        if (s.ok()) {
            key_array = (InSDBKey *)iter_buff;
            key_count = iter_buff_size / 16;
            keyslist.insert(key_array, key_array + key_count);
        } else  {
            break;
        }
    }
    ASSERT_OK(env_->IterReq(0xffffffff, key.db_id, &iter_handle, false));
    iter_index = 0;
    for (auto it = keyslist.begin(); it != keyslist.end(); ++it) {
        //printf("%dth entry--> db_id (%d) seq(%lu)\n", iter_index, (*it).db_id, (*it).seq);
        ASSERT_OK(env_->Delete(*it));
        iter_index++;
    }
    env_->Flush(kWriteFlush);
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    fprintf(stderr, "Get Iterat key execution time(%lu) \n", microsec);
    fprintf(stderr, "Get Iterat and delete  key execution time(%lu) \n", microsec);
    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        snprintf(buffer, 4096, "value%d", i);
        Slice value(buffer, 4096);
        ASSERT_OK(env_->Put(key, value));
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "Put execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);
#endif
    
    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        size = 4096;
        memset(buffer, 0, size);
        ASSERT_OK(env_->Get(key, buffer, &size));
        ASSERT_EQ(size, 4096);
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "Get execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);

    char log_buff[1024*4];
    start_time = env_->NowMicros();
    key.db_id = 0x19191234;
    unsigned char iter_handle = 0;
    ASSERT_OK(env_->IterReq(0xffffffff, key.db_id, &iter_handle, 0, true, true));
    {
        bool b_found = false;
        bool b_finished = false;
        char *data = NULL;
        while(1) {
            s = env_->GetLog(0xD0, log_buff, 4096, 0);
            if (!s.ok()) {
                printf("fail to GetLog\n");
                break;
            }
            data = log_buff;
            for (i = 0; i < 16; i++) {
                printf("nHandle(%02x) bOpened(%02x) nIterTyep(%02x) nKeySpaceId(%02x) niterValue(%08x) niterMaks(%08x) nfinished(%02x) \n",
                        data[0], data[1], data[2], data[3],
                        *((unsigned int *)&data[4]), *((unsigned int *)&data[8]), data[12]);
                data += 16;
            }
            data = log_buff;
            for (i = 0; i < 16; i++) {
                if (data[i*16] == iter_handle) { /* check handle */
                    b_found = true;
                    if (data[i*16 + 12] == 0x01 /* check finished */) {
                        b_finished = true;
                    }
                    break;
                }
            }
            if (b_found != true) {
                printf("iterate handle not exist 0x%02x\n", iter_handle);
                break;
            }
            if (b_finished) {
                printf("End checking 0x%02x iterate delete with get log\n", iter_handle);
                break;
            }
        }
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "Get execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);


    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        size = 4096;
        memset(buffer, 0, size);
        s = env_->Get(key, buffer, &size);
        if (s.ok()) {
            printf("%dth key is exisit\n", i);
        }
    }

#if 0
    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        ASSERT_OK(env_->Delete(key));
    }
    env_->Flush(kWriteFlush);
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "Delete execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);


    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        snprintf(buffer, 4096, "value%d", i);
        Slice value(buffer, 4096);
        ASSERT_OK(env_->Put(key, value, true));
    }
    ASSERT_OK(env_->Flush(kWriteFlush));
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "PutAsync with Flush All execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);


    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        size = 4096;
        ASSERT_OK(env_->Get(key, NULL, &size, true));
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "ReadAhread execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);

    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        size = 4096;
        memset(buffer, 0, size);
        ASSERT_OK(env_->Get(key, buffer, &size));
        //ASSERT_EQ(size, 4096);
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "Get After ReadAhread execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);

    ASSERT_OK(env_->DiscardPrefetch());

    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        key.db_id = 0x19191234;
        key.seq = i;
        ASSERT_OK(env_->Delete(key));
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    iops = 1000000/(microsec/kReqCount);
    fprintf(stderr, "Delete execution time(%lu) : numIo(%d) : IOps(%lf)\n", microsec, kReqCount, iops);

    start_time = env_->NowMicros();
    for (i = 0; i < kReqCount; i++) {
        ASSERT_OK(env_->Flush(kWriteFlush));
    }
    end_time = env_->NowMicros();
    microsec = end_time - start_time;
    fprintf(stderr, "Flush execution time(%lu) : numIo(%d)\n", microsec, kReqCount);

    start_time = env_->NowSecond();
    fprintf(stderr, "Flush current_time time(%lu)\n", start_time);


    char *mem = NULL;
    ASSERT_TRUE(0 == posix_memalign((void **)&mem, 4096, 4096));
    key.seq = 9000001;
    size = 4096;
    memset(mem, 0, size);
    sprintf(mem, "%s", "test value posix 1234");
    Slice value(mem, 4096);
    fprintf(stderr, "before pmem writing %s\n", mem);
    ASSERT_OK(env_->Put(key, value));
 
    key.seq = 9000001;
    size = 4096;
    memset(mem, 0, size);
    fprintf(stderr, "before pmem reading %s\n", mem);
    ASSERT_OK(env_->Get(key, mem, &size));
    fprintf(stderr, "after pmem reading %s\n", mem);




    key.seq = 9000000;
    size = 4096;
    memset(buffer, 0, size);
    sprintf(buffer, "%s", "test value 1234");
    Slice value2(buffer, 4096);
    fprintf(stderr, "before writing %s\n", buffer);
    ASSERT_OK(env_->Put(key, value2));
 
    key.seq = 9000000;
    size = 4096;
    memset(buffer, 0, size);
    fprintf(stderr, "before reading %s\n", buffer);
    ASSERT_OK(env_->Get(key, buffer, &size));
    fprintf(stderr, "after reading %s\n", buffer);
    key.seq = 9000003;
    fprintf(stderr, "delete non exist key %lu\n", key.seq);
    ASSERT_OK(env_->Delete(key));
    fprintf(stderr, "after deleting key %lu\n", key.seq);
#endif

    env_->Close();
}

}  // namespace insdb

int main(int argc, char** argv) {
  return insdb::test::RunAllTests();
}
