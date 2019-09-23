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
#include <stdint.h>
#include <string>
#include <deque>
#include <set>
#include <boost/lockfree/queue.hpp>
#include "insdb/db.h"
#include "insdb/kv_trace.h"
#include "db/db_impl.h"
#include "insdb/env.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "port/port.h"
#include "port/port_posix.h"
#include "port/thread_annotations.h"
#include "util/crc32c.h"


namespace insdb {
#ifdef KV_TIME_MEASURE
    cycles_t msec_value;
    cycles_t usec_value;
    cycles_t nsec_value;
    cycles_t cycle_value;
    std::atomic<uint64_t> p[TRACE_BUF_SIZE];
#endif
    /**
      InSDB Internals.

      Key-Map:
      InSDB builds internal Key-Map, which set a 16B Internal key for user key.
      Internal key privides flexibility for KVSSD.
      1. It provides mechanism to support multiple database with KVSSD, portion of Internal key
      is used to identify database (DB id).
      2. It provides mechanism to repair InDB for power failure, each internal value has next
      internal key, and it gives a way to trace key-value pairs.

      [user key, user value] ---> 
      [internal key, internal value( next internal key, user key, user value)]
      */


    WriteBatchInternal* WBIBuffer::GetWBIBuffer(DBImpl *dbimpl, Manifest *mf, int32_t count, uint64_t ttl) {
        WriteBatchInternal *wbi;
        wbi = reinterpret_cast<WriteBatchInternal *>(pthread_getspecific(pool_id_));
        if (wbi) wbi->InitWBI(dbimpl, mf, count, ttl);
        else{
            wbi = new WriteBatchInternal(dbimpl, mf, count, ttl);;
            pthread_setspecific(pool_id_, wbi);
        }
        return wbi;
    }



    /**
      DBImpl constructor.

      build DBImpl instance and set member.
      */

    template<class T, class V>
        static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
            if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
            if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
        }

    Options SanitizeOptions(const Options& src, const std::string& dbname) {
        Options result = src;
        if (result.info_log == NULL) {
            std::string log_name = dbname + ".insdb_";
            Status s = result.env->NewLogger(log_name, &result.info_log);
            if (!s.ok()) {
                result.info_log = NULL;
            }
        }
        ClipToRange<int,int>(&result.num_write_worker, 1, 1024);
        ClipToRange<int,int>(&result.num_column_count, 1, 1024);
        ClipToRange<int,int>(&result.max_request_size, 1024, 1024*1024*2);
        ClipToRange<int,int>(&result.max_key_size, 16, 1024);
        ClipToRange<int,int>(&result.max_table_size, 4096, 2*1024*1024);
        ClipToRange<size_t,size_t>(&result.max_uval_prefetched_size, 1024, 64*1024*1024*1024UL);
        ClipToRange<size_t,size_t>(&result.max_uval_iter_buffered_size,1024, 1024*1024*1024*1024UL);
        ClipToRange<size_t,size_t>(&result.flush_threshold, 128, 64*1024*1024*1024UL);
        return result;
    }

    DBImpl::DBImpl(const Options& options, const std::string& dbname) :
        options_(SanitizeOptions(options, dbname)), 
        env_(options.env), 
        dbname_(dbname), 
        mf_(NULL),
        wbi_buffer_(),
        snaplist_(),
        /**
         *The default size of the prefetch window is 1, which doubles up to 64 if hit, and half to 1 if not.
         */
        slowdown_(false),
        slowdown_mu_(),
        slowdown_cv_(&slowdown_mu_),
        pending_req_(),
        congestion_control_(false),
        congestion_control_mu_(),
        congestion_control_cv_(&congestion_control_mu_),
        pending_callbacks_(0),
        nr_pending_callbacks_(0),
        callback_mu_(),
        callback_cv_(&callback_mu_),
        callback_cf_flush_timers_(nullptr),
        callback_file_sizes_(nullptr),
        callback_shutdown_cv_(&callback_mu_),
        callback_pending_flush_timers_(0),
        bgwork_shutting_down_(false),
        shutting_down_(false),
        db_exist_(false),
        normal_start_(false)
        {
#ifdef KV_TIME_MEASURE
            for(int i = 0 ; i < TRACE_BUF_SIZE; i++)
                p[i] = 0;
            time_init(&msec_value, &usec_value, &nsec_value, &cycle_value);
            cache_hit_ = 0;
            prefetch_hit_ = 0;
            cache_miss_ = 0;
#endif
#if 0 
#ifdef KV_TIME_MEASURE
            /** New & Reuse memory performance comparison */
            std::string *t1 = new std::string();
            cycles_t st, e;
            uint32_t trace = 0;

            st = get_cycles();
            t1->append(dbname_);
            for(int i = 0 ; i < 128 ; i++)
                t1->append(128, 'a'+i);
            e = get_cycles();
            p[0].fetch_add(time_cycle_measure(st,e));
            printf("1 crc 0x%x(time : %ld)\n", crc32c::Value(t1->data(), t1->size()), p[0].load());
            int sz =  dbname_.size();
            st = get_cycles();
            memcpy((void*)((char*)t1->data()), dbname_.data(), sz);
            for(int i = 0 ; i < 128 ; i++, sz+=128)
                memset((void*)((char*)t1->data()+sz), 'c'+i, 128);
            e = get_cycles();
            p[1].fetch_add(time_cycle_measure(st,e));
            printf("2 crc 0x%x(time : %ld)\n", crc32c::Value(t1->data(), t1->size()), p[1].load());
            t1->resize(0);
            st = get_cycles();
            t1->append(dbname_);
            for(int i = 0 ; i < 128 ; i++)
                t1->append(128, 'b'+i);
            e = get_cycles();
            p[2].fetch_add(time_cycle_measure(st,e));
            printf("3 crc 0x%x(time : %ld)\n", crc32c::Value(t1->data(), t1->size()), p[2].load());
#endif
#endif
#if defined(TRACE_MEM) && defined(HAVE_JEMALLOC)
            fprintf(stderr,"DB constructor\n");
            // Dump allocator statistics to stderr.
            malloc_stats_print(NULL, NULL, NULL);
#endif

            // Workaround to remove stale keys in readahead buffer.
            // TODO: need to remove stale keys belonging to the current DB
            (Env::Default())->DiscardPrefetch();

            mf_=  new Manifest(options_, dbname_);
#if 0
            env_->StartThread(&Manifest::PrefetchWrapper, mf_);
            while(!mf_->pf_thread_started_.load());
#endif
            for(int i = 0 ; i < kWriteWorkerCount ; i++){
                env_->StartThread(&DBImpl::WorkerWrapper, this);
            }
            while(kWriteWorkerCount != mf_->GetWorkerCount());
            assert(kWriteWorkerCount == mf_->GetWorkerCount());

            // start callback thread if needed
            if(options_.AddUserKey)
                env_->StartThread(&DBImpl::CallbackSender, this);
        }

    DBImpl::~DBImpl(){
#if defined(TRACE_MEM) && defined(HAVE_JEMALLOC)
        fprintf(stderr, "DB destructor\n");
        // Dump allocator statistics to stderr.
        malloc_stats_print(NULL, NULL, NULL);
#endif

#ifdef KV_TIME_MEASURE
        int j = 0;
        int print = 0;
        bool first_print = true;

        for(int i = 0 ; i < TRACE_BUF_SIZE; i++){
            if(p[i].load()){
                if(first_print){
                    printf("########################################################################################################################################\n");
                    first_print = false;
                }

                printf("  Section  %4d |", i);
                print++;
            }
            if(print==8){
                printf(" \n");
                for(; j <= i; j++){
                    if(p[j].load()){
                        if(cycle_to_msec(p[j].load()) && j > 10)
                            printf("& %9llu &Msec |", cycle_to_msec(p[j].load()));
                        else if(cycle_to_usec(p[j].load()) && j > 10)
                            printf("& %9llu &usec |", cycle_to_usec(p[j].load()));
                        else
                            printf("& %14lu &noseccycle|", p[j].load());
                    }
                }
                printf(" \n........................................................................................................................................\n");
                print = 0;
            }
        }
        if(!first_print){
            printf(" \n");
            print = 0;
            for(; j < TRACE_BUF_SIZE; j++){
                if(p[j].load()){
                    if(cycle_to_msec(p[j].load()) && j > 10)
                        printf("& %9llu &Msec |", cycle_to_msec(p[j].load()));
                    else if(cycle_to_usec(p[j].load()) && j > 10)
                        printf("& %9llu &usec |", cycle_to_usec(p[j].load()));
                    else
                        printf("& %14lu &|", p[j].load());
                    print++;
                }
            }
            printf("\n########################################################################################################################################\n\n");
            first_print = false;
        }
        printf("prefetch hist %lu\n",prefetch_hit_.load());
        printf("Cache hit %lu\n",cache_hit_.load());
        printf("Cache miss %lu\n",cache_miss_.load());
#endif
#ifdef INSDB_GLOBAL_STATS
        printf(stderr, "DB destructor\n");
        printGlobalStats("before manifest free");
#endif
        mf_->CleanUpSnapshot();

        mf_->WorkerLock();
        shutting_down_ = true; // Any non-NULL value is ok
        mf_->SignalAllWorkerThread();
        mf_->WorkerUnlock();
        mf_->WorkerTerminationWaitSig();
        assert(mf_->GetUncompletedTrxnGroup().size() == 0);
        mf_->TerminateKeyCacheThread();
        assert(pending_req_.empty());
        mf_->TerminateLogCleanupThread();

        /* Clean up key list and move SKTable from clean cache to dirty cache if the SKTable has been dirty*/
        assert(!mf_->PopSKTableMem(kCleanSKTCache));
        assert(!mf_->PopSKTableMem(kDirtySKTCache));
        assert(mf_->IsSKTDirtyListEmpty());
#if 0
        mf_->pf_shutdown_ = true;
        mf_->pf_worker_cv_.Signal();
        while(mf_->pf_thread_started_.load());
#endif
        if(options_.AddUserKey)
            callback_cv_.SignalAll();
        //env_->WaitForJoin();

        //if (normal_start_) {
        mf_->SetNormalStart(normal_start_);
        //}
        delete mf_;
        if (options_.info_log)
            delete options_.info_log;
#ifdef INSDB_GLOBAL_STATS
        printGlobalStats("after manifest free");
#endif
    }

    /** Implementations of the DB interface */
    /**
      Inside this function, it will call DB::Put(),
      and DB::Put() will call DBImpl::Write().
      */

    Status DBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value)
    {
        return DB::Put(options, key, value);
    }

    /**
      Inside this function, it will Call DB::Delete(),
      and DB::Delete() will call DBImpl::Write().
      */
    Status DBImpl::Delete(const WriteOptions& options, const Slice& key)
    {
        return DB::Delete(options, key);
    }


    /**
      From updates(WriteBatch*), this function parses Put/Delete requests
      by calling update->Iterator with WriteBatchInternal handler.
      WriteBatchInternal convert requests as KVctx, which is internal request
      context for InsDB.

      For each Parsed request, Write() get/generate internal keys, internal key
      for this request and next internal_key, next internal key saved for
      tracking key-value pair in case of power failure.

      In case user value size is huge, it need to be splits multiple sub internal
      key-value pairs.

      To tracking the changes of write worker thread's count, SKTable node keeps
      next internal keys for each write worker. In case # of write worker threads
      increased, we need to track those change even if power failure case. So
      first request after this changes, save next ikeys for newly added worker
      thread in extnede worker internal key fields.

      In case WriteBatch has multiple put/delete requests, InSDB treats those as
      a single transaction. To support transanction within power failure, we also
      add internal key link(previous internal key and next internal key) in the
      internal key value. see detail KVctx class and design document..

      After build internal key, It build key-map by building IKeyNode and KVCache,
      and update Key-map in the SKTableNode.

      And then submit request to write worker thread.
      */
    Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates)
    {
        Status s;
        /**
         * Congestion Control has been relocated to PutColumnNodeToRequestQueue()
         */

        uint64_t ttl = Env::Default()->NowSecond();
        /* Use per thread memory pool for WriteBatchInternal & initialize it */
        WriteBatchInternal *handler = GetWBIBuffer(updates->Count(), ttl);
        updates->Iterate(handler);
#ifdef INSDB_GLOBAL_STATS
        if(options.ttl)
            g_api_write_ttl_cnt++;
#endif
        /**
         * If sync option is on, wait until the last command completes.
         */
#if 0
        if(options.sync){
            InSDBKey k = {0, 0};
            ColumnNode* ikey = handler->GetLastColumnNode();
            if (ikey) {
                ikey->Lock();
                k = GetInSDBKey(mf_->GetDBHash(), ikey->GetInSDBKeySeqNum());
                k.split_seq = ((ikey->GetiValueSize()+kMaxRequestSize-1)/kMaxRequestSize) - 1; //last sequence number
                ikey->Unlock();
            }
            s = Env::Default()->Flush(kWriteFlush, k);
        }
#endif
        return s;
    }

    /* NOTE THAT the sequence number can be 1 if the key has not been updated since db was started. */
    Status DBImpl::GetLatestSequenceForKey(const Slice& key, uint64_t* sequence, bool* found, uint16_t col_id) {
        Status s;
        *sequence = kMaxSequenceNumber;
        uint64_t cur_time = Env::Default()->NowSecond();
        *found = false;
        KeySlice hkey(key); 
        UserKey *uk = mf_->FindUserKeyFromHashMap(hkey);

        if(uk){
            uk->ColumnLock(col_id);

            bool deleted = false;
            ColumnNode* col_node = uk->GetColumnNode(col_id, deleted, cur_time, *sequence);
            if (!col_node){ 
                /* in case of snapshot get */
                uk->ColumnUnlock(col_id);
                uk->DecreaseReferenceCount();
                if(uk->IsFetched()) 
                    return Status::NotFound("key does not exit");
                uk = NULL;
            }else{
                *sequence = col_node->GetBeginSequenceNumber();
                //Fix bug: The write conflict detection can not return correct status
                //Set the found mark as true when get the specified key
                *found = true;
                //Fix end
                uk->ColumnUnlock(col_id);
                uk->DecreaseReferenceCount();
                return s;
            }
        }
        /* "NO else" because uk can be NULL after checking the seq #*/
        if(!uk){
            if(mf_->GetLatestSequenceUsingSKTDF(hkey, col_id, sequence, cur_time)){
                *found = true;
            }else
                s = Status::NotFound("key does not exit");
        }
        return s;

    }

    uint64_t DBImpl::GetLatestSequenceNumber() const {
        return mf_->GetLastSequenceNumber();
    }

    // Cleanup function for PinnableSlice
    void PinnableSlice_UserValue_CleanupFunction(void* arg1, void* arg2)
    {
        UserValue* uv = reinterpret_cast<UserValue*>(arg1);
        Manifest* mf = reinterpret_cast<Manifest*>(arg2);
        mf->FreeUserValue(uv);
        //delete reinterpret_cast<std::string *>(arg1);
#ifdef INSDB_GLOBAL_STATS
        g_unpinslice_uv_cnt++;
#endif
    }
    void PinnableSlice_KBPM_CleanupFunction(void* arg1, void* arg2)
    {
        KeyBlockPrimaryMeta* kbpm = reinterpret_cast<KeyBlockPrimaryMeta*>(arg1);
        Manifest* mf = reinterpret_cast<Manifest*>(arg2);
        if(kbpm->DecRefCntAndTryEviction(mf))
            mf->FreeKBPM(kbpm);

    }


    /**
      If snapshot in options is set, Get() read value from snapshot.
      if not, It will read value through key-map hiaraches.

      Manifest --> SKTables --> [UKeyNode -->]IKeyNode --> KVCache.

      If SKTable is not loaded, Get() will fill sktable from storage.
      */
    Status DBImpl::Get(const ReadOptions& options,
            const Slice& key, std::string* value, uint16_t col_id, PinnableSlice* pin_slice) {
        Status s;
        SequenceNumber sequence = 0;
        uint64_t cur_time = 0;
        KeySlice hkey(key); 

        UserKey *uk = NULL;
        if(options.snapshot){
            SnapshotImpl* snap = reinterpret_cast<SnapshotImpl*>(const_cast<Snapshot*>(options.snapshot));
            assert(snap);

            sequence = snap->GetCurrentSequenceNumber(); 
            cur_time = snap->GetCurrentTime();
        }else{
            sequence = ULONG_MAX; /* Max Number */
            cur_time = Env::Default()->NowSecond();
        }
        uk = mf_->FindUserKeyFromHashMap(hkey);

        //return Status::NotFound("key does not exist");
        if(uk){
            uk->ColumnLock(col_id);

            UserValue* uv = NULL;
            bool deleted = false;
            ColumnNode* col_node = uk->GetColumnNode(col_id, deleted, cur_time, sequence, GetTtlFromTtlList(col_id));
            if (!col_node){ 
                /* in case of snapshot get */
                uk->ColumnUnlock(col_id);
                uk->DecreaseReferenceCount();
                if(uk->IsFetched()) 
                    return Status::NotFound("key does not exit");
                uk = NULL;
            }else{
                if((uv = col_node->GetUserValue())){
                    if(value){ 
                        value->assign(uv->GetBuffer(), uv->GetValueSize());
                        //mf_->FreeUserValue(uv); /* GetValue Pin uv inside funciton, so we will decrease Pin and give a chance to free */
                    }else {
                        uv->Pin();
                        pin_slice->PinSlice(uv->ReadValue(), PinnableSlice_UserValue_CleanupFunction, uv, mf_);
                    }
                }else{
                    Slice value_slice;
                    KeyBlockPrimaryMeta *kbpm = mf_->GetValueUsingColumn(col_node, value_slice);
                    if(value){ 
                        value->assign(value_slice.data(), value_slice.size());
                        //mf_->FreeUserValue(uv); /* GetValue Pin uv inside funciton, so we will decrease Pin and give a chance to free */
                        if(kbpm->DecRefCntAndTryEviction(mf_))
                            mf_->FreeKBPM(kbpm);
                    }else {
                        pin_slice->PinSlice(value_slice, PinnableSlice_KBPM_CleanupFunction, kbpm, mf_);
                    }
                }
                uk->ColumnUnlock(col_id);
                uk->DecreaseReferenceCount();
            }
        }
        /* "NO else" because uk can be NULL after checking the seq #*/
        if(!uk){
            Slice value_slice;
            KeyBlockPrimaryMeta *kbpm = mf_->GetValueUsingSKTDF(hkey, value_slice, col_id, sequence, cur_time);
            if(!kbpm)
                return Status::NotFound("key does not exit");
            if(value){ 
                value->assign(value_slice.data(), value_slice.size());
                //mf_->FreeUserValue(uv); /* GetValue Pin uv inside funciton, so we will decrease Pin and give a chance to free */
                if(kbpm->DecRefCntAndTryEviction(mf_))
                    mf_->FreeKBPM(kbpm);
            }else {
                pin_slice->PinSlice(value_slice, PinnableSlice_KBPM_CleanupFunction, kbpm, mf_);
            }
        }
        return s;
    }


    Status DBImpl::Get(const ReadOptions& options,
            const Slice& key, PinnableSlice* value, uint16_t col_id) {
        Status s = Get(options, key, NULL, col_id, value);
        return s;
    }

    Status DBImpl::CompactRange(const Slice *begin, const Slice *end, const uint16_t col_id)
    {
        Status s;
#if 0
        SequenceNumber sequence = 0;
        sequence = mf_->GetLastSequenceNumber();
#endif

        Slice begin_key = *begin;
        Slice end_key = *end;
        UserKey* ukey = NULL;
        UserKey *end_ukey = NULL;
        if(!begin) ukey = mf_->GetFirstUserKey();
        else ukey = mf_->SearchUserKey(begin_key);
        if(!ukey)
            return Status::NotFound("key does not exit");
        if(!end) end_ukey = mf_->GetLastUserKey();
        else end_ukey = mf_->SearchLessThanEqualUserKey(end_key);

        if(!end_ukey){
            ukey->DecreaseReferenceCount();
            return Status::NotFound("key does not exit");
        }

        assert(ukey);
        assert(end_ukey);
        //assert(end_ukey->Compare(ukey->GetKey()) >= 0);

        UserKey *next_ukey = NULL;
        while(ukey){
            ukey->ColumnLock(col_id);
            bool deleted = false;
            ColumnNode *col_node = ukey->GetColumnNode(col_id, deleted);
            if (col_node){ 
#if 0
                ukey->GetCurrentSKTableMem()->SetForceFlush();
#endif
                ukey->SetDirty(); // SetForceFlush() @ EvictUserKeys()
                col_node->DropColumnNode();
            }
            ukey->ColumnUnlock(col_id);
            next_ukey = ukey->NextUserKey(mf_);
            ukey->DecreaseReferenceCount();
            if(ukey == end_ukey)
                break;
            ukey = next_ukey;
        }
        /* The end_ukey ref count has been increased 2 times.*/
        end_ukey->DecreaseReferenceCount();

        return s;
    }

    Status DBImpl::Flush(const FlushOptions& options,
            uint16_t col_id)
    {
        return Env::Default()->Flush(kWriteFlush);
    }

    /**
      Iterator use same mechanism with snapshot. But it is not managed by database.
      When Iterator disappears it need to destory snapshot class.
      see SnapshotIterator class.

      Snapshot and Iterator need to wait until pending request is done at the given time.
      */
    Iterator* DBImpl::NewIterator(const ReadOptions& options, uint16_t col_id)
    {
        return mf_->CreateNewIterator(options, col_id);
    }
    /**
      Snapshot provide database image in calling time. Same as Iterator, It will
      not cover put/delete operation which are issued after this method call.

      GetSnapshot() create snapshot instance and save on snaplist_, and then
      return this instance.
      */
    const Snapshot* DBImpl::GetSnapshot()
    {
        return mf_->CreateSnapshot();
    }

    /**
      This function release snapshot resouce from database.
      it remove snapshot from snaplist_.
      */

    void DBImpl::ReleaseSnapshot(const Snapshot* snapshot)
    {
        mf_->DeleteSnapshot(const_cast<Snapshot*>(snapshot));
    }

    /**
      reproted property will be described below..
      */
    bool DBImpl::GetProperty(const Slice& property, std::string* value)
    {
        return false;
    }

    /**
Note: It provides size information for give #n ranges. assume sizes are
pointer for uint64_t[] array.
*/
    void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes, uint16_t col_id)
    {
#if 1
        uint64_t count_table;
        uint32_t approx_sktable_keyvalue_size = (kMaxRequestSize/options_.approx_key_size) * // Max number of keys
            (options_.approx_key_size + options_.approx_val_size); // key + value size
        for (int i = 0; i < n; i++) {
            // Get key count from table list
            count_table = mf_->GetEstimateNrSKTable(range[i].limit);
#if DEBUG
            printf("approx sizes[%d] table count limit %lu\n", i, count_table);
#endif
            count_table -= mf_->GetEstimateNrSKTable(range[i].start);
#if DEBUG
            printf("approx sizes[%d] table count after start %lu\n", i, count_table);
#endif

            // Sum table-based and keymap-based key counts
            // Add 1 to the count to prevent zero size
            sizes[i] = ( count_table + 1 ) * approx_sktable_keyvalue_size;
#if DEBUG
            printf("approx sizes[%d] %lu\n", i, sizes[i]);
#endif
        }
#else
        for (int i = 0; i < n; i++) {
            sizes[i] = 1000000 * options_.avg_approx_keyval_size;
        }
#endif
    }

    void DBImpl::PutColumnNodeToRequestQueue(RequestNode *req_node, SKTableMem *skt){

        if((pending_req_.size()+1)>= kSlowdownTrigger || slowdown_){
#ifdef CODE_TRACE
            printf("[%d :: %s]Queue wait %lu %u\n", __LINE__, __func__, pending_req_.size(), kSlowdownTrigger);
#endif
            MutexLock l(&slowdown_mu_);
            slowdown_ = true;
            slowdown_cv_.Wait();
        }
        mf_->WorkerLock();
        pending_req_.push(req_node);
        mf_->WorkerUnlock();
#if 0
        /* if skt is not NULL, the UserKey was created in Store() */
        while(skt && skt->IsFlushing() && skt->GetApproximateDirtyColumnCount() >= (kUpdateCntForForceTableFlush<<1)){
#ifdef CODE_TRACE
            printf("[%d :: %s]SKT wait \n", __LINE__, __func__);
#endif
            MutexLock l(&congestion_control_mu_);
            congestion_control_ = true;
            congestion_control_cv_.Wait(); 
        }
#endif

        if(pending_req_.size() > (kSlowdownTrigger >> 3))
            mf_->SignalWorkerThread(false);
    }

    void DBImpl::BuildKeyBlockMeta(int worker_id, std::list<RequestNode*> &submit_list, KeyBlockMetaLog* kbml, std::queue<SKTableMem*> *flush_skt)
    {
        UserKey *uk = NULL;

        uint8_t del_kb_offset = 0;
        /* || value size(21 bit) || key size (11 bit) ||*/
        /* the type of "auto it" is "RequestNode*" */ 
        for(auto it = submit_list.begin()  ; it != submit_list.end() ; it++){
            uk = (*it)->GetRequestNodeUserKey();
            ColumnNode *col_node = (*it)->GetRequestNodeLatestColumnNode();
            uint8_t col_id = (*it)->GetRequestNodeColumnID();
            assert(uk);
            uk->ColumnLock(col_id);
            /* 
             * If the table is being flushed, GetCurrentSKTableMem() may have either main SKTable or new(sub) SKTable.
             * 1. If uk belongs to sub-SKTable but table is main, it can have two situation.
             *    (a) The UK hsa not yet been scanned in FlushSKTableMem().
             *        In this case, The FlushSKTableMem() will scan it soon.
             *    (b) The uk range was already scanned in FlushSKTableMem()(uk is inserted after scan). 
             *        In this case, FlushSKTableMem() can not increase the key_count.
             * 2. If the uk is already scanned in FlushSKTableMem, it has the correct table it which the uk belongs.
             *    because FlushSKTableMem() can change the uk->sktable.
             * 
             * For case 1(a). It does not need to increase sktable->key_count even if it is new.
             *                FlushSKTableMem() will increase the key_count
             * For case 1(b). If uk is new, the key_count must be increased at here
             * For case 2.    FlushSKTableMem() already increase the key_count for the uk.
             *                Therefore, it does not need to increase key_count even if uk is new.
             */

            /*
             * Add merged columns to KeyBlockMetaLog 
             * Cleanup merged columns. 
             */
            assert(kDelType == col_node->GetRequestType());
            uk->DoVerticalMerge((*it), col_id, worker_id, del_kb_offset, mf_, kbml);
            /* KeyBlock must ONLY contain Put requests */
            SKTableMem *skt = mf_->FindSKTableMem(uk->GetKeySlice());
            if(!skt->IsFlushing()){
                if(skt->IncApproximateDirtyColumnCount() >= kUpdateCntForForceTableFlush){
                    flush_skt->push(skt);
                }
            }
            /**
             * The SKTableMem should be exist in one of cache.
             * If SKTableMem had not been cache in Store(), the user sent Prefetch request & insert to dirty cache.
             * If SKTableMem is in Clean cache, migrate to Dirty Cache
             *  - Cache Evictor may do this to avoid atomic variable check.
             */
#ifndef NDEBUG
            mf_->SKTableCacheMemLock();
            assert((skt->GetSKTableDeviceFormatNode()) || skt->SetFetchInProgress());
            assert(skt->IsInSKTableCache(mf_) || skt->IsFlushOrEvicting());
            mf_->SKTableCacheMemUnlock();
#endif
            mf_->MigrateSKTableMemCache(skt);

            col_node->SetKeyBlockOffset(del_kb_offset++);
            uk->ColumnUnlock(col_id);

#ifdef CODE_TRACE
            printf("[%d :: %s] Dec ref for del\n", __LINE__, __func__);
#endif
            uk->DecreaseReferenceCount();

        }
    }

    /*
     * Evict User Value Caches & Device Format SKTable
     **/
    uint32_t DBImpl::BuildKeyBlock(int worker_id, std::list<RequestNode*> &submit_list, KeyBlock* kb, KeyBlockMetaLog* kbml, uint8_t put_cnt, char *buf, uint32_t  buf_size, std::queue<SKTableMem*> *flush_skt)
    {
        /** Get ColumnNode from the Q */
        UserKey *uk = NULL;
        UserValue *uv = NULL;
        uint8_t col_id = 0;
        /* || value size(21 bit) || key size (11 bit) ||*/
        uint32_t value_sz_and_key_sz = 0;
        /* Keyblock Memory Format
         * ** KV info : (Key & value size | User Value | User Key | Col ID | (TTL))
         * < 1st KV Ofs | 2en KV Ofs | ... | Last KV Ofs | Total Size of KV | 1st KV info | 2ed KV Info | ... | Last KV Info >
         * ^^^^^^^^^^^^^^ =>  "offset" starts at here.                      ^^^^^^^^^^^^^^^ => "kv_buffer" start at here. 
         * ^ => "buf" points the begining of the KeyBlock buffer 
         */
        uint32_t *offset = (uint32_t *)(buf);
        /*
         * the total size is used of last kv size( total size - last kv offset)
         * the other kv size = next kv offset - current kv offset
         */
        char *kv_buffer = (char*)(offset + (put_cnt+1/*We use 4B for the total size */));
        uint32_t data_size = 0;

        uint8_t del_kb_offset = 0;
        uint8_t put_kb_offset = 0;
        /* || value size(21 bit) || key size (11 bit) ||*/
        /* the type of "auto it" is "RequestNode*" */ 
        for(auto it = submit_list.begin()  ; it != submit_list.end() ; it++){
            assert( ((uint64_t)kv_buffer - (uint64_t)buf) < 0x100000/*1M*/);
            /* Save offset of current KV */
            EncodeFixed32((char*)offset , ((uint64_t)kv_buffer - (uint64_t)buf));
            uk = (*it)->GetRequestNodeUserKey();
            ColumnNode *col_node = (*it)->GetRequestNodeLatestColumnNode();
            col_id = (*it)->GetRequestNodeColumnID();
            assert(uk);
            /* Column lock may not be needed */ 
            uk->ColumnLock(col_id);
            /* 
             * If the table is being flushed, GetCurrentSKTableMem() may have either main SKTable or new(sub) SKTable.
             * 1. If uk belongs to sub-SKTable but table is main, it can have two situation.
             *    (a) The UK hsa not yet been scanned in FlushSKTableMem().
             *        In this case, The FlushSKTableMem() will scan it soon.
             *    (b) The uk range was already scanned in FlushSKTableMem()(uk is inserted after scan). 
             *        In this case, FlushSKTableMem() can not increase the key_count.
             * 2. If the uk is already scanned in FlushSKTableMem, it has the correct table it which the uk belongs.
             *    because FlushSKTableMem() can change the uk->sktable.
             * 
             * For case 1(a). It does not need to increase sktable->key_count even if it is new.
             *                FlushSKTableMem() will increase the key_count
             * For case 1(b). If uk is new, the key_count must be increased at here
             * For case 2.    FlushSKTableMem() already increase the key_count for the uk.
             *                Therefore, it does not need to increase key_count even if uk is new.
             */
            RequestType type = col_node->GetRequestType();

            /*
             * Add merged columns to KeyBlockMetaLog 
             * Cleanup merged columns. 
             */
            uk->DoVerticalMerge((*it), col_id, worker_id, ((type == kPutType) ? put_kb_offset : del_kb_offset), mf_, kbml);
            uk->ColumnUnlock(col_id);
            /* KeyBlock must ONLY contain Put requests */
            SKTableMem *skt = mf_->FindSKTableMem(uk->GetKeySlice());
            if(!skt->IsFlushing()){
                if(skt->IncApproximateDirtyColumnCount() >= kUpdateCntForForceTableFlush){
                    flush_skt->push(skt);
                }
            }
            /**
             * The SKTableMem should be exist in one of cache.
             * If SKTableMem had not been cache in Store(), the user sent Prefetch request & insert to dirty cache.
             * If SKTableMem is in Clean cache, migrate to Dirty Cache
             *  - Cache Evictor may do this to avoid atomic variable check.
             */
#ifndef NDEBUG
            mf_->SKTableCacheMemLock();
            assert((skt->GetSKTableDeviceFormatNode()) || skt->IsFetchInProgress());
            assert(skt->IsInSKTableCache(mf_) || skt->IsFlushOrEvicting());
            mf_->SKTableCacheMemUnlock();
#endif
            mf_->MigrateSKTableMemCache(skt);

            if(type == kDelType){
                col_node->SetKeyBlockOffset(del_kb_offset++);
#ifdef CODE_TRACE
                printf("[%d :: %s] Dec ref for del\n", __LINE__, __func__);
#endif
                uk->DecreaseReferenceCount();
                continue;
            }
            col_node->SetKeyBlockOffset(put_kb_offset++);
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            /* The reference count was increased in WriteBatchInternal::Store()*/
            uv = col_node->GetUserValue();
            assert(uv);
            value_sz_and_key_sz = uv->GetValueSize() << KEYBLOCK_VALUE_SIZE_SHIFT;
            /* now we have | CRC reserved | Varint32(v&k sz) | ==>  |*/

            KeySlice key = uk->GetKeySlice();
            assert(key.size() < 2048);// key size uses only 11 bits.
            value_sz_and_key_sz |= key.size();
            kv_buffer = EncodeVarint32(kv_buffer, value_sz_and_key_sz);

            assert(uv->GetBuffer());

            uint64_t ts = col_node->GetTS();

            if(ts){
                *offset |= 1 << KEYBLOCK_TTL_SHIFT;
                data_size = (uv->GetValueSize() + key.size() + 1/*Column ID size*/ + 8/*sizeof(ttl)*/);
            }else
                data_size = (uv->GetValueSize() + key.size() + 1/*Column ID size*/);

            /* The size of uval and key are not 0 in most cases*/
            memcpy(kv_buffer, uv->GetBuffer(), data_size);
            /* The counter was increased in WriteBatchInternal::Store()*/
#ifdef CODE_TRACE
            printf("[%d :: %s] Dec ref for put\n", __LINE__, __func__);
#endif
            uk->DecreaseReferenceCount();
            /* Next buffer */
            kv_buffer += data_size;
            offset++;
            /*If this is last uk, the offset has total size of the iValue*/
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        }
        uint32_t total_size = (uint32_t)(kv_buffer - buf);
        EncodeFixed32((char*)offset, total_size);
        assert( total_size < buf_size);

        return total_size;

    }

    void DBImpl::InsertSKTableMemCacheWrapper(SKTableMem* flush_candidate, uint32_t cur_update_cnt){
        if(cur_update_cnt  || mf_->IsNextSKTableDeleted(flush_candidate->GetSKTableSortedListNode()) ){
            flush_candidate->SetPrevDirtyColumnCount(cur_update_cnt);
#ifdef CODE_TRACE
            printf("[%d :: %s]%p(%d) to Dirty \n", __LINE__, __func__, flush_candidate, flush_candidate->GetSKTableID());
#endif
            mf_->InsertSKTableMemCache(flush_candidate, kDirtySKTCache, true/*Clear Flush Flag*/);//Insert it to dirty cache list
        }else{
            flush_candidate->SetPrevDirtyColumnCount(0/*cur_update_cnt*/);
#ifdef CODE_TRACE
            printf("[%d :: %s]%p(%d) to Clean \n", __LINE__, __func__, flush_candidate, flush_candidate->GetSKTableID());
#endif
            mf_->InsertSKTableMemCache(flush_candidate, kCleanSKTCache, true/*Clear Flush Flag*/, true/* wake cache evictor */);
        } /* In this case the SKTable has been deleted */
    }

    struct DeviceFormatKeyBlock{
        InSDBKey ikey;
        char *data;
        uint32_t size;
    }; 
    void DBImpl::WorkerThread()
    {
        /** fetch_add returns old value */
        Status s;
        uint16_t worker_id = mf_->IncWorkerCount();
#if 0
        SKTCacheNode *cache_node = NULL;
#endif
        RequestNode* req_node;
        mf_->IncRunningWorkerCount();

        uint8_t col_id;
        ColumnNode* latest_col_node;
        RequestType type;
        std::list<RequestNode*> submit_list;
        uint16_t merge_trxn_cnt[kNrRequestType][kMaxRequestPerKeyBlock];

        uint64_t next_ikey_seq_num = 0;
        uint64_t cur_ikey_seq_num = 0;
        std::queue<SKTableMem*> flush_skt_queue;
        SKTableMem* flush_candidate = NULL;
        uint32_t cur_update_cnt = 0;
        SKTableDFBuilderInfo dfb_info = SKTableDFBuilderInfo(mf_);
        uint32_t max_value_size = 512;
        uint32_t ivalue_size = 0;
        uint32_t force_submit = 0;

        /* 
         * Keyblock Device Format
         * ** KV info : (Key & value size | User Value | User Key | Col ID | (TTL))
         * < CRC(4B) | Total # of Keys(1B) : Total Size of Compressed KB(3B) | (compressed or uncompressed)KeyBlock data >
         *           <--------------------------------------------    CRC    -------------------------------------------->
         */
        char* kb_dev_buffer = (char*)malloc(kDeviceRquestMaxSize);
        char* kb_crc_loc = kb_dev_buffer;
        uint32_t* kb_keycnt_size_loc = (uint32_t*)(kb_crc_loc + 4);
        char* kb_buffer = (char*)(kb_keycnt_size_loc + 1)/*CRC(4B) + # of Keys(1B) + Size of compressed KB(3B)*/;
        char* kbm_dev_buffer = (char*)malloc(kDeviceRquestMaxSize);
        while(true){

            //This lock is also realsed out of while(true);
            //mf_->RefillFreeUserValueCount(max_value_size);
            mf_->WorkerLock();
            /** Wait for a signal */
            while(!shutting_down_ && (pending_req_.empty() /*|| (sleep_force &&  nr_pending_req_< kMaxRequestPerKeyBlock)*/) /*&& mf_->IsSKTDirtyListEmpty()*/){
                force_submit = 0;
                mf_->DecRunningWorkerCount();
                bool timeout = mf_->WorkerWaitSig();
                mf_->IncRunningWorkerCount();
                // periodic coldness check in dirty cache
                if (timeout && !mf_->IsSKTDirtyListEmpty()) break;
                /* Shuttinn down DB
                 * if the pending request queue is not empty, process remained ColumnNode before shutdown
                 * mf_->WorkerUnlock will be performed in MutexLock destructor.
                 */
            }

            if(shutting_down_ && pending_req_.empty() && mf_->IsSKTDirtyListEmpty())
                break;
#define KEY_BLOCK_SIZE_APPENDIX 12 // Offset ( 2 ) + CRC ( 4 ) + keyvalue size ( 5 ) + Column ID ( 1 )
#define KEY_BLOCK_TOTAL_SIZE 4
            /*
               Additional memory per key (11B) = CRC(4B) + user key/value size(4B) + Column ID(1B) + Offset and flag(2B) (+ TTL(8B) ??) 
               the data will be compressed. So, actual data size must be less than estimated size.
               that's why we don't add TTL size to the estimated size
               */
            if( pending_req_.size() >= kMaxRequestPerKeyBlock || (!(force_submit++ & 0xFFFF)&& !pending_req_.empty())){
                uint32_t estimated_ivalue_size  = 2 + KEY_BLOCK_TOTAL_SIZE;/* The total number of keys in the KeyBlock(Max 32) + total kb size*/
                uint8_t count[kNrRequestType] = { 0 }; // Max 32
                while(!pending_req_.empty()){
                    req_node = pending_req_.front();
                    UserKey *uk = req_node->GetRequestNodeUserKey();
                    uk->ColumnLock(req_node->GetRequestNodeColumnID());
                    latest_col_node = req_node->GetRequestNodeLatestColumnNode();
                    if(!latest_col_node) latest_col_node = uk->GetLatestColumnNode(req_node->GetRequestNodeColumnID());
                    type = latest_col_node->GetRequestType();
                    if(type == kPutType){
                        uint32_t ivalue_size = (latest_col_node->GetUserValue()->GetValueSize() + uk->GetKeySize() +
                                        KEY_BLOCK_SIZE_APPENDIX + (TtlIsEnabled(req_node->GetRequestNodeColumnID())?8:0));
                        if(ivalue_size > max_value_size) max_value_size = ivalue_size;
                        estimated_ivalue_size += ivalue_size;
                        if(estimated_ivalue_size > kMaxInternalValueSize && (count[kPutType] || count[kDelType]) ){
                            estimated_ivalue_size -= ivalue_size;
                            /*if one value exceeds kMaxInternalValueSize, the uk should be written to the device */
                            uk->ColumnUnlock(req_node->GetRequestNodeColumnID());
                            force_submit = 0;
                            break;
                        }
                    }
                    if(!req_node->GetRequestNodeLatestColumnNode()){
                        assert(latest_col_node);
                        assert(uk->GetRequestNode(req_node->GetRequestNodeColumnID()) == req_node);
                        req_node->SetRequestNodeLatestColumnNode(latest_col_node);
                        uk->SetRequestNode(req_node->GetRequestNodeColumnID(), NULL);
                    }
                    pending_req_.pop();

                    /*merge_trxn_cnt[][] can have 0 if there are no merged trxns exist*/
                    if(latest_col_node->IsTransaction()){
                        /*Exclude TRXN header's memory in the KBML*/
                        merge_trxn_cnt[type][count[type]++] = req_node->GetTrxnCount();// - 1; /* Do not allocate memory for the Header of Vertical Merge Group*/
                    }else{
                        assert(!req_node->GetTrxnCount());
                        merge_trxn_cnt[type][count[type]++] = 0;//req_node->GetTrxnCount(); /* GetTrxnCount does not contain header's trxn because the header is non-trxn request. */
                    }
                    assert(worker_id);
                    latest_col_node->SetInSDBKeySeqNum(worker_id); // Worker ID must be started from "1" 
                    latest_col_node->ClearTGIDSplit();//Clear it here because it already set ikey_seq_num.
                    uk->ColumnUnlock(req_node->GetRequestNodeColumnID());
                    submit_list.push_back(req_node);
                    /* "kMaxRequestPerKeyBlock(32)" is the number of bit in uint32_t. We use the bits in "uint32_t" to check valid Key in the Key Block */
                    if(count[kPutType] == kMaxRequestPerKeyBlock || count[kDelType] == kMaxRequestPerKeyBlock){
                        force_submit = 0;
                        break;
                    }
                }
                mf_->WorkerUnlock();
                if( slowdown_ && (pending_req_.size() < kSlowdownLowWaterMark)){
                    MutexLock l(&slowdown_mu_);
                    slowdown_= false;
#ifdef CODE_TRACE
                    printf ("[%u] resume IO %lu %u\n", worker_id, pending_req_.size(), kSlowdownLowWaterMark);
#endif
                    slowdown_cv_.SignalAll();
                }
#ifdef CODE_TRACE
                //if(count[kPutType] != kMaxRequestPerKeyBlock && count[kDelType] != kMaxRequestPerKeyBlock)
                printf("[%d :: %s]put cnt : %d || del cnt : %d\n", __LINE__, __func__, count[kPutType] , count[kDelType]);
#endif
                if(pending_req_.size() >= kMaxRequestPerKeyBlock && mf_->GetRunningWorkerCount() < mf_->GetWorkerCount() )
                    mf_->SignalWorkerThread();
#ifdef TRACE_MEM
                g_io_cnt++;
                if (!(g_io_cnt % 10000)) {
                    mf_->PrintParam();
                    printf("IO [%lu], pending req(nr col)[%u]\n\n", g_io_cnt.load(), pending_req_.size());
                }
#endif

                estimated_ivalue_size+=4;/*Last size*/
                if(estimated_ivalue_size % kRequestAlignSize) estimated_ivalue_size = SizeAlignment(estimated_ivalue_size);
                /* Following memories should be allocated using memory-pool */
                KeyBlock *kb = NULL;
                if (count[kPutType]) {
                    kb = mf_->AllocKB();
                    if (submit_list.empty()) abort();
                }
                KeyBlockPrimaryMeta* kbpm = mf_->AllocKBPM();
                kbpm->Init(count[kPutType], count[kDelType], kb);
                KeyBlockMetaLog* kbml = mf_->AllocKBML();
                next_ikey_seq_num = mf_->GenerateNextInSDBKeySeqNum();
                kbml->Init(next_ikey_seq_num, kbpm, count, merge_trxn_cnt);
                kbpm->SetKBML(kbml);

                uint32_t ivalue_size = 0;

                if (count[kPutType]) {
                    kb->Init(estimated_ivalue_size);
                    ivalue_size = BuildKeyBlock(worker_id, submit_list, kb, kbml, count[kPutType], const_cast<char*>(kb->GetRawData().data()), estimated_ivalue_size, &flush_skt_queue);
                    if (!ivalue_size) abort();
                    if (ivalue_size > estimated_ivalue_size) abort();
                    assert(ivalue_size <= estimated_ivalue_size);
                }else{
                    /* For only delete block*/
                    BuildKeyBlockMeta(worker_id, submit_list, kbml, &flush_skt_queue);
                }
                /* Build & submit KBM Device format */
                //Estimates the size of Device Format KBM
                assert(SizeAlignment(kbml->GetEstimatedBufferSize()) <= kDeviceRquestMaxSize);
                /*Insert KBML to Log Queue and get cur_ikey_seq_num */
                cur_ikey_seq_num = mf_->PushKBMLog(worker_id, kbml, next_ikey_seq_num);
                kbpm->SetInSDBKeySeqNum(cur_ikey_seq_num);
                uint32_t kbm_size = kbml->BuildDeviceFormatKeyBlockMeta(kbm_dev_buffer);
                assert(kbm_size <= kbml->GetEstimatedBufferSize());
                InSDBKey kbm_key = GetKBKey(mf_->GetDBHash(), kKBMeta, cur_ikey_seq_num);
                s= env_->Put(kbm_key, Slice(kbm_dev_buffer, SizeAlignment(kbm_size)), true);
                assert(s.ok());
                if (kb) {
                    assert(ivalue_size <= kDeviceRquestMaxSize);
                    assert(count[kPutType] <= kMaxRequestPerKeyBlock );
                    /* Submit KeyBlock*/
                    *kb_keycnt_size_loc = ((uint32_t)count[kPutType] << KEYBLOCK_KEY_CNT_SHIFT);
#ifdef HAVE_SNAPPY
                    if(options_.compression == kSnappyCompression){ 

                        /* data->size() has been aligned */
                        size_t outlen = 0;
                        //snappy::MaxCompressedLength(length);

                        if(!ivalue_size)
                            abort();

                        snappy::RawCompress(kb->GetRawData().data(), (size_t)ivalue_size, kb_buffer, &outlen);

                        /*
                         * compressed less than 12.5%, just store uncompressed form 
                         * This ratio is the same as that of LevelDB/RocksDB
                         *
                         * Regardless of the compression efficiency, We may not have any penalty even if we store compressed data.
                         * But read have to decompress the data.
                         */
                        if(outlen < (ivalue_size-(ivalue_size / 8u))){
                            assert(outlen <= kDeviceRquestMaxSize);
                            *kb_keycnt_size_loc |= (KEYBLOCK_COMP_MASK | outlen);
                            ivalue_size = outlen;
                        }else{
                            *kb_keycnt_size_loc |= ivalue_size;
                            memcpy(kb_buffer, kb->GetRawData().data(), ivalue_size);
                        }
                    }else
#endif 
                    {
                        *kb_keycnt_size_loc |= ivalue_size;
                        memcpy(kb_buffer, kb->GetRawData().data(), ivalue_size);
                    }

                    uint32_t crc = crc32c::Value((char*)kb_keycnt_size_loc, (*kb_keycnt_size_loc & KEYBLOCK_KB_SIZE_MASK) + 4/* 4 is for "kb_keycnt_size_loc" */); 
                    EncodeFixed32((char*)kb_crc_loc, crc32c::Mask(crc)); //Append the CRC info.
                    ivalue_size+= 8;/*CRC(4) & CNT(1) & SIZE(3)*/
#ifdef CODE_TRACE
                    printf("[%d :: %s] worker %u KeyBlock size : %lu: %lu (ivalue_size : %u) Put: %u\n", __LINE__, __func__, worker_id, SizeAlignment(kb->GetRawData().size()), kb->GetRawData().size(), ivalue_size, count[kPutType]);
#endif
                    InSDBKey kb_key = GetKBKey(mf_->GetDBHash(), kKBlock, cur_ikey_seq_num);
                    //s = env_->Put(kb_key, Slice(kb->GetRawData().data(), SizeAlignment(kb->GetRawData().size())), true);
                    s = env_->Put(kb_key, Slice(kb_dev_buffer, SizeAlignment(ivalue_size)), true);
                    assert(s.ok());
                    mf_->IncreaseMemoryUsage(kb->GetSize());
                    kbpm->SetKBSize(ivalue_size);
                }
                /*To update ColumnNode, ivalue size & ikey */
                for(auto it = submit_list.begin(); it != submit_list.end(); ){
                    req_node = (*it);
                    UserKey *uk = req_node->GetRequestNodeUserKey();
                    ColumnNode *col_node = req_node->GetRequestNodeLatestColumnNode();
                    col_id = req_node->GetRequestNodeColumnID();
#ifdef CODE_TRACE
                    printf("[%d :: %s](submit)(ref : %d)col_node addr %p || type : %s\n", __LINE__, __func__,  uk->GetReferenceCount(), col_node, col_node->GetRequestType() == kPutType ? "Put" : "Del");
#endif
                    TrxnGroupID tgid = req_node->GetTrxnGroupID();
                    if(tgid)
                        mf_->DecTrxnGroup(tgid, 1);/* AddCompletedTrxnGroupRange() is called in DecTrxnGroup() */
                    uk->ColumnLock(col_id);
                    /*Free UserValue in the UpdateColumnNode()*/
                    col_node->UpdateColumnNode(mf_, ivalue_size, cur_ikey_seq_num, (KeyBlockMeta*)kbml);
                    uk->ColumnUnlock(col_id);
                    mf_->FreeRequestNode(req_node);
                    it = submit_list.erase(it);
                }

                /*Insert KBPM to Hash*/
                if (!mf_->InsertKBPMToHashMap(kbpm)) abort();
                assert(submit_list.empty());

                while(!flush_skt_queue.empty()){
                    flush_candidate = flush_skt_queue.front();
                    flush_skt_queue.pop();
                    /*the flush_candidate can be either the dirty cache list or the clean cache list*/
                    if(mf_->RemoveFromSKTableMemDirtyCache(flush_candidate)){
                        /* The other WorkerThread may flushed the SKTable */
                        if(flush_candidate->GetApproximateDirtyColumnCount() >= kUpdateCntForForceTableFlush && !flush_candidate->GetSKTRefCnt()){
                            if(!flush_candidate->GetSKTableDeviceFormatNode()){
                                flush_candidate->LoadSKTableDeviceFormat(mf_);
                            }
                            /* sktdf must not be in df cache */
                            flush_candidate->FlushSKTableMem(mf_, worker_id, &dfb_info);
                        }
                        if(!flush_candidate->IsDeletedSKT()){
                            if(!flush_candidate->GetColdness())
                                flush_candidate->SetColdness(mf_->GetColdnessSeed());
                            InsertSKTableMemCacheWrapper(flush_candidate, flush_candidate->GetApproximateDirtyColumnCount());
                        }else{
                            flush_candidate->ClearFlushing();
                            assert(!flush_candidate->IsInSKTableCache(mf_));
                        }
                        if( congestion_control_){
                            MutexLock l(&congestion_control_mu_);
                            congestion_control_= false;
                            congestion_control_cv_.SignalAll();
                        }
                    }
                }
            }else{
                mf_->WorkerUnlock();
            }
            if( slowdown_ && (pending_req_.size() < kSlowdownLowWaterMark)){
                MutexLock l(&slowdown_mu_);
                slowdown_= false;
                slowdown_cv_.SignalAll();
            }

#if 1
            uint32_t scanned = mf_->SKTCountInSKTableMemCache(kDirtySKTCache);
            bool flushed = false;
            if(scanned > 16) scanned = 16;

            do{
                if((flush_candidate = mf_->PopSKTableMem(kDirtySKTCache))){
                    cur_update_cnt = flush_candidate->GetApproximateDirtyColumnCount();
                    if(!flush_candidate->GetSKTRefCnt() && ( cur_update_cnt >= kMinUpdateCntForTableFlush || 
                            (cur_update_cnt && cur_update_cnt == flush_candidate->GetPrevDirtyColumnCount() && !flush_candidate->DecColdness()) ||
                            mf_->IsNextSKTableDeleted(flush_candidate->GetSKTableSortedListNode()) || shutting_down_)) {
                        if(!flush_candidate->GetSKTableDeviceFormatNode()){
                            flush_candidate->LoadSKTableDeviceFormat(mf_);
                        }
                        /* sktdf must not be in cache */
                        flush_candidate->FlushSKTableMem(mf_, worker_id, &dfb_info);
                        flushed = true;
                    }
                    if(!flush_candidate->IsDeletedSKT()){
                        if(!flush_candidate->GetColdness())
                            flush_candidate->SetColdness(mf_->GetColdnessSeed());
                        InsertSKTableMemCacheWrapper(flush_candidate, flush_candidate->GetApproximateDirtyColumnCount());
                    }else{
                        flush_candidate->ClearFlushing();
                        assert(!flush_candidate->IsInSKTableCache(mf_));
                    }

                    if( congestion_control_){
                        MutexLock l(&congestion_control_mu_);
                        congestion_control_= false;
                        congestion_control_cv_.SignalAll();
                    }

                }else
                    break; // No more SKTable exists in the dirty list
            }while(!flushed && --scanned /* && mf_->GetCountSKTableCleanList() < 4 if the number of clean SKTables is not enough */ );
#endif

            if( congestion_control_){
                MutexLock l(&congestion_control_mu_);
                congestion_control_= false;
                congestion_control_cv_.SignalAll();
            }
        }
        mf_->SignalAllWorkerThread();
        mf_->WorkerUnlock();
        mf_->CleanUpCleanSKTable();
        mf_->SignalKeyCacheThread();
        do{
            while((flush_candidate = mf_->PopSKTableMem(kDirtySKTCache))){
                if(flush_candidate){
                    if(!flush_candidate->GetSKTableDeviceFormatNode()){
                        flush_candidate->LoadSKTableDeviceFormat(mf_);
                    }
                    flush_candidate->FlushSKTableMem(mf_, worker_id, &dfb_info);
                    if( congestion_control_){
                        MutexLock l(&congestion_control_mu_);
                        congestion_control_= false;
                        congestion_control_cv_.SignalAll();
                    }
                    if(!flush_candidate->IsDeletedSKT()){
                        InsertSKTableMemCacheWrapper(flush_candidate, flush_candidate->GetApproximateDirtyColumnCount());
                    }else{
                        flush_candidate->ClearFlushing();
                        assert(!flush_candidate->IsInSKTableCache(mf_));
                    }
                }
            }
        }while(mf_->GetCountSKTableCleanList() || mf_->GetCountSKTableDirtyList() || mf_->IsKeyCacheThreadBusy());
        if(!mf_->DecWorkerCount()){
            mf_->CheckForceFlushSKT(worker_id, &dfb_info);
            mf_->SetWorkerTermination();
            mf_->SignalWorkerTermination();
        }
        assert(kb_dev_buffer);
        assert(kbm_dev_buffer);
        free(kb_dev_buffer);
        free(kbm_dev_buffer);
        //printf("Exit WorkerThread %d\n", worker_id);
    }


    CallbackReq::~CallbackReq() {
        if(UserKey_.data()) delete [] UserKey_.data();
        if(UserValue_) mf_->FreeUserValue(UserValue_);
    }

    void DBImpl::CallAddUserKey(
            UserKey *key,
            UserValue *value,
            RequestType type,
            uint64_t seq,
            uint16_t col_id,
            bool new_key) {

        // Filter out put type for an existing key and del type for a non-existing key
        if ((options_.AddUserKey == nullptr) || (type == kPutType && !new_key) || (type == kDelType && new_key))
            return;

        // Copy key name
        KeySlice key_slice = key->GetKeySlice();
        auto keybuf = new char[key_slice.size()];
        memcpy(keybuf,key_slice.data(), key_slice.size());
        Slice key_copy(keybuf, key_slice.size());

        // Pin user value
        value->Pin();

        // Push a request to callback thread
        // keybuf and value Pin will be released in req destructor
        auto req = new CallbackReq(key_copy, value, type, seq, col_id, new_key, this, mf_);
        pending_callbacks_.push(req);
        if (nr_pending_callbacks_.fetch_add(1) == 0)
            callback_cv_.Signal();
    }

    void DBImpl::CallbackThread(){
        callback_cf_flush_timers_ = new uint64_t[options_.num_column_count]{0};
        callback_file_sizes_ = new uint64_t[options_.num_column_count]{0};

        while(!(shutting_down_ || bgwork_shutting_down_)){
            bool timeout = false;

            callback_mu_.Lock();
            /** Wait for a signal */
            while(!(shutting_down_ || bgwork_shutting_down_) && nr_pending_callbacks_.load()==0)
            {
                if(callback_pending_flush_timers_)
                {
                    uint64_t start_time = env_->NowMicros();
                    // Flush timeout is 1sec
                    timeout = callback_cv_.TimedWait(start_time + 1000000);
                    if(timeout)
                        break;
                }
                else
                {
                    callback_cv_.Wait();
                }
            }
            callback_mu_.Unlock();
            // Check flush period per column family
            if(options_.Flush && timeout)
            {
                for(uint16_t cfid=0; cfid<options_.num_column_count; cfid++)
                {
                    if(callback_cf_flush_timers_[cfid])
                    {
                        // disable the timer
                        callback_cf_flush_timers_[cfid] = 0;
                        // call flush callback
                        Status status = options_.Flush(cfid,options_.FlushContext);
                        assert(status.ok());
                    }
                    callback_file_sizes_[cfid] = 0;
                    callback_pending_flush_timers_--;
                }
            }
            /** Get ColumnNode from the Q */
            pending_callbacks_.consume_all([](CallbackReq *req) {
                    DBImpl *db = req->DBImpl_;
                    db->nr_pending_callbacks_.fetch_sub(1);

                    enum EntryType type;
                    /* GetDataSize returns user data size not IO buffer size*/
                    uint32_t infile_size = req->UserKey_.size() + req->UserValue_->GetValueSize()/*It was GetDataSize()*/ + 40 /* metadata */;
                    switch(req->Type_)
                    {
                    case kPutType:
                    if(req->new_key_)
                    {
                    type = kEntryPut;
                    db->callback_file_sizes_[req->col_id_] += infile_size;
                    }
                    else
                    {
                    type = kEntryOther;
                    }
                    break;
                    case kDelType:
                    type = kEntryDelete;
                    db->callback_file_sizes_[req->col_id_] += infile_size;
                    break;
                    default: type = kEntryOther; break;
                    }

                    //Slice uv_slice(*req->UserValue_->GetData());
                    Slice uv_slice = req->UserValue_->ReadValue();
                    Status status = db->options_.AddUserKey(
                            req->UserKey_, uv_slice, type, req->Sequence_, db->callback_file_sizes_[req->col_id_], req->col_id_, db->options_.AddUserKeyContext);
                    assert(status.ok());

                    // enable flush timeout if not enabled.
                    if(db->callback_cf_flush_timers_[req->col_id_] == 0)
                    {
                        db->callback_cf_flush_timers_[req->col_id_] = db->env_->NowMicros();
                        db->callback_pending_flush_timers_++;
                    }

                    delete req;
            });
        }
        // clean up the queue
        pending_callbacks_.consume_all([](CallbackReq *req) {
                req->DBImpl_->nr_pending_callbacks_--;
                delete req;
                });
        delete [] callback_file_sizes_;
        delete [] callback_cf_flush_timers_;
        callback_shutdown_cv_.SignalAll();
    }

    void DBImpl::CancelAllBackgroundWork(bool wait)
    {
        bgwork_shutting_down_ = true;  // Any non-NULL value is ok
        if(options_.AddUserKey)
        {
            callback_cv_.SignalAll();
            if(wait)
            {
                callback_mu_.Lock();
                callback_shutdown_cv_.Wait();
                callback_mu_.Unlock();
            }
        }
    }

    /**
      Create NewDB, it must be called when need to New DB inserted.
      Initialize manifest structure by calling BuildSuperSKTable().
      */
    Status DBImpl::NewDB() {
        return mf_->CreateNewInSDB();
    }

    Status DBImpl::DeleteDB() {
        return mf_->DeleteInSDB();
    }

    /**
      Recover DB(), it read DB meta and build internal on memory data structure for DB.
      It parsing existing manifest or supper sktable, and then initialize database.
      */
    Status DBImpl::Recover(const Options& options) {
        Status s = mf_->GetSuperSKTable();
        if (s.IsNotFound()) {
            return s; // DB does not exist.
        } else {
            db_exist_ = true;
            if (options.error_if_exists) return Status::OK(); //do not load sktable .. it will report error. 
            s = mf_->RecoverInSDB();
            if(s.ok() || s.IsNotFound())
                s = mf_->InitManifest();
            else if (!s.ok()) {//fail to recover.
                printf("failed to recover %s\n", s.ToString().data());
                return s;
            }
            if (!s.ok())
                return s;
        }
        return Status::OK();
    }

    /* check parameter validation if super sktable exist */
    Status DBImpl::ValidateOption() {
        Status s = mf_->GetSuperSKTable();
        if (!s.ok() && !s.IsNotFound()) {
            /* there is mismatched parameter */
            return s;
        }
        return Status::OK();
    }

    Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
        WriteBatch batch;
        batch.Put(key, value);
        return Write(opt, &batch);
    }

    Status DB::Delete(const WriteOptions& opt, const Slice& key) {
        WriteBatch batch;
        batch.Delete(key);
        return Write(opt, &batch);
    }

    DB::~DB() { }

    std::string kv_ssd("/dev/nvme0n1");

    Status DB::OpenWithTtl(const Options& options, const std::string& name,
                    DB** dbptr, uint64_t* TtlList, bool* TtlEnabledList) {
      Status s = DB::Open(options, name, dbptr);
      if (!s.ok()) {
        return s;
      }
      return (*dbptr)->SetTtlList(TtlList, TtlEnabledList);
    }

    /**
      This method initialize database and return databas instance.
      */
    Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
        *dbptr = NULL;
        kv_ssd.assign(options.kv_ssd);
        Status s = options.env->Open(kv_ssd.c_str());
        if (!s.ok()) {
            return Status::InvalidArgument("Fail to open device");
        }
        DBImpl* impl = new DBImpl(options, dbname);
        assert(impl);

        /* validation check */
        s = impl->ValidateOption();
        if (!s.ok()) {
            delete impl;
            return s;
        }

        s = impl->Recover(options); // try to get super sktable information, and then read manifest or recover.
        if (!s.ok()) {
            if (impl->Is_DB_exist() == false) { // no db, based on option create or return error.
                if (options.create_if_missing) {
                    s = impl->NewDB(); // create new db, create super sktable and firt sktbale.
                    if (!s.ok()) { 
                        delete impl;
                        return Status::IOError(dbname, "fail to create db (create if missing)");
                    }
                } else {
                    delete impl;
                    return Status::InvalidArgument(dbname, "db does not exists (create_if_missing is false)");
                }
            } else { // db exist .... but currupted ... recovery failed.
                delete impl;
                return Status::Corruption(dbname, "db exists but corrupted");
            }
        } else if (options.error_if_exists) { // expect db not exist. 
            delete impl;
            return Status::InvalidArgument(dbname, "db exists (error_if_exists is true)");
        }
        impl->SetNormalStart();
        *dbptr = impl;
        return Status::OK();
    }

    Snapshot::~Snapshot() {
    }


    /**
      DestroyDb need to remove all key-value pair, It is very
      expensive operation.
      */
    Status DestroyDB(const std::string& dbname,
            const Options& options)
    {
        kv_ssd.assign(options.kv_ssd);
        Status s = options.env->Open(kv_ssd.c_str());
        if (!s.ok()) {
            return Status::InvalidArgument("Fail to open device");
        }
        DBImpl* impl = new DBImpl(options, dbname);
        assert(impl);
        s = impl->DeleteDB(); // try to get super sktable information, and then read manifest or recover.
        if (!s.ok()) {
            /* ignor error */
        }
        delete impl;
        return s;
    }
    /**
      RepariDB result can be incomplete database images.
      Maybe some transaction will be gone.
      */
    Status RepairDB(const std::string& dbname,
            const Options& options)
    {
        kv_ssd.assign(options.kv_ssd);
        Status s = options.env->Open(kv_ssd.c_str());
        if (!s.ok()) {
            return Status::InvalidArgument("Fail to open device");
        }
        DBImpl* impl = new DBImpl(options, dbname);
        assert(impl);
        s = impl->Recover(options); // try to get super sktable information, and then read manifest or recover.
        if (s.ok()) impl->SetNormalStart(); //build manifest...
        delete impl;
        return s;
    }

} //namespace insdb
