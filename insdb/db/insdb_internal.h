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


#ifndef STORAGE_INSDB_INTERNAL_H_
#define STORAGE_INSDB_INTERNAL_H_
#include <limits.h>
#include <semaphore.h>
#include <stdint.h>
//#include <iostream>
//#include <string>
#include <unordered_map>
#include <map>
#include <set>
#include <vector>
#include <list>
#include <deque>
#include <queue>
#include <mutex>
#include <shared_mutex>
#include <algorithm>
#include <boost/lockfree/queue.hpp> 
#include <boost/lockfree/spsc_queue.hpp>
#include "insdb/db.h"
#include "insdb/kv_trace.h"
#include "db/skiplist2.h"
#include "db/skiplist.h"
#include "db/spinlock.h"
#include "db/dbformat.h"
#include "db/keyname.h"
#include "db/skldata.h"
#include "insdb/status.h"
#include "insdb/list.h"
#include "util/crc32c.h"
#include "util/arena.h"
#include "db/keymap.h"
//#define USE_HOTSCOTCH
#ifdef USE_HOTSCOTCH
#include "tsl/hopscotch_map.h"
#include "tsl/hopscotch_set.h"
#endif
#include <folly/SharedMutex.h>
#ifdef INSDB_USE_HASHMAP
#include <folly/concurrency/ConcurrentHashMap.h>
#endif
#define NEW_GENERIC_FIRMWARE
#define REFCNT_SANITY_CHECK
//#define BUILDITR_INCLUDE_DIRECTVALUE
namespace insdb {

    // Max Column Family count
    extern int kMaxColumnCount;
    // Write Worker count
    extern int kWriteWorkerCount;
    // Max request size
    extern uint32_t kMaxRequestSize;
    //  Max Key Size
    extern int kMaxKeySize;
    //  Key Count Per SKTable
    extern int kMaxSKTableKeytCnt;
    extern uint32_t kSlowdownTrigger;
    extern uint32_t  kSlowdownLowWaterMark;


    // Cleanup function for PinnableSlice
    void PinnableSlice_UserValue_CleanupFunction(void* arg1, void* arg2);
    void PinnableSlice_ColumnNode_CleanupFunction(void* arg1, void* arg2);
    void PinnableSlice_SKTable_CleanupFunction(void* arg1, void* arg2);

#if 0
    class KeyMap;
#endif
    class UserKey;
    class UserValue;
    class ColumnNode;
    class SKTableMem;
    class Manifest;
    class SnapshotImpl;

    class KeyBlock;
    class KeyBlockPrimaryMeta;

    class UVAllocatorPool;
    class UKAllocatorPool;
    class RequestNodeAllocatorPool;

    class TrxnInfo;

    struct SuperSKTable {
        std::string dbname;
        std::string compName;
        uint16_t max_column_count;
        uint16_t max_req_per_kb;
        uint32_t max_request_size;
        uint32_t max_sktable_size;
        uint32_t max_internalkey_size;
        uint32_t first_skt_id; 
    };

    typedef uint64_t TransactionID;
    //Support 128 Billions(1 << (32+5)) TRXN per DB Open(Intialized to 1 << TGID_SHIFT )
    typedef uint32_t TrxnGroupID; 
    struct SKTableInfo {
        uint32_t skt_id;
        uint64_t next_skt_id;
        uint64_t prealloc_skt_id;
        SKTableInfo():skt_id(0), next_skt_id(0), prealloc_skt_id(0) { }
    };
    struct InSDBThreadsState {
        port::Mutex mu;
        int num_running;
        InSDBThreadsState() : mu(), num_running(0) {}
    };

    struct SKTMetaInfo {
        uint32_t skt_id;
        uint32_t sktdf_size;
        uint32_t hash_size;
    };

    struct DeleteSKTableCtx {
        Manifest* mf;
        struct InSDBThreadsState * state;
        SKTMetaInfo *skt_ids;
        uint32_t num_skt;
    };

    struct UserKeyComparator {
        const Comparator *user_comparator;
        explicit UserKeyComparator(const Comparator *c) : user_comparator(c) {}
        int operator()(const Slice&, const Slice&) const;
    };

    struct Transaction {
        Transaction(): id(0), seq_number(0), count(0){}
        TransactionID id;
        uint32_t count;
        uint32_t seq_number;/* Start from 0 */
    };
    static_assert (sizeof(Transaction) == 16, "Invalid Transaction size");
    struct TrxnGroupRange{ 
        TrxnGroupRange(TrxnGroupID id) : start(id),
        end(id) { }  
        TrxnGroupRange(TrxnGroupID start_, TrxnGroupID end_) : start(start_), end(end_) { }
        TrxnGroupID start;
        TrxnGroupID end;
    };
    enum InsertColumnNodeStatus{
        kFailInsert = 0,
        kNeedToSchedule = 1,
        kHasBeenScheduled = 2,
    };

    template <class T>
        class ObjectAllocator{
            private:
                Spinlock lock_;
                std::queue<T*> queue_;
            public:
                ObjectAllocator(): lock_(), queue_(){ }
                ~ObjectAllocator(){
                    T* obj;
                    while(!queue_.empty()){
                        obj = queue_.front();
                        queue_.pop();
                        delete obj;
                    }
                }
                T* Alloc(uint32_t size = 0){
                    T* obj;
                    if(!queue_.empty() && !lock_.TryLock()){
                        if (!queue_.empty()) {
                            obj = queue_.front();
                            queue_.pop();
                            lock_.Unlock();
                        } else {
                            lock_.Unlock();
                            obj = new T(size);
                            obj->SetAllocator(this);
                        }
                    }else{
                        obj = new T(size);
                        obj->SetAllocator(this);
                    }
                    return obj;
                }
#if 0
                void Free(T* obj, uint32_t max = 0){
                    bool del = false;
                    lock_.Lock();
                    if(!max || queue_.size() < max)
                        queue_.push(obj);
                    else
                        del = true;
                    lock_.Unlock();
                    if(del) delete obj;
                }
#endif
                void Free(T* obj, uint32_t max = 0){
                    bool del = false;
                    T* del_obj;
                    lock_.Lock();
                    queue_.push(obj);
                    if(max && queue_.size() >=  max) {
                        del_obj = queue_.front();
                        queue_.pop();
                        del = true;
                    }
                    lock_.Unlock();
                    if(del) delete del_obj;
                }

        };

    template <class T>
        class SimpleObjectAllocator{
            private:
                Spinlock lock_;
                std::queue<T*> queue_;
            public:
                SimpleObjectAllocator(): lock_(), queue_(){ }
                ~SimpleObjectAllocator(){
                    T* obj;
                    while(!queue_.empty()){
                        obj = queue_.front();
                        queue_.pop();
                        delete obj;
                    }
                }
                T* Alloc(){
                    T* obj;
                    if(!queue_.empty() && !lock_.TryLock()){
                        if (!queue_.empty()) {
                            obj = queue_.front();
                            queue_.pop();
                            lock_.Unlock();
                        } else {
                            lock_.Unlock();
                            obj = new T();
                            obj->SetAllocator(this);
                        }
                    }else{
                        obj = new T();
                        obj->SetAllocator(this);
                    }
                    return obj;
                }
#if 0
                void Free(T* obj, uint32_t max = 0){
                    bool del = false;
                    lock_.Lock();
                    if(!max || queue_.size() < max)
                        queue_.push(obj);
                    else
                        del = true;
                    lock_.Unlock();
                    if(del) delete obj;
                }
#endif
                void Free(T* obj, uint32_t max = 0){
                    bool del = false;
                    T* del_obj;
                    lock_.Lock();
                    queue_.push(obj);
                    if(max && queue_.size() >=  max) {
                        del_obj = queue_.front();
                        queue_.pop();
                        del = true;
                    }
                    lock_.Unlock();
                    if(del) delete del_obj;
                }
        };

    enum RequestType {
        kPutType = 0,
        kDelType = 1,
        kNrRequestType = 2,
    };

    /* The RequestNode is used to insert a user request into the pending request queue.*/
    class RequestNode{
        /* Free RequestNode Registration */
        private:
            uint8_t col_id_;
            ColumnNode *latest_col_node_;
            std::string trxn_;
            uint16_t trxn_cnt_;// merged trxn cnt + head(1)
            ObjectAllocator<RequestNode>* allocator_;
        public:
            RequestNode(ObjectAllocator<RequestNode>* allocator):col_id_(0), latest_col_node_(NULL), allocator_(allocator){}
            RequestNode(uint32_t size):col_id_(0), latest_col_node_(NULL), allocator_(NULL){}
            void RequestNodeInit(uint8_t col_id){
                col_id_ = col_id;
                latest_col_node_ = NULL;
            }

            uint8_t GetRequestNodeColumnID(void){ return col_id_; }

            void InsertTrxn(Transaction &trxn, RequestType type){
                trxn_cnt_++;
                PutVarint64(&trxn_, trxn.id);
                PutVarint32(&trxn_, trxn.seq_number);
                PutVarint32(&trxn_, trxn.count);
            }
            /* The first one is Vertical Group Header's trxn*/
            std::string* GetTrxn(void){
                return &trxn_;
            }

            uint16_t GetTrxnCount(){
                return trxn_cnt_;
            }

            void UpdateLatestColumnNode(Manifest *mf, uint32_t kb_size, uint64_t ikey_seq, KeyBlockPrimaryMeta *kbpm);

            void SetRequestNodeLatestColumnNode(ColumnNode* col_node){
                latest_col_node_ = col_node;
            }
            ColumnNode* GetRequestNodeLatestColumnNode(){
                return latest_col_node_;
            }
            void SetAllocator(ObjectAllocator<RequestNode>* allocator){ allocator_ = allocator; }
            ObjectAllocator<RequestNode>* GetAllocator(){ return allocator_; }
    };

    enum PrefetchDirection {
        kNoDir = 0,
        kPrev = 1,
        kNext = 2,
        kEndType = 3,
    };

#define SizeAlignment(size) ((size + (kRequestAlignSize-1)) & (~(kRequestAlignSize-1)))

    enum ColumnStatus{
        kValidColumn = 0,
        kIterColumn= 1,
        kInvalidColumn= 2,
    };

    struct DeleteInSDBCtx {
        uint32_t next_skt_id;
        uint32_t prealloc_skt_id;
    };

    struct TransactionColCount {
        uint32_t count;
        uint32_t completed;
    };

    struct ColumnInfo {
        std::string key;
        RequestType type;
        SequenceNumber ikey_seq;
        uint64_t ttl;
        TrxnInfo *trxninfo;
        KeyBlockPrimaryMeta* kbpm;
        uint8_t kb_index;
        uint32_t kb_size;

    };

    // log record key
    //
    // Format in little endian
    // Header:
    //   CRC ( uint32 )
    //   Format flags ( uint16 )
    //     0x8000 the rest of format is snappy-compressed
    //     0~0x7fff padding size in bytes
    //   Reserved ( 2 bytes )
    //
    // Body:
    //   number of deleted internal keys ( uint32 )
    //     note that this is fixed size, not varint32 because total number can be obtained after appending all workers deleted ikeys in one pass.
    //   internal key sequence numbers ( varint64 * number of deleted internal keys )
    //
    //   number of workers ( varint32 )
    //   worker's next internal key sequence number ( varint64 * number of workers )
    //
    //   number of completed transactions ( varint32 )
    //   transaction group ranges ( (varint64, varint64) * number of completed transactions )
    //
    class LogRecordKey {
        private:
            static const uint32_t LOGRECKEY_OFFSET_COMP_PADDING_SIZE = 4;
            static const uint32_t LOGRECKEY_OFFSET_BODY = LOGRECKEY_OFFSET_COMP_PADDING_SIZE;
            static const uint16_t  LOGRECKEY_FORMAT_FLAGS_SNAPPY_COMPRESS = 0x8000;
            static const uint16_t  LOGRECKEY_FORMAT_PADDING_SIZE_MASK = 0x7fff;

            std::string &buffer_;
            std::string &io_buffer_;
            std::string buffer1_;
            std::string buffer2_;
            uint32_t dbhash_;

        public:
            LogRecordKey(uint32_t dbhash) : buffer_(buffer1_), io_buffer_(buffer2_), dbhash_(dbhash) { }
            // Write
            void AppendHeaderSpace() { buffer_.append(LOGRECKEY_OFFSET_BODY, 0); }
            void AppendIKeySeqList(std::list<SequenceNumber> &ikey_seq_list) {
                PutVarint32(&buffer_, ikey_seq_list.size());
                for(auto seq : ikey_seq_list) {  PutVarint64(&buffer_, seq); }
            }
            void AppendTrxnGroupList(std::list<TrxnGroupRange> trxn_group_ranges) {
                PutVarint32(&buffer_, trxn_group_ranges.size());
                for(auto range : trxn_group_ranges) { PutVarint64(&buffer_, range.start); PutVarint64(&buffer_, range.end); }
            }
            void AppendVarint32(uint32_t value) { PutVarint32(&buffer_, value); }
            void AppendVarint64(uint64_t value) { PutVarint64(&buffer_, value); }
            void SetIOBufferPaddingSize(bool snappy_compress, uint16_t padding_size) {
                assert(padding_size<=0x7ffff);
                EncodeFixed32(const_cast<char*>(io_buffer_.data()) + LOGRECKEY_OFFSET_COMP_PADDING_SIZE, (snappy_compress?LOGRECKEY_FORMAT_FLAGS_SNAPPY_COMPRESS:0) | padding_size);
            }
            void BuildIOBuffer();
            std::string &GetIOBuffer() { return io_buffer_; }
            void Reset() { buffer1_.resize(0); buffer2_.resize(0); buffer_ = buffer1_; io_buffer_ = buffer2_; }
            // Read
            InSDBKey GetInSDBKey() { return GetMetaKey(dbhash_, kLogRecord); }
            Status Load(Env *env);
            bool DecodeBuffer(std::list<uint64_t> &deleted_ikey_seqs, std::vector<uint64_t> &workers_next_ikey_seqs, std::list<TrxnGroupRange> &completed_trxn_groups);
    };

    //
    // Recovery work
    // Manifest creates
    //
    class DBRecovery {
        private:
            Manifest *mf_;
            Env *env_;
            LogRecordKey logkey_;
            std::list<TrxnGroupRange> completed_trxn_groups_;
            std::vector<uint64_t> workers_next_ikey_seqs_;
            std::list<uint64_t> deleted_ikey_seqs_;

        public:
            DBRecovery(Manifest *mf);
            ~DBRecovery() { }
            Status LoadLogRecordKey();
            Status CleanupSktUpdateList(SKTableMem *skt, InSDBMetaType updatelist_type);
            Status CleanupSktUpdateList(SKTableMem *skt);
            Status LoadBeginsKeysAndCLeanupSKTableMeta(uint32_t &sktid);
            bool AddCompletedColumn(std::unordered_map<TransactionID, TransactionColCount> &TRXNList, TransactionID col_trxn_id, uint32_t count);
            void AddUncompletedColumn(std::unordered_map<TransactionID, TransactionColCount> &TRXNList, TransactionID col_trxn_id, uint32_t count);
            void InsertRecoveredKeyToKeymap(Manifest *mf, ColumnInfo &colinfo);
            bool UpdateAllKBMsInOrder();
            Status RunRecovery();
            Env *GetEnv() { return env_; }
            Manifest *GetManifest() { return mf_; }
            std::list<TrxnGroupRange> &GetCompletedTrxnGroup() { return completed_trxn_groups_; }
            uint64_t GetWorkerNextIKeySeq(uint16_t worker_id) { return workers_next_ikey_seqs_[worker_id]; }
    };

    class DBRecoveryThread {
        private:
            DBRecovery *recovery_;
            uint16_t worker_id_;
            uint64_t thread_id_;
            Status status_;
            std::unordered_map<TransactionID, TransactionColCount> UncompletedTRXNList_;
            std::list<ColumnInfo> UncompletedKeyList_;

        public:
            DBRecoveryThread(DBRecovery *recovery, uint16_t worker_id) :
                recovery_(recovery), worker_id_(worker_id), thread_id_(0), status_() { }
            static void DBRecoveryThread_(void* recovery)
            { reinterpret_cast<DBRecoveryThread*>(recovery)->ThreadFn(); }
            void ThreadFn();

            bool StartThread() { 
                thread_id_ = recovery_->GetEnv()->StartThread(&DBRecoveryThread_, this); 
                return thread_id_;
            }
            void JointThread() { if(thread_id_) recovery_->GetEnv()->ThreadJoin(thread_id_); }
            Status GetStatus() { return status_; }
            std::unordered_map<TransactionID, TransactionColCount> &GetUncompletedTrxnList() { return UncompletedTRXNList_; }
            std::list<ColumnInfo> &GetUncompletedKeyList() { return UncompletedKeyList_; }
    };

    struct SnapInfo {
        uint64_t cseq;
        uint64_t exp;
    };


    struct KBMHash {
        inline uint32_t operator() (const SequenceNumber& key) noexcept {
            uint64_t tkey = key;
            tkey = (~tkey) + (tkey << 18); 
            tkey = tkey ^ (tkey >> 31); 
            tkey = tkey * 21; 
            tkey = tkey ^ (tkey >> 11); 
            tkey = tkey + (tkey << 6); 
            tkey = tkey ^ (tkey >> 22); 
            return (uint32_t)tkey; 
        } 
    };


    /*static uint64_t gettid() {
      pthread_t tid = pthread_self();
      uint64_t thread_id = 0;
      memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
      return thread_id;
      }*/
    /**
      Simple Memory Pool logic.
      Thie framework provide a simple interface for memory pool.
      Only AllocMemoryNode() and FreeMomoryNode() interface provides.

      1. Constructor takes on parameter for qsize, which will keep in memory.
      2. class use this mechanism must provide it's own mechnaism based on it's internal requirement.
      a. If class need to reinitialize object, each class must provides itself.
      b. If class need to cleanup some extra information, each class need to implement it on destructor fucntion.
      */
    template <class T>
        class MemoryPool {
            private:
                Spinlock lock_;
                uint32_t qsize_;
                std::queue<T*>  free_node_;
                Manifest* mf_;
            public:
                MemoryPool(int qsize, Manifest *mf) : lock_(), qsize_(qsize), free_node_(), mf_(mf) {
                    for (int i = 0; i < qsize; i++) free_node_.push(new T(mf_));
                }
                ~MemoryPool() {
                    T* node = NULL;
                    while(!free_node_.empty()) {
                        node = free_node_.front();
                        assert(node);
                        free_node_.pop();
                        delete node;
                    }
                }
                T* AllocMemoryNode() {
                    T* node = NULL;
                    if (/*free_node_.size() > 128 && */!lock_.TryLock()) {
                        if (free_node_.empty()) {
                            node = new T(mf_);
                        } else {
                            node = free_node_.front();
                            free_node_.pop();
                        }
                        lock_.Unlock();
                    } else node = new T(mf_);
                    return node;
                }

                void FreeMemoryNode(T* node) {
                    node->CleanUp();
                    if (free_node_.size() < qsize_ && !lock_.TryLock()) {
                        free_node_.push(node);
                        lock_.Unlock();
                    } else delete node;
                }
        };
    class RawMemoryPool {
        private:
            /*this memory pool is per thread pool So, we don't need to have lock */
            //Spinlock lock_;
            uint32_t qsize_;
            std::queue<Slice>  free_node_;
        public:
            RawMemoryPool(int qsize) : /*lock_(),*/ qsize_(qsize), free_node_(){}
            ~RawMemoryPool() {
                Slice node;
                while(!free_node_.empty()) {
                    node = free_node_.front();
                    free_node_.pop();
                    free(const_cast<char*>(node.data()));
                }
            }
            Slice AllocRawMemoryNode(uint32_t size) {
                Slice node;
                //lock_.Lock();
                if (free_node_.empty()) {
                    node = Slice((char*)malloc(size), size);
                } else {
                    node = free_node_.front();
                    free_node_.pop();
                    if(node.size() < size){
                        node = Slice((char*)realloc(const_cast<char*>(node.data()), size), size);
                    }
                }
                //lock_.Unlock();
                return node;
            }

            void FreeRawMemoryNode(Slice node) {
                //lock_.Lock();
                if (free_node_.size() < qsize_) {
                    free_node_.push(node);
                } else {
                    free(const_cast<char*>(node.data()));
                }
                //lock_.Unlock();
            }
    };

    /* Per Thread Raw Memory Pool*/
    class PerThreadRawMemoryPool{
        private:
            pthread_key_t pool_id_;
            static void DestroyBuffer(void *arg){
                RawMemoryPool *mem_pool = reinterpret_cast<RawMemoryPool*>(arg);
                delete mem_pool;
            }
        public:
            explicit PerThreadRawMemoryPool() { 
                if(pthread_key_create(&pool_id_, DestroyBuffer)) abort(); 
            } 
            ~PerThreadRawMemoryPool(){
                if (pthread_key_delete(pool_id_)) abort();
            }
            Slice PerThreadAllocRawMemoryNode(uint32_t size) {

                RawMemoryPool *mem_pool = reinterpret_cast<RawMemoryPool *>(pthread_getspecific(pool_id_));
                if(!mem_pool){
                    mem_pool= new RawMemoryPool(16*1024);
                    pthread_setspecific(pool_id_, mem_pool);
                }
                return mem_pool->AllocRawMemoryNode(size);
            }
            void PerThreadFreeRawMemoryNode(Slice node) {
                RawMemoryPool *mem_pool = reinterpret_cast<RawMemoryPool *>(pthread_getspecific(pool_id_));
                /*It must be NOT NULL. it should be allocated in AllocRawMemoryNode()*/
                assert(mem_pool);
                mem_pool->FreeRawMemoryNode(node);
            }
    };

    /* DIOBuffer : Per Thread Device I/O Buffer */
    class DIOBuffer{
        private:
            pthread_key_t key_;
            static void DestroyBuffer(void *arg){
                Slice *buf = reinterpret_cast<Slice *>(arg);
                free(const_cast<char*>(buf->data()));
                delete buf;
            }
        public:
            explicit DIOBuffer() { 
                /* For I/O & decompression */
                if(pthread_key_create(&key_, DestroyBuffer))
                    abort(); 
            } 
            ~DIOBuffer(){
                if (pthread_key_delete(key_)) 
                    abort();
            }
            Slice* GetDIOBuffer(uint32_t size) {
                Slice *buf;
                buf = reinterpret_cast<Slice *>(pthread_getspecific(key_));
                if (NULL == buf) {
                    char *mem = (char*)malloc(size);
                    buf = new Slice(mem, size);
                    pthread_setspecific(key_, buf);
                }else if(buf->size() < size){
                    char *mem = (char*)realloc(const_cast<char*>(buf->data()), size);
                    buf->clear();
                    delete buf;
                    buf = new Slice(mem, size);
                    pthread_setspecific(key_, buf);
                }
                return buf;
            }

    };

    struct IterCurrentKeyInfo{
        int offset;
        std::string key;//To get key from SKTDF
        /* 
         * next key may have same kbpm
         * In this case, we don't need to find the kbpm from hash
         */
        KeyBlockPrimaryMeta* kbpm;
        SequenceNumber ikey;/*0 means the value is in UserValue*/
        uint32_t ivalue_size;
        IterCurrentKeyInfo() : offset(-1), key(), kbpm(NULL), ikey(0), ivalue_size(0){}
        void InitIterCurrentKeyInfo() {
            offset = -1;
            key.resize(0);
            kbpm = NULL;
            ikey = 0;
            ivalue_size = 0;
        }
    };

    // Two queue
    class WorkerData {
        private:
            Spinlock lock_[2];
            std::deque<KeyBlockPrimaryMeta*> queue_[2];
            std::atomic<uint8_t> idx_; /* idx indicate active queue : inactive idx^1 */
            uint64_t next_worker_ikey_;
            uint64_t last_next_worker_ikey_;
        public:
            WorkerData() : idx_(0), next_worker_ikey_(0), last_next_worker_ikey_(0) {}
            ~WorkerData() {}
            uint64_t PushKBML(KeyBlockPrimaryMeta* kbpm, uint64_t next_worker_ikey){
                uint64_t cur_next_ikey = 0;
retry:
                uint8_t idx = idx_.load(std::memory_order_relaxed);
                if (lock_[idx].TryLock()) goto retry;
                if (idx != idx_.load(std::memory_order_relaxed)) {
                    lock_[idx].Unlock();
                    goto retry;
                }
                queue_[idx].push_back(kbpm);
                cur_next_ikey = next_worker_ikey_;
                next_worker_ikey_ = next_worker_ikey;
                lock_[idx].Unlock();
                return cur_next_ikey;
            }

            bool IsEmpty(uint8_t index) {
                bool empty;
                lock_[index].Lock();
                empty = queue_[index].empty();
                lock_[index].Unlock();
                return empty;
            }

            uint32_t Count(){
                return queue_[0].size()+queue_[1].size();
            }
            uint32_t ZeroNRSCount();
            /* called only from log thread */
            void ChangeActive() {
                uint8_t active_idx = idx_.load(std::memory_order_relaxed);
                lock_[active_idx].Lock();
                idx_.store((active_idx ^ 1), std::memory_order_relaxed);
                lock_[active_idx].Unlock();
            }

            /* called only from log thread */
            std::deque<KeyBlockPrimaryMeta*> * GetInActiveQueue() {
                return &queue_[idx_.load(std::memory_order_relaxed) ^ 1];
            }

            /* called only from log thread */
            Spinlock* GetInActiveLock() {
                return lock_ + (idx_.load(std::memory_order_relaxed) ^ 1);
            }

            /* called only from log thread */
            uint64_t GetWorkerNextIKeySeq() {
                lock_[idx_.load(std::memory_order_relaxed)].Lock();
                uint64_t next_ikey_seq = next_worker_ikey_;
                lock_[idx_.load(std::memory_order_relaxed)].Unlock();
                return next_ikey_seq;
            }

            uint64_t GetWorkerNextIKeySeqUnSafe() {
                return next_worker_ikey_;
            }

            uint64_t GetLastWorkerNextIKeySeq() {
                return last_next_worker_ikey_;
            }

            void SetLastWorkerNextIKeySeq(uint64_t last_next_worker_ikey) {
                last_next_worker_ikey_ = last_next_worker_ikey;
            }

            void InitWorkerNextIKeySeq(uint64_t seq) {
                last_next_worker_ikey_ = next_worker_ikey_ = seq;
            }
    };
    enum SKTCacheType{
        kDirtySKTCache      = 0,
        kCleanSKTCache      = 1,
        kNrCacheType        = 2,
    };
    inline SKTCacheType& operator++(SKTCacheType& t, int)
    {
        const int i = static_cast<int>(t);
        t = static_cast<SKTCacheType>((i + 1));
        return t;
    }

    struct ColumnScratchPad{
        UserValue* uv;
        uint64_t ikey_seq_num;
        uint32_t ivalue_size; 
        uint8_t offset;
    };

    class UVAllocatorPool{
        private:
            pthread_key_t pool_id_;
            volatile uint16_t id_;
            Manifest *mf_;
        public:
            explicit UVAllocatorPool(Manifest *mf) { 
                if(pthread_key_create(&pool_id_, NULL)) abort(); 
                id_ = 0;
                mf_ = mf;
            } 
            ~UVAllocatorPool(){
                if (pthread_key_delete(pool_id_)) abort();
            }
            ObjectAllocator<UserValue>* GetUVAllocator();
    };

    class CNAllocatorPool{
        private:
            pthread_key_t pool_id_;
            volatile uint16_t id_;
            Manifest *mf_;
        public:
            explicit CNAllocatorPool(Manifest *mf) {
                if(pthread_key_create(&pool_id_, NULL)) abort();
                id_ = 0;
                mf_ = mf;
            }
            ~CNAllocatorPool(){
                if (pthread_key_delete(pool_id_)) abort();
            }
            SimpleObjectAllocator<ColumnNode>* GetCNAllocator();
    };

    class UKAllocatorPool{
        private:
            pthread_key_t pool_id_;
            volatile uint16_t id_;
            Manifest *mf_;
        public:
            explicit UKAllocatorPool(Manifest *mf) { 
                if(pthread_key_create(&pool_id_, NULL)) abort(); 
                id_ = 0;
                mf_ = mf;
            } 
            ~UKAllocatorPool(){
                if (pthread_key_delete(pool_id_)) abort();
            }
            ObjectAllocator<UserKey>* GetUKAllocator();
    };
    class RequestNodeAllocatorPool{
        private:
            pthread_key_t pool_id_;
            volatile uint16_t id_;
            Manifest *mf_;
        public:
            explicit RequestNodeAllocatorPool(Manifest *mf) { 
                if(pthread_key_create(&pool_id_, NULL)) abort(); 
                id_ = 0;
                mf_ = mf;
            } 
            ~RequestNodeAllocatorPool(){
                if (pthread_key_delete(pool_id_)) abort();
            }
            ObjectAllocator<RequestNode>* GetRequestNodeAllocator();
    };


    struct KBPMUpdateVector{
        std::vector<InSDBKey> key_vec;
        std::vector<char*> data_vec;
        std::vector<int> size_vec;
        //uint32_t data_size;
        KBPMUpdateVector() : key_vec(), data_vec(), size_vec(){ }
        void Init(){
            key_vec.resize(0);
            data_vec.resize(0);
            size_vec.resize(0);
        }
        void Insert(const InSDBKey& key, char* data, const int size){
            key_vec.push_back(key);
            data_vec.push_back(data);
            size_vec.push_back(size);
        }
        bool empty(){
            assert(key_vec.size() == data_vec.size());
            assert(data_vec.size() == size_vec.size());
            return key_vec.empty();
        }
#if 0
        uint32_t GetDataBufferSize(){
            return data_size;
        }
        uint32_t GetKeyCount(){
            assert(key_vec.size() == data_vec.size());
            assert(data_vec.size() == size_vec.size());
            return key_vec.size();
        }
#endif
    };

    class InSDBKeyCompare {
        public:
            bool operator() (const InSDBKey &lhs, const InSDBKey &rhs) const {
                if (lhs.type != rhs.type) return lhs.type < rhs.type;
                if (lhs.split_seq != rhs.split_seq) return lhs.split_seq < rhs.split_seq;
                if (lhs.seq != rhs.seq) return lhs.seq < rhs.seq;
                return false;
            }
    };

#ifdef USE_HOTSCOTCH
    typedef tsl::hopscotch_map<KeySlice, UserKey*, UKHash, UKEqual> HotscotchKeyHashMap;
#endif
#ifdef INSDB_USE_HASHMAP
    typedef folly::ConcurrentHashMap<KeySlice, UserKey*, UKHash, UKEqual> KeyHashMap;
#endif
    class Manifest {
        private:
            typedef SkipList<SKTableMem*, KeySlice, UserKeyComparator> SKTList;
#ifdef INSDB_USE_HASHMAP
            typedef folly::ConcurrentHashMap<SequenceNumber, KeyBlockPrimaryMeta*, KBMHash> KBPMHashMap;
#endif
            Env *env_;
            const Options options_;
            std::string dbname_;
            port::Mutex worker_mu_;
            port::CondVar worker_cv_;
            std::atomic<uint16_t> worker_cnt_;
            std::atomic<uint16_t> term_worker_cnt_;

            std::atomic<bool> worker_terminated_;
            port::Mutex worker_termination_mu_;
            port::CondVar worker_termination_cv_;
            std::atomic<uint16_t> nr_worker_in_running_;
            std::atomic<uint64_t> next_InSDBKey_seq_num_;
            std::atomic<SequenceNumber> sequence_number_;
            /////////////////////////ADD FOR PACKING////////////////////////
            /*
             * When assigning new trxn id, the new trxn id must be inserted into uncompleted_trxn_group_.
             * and uncompleted_trxn_group_ must acquire a lock.
             * It is therefore recommended to use non-atomic variable for next_trxn_id_
             * And during locking, assign a trxn id from the next_trxn_id_. 
             * std::atomic<TransactionID> next_trxn_id_;
             */
            TransactionID next_trxn_id_;
            ////////////////////////////////////////////////////////////////
            std::atomic<uint32_t> next_sktable_id_; 
            std::atomic<uint64_t> memory_usage_;
#ifdef MEM_TRACE
            std::atomic<uint64_t> skt_usage_;
            std::atomic<uint64_t> uk_usage_;
            std::atomic<uint64_t> col_usage_;
            std::atomic<uint64_t> sktdf_usage_;
            std::atomic<uint64_t> kbpm_usage_;
            std::atomic<uint64_t> iter_usage_;
            uint64_t flush_miss_;
            uint64_t flush_hit_;
            uint64_t update_to_clean_;
            uint64_t evicted_skt_;
            uint64_t flush_to_clean_;
            uint64_t flush_to_dirty_;
            uint64_t flush_split_inc_;
            std::atomic<uint64_t> pr_iterbuild_cnt_;
            std::atomic<uint64_t> pr_next_cnt_;
            std::atomic<uint64_t> clear_pr_cnt_;
            std::atomic<uint64_t> skip_pr_cnt_;
            uint64_t evict_with_pr_;
            uint64_t evict_iter_;
            uint64_t evict_skt_iter_;
            uint64_t evict_stop_skt_ref_;
            uint64_t evict_recalim_;
#endif
            std::atomic<uint64_t> update_cnt_;
            std::atomic<uint32_t> evict_sktable_id_;  

            std::atomic<uint32_t> nr_iterator_;
            UserKeyComparator ukey_comparator_;
            GCInfo gcinfo_;
            SKTList sktlist_;  // skiplist for sktable
#ifdef USE_HOTSCOTCH
            Spinlock hopscotch_key_hasp_lock_;
            HotscotchKeyHashMap  *hopscotch_key_hashmap_;
#endif
#ifdef INSDB_USE_HASHMAP
            KeyHashMap  *key_hashmap_;
            std::atomic<uint32_t> uk_count_;
            bool congestion_control_;
            port::Mutex congestion_control_mu_;
            port::CondVar congestion_control_cv_;

            KBPMHashMap *kbpm_hashmap_;
            KeyHashMap  *flushed_key_hashmap_;
#endif
            /*The last Col Info Table key for flush the table before update in-log KBPM in KBMUpdateThread */
            std::atomic<InSDBKey> last_cit_key_;
            /**
             * Managing all SKTables in order, including fetched and unfetched 
             * The perpose of this list is 
             *  1. To find prev SKTable(Backward Round Robin) to flush 
             *  2. To find the previous SKTable from a empty SKTable
             *  3. To find the next SKTable for merge
             */
            Spinlock skt_sorted_simplelist_lock_;
            Spinlock snapshot_simplelist_lock_;
            SimpleLinkedList skt_sorted_simplelist_;
            SimpleLinkedList snapshot_simplelist_;
            uint64_t *TtlList_;
            bool* TtlEnabledList_;

            // SKTable cache management
            template <class T>
                struct CacheList{
                    Spinlock &lock;
                    std::list<T*>  cache;
                    CacheList(Spinlock &_lock) : lock(_lock), cache(){}
                    void Lock(){ lock.Lock(); }
                    void Unlock(){ lock.Unlock(); }

                    size_t GetCacheSize(){
                        return cache.size();
                    }
                    bool IsEmpty(){
                        return cache.empty();
                    }
                    T* PopItem(){
                        T* item = NULL;
                        Lock();
                        if (!cache.empty()) {
                            item = cache.front();
                            cache.pop_front();
                        }
                        Unlock();
                        return item;
                    }
                    void SwapCacheList(std::list<T*> &item_list){
                        Lock();
                        cache.swap(item_list);
                        Unlock();
                    }
                    void SpliceToFront(std::list<T*> &item_list){
                        Lock();
                        cache.splice(cache.begin(), item_list);
                        Unlock();
                    }
                    void SpliceToBack(std::list<T*> &item_list){
                        Lock();
                        cache.splice(cache.end(), item_list);
                        Unlock();
                    }
                    void PushFront(T* item){
                        Lock();
                        cache.push_front(item);
                        Unlock();
                    }
                    void PushBack(T* item){
                        Lock();
                        cache.push_back(item);
                        Unlock();
                    }
                    /* caller must have lock */
                    /* No delete support anymore */
#if 0
                    void DeleteItem(T* item){
                        cache.erase(item->GetLocation());
                        SubCache(item->GetSize());
                    }
#endif
                };

            Spinlock skt_dirty_cache_lock_;
            Spinlock skt_clean_cache_lock_;
            CacheList<SKTableMem> skt_cache_[kNrCacheType];
            Spinlock skt_kbm_update_cache_lock_;
            CacheList<KeyBlockPrimaryMeta> kbpm_update_list_;

            /* memory pool definition */
            MemoryPool <KeyBlock> *kb_pool_;
            MemoryPool <KeyBlockPrimaryMeta> *kbpm_pool_;

            port::Mutex skt_flush_mu_;
            port::CondVar skt_flush_cv_;

            Spinlock offload_lock_;
            std::atomic<uint16_t> offload_cnt_;
            std::queue<SKTableMem*> flush_offload_queue_;

            std::atomic<bool> force_kbm_update_;
            port::Mutex kbpm_update_mu_;
            port::CondVar kbpm_update_cv_;

            port::Mutex skt_eviction_mu_;
            port::CondVar skt_eviction_cv_;

            std::atomic<uint8_t> flags_;
            std::atomic<bool> cache_thread_shutdown_;

            uint64_t skt_flush_thread_tid_;
            uint64_t skt_eviction_thread_tid_;
            uint64_t kbm_update_thread_tid_;

            uint64_t logcleanup_tid_;

            DIOBuffer dio_buffer_;

            /////////////////////****ADD FOR PACKING****////////////////////
#define TGID_SHIFT 5 /* Max 32 per group */
            Spinlock uctrxn_group_lock_;
            std::unordered_map<TrxnGroupID, uint32_t> uncompleted_trxn_group_;
            //////////////////***************************///////////////////
            Spinlock ctrxn_group_lock_;
            std::list<TrxnGroupRange> completed_trxn_group_list_;
            ////////////////////////////////////////////////////////////////
            uint32_t max_iter_key_buffer_size_;
            uint32_t max_iter_key_offset_buffer_size_;
            ObjectAllocator<UserValue> *uv_allocator_;
            SimpleObjectAllocator<ColumnNode> *cn_allocator_;
            ObjectAllocator<UserKey>   *uk_allocator_;
            ObjectAllocator<RequestNode>   *request_node_allocator_;

            UVAllocatorPool uv_allocator_pool_;
            CNAllocatorPool cn_allocator_pool_;
            UKAllocatorPool uk_allocator_pool_;
            RequestNodeAllocatorPool request_node_allocator_pool_;
            //Spinlock *free_request_node_spinlock_;
            //std::deque<RequestNode*> *free_request_node_;

            /* LogCleanup thread queues */
            WorkerData *logcleanup_data_; /* need to create as worker count */

            Spinlock free_skt_buffer_spinlock_;
            std::deque<Slice> free_skt_buffer_;

            //PerThreadRawMemoryPool thread_memory_pool_;
            bool normal_start_;
            uint32_t dbhash_;
        public:
            enum ManifestConst {
                MF_HASH_SEED = 0xbc9f1d34,
                MF_SKT_ID_MASK = 0xC0000000,
                MF_OFFSET_CRC = 4,
                MF_OFFSET_LAST_IKEY_SEQ = 8,
                MF_OFFSET_LAST_SEQ = 16,
                MF_OFFSET_LAST_SKT_SEQ = 24,
                MF_OFFSET_NUM_SKT_INFO = 28,
                MF_OFFSET_NUM_WORKER = 32,
                MF_OFFSET_NEXT_WORKER_INFO = 36,
            };
            Manifest(const Options& option, std::string& dbname);
            ~Manifest();

#ifdef USE_HOTSCOTCH
            void KeyHashLock(){hopscotch_key_hasp_lock_.Lock();}
            void KeyHashUnlock(){hopscotch_key_hasp_lock_.Unlock();}
#endif
            uint32_t GetHashSize(){ return uk_count_.load(std::memory_order_relaxed); }
            Env* GetEnv() { return env_; }
            uint32_t GetDBHash() { return dbhash_; }
            /////////////////////////////////////////// BEGIN : Write Worker related functions ///////////////////////////////////////////
            void WorkerLock(){worker_mu_.Lock();}
            void WorkerUnlock(){worker_mu_.Unlock();}
            bool WorkerWaitSig(){ return worker_cv_.TimedWait(env_->NowMicros() + 1000000/*Wake up every 1 second*/); }
            void SignalWorkerThread(bool force = true){
                if(force){
                    if(nr_worker_in_running_.load(std::memory_order_relaxed) < worker_cnt_)
                        worker_cv_.Signal();
                }else if(!nr_worker_in_running_.load(std::memory_order_relaxed)){
                    /* If no worker is running right now */
                    worker_cv_.Signal();
                }
            }
            void SignalAllWorkerThread(void){ worker_cv_.SignalAll(); }
            void WaitForWorkerThread(){ 
                while(worker_cnt_.load(std::memory_order_relaxed));
            }
            void WorkerTerminationWaitSig(){ 
                worker_termination_mu_.Lock();
                while(!worker_terminated_.load(std::memory_order_seq_cst)){
                //while(term_worker_cnt_.load(std::memory_order_seq_cst) != kWriteWorkerCount){
                    worker_termination_cv_.TimedWait(env_->NowMicros() + 100000/*Wake up every 0.1 second*/); 
                }
                worker_termination_mu_.Unlock();
            }
            void SignalWorkerTermination(void){ 
                worker_termination_mu_.Lock();
                worker_terminated_ = true;
                worker_termination_cv_.SignalAll(); 
                worker_termination_mu_.Unlock();
            }
            // pre increament
            uint16_t IncWorkerCount(){ return ++worker_cnt_; }
            uint16_t DecWorkerCount(){ return --worker_cnt_; }
            uint16_t IncTerminatedWorkerCount(){ return ++term_worker_cnt_; }
            uint16_t ResetTerminatedWorkerCount(){ term_worker_cnt_ = 0; }

            uint16_t GetWorkerCount(){ return worker_cnt_.load(std::memory_order_relaxed); }
            void IncRunningWorkerCount(){ nr_worker_in_running_.fetch_add(1, std::memory_order_relaxed); }
            void DecRunningWorkerCount(){ nr_worker_in_running_.fetch_sub(1, std::memory_order_relaxed); }
            uint16_t GetRunningWorkerCount(){ return nr_worker_in_running_.load(std::memory_order_relaxed); }
            /////////////////////////////////////////// END : Write Worker related functions ///////////////////////////////////////////

            /////////////////////////////////////////// START: Status Flags ///////////////////////////////////////////
            const static uint8_t flags_sktable_flush            = 0x01;
            const static uint8_t flags_memory_reclaim           = 0x02;
            const static uint8_t flags_kbm_update               = 0x04;
            const static uint8_t flags_skt_eviction_thread_busy = 0x08;
            const static uint8_t flags_kbm_update_thread_busy   = 0x10;
            const static uint8_t flags_skt_flush_thread_busy    = 0x20;
            const static uint8_t flags_log_cleanup_requested    = 0x40;
            const static uint8_t flags_clean_to_dirty           = 0x80;

            bool IsInSKTableFlush() { return (flags_.load() & flags_memory_reclaim); }
            void SetSKTableFlush() { flags_.fetch_or(flags_memory_reclaim) ; }
            void ClearSKTableFlush() { flags_.fetch_and(~flags_memory_reclaim); }

            bool IsInMemoryReclaim() { return (flags_.load() & flags_memory_reclaim); }
            void SetMemoryReclaim() { flags_.fetch_or(flags_memory_reclaim) ; }
            void ClearMemoryReclaim() { flags_.fetch_and(~flags_memory_reclaim); }

            bool IsInKBPMUpdate() { return (flags_.load() & flags_kbm_update); }
            void SetKBPMUpdate() { flags_.fetch_or(flags_kbm_update) ; }
            void ClearKBPMUpdate() { flags_.fetch_and(~flags_kbm_update); }

            bool IsCacheThreadsBusy(){ return (flags_.load(std::memory_order_seq_cst) & (flags_skt_eviction_thread_busy| flags_kbm_update_thread_busy|flags_skt_flush_thread_busy));}

            bool IsFlushThreadBusy() { return (flags_.load(std::memory_order_seq_cst) & flags_skt_flush_thread_busy); }
            void SetFlushThreadBusy() { flags_.fetch_or(flags_skt_flush_thread_busy, std::memory_order_seq_cst) ; }
            void ClearFlushThreadBusy() { flags_.fetch_and(~flags_skt_flush_thread_busy, std::memory_order_seq_cst); }

            bool IsSKTEvictionThreadBusy(){ return (flags_.load(std::memory_order_seq_cst) & flags_skt_eviction_thread_busy);}
            void SetSKTEvictionThreadBusy(){ flags_.fetch_or(flags_skt_eviction_thread_busy, std::memory_order_seq_cst); }
            void ClearSKTEvictionThreadBusy(){ flags_.fetch_and(~flags_skt_eviction_thread_busy, std::memory_order_seq_cst); }

            bool IsKBMUpdateThreadBusy() { return (flags_.load(std::memory_order_seq_cst) & flags_kbm_update_thread_busy); }
            void SetKBMUpdateThreadBusy() { flags_.fetch_or(flags_kbm_update_thread_busy, std::memory_order_seq_cst) ; }
            void ClearKBMUpdateThreadBusy() { flags_.fetch_and(~flags_kbm_update_thread_busy, std::memory_order_seq_cst); }

            void SetLogCleanupRequest() { flags_.fetch_or(flags_log_cleanup_requested) ; }
            bool ClearLogCleanupRequest() { return flags_.fetch_and(~flags_log_cleanup_requested) & flags_log_cleanup_requested; }

            bool IsCleanToDirty() { return (flags_.load(std::memory_order_relaxed) & flags_clean_to_dirty); }
            void SetCleanToDirty() { flags_.fetch_or(flags_clean_to_dirty, std::memory_order_relaxed); }
            bool ClearCleanToDirty() { return (((uint8_t)flags_.fetch_and(0x7F/*~flags_clean_to_dirty makes compile warning??*/, std::memory_order_relaxed)) & flags_clean_to_dirty); }
            /////////////////////////////////////////// END  : Status Flags ///////////////////////////////////////////
            
            /////////////////////////////////////////// START: InternalKey/SKTID related functions ///////////////////////////////////////////
#if 0
            uint64_t GenerateNextInSDBKeySeqNum() { return next_InSDBKey_seq_num_.fetch_add(1, std::memory_order_seq_cst); }
            uint64_t GetNextInSDBKeySeqNum() { return next_InSDBKey_seq_num_.load(std::memory_order_seq_cst); }
#else
            uint64_t GenerateNextInSDBKeySeqNum() { return next_InSDBKey_seq_num_.fetch_add(1, std::memory_order_relaxed); }
            uint64_t GetNextInSDBKeySeqNum() { return next_InSDBKey_seq_num_; }
#endif
            SequenceNumber GenerateSequenceNumber() { return sequence_number_.fetch_add(1, std::memory_order_relaxed) + 1; }
            SequenceNumber GetLastSequenceNumber() { return sequence_number_; }
#if 0
            void SetNextInSDBKeySeqNum(uint64_t ikey_seq) { next_InSDBKey_seq_num_.store(ikey_seq, std::memory_order_seq_cst); }
#else
            void SetNextInSDBKeySeqNum(uint64_t seq) { next_InSDBKey_seq_num_.store(seq); }
#endif
            uint32_t GenerateNextSKTableID() { return next_sktable_id_.fetch_add(1, std::memory_order_relaxed) + 1; }
            void SetNextSKTableID(uint32_t skt_id) { next_sktable_id_.store(skt_id); }
            /////////////////////////////////////////// END  : InternalKey/SKTID related functions ///////////////////////////////////////////

            /////////////////////////////////////////// START: SKTableMem Skiplist ///////////////////////////////////////////
            SKTableMem* FindSKTableMem(const Slice& ukey);
            bool InsertSKTableMem(SKTableMem* table);
            SKTableMem* RemoveSKTableMemFromSkiplist(SKTableMem* table);
            SKTableMem* Next(SKTableMem* cur);
            SKTableMem* Prev(SKTableMem* cur);
            SKTableMem* GetFirstSKTable();
            SKTableMem* GetLastSKTable();
            SKTableMem* GetFromNode(void *anon_node);
            uint64_t GetEstimateNrSKTable(Slice key);
            /////////////////////////////////////////// END  : SKTableMem Skiplist ///////////////////////////////////////////
            
            /////////////////////////////////////////// START: Manifest/InSDB Creation/Recovery/Initializtion/Deletion ///////////////////////////////////////////
            Status ReadManifest(std::string * contents);
            Status DeleteManifest();
            void InitNextInSDBKeySeqNum(int old_worker_count, uint64_t* seq);
            Status InitManifest();
            Status BuildManifest();
            Status ReadSuperSKTable(SuperSKTable* super_skt);
            Status GetSuperSKTable();
            Status BuildSuperSKTable();
            Status BuildFirstSKTable(int32_t skt_id, uint32_t &io_size, uint32_t &hash_data_size);
            Status CreateNewInSDB();
            int ReadSKTableMeta(char* buffer, InSDBMetaType type, uint32_t skt_id, GetType req_type, int buffer_size);
            Arena* LoadKeymapFromDevice(uint32_t skt_id, uint32_t keymap_size, uint32_t keymap_hash_size, KeyMapHashMap*& hashmap,  GetType type);
            Status ReadAheadSKTableMeta(InSDBMetaType type, uint32_t skt_id, int buffer_size = kMaxSKTableSize);
            Status DeleteSKTableMeta(InSDBMetaType type, uint32_t skt_id, uint16_t nr_key = 1);
            Status ReadSKTableInfo(uint32_t skt_id, SKTableInfo* reocver);
            Status CleanupPreallocSKTable(uint32_t prealloc_skt, uint32_t next_skt_id);
            Status RecoverInSDB();
            Status DeleteInSDBWithIter();
            Status DeleteInSDBWithManifest(uint16_t column_count);
            Status DeleteInSDB();
            void DeleteSKTable(void *arg);
            static void DeleteSKTableWrapper(void* arg){ 
                (void)pthread_setname_np(pthread_self(), "delete sktable");
                DeleteSKTableCtx * delctx = reinterpret_cast<DeleteSKTableCtx*>(arg);
                delctx->mf->DeleteSKTable(arg);
                return;
            }
            /* GetValue() return data src information last data_src_info bytes */
            void SetNormalStart(bool v){ normal_start_ = v;} 
            /////////////////////////////////////////// END  : Manifest Creation/Initializtion/Deletion ///////////////////////////////////////////

            /////////////////////////////////////////// START: Sorted list related functions ///////////////////////////////////////////
            void InsertFrontToSKTableSortedList(SimpleLinkedNode* skt_node) { 
                skt_sorted_simplelist_lock_.Lock(); 
                skt_sorted_simplelist_.InsertFront(skt_node);
                skt_sorted_simplelist_lock_.Unlock();
            }
            void InsertNextToSKTableSortedList(SimpleLinkedNode* new_skt_node, SimpleLinkedNode* base_skt_node)
            { 
                skt_sorted_simplelist_lock_.Lock(); 
                skt_sorted_simplelist_.InsertToNext(new_skt_node, base_skt_node);
                skt_sorted_simplelist_lock_.Unlock();
            }
            void RemoveFromSKTableSortedList(SimpleLinkedNode* skt_node){ 
                skt_sorted_simplelist_lock_.Lock();
                skt_sorted_simplelist_.Delete(skt_node); 
                skt_sorted_simplelist_lock_.Unlock();
            }
            uint64_t GetSKTableCountInSortedList(){ 
                return skt_sorted_simplelist_.Size(); 
            }
            SKTableMem* GetSKTableMem(SimpleLinkedNode *node);
#if 0
            { return node ? class_container_of(node, SKTableMem, skt_sorted_node_) : NULL; }
#endif

            SKTableMem* GetLastSKTableInSortedList();
#if 0
            {
                //return skt_sorted_list_.oldest()->GetOwner();
                return GetSKTableMem(skt_sorted_simplelist_.oldest());
            }
#endif
            SKTableMem* NextSKTableInSortedList(SimpleLinkedNode* skt_node);
            bool IsNextSKTableDeleted(SimpleLinkedNode* skt_node);
            SKTableMem* PrevSKTableInSortedList(SimpleLinkedNode* skt_node);
            /////////////////////////////////////////// END  : Sorted list related functions ///////////////////////////////////////////

            /////////////////////////////////////////// START: Dirty/KBMUpdate/Clean Cache Management ///////////////////////////////////////////
            //For Eviction
            uint64_t MemoryUsage(){
                return memory_usage_.load(std::memory_order_relaxed);
            }
            void IncreaseMemoryUsage(uint32_t size){
#ifdef MEM_TRACE
                uint64_t old_val = memory_usage_.fetch_add(size, std::memory_order_relaxed);
                if ((old_val >> 27) !=  ((old_val + size) >> 27)) {
                    printf("MEM: cur mem %ld: skt %ld, uk %ld, col %ld, sktd %ld, kv %ld, iterpad %ld\n"
                            "FLUSH & SKT: miss(%ld):hit(%ld):ToClean(%ld):ToDirt(%ld):new skt(%ld), Log:ToClean(%ld),Evict:evicted_skt(%ld)\n"
                            "PR: iter(%ld) next(%ld) clear(%ld) skip(%ld) evicted_with_pr(%ld)"
                            "evict_iter(%ld) evict_skt_iter(%ld) evict_stop(%ld) evict_reclaim(%ld)\n\n",
                            memory_usage_.load(std::memory_order_relaxed),
                            skt_usage_.load(std::memory_order_relaxed),
                            uk_usage_.load(std::memory_order_relaxed),
                            col_usage_.load(std::memory_order_relaxed),
                            sktdf_usage_.load(std::memory_order_relaxed),
                            kbpm_usage_.load(std::memory_order_relaxed),
                            iter_usage_.load(std::memory_order_relaxed),
                            flush_miss_, flush_hit_,flush_to_clean_, flush_to_dirty_, flush_split_inc_,
                            update_to_clean_,
                            evicted_skt_,
                            pr_iterbuild_cnt_.load(std::memory_order_relaxed),
                            pr_next_cnt_.load(std::memory_order_relaxed),
                            clear_pr_cnt_.load(std::memory_order_relaxed),
                            skip_pr_cnt_.load(std::memory_order_relaxed),
                            evict_with_pr_, evict_iter_, evict_skt_iter_,
                            evict_stop_skt_ref_, evict_recalim_);
                }
#else
                memory_usage_.fetch_add(size, std::memory_order_relaxed);
#endif 
            }
            void DecreaseMemoryUsage(uint32_t size){
#ifdef MEM_TRACE
                uint64_t old_val = memory_usage_.fetch_sub(size, std::memory_order_relaxed);
                if ((old_val >> 27) !=  ((old_val - size) >> 27)) {
                    printf("MEM: cur mem %ld: skt %ld, uk %ld, col %ld, sktd %ld, kv %ld, iterpad %ld\n"
                            "FLUSH & SKT: miss(%ld):hit(%ld):ToClean(%ld):ToDirt(%ld):new skt(%ld), Log:ToClean(%ld),Evict:evicted_skt(%ld)\n"
                            "PR: iter(%ld) next(%ld) clear(%ld) skip(%ld) evicted_with_pr(%ld)"
                            "evict_iter(%ld) evict_skt_iter(%ld) evict_stop(%ld) evict_reclaim(%ld)\n\n",
                            memory_usage_.load(std::memory_order_relaxed),
                            skt_usage_.load(std::memory_order_relaxed),
                            uk_usage_.load(std::memory_order_relaxed),
                            col_usage_.load(std::memory_order_relaxed),
                            sktdf_usage_.load(std::memory_order_relaxed),
                            kbpm_usage_.load(std::memory_order_relaxed),
                            iter_usage_.load(std::memory_order_relaxed),
                            flush_miss_, flush_hit_,flush_to_clean_, flush_to_dirty_, flush_split_inc_,
                            update_to_clean_,
                            evicted_skt_,
                            pr_iterbuild_cnt_.load(std::memory_order_relaxed),
                            pr_next_cnt_.load(std::memory_order_relaxed),
                            clear_pr_cnt_.load(std::memory_order_relaxed),
                            skip_pr_cnt_.load(std::memory_order_relaxed),
                            evict_with_pr_, evict_iter_, evict_skt_iter_,
                            evict_stop_skt_ref_, evict_recalim_);
                }
#else
                memory_usage_.fetch_sub(size, std::memory_order_relaxed);
#endif 
            }

            void IncreaseSKTDFNodeSize(uint32_t size){
                IncreaseMemoryUsage(size);
#ifdef MEM_TRACE
                IncSKTDF_MEM_Usage(size);
#endif
            }

            void DecreaseSKTDFNodeSize(uint32_t size){
                DecreaseMemoryUsage(size);
#ifdef MEM_TRACE
                DecSKTDF_MEM_Usage(size);
#endif
            }

#ifdef MEM_TRACE
            uint64_t SKT_MEM_Usage(){
                return skt_usage_.load(std::memory_order_relaxed);
            }
            void IncSKT_MEM_Usage(uint32_t size){
                skt_usage_.fetch_add(size, std::memory_order_relaxed);
            }
            void DecSKT_MEM_Usage(uint32_t size){
                skt_usage_.fetch_sub(size, std::memory_order_relaxed);
            }
            uint64_t UK_MEM_Usage(){
                return uk_usage_.load(std::memory_order_relaxed);
            }
            void IncUK_MEM_Usage(uint32_t size){
                uk_usage_.fetch_add(size, std::memory_order_relaxed);
            }
            void DecUK_MEM_Usage(uint32_t size){
                uk_usage_.fetch_sub(size, std::memory_order_relaxed);
            }
            uint64_t COL_MEM_Usage(){
                return col_usage_.load(std::memory_order_relaxed);
            }
            void IncCOL_MEM_Usage(uint32_t size){
                col_usage_.fetch_add(size, std::memory_order_relaxed);
            }
            void DecCOL_MEM_Usage(uint32_t size){
                col_usage_.fetch_sub(size, std::memory_order_relaxed);
            }
            uint64_t SKTDF_MEM_Usage(){
                return sktdf_usage_.load(std::memory_order_relaxed);
            }
            void IncSKTDF_MEM_Usage(uint32_t size){
                sktdf_usage_.fetch_add(size, std::memory_order_relaxed);
            }
            void DecSKTDF_MEM_Usage(uint32_t size){
                sktdf_usage_.fetch_sub(size, std::memory_order_relaxed);
            }
            uint64_t KBPM_MEM_Usage(){
                return kbpm_usage_.load(std::memory_order_relaxed);
            }
            void IncKBPM_MEM_Usage(uint32_t size){
                kbpm_usage_.fetch_add(size, std::memory_order_relaxed);
            }
            void DecKBPM_MEM_Usage(uint32_t size){
                kbpm_usage_.fetch_sub(size, std::memory_order_relaxed);
            }
            uint64_t ITER_MEM_Usage(){
                return iter_usage_.load(std::memory_order_relaxed);
            }
            void IncITER_MEM_Usage(uint32_t size){
                iter_usage_.fetch_add(size, std::memory_order_relaxed);
            }
            void DecITER_MEM_Usage(uint32_t size){
                iter_usage_.fetch_sub(size, std::memory_order_relaxed);
            }
            void IncMissFlush() {
                flush_miss_++;
            }
            uint64_t GetMissFlush() {
                return flush_miss_;
            }
            void IncHitFlush() {
                flush_hit_++;
            }
            uint64_t GetHitFlush() {
                return flush_hit_;
            }
            void IncUpdateToClean() {
                update_to_clean_++;
            }
            uint64_t GetUpdateToClean() {
                return update_to_clean_;
            }
            void IncEvictedSKT() {
                evicted_skt_++;
            }
            uint64_t GetEvictedSKT() {
                return evicted_skt_;
            }

            void IncFlushToClean(uint64_t cnt) {
                flush_to_clean_ += cnt;
            }
            uint64_t GetFlushToClean() {
                return flush_to_clean_;
            }
            void IncFlushToDirty(uint64_t cnt) {
                flush_to_dirty_ += cnt;
            }
            uint64_t GetFlushToDirty() {
                return flush_to_dirty_;
            }
            void IncFlushSplitCnt(uint64_t cnt) {
                flush_split_inc_ += cnt;
            }
            uint64_t GetFlushSplitCnt() {
                return flush_split_inc_;
            }
            uint64_t GetPrWithIter(){
                return pr_iterbuild_cnt_.load(std::memory_order_relaxed);
            }
            void IncPrWithIter(){
                pr_iterbuild_cnt_.fetch_add(1, std::memory_order_relaxed);
            }
            uint64_t GetPrWithNext(){
                return pr_next_cnt_.load(std::memory_order_relaxed);
            }
            void IncPrWithNext(){
                pr_next_cnt_.fetch_add(1, std::memory_order_relaxed);
            }
            uint64_t GetClearPr(){
                return clear_pr_cnt_.load(std::memory_order_relaxed);
            }
            void IncClearPr(){
                clear_pr_cnt_.fetch_add(1, std::memory_order_relaxed);
            }
            uint64_t GetSkipPr(){
                return skip_pr_cnt_.load(std::memory_order_relaxed);
            }
            void IncSkipPr(){
                skip_pr_cnt_.fetch_add(1, std::memory_order_relaxed);
            }
            uint64_t GetEvictWithPr(){
                return evict_with_pr_;
            }
            void IncEvictWithPr(){
                evict_with_pr_++;;
            }
            uint64_t GetEvictIter(){
                return evict_iter_;
            }
            void IncEvictIter(){
                evict_iter_++;;
            }
            uint64_t GetEvictSKTIter(){
                return evict_skt_iter_;
            }
            void IncEvictSKTIter(){
                evict_skt_iter_++;;
            }
            uint64_t GetEvictStopSKT(){
                return evict_stop_skt_ref_;
            }
            void IncEvictStopSKT(){
                evict_stop_skt_ref_++;;
            }
            uint64_t GetEvictReclaim(){
                return evict_recalim_;
            }
            void IncEvictReclaim(){
                evict_recalim_++;
            }
#endif
            //For Flush
            uint64_t GetUpdateCount(){
                return update_cnt_.load(std::memory_order_relaxed);
            }
            uint64_t IncreaseUpdateCount(uint64_t update_cnt){
                return update_cnt_.fetch_add(update_cnt, std::memory_order_relaxed) + update_cnt;
            }

            void SetSKTableIDForEviction(uint32_t sktid){
                evict_sktable_id_.store(sktid, std::memory_order_seq_cst);
            }
            void ClearSKTableIDForEviction(){
                evict_sktable_id_.store(0, std::memory_order_seq_cst);
            }
            uint32_t GetSKTableIDForEviction(){
                return evict_sktable_id_.load(std::memory_order_seq_cst);
            }
#if 0
            void DecreaseUpdateCount(uint32_t size){
                update_cnt_.fetch_sub(size, std::memory_order_relaxed);
            }
#endif
            bool CheckKeyEvictionThreshold(void){ return (MemoryUsage() > kMaxCacheSize); }
            void InsertSKTableMemCache(SKTableMem* sktm, SKTCacheType type, uint16_t cache);
            void InsertSKTableMemCache(SKTableMem* sktm, SKTCacheType type);
            void SpliceCacheList(std::list<SKTableMem*> &skt_list, SKTCacheType type, bool to_front = false);
            void SwapSKTCacheList(std::list<SKTableMem*> &skt_list, SKTCacheType type);

            void SpliceKBPMCacheList(std::list<KeyBlockPrimaryMeta*> &kbpm_list);
            void SwapKBPMCacheList(std::list<KeyBlockPrimaryMeta*> &kbpm_list);
#if 0
            size_t SKTCountInSKTableMemCache(SKTCacheType type);
            SKTableMem* PopSKTableMem(SKTCacheType type); 
#endif
            bool IsSKTDirtyListEmpty(void){ return skt_cache_[kDirtySKTCache].cache.empty();}
            bool IsSKTCleanListEmpty(void){ return skt_cache_[kCleanSKTCache].cache.empty();}
            bool IsAllSKTCacheListEmpty(void){ 
                for(SKTCacheType t = kDirtySKTCache; t < kNrCacheType ; t++)
                    if(!skt_cache_[t].cache.empty()) return false;
                return true; 
            }
            bool IsSKTKBMUpdateListEmpty(void){ return kbpm_update_list_.cache.empty();}
            /////////////////////////////////////////// END  : Dirty/KBMUpdate/Clean Cache Management ///////////////////////////////////////////

            /////////////////////////////////////////// START: Cache Threads(Dirty/KBM update/Clean/Log) related functions ///////////////////////////////////////////
            uint64_t  PushKBMLog(uint16_t worker_id, KeyBlockPrimaryMeta* kbpm, uint64_t next_worker_ikey) {
                return logcleanup_data_[worker_id-1].PushKBML(kbpm, next_worker_ikey);
            }
            bool IsWorkerLogCleanupQueueEmpty()
            {
                for(int worker_id = 0; worker_id < kWriteWorkerCount; worker_id++){
                    WorkerData *logcleanup_queue = logcleanup_data_ + worker_id;
                    if(!logcleanup_queue->IsEmpty(0) || !logcleanup_queue->IsEmpty(1))
                        return false;
                }
                return true;
            }
            bool CheckExitCondition(){
                if(cache_thread_shutdown_.load(std::memory_order_relaxed) && IsAllSKTCacheListEmpty() && !IsCacheThreadsBusy() && IsWorkerLogCleanupQueueEmpty() && flushed_key_hashmap_->empty())
                    return true;
                return false;
            }

            bool CheckExitConditionExceptLog(){
                if(cache_thread_shutdown_.load(std::memory_order_relaxed) && IsAllSKTCacheListEmpty() && !IsCacheThreadsBusy())
                    return true;
                return false;
            }

            void SignalSKTFlushThread();
            void SignalKBPMUpdateThread();
            void SignalSKTEvictionThread(bool move_to_dirty = false);
            // Log cleanup
            void WorkerLogCleanupInternal(WorkerData &logcleanup_data, std::list<SequenceNumber> &worker_next_ikey_seq, std::queue<KeyBlockPrimaryMeta*> &kbpm_cleanup, SequenceNumber &smallest_ikey);
            void WorkerLogCleanupAndSubmission(std::queue<KeyBlockPrimaryMeta*> kbpm_cleanup, SequenceNumber &smallest_ikey);
            void LogCleanupInternal(LogRecordKey &GLogkey);
            //Update KBM
            uint32_t InsertUpdatedKBPMToMap(std::list<KeyBlockPrimaryMeta*> *kbpm_list, std::map<uint64_t, KeyBlockPrimaryMeta*> *kbpm_map);
            uint32_t BuildKBPMUpdateData(std::map<uint64_t, KeyBlockPrimaryMeta*> *kbpm_map, struct KBPMUpdateVector *kbpm_update, std::vector<InSDBKey> *delete_key_vec, std::deque<KeyBlockPrimaryMeta*> &updated_kbpm, char* kbpm_io_buf);
            //Eviction
            void EvictUserKeys(std::list<SKTableMem*> &skt_list, int64_t &reclaim_size, bool cleanup);

            void DecOffloadFlushCount();
            SKTableMem* GetFlushOffloadSKTable();

            void SKTFlushThread();
            void KBMUpdateThread();
            void SKTEvictionThread();

            static void SKTFlushThread_(void* manifest)
            { reinterpret_cast<Manifest*>(manifest)->SKTFlushThread(); }

            static void SKTEvictionThread_(void* manifest)
            { reinterpret_cast<Manifest*>(manifest)->SKTEvictionThread(); }

            static void KBMUpdateThread_(void* manifest)
            { reinterpret_cast<Manifest*>(manifest)->KBMUpdateThread(); }

            void CheckForceFlushSKT(void);
            void TerminateCacheThread();
            /////////////////////////////////////////// END  : Cache Threads(Dirty/KBM update/Clean) related functions ///////////////////////////////////////////

            /////////////////////////////////////////// START: Snapshot/Iterator related functions ///////////////////////////////////////////
            SnapshotImpl* GetSnapshotImpl(SimpleLinkedNode *node);
            //{ return node ? class_container_of(node, SnapshotImpl, snapshot_node_) : NULL; }
            Status GetSnapshotInfo(std::vector<SnapInfo> *snapinfo_list);
            SequenceNumber GetOldestSnapshotSequenceNumber();
            SequenceNumber GetLatestSnapshotSequenceNumber();
            SnapshotImpl* __CreateSnapshot();
            Iterator* CreateNewIterator(const ReadOptions& options, uint8_t col_id = 0);
            void DeleteIterator(Iterator *iter);
            bool __DisconnectSnapshot(SnapshotImpl* snap);
            Snapshot* CreateSnapshot();
            void DeleteSnapshot(Snapshot *handle);
            void CleanUpSnapshot();
            void IncIteratorCount() {
                nr_iterator_.fetch_add(1, std::memory_order_relaxed);
            }
            void DecIteratorCount() {
                if (nr_iterator_.load() < 1) printf("!!!! check nr_iterator_ count\n");
                nr_iterator_.fetch_sub(1, std::memory_order_relaxed);
            }
            /////////////////////////////////////////// END  : Snapshot related functions ///////////////////////////////////////////

            const Comparator* GetComparator(){ return options_.comparator; }
            CompressionType GetCompressionType(){ return options_.compression; }
            const uint32_t GetItrPrefetchHint(){ return options_.iter_prefetch_hint; }
            bool IsPrefixDetectionEnabled(){ return options_.prefix_detection; }
            bool IsCacheDisabled(){ return options_.disable_cache; }
            bool IsIOSizeCheckDisabled(){ return options_.disable_io_size_check; }
            const uint32_t GetRandomPrefetchHint(){ return options_.random_prefetch_hint; }


            /////////////////////////////////////////// START: Memory Allocation/Deallocation ///////////////////////////////////////////
            Slice* GetIOBuffer(uint32_t size) { return dio_buffer_.GetDIOBuffer(size); }
            Slice AllocSKTableBuffer(uint32_t size);
            void FreeSKTableBuffer(Slice &buffer);
            void CleanupFreeSKTableBuffer();
#if 0
            void RefillFreeUserValueCount(uint32_t size);
#endif
            UserValue* AllocUserValue(uint32_t size);
            void FreeUserValue(UserValue*);
            ColumnNode* AllocColumnNode();
            void FreeColumnNode(ColumnNode* col_node);
#if 0
            void RefillFreeUserKeyCount(uint32_t key_size);
#endif
            UserKey* AllocUserKey(uint32_t key_size);
            void FreeUserKey(UserKey* ukey);

            /* this is called from WriteBatchInternal::Store()*/
            RequestNode* AllocRequestNode();
            /* Worker Thread calls it after request submission */
            void FreeRequestNode(RequestNode* req_node);

            /* memory pool function */
            void CreateMemoryPool() {
                kb_pool_ = new MemoryPool<KeyBlock>(1024, this);
                kbpm_pool_ = new MemoryPool<KeyBlockPrimaryMeta>(1024, this);
            }
            void DestroyMemoryPool() {
                delete kb_pool_;
                delete kbpm_pool_;
            }

            KeyBlock *AllocKB() {
                return kb_pool_->AllocMemoryNode();
            }
            void FreeKB(KeyBlock *node) { 
                kb_pool_->FreeMemoryNode(node);
            }
            KeyBlockPrimaryMeta *AllocKBPM() { return kbpm_pool_->AllocMemoryNode(); }
            void FreeKBPM(KeyBlockPrimaryMeta * node) { kbpm_pool_->FreeMemoryNode(node); }


            /*Iterator Scratch Pad*/
            uint32_t GetMaxIterKeyBufferSize(void){
                return max_iter_key_buffer_size_;
            }
            uint32_t IncMaxIterKeyBufferSize(uint32_t cur_size){
                uint32_t new_size = 0;
                cur_size += (2*kMinSKTableSize - (cur_size % kMinSKTableSize)); /* reserve 4KB for cover keystring */
                if (cur_size < kMaxSKTableSize) {
                    new_size = kMaxSKTableSize;
                    if (((cur_size + kMinSKTableSize) < kMaxSKTableSize) &&
                            (cur_size + kMinSKTableSize > max_iter_key_buffer_size_))
                        max_iter_key_buffer_size_ = cur_size + kMinSKTableSize;
                }
                else new_size = cur_size << 2;
                return new_size;
            }
            uint32_t SetMaxIterKeyBufferSize(uint32_t max_size){
                uint32_t new_size = max_size  + (kMinSKTableSize - (max_size % kMinSKTableSize));
                if (new_size > max_iter_key_buffer_size_ &&  new_size < kMaxSKTableSize)
                    max_iter_key_buffer_size_ = new_size;
                return new_size;
            }
            uint32_t GetMaxIterKeyOffsetBufferSize(void){
                return max_iter_key_offset_buffer_size_;
            }
            uint32_t IncMaxIterKeyOffsetBufferSize(uint32_t cur_size){
                uint32_t new_size = 0;
                cur_size += (2*kMinSKTableSize - (cur_size % kMinSKTableSize)); /* reserve 4KB for cover entry */
                if (cur_size < kMaxSKTableSize) {
                    new_size = kMaxSKTableSize;
                    if (((cur_size + kMinSKTableSize) < kMaxSKTableSize) &&
                            (cur_size + kMinSKTableSize > max_iter_key_offset_buffer_size_))
                        max_iter_key_offset_buffer_size_ = cur_size + kMinSKTableSize;
                }
                else new_size = cur_size << 2;
                return new_size;
            }
            uint32_t SetMaxIterKeyOffsetBufferSize(uint32_t max_size){
                uint32_t new_size = max_size  + (kMinSKTableSize - (max_size % kMinSKTableSize));
                if (new_size > max_iter_key_buffer_size_ &&  new_size < kMaxSKTableSize)
                    max_iter_key_offset_buffer_size_ = new_size;
                return new_size;
            }

            ObjectAllocator<UserValue>* GetUVAllocator(uint16_t id){ return &uv_allocator_[id]; }
            SimpleObjectAllocator<ColumnNode>* GetCNAllocator(uint16_t id){ return &cn_allocator_[id]; }
            ObjectAllocator<UserKey>* GetUKAllocator(uint16_t id){ return &uk_allocator_[id]; }
            ObjectAllocator<RequestNode>* GetRequestNodeAllocator(uint16_t id){ return &request_node_allocator_[id]; }
            /////////////////////////////////////////// END  : Memory Allocation/Deallocation ///////////////////////////////////////////

            /////////////////////////////////////////// START: User Key & Value[UserKey/SKTDF/KBPM/KBML] Manageement ///////////////////////////////////////////

            KeyBlock* LoadValue(uint64_t insdb_key_seq, uint32_t ivalue_size, bool *from_readahead, KeyBlockPrimaryMeta* kbpm);
#ifdef USE_HOTSCOTCH
            bool InsertUserKeyToHopscotchHashMap(UserKey* uk);
            UserKey* FindUserKeyFromHopscotchHashMap(KeySlice& key);
            bool RemoveUserKeyFromHopscotchHashMap(KeySlice key);
#endif
#ifdef INSDB_USE_HASHMAP
            bool InsertUserKeyToHashMap(UserKey* uk);
            UserKey* FindUserKeyFromHashMap(KeySlice& key);
            bool RemoveUserKeyFromHashMap(KeySlice key);

            UserKey* LookupUserKeyFromFlushedKeyHashMap(const KeySlice &key); 
            bool InsertUserKeyToFlushedKeyHashMap(UserKey* uk);
            bool RemoveUserKeyFromFlushedKeyHashMap(const KeySlice& key);
            void FlushedKeyHashMapFindAndMergeUserKey(UserKey* new_uk);
            KeyHashMap* GetFlushedKeyHash() {
                return flushed_key_hashmap_;
            }
            void FlushUserKey(std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue, std::list<KeyBlockPrimaryMeta*> &kbpm_list);

            void SetLastColInfoTableKey(InSDBKey last_cit_key){
                last_cit_key_.store(last_cit_key, std::memory_order_relaxed);
            }

            InSDBKey GetLastColInfoTableKey(void){
                return last_cit_key_.load(std::memory_order_relaxed);
            }

            void FlushSingleSKTableMem(SKTableMem* sktable, std::string &col_info_table, Slice &comp_col_info_table);
            void WorkerThreadFlushSKTableMem(std::string &col_info_table, Slice &comp_col_info_table);
            void DoCongestionControl();
            void ReleaseCongestionControl();

            bool InsertKBPMToHashMap(KeyBlockPrimaryMeta *kbpm);
            KeyBlockPrimaryMeta* FindKBPMFromHashMap(SequenceNumber ikey);
            void RemoveKBPMFromHashMap(SequenceNumber ikey);
            bool KeyHashMapFindOrInsertUserKey(UserKey*& uk, const KeySlice& key, const uint8_t col_id );
#endif

            UserKey* GetFirstUserKey();
            UserKey* GetLastUserKey();

            UserKey* SearchUserKey(const Slice& key);
            UserKey* SearchLessThanEqualUserKey(const Slice& key);

            bool InsertUserKey(UserKey* ukey, bool begin_key = false);
            KeyBlockPrimaryMeta* GetKBPMWithiKey(SequenceNumber seq, uint8_t offset);

            SequenceNumber GetLatestSequenceFromKeymap(const KeySlice& key, uint8_t col_id, bool &deleted);

            Status GetValueFromKeymap(const KeySlice& key, std::string* value, PinnableSlice* pin_slice, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time);
            /////////////////////////////////////////// END  : UserKey/KBPM/KBML Manageement ///////////////////////////////////////////
            
            /////////////////////////////////////////// START: Transaction(Group) Management ///////////////////////////////////////////
            /* Transaction Management functions */
            bool IsTrxnGroupCompleted(TrxnGroupID tgid);
            TransactionID GetTrxnID(uint32_t count);
            void DecTrxnGroup(TrxnGroupID tgid, uint32_t cnt);
            bool IsCompletedTrxnGroupRange(std::list<TrxnGroupRange> &completed_trxn_group_list, TrxnGroupID tgid);
            //bool IsCompletedTrxnGroupRange(TrxnGroupID tgid);
            void AddCompletedTrxnGroupRange(TrxnGroupID tgid);
            void CopyCompletedTrxnGroupRange(std::list<TrxnGroupRange> &completed_trxn_groups);
            std::unordered_map<TrxnGroupID, uint32_t> &GetUncompletedTrxnGroup() { return uncompleted_trxn_group_; }
            /////////////////////////////////////////// END  : Transaction(Group) Management ///////////////////////////////////////////

            /////////////////////////////////////////// START: TTL(Time To Live) Management ///////////////////////////////////////////
            Status SetTtlList(uint64_t* TtlList, bool* TtlEnabledList) {
              for (int i = 0; i < kMaxColumnCount; i++) {
                TtlList_[i] =  TtlList[i];
                TtlEnabledList_[i] = TtlEnabledList[i];
              }
              return Status::OK();
            }
            Status SetTtlInTtlList(uint16_t id, uint64_t ttl) {
              if (id > kMaxColumnCount)
                return Status::NotFound("Column family is not found in insdb.");
              TtlList_[id] = ttl;
              TtlEnabledList_[id] = true;
              return Status::OK();
            }
            uint64_t GetTtlFromTtlList(uint16_t id) { return  TtlList_[id]; }
            bool TtlIsEnabled(uint16_t id) { return TtlEnabledList_[id]; }
            /////////////////////////////////////////// END  : TTL(Time To Live) Management ///////////////////////////////////////////

            void PrintParam();
#if 0
            Slice PerThreadAllocRawMemoryNodeWrapper(uint32_t size) {
                return thread_memory_pool_.PerThreadAllocRawMemoryNode(size);
            }

            void PerThreadFreeRawMemoryNodeWrapper(Slice node) {
                thread_memory_pool_.PerThreadFreeRawMemoryNode(node);
            }
#endif


        private:
            Status CleanUpSKTable(int skt_id, int skt_size, int hash_size,  DeleteInSDBCtx *Delctx, std::set<uint64_t> &keylist);
            // No copying allowed
            Manifest(const Manifest&);
            void operator=(const Manifest&);
    }; //Manifest

#define ADDRESS_SHIFT           16
#define ADDRESS_MASK            0x0000FFFFFFFFFFFF
    enum KeyBlockMetaType{
        kKeyBlockPrimary = 0,
        kKeyBlockMetaLog = 1,
    };


#define DEL_FLAG 0x80
#define COL_INFO_SIZE_MASK 0x7F
#define TTL_BIT 0x80
#define USERVALUE_FLAG 0x8000000000000000UL
#define KB_OFFSET_LOC 2
#define TTL_FLAG_LOC 2
    class ColumnNode{// 64 Byte
        private:
            friend class UserKey;
            SimpleObjectAllocator<ColumnNode> *allocator_;

            volatile UserKey* uk_;                                       //1byte
            volatile RequestNode* req_node_;                                       //1byte
            TrxnGroupID tgid_;                                       //1byte
            uint64_t ikey_seq_;
            uint64_t end_seq_;
            uint32_t ivalue_size_;
            /*
             * col_info_ : It will be copied to node in keymap
             * ||userkey(1bit) used in keymap | del type(1bit) | col id(6bit) | ttl flag(1bit)
             *     | KB offset(7bit) | iter seq #(varint64) | (TTL(varint64)) | ikey(varint64) | KB Size(varint32)||
             */
            std::string col_info_;
            uint64_t uvalue_or_kbm_ ;                                       //8byte
            volatile uint8_t flags_;                                       //1byte
            volatile uint8_t col_pinned_; //adjust it with lock                                       //1byte
            SimpleLinkedNode col_list_node_;                                //16byte
            /*Set the ikey_seq_ with ColumnLock*/
        public:

            /*Non-atomic*/
            const static uint8_t flags_submitted                       = 0x01;
            /* Indicates whether it is trxn or not(:Used before cache insertion for Write) */
            const static uint8_t flags_dropped                         = 0x04; /*Access with ColumnLock*/
            const static uint8_t flags_overwritten                     = 0x08;
            const static uint8_t flags_trxn_committed                  = 0x10; // Set it under lock
            /* Split Vertical merge because of different Trxn Group ID(:Used before cache insertion for Write) */
            const static uint8_t flags_pending_request                 = 0x20; // Set it under lock
            const static uint8_t flags_delete                          = 0x40; // Set it under lock

            ColumnNode() : allocator_(NULL), uk_(NULL), req_node_(NULL), tgid_(0), ikey_seq_(0), end_seq_(0), ivalue_size_(0), col_info_(), uvalue_or_kbm_(0), flags_(0), col_pinned_(0), col_list_node_(){ }
            ~ColumnNode(){ assert(!GetUserValue()); }

            void ExistColInit(Slice &data, UserKey *uk) {
                uk_ = uk;
                assert(!req_node_);
                tgid_ = 0;
                ikey_seq_ = 0;
                end_seq_ = ULONG_MAX;
                ivalue_size_ = 0;
                col_info_.resize(0);
                col_info_.append(data.data(), data.size());

                uvalue_or_kbm_ = 0;
                flags_ = flags_trxn_committed;
                col_pinned_ = 0;

                col_list_node_.next_ = col_list_node_.prev_ = nullptr;
            }
            /* Call from Store function(New column)  */
            void NewColumnInit(UserKey* uk, RequestType type, uint8_t col_id, uint64_t ttl, SequenceNumber seq, uint32_t data_size/*For estimating KB size : key size + value size*/) {
                uint8_t size = 0;
                uk_ = uk;
                req_node_ = NULL;
                tgid_ = 0;
                ikey_seq_ = 0;
                end_seq_ = ULONG_MAX;
                ivalue_size_ = 0;
                /* For size Info */
                col_info_.resize(1);
                /* For Column Size */
                col_info_.append(1, col_id);
                if(ttl){
                    col_info_.append(1, TTL_BIT);/* ttl bit + KB offset */
                    size = PutVarint64(&col_info_, seq);
                    PutVarint64(&col_info_, ttl);
                    data_size += 8/*sizeof(ttl)*/;
                }else{
                    col_info_.append(1, 0);
                    size = PutVarint64(&col_info_, seq);
                }

                //uvalue_or_kbm_ = 0; // UserValue has been set
                flags_ = flags_pending_request;
                col_pinned_ = 0;

                size += col_info_.size();
                if(type == kDelType){
                    assert(size < DEL_FLAG);
                    size |= DEL_FLAG;
                }else{
                    size += VarintLength(data_size + kMaxInternalValueSize)/*Max iKey size*/;
                    assert(size < DEL_FLAG);
                }
                col_info_[0] =  size;
                col_list_node_.next_ = col_list_node_.prev_ = nullptr;
            }

            void ColPin(){ col_pinned_++; }
            void ColUnpin(){ col_pinned_--; }
            uint8_t ColGetpin(){ return col_pinned_; }

            void SetAllocator(SimpleObjectAllocator<ColumnNode> *allocator) { allocator_ = allocator; }
            SimpleObjectAllocator<ColumnNode> *GetAllocator() { return allocator_; }

            void SetTGID(TrxnGroupID tgid){ tgid_ = tgid; }
            TrxnGroupID GetTGID(void){ return tgid_; }
            /* call from SKT flush function(Existing column)*/

            void UpdateUserKey(UserKey* uk){ uk_ = uk; }
            UserKey* GetUserKey(){ return (UserKey*)uk_; }


            void SetSubmitted(){ flags_ |= flags_submitted; }
            bool IsSubmitted(){ return (flags_ & flags_submitted); }

            void DropColumnNode(){ flags_ |= flags_dropped; }
            bool IsDropped(){ return (flags_ & flags_dropped); }

            bool IsOverwritten(){ return flags_ & flags_overwritten; }
            void SetOverwritten(){ flags_ |= flags_overwritten; }

            /*
             * Call these functions under column lock in DeviceFormatBuilder()
             */

            void SetTRXNCommitted(){ flags_ |= flags_trxn_committed; }
            bool IsTRXNCommitted(){ return flags_ & flags_trxn_committed; }

            void ClearPendingRequest(){ flags_ &= ~flags_pending_request; }
            bool IsPendingRequest(){ return (flags_ & flags_pending_request); }

            void SetRequestNode(RequestNode* req_node){
                req_node_ = req_node;
            }
            RequestNode* GetRequestNode(){
                return (RequestNode*)req_node_;
            }

            RequestType GetRequestType() {
                /* UserKey bit is only used in keymap */
                if(col_info_[0] & DEL_FLAG)
                    return kDelType;
                return kPutType;
            }
            uint8_t GetRequestTypeAndColID() {
                /* UserKey bit is only used in keymap */
                return  *col_info_.data();
            }

            void RemoveUserValue(){
                if(!(USERVALUE_FLAG & uvalue_or_kbm_)) abort();
                uvalue_or_kbm_ = 0;
            }

            void InsertUserValue(UserValue* uv) {
                //assert(!uvalue_or_kbm_);
                assert((uint64_t)uv < ADDRESS_MASK);
                uvalue_or_kbm_  = (USERVALUE_FLAG | ((uint64_t)uv) ) ;
            }

            UserValue* GetUserValue() {
                if(uvalue_or_kbm_ & USERVALUE_FLAG)
                    return (UserValue*)(uvalue_or_kbm_& ADDRESS_MASK);
                return NULL;
            }
            void InvalidateUserValue(Manifest *mf) {
                UserValue* uv = GetUserValue();
                if(uv){
                    RemoveUserValue();
                    mf->FreeUserValue(uv);
                }
            }

            void SetKeyBlockMeta(KeyBlockPrimaryMeta* kbm){
                assert((uint64_t)kbm == uvalue_or_kbm_ || !uvalue_or_kbm_);
                if (!kbm) abort();
                uvalue_or_kbm_ =  ((uint64_t)kbm);
            }
            bool HasUserValue(){
                return (uvalue_or_kbm_ & USERVALUE_FLAG);
            }
            KeyBlockPrimaryMeta* GetKeyBlockMeta(){
                if(uvalue_or_kbm_ & USERVALUE_FLAG) return nullptr;
                return (KeyBlockPrimaryMeta*)(uvalue_or_kbm_);
            }
            void RemoveKeyBlockMeta(){
                if(uvalue_or_kbm_ & USERVALUE_FLAG) abort();
                uvalue_or_kbm_ = 0;
            }

            /**
             * Prev & Next functions are only used in Iterator/Snapshot, which means that it does not need to increase UserKey->ref_cnt_
             * because FlushSKTableMem does not delete the iterator key.
             */
            /**
             * TODO CAUTION Return locked ColumnNode
             */

            /////////////// Implement KBM/KBML related functions at here //////////////////
            //
            SimpleLinkedNode* GetColumnNodeNode(){ return &col_list_node_; }
            static ColumnNode *GetColumnFromLink(SimpleLinkedNode *node) { return class_container_of(node, ColumnNode, col_list_node_); }

            // Call Update related functions after locking ColumnNode
            uint64_t GetInSDBKeySeqNum(){
                if(ikey_seq_) return ikey_seq_;

                uint64_t ikey_seq;
                Slice col_info(col_info_);
                bool ttl = *(col_info.data() + TTL_FLAG_LOC) & TTL_BIT;
                col_info.remove_prefix(TTL_FLAG_LOC+1);
                GetVarint64(&col_info, &ikey_seq);//to skip iter seq number
                if(ttl)
                    GetVarint64(&col_info, &ikey_seq);//to skip TTL

                if(GetVarint64(&col_info, &ikey_seq))//Get ikey seq number
                    ikey_seq_ = ikey_seq;
                return ikey_seq_;//ikey_seq has not been assigned yet!

            }
            /*
             * It will be relocated to KBML
             * Transaction GetTrxnInfo() { return trxn_; }
             */

#define SEQNUM_LOC (TTL_FLAG_LOC+1)
#if 0
            SequenceNumber  GetBeginSequenceNumber() {
                SequenceNumber seq = 0;
                Slice col_info(col_info_);
                col_info.remove_prefix(SEQNUM_LOC);
                GetVarint64(&col_info, &seq);
                return seq;
            }
#else
            SequenceNumber GetBeginSequenceNumber();
#endif
            void SetEndSequenceNumber(SequenceNumber seq) {
                end_seq_ = seq;
            }
            SequenceNumber  GetEndSequenceNumber() {
                return end_seq_;
            }

            /* Time To Live */
            uint64_t GetTS() {
                uint64_t ttl = 0;

                Slice col_info(col_info_);
                col_info.remove_prefix(TTL_FLAG_LOC);
                if(*col_info.data() & TTL_BIT){
                    col_info.remove_prefix(1);
                    GetVarint64(&col_info, &ttl);//to skip iter seq number
                    GetVarint64(&col_info, &ttl);//Get TTL
                }
                return ttl;
            }
            /*
             * SKTable keeps largest iValue Size in the its range.
             *
             * inline void UpdateiValueSize(uint32_t ivalue_size) { ivalue_size_ = ivalue_size; }
             * uint32_t GetiValueSize() { return (ivalue_size_and_flags_and_offset_.load(memory_order_seq_cst) >> IVALUE_SIZE_SHIFT); }
             */
            uint32_t GetiValueSize() {
                if (ivalue_size_) return ivalue_size_;
                uint64_t temp= 0;

                Slice col_info(col_info_);
                col_info.remove_prefix(TTL_FLAG_LOC);
                if(*col_info.data() & TTL_BIT){
                    col_info.remove_prefix(1);
                    GetVarint64(&col_info, &temp);//to skip iter seq number
                    GetVarint64(&col_info, &temp);//to skip TTL
                }else{
                    col_info.remove_prefix(1);
                    GetVarint64(&col_info, &temp);//to skip iter seq number
                }
                GetVarint64(&col_info, &temp);//to skip ikey seq number
                GetVarint32(&col_info, &ivalue_size_);//Get KB size
                return ivalue_size_;

            }

            uint8_t GetKeyBlockOffset(){
                return (uint8_t) (*(col_info_.data()+KB_OFFSET_LOC) & ~TTL_BIT);
            }
            void SetKeyBlockOffset(uint8_t offset){
                assert(offset < kMaxRequestPerKeyBlock);
                col_info_[KB_OFFSET_LOC] += offset;
            }

            void UpdateColumnNode(Manifest *mf, uint32_t kb_size, uint64_t ikey_seq, KeyBlockPrimaryMeta *kbpm);
            void PrintColumnNodeInfo(std::string &str);

            uint8_t GetColInfoSize(){
                uint8_t size = 0;
                size = (col_info_[0] & COL_INFO_SIZE_MASK) ;
                if(size < 8) size = 8;
                return size;

            }
            std::string* GetColInfo(){
                return &col_info_;
            }
        private:
            // No copying allowed
            ColumnNode(const ColumnNode&);
            void operator=(const ColumnNode&);
    }/*ColumnNode*/;



    /*
     * User Address Space in Linux System is 0x0 ~ 0x0000 7FFF FFFF FFFF
     */
#define ITER_KEY_BUFFER_FLAG        0x80000000
#define ITER_KEY_BUFFER_OFFSET_MASK 0x7FFFFFFF
    class UserKey /*: public SKLData<KeySlice, UserKeyComparator> */{ /* 32 Byte */
        private:
            /*  Colocating a column's data into adjacent memory space.
             *  For CPU Cache Prefetch Optimization
             *  Access seq : latest_col_node_ => col_lock_ => col_list_
             */
            enum MoveDirection {
                kForward= 0,
                kBackward = 1,
            };
        public:
            SimpleLinkedNode link_;
            class ColumnData{ // 40 Byte( Valid 38 Byte)
                public:
                    friend class UserKey;
                    ColumnData() : latest_col_node_(NULL), col_list_(){}
                    ColumnNode* GetLatestColumnNode(){ return latest_col_node_; }
                    /* It returns the last TGID and save current TGDI */
                    void SetLatestColumnNode(ColumnNode* new_col_node){ latest_col_node_ = new_col_node; }
#ifndef NDEBUG
                    bool col_list_size() { return col_list_.Size(); }
#endif
                    SimpleLinkedList* GetColumnList() { return &col_list_; }
                private:
                    /*
                     * Purpose of latest_col_node_
                     *  1. Cache : most of get request access the latest column.
                     *           : In this case, we can avoid to access list which need to aquire lock.
                     *  2. Get value size, request type, and TRXN Group info for key packing
                     *     : A Worker Thread needs to know these data to decide # of requests in a Key Block.
                     */
                    ColumnNode *latest_col_node_;   // 8 Byte
                    SimpleLinkedList col_list_;     // 24Byte
                    /* To protect ColumnNode Modification such as get user value */
            };
        private:
            Spinlock col_lock_;             // 1 Byte
            std::atomic<uint16_t> ref_cnt_;                             //8byte
            uint16_t key_buffer_size_;
            char* key_buffer_;
            KeySlice user_key_;                                                         //8byte
            std::atomic<bool> in_hash;
            /*
             * Make a structure for (col_date_ ~ lock_) to secure KeySlice Space within 32 byte UserKey
             * If we use "user_key_and_keysize_" instead of "KeySlice", we have to allocate & copy 16 byte KeySlice for GetKey()
             * for every accesses.
             *
             * But the user will get the pointer of "col_data & skt" before use it. and than accessing the object using the pointer
             */
            ColumnData *col_data_;                                                                  // 8byte

            ObjectAllocator<UserKey>* allocator_;

        public:
            /* For Free Key Registration */
            UserKey(uint16_t key_size, ObjectAllocator<UserKey>* allocator) :
                col_lock_(),
                ref_cnt_(0), // Increase reference count for new key
                key_buffer_size_(0),
                key_buffer_(NULL),
                user_key_(),
                in_hash(false),
                col_data_(NULL),
                allocator_(allocator),
                link_()
        {
            col_data_= new ColumnData[kMaxColumnCount];
            key_buffer_ = (char*)malloc(key_size+1);
            key_buffer_size_ = key_size;
        }

       UserKey(uint32_t key_size) :
                col_lock_(),
                ref_cnt_(0), // Increase reference count for new key
                key_buffer_size_(0),
                key_buffer_(NULL),
                user_key_(),
                in_hash(false),
                col_data_(NULL),
                allocator_(NULL),
                link_()
        {
            col_data_= new ColumnData[kMaxColumnCount];
            key_buffer_ = (char*)malloc(key_size+1);
            key_buffer_size_ = (uint16_t)key_size;
        }

            ~UserKey(){
                //assert(!IsDirty());
                /* UserKey::Cleanup() must be executed before calling UserKey Destructor */
                assert(col_data_);
                assert(key_buffer_);
                delete [] col_data_;
                free(key_buffer_);
            }

#ifndef CODE_TRACE
            void ScanColumnInfo(std::string &str);
#endif
            bool BuildColumnInfo(Manifest *mf, uint16_t size, Slice buffer, SequenceNumber &largest_ikey, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue);
            void InitUserKey(const KeySlice& user_key);
            ColumnNode* GetColumnNodeContainerOf(SimpleLinkedNode *ikey_node);

            void ColumnLock(uint8_t col_id = 0){ col_lock_.Lock(); }
            /*bool ColumnTryLock(uint8_t col_id) { return col_lock_.TryLock(); }*/
            void ColumnUnlock(uint8_t col_id = 0){ col_lock_.Unlock(); }

            /* It returns the last TGID and save current TGDI */
            void SetLatestColumnNode(uint8_t col_id, ColumnNode* new_col_node){ col_data_[col_id].SetLatestColumnNode(new_col_node); }
            ColumnNode* GetLatestColumnNode(uint8_t col_id){ return col_data_[col_id].GetLatestColumnNode(); }
            // SKLData Skiplist interface
            //void SetNode(void* node) { internal_node_.Release_Store(node); }
            //void* GetNode() { return internal_node_.Acquire_Load(); }
            SimpleLinkedNode *GetLink() { return &link_; }
            /* Key & key size is in same cache line */
#define HASH_OFFSET 0
#define USERKEY_OFFSET 4
            KeySlice GetKeySlice() {
                return user_key_;
            }

            bool CompareKey(UserKeyComparator comp, const KeySlice& key, int& delta) {
                delta = comp(user_key_, key);
                return true;
            }


            uint32_t GetKeySize() { return user_key_.size(); }
            uint32_t GetHashValue() { return user_key_.GetHashValue(); }

            bool IsInHash(){
                return in_hash.load(std::memory_order_seq_cst);
            
            }

            int Compare(const Slice& key, Manifest *mf);

            SimpleLinkedList* GetColumnList(uint8_t col_id)
            {
                return col_data_[col_id].GetColumnList();
            }
            SequenceNumber GetLatestColumnNodeSequenceNumber(uint8_t col_id, bool &deleted);
            ColumnNode* GetColumnNode(uint16_t col_id, bool& deleted, uint64_t cur_time = -1, SequenceNumber seq_num = -1, uint64_t ttl = 0);
            void DoVerticalMerge(RequestNode *req_node, UserKey* uk, uint16_t col_id, uint8_t worker_id, uint8_t idx, Manifest *mf, KeyBlockPrimaryMeta* kbml);
            bool InsertColumnNode(ColumnNode* input_col_node, uint16_t col_id, bool front_merge = false/*For the UserKey in iterator hash*/);
            void InsertColumnNodes(Manifest *mf, uint8_t nr_column, Slice &col_info);
            void UserKeyMerge(UserKey *old_uk, bool front_merge = false/*For the UserKey in iterator hash*/);
            bool UpdateColumn(Manifest *mf, RequestNode*& req_node, ColumnNode* new_col_node, uint16_t col_id, bool force, TrxnGroupID &old_tgid);
            /**
             * All caller that get UserKey must increase ref_cnt_ before use and decrease ref_cnt_ after use
             * except SKTable Load/Flush functions, Get UserKEy APIs & Iterator
             *
             * < Usage >
             ukey->IncreaseReferenceCount();
             if(uk->IsTryingToDelete()){
             ukey->DecreaseReferenceCount();
             goto retry; //if flush detects it is referenced try_delete_ can be changed to false after retry
             }
             Do something ..
             ukey->DecreaseReferenceCount();

*/
            /**
             * In FlushSKTableMem, if a UserKey is seleted to delete, set try_delete_ and then check ref_cnt_
             * try_delete_ prevents to use this key.
             */


            uint16_t GetReferenceCount(){ return (uint16_t)(ref_cnt_.load(std::memory_order_relaxed)); }
            void IncreaseReferenceCount(uint8_t cnt = 1)
            {
#ifndef REFCNT_SANITY_CHECK
                ref_cnt_.fetch_add(cnt, std::memory_order_relaxed);
#else
                uint16_t old = ref_cnt_.fetch_add(cnt, std::memory_order_relaxed);
                if (old+1==0) abort();
#endif
            }
            uint16_t DecreaseReferenceCount()
            {
#ifndef REFCNT_SANITY_CHECK
                return (ref_cnt_.fetch_sub(1, std::memory_order_relaxed));
#else
                uint16_t ref_cnt = (ref_cnt_.fetch_sub(1, std::memory_order_seq_cst));
                if (ref_cnt==0) abort();
                return ref_cnt;
#endif
            }

            uint16_t DecreaseReferenceCount(uint16_t dec)
            {
#ifndef REFCNT_SANITY_CHECK
                return (ref_cnt_.fetch_sub(dec, std::memory_order_relaxed));
#else
                uint16_t ref_cnt = (ref_cnt_.fetch_sub(dec, std::memory_order_seq_cst));
                if (ref_cnt==0) abort();
                return ref_cnt;
#endif
            }

            uint16_t GetKeyInfoSizeForNew(){
                ColumnNode* col_node = NULL;
                SimpleLinkedList* col_list = NULL;
                SimpleLinkedNode* col_list_node = NULL;
                uint16_t col_info_size = 0;
                for( uint8_t col_id = 0 ; col_id < kMaxColumnCount ; col_id++){

                    ColumnLock(col_id);
#if 1
                    if(col_node = col_data_[col_id].latest_col_node_){
                        col_info_size += col_node->GetColInfoSize();
                    }
#else
                    col_list = GetColumnList(col_id);
                    col_list_node = col_list->newest();
                    if(col_list_node) {
                        col_node = GetColumnNodeContainerOf(col_list_node);
                        col_info_size += col_node->GetColInfoSize() + 1/*To store col info size*/;
                    }
#endif
                    ColumnUnlock(col_id);
                }
                col_info_size++;
                return col_info_size;
            }

            uint16_t GetKeyInfoSize(Manifest *mf){
                in_hash.store(false, std::memory_order_seq_cst);
                if(GetReferenceCount()){
                    in_hash.store(true, std::memory_order_seq_cst);
                    return 0; // If the UserKey has ref count, does not move the key to flushed_key_hash
                }
                ColumnNode* col_node = NULL;
                SimpleLinkedList* col_list = NULL;
                SimpleLinkedNode* col_list_node = NULL;
                uint16_t col_info_size = 0;
                for( uint8_t col_id = 0 ; col_id < kMaxColumnCount ; col_id++){
#if 1
                    if(col_node = col_data_[col_id].latest_col_node_){
                        col_info_size += col_node->GetColInfoSize();
                        if(!col_node->IsTRXNCommitted() && col_node->GetTGID() && !mf->IsTrxnGroupCompleted(col_node->GetTGID())){
                            in_hash.store(true, std::memory_order_seq_cst);
                            return 0; // If TRXN is not completed yet, does not move the key to flushed_key_hash
                        }
                    }

#else
                    col_list = GetColumnList(col_id);
                    col_list_node = col_list->newest();
                    if(col_list_node) {
                        col_node = GetColumnNodeContainerOf(col_list_node);
                        col_info_size += col_node->GetColInfoSize() + 1/*To store col info size*/;
                    }
#endif
                }
                col_info_size++;
                return col_info_size;
            }




            /*
             * In key remove/key insert function, The caller always knows whether the key is begin key or not.
             * return UserKey : No eviction in pregress
             * return NULL    : eviction in progress
             */
            size_t GetSize();
            void SetAllocator(ObjectAllocator<UserKey>* allocator){ allocator_ = allocator; }
            ObjectAllocator<UserKey>* GetAllocator(){ return allocator_; }

        private:
            // No copying allowed
            UserKey(const UserKey&);
            void operator=(const UserKey&);
    }/*UserKey */;

    /* 
     * Allocate fixed size memory during DB initialization
     * And then reuse the allocated memory until DB termination 
     */

#define COLUMN_NODE_REF_BIT 0xC0
#define COLUMN_NODE_ID_MASK      0x3F
#define COLUMN_NODE_HAS_TTL_BIT 0x80
#define COLUMN_NODE_OFFSET_MASK 0x7F

#define NR_COL_MASK  0x3F
#define RELOC_KEY_INFO  0x80
#define USE_USERKEY     0x40

    class SKTableMem : public SKLData<KeySlice, UserKeyComparator>{// 104 Byte
        private:
            friend class Manifest;
            Spinlock keyqueue_lock_;
            volatile uint8_t active_key_queue_;
            /*Two Queue : Active & Inactive key queue*/
            SimpleLinkedList *key_queue_/*[2]*/;

            folly::SharedMutex keymap_mu_;
#if 1
            folly::SharedMutex keymap_random_read_mu_;
#endif
            std::atomic<uint8_t> random_read_wait_cnt_;                             //8byte
            //GCInfo gcinfo_;
            Keymap *keymap_;
            uint32_t keymap_node_size_; /* size of keymap on device */
            uint32_t keymap_hash_size_; /* size of keymap hash on device */
            uint32_t keymap_mem_size_;
            /* Mutex lock for loading SKTable Device Format from device(KVSSD) */
            port::Mutex sktdf_load_mu_;                    // 40 Byte
            /* Shared lock for replacing the SKTDF in FlushSKTableMem()
             * No one can fetch a data from SKTDF during replacing SKTDF 
             */
            //folly::SharedMutex sktdf_update_mu_;

#if 0
            port::Mutex build_iter_mu_;                    // 40 Byte
            port::Mutex iter_mu_;                    // 40 Byte
#endif

            std::list<uint32_t> flush_candidate_key_deque_;
            /* the keys that are moved to sub-SKTable */

            port::AtomicPointer internal_node_;             // 8 Byte
            /* Spinlock for fetch a entry(s) from SKTable Device Format */
#if 0
            Spinlock sktdf_fetch_spinlock_;
#endif
            std::atomic<uint16_t> flags_;                    // 1 Byte
            /**
             * The sorted_list_ is used for the flush
             *  1. Find next candidata for flush(previous valid SKTable : backward Round-Robin )
             *  2. Find the prev SKTable after a SKTable becomes empty and removed from the skiplist of the SKTable.
             *  3. To check whether the next SKTable is empty.
             */

            SimpleLinkedNode skt_sorted_node_;              // 16Byte

            /* The skt_id may not change while updating the same key many times.(consume many ikey seq num) */
            uint32_t skt_id_; // 32 bit sktid.  // 4 Byte
            std::atomic<uint32_t> ref_cnt_;                    // 4 Byte

        public:
            SKTableMem(Manifest* mf, KeySlice& key, uint32_t skt_id, uint32_t keymap_size, uint32_t keymap_hash_size);
            SKTableMem(Manifest *mf, Keymap *kemap);
            ~SKTableMem();

            void SetSKTableID(uint32_t skt_id);
            void SetPreAllocSKTableID(uint32_t skt_id);
            void SetNextSKTableID(uint32_t skt_id);
            uint32_t  IncCurColInfoID();
            void SetPrevColInfoID();
            uint32_t GetSKTableID() { return skt_id_; }
            uint32_t GetPreAllocSKTableID();
            uint32_t GetNextSKTableID();
            uint32_t GetPrevColInfoID();
            uint32_t GetCurColInfoID();

            void SetKeymapNodeSize(uint32_t size, uint32_t hash_data_size) { 
                keymap_node_size_ = size;
                keymap_hash_size_ = hash_data_size;
            }
            uint32_t GetKeymapNodeSize() { return keymap_node_size_; /* size of keymap on device */ }
            uint32_t GetKeymapHashSize() { return keymap_hash_size_; /* size of keymap hash on device */ }
            void SetKeymapMemSize(uint32_t size) { keymap_mem_size_ = size;} 
            uint32_t GetKeymapMemSize() { return keymap_mem_size_; /* size of keymap in memory */ }
            KeySlice GetBeginKeySlice() { return keymap_->GetKeymapBeginKey(); }
            void DeleteSKTDeviceFormatCache(Manifest* mf, bool skip = false);

            bool NeedToSplit() {
                assert(keymap_->HasArena());
                // If size of keymap is bigger than 2 times of kMaxSKTableSize, split the SKTable
                if(keymap_->GetAllocatedSize() - keymap_->GetFreedMemorySize() > kMaxSKTableSize * 2) return true;
                return false;
            }


            void InsertArena(Arena* arena) {
                keymap_->InsertArena(arena);
            }
            void InsertArena(Arena* arena, KeyMapHashMap* hashmap) {
                keymap_->InsertArena(arena, hashmap);
            }
            Arena* MemoryCleanupSKTDeviceFormat(Manifest* mf, KeyMapHashMap* hashmap) {
                return keymap_->MemoryCleanup(mf, hashmap);
            }
            bool IsActiveKeyQueueEmpty(){
                keyqueue_lock_.Lock();
                bool empty = (key_queue_[active_key_queue_].empty());
                keyqueue_lock_.Unlock();
                return empty;
            }
            bool IsInactiveKeyQueueEmpty(){
                return (key_queue_[active_key_queue_^1].empty());
            }
            bool IsKeyQueueEmpty(){
                return (key_queue_[0].empty() && key_queue_[1].empty());
            }
            void PushUserKeyToActiveKeyQueue(UserKey* uk){
                keyqueue_lock_.Lock();
                key_queue_[active_key_queue_].InsertBack(uk->GetLink());
                keyqueue_lock_.Unlock();
            }
            void PushUserKeyToInactiveKeyQueue(UserKey* uk){
                /*No lock is needed*/
                //keyqueue_lock_.Lock();
                key_queue_[active_key_queue_^1].InsertBack(uk->GetLink());
                //keyqueue_lock_.Unlock();
            }
            size_t KeyQueueKeyCount(){
                return (key_queue_[0].Size() + key_queue_[1].Size());
            }
            uint8_t ExchangeActiveAndInActiveKeyQueue(){
                keyqueue_lock_.Lock();
                /*Inactive queue must be empty!*/
                assert(key_queue_[active_key_queue_ ^ 1].empty());
                active_key_queue_ ^= 1;
                keyqueue_lock_.Unlock();

                return active_key_queue_ ^ 1;
            }
            uint8_t InactiveKeyQueueIdx(){
                return active_key_queue_ ^ 1;
            }
            UserKey* PopUserKeyFromKeyQueue(uint8_t idx){
                assert(idx == (active_key_queue_^1));
                /* Lock is not required because it is Inactive Key Queue*/
                UserKey* uk = NULL;
                if(!key_queue_[idx].empty()){
                    SimpleLinkedNode *node = key_queue_[idx].Pop();
                    if ( node ) {
                        uk = class_container_of(node, UserKey, link_);
                    }
                }
                return uk;
            }
            bool InsertUserKeyToKeyMapFromInactiveKeyQueue(Manifest *mf, bool inactive, bool acquirelock = true);
            Keymap* GetKeyMap() { return keymap_; }
#if 0
            void SKTDFLoadLock() { sktdf_update_mu_.lock(); }
            bool SKTDFLoadTryLock() { return !sktdf_update_mu_.try_lock(); }
            void SKTDFLoadUnlock() { sktdf_update_mu_.unlock(); }
#else
            void SKTDFLoadLock() { sktdf_load_mu_.Lock(); }
            bool SKTDFLoadTryLock() { return sktdf_load_mu_.TryLock(); }
            void SKTDFLoadUnlock() { sktdf_load_mu_.Unlock(); }
#endif
            void KeyMapSharedLock() { keymap_mu_.lock_shared(); }
            void KeyMapSharedUnLock() { keymap_mu_.unlock_shared(); }
            void KeyMapGuardLock() { keymap_mu_.lock(); }
            void KeyMapGuardUnLock() { keymap_mu_.unlock(); }
            void KeyMapUpgradeLock() { keymap_mu_.lock_upgrade(); }
            void KeyMapDowngradeLock() { keymap_mu_.unlock_upgrade(); }

#if 1
            void KeyMapRandomReadSharedLock() { keymap_random_read_mu_.lock_shared(); }
            void KeyMapRandomReadSharedUnLock() { keymap_random_read_mu_.unlock_shared(); }
            void KeyMapRandomReadGuardLock() { keymap_random_read_mu_.lock(); }
            void KeyMapRandomReadGuardUnLock() { keymap_random_read_mu_.unlock(); }
#endif

            void IncRandomReadWaitCount(void){ random_read_wait_cnt_.fetch_add(1, std::memory_order_relaxed); }
            void DecRandomReadWaitCount(void){ random_read_wait_cnt_.fetch_sub(1, std::memory_order_relaxed);}
            uint8_t GetRandomReadWaitCount(void){ return random_read_wait_cnt_.load(std::memory_order_relaxed); }

#if 0
            void SKTDFUpdateSharedLock() { sktdf_update_mu_.lock_shared(); }
            void SKTDFUpdateSharedUnLock() { sktdf_update_mu_.unlock_shared(); }
            void SKTDFUpdateGuardLock() { sktdf_update_mu_.lock(); }
            void SKTDFUpdateGuardUnLock() { sktdf_update_mu_.unlock(); }
#endif

#if 0
            void SKTDFFetchSpinLock() { sktdf_fetch_spinlock_.Lock(); }
            void SKTDFFetchSpinUnlock() { sktdf_fetch_spinlock_.Unlock(); }

            void BuildIterLock(){build_iter_mu_.Lock();}
            void BuildIterUnlock(){build_iter_mu_.Unlock();}

            void IterLock(){ iter_mu_.Lock(); }
            void IterUnlock(){ iter_mu_.Unlock(); }
#endif

            bool PrefetchSKTable(Manifest* mf, bool dirtycache = true);
            void DiscardSKTablePrefetch(Manifest* mf);
            bool LoadSKTableDeviceFormat(Manifest *mf, GetType type = kSyncGet);

            SequenceNumber GetLatestSequenceNumberFromSKTableMem(Manifest* mf, const KeySlice& key, uint8_t col_id, bool &deleted);
            bool GetColumnInfo(Manifest* mf, const Slice& key, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, ColumnScratchPad& column, bool skt_load = true);
            uint16_t LookupColumnInfo(Manifest *mf, Slice& col_info_slice, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, SequenceNumber &biggest_begin_key, bool force_prefetch = false);

            ColumnNode* FindNextValidColumnNode(Manifest *mf, UserKey*&  ukey, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, KeySlice* end_key);
            //UserKey* MoveToNextKeyInKeyMap(Manifest* mf, UserKey* ukey);
            void EvictKBPM(Manifest *mf);
            bool SKTDFReclaim(Manifest *mf, int64_t &reclaim_size, bool &evicted, bool &active, bool force_evict, uint32_t &put_col, uint32_t &del_col);
            int SearchSktableIndexInVector(Manifest *mf, Slice key, std::vector<std::string *> &sub_skt_begin_keys);

#if 0
            void UpdateSKTableId(uint32_t prealloc_skt_id, uint32_t next_skt_id) { prealloc_skt_id_ = prealloc_skt_id; next_skt_id_ = next_skt_id; }
#endif
            // SKLData Skiplist interface
            void SetNode(void* node) { internal_node_.Release_Store(node); }
            void* GetNode() { return internal_node_.Acquire_Load(); }
            KeySlice GetKeySlice() ; //override;
            bool CompareKey(UserKeyComparator comp, const KeySlice& key, int& delta) ; //override;

            // SKTableMem flags
            const static uint16_t flags_in_dirty_cache          = 0x0001;
            const static uint16_t flags_in_clean_cache          = 0x0004;
            /* If one of above status is set, the SKT is in cache(flushing/kbm updating/evicting is considered as in-cache)*/
            const static uint16_t flags_cached                  = 0x0007;

            const static uint16_t flags_caching_in_progress     = 0x0008;
            const static uint16_t flags_fetch_in_progress       = 0x0010;
            const static uint16_t flags_deleted_skt             = 0x0020;
            const static uint16_t flags_prefetch_keyblock       = 0x0040;
            const static uint16_t flags_split_sub               = 0x0080;
            //Evicting in progress
            bool IsSet(uint16_t bit){
                return (flags_.load(std::memory_order_relaxed) & bit);
            }
            bool IsCached() { return (flags_.load(std::memory_order_relaxed) & flags_cached); }
            bool IsCachedOrPrefetched() { return (flags_.load(std::memory_order_relaxed) & (flags_cached | flags_fetch_in_progress)); }
            void ClearCached() { flags_.fetch_and(~flags_cached, std::memory_order_relaxed); }

            void SetCacheBit(uint16_t cache){
                assert(!IsCached());
                flags_.fetch_or(cache, std::memory_order_relaxed);
            }

            void ChangeStatus(uint16_t from, uint16_t to){
                if(from && !IsSet(from)) abort();
                if(to && IsSet(to)) abort();
                flags_.fetch_xor((from|to), std::memory_order_relaxed);
                assert(IsSet(to));
                assert(!IsSet(from));
            }
            uint16_t GetFlags() { return flags_.load(std::memory_order_relaxed);}

            bool IsInDirtyCache() { return (flags_.load(std::memory_order_relaxed) & flags_in_dirty_cache); }
            bool SetInDirtyCache() { return (flags_.fetch_or(flags_in_dirty_cache, std::memory_order_relaxed) & flags_in_dirty_cache); }
            bool ClearInDirtyCache() { return flags_.fetch_and((uint8_t)(~flags_in_dirty_cache), std::memory_order_relaxed) & flags_in_dirty_cache; }

            bool IsInCleanCache() { return (flags_.load(std::memory_order_relaxed) & flags_in_clean_cache); }
            bool SetInCleanCache() { return (flags_.fetch_or(flags_in_clean_cache, std::memory_order_relaxed) & flags_in_clean_cache); }
            bool ClearInCleanCache() { return flags_.fetch_and((uint8_t)(~flags_in_clean_cache), std::memory_order_relaxed) & flags_in_clean_cache; }

            //Caching in progress
            bool IsCachingInProgress() { return (flags_.load(std::memory_order_seq_cst) & flags_caching_in_progress); }
            bool SetCachingInProgress() { return (flags_.fetch_or(flags_caching_in_progress, std::memory_order_seq_cst) & flags_caching_in_progress); }
            void ClearCachingInProgress() { flags_.fetch_and(~flags_caching_in_progress, std::memory_order_seq_cst); }

            //Fetch in progress
            bool IsFetchInProgress() { return (flags_.load(std::memory_order_relaxed)& flags_fetch_in_progress); }
            /*This is accessed under lock(sktdf_load_mu_) */
            bool SetFetchInProgress() { return (flags_.fetch_or(flags_fetch_in_progress, std::memory_order_relaxed) & flags_fetch_in_progress); }
            /*If prefetch was not issued, insert the SKTableMem to cache
              This is accessed under lock(sktdf_load_mu_) */
            bool ClearFetchInProgress() { return flags_.fetch_and(~flags_fetch_in_progress, std::memory_order_relaxed) & flags_fetch_in_progress; }

            void SetDeletedSKT(Manifest* mf) {
                mf->DecreaseMemoryUsage(sizeof(SKTableMem));
#ifdef MEM_TRACE
                mf->DecSKT_MEM_Usage(sizeof(SKTableMem));
#endif
                flags_.fetch_or(flags_deleted_skt, std::memory_order_relaxed);
            }
            void ClearDeletedSKT() { flags_.fetch_and(~flags_deleted_skt, std::memory_order_relaxed); }
            bool IsDeletedSKT() { return (flags_.load(std::memory_order_relaxed) & flags_deleted_skt); }

            bool SetPrefetchKeyBlock() { return (flags_.fetch_or(flags_prefetch_keyblock, std::memory_order_relaxed) & flags_prefetch_keyblock); }
            bool ClearPrefetchKeyBlock() { return flags_.fetch_and(~flags_prefetch_keyblock, std::memory_order_relaxed) & flags_prefetch_keyblock; }
            bool IsPrefetchKeyBlock() { return (flags_.load(std::memory_order_relaxed) & flags_prefetch_keyblock); }

            bool SetSplitSub() { return (flags_.fetch_or(flags_split_sub, std::memory_order_relaxed) & flags_split_sub); }
            bool ClearSplitSub() { return flags_.fetch_and(~flags_split_sub, std::memory_order_relaxed) & flags_split_sub; }
            bool IsSplitSub() { return (flags_.load(std::memory_order_relaxed) & flags_split_sub); }

            void UpdateKeyInfo(Manifest* mf, std::string &col_info_table, SequenceNumber &largest_ikey, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue);

            bool IsKeymapLoaded(){ return keymap_->HasArena(); }
            uint32_t GetAllocatedSize() { return keymap_->GetAllocatedSize(); }
            uint32_t GetFreedMemorySize() { return keymap_->GetFreedMemorySize(); }

            Status FlushSKTableMem(Manifest *mf, std::string &col_info_table, Slice &comp_col_info_table, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue, std::list<KeyBlockPrimaryMeta*> &kbpm_list);

            void SplitInactiveKeyQueue(Manifest* mf, std::vector<std::pair<SKTableMem*, Slice>> &sub_skt);

            bool SplitByPrefix(const Slice& a, const Slice& b);
            InSDBKey StoreColInfoTable(Manifest *mf, std::string &col_info_table, Slice &comp_col_info_table);
            Status StoreKeymap(Manifest *mf, Keymap* sub, uint32_t &io_size, uint32_t &hash_data_size);

            SKTableMem* Next(Manifest* mf);
            SKTableMem* Prev(Manifest* mf);

            SimpleLinkedNode* GetSKTableSortedListNode(){return &skt_sorted_node_;}

            bool IsSKTDirty(Manifest *mf)
            {
                return (!flush_candidate_key_deque_.empty() || !IsKeyQueueEmpty() || mf->IsNextSKTableDeleted(GetSKTableSortedListNode()));
            }

            bool FlushCandidateDequeEmpty() { return flush_candidate_key_deque_.empty();}


            void IncSKTRefCnt(Manifest *mf){ 
                uint32_t old = ref_cnt_.fetch_add(1, std::memory_order_relaxed);
#ifdef REFCNT_SANITY_CHECK
                if ((old+1)==0) abort();
#endif
                while(skt_id_ == mf->GetSKTableIDForEviction());
            }
            void DecSKTRefCnt(void){
                uint32_t old = ref_cnt_.fetch_sub(1, std::memory_order_seq_cst);
#ifdef REFCNT_SANITY_CHECK
                if (old==0) abort();
#endif
            }
            uint32_t GetSKTRefCnt(void){ return ref_cnt_.load(std::memory_order_relaxed); }

            //uint32_t GetSize(){ return sizeof(class SKTableMem);}
            uint32_t GetSize(){ return 1;}

        private:
            // No copying allowed
            SKTableMem(const SKTableMem&);
            void operator=(const SKTableMem&);
    }/*SKTableMem*/;

#define KEYBLOCK_VALUE_SIZE_SHIFT 11
#define KEYBLOCK_KEY_SIZE_MASK  0x000007ff
#define KEYBLOCK_CRC_SIZE       4 
#define KEYBLOCK_COMP_SHIFT     31
#define KEYBLOCK_COMP_MASK      0x80000000
#define KEYBLOCK_KB_SIZE_MASK   0x00FFFFFF
#define KEYBLOCK_KEY_CNT_SHIFT  24

#define KEYBLOCK_TTL_SHIFT      31
#define KEYBLOCK_TTL_MASK       0x80000000
#define KEYBLOCK_OFFSET_MASK    0x7fffffff
    class KeyBlock{
        private:
            uint32_t raw_data_size_;
            uint32_t data_size_;
            char* raw_data_;

        public:
            KeyBlock(Manifest *mf) : raw_data_size_(0), data_size_(0), raw_data_(NULL){}
            ~KeyBlock() {
                if (raw_data_){
                    free(raw_data_);
                }
            }
            void KeyBlockInit(uint32_t size) {
                if (raw_data_size_) {
                    data_size_ = size = SizeAlignment(size);
                    if (size > raw_data_size_) {
                        //raw_data_ = (char*)realloc((void*)raw_data_, size);
                        free(raw_data_);
                        raw_data_ = (char*)malloc(size);
                        raw_data_size_ = size;
                    }
                // alloc memory
                } else if (size){
                    assert(!raw_data_);
                    data_size_ = raw_data_size_ = SizeAlignment(size);
                    raw_data_ = (char*)malloc(raw_data_size_);
                }else
                    data_size_ = 0;
            }
            //SubmitKeyBlock();
            Slice GetValueFromKeyBlock(Manifest* mf, uint8_t index);
            Slice GetKeyFromKeyBlock(Manifest* mf, uint8_t index);
            Slice GetRawData(void){
                return Slice(raw_data_,  data_size_);
            }
            void SetSize(uint32_t size) {
                assert(size < data_size_);
                data_size_ = size;
            }
            uint32_t GetSize(void) {
                //return data_size_;
                return raw_data_size_;
            }
            void CleanUp() { }
            bool DecodeAllKeyMeta(KeyBlockPrimaryMeta *kbpm, std::list<ColumnInfo> &key_list);
    };


#define NEXT_IKEY_SEQ_LOC 16
#define USE_DEV_DATA_FOR_NEXT_IKEY_SEQ 0x80000000
#define kOutLogKBMSize 12
    class KeyBlockPrimaryMeta{// 82 Byte = 42 + 40(Mutex)
        private:
            char *dev_data_; 
            uint32_t dev_data_size_; 
            std::atomic<uint8_t> flags_;               // 1 Byte
            uint8_t org_key_cnt_;    // 1 Byte
            uint64_t ikey_seq_num_;/* For device write*/            // 8 Byte
            std::atomic<uint16_t> nrs_key_cnt_;               // 2 Byte
            std::atomic<uint64_t> kb_bitmap_;                       // 4 Byte
            std::atomic<uint64_t> ref_bitmap_;                      // 4 Byte
            volatile KeyBlock* kb_ptr_;    // 8 Byte
            std::atomic<uint8_t> pending_updated_value_cnt_;                      // 8 Byte

            /* lock contention may be low because of fine grained locking. */
            //port::Mutex mu_;                                        //40 Byte
            Spinlock lock_;                                        //1 Byte

            /* Next ikey seq num will be added to end of data_ */
            //uint64_t next_ikey_seq_num_;                                            //8 Byte

            const static uint8_t flags_write_protection         = 0x01;
            const static uint8_t flags_in_updated_kbm_map       = 0x02;
            const static uint8_t flags_dummy                    = 0x04;
            /* The Key Block only contains delete commands. In this case, KeyBlock is not created. only create KeyBlockMetas*/
            const static uint8_t flags_evicting                 = 0x08;
            const static uint8_t flags_in_log                   = 0x10;/* 1 is for in-log */
            const static uint8_t flags_kb_prefetched            = 0x20;
            const static uint8_t flags_kbm_prefetched           = 0x40;
            const static uint8_t flags_need_to_update           = 0x80;//set in FlushSKTableMem, clean in KBMUpdateThread
        public:
#define KBPM_HEADER_SIZE 7
            KeyBlockPrimaryMeta(Manifest *mf): flags_(0), nrs_key_cnt_(0),
            kb_bitmap_(0UL), ref_bitmap_(0UL), kb_ptr_(NULL), org_key_cnt_(0), ikey_seq_num_(0){
            }
            void KBPMLock(){ lock_.Lock(); }
            void KBPMUnlock(){ lock_.Unlock(); }

            KeyBlock*  LoadKeyBlock(Manifest* mf, uint64_t ikey, uint32_t ivalue_size, bool *ra_hit){
                KeyBlock *kb = NULL;
                KBPMLock();
                if (!(kb = GetKeyBlockAddr())){
                    kb = mf->LoadValue(ikey, ivalue_size, ra_hit, this);
                    SetKeyBlockAddr(kb);
                }
                KBPMUnlock();
                return kb;
            }

            void InitInDummyState(uint8_t orig_keycnt) {
                assert(IsDummy());
                /* Set Out log*/
                SetOutLogRelaxed();
                // update original key count
                org_key_cnt_ = 0;
                org_key_cnt_ = orig_keycnt;
                /* Set bitmap */
                assert(orig_keycnt <= kMaxRequestPerKeyBlock);
                kb_bitmap_.store(0UL, std::memory_order_relaxed);
                ref_bitmap_.store(0UL, std::memory_order_relaxed);
            }

            void DummyKBPMInit(uint64_t ikey_seq_num) {
                nrs_key_cnt_.store(0, std::memory_order_relaxed);
                kb_bitmap_.store(0UL, std::memory_order_relaxed);
                ref_bitmap_.store(0UL, std::memory_order_relaxed);
                kb_ptr_ = NULL;
                org_key_cnt_ = 0;
                pending_updated_value_cnt_ = 0;
                flags_.store(0, std::memory_order_relaxed);
                ikey_seq_num_ = ikey_seq_num;
            }

            void NewKBPMInit(uint8_t put_cnt, uint8_t del_cnt, uint64_t next_ikey_seq_num, uint8_t nr_trxn, KeyBlock *kb, char* kbm_dev_buf, bool set_refbits = true) {
                /* Set In log*/
                dev_data_ = kbm_dev_buf;
                dev_data_size_ = 0;
                flags_.store(flags_in_log | flags_need_to_update, std::memory_order_relaxed);
                org_key_cnt_ = put_cnt; // If put_cnt is 0, this is del only request.
                ikey_seq_num_ = 0;
                /*
                 * nrs_key_cnt_ is used for NRS before becoming out log.
                 * Add extra NRS to prevent log cleanup during handling last request(see FlushSKTableMem)
                 */
                nrs_key_cnt_.store(put_cnt + del_cnt + 1, std::memory_order_relaxed);

                kb_bitmap_.store(0UL, std::memory_order_relaxed);
                ref_bitmap_.store(0UL, std::memory_order_relaxed);
                for(uint8_t i = 0 ; i < put_cnt ; i++){
                    kb_bitmap_.fetch_or(((uint64_t)(1UL << i)), std::memory_order_relaxed);
                    /* set ref bitmap to prevent removing */
                    if (set_refbits) ref_bitmap_.fetch_or(((uint64_t)(1UL << i)), std::memory_order_relaxed);
                }

                kb_ptr_ = kb; // kb can be NULL
                /* 
                 * The initial value of pending_updated_value_cnt_ is 1
                 * It will be decreased after it becomes out-log
                 */
                pending_updated_value_cnt_.store(1, std::memory_order_relaxed);
                // Fill KBM Device Format 
#define KB_BITMAP_LOC 4
#define KB_BITMAP_SIZE 8
#define KBM_INLOG_FLAG 0x80
                *dev_data_ = (org_key_cnt_ | KBM_INLOG_FLAG);
                dev_data_size_ = KB_BITMAP_LOC;// 1 byte for org_key_cnt_ & 3 byte for reserved(GC??)

                EncodeFixed64((dev_data_+dev_data_size_), kb_bitmap_.load(std::memory_order_relaxed));
                dev_data_size_ += (KB_BITMAP_SIZE + 4/*to save KBM size*/);
#if 0
                buf = buffer;
                buffer = EncodeVarint64(buf, ts);
                buffer_size+= (buffer-buf);
                //////////////////////////////////////////////////////////////////////////////////////////////
                dev_data_size_ = (EncodeVarint64((dev_data_ + dev_data_size_), next_ikey_seq_num) - dev_data_);
#endif
                *((uint64_t*)(dev_data_+dev_data_size_)) =  next_ikey_seq_num;
                dev_data_size_ += 8/*sizeof(SequenceNumber)*/;
                //next_ikey_seq_num_ = next_ikey_seq_num;
                *(dev_data_ + dev_data_size_) = nr_trxn;
                dev_data_size_++;
                /* If TRXN info exists, the TRXN Info will be added to the end of the data_ strinc*/
                assert(put_cnt <= kMaxRequestPerKeyBlock);
            }
            void AddCharToDeviceFormat(char data){

                *(dev_data_+dev_data_size_) =  data;
                dev_data_size_++;
            }
            void AddStringToDeviceFormat(const char* data, uint32_t size){
                memcpy(dev_data_+dev_data_size_, data, size);
                dev_data_size_ += size;
            }
            void AddVarint16ToDeviceFormat(uint16_t data){
                dev_data_size_ = (EncodeVarint16((dev_data_ + dev_data_size_), data) - dev_data_);
            }
            void SetKeyBlockAddr(KeyBlock* kb){
                assert(!kb_ptr_);
                kb_ptr_ = kb;
            }
            void ClearKeyBlockAddr(){
                assert(kb_ptr_);
                kb_ptr_ = NULL;
            }
            /*KB pointer is only accessed with Lock or in deletion phase. So it does not need to be atomic */
            KeyBlock* GetKeyBlockAddr(){ return (KeyBlock*)kb_ptr_; }

            uint8_t GetOriginalKeyCount() { assert(!IsDummy()); return org_key_cnt_; }

            bool SetWriteProtection() { return (flags_.fetch_or(flags_write_protection, std::memory_order_acquire) & flags_write_protection); }
            void ClearWriteProtection() { flags_.fetch_and((~flags_write_protection), std::memory_order_release); }

            /*If the caller has KBPM(not KBML), NRS must already be decreased */
            /* If flags_in_lazy_updated_kbm_map has been set, do not insert the kbm into the lazy update kbm thread's map */
            bool IsInUpdateKBMMap() { return (flags_.load(std::memory_order_relaxed) & flags_in_updated_kbm_map); }
            bool SetUpdateKBMMap() { return (flags_.fetch_or(flags_in_updated_kbm_map, std::memory_order_relaxed) & flags_in_updated_kbm_map); }
            void ClearUpdateKBMMap() { flags_.fetch_and(~flags_in_updated_kbm_map, std::memory_order_relaxed); }

            void SetDummy() { assert(!IsDummy()); flags_.fetch_or(flags_dummy, std::memory_order_seq_cst); }
            void ClearDummy() { flags_.fetch_and(~flags_dummy, std::memory_order_seq_cst); }
            bool IsDummy() { return (flags_.load(std::memory_order_seq_cst) & flags_dummy); }

            bool SetEvicting() { return flags_.fetch_or(flags_evicting, std::memory_order_seq_cst) & flags_evicting; }
            void ClearEvicting() { flags_.fetch_and(~flags_evicting, std::memory_order_seq_cst); }
            bool IsEvicting() { return (flags_.load(std::memory_order_seq_cst) & flags_evicting); }

            void SetInLog() { assert(!IsDummy()); flags_.fetch_or(flags_in_log, std::memory_order_seq_cst); }
            void SetOutLog() { flags_.fetch_and(~flags_in_log, std::memory_order_seq_cst); }
            void SetOutLogRelaxed() { flags_.fetch_and(~flags_in_log, std::memory_order_relaxed); }
            /* If it is evictable, return true*/
#if 0
            bool SetOutLogAndCheckEvicable() { 
                flags_.fetch_and(~flags_in_log, std::memory_order_seq_cst); 
                return (!ref_cnt_ && !CheckRefBitmap());
            }
#endif
            bool IsInlog() { return (flags_.load(std::memory_order_seq_cst) & flags_in_log); }

            /* Under lock */
            bool SetKBPrefetched() { return flags_.fetch_or(flags_kb_prefetched, std::memory_order_release) & flags_kb_prefetched; }
            bool ClearKBPrefetched() { return flags_.fetch_and(~flags_kb_prefetched, std::memory_order_release) & flags_kb_prefetched; }
            bool IsKBPrefetched() { return (flags_.load(std::memory_order_release) & flags_kb_prefetched); }

            bool SetKBMPrefetched() { return (flags_.fetch_or(flags_kbm_prefetched, std::memory_order_release) & flags_kbm_prefetched); }
            bool IsKBMPrefetched() { return (flags_.load(std::memory_order_release) & flags_kbm_prefetched); }
            bool ClearKBMPrefetched() { return (flags_.fetch_and((uint8_t)~flags_kbm_prefetched, std::memory_order_release) & flags_kbm_prefetched); }

            bool SetNeedToUpdate() { return (flags_.fetch_or(flags_need_to_update, std::memory_order_seq_cst) & flags_need_to_update); }
            bool ClearNeedToUpdate() { return (flags_.fetch_and((uint8_t)~flags_need_to_update, std::memory_order_release) & flags_need_to_update); }
            bool IsNeedToUpdate() { return (flags_.load(std::memory_order_release) & flags_need_to_update); }
            uint8_t GetFlag(){ return flags_; }

            uint64_t CheckRefBitmap(){
                return ref_bitmap_.load(std::memory_order_seq_cst);
            }
            bool SetRefBitmap(uint8_t bit_index){ 
                return ref_bitmap_.fetch_or((uint64_t)(1UL << bit_index), std::memory_order_seq_cst) & ((uint64_t)(1UL << bit_index));
            }
            uint64_t ClearRefBitmap(uint8_t bit_index){
#ifdef CODE_TRACE
                printf("[%d :: %s]kbpm : %p offset :%d\n", __LINE__, __func__, this, bit_index);
#endif
                return (ref_bitmap_.fetch_and(~((uint64_t)(1UL << bit_index)), std::memory_order_seq_cst) & ~((uint64_t)(1UL << bit_index)));
            }
            bool IsRefBitmapSet(uint8_t bit_index){
                return (ref_bitmap_.load(std::memory_order_seq_cst) & ((uint64_t)(1UL << bit_index)));
            }
#if 0
            void RestoreRefBitmap(uint8_t bit_index){
                ref_bitmap_.fetch_and(~((1 << bit_index)-1), std::memory_order_seq_cst);
            }
            bool ClearAllRefBitmap(){
                return ref_bitmap_ = 0;
            }
#endif
            ////////////
            /* Hoding Iter functions are Protected by flags_write_protction */
#if 0
            void SetKeyBlockBitmap(uint8_t bit_index){ 
                assert(!IsDummy());
                kb_bitmap_.fetch_or(1 << bit_index, std::memory_order_seq_cst);
                ref_bitmap_.fetch_or(1 << bit_index, std::memory_order_seq_cst);
            }
#endif
            void ClearKeyBlockBitmap(uint8_t bit_index){
                assert(!IsDummy());
                kb_bitmap_.fetch_and(~((uint64_t)(1UL << bit_index)), std::memory_order_seq_cst);
                ref_bitmap_.fetch_and(~((uint64_t)(1UL << bit_index)), std::memory_order_seq_cst);
            }
            //////////////
            void IncPendingUpdatedValueCnt(){
                if(!pending_updated_value_cnt_.fetch_add(1, std::memory_order_relaxed)){
                    /* If there is no pending, set flags_need_to_update in order to prevent evict*/
                    SetNeedToUpdate();
                }
            }
            void DecPendingUpdatedValueCnt() {
                pending_updated_value_cnt_.fetch_sub(1, std::memory_order_relaxed);
            }
            uint8_t GetPendingUpdatedValueCnt(){
                return pending_updated_value_cnt_.load(std::memory_order_relaxed);
            }

            void OverrideKeyBlockBitmap(uint64_t new_bitmap) {
                assert(IsDummy());
                kb_bitmap_ = new_bitmap;
            }
            uint64_t GetKeyBlockBitmap(){ assert(!IsDummy()); return kb_bitmap_.load(std::memory_order_relaxed); }
            bool TestKeyBlockBitmap(){
                assert(!IsDummy());
                return GetKeyBlockBitmap();
            }
            KeyBlockMetaType GetKeyBlockMetaType(){ return kKeyBlockPrimary; }
            void SetInSDBKeySeqNum(uint64_t ikey_seq_num) {
                ikey_seq_num_ = ikey_seq_num;
            }
            Slice BuildDeviceFormatKeyBlockMeta(char* kbm_buffer);
            static const uint32_t max_size = 12;/*The size of out-log KBM is 12 byte*/
            Status PrefetchKBPM(Manifest *mf) {
                int size = max_size;
#ifdef TRACE_READ_IO
                g_kbm_async++;
#endif
#ifdef CODE_TRACE
                printf("[%d :: %s]Try to prefetch (KBPM : 0x%p)\n", __LINE__, __func__, this);
#endif
                if (!SetKBMPrefetched()){
#ifdef CODE_TRACE
                    printf("[%d :: %s]Do prefetch (KBPM : 0x%p)\n", __LINE__, __func__, this);
#endif
                    return mf->GetEnv()->Get(GetKBKey(mf->GetDBHash(), kKBMeta, ikey_seq_num_), nullptr, &size, kReadahead);
                }
                return Status::OK();
            }
            bool LoadKBPMDeviceFormat(Manifest *mf, char* kbm_buffer, int &buffer_size, bool prefetched = true) {
                assert(IsDummy());
                bool from_readahead = false;
                kbm_buffer = new char[max_size];
                buffer_size = max_size;
                /*Always kNonblokcingRead*/
                if(prefetched)
                    mf->GetEnv()->Get(GetKBKey(mf->GetDBHash(), kKBMeta, ikey_seq_num_), kbm_buffer, &buffer_size, kNonblokcingRead, mf->IsIOSizeCheckDisabled()?Env::GetFlag_NoReturnedSize:0, &from_readahead);
                else
                    mf->GetEnv()->Get(GetKBKey(mf->GetDBHash(), kKBMeta, ikey_seq_num_), kbm_buffer, &buffer_size, kSyncGet, mf->IsIOSizeCheckDisabled()?Env::GetFlag_NoReturnedSize:0, &from_readahead);
#ifndef NDEBUG
                if (!mf->IsIOSizeCheckDisabled())
                    assert(buffer_size <= max_size);
#endif
#ifdef TRACE_READ_IO
                if(s.ok()) {
                    g_kbm_sync++;
                    if (from_readahead)
                        g_kbm_sync_hit++;
                }
#endif
                return from_readahead;
            }
            void FreeDeviceFormatBuffer(char* kbm_buffer) { delete [] kbm_buffer; }
            void InitWithDeviceFormat(Manifest  *mf, const char *buffer, uint32_t buffer_size);
            SequenceNumber GetiKeySequenceNumber(){ return ikey_seq_num_; }

#if 0
            void SetWorkerBit(uint8_t worker_id){
                worker_bit_.fetch_or((1 << worker_id) , std::memory_order_seq_cst);
            }
            void ClearWorkerBit(){
                worker_bit_.store(0, std::memory_order_seq_cst);
            }
            bool TestWorkerBit(uint8_t worker_id){
                return worker_bit_.load(std::memory_order_seq_cst) & (1 << worker_id);
            }
#else
#endif

            void CleanUp() {
                assert((flags_.load(std::memory_order_seq_cst)&(~(flags_evicting | flags_dummy))) == 0);
                assert(!kb_ptr_);
                kb_bitmap_.store(0UL,std::memory_order_relaxed);
                org_key_cnt_ = 0;
            }

            bool EvictColdKBPM(Manifest *mf)
            {
                if(CheckRefBitmap() == 0){
                    /* If it is last one */
                    KeyBlock *kb;
                    if(!SetEvicting()){
                        if(CheckRefBitmap() == 0) {
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(CheckRefBitmap() != 0);/* Wait until deref */
                            kb = GetKeyBlockAddr();
                            if(kb){
                                mf->DecreaseMemoryUsage(kb->GetSize() + sizeof(KeyBlockPrimaryMeta));
#ifdef MEM_TRACE
                                mf->DecKBPM_MEM_Usage(kb->GetSize() + sizeof(KeyBlockPrimaryMeta));
#endif
                                mf->FreeKB(kb);
                                ClearKeyBlockAddr();
                            }else{
                                mf->DecreaseMemoryUsage(sizeof(KeyBlockPrimaryMeta));
#ifdef MEM_TRACE
                                mf->DecKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif
                                if (ClearKBPrefetched()) {
                                    InSDBKey kbkey = GetKBKey(mf->GetDBHash(), kKBlock, ikey_seq_num_);
                                    (mf->GetEnv())->DiscardPrefetch(kbkey);
                                }
                            }
                            // Discard KBM prefetch if exist
                            if (ClearKBMPrefetched()){
                                assert(IsDummy());
                                InSDBKey kbmkey = GetKBKey(mf->GetDBHash(), kKBMeta, ikey_seq_num_);
                                (mf->GetEnv())->DiscardPrefetch(kbmkey);
                            }
                            return true;
                        }
                        ClearEvicting();
                    }
                }
                return false;
            }
            bool ClearRefBitAndTryEviction(Manifest *mf, uint8_t offset){
                if(ClearRefBitmap(offset) == 0 && !IsNeedToUpdate() && !IsInlog() && !IsKBMPrefetched()){
                    /* If it is last one */
                    KeyBlock *kb;

                    if(!SetEvicting()){
                        if(!IsNeedToUpdate() && CheckRefBitmap() == 0 && !IsInlog() && !IsKBMPrefetched()) {
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(CheckRefBitmap() != 0);/* Wait until deref */
                            kb = GetKeyBlockAddr();
                            if(kb){
                                mf->DecreaseMemoryUsage(kb->GetSize() + sizeof(KeyBlockPrimaryMeta));
#ifdef MEM_TRACE
                                mf->DecKBPM_MEM_Usage(kb->GetSize() + sizeof(KeyBlockPrimaryMeta));
#endif
                                mf->FreeKB(kb);
                                ClearKeyBlockAddr();
                            }else{
                                mf->DecreaseMemoryUsage(sizeof(KeyBlockPrimaryMeta));
#ifdef MEM_TRACE
                                mf->DecKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif
                                if (ClearKBPrefetched()) {
                                    InSDBKey kbkey = GetKBKey(mf->GetDBHash(), kKBlock, ikey_seq_num_);
                                    (mf->GetEnv())->DiscardPrefetch(kbkey);
                                }
                            }
                            return true;
                        }
                        ClearEvicting();
                    }
                }
                return false;
            }
            bool ClearRefBitAndTryEviction(Manifest *mf, uint8_t offset, int64_t &reclaim_size){
                if(ClearRefBitmap(offset) == 0 && !IsNeedToUpdate() && !IsInlog() && !IsKBMPrefetched()){
                    /* If it is last one */
                    KeyBlock *kb;

                    if(!SetEvicting()){
                        if(!IsNeedToUpdate() && CheckRefBitmap() == 0 && !IsInlog() && !IsKBMPrefetched()) {
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(CheckRefBitmap() != 0);/* Wait until deref */
                            kb = GetKeyBlockAddr();
                            if(kb){
                                mf->DecreaseMemoryUsage(kb->GetSize() + sizeof(KeyBlockPrimaryMeta));
#ifdef MEM_TRACE
                                mf->DecKBPM_MEM_Usage(kb->GetSize() + sizeof(KeyBlockPrimaryMeta));
#endif
                                reclaim_size-=kb->GetSize() + sizeof(KeyBlockPrimaryMeta);
                                mf->FreeKB(kb);
                                ClearKeyBlockAddr();
                            }else{
                                mf->DecreaseMemoryUsage(sizeof(KeyBlockPrimaryMeta));
#ifdef MEM_TRACE
                                mf->DecKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif
                                reclaim_size-= sizeof(KeyBlockPrimaryMeta);
                                if (ClearKBPrefetched()) {
                                    InSDBKey kbkey = GetKBKey(mf->GetDBHash(), kKBlock, ikey_seq_num_);
                                    (mf->GetEnv())->DiscardPrefetch(kbkey);
                                }
                            }
                            return true;
                        }
                        ClearEvicting();
                    }
                }
                return false;
            }
            uint64_t GetNextIKeySeqNum() { 
                assert(IsInlog());
                assert(dev_data_size_ == USE_DEV_DATA_FOR_NEXT_IKEY_SEQ);
                assert(dev_data_);
                uint64_t next_ikey = ((uint64_t)(dev_data_));
                dev_data_size_ = 0;
                dev_data_ = NULL;
                return next_ikey;
            }
            uint8_t GetNRSCount(){//Number of keys not recorded in table
                return nrs_key_cnt_.load(std::memory_order_seq_cst);
            }
            uint8_t DecNRSCount(){//Number of keys not recorded in table
                uint8_t nrs_cnt = nrs_key_cnt_.fetch_sub(1, std::memory_order_seq_cst);
                assert(nrs_cnt);
                return  nrs_cnt - 1;//return new count
            }

    };


#define UV_BUF_SIZE_SHIFT 5
#define UV_BUF_SIZE_MASK 0x000000000000FFFF
#define PINMASK 0x0FFFFFFF

    class UserValue{ //16 Byte
        private:
            /* Maximum Buffer size is 2MB(21bit) 
             *  => 32Byte Alignment(16 bit) */
            /* | 6 Byte = buffer address || 2 Byte = size | */
            char* buf_;
            uint32_t value_size_;// max 21 bit
            uint16_t buf_size_;
            /*
             * First 4 bits are used for flags
             * the least 28 bits are for pin.
             */
            std::atomic<int16_t> pinned_;
            ObjectAllocator<UserValue>* allocator_;
        public:
                                    
            /*For Free UserValue Registration */
            UserValue(uint32_t size, ObjectAllocator<UserValue>* allocator) : buf_(NULL), value_size_(0), buf_size_(0), pinned_(0), allocator_(allocator_) { 
                if(size){
                    size = ((size + 0x1F)  & 0xFFFFFFE0);// For 32byte alignment
                    buf_ = (char*)calloc(1, size);
                    buf_size_ = (uint16_t)(size >>  UV_BUF_SIZE_SHIFT); 

                }
            }

            UserValue(uint32_t size) : buf_(NULL), value_size_(0), buf_size_(0), pinned_(1), allocator_(NULL) { 
                if(size){
                    size = ((size + 0x1F)  & 0xFFFFFFE0);// For 32byte alignment
                    buf_ = (char*)calloc(1, size);
                    buf_size_ = (uint16_t)(size >>  UV_BUF_SIZE_SHIFT); 

                }
            }

            ~UserValue() { 
                /*TODO : decrease count in cache or prefetch/iterator buffer */
                /**
                 * Lock ColumnNode in RemoveUserValue()
                 * destruct can be called from SKTable flush that has already aquired a lock on a column or a internalkey
                 *
                 * owner_->RemoveUserValue(); 
                 */
                if (buf_) free((void*)buf_);
            }
            char* GetBuffer(){ return buf_; }

            void InitUserValue(const Slice& uval){
                assert(pinned_ == 1);
                value_size_ = uval.size();
                uint32_t buf_size = ((uint32_t)buf_size_ << UV_BUF_SIZE_SHIFT);

                if(value_size_){
                    if(buf_size < value_size_){
                        free((void*)buf_);
                        buf_size = ((value_size_ + 0x1F)  & 0xFFFFFFE0);// For 32byte alignment
                        buf_ = (char*)calloc(1, buf_size);
                        buf_size_ = buf_size >> UV_BUF_SIZE_SHIFT;
                    }
                    memcpy((void*)buf_, uval.data(), value_size_);
                }
            }

            void InitUserValue(const Slice& uval, const Slice& key, uint8_t col_id, uint64_t ttl){
                assert(pinned_ == 1);
                value_size_ = uval.size();
                uint32_t buf_size = ((uint32_t)buf_size_ << UV_BUF_SIZE_SHIFT);
                if(buf_size < value_size_ + key.size() + 1/*sizeof(col_id)*/ + (ttl?8:0)){
                    buf_size = value_size_ + key.size() + 1/*sizeof(col_id)*/ + (ttl?8:0);
                    buf_size = ((buf_size + 0x1F)  & 0xFFFFFFE0);// For 32byte alignment
                    free((void*)buf_);
                    buf_ = (char*)calloc(1, buf_size);
                    buf_size_ = buf_size >> UV_BUF_SIZE_SHIFT;
                }
                /* Copy "User value" + "User Key" + "Column ID" + ("TTL") for data compression in BuildKeyBlock() */
                memcpy((void*)buf_, uval.data(), value_size_);
                buf_size = uval.size();
                memcpy((void*)(buf_ + buf_size), key.data(), key.size());
                buf_size += key.size();
                *(buf_+buf_size) = col_id;
                buf_size += 1;/* size of col id */
                if(ttl) memcpy((void*)(buf_ + buf_size),(void *) &ttl, 8);
            }

            void InitPinAndFlag() { pinned_.store(1, std::memory_order_relaxed); }
            void Pin() {
#ifdef INSDB_GLOBAL_STATS
                g_pin_uv_cnt++;
#endif
                int16_t old = pinned_.fetch_add(1, std::memory_order_relaxed);
#ifdef REFCNT_SANITY_CHECK
                if (old+1<0) abort();
#endif
            }
#ifndef CODE_TRACE
            uint16_t GetPinned() { return pinned_.load(std::memory_order_relaxed); }
#endif
            bool UnPin() {
                int16_t new_pinned = --pinned_;
#ifdef REFCNT_SANITY_CHECK
                if (new_pinned < 0) abort();
#endif
#ifdef INSDB_GLOBAL_STATS
                g_unpin_uv_cnt++;
#endif
                return new_pinned != 0;
            }
            Slice GetKeySlice(uint16_t key_size) {
                assert(value_size_ + key_size <= (buf_size_ << UV_BUF_SIZE_SHIFT));
                return Slice((buf_ + value_size_), key_size);
            }
            Slice ReadValue() { return Slice(buf_, value_size_);  }
            uint32_t GetValueSize() { return value_size_; }
            void SetAllocator(ObjectAllocator<UserValue>* allocator){ allocator_ = allocator; }
            ObjectAllocator<UserValue>* GetAllocator(){ return allocator_; }

        private:
            // No copying allowed
            UserValue(const UserValue&, size_t);
            void operator=(const UserValue&);
    }/* End UserValue */;

    /**
      @ SnapshotImpl and SnapshotIterator claas.
      */
    // Snapshots are kept in a doubly-linked list in the DB.
    // Each SnapshotImpl corresponds to a particular sequence number.
#define MAX_ITER_SUSTAIN_SEC    30 /* 30sec */
    class SnapshotIterator;
    class SnapshotImpl : public Snapshot {
        private:
            friend class Manifest;
            Manifest *mf_; /** manifest pointer */
            SequenceNumber sequence_;
            uint64_t cur_time_;
            std::atomic<int> refcount_;
            SimpleLinkedNode snapshot_node_;
            std::list<SnapshotIterator *>iterators_;
            port::Mutex mu_;

        public:
            ~SnapshotImpl();

            SnapshotImpl(Manifest *mf, SequenceNumber seq, uint64_t cur_time) : 
                mf_(mf), sequence_(seq), cur_time_(cur_time), refcount_(0), snapshot_node_(), mu_(){
                    iterators_.clear();
#ifdef INSDB_GLOBAL_STATS
       g_new_snap_cnt++;
#endif
                }
            virtual uint64_t GetSequenceNumber() const override {
                return sequence_;
            }

            SimpleLinkedNode* GetSnapshotListNode(){ return &snapshot_node_; }
            Iterator* CreateNewIterator(uint16_t col_id);
            uint64_t GetCurrentSequenceNumber() { return sequence_; }
            uint64_t GetCurrentTime() { return cur_time_; }
            uint32_t GetIterCount() { return iterators_.size(); }
            void RemoveIterator(SnapshotIterator* iter);
            void IncRefCount() {
                int old = refcount_.fetch_add(1, std::memory_order_relaxed);
#ifdef REFCNT_SANITY_CHECK
                if (old+1<0) abort();
#endif
            }
            //bool DisconnectSnapshot(){ return mf_->__DisconnectSnapshot(this); }
            int DecRefCount() {
                int newv = refcount_.fetch_sub(1, std::memory_order_seq_cst)-1;
#ifdef REFCNT_SANITY_CHECK
                if (newv < 0) abort();
#endif
                return newv;
            }
            int GetRefCount() { return refcount_.load(std::memory_order_relaxed); }
    };

#if 0
    static void RefSnapshot(SnapshotImpl* snap) { snap->IncRefCount(); }
    static void DerefSnapshot(SnapshotImpl* snap) { 
        if (snap->DisconnectSnapshot())
        {
            delete snap;
#ifdef INSDB_GLOBAL_STATS
            g_del_snap_cnt++;
#endif
        }
    }
#endif

    struct IterKey {
        // No split allowed, allow flush
        SKTableMem *skt;
        // immutable
        Keymap *keymap;
        Slice userkey_buff;

        // valid when keyhandle != 0
        uint64_t kb_ikey_seq;
        void *uv_or_kbpm;
        uint32_t kb_ivalue_size;
        KeyMapHandle keyhandle;
        uint8_t kb_index;
        uint8_t is_uv:1;
        uint8_t skt_ref:1;

        void InitAfterCopy(Manifest *mf) {
            skt_ref = 0;
            userkey_buff = Slice(0);
            uv_or_kbpm = nullptr;
        }
        void IncSktRef(Manifest *mf) {
            if (!skt || skt_ref) return;
            skt->IncSKTRefCnt(mf);
            keymap->IncRefCnt();
            skt_ref = 1;
        }
        void CleanupSkt() {
            if (!skt) return;
            if (skt_ref) {
                skt_ref = 0;
                skt->DecSKTRefCnt();
                if (keymap->DecRefCntCheckForDelete())
                    delete keymap;
            }
            skt = nullptr;
            keymap = nullptr;
        }
        void CleanupUserkeyBuff() {
            if (!userkey_buff.size()) return;
            free((void*)userkey_buff.data());
            userkey_buff = Slice(0);
        }
        void CleanupValue(Manifest *mf) {
            if (!uv_or_kbpm) return;
            if (is_uv)
                mf->FreeUserValue((UserValue*)uv_or_kbpm);
            uv_or_kbpm = nullptr;
        }
        void Cleanup(Manifest *mf) {
            CleanupSkt();
            CleanupUserkeyBuff();
            CleanupValue(mf);
        }
    };

    class SnapshotIterator : public Iterator {
        public:
            enum SeekDirection {
                kNext = 0,
                kPrev = 1,
                kSeekNext = 2,
                kSeekPrev = 3,
                kSeekFirst = 4,
                kSeekLast = 5,
            };

            SnapshotIterator(Manifest* mf,  SnapshotImpl *snap, uint16_t col_id = 0, Slice skey = 0, Slice lkey = 0) : 
                mf_(mf),
                snap_(snap),
                column_id_(col_id),
                start_key_(skey.data(),
                skey.size()),
                cur_{0},
                last_key_(lkey.data(), lkey.size()), 
                comparator_(const_cast<Comparator*>(mf_->GetComparator())),
                last_kb_ikey_seq_(0),
                last_kbpm_(nullptr),
                prefix_size_(0),
                prefix_{},
                iter_history_(0), prefetch_window_shift_(0), prefetch_scanned_queue_(), prefetched_skt_(nullptr), free_pf_node_(), pf_hit_history_(0)
                {
                }

            /*
             * To remove snapshot bind to this iterator, caller must save snpashot pointer by calling GetSnapshot(),
             * and delete snapshot after delete iterator, 
             */
            ~SnapshotIterator();

            bool Valid() const;
            void Seek(const Slice& key);
            void SeekForPrev(const Slice& key);
            void SeekToFirst();
            void SeekToLast();
            void Next();
            void Prev();
            Slice key() const;
            Slice value() const;
            Status status() const {
                // FIX: Iterator status
                // return OK same as RocksDB
                return Status::OK();
                // FIX: end
            }

        private:
            SKTableMem* SingleSKTablePrefetch(SKTableMem *last_prefetched_skt, PrefetchDirection dir);
            void PrefetchSktable(uint8_t dir, SKTableMem *prefetching_skt, uint8_t count);
            void KeyBlockPrefetch(SKTableMem* cur_skt, bool search_less_than);
            void SetIterKeyKeymap(SKTableMem *skt, const Slice *key, SeekDirection dir, IterKey& iterkey);
            void SetIterKey(SKTableMem *skt, const Slice *key, SeekDirection dir, IterKey& iterkey);
            void SetIterKeyForPrefetch(SKTableMem *skt, const Slice *key, SeekDirection dir, IterKey& iterkey);
            uint16_t GetPFWindowSize(bool ra_hit, bool need_to_inc = false) {
                if (!ra_hit && need_to_inc) {
                    if (prefetch_window_shift_ < flags_pf_max_shift) prefetch_window_shift_++; //max pf window is 2048.
                    return ((uint16_t)flags_pf_incfactor << prefetch_window_shift_);
                } else {
                    return flags_pf_incfactor;
                }
            }

            IterKey *AllocIterKey() {
                return new IterKey();
            }

            void FreeIterKey(IterKey *node) {
                delete node;
            }

            void DestroyPrefetch();
            void Prefetch(IterKey &iterkey, uint8_t dir, uint16_t pf_size, const char *prefix, uint16_t prefix_size = 0);
            void ResetPrefetch(void);
            Slice GetStartKey() { return start_key_; }
            Slice GetLastKey() { return last_key_; }
            bool IsSetRange() {
                if (start_key_.size() != 0 || last_key_.size() != 0) return true;
                return false;
            }
            bool IsValidKeyRange(const Slice& key);
            int Compare(const Slice& a, const Slice& b);
            Manifest* mf_;
            SnapshotImpl * snap_;
            uint16_t column_id_;
            /**
              Key Range management.
              */
            std::string start_key_;
            std::string last_key_;
            Comparator* comparator_;

            // curent user key
            IterKey cur_;
            uint64_t last_kb_ikey_seq_;
            KeyBlockPrimaryMeta *last_kbpm_;

            /**
            Prefetch management.
            1. iter_history_ for tracking iterator history - prev/next/value for 2 cycles.
            |2bit(old diection)|2bit (old action)| 2bit (current direction)| 2bit current_action|
            see enum IterHistory.
            2. prefetch_scanned_queue - used for traverse iterator and prefetch.
            3. prefetched_queue - keep prefetched information.

            2. Moving throught iterator.
            1) Shift 4bit of iter_history_ and set direction.
            2) Discard prefetch scanned queue if below condition.
            a. Not Sequenctial pattern, direction is changed.
            3) Check prefetch_candiate_queue whether it has next/previous kv information.
            - If it has, use this for moving key. If not, get next/previous with normal procedure.
            3. Value, find KeyBlock and get Value.
            1) Set iter_history.
            2) find KB and read value. (Maybe use new value allocator to save in sequential address).
            - InsertValue to cache.
            - remove KB sequence in prefetched_queue if it matched of first item of prefetched_queue.
            3) Prefetch KB.
            - Update prefetch windows with ra_hit information.
            - Check pattern and if it need, traverse entries send readahead request and isert both prefetch_scanned_queue and
            prefetched_queue.
            */
            const static uint8_t flags_iter_none = 0x0;
            const static uint8_t flags_iter_value = 0x1;
            const static uint8_t flags_iter_prev = 0x4;
            const static uint8_t flags_iter_next = 0x8;
            const static uint8_t flags_iter_next_prev_history = 0x84;
            const static uint8_t flags_iter_prev_next_history = 0x48;
            const static uint8_t flags_iter_dir_history_mask = 0xcc;
            const static uint8_t flags_iter_dir_mask = 0xc;
            const static uint8_t flags_iter_value_mask = 0x11;
            const static uint8_t flags_iter_valid_mask = 0xDD;
            const static uint8_t flags_iter_shift = 0x4;
            uint8_t iter_history_;
            const static uint8_t flags_pf_shift_none = 0x0;
            const static uint8_t flags_pf_incfactor = 0x8;
            const static uint16_t flags_pf_max_shift = 12;
            uint8_t prefetch_window_shift_;

            std::queue<IterKey*> prefetch_scanned_queue_;
            SKTableMem* prefetched_skt_;
            /**
            - prefetched_queue_ -
            We hope adjacent KV may lie on same KeyBlock.
            But even though it is not true, it's OK because we may not pay too much for duplicated discard.

            Another candidate can be hash table, or hash map. but considering memory usage and cpu usage. we just choose
            simple std::queue.
            */
            std::queue<IterKey *> free_pf_node_;
            const static uint32_t flags_pf_hit_none = 0x0;
            const static uint32_t flags_pf_prev_hit_mask = 0xffff0000;
            const static uint32_t flags_pf_cur_hit_mask = 0x0000ffff;
            const static uint8_t flags_pf_hit_shift = 16;
            /**
            Save previous hit count and current hit count.
            upper 16bit is previous hit count.
            lower 16bit is current hit count.
            */
            uint32_t pf_hit_history_;
            /* prefix optimization */
            mutable uint16_t prefix_size_;
            mutable char prefix_[16];

            //No copying allowed
            SnapshotIterator(const SnapshotIterator &);
            void operator=(const SnapshotIterator &);
    };
} // namespace insdb
#endif //STORAGE_INSDB_INTERNAL_H_
