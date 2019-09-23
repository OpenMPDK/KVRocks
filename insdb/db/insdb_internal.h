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
#include <folly/SharedMutex.h>
#ifdef INSDB_USE_HASHMAP
#include <folly/concurrency/ConcurrentHashMap.h>
#endif
#define NEW_GENERIC_FIRMWARE
#define REFCNT_SANITY_CHECK

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
    class KeyMap;
    class UserKey;
    class UserValue;
    class ColumnNode;
    class SKTDFCacheNode;
    class SKTableMem;
    class Manifest;
    class SnapshotImpl;

    class KeyBlock;
    class KeyBlockMeta;
    class KeyBlockPrimaryMeta;
    class KeyBlockMetaLog;

    class IterScratchPad;
    class IterScratchPadAllocatorPool;
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
        uint64_t largest_seq_num;
        SKTableInfo():skt_id(0), next_skt_id(0), prealloc_skt_id(0), largest_seq_num(0) { }
    };
    struct InSDBThreadsState {
        port::Mutex mu;
        int num_running;
        InSDBThreadsState() : mu(), num_running(0) {}
    };

    struct DeleteSKTableCtx {
        Manifest* mf;
        struct InSDBThreadsState * state;
        uint32_t *skt_ids;
        uint32_t num_skt;
    };

    struct UserKeyComparator {
        const Comparator *user_comparator;
        explicit UserKeyComparator(const Comparator *c) : user_comparator(c) {}
        int operator()(const Slice&, const Slice&) const;
    };

    struct Transaction {
        Transaction():rep(0),id(0){}
        Transaction(int c) : rep(0), id(0) {}
        union{
            struct{
                uint32_t count;
                uint32_t seq_number;/* Start from 0 */
            };
            uint64_t rep;
        };
        TransactionID id;
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
        kSuccessInsert = 1,
        kInsertedToLast = 2,
        kNeedToSchedule = 3,
        kHasBeenScheduled = 4,
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
                T* Alloc(uint32_t size = 0, uint32_t size_2 = 0){
                    T* obj;
                    if(!queue_.empty() && !lock_.TryLock()){
                        if (!queue_.empty()) {
                            obj = queue_.front();
                            queue_.pop();
                            lock_.Unlock();
                        } else {
                            lock_.Unlock();
                            obj = new T(size, size_2);
                            obj->SetAllocator(this);
                        }
                    }else{
                        obj = new T(size, size_2);
                        obj->SetAllocator(this);
                    }
                    return obj;
                }
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
        };
    enum RequestType {
        kPutType = 0,
        kDelType = 1,
        kNrRequestType = 2,
    };

    /* The RequestNode is used to insert a user request into the pending request queue.*/
    struct RequestNode{
        /* Free RequestNode Registration */
        RequestNode(ObjectAllocator<RequestNode>* allocator):uk_(NULL), col_id_(0), trxn_deque_(), trxn_group_id_for_dec_(0), latest_col_node_(NULL), allocator_(allocator){}
        RequestNode(uint32_t size, uint32_t size_2/*dummy size of allocator*/):uk_(NULL), col_id_(0), trxn_deque_(), trxn_group_id_for_dec_(0), latest_col_node_(NULL), allocator_(NULL){}
        void Init(UserKey* uk, uint8_t col_id, Transaction &trxn){
            uk_ = uk;
            col_id_ = col_id;
            latest_col_node_ = NULL;
            assert(trxn_deque_.empty());
            trxn_group_id_for_dec_ = 0;
            if(trxn.id)
                trxn_deque_.push_back(trxn);
        }
        UserKey* GetRequestNodeUserKey(void){ return uk_; }
        uint8_t GetRequestNodeColumnID(void){ return col_id_; }

        void InsertTrxn(Transaction &trxn){
            assert(!trxn_deque_.empty());
            trxn_deque_.push_back(trxn);
        }
        /* The first one is Vertical Group Header's trxn*/
        Transaction GetTrxn(void){
            if(trxn_deque_.empty()) return 0;
            Transaction t = trxn_deque_.back();
            trxn_deque_.pop_back();
            return t;
        }
        uint16_t GetTrxnCount(){
            return trxn_deque_.size();
        }
        void SetTrxnGroupID(TrxnGroupID tgid){
            trxn_group_id_for_dec_ = tgid;
        }
        TrxnGroupID GetTrxnGroupID(){
            return trxn_group_id_for_dec_;
        }

        void SetRequestNodeLatestColumnNode(ColumnNode* node){
            latest_col_node_ = node;
        }
        ColumnNode* GetRequestNodeLatestColumnNode(){
            return latest_col_node_;
        }
        void SetAllocator(ObjectAllocator<RequestNode>* allocator){ allocator_ = allocator; }
        ObjectAllocator<RequestNode>* GetAllocator(){ return allocator_; }

        private:
        UserKey *uk_;
        uint8_t col_id_;
        std::deque<Transaction> trxn_deque_;
        TrxnGroupID trxn_group_id_for_dec_;
        ColumnNode *latest_col_node_;
        ObjectAllocator<RequestNode>* allocator_;

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
        KeyBlockMetaLog* kbml;
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
            void InsertRecoveredKeyToKeymap(ColumnInfo &colinfo);
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

    class KeyMap {
        private:
            typedef SkipList2<UserKey*, KeySlice, UserKeyComparator> KeyMapList;
            UserKeyComparator keymap_comparator_;
            KeyMapList keymap_list_;
            Manifest* mf_;
        public:
            KeyMap(const Comparator *comparator) : keymap_comparator_(comparator), keymap_list_(keymap_comparator_) {}
            //KeyMap(const Comparator *comparator, GCInfo *gcinfo, Manifest *mf) : keymap_comparator_(comparator), keymap_list_(keymap_comparator_, gcinfo,12), mf_(mf) {}

            ~KeyMap(){};
            void CleanUp();

            /** Wrapper interface for skiplist */
            bool Insert(const KeySlice& key, UserKey* data) {
                return keymap_list_.Insert(key, data);
            }
            void InitSplitTarget(SLSplitCtx* m_ctx) {
                return keymap_list_.InitSplitTarget(m_ctx);
            }
            void Append(SLSplitCtx* m_ctx) {
                return keymap_list_.Append(m_ctx);
            }

            UserKey* Remove(const KeySlice& key) {
                return keymap_list_.Remove(key);
            }
            UserKey* Remove(SLSplitCtx* m_ctx) {
                return keymap_list_.Remove(m_ctx);
            }
            bool Contains(const KeySlice& key) {
                return keymap_list_.Contains(key);
            }
            UserKey* Search(const KeySlice& key, SLSplitCtx *m_ctx=nullptr) {
                return keymap_list_.Search(key, m_ctx);
            }
            UserKey* SearchLessThanEqual(const KeySlice& key) {
                return keymap_list_.SearchLessThanEqual(key);
            }
            UserKey* SearchGreaterOrEqual(const KeySlice& key, SLSplitCtx *m_ctx=nullptr) {
                return keymap_list_.SearchGreaterOrEqual(key, m_ctx);
            }
            UserKey* Next(UserKey* data, SLSplitCtx *m_ctx=nullptr) {
                return keymap_list_.Next(data, m_ctx);
            }
            UserKey* Prev(UserKey* data) {
                return keymap_list_.Prev(data);
            }
            UserKey* First(SLSplitCtx *m_ctx=nullptr) {
                return keymap_list_.First(m_ctx);
            }
            UserKey* Last() {
                return keymap_list_.Last();
            }
            KeySlice GetKeySlice(UserKey* data) {
                return keymap_list_.GetKeySlice(data);
            }
            size_t GetCount() { return keymap_list_.GetCount(); }
            /*
             * In order to find a node having max height
             * Sub-SKTable's UserKey should be one of the UserKey with the max height.
             */
            int GetMaxHeight(){ return keymap_list_.GetMaxHeight(); }
        private:
            // No copying allowed
            KeyMap(const KeyMap&);
            void operator=(const KeyMap&);
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
                    if (free_node_.size() > 128 && !lock_.TryLock()) {
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

    struct KeyBufferEntry {
        uint64_t uv_kb;
        uint64_t key;
    };

    struct IterCurrentKeyInfo{
        KeyBufferEntry *offset;
        std::string key;//To get key from SKTDF
        /* 
         * next key may have same kbpm
         * In this case, we don't need to find the kbpm from hash
         */
        KeyBlockPrimaryMeta* kbpm;
        SequenceNumber ikey;/*0 means the value is in UserValue*/
        uint32_t ivalue_size;
        IterCurrentKeyInfo() : offset(NULL), key(), kbpm(NULL), ikey(0), ivalue_size(0){}
        void InitIterCurrentKeyInfo() {
            offset = NULL; 
            key.resize(0);
            kbpm = NULL;
            ikey = 0;
            ivalue_size = 0;
        }
    };


    /* 
     * Deleted/Updated List used in SKTable Flush : Per Thread Buffer 
     * Allocate Device Max Value size
     */
    class SKTUpdateListBuffer{
        private:
            pthread_key_t non_iter_key_;
            pthread_key_t iter_key_;
            static void DestroyBuffer(void *arg){
                std::string * buf = reinterpret_cast<std::string *>(arg);
                if (buf) delete buf;
            }
        public:
            explicit SKTUpdateListBuffer() { 
                /* For I/O & decompression */
                if(pthread_key_create(&non_iter_key_, DestroyBuffer) || pthread_key_create(&iter_key_, DestroyBuffer))
                    abort(); 
            } 
            ~SKTUpdateListBuffer(){

                if (pthread_key_delete(non_iter_key_) || pthread_key_delete(iter_key_)) 
                    abort();
            }
            std::string* GetBuffer(bool iter/*true : Iterator List buffer, false = Non-Iterator list buffer*/) {
                std::string *buf = NULL;
                pthread_key_t key;
                if(iter)
                    key = iter_key_;
                else
                    key = non_iter_key_;
                buf = reinterpret_cast<std::string *>(pthread_getspecific(key));
                if (NULL == buf) {
                    buf = new std::string(kDeviceRquestMaxSize, 0xff);
                    pthread_setspecific(key, buf);
                }
                buf->clear(); /* set as init state except memory */
                return buf;
            }

    };

    struct SKTableDFBuilderInfo{/* SKTable Device Format Builder Information */
        /* Pass following data to SKTableMem::DeviceFormatBuilder()*/
        Manifest *mf;
        /*main/sub SKTable pointer & Slice(DeviceFormatSKTable, size)*/
        std::list<std::tuple<SKTableMem*, Slice, bool/*true = compressed, false = uncompressed*/>> skt_list; 
        std::list<Slice> skt_comp_slice;/*TODO : it is only used for free compressed SKTDF. the code can be replaced by skt_list*/ 
        uint32_t sktdf_next_key_offset;
        uint32_t sktdf_cur_key_offset;/* = SKTDF_BEGIN_KEY_OFFSET;*/
        SKTableMem* target_skt;
        uint64_t flush_ikey;
        uint64_t cur_time;
        char* begin_key_buffer;
        KeySlice begin_key; /*not included*/
        UserKey* ukey;
        std::vector<SnapInfo> snapinfo_list;
        SequenceNumber smallest_snap_seq;
        int32_t nr_snap;
        /*
         * Uncompressed SKTable Buffer 
         * it will be exchanged with DFSKTable Cache.
         */
        std::list<KeyBlockMeta*> updated_kbm_list;
        /* 
         * Offset is needed to record the KBM->ikey & offset to log before actual update 
         */
        /*
         * 1. Delayed Deletion for its iter : KBM can be deleted but an iterator is holding it
         * 2. Updated column in iter : The KBM has valid Column nodes now. But they can updated
         *          In this case, other thread can not delete the KBM because of iter.
         *          if Iter was not recored to device, we can lose the column(become gabage)
         * - This list will be stored to device with begin key & largest Seq Number.
         */
        std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>> iter_delayed_del; 
        std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>> iter_update; 
        /* For  
         * 1. In-Log deletion : the KBM can not be deleted in SKTable Flush function if it is in log.
         * 2. Delayed Deletion for other's iter : The KBM can not be deleted because a deleted key in same KBM belongs to an iterator 
         * 3. Normal update
         * - Following two list will be stored to device together.
         */
        std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>> update_list; 
        /* For out-log deleted KeyBlocks */
        std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>> delete_list; 
        SKTDFCacheNode *skt_df_cache; 
        Slice new_skt_df_cache;
        uint8_t worker_id;
        std::string key_offset;

        SKTableDFBuilderInfo(Manifest *m):
            mf(m),
            skt_list(),
            skt_comp_slice(),
            sktdf_next_key_offset(0),
            sktdf_cur_key_offset(0),/* = SKTDF_BEGIN_KEY_OFFSET;*/
            target_skt(NULL),
            flush_ikey(0),
            cur_time(0),
            begin_key_buffer(NULL),
            begin_key(),
            ukey(NULL),
            snapinfo_list(),
            smallest_snap_seq(0),
            nr_snap(0),
            updated_kbm_list(),
            iter_delayed_del(), 
            iter_update(),
            update_list(),
            delete_list(),
            skt_df_cache(NULL),
            new_skt_df_cache(),
            worker_id(0),
            key_offset()
        {
            begin_key_buffer = (char*)malloc(kMaxKeySize);
            key_offset.reserve(30000*sizeof(uint32_t));/* Max # of keys per SKTable */
        }
        ~SKTableDFBuilderInfo(){
            free(begin_key_buffer);

        }
    };

    // Two queue
    class WorkerData {
        private:
            Spinlock lock_[2];
            std::queue<KeyBlockMetaLog*> queue_[2];
            std::atomic<uint8_t> idx_; /* idx indicate active queue : inactive idx^1 */
            uint64_t next_worker_ikey_;
            uint64_t last_next_worker_ikey_;
        public:
            WorkerData() : idx_(0), next_worker_ikey_(0), last_next_worker_ikey_(0) {}
            ~WorkerData() {}
            uint64_t PushKBML(KeyBlockMetaLog* kbml, uint64_t next_worker_ikey){
                uint64_t cur_next_ikey = 0;
retry:
                uint8_t idx = idx_.load(std::memory_order_relaxed);
                if (lock_[idx].TryLock()) goto retry;
                if (idx != idx_.load(std::memory_order_relaxed)) {
                    lock_[idx].Unlock();
                    goto retry;
                }
                queue_[idx].push(kbml);
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

            /* called only from log thread */
            void ChangeActive() {
                uint8_t active_idx = idx_.load(std::memory_order_relaxed);
                lock_[active_idx].Lock();
                idx_.store((active_idx ^ 1), std::memory_order_relaxed);
                lock_[active_idx].Unlock();
            }

            /* called only from log thread */
            std::queue<KeyBlockMetaLog*> * GetInActiveQueue() {
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
        kCleanSKTCache  = 0,
        kDirtySKTCache  = 1,
        kNrCacheType    = 2,
    };

    // Acquire lock of the owning sktable to access device format cache node
    enum SKTDFOffset{
        SKTDF_CRC_1_OFFSET = 0, 
        SKTDF_CRC_2_OFFSET = 4, 
        SKTDF_NR_KEY_OFFSET = 8,
        SKTDF_INFO_2_OFFSET = 12,
        SKTDF_COMP_START_OFFSET = 16,
        SKTDF_SKT_SIZE_OFFSET = 20,/* For CRC & decomp. Exclude the size of "SKT info 1(0 ~ SKTDF_PREALLOC_ID_OFFSET)" */
        SKTDF_LARGEST_SEQ_NUM = 24,
        SKTDF_NEXT_ID_OFFSET = 32,
        SKTDF_PREALLOC_ID_OFFSET = 36,
        SKTDF_BEGIN_KEY_OFFSET = 40,
    };
#define SKTDF_HASH_SHIFT 32
#define SKTDF_OFFSET_MASK 0x00000000ffffffffULL
    class SKTDFCacheNode { /* 64 Byte */
        private:
            SKTableMem *skt_;                                               // 8 Byte
            Slice device_format_;//uncompressed SKTable device format       // 16Byte
            Slice decoding_base_key_;                                       // 16Byte
            uint32_t *key_offset_;                                     // 8 Byte
            uint32_t next_skt_id_;                                          // 4 Byte
            uint32_t preallocated_skt_id_;                                  // 4 Byte
            std::string key_;
            /*
             * SKTableMem will have a lock to access the SKTDFCacheNode.
             * Spinlock lock;
             */
        public:
            const static uint32_t flag_compression= 0x80000000;
            SKTDFCacheNode(Manifest* mf, SKTableMem *skt, Slice device_format/* comp/decomp DF */, bool from_mem = false);
            ~SKTDFCacheNode(){
                assert(!skt_);                                               // 8 Byte
                assert(!device_format_.size());
                assert(!decoding_base_key_.size());
                assert(!key_offset_);                                     // 8 Byte
                assert(!next_skt_id_);                                          // 4 Byte
                assert(!preallocated_skt_id_);                                  // 4 Byte
            }
            SKTableMem* GetSKTable() { return skt_; }
            /* must be called before delete SKTDFCacheNode */
            void CleanUp(Manifest* mf) ;
            void SetNextSKTableID(uint32_t id){ next_skt_id_ = id; }
            uint32_t GetNextSKTableID(){ return next_skt_id_; }
            void SetPreallocSKTableID(uint32_t id){ preallocated_skt_id_ = id; }
            uint32_t GetPreallocSKTableID(){ return preallocated_skt_id_; }

            Slice GetInitBeginKey(){
                return decoding_base_key_;
            }

            uint32_t GetKeyCount(){
                return DecodeFixed32(device_format_.data()+SKTDF_NR_KEY_OFFSET);
            }

            Slice GetDeviceFormat(){
                return device_format_;
            }
            char* GetDeviceFormatData(){
                return const_cast<char*>(device_format_.data());
            }
            size_t GetSize(){
                return device_format_.size();
            }
            /* After Flush SKTable */
            void UpdateDeviceFormat(Manifest* mf, Slice device_format/*uncomp DF*/);
            /* 
             * Caller knows the current offset and next offset.
             * So, the caller can calculate the size of the column that the caller is looking for.
             * return column information only */
            /* To Get Begin Key, offset == SKTDF_BEGIN_KEY_OFFSET */
            Slice GetKeyWithOffset(uint32_t offset, std::string *key, /*uint16_t& prev_key_offset,*/ uint32_t& next_key_offset){

                key->resize(0);
                /* The offset of a key-column must be less than that of sktdf info 2 */
                assert(offset);
                assert(offset  < *((uint32_t*)(const_cast<char*>(device_format_.data()) + SKTDF_INFO_2_OFFSET)));
                char* key_info_start = (const_cast<char*>(device_format_.data()) + offset);
                /* Prev key offset */
                uint32_t prev_key_offset= DecodeFixed16(key_info_start) + offset;
                key_info_start+=2;
                /* Current key size */
                uint16_t key_size = DecodeFixed16(key_info_start);
                next_key_offset = key_size + offset;
                if(next_key_offset ==  *((uint32_t*)(device_format_.data() + SKTDF_INFO_2_OFFSET)))
                    next_key_offset = 0;//this is last key
                key_info_start+=2;

                Slice key_info = Slice(key_info_start, key_size - 4 /* Prev/Next key offset */);

                /* Shared Key Size */
                if(!GetVarint16(&key_info, &key_size)) port::Crash(__FILE__,__LINE__);
                key->append(decoding_base_key_.data(), key_size);
                /* Non-Shared Key Size */
                if(!GetVarint16(&key_info, &key_size)) port::Crash(__FILE__,__LINE__);
                key->append(key_info.data(), key_size);
                key_info.remove_prefix(key_size);

                return key_info;
            }

            void GetKeyStringWithOffset(uint32_t offset, std::string *key){

                key->resize(0);
                /* The offset of a key-column must be less than that of sktdf info 2 */
                assert(offset);
                if(offset  >= *((uint32_t*)(const_cast<char*>(device_format_.data()) + SKTDF_INFO_2_OFFSET))) abort();
                char* key_info_start = (const_cast<char*>(device_format_.data()) + offset);
                /* Skip Prev key offset */
                key_info_start+=2;

                /* Current key size */
                uint16_t key_size = DecodeFixed16(key_info_start);
                key_info_start+=2;

                Slice key_info = Slice(key_info_start, key_size - 4 );

                /* Shared Key Size */
                if(!GetVarint16(&key_info, &key_size)) port::Crash(__FILE__,__LINE__);
                key->append(decoding_base_key_.data(), key_size);
                /* Non-Shared Key Size */
                if(!GetVarint16(&key_info, &key_size)) port::Crash(__FILE__,__LINE__);
                key->append(key_info.data(), key_size);
            }
            Slice GetColumnInfoWithOffset(uint32_t offset, /*uint16_t& prev_key_offset,*/ uint32_t& next_key_offset){

                /* The offset of a key-column must be less than that of sktdf info 2 */
                assert(offset);
                assert(offset  < *((uint32_t*)(const_cast<char*>(device_format_.data()) + SKTDF_INFO_2_OFFSET)));
                char* key_info_start = (const_cast<char*>(device_format_.data()) + offset);
                /* Prev key offset */
                //prev_key_offset = DecodeFixed16(key_info_start) + offset;
                key_info_start+=2;

                /* Current key size */
                uint16_t key_size = DecodeFixed16(key_info_start);
                next_key_offset = key_size + offset;
                if(next_key_offset ==  *((uint32_t*)(device_format_.data() + SKTDF_INFO_2_OFFSET)))
                    next_key_offset = 0;//this is last key

                key_info_start+=2;

                Slice key_info = Slice(key_info_start, key_size - 4);

                /* Shared Key Size */
                if(!GetVarint16(&key_info, &key_size)) port::Crash(__FILE__,__LINE__);
                /* Non-Shared Key Size */
                if(!GetVarint16(&key_info, &key_size)) port::Crash(__FILE__,__LINE__);
                key_info.remove_prefix(key_size);

                return key_info;
            }

            Slice GetColumnInfoWithOffset(uint32_t offset){

                /* The offset of a key-column must be less than that of sktdf info 2 */
                assert(offset);
                assert(offset  < *((uint32_t*)(const_cast<char*>(device_format_.data()) + SKTDF_INFO_2_OFFSET)));
                char* key_info_start = (const_cast<char*>(device_format_.data()) + offset);
                /* Skip Prev key offset */
                key_info_start+=2;

                /* Current key size */
                uint16_t key_size = DecodeFixed16(key_info_start);
                key_info_start+=2;

                Slice key_info = Slice(key_info_start, key_size - 4);

                /* Shared Key Size */
                if(!GetVarint16(&key_info, &key_size)) port::Crash(__FILE__,__LINE__);
                /* Non-Shared Key Size */
                if(!GetVarint16(&key_info, &key_size)) port::Crash(__FILE__,__LINE__);
                key_info.remove_prefix(key_size);

                return key_info;
            }
            void ClearValidColumnCount(uint32_t offset){
                Slice col_info_slice = GetColumnInfoWithOffset(offset);
                char* valid_col_count = const_cast<char*>(col_info_slice.data());
                *valid_col_count = 0;
            }

            KeyBlockPrimaryMeta* GetKBPMAndiKeyWithOffset(Manifest *mf, uint8_t& kb_offset, SequenceNumber& ikey, KeyBlockPrimaryMeta* kbpm, uint32_t& ivalue_size, uint32_t offset, uint16_t remove_size/*, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time*/);
            /*
             * For single key lookup by hash
             * the caller already know the Key
             * So, we don't need to decoding the key from shared/non-shared key
             * Size can be calculated by caller becauese the # of columns is stored in first byte in returned buffer
             *
             * Binary Search(O(log n)
             *  to avoid use std::unordered_map which uses 56 byte for data structure.
             */

            Slice GetKeyColumnInfobyHash(Manifest *mf, KeySlice key);
    }; 

    struct ColumnScratchPad{
        uint64_t ikey_seq_num;
        uint32_t ivalue_size; 
        uint8_t offset;
    };

    enum IterScratchPadOffset{
        kUserValueAddrOrKBPM = 0,
        kNextEntry = 8,
        kPrevEtnry = -8,
    };

    enum IterScratchPadSize{
        kUserValueAddrOrKBPMSize = sizeof(KeyBufferEntry),
        kKeyEntrySize = sizeof(KeyBufferEntry),
        kKeySizeOffsetShift = 48,
        kKeySizeOffsetMask = 0x7FFF000000000000L,
        kKeySizeOffsetType = 0x8000000000000000L,
        kUserValueAddrOrKBPMMask = 0x0000FFFFFFFFFFFFL, 
    };

    class IterScratchPad{
        public:
            std::atomic<uint16_t> ref_cnt_;
            std::atomic<uint16_t> active_ref_cnt_;
            uint64_t last_access_time_;
            SKTableMem* skt_; 
            SKTableMem* next_skt_; 
            SequenceNumber begin_seq_;
            SequenceNumber last_seq_;
            SKTDFCacheNode* sktdf_;
            uint16_t sktdf_version_;
            bool use_org_sktdf_;
            /* Build key_buffer_ using SKTableMem->keymap_ */
            /* buffer list 
             * each entry has 8 byte
             *  - key_size in uv : 0x8000 0000 0000 0000(flag) + key_size << 48 + uv
             *  - offset in kbpm_ : 0x8000 0000 0000 0000(flag) + offset << 48 + kbpm
             */
            char* key_buffer_;
            uint32_t key_buffer_size_;     
            uint32_t key_entry_count_;
                 
            std::queue<UserValue*> uv_queue_;
            std::queue<KeyBlockPrimaryMeta*> kbpm_queue_;
            ObjectAllocator<IterScratchPad>* allocator_;

            IterScratchPad(uint32_t buf_size, uint32_t size2):
                ref_cnt_(0), 
                active_ref_cnt_(0),
                last_access_time_(0),
                skt_(NULL), 
                next_skt_(NULL), 
                begin_seq_(0),
                last_seq_(0),
                sktdf_(NULL),
                sktdf_version_(0),
                use_org_sktdf_(false),
                key_buffer_(NULL), 
                key_buffer_size_(0), 
                key_entry_count_(0), 
                uv_queue_(), 
                allocator_(NULL) {
                    key_buffer_ = (char*)malloc(buf_size);
                    key_buffer_size_ = buf_size;
                }
            ~IterScratchPad(){
                assert(!sktdf_);
                assert(key_buffer_);
                free(key_buffer_);
            }
            /*Call this function with sktdf_update_mu_*/
            bool SetIterSKTDF(Manifest* mf, SKTableMem* iter_skt);
            /*true if usable*/
            void IncIterScratchPadRef(){
                ref_cnt_.fetch_add(1, std::memory_order_relaxed);
            }
            /*true if freeable*/
            bool DecIterScratchPadRef(){
                if(1 == ref_cnt_.fetch_sub(1, std::memory_order_seq_cst))
                    return true;
                return false;
            }

            void IncActiveIterScratchPadRef(){ active_ref_cnt_.fetch_add(1, std::memory_order_relaxed); }
            void DecActiveIterScratchPadRef(){ active_ref_cnt_.fetch_sub(1, std::memory_order_seq_cst); }
            uint16_t GetActiveIterScratchPadRef(){ return active_ref_cnt_.load(std::memory_order_relaxed); }
            void SetLastAccessTimeSec(uint64_t last_access) { last_access_time_ = last_access; }
            uint64_t GetLastAccessTimeSec() { return last_access_time_; }

            void InitIterScratchPad(uint32_t buf_size){
                assert(!ref_cnt_.load(std::memory_order_relaxed));
                assert(!skt_);
                assert(!next_skt_);
                assert(!begin_seq_);
                assert(!last_seq_);
                assert(!sktdf_);
                assert(!key_entry_count_);
                assert(uv_queue_.empty());
                assert(allocator_);

                ref_cnt_.store(1, std::memory_order_relaxed);/*For current in Iterator */
                active_ref_cnt_.store(0, std::memory_order_relaxed);/*For current in Iterator */
                last_access_time_ = 0;
                if(buf_size > key_buffer_size_){
                    if(key_buffer_) free(key_buffer_);
                    key_buffer_ = (char*)malloc(buf_size);
                    key_buffer_size_ = buf_size;
                }
            }
            void ClearIterScratchpad(Manifest *mf);
            void SetAllocator(ObjectAllocator<IterScratchPad>* allocator){ allocator_ = allocator; }
            ObjectAllocator<IterScratchPad>* GetAllocator(){ return allocator_; }
    };


    class IterScratchPadAllocatorPool{
        private:
            pthread_key_t pool_id_;
            volatile uint16_t id_;
            Manifest *mf_;
        public:
            explicit IterScratchPadAllocatorPool(Manifest *mf) { 
                if(pthread_key_create(&pool_id_, NULL)) abort(); 
                id_ = 0;
                mf_ = mf;
            } 
            ~IterScratchPadAllocatorPool(){
                if (pthread_key_delete(pool_id_)) abort();
            }
            ObjectAllocator<IterScratchPad>* GetIterScratchPadAllocator();
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



    class Manifest {
        private:
            typedef SkipList<SKTableMem*, KeySlice, UserKeyComparator> SKTList;
#ifdef INSDB_USE_HASHMAP
            typedef folly::ConcurrentHashMap<KeySlice, UserKey*, UKHash, UKEqual> KeyHashMap;
            typedef folly::ConcurrentHashMap<SequenceNumber, KeyBlockPrimaryMeta*, KBMHash> KBPMHashMap;
#endif
            Env *env_;
            const Options options_;
            std::string dbname_;
            port::Mutex worker_mu_;
            port::CondVar worker_cv_;
            std::atomic<uint16_t> worker_cnt_;
            std::atomic<bool> worker_terminated_;
            port::Mutex worker_termination_mu_;
            port::CondVar worker_termination_cv_;
            SKTUpdateListBuffer kbm_update_list_;   // 8 Byte
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
            std::atomic<uint32_t> nr_iterator_;
            UserKeyComparator ukey_comparator_;
            GCInfo gcinfo_;
            SKTList sktlist_;  // skiplist for sktable
#ifdef INSDB_USE_HASHMAP
            KeyHashMap  *key_hashmap_;
            KBPMHashMap *kbpm_hashmap_;
#endif
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
            /* Each instance(SKTableMem or SKTDFCacheNode) has thier position in the list */
            template <class T>
                struct SKTCache{
                    Spinlock &lock;
                    std::list<T*>  cache;
                    size_t cache_size;
                    SKTCache(Spinlock &_lock) : lock(_lock), cache(), cache_size(0){}
                    void Lock(){ lock.Lock(); }
                    void Unlock(){ lock.Unlock(); }

                    void AddCache(size_t size){
                        cache_size+=size;
                    }
                    void SubCache(size_t size){
                        assert(cache_size >= size);
                        cache_size-=size;
                    }
                    size_t GetCacheSize(){
                        return cache_size;
                    }
                    bool IsEmpty(){
                        Lock();
                        if (cache.empty()) {
                            Unlock();
                            return true;
                        }
                        Unlock();
                        return false;
                    }
                    T* NextItem(){
                        if (cache.empty()) return NULL;
                        return cache.back();
                    }
                    void PopNextItem(size_t size){
                        SubCache(size);
                        cache.pop_back();
                    }
                    T* PopItem(bool lock = true){
                        T* item = NULL;
                        if(lock) Lock();
                        if (!cache.empty()) {
                            item = cache.back();
                            SubCache(item->GetSize());
                            cache.pop_back();
                        }
                        if(lock) Unlock();
                        return item;
                    }
                    void PushItem(T* item, bool lock = true){
                        if(lock) Lock();
                        AddCache(item->GetSize());
                        cache.push_front(item);
                        item->SetLocation(cache.begin());
                        if(lock) Unlock();
                    }
                    /* caller must have lock */
                    void DeleteItem(T* item){
                        cache.erase(item->GetLocation());
                        SubCache(item->GetSize());
                    }
                };

            Random coldness_rnd_; // seed value for coldness decision 
            Spinlock skt_cache_lock_;
            SKTCache<SKTableMem> skt_cache_[kNrCacheType];

            /* memory pool definition */
            MemoryPool <KeyBlock> *kb_pool_;
            MemoryPool <KeyBlockPrimaryMeta> *kbpm_pool_;
            MemoryPool <KeyBlockMetaLog> *kbml_pool_;
            port::Mutex key_cache_mu_;
            std::atomic<bool> clean_skt_cleanup_;
            port::CondVar key_cache_cv_;

            sem_t key_cache_shutdown_sem_;
            std::atomic<uint8_t> flags_;
            std::atomic<bool> key_cache_shutdown_;

            bool logcleanup_shutdown_;
            port::Mutex logcleanup_mu_;
            port::CondVar logcleanup_cv_;
            uint64_t logcleanup_tid_;
            std::atomic<uint64_t> cleanable_kbm_cnt_;

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
            ObjectAllocator<IterScratchPad> *sp_allocator_;
            ObjectAllocator<UserValue> *uv_allocator_;
            ObjectAllocator<UserKey>   *uk_allocator_;
            ObjectAllocator<RequestNode>   *request_node_allocator_;

            IterScratchPadAllocatorPool sp_allocator_pool_;
            UVAllocatorPool uv_allocator_pool_;
            UKAllocatorPool uk_allocator_pool_;
            RequestNodeAllocatorPool request_node_allocator_pool_;
            //Spinlock *free_request_node_spinlock_;
            //std::deque<RequestNode*> *free_request_node_;

            /* LogCleanup thread queues */
            WorkerData *logcleanup_data_; /* need to create as worker count */

            Spinlock free_skt_buffer_spinlock_;
            std::deque<Slice> free_skt_buffer_;

            PerThreadRawMemoryPool thread_memory_pool_;
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

            Env* GetEnv() { return env_; }
            uint32_t GetDBHash() { return dbhash_; }
            void WorkerLock(){worker_mu_.Lock();}
            void WorkerUnlock(){worker_mu_.Unlock();}
            bool WorkerWaitSig(){ return worker_cv_.TimedWait(env_->NowMicros() + 1000000/*Wake up every 1 second*/); }
            void SignalWorkerThread(bool force = true){
                if(force)
                    force = nr_worker_in_running_.load(std::memory_order_relaxed) < worker_cnt_;
                else
                    force = !nr_worker_in_running_.load(std::memory_order_relaxed);
                if(force)
                    worker_cv_.Signal();
            }
            void SignalAllWorkerThread(void){ worker_cv_.SignalAll(); }
            void SetWorkerTermination(){
                worker_terminated_ = true;
            }
            void WorkerTerminationWaitSig(){ 
                while(!worker_terminated_.load(std::memory_order_seq_cst)){
                    worker_termination_mu_.Lock();
                    worker_termination_cv_.TimedWait(env_->NowMicros() + 1000000/*Wake up every 1 second*/); 
                    worker_termination_mu_.Unlock();
                }
            }
            void SignalWorkerTermination(void){ 
                worker_termination_mu_.Lock();
                worker_termination_cv_.SignalAll(); 
                worker_termination_mu_.Unlock();
            }
            // pre increament
            uint16_t IncWorkerCount(){ return ++worker_cnt_; }
            uint16_t DecWorkerCount(){ return --worker_cnt_; }
            uint16_t GetWorkerCount(){ return worker_cnt_.load(std::memory_order_relaxed); }
            void IncRunningWorkerCount(){ nr_worker_in_running_.fetch_add(1, std::memory_order_relaxed); }
            void DecRunningWorkerCount(){ nr_worker_in_running_.fetch_sub(1, std::memory_order_relaxed); }
            uint16_t GetRunningWorkerCount(){ return nr_worker_in_running_.load(std::memory_order_relaxed); }



            std::string* GetKBMListBuffer(bool iter){
                return kbm_update_list_.GetBuffer(iter/*iter*/);
            }

            const static uint8_t flags_memory_reclaim           = 0x01;
            const static uint8_t flags_logcleanup_requested     = 0x02;
            const static uint8_t flags_key_cache_thread_busy    = 0x04;

            bool IsInMemoryReclaim() { return (flags_.load() & flags_memory_reclaim); }
            bool SetMemoryReclaim() { flags_.fetch_or(flags_memory_reclaim) ; }
            bool ClearMemoryReclaim() { flags_.fetch_and(~flags_memory_reclaim); }

            bool SetLogCleanupRequested() { return (flags_.fetch_or(flags_logcleanup_requested, std::memory_order_relaxed) & flags_logcleanup_requested); }
            bool ClearLogCleanupRequested() { return (flags_.fetch_and(~flags_logcleanup_requested, std::memory_order_relaxed) & flags_logcleanup_requested); }

            bool IsKeyCacheThreadBusy(){ return (flags_.load(std::memory_order_seq_cst) & flags_key_cache_thread_busy);}
            void SetKeyCacheThreadBusy(){ flags_.fetch_or(flags_key_cache_thread_busy, std::memory_order_seq_cst); }
            void ClearKeyCacheThreadBusy(){ flags_.fetch_and(~flags_key_cache_thread_busy, std::memory_order_seq_cst); }
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

            uint64_t IncCleanableKBM(){ return cleanable_kbm_cnt_.fetch_add(1, std::memory_order_relaxed) + 1;}
            Slice* GetIOBuffer(uint32_t size) {
                return dio_buffer_.GetDIOBuffer(size);
            }

            Slice AllocSKTableBuffer(uint32_t size);
            void FreeSKTableBuffer(Slice &buffer);
            void CleanupFreeSKTableBuffer();

            SKTableMem* FindSKTableMem(const Slice& ukey);
            bool InsertSKTableMem(SKTableMem* table);
            SKTableMem* RemoveSKTableMemFromCacheAndSkiplist(SKTableMem* table);
            SKTableMem* Next(SKTableMem* cur);
            SKTableMem* Prev(SKTableMem* cur);
            Status ReadManifest(std::string * contents);
            Status DeleteManifest();
            void InitNextInSDBKeySeqNum(int old_worker_count, uint64_t* seq);
            Status InitManifest();
            Status BuildManifest();
            Status ReadSuperSKTable(SuperSKTable* super_skt);
            Status GetSuperSKTable();
            Status BuildSuperSKTable();
            int ReadSKTableMeta(char* buffer, InSDBMetaType type, uint32_t skt_id, GetType req_type, int buffer_size);
            Status ReadAheadSKTableMeta(InSDBMetaType type, uint32_t skt_id, int buffer_size = kMaxSKTableSize);
            Status DeleteSKTableMeta(InSDBMetaType type, uint32_t skt_id, uint16_t nr_key = 1);

            Status ReadSKTableInfo(uint32_t skt_id, SKTableInfo* reocver);

            uint32_t BuildFirstSKTable(int32_t skt_id);
            Status CreateNewInSDB();
            Status CleanupPreallocSKTable(uint32_t prealloc_skt, uint32_t next_skt_id);
            Status RecoverInSDB();
            SKTableMem* GetFirstSKTable();
            SKTableMem* GetLastSKTable();
            SKTableMem* GetFromNode(void *anon_node);
            uint64_t GetEstimateNrSKTable(Slice key);

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

            SnapshotImpl* GetSnapshotImpl(SimpleLinkedNode *node);
            //{ return node ? class_container_of(node, SnapshotImpl, snapshot_node_) : NULL; }


            bool EvictUserKeys(int64_t reclaim_size, char* begin_key_buffer, char* end_key_buffer,bool discard_prefetch);
            void CleanUpCleanSKTable();
            void SignalKeyCacheThread();
            void KeyCacheThread();
            uint64_t EvictSktDFCacheNodes(uint64_t evict_window, bool force);


            void TryFlushSKTableMem(SKTableMem* table, uint8_t worker_id);
            bool CheckDirtyCacheNode(SKTableMem* skt, uint8_t worker_id);

            // Log cleanup
            void WorkerLogCleanupInternal(WorkerData &logcleanup_data, std::list<SequenceNumber> &worker_next_ikey_seq, std::queue<KeyBlockMetaLog*> &kbml_cleanup, SequenceNumber &smallest_ikey);
            void WorkerLogCleanupDelete(LogRecordKey &logkey, std::queue<KeyBlockMetaLog*> kbml_cleanup, SequenceNumber &smallest_ikey);
            void LogCleanupInternal(LogRecordKey &GLogkey);
            bool IsWorkerLogCleanupQueueEmpty();
            void LogCleanupSignal();
            void LogCleanupThreadFn();
            static void LogCleanupThreadFn_(void* manifest)
            { reinterpret_cast<Manifest*>(manifest)->LogCleanupThreadFn(); }
            void TerminateLogCleanupThread();

            static void KeyCacheThread_(void* manifest)
            { reinterpret_cast<Manifest*>(manifest)->KeyCacheThread(); }

            Status GetSnapshotInfo(std::vector<SnapInfo> *snapinfo_list);
            SequenceNumber GetLatestSnapshotSequenceNumber();
            SequenceNumber GetOldestSnapshotSequenceNumber();


            /* SKTable Cache List : Dirty & Clean Caches */

#ifndef NDEBUG
            void SKTableCacheMemLock(){
                skt_cache_lock_.Lock();
            }
            void SKTableCacheMemUnlock(){
                skt_cache_lock_.Unlock();
            }
#endif
            void InsertSKTableMemCache(SKTableMem* sktm, SKTCacheType type, bool clear_stat = false, bool  wakeup = false);
            bool RemoveFromSKTableMemDirtyCache(SKTableMem* sktm);
            void MigrateSKTableMemCache(SKTableMem* sktm);
            size_t SKTCountInSKTableMemCache(SKTCacheType type);
            SKTableMem* PopSKTableMem(SKTCacheType type); 
            uint8_t GetColdnessSeed(){ return (coldness_rnd_.Next() & (0xFD)) + 1; }
            bool IsSKTDirtyListEmpty(void){ return skt_cache_[kDirtySKTCache].cache.empty();}
            bool IsSKTCleanListEmpty(void){ return skt_cache_[kCleanSKTCache].cache.empty();}
            size_t GetCountSKTableDirtyList(){return skt_cache_[kDirtySKTCache].GetCacheSize();}
            size_t GetCountSKTableCleanList(){return skt_cache_[kCleanSKTCache].GetCacheSize();}
            std::list<SKTableMem*>* GetSKTCleanCache(){
                return &(skt_cache_[kCleanSKTCache].cache);
            }

            const Comparator* GetComparator(){ return options_.comparator; }
            CompressionType GetCompressionType(){ return options_.compression; }
            const uint32_t GetItrPrefetchHint(){ return options_.iter_prefetch_hint; }
            bool IsPrefixDetectionEnabled(){ return options_.prefix_detection; }
            const uint32_t GetRandomPrefetchHint(){ return options_.random_prefetch_hint; }

            Iterator* CreateNewIterator(const ReadOptions& options, uint16_t col_id = 0);
            void DeleteIterator(Iterator *iter);
            Snapshot* CreateSnapshot();

            SnapshotImpl* __CreateSnapshot();
            bool __DisconnectSnapshot(SnapshotImpl* snap);

            void DeleteSnapshot(Snapshot *handle);
            Status DeleteInSDB();
            void DeleteSKTable(void *arg);
            static void DeleteSKTableWrapper(void* arg)
            { 
                DeleteSKTableCtx * delctx = reinterpret_cast<DeleteSKTableCtx*>(arg);
                delctx->mf->DeleteSKTable(arg);
                return;
            }

            Status DeleteInSDBWithManifest(uint16_t column_count);
            Status DeleteInSDBWithIter();

            void TerminateKeyCacheThread();
            void CleanUpSnapshot();
            void CheckForceFlushSKT(uint16_t worker_id, SKTableDFBuilderInfo *dfb_info);

            IterScratchPad* AllocIterScratchPad(uint32_t size);
            void FreeIterScratchPad(IterScratchPad *sp);
#if 0
            void RefillFreeUserValueCount(uint32_t size);
#endif
            UserValue* AllocUserValue(uint32_t size);
            void FreeUserValue(UserValue*);

#if 0
            void RefillFreeUserKeyCount(uint32_t key_size);
#endif
            UserKey* AllocUserKey(uint32_t key_size);
            void FreeUserKey(UserKey* ukey);

            /* this is called from WriteBatchInternal::Store()*/
            RequestNode* AllocRequestNode();
            /* Worker Thread calls it after request submission */
            void FreeRequestNode(RequestNode* req_node);
            //void CleanupRequestNodePool();
            void CleanupFreeRequestNode();

            /* memory pool function */
            void CreateMemoryPool() {
                kb_pool_ = new MemoryPool<KeyBlock>(1024, this);
                kbpm_pool_ = new MemoryPool<KeyBlockPrimaryMeta>(1024, this);
                kbml_pool_ = new MemoryPool<KeyBlockMetaLog>(1024, this);
            }
            void DestroyMemoryPool() {
                delete kb_pool_;
                delete kbpm_pool_;
                delete kbml_pool_;
            }

            KeyBlock *AllocKB() {
                return kb_pool_->AllocMemoryNode();
            }
            void FreeKB(KeyBlock *node) { 
                kb_pool_->FreeMemoryNode(node);
            }
            KeyBlockPrimaryMeta *AllocKBPM() { return kbpm_pool_->AllocMemoryNode(); }
            void FreeKBPM(KeyBlockPrimaryMeta * node) { kbpm_pool_->FreeMemoryNode(node); }
            KeyBlockMetaLog *AllocKBML() { return kbml_pool_->AllocMemoryNode(); }
            void FreeKBML(KeyBlockMetaLog *node) { kbml_pool_->FreeMemoryNode(node); }

            uint32_t GetMaxIterKeyBufferSize(void){
                return max_iter_key_buffer_size_;
            }
            uint32_t IncMaxIterKeyBufferSize(void){
                max_iter_key_buffer_size_+=kMinSKTableSize;
                return max_iter_key_buffer_size_;
            }

            uint32_t GetMaxIterKeyOffsetBufferSize(void){
                return max_iter_key_offset_buffer_size_;
            }
            uint32_t IncMaxIterKeyOffsetBufferSize(void){
                max_iter_key_offset_buffer_size_+=kMinSKTableSize;
                return max_iter_key_offset_buffer_size_;
            }

            ObjectAllocator<IterScratchPad>* GetIterScratchPadAllocator(uint16_t id){ return &sp_allocator_[id]; }
            ObjectAllocator<UserValue>* GetUVAllocator(uint16_t id){ return &uv_allocator_[id]; }
            ObjectAllocator<UserKey>* GetUKAllocator(uint16_t id){ return &uk_allocator_[id]; }
            ObjectAllocator<RequestNode>* GetRequestNodeAllocator(uint16_t id){ return &request_node_allocator_[id]; }

            uint64_t  PushKBMLog(uint16_t worker_id, KeyBlockMetaLog* kbml, uint64_t next_worker_ikey) {
                return logcleanup_data_[worker_id-1].PushKBML(kbml, next_worker_ikey);
#if 0
                LogCleanupSignal();
#endif
            }

            KeyBlock* LoadValue(uint64_t insdb_key_seq, uint32_t ivalue_size, bool *from_readahead, KeyBlockPrimaryMeta* kbpm);
#if 0
            bool DoPrefetch(ValuePrefetchNode *node);
            void PrefetchThread();
            static void PrefetchWrapper(void* mf) { reinterpret_cast<Manifest*>(mf)->PrefetchThread(); }
#endif
            UserKey* IncreaseUserKeyReference(UserKey* uk);
#ifdef INSDB_USE_HASHMAP
            bool InsertUserKeyToHashMap(UserKey* uk);
            UserKey* FindUserKeyFromHashMap(KeySlice& key);
            bool RemoveUserKeyFromHashMap(KeySlice& key);

            bool InsertKBPMToHashMap(KeyBlockPrimaryMeta *kbpm);
            KeyBlockPrimaryMeta* FindKBPMFromHashMap(SequenceNumber ikey);
            void RemoveKBPMFromHashMap(SequenceNumber ikey);
            bool KeyHashMapFindOrInsertUserKey(UserKey*& uk, const KeySlice& key, const uint16_t col_id );
#endif

            UserKey* GetFirstUserKey();
            UserKey* GetLastUserKey();
            UserKey* GetUserKeyFromKeymap(const KeySlice& key);

            UserKey* SearchUserKey(const Slice& key);
            UserKey* SearchLessThanEqualUserKey(const Slice& key);

            bool InsertUserKey(UserKey* ukey, bool begin_key = false);
            void RemoveUserKeyFromHashMapWrapper(UserKey* ukey);
            KeyBlockPrimaryMeta* GetKBPM(SequenceNumber seq, uint32_t size, bool ref_cnt_inc = true);
            KeyBlockPrimaryMeta* GetKBPM(ColumnNode* col_node);
            /* GetValue() return data src information last data_src_info bytes */
            const static uint8_t flags_data_from_dev = 0x10;
            const static uint8_t flags_data_ra_hit = 0x01;
            KeyBlockPrimaryMeta* GetValueUsingColumn(ColumnNode* col_node, Slice& value);
            bool GetLatestSequenceUsingSKTDF(const KeySlice& key, uint8_t col_id, SequenceNumber* seq_num, uint64_t cur_time);
            KeyBlockPrimaryMeta* GetValueUsingSKTDF(const KeySlice& key, Slice& value, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time);
            void SetNormalStart(bool v){ normal_start_ = v;} 
            bool CheckKeyEvictionThreshold(void);

            uint64_t MemoryUsage(){
                return memory_usage_.load(std::memory_order_relaxed);
            }
            void IncreaseMemoryUsage(uint32_t size){
                memory_usage_.fetch_add(size, std::memory_order_relaxed);
            }
            void DecreaseMemoryUsage(uint32_t size){
                memory_usage_.fetch_sub(size, std::memory_order_relaxed);
            }
            void IncIteratorCount() {
                nr_iterator_.fetch_add(1, std::memory_order_relaxed);
            }
            void DecIteratorCount() {
                if (nr_iterator_.load() < 1) printf("!!!! check nr_iterator_ count\n");
                nr_iterator_.fetch_sub(1, std::memory_order_relaxed);
            }
            /* Transaction Management functions */
            bool IsTrxnGroupCompleted(TrxnGroupID tgid);
            TransactionID GetTrxnID(uint32_t count);
            void DecTrxnGroup(TrxnGroupID tgid, uint32_t cnt);
            bool IsCompletedTrxnGroupRange(std::list<TrxnGroupRange> &completed_trxn_group_list, TrxnGroupID tgid);
            //bool IsCompletedTrxnGroupRange(TrxnGroupID tgid);
            void AddCompletedTrxnGroupRange(TrxnGroupID tgid);
            void CopyCompletedTrxnGroupRange(std::list<TrxnGroupRange> &completed_trxn_groups);
            std::unordered_map<TrxnGroupID, uint32_t> &GetUncompletedTrxnGroup() { return uncompleted_trxn_group_; }

            Slice PerThreadAllocRawMemoryNodeWrapper(uint32_t size) {
                return thread_memory_pool_.PerThreadAllocRawMemoryNode(size);
            }

            void PerThreadFreeRawMemoryNodeWrapper(Slice node) {
                thread_memory_pool_.PerThreadFreeRawMemoryNode(node);
            }

            void PrintParam();

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

        private:
            Status CleanUpSKTable(int skt_id, DeleteInSDBCtx *Delctx);
            // No copying allowed
            Manifest(const Manifest&);
            void operator=(const Manifest&);
    }; //Manifest

    /* 
     * Allocate fixed size memory during DB initialization
     * And then reuse the allocated memory until DB termination 
     */

#define COLUMN_NODE_REF_BIT 0xC0
#define COLUMN_NODE_ID_MASK      0x3F
#define COLUMN_NODE_HAS_SEQ_NUM_BIT 0x80
#define COLUMN_NODE_HAS_TTL_BIT 0x40
#define COLUMN_NODE_OFFSET_MASK 0x3f

#define LOWER_ADDR_MASK 0x00000000FFFFFFFF
#define UPPER_ADDR_SHIFT 32

#define ITER_SKTDF_OFFSET_MASK 0x000FFFFF
#define ITER_SKTDF_REMOVE_SIZE_SHIFT 20

    class SKTableMem : public SKLData<KeySlice, UserKeyComparator>{// 104 Byte
        private:
            friend class Manifest;
            Spinlock keyqueue_lock_;
            volatile uint8_t active_key_queue_;
            /*Two Queue : Active & Inactive key queue*/
            std::queue<UserKey*> *key_queue_/*[2]*/;

            folly::SharedMutex keymap_mu_;
            //GCInfo gcinfo_;
            KeyMap keymap_;

            /* Mutex lock for loading SKTable Device Format from device(KVSSD) */
            port::Mutex sktdf_load_mu_;                    // 40 Byte
            /* Shared lock for replacing the SKTDF in FlushSKTableMem()
             * No one can fetch a data from SKTDF during replacing SKTDF 
             */
            folly::SharedMutex sktdf_update_mu_;

            port::Mutex build_iter_mu_;                    // 40 Byte
            port::Mutex iter_mu_;                    // 40 Byte
            IterScratchPad** latest_iter_pad_;

#if 0
            /*
             * SKTDFCacheNode has these two variables.
             * So, deleted SKT should keep the SKTDFCacheNode until it is merged by previous valid SKTable
             */
            uint64_t next_skt_id_;
            uint64_t prealloc_skt_id_;
#endif
#if 0
            port::AtomicPointer begin_userkey_;             // 8 Byte
#else
            //char *key_buffer_[2];
            //uint16_t key_buffer_size_[2];
            char *begin_key_buffer_[2];
            uint16_t begin_key_buffer_size_[2];
            uint16_t begin_key_size_[2];
            uint32_t begin_key_hash_[2];
            volatile uint8_t active_begin_key_buffer_;
#endif
            port::AtomicPointer internal_node_;             // 8 Byte
            /* Spinlock for fetch a entry(s) from SKTable Device Format */
#if 0
            /* In new design(IterScratchPad), LoadSKTable is rarely used(only called by CompactRange())
             * But Mutex takes 40byte while spinlock needs 1 byte
             */
            port::Mutex sktdf_fetch_mu_;
#else
            Spinlock sktdf_fetch_spinlock_;
#endif
            std::atomic<uint8_t> flags_;                    // 1 Byte
            /**
             * The sorted_list_ is used for the flush
             *  1. Find next candidata for flush(previous valid SKTable : backward Round-Robin )
             *  2. Find the prev SKTable after a SKTable becomes empty and removed from the skiplist of the SKTable.
             *  3. To check whether the next SKTable is empty.
             */

            SimpleLinkedNode skt_sorted_node_;              // 16Byte
            std::list<SKTableMem*>::iterator cache_pos_;    // 8 Byte
            std::atomic<SKTDFCacheNode*> skt_df_node_;      // 8 Byte
            uint32_t skt_df_node_size_;
            uint16_t skt_df_version_;

            /* The skt_id may not change while updating the same key many times.(consume many ikey seq num) */
            uint32_t skt_id_; // 32 bit sktid.  // 4 Byte
            std::atomic<uint32_t> approximate_dirty_col_cnt_;                    // 4 Byte
            std::atomic<uint32_t> ref_cnt_;                    // 4 Byte
            /* 
             * To check whether the SKTable is cold or hot.
             * if prev_update_cnt_ is same as update_cnt_ during finding flush candidate, the SKTable is cold.
             */
            uint32_t prev_dirty_col_cnt_;                    // update it under locking
            /* 
             * coldness_ (0~255) : 0 is coldest
             * Random number
             */
            uint8_t coldness_;                    // update it under locking

            void DeviceFormatBuilder(Manifest* mf, SKTableDFBuilderInfo *dfb_info, bool sub_skt);
        public:
            SKTableMem(Manifest* mf, KeySlice& key, uint32_t skt_id, uint32_t sktdf_size);
            SKTableMem(Manifest *mf, KeySlice *begin_key);
            ~SKTableMem();
            bool IsActiveKeyQueueEmpty(){
                keyqueue_lock_.Lock();
                bool empty = (key_queue_[active_key_queue_].empty());
                keyqueue_lock_.Unlock();
                return empty;
            }
            bool IsKeyQueueEmpty(){
                return (key_queue_[0].empty() && key_queue_[1].empty());
            }
            void PushUserKeyToActiveKeyQueue(UserKey* uk){
                keyqueue_lock_.Lock();
                key_queue_[active_key_queue_].push(uk);
                keyqueue_lock_.Unlock();
            }
            uint8_t ExchangeActiveAndInActiveKeyQueue(){
                keyqueue_lock_.Lock();
                active_key_queue_ ^= 1;
                keyqueue_lock_.Unlock();

                return active_key_queue_ ^ 1;
            }
            UserKey* PopUserKeyFromKeyQueue(uint8_t idx){
                assert(idx == (active_key_queue_^1));
                /* Lock is not required because it is Inactive Key Queue*/
                UserKey* uk = NULL;
                if(!key_queue_[idx].empty()){
                    uk = key_queue_[idx].front();
                    key_queue_[idx].pop();
                }
                return uk;
            }
            bool InsertUserKeyToKeyMapFromInactiveKeyQueue(Manifest *mf); 
            bool EvictCleanUserKeyFromKeymap(Manifest *mf, bool eviction_only, bool &evicted, bool cache_evict = false);
            KeyMap* GetKeyMap() { return &keymap_; }
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

            void SKTDFUpdateSharedLock() { sktdf_update_mu_.lock_shared(); }
            void SKTDFUpdateSharedUnLock() { sktdf_update_mu_.unlock_shared(); }
            void SKTDFUpdateGuardLock() { sktdf_update_mu_.lock(); }
            void SKTDFUpdateGuardUnLock() { sktdf_update_mu_.unlock(); }

            void SKTDFFetchSpinLock() { sktdf_fetch_spinlock_.Lock(); }
            void SKTDFFetchSpinUnlock() { sktdf_fetch_spinlock_.Unlock(); }

            void BuildIterLock(){build_iter_mu_.Lock();}
            void BuildIterUnlock(){build_iter_mu_.Unlock();}

            void IterLock(){ iter_mu_.Lock(); }
            void IterUnlock(){ iter_mu_.Unlock(); }

            void SetLatestIterPad(Manifest* mf, IterScratchPad* pad, uint8_t col_id){
                assert(col_id < kMaxColumnCount);
                if(!latest_iter_pad_[col_id]){
                    pad->IncIterScratchPadRef();
                    latest_iter_pad_[col_id] = pad;
                }else if(pad->begin_seq_ > latest_iter_pad_[col_id]->begin_seq_){
                    pad->IncIterScratchPadRef();
                    if(latest_iter_pad_[col_id]->DecIterScratchPadRef()){
                        latest_iter_pad_[col_id]->ClearIterScratchpad(mf);
                        mf->FreeIterScratchPad(latest_iter_pad_[col_id]);
                    }
                    latest_iter_pad_[col_id] = pad;
                }
            }

            //ttl should be considered, but now we just ignore the ttl for quick/simple implementation.
            IterScratchPad* GetLatestIterPad(Manifest *mf, SequenceNumber begin_seq/*, uint64_t ttl*/, uint8_t col_id){ 
                if(latest_iter_pad_[col_id] && begin_seq >= latest_iter_pad_[col_id]->begin_seq_ && begin_seq <= latest_iter_pad_[col_id]->last_seq_){
                    /* begin_seq is within the range of latest_iter_pad_ */
                    latest_iter_pad_[col_id]->IncIterScratchPadRef();
                    latest_iter_pad_[col_id]->IncActiveIterScratchPadRef();
#ifdef CODE_TRACE
                    printf("reuse  Iterpad for skt *%d) from skt latest iter\n", GetSKTableID());
#endif
                    return latest_iter_pad_[col_id];

                }
                if(!InsertUserKeyToKeyMapFromInactiveKeyQueue(mf) && latest_iter_pad_[col_id] && begin_seq >= latest_iter_pad_[col_id]->begin_seq_){
                    /*There is no unflushed UserKey and begin_seq belongs to the latest_iter_pad_ */
                    if(latest_iter_pad_[col_id]->last_seq_ < begin_seq)
                        latest_iter_pad_[col_id]->last_seq_ = begin_seq;
                    latest_iter_pad_[col_id]->IncIterScratchPadRef();
                    latest_iter_pad_[col_id]->IncActiveIterScratchPadRef();
#ifdef CODE_TRACE
                    printf("reuse  Iterpad for skt *%d) from skt latest iter after apply Inactive queue\n", GetSKTableID());
#endif
                    return latest_iter_pad_[col_id];
                }
                return NULL;
            }

            void ReturnIterScratchPad(Manifest* mf){
                for (int i = 0; i < kMaxColumnCount; i++) {
                    if(latest_iter_pad_[i] && latest_iter_pad_[i]->DecIterScratchPadRef()){
                        latest_iter_pad_[i]->ClearIterScratchpad(mf);
                        mf->FreeIterScratchPad(latest_iter_pad_[i]);
                    }
                    latest_iter_pad_[i] = nullptr;
                }
            }

            bool PrefetchSKTable(Manifest* mf, bool dirtycache = true);
            void DiscardSKTablePrefetch(Manifest* mf);
            bool LoadSKTableDeviceFormat(Manifest *mf, GetType type = kSyncGet);

            UserKey* InsertDeviceFormatUserKey(Manifest* mf, KeySlice& target_key, Slice col_info_slice, bool inc_ref_cnt);
            void LoadSKTable(Manifest* mf);
            bool GetLatestSequenceNumber(Manifest* mf, const KeySlice& key, uint8_t col_id, SequenceNumber* seq_num, uint64_t cur_time);
            bool GetColumnInfo(Manifest* mf, const KeySlice& key, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, ColumnScratchPad& column);
            uint16_t LookupColumnInfo(Manifest *mf, Slice& col_info_slice, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, SequenceNumber &biggest_begin_key, bool force_prefetch = false);
            void FillIterScaratchPadWithColumnInfo(Manifest *mf, IterScratchPad* iter_pad, char*& key_buffer_loc, Slice& col_info_slice, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, SequenceNumber &biggest_begin_key, bool force_prefetch = false);

            bool PrefetchKeyBlocks(Manifest *mf, uint8_t col_id, uint64_t cur_time, SequenceNumber seq_num, PrefetchDirection dir);
            ColumnNode* FindNextValidColumnNode(Manifest *mf, UserKey*&  ukey, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, KeySlice* end_key);
            void SetIterCurrentOffset(Manifest* mf, KeyBufferEntry*& key_buffer_loc, /*KeyBlockPrimaryMeta* kbpm,*/ IterCurrentKeyInfo* cur_info, const KeySlice* key, std::string& key_from_skt_df, bool search_less_than);
            UserKey* MoveToNextKeyInKeyMap(Manifest* mf, UserKey* ukey);
            bool BuildIteratorScratchPad(Manifest *mf, IterScratchPad* iter_pad, const KeySlice* key, uint8_t col_id, uint64_t cur_time, SequenceNumber seq_num);
            bool SKTDFReclaim(Manifest *mf, int64_t &reclaim_size, bool &evicted, bool force_evict);


            void BuildUpdateList(std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>>& del, std::list<std::tuple<KeyBlockMeta*,
                    uint8_t/*offset*/, RequestType, bool>>& update, std::string* buffer, uint32_t& size);
            bool DecodeUpdateList(std::list<std::tuple<uint64_t, uint8_t/*offset*/, RequestType>>& del, std::list<std::tuple<uint64_t,
                    uint8_t/*offset*/, RequestType>>& update, char* buffer, uint32_t& size);
#if 0
            void UpdateSKTableId(uint32_t prealloc_skt_id, uint32_t next_skt_id) { prealloc_skt_id_ = prealloc_skt_id; next_skt_id_ = next_skt_id; }
#endif
            // SKLData Skiplist interface
            void SetNode(void* node) { internal_node_.Release_Store(node); }
            void* GetNode() { return internal_node_.Acquire_Load(); }
            KeySlice GetKeySlice() ; //override;
            bool CompareKey(UserKeyComparator comp, const KeySlice& key, int& delta) ; //override;

            uint32_t GetSKTableID() { return skt_id_; }
            void SetSKTableID(uint32_t skt_id) { skt_id_ = skt_id; }

            uint32_t GetNextSKTableID() { 
                assert(skt_df_node_);
                return skt_df_node_.load()->GetNextSKTableID();
            }
            void SetNextSKTableID(uint32_t skt_id) { 
                assert(skt_df_node_);
                skt_df_node_.load()->SetNextSKTableID(skt_id);
            }

            uint32_t GetPreallocSKTableID() { 
                assert(skt_df_node_);
                return skt_df_node_.load()->GetPreallocSKTableID();
            }
            void SetPreallocSKTableID(uint32_t skt_id) { 
                assert(skt_df_node_);
                skt_df_node_.load()->SetPreallocSKTableID(skt_id);
            }

            /*
             * Copy the user key to the given buffer
             * Return key size
             */
            uint16_t GetBeginKey(char*& key_buffer, uint16_t& key_buffer_size, uint32_t& key_hash){
                uint8_t idx = active_begin_key_buffer_;
                if(!begin_key_size_[idx])
                    return 0;
                if(key_buffer_size < begin_key_size_[idx]){
                    free(key_buffer);
                    key_buffer = (char*)malloc(begin_key_size_[idx]);
                    key_buffer_size = begin_key_size_[idx];
                }
                memcpy(key_buffer, begin_key_buffer_[idx], begin_key_size_[idx]);
                key_hash = begin_key_hash_[idx];
                return begin_key_size_[idx];
            }
            KeySlice GetBeginKeySlice(void){
                uint8_t idx = active_begin_key_buffer_;
                if(!begin_key_size_[idx])
                    return KeySlice(0);
                return KeySlice(begin_key_buffer_[idx], begin_key_size_[idx], begin_key_hash_[idx]);
            }
            void SetBeginKey(KeySlice new_begin){
                uint8_t new_idx = active_begin_key_buffer_ ^ 1;
                uint16_t size = new_begin.size();
                if (!size) abort();
                if(begin_key_buffer_size_[new_idx] < size){
                    free(begin_key_buffer_[new_idx]);
                    begin_key_buffer_[new_idx] = (char*)malloc(size+1);
                    begin_key_buffer_size_[new_idx] = size;
                }
                memcpy(begin_key_buffer_[new_idx], new_begin.data(), size);

                begin_key_buffer_[begin_key_buffer_size_[new_idx]] = '\0';
                begin_key_size_[new_idx] = size;
                begin_key_hash_[new_idx] = new_begin.GetHashValue();
                active_begin_key_buffer_ = new_idx;
            }
            // SKTableMem flags
            const static uint8_t flags_fetch_in_progress        = 0x01;
            const static uint8_t flags_flush_in_progress        = 0x02;
            const static uint8_t flags_eviction_in_progress     = 0x04;
            const static uint8_t flags_in_dirty_cache           = 0x08;
            const static uint8_t flags_has_been_flushed         = 0x10;
            const static uint8_t flags_has_iter_update_list     = 0x20;
            const static uint8_t flags_deleted_skt              = 0x40;
            const static uint8_t flags_prefetch_keyblock        = 0x80;
            const static uint8_t flags_prefetch_keyblock_and    = 0x7F;

            // flushing
            /* Set the flags_flush_in_progress under lock */
            bool IsFlushOrEvicting() { return flags_.load(std::memory_order_seq_cst) & (flags_flush_in_progress | flags_eviction_in_progress); }
            bool IsFlushing() { return (flags_.load(std::memory_order_seq_cst)& flags_flush_in_progress); }
            bool SetFlushing() { return flags_.fetch_or(flags_flush_in_progress, std::memory_order_seq_cst)& flags_flush_in_progress; }
            void ClearFlushing() { flags_.fetch_and(~flags_flush_in_progress, std::memory_order_seq_cst); }
            //Evicting

            bool IsEvictingSKT() { return (flags_.load(std::memory_order_seq_cst)& flags_eviction_in_progress); }
            void SetEvictingSKT() { flags_.fetch_or(flags_eviction_in_progress, std::memory_order_seq_cst); }
            void ClearEvictingSKT() { flags_.fetch_and(~flags_eviction_in_progress, std::memory_order_seq_cst); }
            void ClearFlushAndEvicting() { flags_.fetch_and(~(flags_flush_in_progress | flags_eviction_in_progress), std::memory_order_seq_cst); }
            // fetch in progress
            bool IsFetchInProgress() {
                return (flags_.load(std::memory_order_relaxed)& flags_fetch_in_progress); 
            }
            bool SetFetchInProgress() { 
                /*This is accessed under lock(sktdf_load_mu_) */
                return (flags_.fetch_or(flags_fetch_in_progress, std::memory_order_relaxed) & flags_fetch_in_progress); 
            }

            /*If prefetch was not issued, insert the SKTableMem to cache 
            */
            bool ClearFetchInProgress() { 
                /*This is accessed under lock(sktdf_load_mu_) */
                return flags_.fetch_and(~flags_fetch_in_progress, std::memory_order_relaxed) & flags_fetch_in_progress; }

            bool HasIterUpdateList() { return (flags_.load(std::memory_order_relaxed)& flags_has_iter_update_list); }
            void SetIterUpdateList() { flags_.fetch_or(flags_has_iter_update_list, std::memory_order_relaxed); }
            void ClearIterUpdateList() { flags_.fetch_and((uint8_t)(~flags_has_iter_update_list), std::memory_order_relaxed); }

            bool IsInDirtyCache() { return (flags_.load(std::memory_order_relaxed) & flags_in_dirty_cache); }
            bool SetInDirtyCache() { return (flags_.fetch_or(flags_in_dirty_cache, std::memory_order_relaxed) & flags_in_dirty_cache); }
            bool ClearInDirtyCache() { return flags_.fetch_and((uint8_t)(~flags_in_dirty_cache), std::memory_order_relaxed) & flags_in_dirty_cache; }

            bool HasBeenFlushed() { return (flags_.load(std::memory_order_relaxed)& flags_has_been_flushed) != 0; }
            void SetHasBeenFlushed() {  flags_.fetch_or(flags_has_been_flushed, std::memory_order_relaxed); }

            void SetDeletedSKT() { flags_.fetch_or(flags_deleted_skt, std::memory_order_relaxed); }
            void ClearDeletedSKT() { flags_.fetch_and(~flags_deleted_skt, std::memory_order_relaxed); }
            bool IsDeletedSKT() { return (flags_.load(std::memory_order_relaxed) & flags_deleted_skt); }

            bool SetPrefetchKeyBlock() { return (flags_.fetch_or(flags_prefetch_keyblock, std::memory_order_relaxed) & flags_prefetch_keyblock); }
            void ClearPrefetchKeyBlock() { flags_.fetch_and(flags_prefetch_keyblock_and, std::memory_order_relaxed); }
            bool IsPrefetchKeyBlock() { return (flags_.load(std::memory_order_relaxed) & flags_prefetch_keyblock); }

            /**
             * Return whether the SKTableMem was flushed or not
             * force : if any update has occured, perform flush
             *         For DB termination, force must be true. 
             */
            void GetUpdatedKBMList(Manifest *mf, std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>> *kbm_list, std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>> *kbm_pending_list, std::list<std::tuple<KeyBlockMeta*, bool>> *device_submit_queue, uint8_t worker_id);
            Status FlushSKTableMem(Manifest *mf, uint8_t worker_id, SKTableDFBuilderInfo *dfb_info);
            char* EncodeFirstKey(const KeySlice &key, char *buffer, uint32_t &buffer_size);
            char* EncodeKey(const std::string &begin_key_of_target, const KeySlice &key, char *buffer, uint32_t &buffer_size);
            char* EncodeKey(char *buffer, uint32_t &buffer_size);

            ColumnStatus CheckColumnStatus(Manifest* mf, SKTableDFBuilderInfo *dfb_info, UserKey* uk, ColumnNode* col_node, ColumnNode* next_col_node, uint8_t col_id,
                    KeyBlockMeta* kbm, bool iter, bool &drop_col_node, bool& recent_col_node_deleted, bool bDecNrs);
            bool SplitByPrefix(const Slice& a, const Slice& b);
            //bool CheckSplitCondition(Manifest* mf, bool sktdf_data, UserKey* next_uk, std::string& key_from_skt_df, KeySlice cur_key_slice, uint32_t buffer_size, SKTableDFBuilderInfo *dfb_info, uint32_t skt_info_2_size, uint32_t max_buffer_size);

            SKTableMem* Next(Manifest* mf);
            SKTableMem* Prev(Manifest* mf);

            SimpleLinkedNode* GetSKTableSortedListNode(){return &skt_sorted_node_;}
            bool IsInSKTableCache(Manifest* mf) {
                if (cache_pos_ != mf->GetSKTCleanCache()->end()) return true;
                return false;
            }

            SKTDFCacheNode* GetSKTableDeviceFormatNode(){ return skt_df_node_; }
            void SetSKTableDeviceFormatNode(Manifest *mf, SKTDFCacheNode* skt_df) { 
                mf->IncreaseMemoryUsage(skt_df->GetSize());
                skt_df_node_ = skt_df;
            }

            void UpdateSKTDFNodeSize(Manifest *mf, uint32_t new_size, uint32_t old_size){
                if(new_size > old_size)
                    mf->IncreaseMemoryUsage(new_size - old_size);
                else if( new_size < old_size)
                    mf->DecreaseMemoryUsage(old_size - new_size );
            }

            void DecreaseSKTDFNodeSize(Manifest *mf){
                mf->DecreaseMemoryUsage(skt_df_node_.load()->GetSize());
            }

            void DeleteSKTDeviceFormatCache(Manifest* mf) {
                // Remove SKT DF format

                SKTDFCacheNode* sktdf = skt_df_node_;
                sktdf->CleanUp(mf);
                delete sktdf;
                skt_df_node_ = nullptr;
            }

            void SetSKTDFNodeSize(uint32_t size){
                skt_df_node_size_ = size;
            }
            uint32_t GetSKTDFNodeSize(void){
                return skt_df_node_size_;
            }
            void IncSKTDFVersion(){
                skt_df_version_++;
            }
            uint16_t GetSKTDFVersion(void){
                return skt_df_version_;
            }

            void SetLocation(std::list<SKTableMem*>::iterator iter) { cache_pos_ = iter; } 
            std::list<SKTableMem*>::iterator GetLocation() { return cache_pos_; }

            /* Use approximate_dirty_col_cnt_ instead of update_cnt_ 
             * an update may count twice when the SKTable is being flushed(one in BuildKeyBlock and one in DeviceFormatBuilder).
             */
            uint32_t IncApproximateDirtyColumnCount(void){ return approximate_dirty_col_cnt_.fetch_add(1, std::memory_order_relaxed); }
            /* Set approximate_dirty_col_cnt_ to 0 before start flushing*/
            void ClearApproximateDirtyColumnCount(){ approximate_dirty_col_cnt_.store(0, std::memory_order_relaxed); }
            uint32_t GetApproximateDirtyColumnCount(void){
                return approximate_dirty_col_cnt_.load(std::memory_order_seq_cst);
            }
            void SetPrevDirtyColumnCount(uint32_t cnt){ prev_dirty_col_cnt_= cnt; }
            uint32_t GetPrevDirtyColumnCount(void){ return prev_dirty_col_cnt_; }

            bool IsSKTDirty(Manifest *mf)
            {
                return GetKeyMap()->GetCount() || !IsKeyQueueEmpty() || GetApproximateDirtyColumnCount() || mf->IsNextSKTableDeleted(GetSKTableSortedListNode());
            }

            void IncSKTRefCnt(void){ 
                uint32_t old = ref_cnt_.fetch_add(1, std::memory_order_seq_cst);

#ifdef REFCNT_SANITY_CHECK
                if ((old+1)==0) abort();
#endif
                while(IsEvictingSKT());
            }
            void DecSKTRefCnt(void){
                uint32_t old = ref_cnt_.fetch_sub(1, std::memory_order_seq_cst);
#ifdef REFCNT_SANITY_CHECK
                if (old==0) abort();
#endif
            }
            uint32_t GetSKTRefCnt(void){ return ref_cnt_.load(std::memory_order_seq_cst); }

            //uint32_t GetSize(){ return sizeof(class SKTableMem);}
            uint32_t GetSize(){ return 1;}
            void SetColdness(uint8_t seed){
                assert(seed);
                coldness_ = seed;
            }
            uint8_t DecColdness(){
                assert(coldness_);
                return --coldness_;
            }
            uint8_t GetColdness(){
                return coldness_;
            }

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
            void Init(uint32_t size) {
                if (raw_data_size_) {
                    data_size_ = size = SizeAlignment(size);
                    if (size > raw_data_size_) {
                        //raw_data_ = (char*)realloc((void*)raw_data_, size);
                        free(raw_data_);
                        raw_data_ = (char*)malloc(size);
                        raw_data_size_ = size;
                    }
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
            bool DecodeAllKeyMeta(KeyBlockMetaLog *kbml, std::list<ColumnInfo> &key_list);
    };

#define ADDRESS_SHIFT           16
#define ADDRESS_MASK            0x0000FFFFFFFFFFFF
    enum KeyBlockMetaType{
        kKeyBlockPrimary = 0,
        kKeyBlockMetaLog = 1,
    };

    class KeyBlockMeta{
        public:
            virtual ~KeyBlockMeta() = default;
            virtual TrxnGroupID GetTrxnGroupID(RequestType t, uint8_t idx) = 0;
            virtual KeyBlockMetaType GetKeyBlockMetaType() = 0;
            virtual uint32_t BuildDeviceFormatKeyBlockMeta(char* kbm_buffer) = 0;
            virtual bool SetWriteProtection() = 0;
            virtual void ClearWriteProtection() = 0;

            virtual bool SetSubmitInProgress()  = 0;
            virtual void ClearSubmitInProgress() = 0;

            virtual bool TestKeyBlockBitmap() = 0;
            virtual void SetKeyBlockBitmap(uint8_t bit_index) = 0;
            virtual bool ClearKeyBlockBitmap(uint8_t bit_index, RequestType t) = 0; 
            virtual uint32_t GetKeyBlockBitmap() = 0;

            virtual KeyBlock* GetKeyBlock() = 0;
            virtual KeyBlockPrimaryMeta* GetKBPM() = 0;
            virtual void SetWorkerBit(uint8_t) = 0;
            virtual void ClearWorkerBit() = 0;
            virtual bool TestWorkerBit(uint8_t) = 0;
            virtual SequenceNumber GetiKeySequenceNumber() = 0;
            virtual bool IsInlog() = 0;
            virtual uint8_t DecNRSCount() = 0;
            virtual uint8_t GetOriginalKeyCount() = 0;
            virtual uint32_t GetEstimatedBufferSize() = 0;
            virtual uint16_t IncRefCnt() = 0;
            virtual uint16_t DecRefCnt() = 0;
            virtual uint16_t GetRefCnt() = 0;
            virtual bool IsDummy() = 0;
            virtual Status Prefetch(Manifest *mf) = 0;
    };
    class KeyBlockPrimaryMeta : public KeyBlockMeta {// 82 Byte = 42 + 40(Mutex)
        private:
            std::atomic<uint8_t> flags_;               // 1 Byte
            std::atomic<uint16_t> ref_cnt_;               // 2 Byte
            uint32_t kb_size_;/* For device write*/               // 4 Byte
            std::atomic<uint32_t> kb_bitmap_;                       // 4 Byte
            std::atomic<uint32_t> ref_bitmap_;                      // 4 Byte
            volatile KeyBlock* kb_ptr_;    // 8 Byte
            uint64_t org_key_cnt_;    // 8 Byte
            uint64_t ikey_seq_num_;/* For device write*/            // 8 Byte
            std::atomic<uint64_t> worker_bit_;                      // 8 Byte
            KeyBlockMetaLog *kbml_;                                 //8 Byte
            port::Mutex mu_;                                        //40 Byte

            const static uint8_t flags_write_protection    = 0x01;
            const static uint8_t flags_submit_in_progress  = 0x02;
            const static uint8_t flags_dummy               = 0x04;
            /* The Key Block only contains delete commands. In this case, KeyBlock is not created. only create KeyBlockMetas*/
            const static uint8_t flags_del_only            = 0x08;
            const static uint8_t flags_evicting            = 0x10;
            const static uint8_t flags_in_out_log          = 0x20;/* 1 is for in-log */
            const static uint8_t flags_kb_prefetched       = 0x40;/* 1 is for in-log */
        public:
#define KBPM_HEADER_SIZE 5
            KeyBlockPrimaryMeta(Manifest *mf): flags_(0), ref_cnt_(0), kb_size_(0),
            kb_bitmap_(0), ref_bitmap_(0), kb_ptr_(NULL), org_key_cnt_(0), ikey_seq_num_(0), worker_bit_(0), kbml_(NULL), mu_(){
            }
            void Lock(){ mu_.Lock(); }
            void Unlock(){ mu_.Unlock(); }

            void InitInDummyState(uint8_t orig_keycnt) {
                assert(IsDummy());
                kbml_ = NULL;
                /* Set Out log*/
                SetOutLogRelaxed();
                // update original key count
                org_key_cnt_ = 0;
                if(!orig_keycnt) {
                    flags_.fetch_or(flags_del_only, std::memory_order_relaxed);
                } else
                    org_key_cnt_ = orig_keycnt;
                /* Set bitmap */
                assert(orig_keycnt <= kMaxRequestPerKeyBlock);
                kb_bitmap_.store(0, std::memory_order_relaxed);
                ref_bitmap_.store(0, std::memory_order_relaxed);
            }
            void Init(uint64_t ikey_seq_num, uint32_t size = 0) {
                ref_cnt_.store(0, std::memory_order_relaxed);
                kb_size_ = size;
                kb_bitmap_.store(0, std::memory_order_relaxed);
                ref_bitmap_.store(0, std::memory_order_relaxed);
                kb_ptr_ = NULL;
                org_key_cnt_ = 0;
                worker_bit_.store(0, std::memory_order_relaxed);
                kbml_ = NULL;
                flags_.store(0, std::memory_order_relaxed);
                ikey_seq_num_ = ikey_seq_num;
            }
            void Init(uint8_t put_cnt, uint8_t del_cnt, KeyBlock *kb, uint32_t size = 0) {
                ref_cnt_.store(put_cnt + del_cnt, std::memory_order_relaxed);
                kb_size_ = size;
                kb_bitmap_.store(0, std::memory_order_relaxed);
                ref_bitmap_.store(0, std::memory_order_relaxed);
                kb_ptr_ = NULL;
                org_key_cnt_ = 0;
                worker_bit_.store(0, std::memory_order_relaxed);
                kbml_ = NULL;
                /* Set In log*/
                flags_.store(flags_in_out_log, std::memory_order_relaxed);
                if(!put_cnt)
                    flags_.fetch_or(flags_del_only, std::memory_order_relaxed);
                else
                    org_key_cnt_ = put_cnt;
                if(kb) kb_ptr_ = kb;
                /* Set bitmap */
                assert(put_cnt <= kMaxRequestPerKeyBlock);
                for(uint8_t i = 0 ; i < put_cnt ; i++){
                    kb_bitmap_.fetch_or((1 << i), std::memory_order_relaxed);
                }
                ikey_seq_num_ = 0;
            }
            void SetKBSize(uint32_t kb_size) { kb_size_ = kb_size;}
            uint32_t GetKBSize() { return kb_size_; }
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

            void SetKBML(KeyBlockMetaLog* kbml){
                assert(!IsDummy());
                kbml_ = kbml;
            }

            KeyBlockMetaLog *GetKBML() {
                return kbml_;
            }

            uint8_t GetOriginalKeyCount() { assert(!IsDummy()); return org_key_cnt_; }

            bool SetWriteProtection() { return (flags_.fetch_or(flags_write_protection, std::memory_order_acquire) & flags_write_protection); }
            void ClearWriteProtection() { flags_.fetch_and((~flags_write_protection), std::memory_order_release); }

            /*If the caller has KBPM(not KBML), NRS must already be decreased */
            uint8_t DecNRSCount(){ assert(0); return 0; }
            bool SetSubmitInProgress() { assert(!IsDummy()); return (flags_.fetch_or(flags_submit_in_progress, std::memory_order_acquire) & flags_submit_in_progress); }
            void ClearSubmitInProgress() { assert(!IsDummy()); flags_.fetch_and(~flags_submit_in_progress, std::memory_order_release); }

            void SetDummy() { assert(!IsDummy()); flags_.fetch_or(flags_dummy, std::memory_order_seq_cst); }
            void ClearDummy() { flags_.fetch_and(~flags_dummy, std::memory_order_seq_cst); }
            bool IsDummy() { return (flags_.load(std::memory_order_seq_cst) & flags_dummy); }

            bool IsDeleteOnly() { assert(!IsDummy()); return (flags_.load(std::memory_order_seq_cst) & flags_del_only); }
            void ClearDeleteOnly() { flags_.fetch_and(~flags_del_only, std::memory_order_seq_cst); }

            bool SetEvicting() { return flags_.fetch_or(flags_evicting, std::memory_order_seq_cst) & flags_evicting; }
            void ClearEvicting() { flags_.fetch_and(~flags_evicting, std::memory_order_seq_cst); }
            bool IsEvicting() { return (flags_.load(std::memory_order_seq_cst) & flags_evicting); }

            void SetInLog() { assert(!IsDummy()); flags_.fetch_or(flags_in_out_log, std::memory_order_seq_cst); }
            void SetOutLog() { flags_.fetch_and(~flags_in_out_log, std::memory_order_seq_cst); }
            void SetOutLogRelaxed() { flags_.fetch_and(~flags_in_out_log, std::memory_order_relaxed); }
            /* If it is evictable, return true*/
            bool SetOutLogAndCheckEvicable() { 
                flags_.fetch_and(~flags_in_out_log, std::memory_order_seq_cst); 
                return (!ref_cnt_ && !CheckRefBitmap());
            }
            bool IsInlog() { return (flags_.load(std::memory_order_seq_cst) & flags_in_out_log); }

            /* Under lock */
            void SetKBPrefetched() { flags_.fetch_or(flags_kb_prefetched, std::memory_order_release); }
            bool ClearKBPrefetched() { return flags_.fetch_and(~flags_kb_prefetched, std::memory_order_release) & flags_kb_prefetched; }
            bool IsKBPrefetched() { return (flags_.load(std::memory_order_release) & flags_kb_prefetched); }


            /* Hoding Iter functions are Protected by flags_write_protction */
            void SetKeyBlockBitmap(uint8_t bit_index){ 
                assert(!IsDummy());
                kb_bitmap_.fetch_or(1 << bit_index, std::memory_order_seq_cst);
            }
            bool ClearKeyBlockBitmap(uint8_t bit_index, RequestType t){
                assert(t == kPutType);
                assert(!IsDummy());
                return (kb_bitmap_.fetch_and(~(1 << bit_index), std::memory_order_seq_cst) & ~(1 << bit_index));
            }

            //////////////

            bool CheckRefBitmap(){
                return ref_bitmap_;
            }
            bool SetRefBitmap(uint8_t bit_index){ 
                return ref_bitmap_.fetch_or(1 << bit_index, std::memory_order_seq_cst) & (1 << bit_index);
            }
            bool ClearRefBitmap(uint8_t bit_index){
                return (ref_bitmap_.fetch_and(~(1 << bit_index), std::memory_order_seq_cst) & ~(1 << bit_index));
            }
            bool IsRefBitmapSet(uint8_t bit_index){
                return (ref_bitmap_.load(std::memory_order_seq_cst) & (1 << bit_index));
            }
            void RestoreRefBitmap(uint8_t bit_index){
                ref_bitmap_.fetch_and(~((1 << bit_index)-1), std::memory_order_seq_cst);
            }
            bool ClearAllRefBitmap(){
                return ref_bitmap_ = 0;
            }
            ////////////

            /* Ref count & holding iter cnt share same atomic variable */
            uint16_t GetRefCnt() { return ref_cnt_.load(std::memory_order_seq_cst); }
            uint16_t IncRefCnt() {
                uint16_t old = ref_cnt_.fetch_add(1, std::memory_order_seq_cst);
#ifdef REFCNT_SANITY_CHECK
                if(old ==USHRT_MAX) abort();
#endif
                return old;
            }
            uint16_t DecRefCnt() {
                uint16_t old = ref_cnt_.fetch_sub(1, std::memory_order_seq_cst);
#ifdef REFCNT_SANITY_CHECK
                if(old == 0) abort();
#endif
                return old;
            }
            bool DecRefCntAndCheckEvicable() {
                uint16_t old = ref_cnt_.fetch_sub(1, std::memory_order_seq_cst);
#ifdef REFCNT_SANITY_CHECK
                if(old == 0) abort();
#endif
                return !((old - 1) || IsInlog() || CheckRefBitmap());
            }

            void OverrideKeyBlockBitmap(uint32_t new_bitmap) {
                assert(IsDummy());
                kb_bitmap_ = new_bitmap;
            }
            uint32_t GetKeyBlockBitmap(){ assert(!IsDummy()); return kb_bitmap_; }
            bool TestKeyBlockBitmap(){
                assert(!IsDummy());
                return GetKeyBlockBitmap();
            }
            KeyBlockMetaType GetKeyBlockMetaType(){ return kKeyBlockPrimary; }
            uint32_t GetEstimatedBufferSizeInternal(){
                assert(!IsDummy());
                return KBPM_HEADER_SIZE ; //bitmap + orignal key count
            }
            uint32_t GetEstimatedBufferSize();
            void SetInSDBKeySeqNum(uint64_t ikey_seq_num) {
                ikey_seq_num_ = ikey_seq_num;
            }
            uint32_t BuildDeviceFormatKeyBlockMetaInternal(char* kbm_buffer){
                assert(!IsDummy());
                char* buf = kbm_buffer;
                uint32_t size = 0;
                EncodeFixed32(buf, kb_bitmap_.load(std::memory_order_seq_cst));
                size += sizeof(uint32_t);
                buf += sizeof(uint32_t);
                *buf = GetOriginalKeyCount();
                size++;
                return size;
            }
            uint32_t BuildDeviceFormatKeyBlockMeta(char* kbm_buffer);
            static const uint32_t max_size = 64*1024;
            Status Prefetch(Manifest *mf) {
                int size = max_size;
                return mf->GetEnv()->Get(GetKBKey(mf->GetDBHash(), kKBMeta, ikey_seq_num_), nullptr, &size, kReadahead);
            }
            Status LoadDeviceFormat(Manifest *mf, char* &kbm_buffer, int &buffer_size) {
                assert(IsDummy());
                kbm_buffer = new char[max_size];
                buffer_size = max_size;
                Status s = mf->GetEnv()->Get(GetKBKey(mf->GetDBHash(), kKBMeta, ikey_seq_num_), kbm_buffer, &buffer_size);
                assert(buffer_size <= max_size);
                return s;
            }
            void FreeDeviceFormatBuffer(char* kbm_buffer) { delete [] kbm_buffer; }
            bool InitWithDeviceFormat(Manifest  *mf, const char *buffer, uint32_t buffer_size, KeyBlockMetaLog* &kbml, bool outlog = true);
            SequenceNumber GetiKeySequenceNumber(){ return ikey_seq_num_; }

            TrxnGroupID GetTrxnGroupID(RequestType t, uint8_t idx){
                abort();/* Flushed(Committed) TRXN never call this function */
                return UINT_MAX;
            }
            KeyBlockPrimaryMeta* GetKBPM() {
                return this;
            }

            KeyBlock* GetKeyBlock() {
                return GetKeyBlockAddr();
            }

            void SetWorkerBit(uint8_t worker_id){
                worker_bit_.fetch_or((1 << worker_id) , std::memory_order_seq_cst);
            }
            void ClearWorkerBit(){
                worker_bit_.store(0, std::memory_order_seq_cst);
            }
            bool TestWorkerBit(uint8_t worker_id){
                return worker_bit_.load(std::memory_order_seq_cst) & (1 << worker_id);
            }

            void CleanUp() {
                assert(!GetRefCnt());
                assert((flags_.load(std::memory_order_seq_cst)&(~(flags_evicting | flags_dummy))) == 0);
                assert(!kbml_);
                assert(!kb_ptr_);
                kb_bitmap_.store(0,std::memory_order_relaxed);
                org_key_cnt_ = 0;
                worker_bit_.store(0,std::memory_order_relaxed);
                kbml_ = NULL;
            }

            bool DecRefCntAndTryEviction(Manifest *mf, bool cache_evict = false) {
                bool deletable = false;
                if(DecRefCntAndCheckEvicable() && ( cache_evict || mf->CheckKeyEvictionThreshold())){
                    /* If it is last one */
                    KeyBlock *kb;
                    if(!SetEvicting()){
                        if(GetRefCnt() == 0 && !CheckRefBitmap()) {
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(GetRefCnt() || CheckRefBitmap());/* Wait until deref */
                            assert(!GetRefCnt());
                            kb = GetKeyBlockAddr();
                            if(kb){
                                mf->DecreaseMemoryUsage(kb->GetSize());
                                mf->FreeKB(kb);
                                ClearKeyBlockAddr();
                            }else{
                                if (ClearKBPrefetched()) {
                                    InSDBKey kbkey = GetKBKey(mf->GetDBHash(), kKBlock, ikey_seq_num_);
                                    (mf->GetEnv())->DiscardPrefetch(kbkey);
                                }
                            }
                            deletable = true;
                        }else
                            ClearEvicting();
                    }
                }
                return deletable;
            }
            bool ClearRefBitAndTryEviction(Manifest *mf, uint8_t offset, int64_t &reclaim_size){
                bool deletable = false;
                if( GetRefCnt() == 0 && !ClearRefBitmap(offset) && !IsInlog()){
                    /* If it is last one */
                    KeyBlock *kb;

                    if(!SetEvicting()){
                        if(GetRefCnt() == 0 && !CheckRefBitmap()) {
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(GetRefCnt() || CheckRefBitmap());/* Wait until deref */
                            assert(!GetRefCnt());
                            kb = GetKeyBlockAddr();
                            if(kb){
                                mf->DecreaseMemoryUsage(kb->GetSize());
                                reclaim_size-=kb->GetSize();
                                mf->FreeKB(kb);
                                ClearKeyBlockAddr();
                            }else{
                                if (ClearKBPrefetched()) {
                                    InSDBKey kbkey = GetKBKey(mf->GetDBHash(), kKBlock, ikey_seq_num_);
                                    (mf->GetEnv())->DiscardPrefetch(kbkey);
                                }
                            }
                            deletable = true;
                        }else
                            ClearEvicting();
                    }
                }
                return deletable;
            }
    };

    class TrxnInfo{
        private:
            Slice key_;
            uint8_t keyoffset_;
            uint8_t col_id_;
            Transaction head_;
            uint16_t nr_merged_trxn_;
            Transaction *merged_trxn_list_;
            TransactionID smallest_trxn_id_;
        public:
            TrxnInfo(): key_(), col_id_(0), head_(), nr_merged_trxn_(0), merged_trxn_list_(), smallest_trxn_id_(0){}
            ~TrxnInfo(){
                assert(!nr_merged_trxn_);
                assert(!merged_trxn_list_);
            }

            uint64_t InitMemory(uint16_t merged_trxn_cnt){ 
                if(merged_trxn_cnt){
                    /*
                     * merged_trxn_cnt includes header count(1). 
                     * So, nr_merged_trxn_ can be 0.
                     */
                    /*header space : header is counted in WorkerThread*/
                    nr_merged_trxn_ = merged_trxn_cnt-1;
                }else
                    nr_merged_trxn_ = 0;
                if(nr_merged_trxn_){
                    merged_trxn_list_ = new Transaction[nr_merged_trxn_];
                    return 1;
                }
                /*head_ is not set yet*/
                if(merged_trxn_cnt/*head_.id*/) return 1;
                return 0;
            }

            void InitHead(Slice &key, uint8_t col_id, Transaction &t){
                key_ = key;
                col_id_ = col_id;
                head_ = t;
            }
            void SetKeyOffset(uint8_t keyoffset) {
                keyoffset_ = keyoffset;
            }
            uint8_t GetKeyOffset() {
                return keyoffset_;
            }
            void InsertMergedTrxn(Transaction &trxn, uint16_t idx){
                assert(idx < nr_merged_trxn_);
                merged_trxn_list_[idx] = trxn;
            }
            void InsertSmallestTrxn(TransactionID tid){
                smallest_trxn_id_ = tid; 
            }
            Slice &GetKey() { return key_; }
            Transaction GetTrxn(){
                return head_;
            }
            TransactionID GetTrxnID(){
                return head_.id;
            }
            TrxnGroupID GetTrxnGroupID(){
                return (head_.id >> TGID_SHIFT);
            }
            uint8_t GetColId() {
                return col_id_;
            }
            uint16_t GetNrMergedTrxnList() {
                return nr_merged_trxn_;
            }
            Transaction *GetMergedTrxnList() {
                return merged_trxn_list_;
            }

            bool CleanupTrxnInfo(){
                bool ret = false;

                if(nr_merged_trxn_ || head_.id)
                    ret = true;
                // TODO: free key_ data memory
                key_ = Slice(0);
                col_id_ = 0;
                head_.id= 0;
                if(nr_merged_trxn_){
                    delete [] merged_trxn_list_;
                    nr_merged_trxn_ = 0;
                    merged_trxn_list_ = NULL;
                }
                assert(!nr_merged_trxn_);
                assert(!merged_trxn_list_);
                return ret;

            }

            uint32_t GetEstimatedBufferSize(RequestType type){
                uint32_t size = 0;
                if(nr_merged_trxn_ || head_.id){
                    if(type == kPutType){
                        /*
                         * For put command
                         * Format || key offset(1) || smallest TID(8) || head(16) || nr merged key cnt(2) || merged trxn info(n) ||
                         */
                        size += 27;/* For Trxn Header */
                    }else{
                        /*
                         * For delete command
                         * Format || key size(2) || key(key_.size()) ||  columnid(1) || smallest TID(8) || head(16) || nr merged key cnt(2) || merged trxn info(n) ||
                         */
                        size += 29 + key_.size();/* For Trxn Header */
                    }
                    size += sizeof(Transaction) * nr_merged_trxn_/* nr_merged_trxn_ can be 0 */;
                } else {
                    // If delete transaction is invalidated, only key length 0xffff will be written.
                    if(type == kDelType)
                        size += 2;
                }
                return size;
            }

            // set
            static const uint16_t InvalidDelTrxn = 0xffff;

            uint32_t BuildDeviceFormatKeyBlockMeta(char* kbm_buffer, RequestType type, uint8_t offset){
                char* buf = kbm_buffer;
                if(nr_merged_trxn_ || head_.id){
                    if(type == kPutType){
                        /*
                         * For put command
                         * Format || key offset(1) || smallest TID(8) || head(16) || nr merged key cnt(2) || merged trxn info(n) ||
                         */
                        *buf = offset;
                        buf++;
                    }else{
                        /*
                         * For delete command
                         * Format || key size(2) || key(key_.size()) ||  columnid(1) || smallest TID(8) || head(16) || nr merged key cnt(2) || merged trxn info(n) ||
                         */
                        EncodeFixed16(buf, key_.size());
                        buf += 2;
                        memcpy(buf, key_.data(), key_.size());
                        buf += key_.size();
                        *buf = col_id_;
                        buf++;

                    }
                    buf = EncodeVarint64(buf, smallest_trxn_id_);
                    buf = EncodeVarint32(buf, head_.count);
                    buf = EncodeVarint32(buf, head_.seq_number);
                    //assert(head_.id);
                    if(head_.id) buf = EncodeVarint64(buf, head_.id - smallest_trxn_id_);
                    else buf = EncodeVarint64(buf, 0); //save non-trxn because it is head
                    buf = EncodeVarint16(buf, nr_merged_trxn_);
                    //         printf("[%d :: %s]Trxn id :%ld(%u/%u)||\n", __LINE__, __func__, head_.id, head_.seq_number, head_.count);
                    assert(head_.id);
                    if(nr_merged_trxn_){
                        for(int i = 0 ; i < nr_merged_trxn_ ; i++){
                            buf = EncodeVarint32(buf, merged_trxn_list_[i].count);
                            buf = EncodeVarint32(buf, merged_trxn_list_[i].seq_number);
                            buf = EncodeVarint64(buf, merged_trxn_list_[i].id);
                            assert(merged_trxn_list_[i].id);
                        }
                    }
                } else {
                    // if delete transaction is invalidated, write key length 0xffff
                    if(type == kDelType) {
                        EncodeFixed16(buf, InvalidDelTrxn);
                        buf += 2;
                    }

                }
                return (uint32_t)(buf - kbm_buffer);
            }
            // Decode transaction info and create an object with it.
            static bool DecodeTransactionInfo(Slice &df, TrxnInfo* &trxninfo, Slice &key, uint8_t col_id = 0) {
                trxninfo = nullptr;
                uint16_t merged_trxn_cnt;
                Transaction trxn;

                // smallest transaction ID
                uint64_t smallest_trxn_id;
                bool success = GetVarint64(&df, &smallest_trxn_id);
                if(!success)
                    goto error_out;
                // head transaction count
                success = GetVarint32(&df, &trxn.count);
                if(!success)
                    goto error_out;
                // head transaction sequence
                success = GetVarint32(&df, &trxn.seq_number);
                if(!success)
                    goto error_out;
                // head transaction ID
                success = GetVarint64(&df, &trxn.id);
                if(!success)
                    goto error_out;
                trxn.id += smallest_trxn_id;
                // number of vertically merged transactions
                merged_trxn_cnt = DecodeFixed16(df.data());
                df.remove_prefix(sizeof(uint16_t));

                // create a transaction info
                trxninfo = new TrxnInfo();
                trxninfo->InitMemory(merged_trxn_cnt);
                trxninfo->InitHead(key, col_id, trxn);
                trxninfo->InsertSmallestTrxn(smallest_trxn_id);

                for(int i=0; i < merged_trxn_cnt; i++) {
                    // merged transaction count
                    success = GetVarint32(&df, &trxn.count);
                    if(!success)
                        goto error_out;
                    // merged transaction sequence
                    success = GetVarint32(&df, &trxn.seq_number);
                    if(!success)
                        goto error_out;
                    // merged transaction ID
                    success = GetVarint64(&df, &trxn.id);
                    if(!success)
                        goto error_out;
                    // add to the transaction info
                    trxninfo->InsertMergedTrxn(trxn, i);
                }
                return true;
error_out:
                if(trxninfo)
                    delete trxninfo;
                return false;
            }

            static bool DecodePutTransactionList(Slice &df, std::list<TrxnInfo*> &trxn_list) {
                // number of put transaction
                uint8_t nr_puts = df.data()[0];
                df.remove_prefix(sizeof(uint8_t));

                for(int i = 0; i < nr_puts ; i++) {
                    // key offset
                    uint8_t keyoffset = df.data()[0];
                    df.remove_prefix(sizeof(uint8_t));

                    // transaction info
                    TrxnInfo *trxninfo;
                    Slice empty_key;
                    bool success = DecodeTransactionInfo(df, trxninfo, empty_key);
                    if(!success)
                        goto error_out;
                    trxninfo->SetKeyOffset(keyoffset);
                    trxn_list.push_back(trxninfo);
                }
                return true;

error_out:
                for(auto it : trxn_list)
                    delete it;

                return false;
            }

            static bool DecodeDelTransactionList(Slice &df, std::list<TrxnInfo*> &trxn_list) {
                // number of delete transaction
                uint8_t nr_deletes = df.data()[0];
                df.remove_prefix(sizeof(uint8_t));

                for(int i = 0; i < nr_deletes ; i++) {
                    // key size
                    uint16_t key_size = DecodeFixed16(df.data());
                    df.remove_prefix(sizeof(uint16_t));
                    if(key_size == InvalidDelTrxn)
                        continue;
                    //
                    // key name
                    //
                    // note that the device format buffer is pointed for the key name buffer.
                    // key name is invalid once the device format buffer is invalid.
                    Slice key(df.data(), key_size);
                    df.remove_prefix(key_size);
                    // column ID
                    uint8_t col_id = df.data()[0];
                    df.remove_prefix(sizeof(uint8_t));

                    // transaction info
                    TrxnInfo *trxninfo;
                    bool success = DecodeTransactionInfo(df, trxninfo, key, col_id);
                    if(!success)
                        goto error_out;
                    trxn_list.push_back(trxninfo);
                }
                return true;

error_out:
                for(auto it : trxn_list)
                    delete it;

                return false;
            }

    };

    class KeyBlockMetaLog : public KeyBlockMeta { //32Byte. It is only temporary used.
        private:
            /* next_ikey_seq_num_ is only vaild when the KeyBlock is in the Log */  
            uint64_t next_ikey_seq_num_;                                            //8 Byte
            /* nrs : Number of keys (N)ot (R)ecored in the (S)KTable
             * Initially it has "(del count) + (put count)"
             * Decrease if the trxn is completed(CleanupTrxnInfo()).
             */
            /* KeyBlockPrimaryMeta address & NRS(# of keys that are Not Recored in SKTable) */
            std::atomic<uint8_t> nrs_key_cnt_;                      //1 Byte
            KeyBlockPrimaryMeta* kbpm_;                             //8 Byte
            /* Delete command & Trxn list & put command Trxn list */
            TrxnInfo* trxn_info_[kNrRequestType]; //8 Byte
            volatile uint8_t org_trxn_cnt_[kNrRequestType];//1 Byte
            std::atomic<uint8_t> uncompleted_trxn_cnt_[kNrRequestType];//1 Byte
        public:
            KeyBlockMetaLog(Manifest *mf): 
                next_ikey_seq_num_(0), 
                nrs_key_cnt_(0), 
                kbpm_(NULL),
                trxn_info_{NULL,NULL}, 
                org_trxn_cnt_{0,0} 
            //uncompleted_trxn_cnt_{0,0},
            {
                uncompleted_trxn_cnt_[kPutType].store(0,std::memory_order_relaxed);
                uncompleted_trxn_cnt_[kDelType].store(0,std::memory_order_relaxed);
            }
            ~KeyBlockMetaLog(){ }

            uint8_t GetNRSCount(){//Number of keys not recorded in table
                return nrs_key_cnt_.load(std::memory_order_seq_cst);
            }
            uint8_t DecNRSCount(){//Number of keys not recorded in table
                uint8_t nrs_cnt = nrs_key_cnt_.fetch_sub(1, std::memory_order_seq_cst);
                assert(nrs_cnt);
                return  nrs_cnt - 1;//return new count
            }
            TrxnInfo* GetTrxnInfo(RequestType type){
                return trxn_info_[type];
            }
            uint8_t GetTrxnInfoCount(RequestType type){
                return org_trxn_cnt_[type];
            }
            void DecValidTrxnCount(RequestType type){
                assert(uncompleted_trxn_cnt_[type].load(std::memory_order_relaxed));
                uncompleted_trxn_cnt_[type].fetch_sub(1, std::memory_order_seq_cst);
            }
            uint16_t GetValidTrxnCount(RequestType type){
                return uncompleted_trxn_cnt_[type].load(std::memory_order_seq_cst);
            }
            uint64_t GetNextIKeySeqNum() { return next_ikey_seq_num_; }
            void Init(uint64_t next_ikey_seq_num, KeyBlockPrimaryMeta* primary, uint8_t *count, uint16_t merge_cnt[][kMaxRequestPerKeyBlock]) {
                next_ikey_seq_num_ = next_ikey_seq_num;
                nrs_key_cnt_.store(count[kPutType] + count[kDelType], std::memory_order_relaxed);
                assert(nrs_key_cnt_.load(std::memory_order_relaxed));
                assert(nrs_key_cnt_.load(std::memory_order_relaxed) < (kMaxRequestPerKeyBlock * 2)/*Put & Delete*/);
                assert(primary);
                kbpm_ = primary;
                TrxnInfo *cmd_list;
                for(int type = 0 ; type < kNrRequestType ;  type++){
                    org_trxn_cnt_[type] = count[type];
                    uncompleted_trxn_cnt_[type] = 0;
                    trxn_info_[type] = NULL;
                    /* Get total trxn count */
                    assert( count[type] <= kMaxRequestPerKeyBlock );
                    if(count[type]){
                        cmd_list = new TrxnInfo[count[type]];/* Use Memory-pool : the # of entries in an array is kMaxRequestPerKeyBlock(32) */
                        for(int idx = 0 ; idx < count[type] ; idx++)
                            /* Get valid trxn count */
                            uncompleted_trxn_cnt_[type].fetch_add(cmd_list[idx].InitMemory(merge_cnt[type][idx]), std::memory_order_relaxed);
                        trxn_info_[type] = cmd_list;
                    }
                }
            }
            KeyBlockMetaType GetKeyBlockMetaType(){ return kKeyBlockMetaLog; }

            void SetHeadTrxn(Slice key, uint8_t col_id, RequestType t, uint8_t idx, Transaction &trxn ){
                assert(IsInlog());
#if 0
                if(trxn.id)
                    uncompleted_trxn_cnt_[t].fetch_add(1, std::memory_order_relaxed);
#endif
                (GetTrxnInfo(t) + idx)->InitHead(key, col_id, trxn);
            }
            void InsertMergedTrxn(uint8_t idx, RequestType t, Transaction &trxn, uint16_t trxn_idx/* in Vertical merged */){
                assert(IsInlog());
                (GetTrxnInfo(t) + idx)->InsertMergedTrxn(trxn, trxn_idx);
            }
            void InsertSmallestTrxn(uint8_t idx, RequestType t, TransactionID tid){
                assert(IsInlog());
                (GetTrxnInfo(t) + idx)->InsertSmallestTrxn(tid);
            }

            TrxnGroupID GetTrxnGroupID(RequestType t, uint8_t idx){
                assert(!IsDummy());
                return (GetTrxnInfo(t) + idx)->GetTrxnGroupID();
            }
            uint32_t GetEstimatedBufferSize(){
                uint32_t size = 10;/* next_ikey_seq_num(8). put & del cmd count. 1 byte for each*/
                uint8_t count;
                for(uint8_t type = 0 ; type < kNrRequestType ;  type++){
                    count = GetTrxnInfoCount((RequestType)type);
                    TrxnInfo* t_info = GetTrxnInfo((RequestType)type);
                    for(int i = 0 ; i < count; i++){
                        size+=t_info[i].GetEstimatedBufferSize((RequestType)type);
                    }
                }
                size += kbpm_->GetEstimatedBufferSizeInternal();
                return size;
            }

            uint32_t BuildDeviceFormatKeyBlockMeta(char* kbm_buffer){
                char* buf = kbm_buffer;
                uint32_t size = 0;
                uint8_t count;
                /* pass ikey_seq_num to get the ikey from KeyBlockPrimaryMeta */
                size = kbpm_->BuildDeviceFormatKeyBlockMetaInternal(buf);
                for(uint8_t type = 0 ; type < kNrRequestType ;  type++){
                    count = GetTrxnInfoCount((RequestType)type);
                    *(buf+size++) = count;
                    TrxnInfo* t_info = GetTrxnInfo((RequestType)type);
                    for(int i = 0 ; i < count; i++){
                        if(t_info[i].GetEstimatedBufferSize((RequestType)type))
                            size += t_info[i].BuildDeviceFormatKeyBlockMeta(buf+size, (RequestType)type, i);
                    }
                }
                EncodeFixed64(buf+size, next_ikey_seq_num_);
                size += sizeof(uint64_t);
                return size;
            }

            SequenceNumber GetiKeySequenceNumber(){ return kbpm_->GetiKeySequenceNumber(); }

            bool SetWriteProtection() { return kbpm_->SetWriteProtection(); }
            void ClearWriteProtection() { kbpm_->ClearWriteProtection(); }
            bool SetSubmitInProgress() { return kbpm_->SetSubmitInProgress();}
            void ClearSubmitInProgress() {  kbpm_->ClearSubmitInProgress();}

            bool IsInlog() { return kbpm_->IsInlog(); }

            void SetKeyBlockBitmap(uint8_t bit_index){
                assert(!IsDummy());
                kbpm_->SetKeyBlockBitmap(bit_index);
            }
            /* Return true if there is valid or uncommitted TRXN exists */

            bool ClearKeyBlockBitmap(uint8_t bit_index, RequestType t){ 
                assert(!IsDummy());
                TrxnInfo* t_info = GetTrxnInfo(t);
                if(t_info[bit_index].CleanupTrxnInfo())
                    DecValidTrxnCount(t);
                if(t == kPutType)
                    kbpm_->ClearKeyBlockBitmap(bit_index, t);

                return kbpm_->GetKeyBlockBitmap() || GetValidTrxnCount(kPutType) || GetValidTrxnCount(kDelType);
            }

            void UpdateTrxnInfo(uint8_t bit_index, RequestType t){ 
                assert(!IsDummy());
                TrxnInfo* t_info = GetTrxnInfo(t);
                if(t_info[bit_index].CleanupTrxnInfo())
                    DecValidTrxnCount(t);
            }

            uint32_t GetKeyBlockBitmap(){ 
                assert(!IsDummy());
                return kbpm_->GetKeyBlockBitmap();
            }
            bool TestKeyBlockBitmap(){
                assert(!IsDummy());
                return (kbpm_->GetKeyBlockBitmap() || GetValidTrxnCount(kPutType) || GetValidTrxnCount(kDelType));
            }

            KeyBlockPrimaryMeta* GetKBPM() {
                return kbpm_;
            }

            KeyBlock* GetKeyBlock() {
                if (kbpm_) return kbpm_->GetKeyBlock();
                return NULL;
            }
            void SetWorkerBit(uint8_t worker_id){
                kbpm_->SetWorkerBit(worker_id);
            }
            void ClearWorkerBit(){
                kbpm_->ClearWorkerBit();
            }
            bool TestWorkerBit(uint8_t worker_id){
                return kbpm_->TestWorkerBit(worker_id);
            }

            uint8_t GetOriginalKeyCount() { return kbpm_->GetOriginalKeyCount(); }


            bool IsDummy() { return kbpm_->IsDummy(); }
            Status Prefetch(Manifest *mf) { return kbpm_->Prefetch(mf); }
            uint16_t IncRefCnt() { return  kbpm_->IncRefCnt();}
            uint16_t DecRefCnt() { return  kbpm_->DecRefCnt();}
            uint16_t GetRefCnt() { return  kbpm_->GetRefCnt();}

            void CleanUp() {
                for(uint8_t type = 0 ; type < kNrRequestType ;  type++){
                    if (trxn_info_[type]) delete [] trxn_info_[type];
                    trxn_info_[type] = NULL;
                    org_trxn_cnt_[type] = 0;
                    assert(!uncompleted_trxn_cnt_[type].load(std::memory_order_relaxed));
                }
                assert(!GetNRSCount());
                next_ikey_seq_num_ = 0;
                kbpm_ = NULL;
            }
    };

    /*
     * User Address Space in Linux System is 0x0 ~ 0x0000 7FFF FFFF FFFF
     */
#define ITER_KEY_BUFFER_FLAG        0x80000000 
#define ITER_KEY_BUFFER_OFFSET_MASK 0x7FFFFFFF
    class UserKey : public SKLData<KeySlice, UserKeyComparator> { /* 32 Byte */
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
            class ColumnData{ // 40 Byte( Valid 38 Byte)
                public:
                    friend class UserKey;
                    ColumnData()
                        : latest_col_node_(NULL), col_list_(), req_node_(NULL), last_tgid_(0), col_list_lock_(){}

                    void ColumnListLock(){ col_list_lock_.Lock(); }
                    void ColumnListUnlock(){ col_list_lock_.Unlock(); }

                    ColumnNode* GetLatestColumnNode(){ return latest_col_node_; }
                    /* It returns the last TGID and save current TGDI */
                    TrxnGroupID SetLastTGID(TrxnGroupID tgid){ TrxnGroupID id = last_tgid_; last_tgid_ = tgid; return id; }
                    void SetRequestNode(RequestNode* req_node){ req_node_ = req_node; }
                    RequestNode* GetRequestNode(){ return req_node_; }
                    void SetLatestColumnNode(ColumnNode* new_col_node){ latest_col_node_ = new_col_node; }
                    bool col_list_empty() { return col_list_.empty(); }
                    bool col_list_size() { return col_list_.Size(); }
                    SimpleLinkedNode* col_list_pop() { return col_list_.Pop(); }
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
                    RequestNode* req_node_;         // 8 Byte
                    /* To protect ColumnNode Modification such as get user value */
                    TrxnGroupID last_tgid_;         // 4 Byte
                    /* To protect list only */
                    Spinlock col_list_lock_;        // 1Byte
#if 0
                    uint32_t max_merge_node_cnt_;
                    uint32_t merge_node_cnt_;
#endif

            };
        private:
            /*
             * In the linux system, User Space only uses 6 Byte & upper 2 Byte is always 0. 
             * Therefore, we are going to use upper 2 byte for other purpose.
             */
#define REF_CNT_BITMASK         0x3FF
#define KEYSIZE_BITMASK         0xFFFF
            /* 
             * std::atomic<uint32_t> inmem_size_;
             * we don't need inmem_size_ to keep the size of memory used by a Key
             * this is mainly used when begin key is changed.
             * But we have col_list_ which have number of ColumnNode or we can add actual size of ColumnNode.
             * and we can check all the columns.
             */
            //void* internal_node_;
            //std::atomic<uint16_t> ref_cnt_;//2byte
            /*
             * Flags are integrated into "ref. cnt" & "internal node ptr" variables
             * first 9 bit is for ref. cnt(~511)
             * remained 7 bit for flags
             * 0x 200 : flags_fetched
             * 0x 400 : flags_referenced
             * 0x 800 : flags_evict_prepare
             * 0x4000 : flags_userkey_dirty
             * 0x8000 : flags_survive
             */
            /* 8Byte = Internal Node pointer(6B) + Flags(7 bit) + reference count(9 bit : 511)*/
            port::AtomicPointer internal_node_;             // 8 Byte
            std::atomic<uint8_t> flags_;                             //8byte
            std::atomic<uint16_t> ref_cnt_;                             //8byte
            char* key_buffer_;
            uint16_t key_buffer_size_;
            KeySlice user_key_;                                                         //8byte

            /* 
             * Make a structure for (col_date_ ~ lock_) to secure KeySlice Space within 32 byte UserKey 
             * If we use "user_key_and_keysize_" instead of "KeySlice", we have to allocate & copy 16 byte KeySlice for GetKey()
             * for every accesses.
             *
             * But the user will get the pointer of "col_data & skt" before use it. and than accessing the object using the pointer
             */
            ColumnData *col_data_;                                                                  // 8byte

            /* 
             * lock for loading columns from sktable device format
             *  - single key lookup
             *  - loadsktable
             *  - key loading in flush sktable
             */
            Spinlock lock_;        
            ////port::Mutex lock_;        

            port::Mutex col_lock_;             // 40 Byte

            ObjectAllocator<UserKey>* allocator_;

        public:
            /* For Free Key Registration */
            UserKey(uint16_t key_size, ObjectAllocator<UserKey>* allocator) :
                internal_node_(NULL), // Increase reference count for new key
                flags_(0), // Increase reference count for new key
                ref_cnt_(0), // Increase reference count for new key
                key_buffer_(NULL),
                key_buffer_size_(0),
                user_key_(),
                col_data_(NULL), 
                lock_(),
                col_lock_(),
                allocator_(allocator)
        {
            col_data_= new ColumnData[kMaxColumnCount];
            key_buffer_ = (char*)malloc(key_size+1);
            key_buffer_size_ = key_size;
        }





            UserKey(uint32_t key_size, uint32_t size_2) :
                internal_node_(NULL), // Increase reference count for new key
                flags_(0), // Increase reference count for new key
                ref_cnt_(0), // Increase reference count for new key
                key_buffer_(NULL),
                key_buffer_size_(0),
                user_key_(),
                col_data_(NULL), 
                lock_(),
                col_lock_(),
                allocator_(NULL)
        {
            col_data_= new ColumnData[kMaxColumnCount];
            key_buffer_ = (char*)malloc(key_size+1);
            key_buffer_size_ = (uint16_t)key_size;
        }


            ~UserKey(){
                assert(!IsDirty());
                /* UserKey::Cleanup() must be executed before calling UserKey Destructor */
                assert(col_data_);
                assert(key_buffer_);
                delete [] col_data_;
                free(key_buffer_);
            }



            void InitUserKey(const KeySlice& user_key);
            bool UserKeyCleanup(Manifest *mf, bool cache_evict = false);
            void FillIterScratchPad(Manifest *mf, IterScratchPad* iter_pad, char*& key_buffer_loc, const KeySlice* key, ColumnNode* col_node, uint8_t col_id);
            ColumnNode* GetColumnNodeContainerOf(SimpleLinkedNode *ikey_node); 

            void UserKeyLock(){ lock_.Lock(); }
            void UserKeyUnlock(){ lock_.Unlock(); }

            void ColumnLock(uint8_t col_id){ col_lock_.Lock(); }
            bool ColumnTryLock(uint8_t col_id) { return col_lock_.TryLock(); }
            void ColumnUnlock(uint8_t col_id){ col_lock_.Unlock(); }

            void ColumnListLock(uint8_t col_id){ col_data_[col_id].ColumnListLock(); }
            void ColumnListUnlock(uint8_t col_id){ col_data_[col_id].ColumnListUnlock(); }

            /* It returns the last TGID and save current TGDI */
            TrxnGroupID SetLastTGID(uint8_t col_id, TrxnGroupID tgid){ return col_data_[col_id].SetLastTGID(tgid); }
            void SetRequestNode(uint8_t col_id, RequestNode* req_node){ col_data_[col_id].SetRequestNode(req_node); }
            RequestNode* GetRequestNode(uint8_t col_id){ return col_data_[col_id].GetRequestNode(); }
            void SetLatestColumnNode(uint8_t col_id, ColumnNode* new_col_node){ col_data_[col_id].SetLatestColumnNode(new_col_node); }
            ColumnNode* GetLatestColumnNode(uint8_t col_id){ return col_data_[col_id].GetLatestColumnNode(); }
            // SKLData Skiplist interface
            void SetNode(void* node) { internal_node_.Release_Store(node); }
            void* GetNode() { return internal_node_.Acquire_Load(); }
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

            UserKey* NextUserKey(Manifest *mf);
            int Compare(const Slice& key, Manifest *mf);
            bool IsColumnEmpty(uint16_t col_id) { return col_data_[col_id].col_list_empty(); }

            SimpleLinkedList* GetColumnList(uint8_t col_id) 
            {
                return col_data_[col_id].GetColumnList();
            }
            ColumnNode* GetColumnNode(uint16_t col_id, bool& deleted, uint64_t cur_time = -1, SequenceNumber seq_num = -1, uint64_t ttl = 0);
            void DoVerticalMerge(RequestNode *req_node, uint16_t col_id, uint8_t worker_id, uint8_t idx, Manifest *mf, KeyBlockMetaLog* kbml);
            InsertColumnNodeStatus InsertColumnNode(ColumnNode* new_ik, uint16_t col_id);
            void InsertColumnNodes(Slice &col_info, bool discard_seq);
            InsertColumnNodeStatus UpdateColumn(ColumnNode* new_ik, uint16_t col_id, bool force, Manifest *mf);
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
            // UserKey flags
            const static uint8_t flags_fetched         = 0x01;
            const static uint8_t flags_referenced      = 0x02;
            const static uint8_t flags_evict_prepare   = 0x04;
            const static uint8_t flags_userkey_dirty   = 0x08;

            bool IsFetched() { return (flags_.load(std::memory_order_seq_cst) & flags_fetched); }
            void SetFetched(){ flags_.fetch_or(flags_fetched, std::memory_order_seq_cst); }

            bool ClearReferenced(){ return flags_.fetch_and(~flags_referenced, std::memory_order_relaxed) & flags_referenced; }
            void SetReferenced(){ flags_.fetch_or(flags_referenced, std::memory_order_relaxed); }

            void SetEvictionPrepare() { flags_.fetch_or(flags_evict_prepare, std::memory_order_seq_cst); }
            void ClearEvictionPrepare() { flags_.fetch_and(~flags_evict_prepare, std::memory_order_seq_cst); }
            bool IsEvictionPrepared() { return (flags_.load(std::memory_order_seq_cst) & flags_evict_prepare); }

            /**
             * In FlushSKTableMem, if a UserKey is seleted to delete, set try_delete_ and then check ref_cnt_
             * try_delete_ prevents to use this key.
             */

            bool SetDirty() { return (flags_.fetch_or(flags_userkey_dirty, std::memory_order_seq_cst)& flags_userkey_dirty); }
            bool ClearDirty() { return (flags_.fetch_and(~flags_userkey_dirty, std::memory_order_seq_cst) & flags_userkey_dirty); }
            bool IsDirty() { return (flags_.load(std::memory_order_seq_cst) & flags_userkey_dirty); }

            uint16_t GetReferenceCount(){ return (uint16_t)(ref_cnt_.load(std::memory_order_seq_cst)); }
            void IncreaseReferenceCount()
            { 
                uint16_t old = ref_cnt_.fetch_add(1, std::memory_order_relaxed);
#ifdef REFCNT_SANITY_CHECK
                if (old+1==0) abort();
#endif
            }
            uint16_t DecreaseReferenceCount()
            {
                uint16_t ref_cnt = (ref_cnt_.fetch_sub(1, std::memory_order_seq_cst));
#ifdef REFCNT_SANITY_CHECK
                if (ref_cnt==0) abort();
#endif
                return ref_cnt; 
            }


            /* 
             * In key remove/key insert function, The caller always knows whether the key is begin key or not.
             * return UserKey : No eviction in pregress
             * return NULL    : eviction in progress
             */
            UserKey* IsSKTableEvictionInProgress() {
                // If eviction is prepared, try to cancel the eviction

                if(IsEvictionPrepared()){  
                    DecreaseReferenceCount();
                    return NULL;
                }
                return this;
            }

            size_t GetSize(); 
            void SetAllocator(ObjectAllocator<UserKey>* allocator){ allocator_ = allocator; }
            ObjectAllocator<UserKey>* GetAllocator(){ return allocator_; }

        private:
            // No copying allowed
            UserKey(const UserKey&);
            void operator=(const UserKey&);
    }/*UserKey */;


    class ColumnNode{// 64 Byte
#define USERVALUE_FLAG 0x8000000000000000UL
        private:
            friend class UserKey;

            /*
             * Once submitted, trxn_ will no longer be used. it is just for passing the trxn to worker thread.
             * In FlushSKTableMem, we can get the trxn from KBML.
             * So, we'll define a struct RequestNode for node inserted to pending queue.
             * and the struct RequestNode will have the trxn_.
             * the struct RequestNode will be allocated from memory-pool.
             * We'll create multiple memory-pool( = # of WorkerThread).
             * And the memory pool is used in a round-robin fashion in order to reduce lock contention.
             * Transaction trxn_;  
             */

            /*  
             *  Variable             
             *  uvalue_or_kbm_ :      0x[Flag][    Address   ]
             *  UserValue      :      0x[8000][0000 0000 0000]
             *  KeyBlockMeta   :      0x[0000][0000 0000 0000]
             *                          2Byte      6Byte
             */
            uint64_t uvalue_or_kbm_ ;                                       //8byte
            uint64_t ttl_;                                                  //8byte
            /*  
             *  **offset is used for position in the bitmap or delete command  in Key Block Meta & Key Block
             *
             *  Variable             
             *  kb_offset_and_size_ :      0x[  00  ][00 0000]
             *                             0x[Offset][KB Size]
             *                                1Byte   3Byte
             */
#define COL_KB_SIZE_MASK    (0x00FFFFFF)
#define COL_KB_OFFSET_SHIFT (24) /*Shift 3 byte*/
            uint32_t kb_offset_and_size_;                                   //4byte
            /* In new design(Packing), the next_ikey_seq_  is no longer needed.
             * It is used for deciding whether the key can be applied to SKTable or not.
             * It was important because the log in the SKTable will be updated based on this number.
             * If the key is written to the SKTable, it means the key does not exist in the log anymore.
             * But more recent key can exist before the written key.
             * In this case, the recent key can be lose.
             *
             * However, in new design, if a log is not written yet, we do not move the position in the log at a WorkerThread.
             * That means that a log can keep the key which has been written to the SKTable if previously inserted key is not cleared yet.
             * uint64_t next_ikey_seq_;
             */
            /* Raw Key Block Size include padding(max 21 bit : 2MB) */

            SimpleLinkedNode col_list_node_;                                //16byte
            SequenceNumber begin_sequence_number_;                          //8byte
            SequenceNumber end_sequence_number_;                            //8byte
            /*Set the ikey_seq_ with ColumnLock*/
            volatile uint64_t ikey_seq_;                                             //8byte
            volatile uint8_t flags_;                                       //1byte 
        public:

            /* This bit is used to prevent multiple threads from calling InvalidateUserValue() at the same time.
             * But this must not happen.(Protected by mechanism).
             * So, if it happens again, we'are going to figure out what the problem is.
             * const static uint16_t flags_invalidation_in_progress    = 0x0004;
             */
            /* 
             * This is for Trxn. If the ikey has flags_submitted, it can be candidate for committed TRXN 
             * However, we don't need to do this because we changed log management mechanism.
             * const static uint16_t flags_submitted                   = 0x0010;
             */
            /* 
             * This is for deletion during vertical merge.
             * However, if crash occures right after deleting the node in vertical merge, we can not recover the original status.
             * So, we do not delete the previously submitted ikey during vertinal merge.
             * const static uint16_t flags_deleted                     = 0x0020;
             */


            /*Non-atomic*/
            const static uint8_t flags_del_type                        = 0x01; // Set it olny when new ColumnNode, 1 for del type, 0 for put type
            /* Indicates whether it is trxn or not(:Used before cache insertion for Write) */
            const static uint8_t flags_trxn                            = 0x02; /*Access with ColumnLock*/
            const static uint8_t flags_dropped                         = 0x04; /*Access with ColumnLock*/
            const static uint8_t flags_holding_iter                    = 0x08; // ture : the column node has been updated or deleted. but iter is holding this col. 
            const static uint8_t flags_recent_col_node_deleted         = 0x10; // Used in FlushSKTableMem to check whether the next key has been deleted or not
            const static uint8_t flags_trxn_committed                  = 0x20; // Set it under lock
            /* Split Vertical merge because of different Trxn Group ID(:Used before cache insertion for Write) */
            const static uint8_t flags_tgid_split                      = 0x40; // Set it under lock


            /*For Recovery */
            ColumnNode(RequestType type/* Can be kDelType from Recovery*/, uint64_t ikey_seq, uint64_t ttl, uint32_t ivalue_size, uint8_t offset) :
                uvalue_or_kbm_(0), ttl_(ttl), kb_offset_and_size_(((offset <<COL_KB_OFFSET_SHIFT) | (ivalue_size) )), 
                col_list_node_(), begin_sequence_number_(0), end_sequence_number_(ULONG_MAX),
                ikey_seq_(ikey_seq), flags_(type|flags_trxn_committed) {
                    assert(ivalue_size <= COL_KB_SIZE_MASK);
                }


            /*Fetch a column from SKTDF */
            ColumnNode(SequenceNumber seq, uint64_t ttl, uint64_t ikey_seq, uint32_t ivalue_size, uint8_t offset) :
                uvalue_or_kbm_(0), ttl_(ttl), kb_offset_and_size_(((offset <<COL_KB_OFFSET_SHIFT) | (ivalue_size) )), 
                col_list_node_(), begin_sequence_number_(seq), end_sequence_number_(ULONG_MAX),
                ikey_seq_(ikey_seq), flags_(kPutType/*0*/|flags_trxn_committed) {
                    assert(ivalue_size <= COL_KB_SIZE_MASK);
                }

            /* Newely inserted column(from Store()) */
            ColumnNode(RequestType type, uint64_t ttl ) :
                uvalue_or_kbm_(0), ttl_(ttl), kb_offset_and_size_(0), 
                col_list_node_(), begin_sequence_number_(0), end_sequence_number_(ULONG_MAX),
                ikey_seq_(0), flags_(type) {
                }

            ~ColumnNode(){
                assert(!GetUserValue());
                /* The UserValue will be freed by a value eviction thread */
            }

            void UpdateSequenceNumber(SequenceNumber seq){
                begin_sequence_number_ = seq;
                end_sequence_number_ = ULONG_MAX;
            }

            /*
             * TGID Split (TRXN Group ID based Split)
             * The ColumnNodes that has different TGID can not be Vertically Merged even if they are all unsubmitted.
             * Call these functions under column lock
             */
            void SetTGIDSplit(){ flags_ |= flags_tgid_split; }
            void ClearTGIDSplit(){ flags_ &= ~flags_tgid_split; }
            bool IsTGIDSplit(){ return (flags_ & flags_tgid_split); }

            /*
             * Call these functions under column lock
             */
            void SetTransaction(){ flags_ |= flags_trxn; }
            /* Return whether it is trxn or not */
            bool IsTransaction(){ return (flags_ & flags_trxn); }
            void ClearTransaction(){ flags_ &= ~flags_trxn; }

            void DropColumnNode(){ flags_ |= flags_dropped; }
            bool IsDropped(){ return (flags_ & flags_dropped); }

            /* Only update can be KBML(if it is in the log). 
             * Call these functions under column lock in DeviceFormatBuilder()
             */
            bool IsHoldingIter(){ return flags_ & flags_holding_iter; }
            void SetHoldingIter(){ flags_ |= flags_holding_iter; }
            void ClearHoldingIter(){ flags_ &= ~flags_holding_iter; }

            /*
             * Call these functions under column lock in DeviceFormatBuilder()
             */
            bool GetRecentColNodeyDeleted() { return flags_ & flags_recent_col_node_deleted;  }
            void SetRecentColNodeyDeleted() { flags_ |= flags_recent_col_node_deleted; }

            RequestType GetRequestType() { return  (RequestType)(flags_ & flags_del_type); }

            void SetTRXNCommitted(){ flags_ |= flags_trxn_committed; }
            bool IsTRXNCommitted(){ return flags_ & flags_trxn_committed; }

            void RemoveUserValue(){ 
                if(!(USERVALUE_FLAG & uvalue_or_kbm_)) abort();
                uvalue_or_kbm_ = 0; }

            void InsertUserValue(UserValue* uv) {
                assert(!uvalue_or_kbm_);
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

            void SetKeyBlockMeta(KeyBlockMeta* kbm){
                assert((uint64_t)kbm == uvalue_or_kbm_ || !uvalue_or_kbm_);
                if (!kbm) abort();
                uvalue_or_kbm_ =  ((uint64_t)kbm); 
            }
            bool HasUserValue(){
                return (uvalue_or_kbm_ & USERVALUE_FLAG);
            }
            KeyBlockMeta* GetKeyBlockMeta(){
                if(uvalue_or_kbm_ & USERVALUE_FLAG) return nullptr;
                return (KeyBlockMeta*)(uvalue_or_kbm_); 
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
            void SetInSDBKeySeqNum(uint64_t ikey_seq) { ikey_seq_ = ikey_seq; }
            uint64_t GetInSDBKeySeqNum() { return ikey_seq_; }
            /*
             * It will be relocated to KBML
             * Transaction GetTrxnInfo() { return trxn_; }
             */

            void SetBeginSequenceNumber(SequenceNumber seq) { begin_sequence_number_ = seq; }
            SequenceNumber  GetBeginSequenceNumber() { return begin_sequence_number_; }
            void SetEndSequenceNumber(SequenceNumber seq) { end_sequence_number_ = seq; }
            SequenceNumber  GetEndSequenceNumber() { return end_sequence_number_; }

            /* Time To Live */
            uint64_t GetTS() { return ttl_; }
            /*
             * SKTable keeps largest iValue Size in the its range.
             *
             * inline void UpdateiValueSize(uint32_t ivalue_size) { ivalue_size_ = ivalue_size; }
             * uint32_t GetiValueSize() { return (ivalue_size_and_flags_and_offset_.load(memory_order_seq_cst) >> IVALUE_SIZE_SHIFT); }
             */
            uint32_t GetiValueSize() { return (uint32_t)(kb_offset_and_size_ & COL_KB_SIZE_MASK); }

            uint8_t GetKeyBlockOffset(){ return (uint8_t)(kb_offset_and_size_ >> COL_KB_OFFSET_SHIFT); }
            void SetKeyBlockOffset(uint8_t offset){ 
                assert(offset < kMaxRequestPerKeyBlock);
                assert(!kb_offset_and_size_);
                kb_offset_and_size_|= (offset<<COL_KB_OFFSET_SHIFT); 
            }

            void UpdateColumnNode(Manifest *mf, uint32_t kb_size, uint64_t ikey_seq, KeyBlockMeta *kbm){

                assert(!GetiValueSize());
                assert(kb_size < kDeviceRquestMaxSize);
                assert(!(kb_offset_and_size_ & COL_KB_SIZE_MASK));

                UserValue* uv = GetUserValue();
                assert(uv || GetRequestType() == kDelType);
                /* Delete Command did not create a UserValue */
                /* Offest was already set in BuildKeyBlock(Meta)()*/
                kb_offset_and_size_ |= kb_size ; 
                uvalue_or_kbm_ =  ((uint64_t)kbm); 
                ikey_seq_ = ikey_seq;
                /* Delete Command did not create a UserValue */
                if(uv) mf->FreeUserValue(uv);
            }

        private:
            // No copying allowed
            ColumnNode(const ColumnNode&);
            void operator=(const ColumnNode&);
    }/*ColumnNode*/;
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

            UserValue(uint32_t size, uint32_t size_2) : buf_(NULL), value_size_(0), buf_size_(0), pinned_(1), allocator_(NULL) { 
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
            Spinlock *iter_pad_lock_;
            std::unordered_map<uint32_t, IterScratchPad*> *iter_pad_map_;

        public:
            ~SnapshotImpl();

            SnapshotImpl(Manifest *mf, SequenceNumber seq, uint64_t cur_time) : 
                mf_(mf), sequence_(seq), cur_time_(cur_time), refcount_(0), snapshot_node_(), mu_(), iter_pad_lock_(), iter_pad_map_(){
                    iter_pad_lock_= new Spinlock[kMaxColumnCount];
                    iter_pad_map_ = new std::unordered_map<uint32_t, IterScratchPad*>[kMaxColumnCount];
                    iterators_.clear();
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
            IterScratchPad* GetIterScratchPad(uint32_t skt_id, uint8_t col_id){
                std::unordered_map<uint32_t, IterScratchPad*> *iter_pad_map = &iter_pad_map_[col_id];
                IterScratchPad* pad = NULL;
                IterScratchPad* check_pad = NULL;
                iter_pad_lock_[col_id].Lock();
#if 1
                std::unordered_map<uint32_t, IterScratchPad*>::iterator it = iter_pad_map->find(skt_id);
                if(it!=iter_pad_map->end()){ 
                    pad = it->second; 
                    pad->IncIterScratchPadRef();
                    pad->IncActiveIterScratchPadRef();
                }
#else
                std::queue<IterScratchPad*> remove_iter_pad;
                for (auto it = iter_pad_map->begin(); it != iter_pad_map->end();) {
                    if (it->first == skt_id) {
                        pad = it->second;
                        pad->IncIterScratchPadRef();
                        pad->IncActiveIterScratchPadRef();
#ifdef CODE_TRACE
                        printf("reuse  Iterpad for skt *%d) from snapshot\n", pad->skt_->GetSKTableID());
#endif
                        it++;
                    } else {
                        check_pad = it->second;
                        if (!check_pad->GetActiveIterScratchPadRef() &&
                            ((mf_->GetEnv()->NowSecond() - check_pad->GetLastAccessTimeSec()) >  MAX_ITER_SUSTAIN_SEC)) {
                            it = iter_pad_map->erase(it);
#ifdef CODE_TRACE
                            printf("take-out  Iterpad for skt *%d) from snapshot\n", check_pad->skt_->GetSKTableID());
#endif
                            if (check_pad->DecIterScratchPadRef()) {
                                remove_iter_pad.push(check_pad);
                            }
                        } else it++;
                    }
                }
#endif
                iter_pad_lock_[col_id].Unlock();
#if 0
                while(!remove_iter_pad.empty()) {
                    check_pad = remove_iter_pad.front();
                    remove_iter_pad.pop();
                    assert(check_pad);
#ifdef CODE_TRACE
                    printf("Remove Iterpad for skt *%d)\n", check_pad->skt_->GetSKTableID());
#endif
                    check_pad->ClearIterScratchpad(mf_);
                    mf_->FreeIterScratchPad(check_pad);
                }
#endif
                return pad;
            }
            void InsertIterScratchPad(IterScratchPad* pad, uint8_t col_id){
                std::unordered_map<uint32_t, IterScratchPad*> *iter_pad_map = &iter_pad_map_[col_id];
                uint32_t skt_id = pad->skt_->GetSKTableID();
                iter_pad_lock_[col_id].Lock();
                std::unordered_map<uint32_t, IterScratchPad*>::iterator it = iter_pad_map->find(skt_id);
                if(it==iter_pad_map->end()) {
#ifdef CODE_TRACE
                    printf("Insert Iterpad for skt *%d) to snapshot\n", skt_id);
#endif
                    /* Increase ref cnt for SnapshotImpl(not SnapshotIterator) */
                    pad->IncIterScratchPadRef();
                    pad->SetLastAccessTimeSec(mf_->GetEnv()->NowSecond());
                    iter_pad_map->insert({skt_id, pad});
                }
                iter_pad_lock_[col_id].Unlock();
            }

#if 1
            void ReleaseIterScratchPad(IterScratchPad* pad, uint8_t col_id){
                bool free = false;
                std::unordered_map<uint32_t, IterScratchPad*> *iter_pad_map = &iter_pad_map_[col_id];
                uint32_t skt_id = pad->skt_->GetSKTableID();
                iter_pad_lock_[col_id].Lock();
                std::unordered_map<uint32_t, IterScratchPad*>::iterator it = iter_pad_map->find(skt_id);
                if(it!=iter_pad_map->end()){ 
                    if (pad == it->second) {
                        iter_pad_map->erase(it);
                        free = pad->DecIterScratchPadRef();
                    }
                }
                iter_pad_lock_[col_id].Unlock();
                if(free){
                    pad->ClearIterScratchpad(mf_);
                    mf_->FreeIterScratchPad(pad);
                }
            }
#endif
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
    enum IterBufferType{
        kSKTDFBuffer = 0,
        kKeymapBuffer= 1,
        kIterBufferCount = 2,
    };

    class SnapshotIterator : public Iterator {
        public:
            SnapshotIterator(Manifest* mf,  SnapshotImpl *snap, uint16_t col_id = 0, Slice skey = 0, Slice lkey = 0) : 
                mf_(mf), 
                snap_(snap), 
                column_id_(col_id), 
                start_key_(skey.data(), 
                skey.size()), 
                last_key_(lkey.data(), lkey.size()), 
                comparator_(const_cast<Comparator*>(mf_->GetComparator())), 
                prefix_size_(0), 
                prefix_{}, 
                iter_pad_(NULL), 
                cur_info_(), 
                next_prefetch_skt_(NULL), 
                next_kb_prefetch_skt_(NULL), 
                prev_prefetch_skt_(NULL), 
                prev_kb_prefetch_skt_(NULL), 
                dir()
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
            void ReturnIterScratchPad(IterScratchPad*& iter_pad);
            Slice GetKey(KeyBufferEntry *cur_offset);
            void SetCurrentKeyInfo(const Slice key, IterScratchPad* iter_pad, bool search_less_than);
            SKTableMem* SingleSKTablePrefetch(SKTableMem *last_prefetched_skt, PrefetchDirection dir);
            SKTableMem* SKTablePrefetch(SKTableMem *cur_skt, PrefetchDirection dir);
            IterScratchPad *SeekInternal(const KeySlice* key, SKTableMem*& skt, bool search_less_than/*or last(true)/first(false) key*/);
            void KeyBlockPrefetch(SKTableMem* cur_skt, bool search_less_than);
            void SeekWrapper(SKTableMem* skt, KeySlice* key, bool search_less_than);
            void ResetPrefetch(void);
            void Move(uint8_t dir);
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

            /* prefix optimization */
            mutable uint16_t prefix_size_;
            mutable char prefix_[16];

            IterScratchPad *iter_pad_;
            IterCurrentKeyInfo cur_info_;
            /*Offset in IterScratchPad->key_offset_buffer_*/
            SKTableMem* next_prefetch_skt_;
            SKTableMem* next_kb_prefetch_skt_;
            SKTableMem* prev_prefetch_skt_;
            SKTableMem* prev_kb_prefetch_skt_;
            PrefetchDirection dir;

            //No copying allowed
            SnapshotIterator(const SnapshotIterator &);
            void operator=(const SnapshotIterator &);
    };
} // namespace insdb
#endif //STORAGE_INSDB_INTERNAL_H_
