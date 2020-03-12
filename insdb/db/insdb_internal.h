/*
 *
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
    void PinnableSlice_KeyBlock_CleanupFunction(void* arg1, void* arg2);

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

    class LRU_Cache;
    class KBPM_Cache;
    template <class T> class ObjectPoolShard;


#ifdef USE_HOTSCOTCH
    typedef tsl::hopscotch_map<KeySlice, UserKey*, UKHash, UKEqual> HotscotchKeyHashMap;
#endif
#ifdef INSDB_USE_HASHMAP
    typedef folly::ConcurrentHashMap<KeySlice, UserKey*, UKHash, UKEqual> KeyHashMap;
#endif

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

#define TGID_SHIFT 5 /* Max 32 per group */
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
#ifdef USE_HASHMAP_RANDOMREAD
        uint32_t hash_size;
#endif
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

    enum RequestType {
        kPutType = 0,
        kDelType = 1,
        kNrRequestType = 2,
    };


    /* The RequestNode is used to insert a user request into the pending request queue.*/
    class RequestNode {
        /* Free RequestNode Registration */
        private:
            uint8_t col_id_;
            ColumnNode *latest_col_node_;
            std::string trxn_;
            uint16_t trxn_cnt_;// merged trxn cnt + head(1)
            ObjectPoolShard<RequestNode>* alloc_;
        public:
            RequestNode(uint32_t size, ObjectPoolShard<RequestNode>* alloc) :
                col_id_(0), latest_col_node_(NULL), trxn_(), trxn_cnt_(0), alloc_(alloc) {}

            void RequestNodeInit(uint8_t col_id){
                col_id_ = col_id;
                latest_col_node_ = NULL;
                trxn_.resize(0);
                trxn_cnt_ = 0;
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

            void* GetAllocator() { return alloc_; }

            unsigned Realloc(unsigned size) { return 0; }

            void CleanUp() {;}
    };

    enum PrefetchDirection {
        kNoDir = 0,
        kPrev = 1,
        kNext = 2,
        kEndType = 3,
    };

#define P2ROUNDUP(x, a)     (-(-(x) & -(__typeof__(x))(a)))
#define SizeAlignment(s)    P2ROUNDUP((s), kRequestAlignSize) /* Drive IO alignment requirement */

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

    struct TrxnInfo{
        KeySlice key;
        uint8_t keyoffset;
        uint8_t col_id;
        Transaction head;
        uint16_t nr_merged_trxn;
        Transaction *merged_trxn_list;


        TrxnInfo(): key(), col_id(0), head(), nr_merged_trxn(0), merged_trxn_list(nullptr){}
        ~TrxnInfo(){
            if(merged_trxn_list){
                delete [] merged_trxn_list;
            }
        }
        uint8_t GetKeyOffset() {
            return keyoffset;
        }
        Slice &GetKey() { return key; }
        Transaction GetTrxn(){
            return head;
        }
        TransactionID GetTrxnID(){
            return head.id;
        }
        TrxnGroupID GetTrxnGroupID(){
            return (head.id >> TGID_SHIFT);
        }
        uint8_t GetColId() {
            return col_id;
        }
        uint16_t GetNrMergedTrxnList() {
            return nr_merged_trxn;
        }
        Transaction *GetMergedTrxnList() {
            return merged_trxn_list;
        }
    };

    struct ColumnInfo {
        ColumnInfo() : key(0), type(kPutType), col_id(0), ikey_seq(0), /* iter_seq(0), */ ttl(0), trxninfo(nullptr), kbpm(nullptr), kb_index(0), kb_size(0) {}
        ~ColumnInfo() {}
        KeySlice key;
        RequestType type;
        uint8_t col_id;
        SequenceNumber ikey_seq;
        SequenceNumber iter_seq;
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
            static const uint32_t LOGRECKEY_OFFSET_BODY = LOGRECKEY_OFFSET_COMP_PADDING_SIZE + 4;
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
            bool DecodeBuffer(std::vector<uint64_t> &workers_next_ikey_seqs, std::list<TrxnGroupRange> &completed_trxn_groups);
    };

    //
    // Recovery work
    // Manifest creates
    //
    class DBRecovery;
    struct RecoverSKTableCtx {
        struct InSDBThreadsState * state;
        SequenceNumber largest_ikey;
        SequenceNumber largest_seq;
        uint32_t new_uk;
        uint32_t removed_uk;
        uint32_t replace_uk;
        uint32_t start_index;
        uint32_t count;
        DBRecovery* recovery;
    };


    /*
     * RecoverLog.
     *  - During DB recovery, it save lastest recovered information ...
     *  - RecoverLog can provide lastest recovered point.
     * |4B(CRC)|4B(size)|8B(latest_ikey)|8B(latest_seq)|
     *
     */
#define RECOVER_LOG_CRC_OFFSET      (0)
#define RECOVER_LOG_SIZE_OFFSET     (4)
#define RECOVER_LOG_IKEY_OFFSET     (8)
#define RECOVER_LOG_SEQ_OFFSET      (16)
#define RECOVER_LOG_SIZE            (24)

    class DBRecovery {
        private:
            Manifest *mf_;
            Env *env_;
            port::Mutex rdb_mu_;
            LogRecordKey logkey_;
            std::list<TrxnGroupRange> completed_trxn_groups_;
            std::vector<uint64_t> workers_next_ikey_seqs_;
            std::list<uint64_t> deleted_ikey_seqs_;
            std::vector<KEYMAP_HDR*> keymap_hdrs_;

        public:
            DBRecovery(Manifest *mf);
            ~DBRecovery() { }
            const static uint32_t kmax_recover_skt_threads = 16;
            Status LoadLogRecordKey();
            Status LoadBeginsKeysAndCLeanupSKTableMeta(uint32_t &sktid);
            void  PrepareLoadForSKTable(std::vector<KEYMAP_HDR*> &keymap_hdrs, uint32_t sktid);
#ifdef INSDB_USE_HASHMAP
            void UpdateColInfoToUk(std::string &col_info, KeyHashMap *key_hashmap, uint64_t &largest_ikey, uint64_t &largest_seq);
#endif
            void FlushUserKey(UserKey* uk, uint64_t &largest_ikey, uint64_t &largest_seq);
            void LoadKeymapAndProcessColTable(std::vector<KEYMAP_HDR*> &keymap_hdrs, SequenceNumber &largest_ikey, SequenceNumber &largest_seq,
                   uint32_t &new_uk, uint32_t &removed_uk, uint32_t &replace_uk, uint32_t start_index, uint32_t count);
            void RecoverSKTable(RecoverSKTableCtx* recover_ctx) {
                LoadKeymapAndProcessColTable(keymap_hdrs_, recover_ctx->largest_ikey, recover_ctx->largest_seq, recover_ctx->new_uk, recover_ctx->removed_uk, recover_ctx->replace_uk,
                        recover_ctx->start_index, recover_ctx->count);
                recover_ctx->state->mu.Lock();
                recover_ctx->state->num_running -= 1;
                recover_ctx->state->mu.Unlock();
            }
            static void RecoverSKTableWrapper(void* arg){
                (void)pthread_setname_np(pthread_self(), "recover sktable");
                RecoverSKTableCtx * recover_ctx = reinterpret_cast<RecoverSKTableCtx*>(arg);
                recover_ctx->recovery->RecoverSKTable(recover_ctx);
               return;
            }
            bool AddCompletedColumn(std::unordered_map<TransactionID, TransactionColCount> &TRXNList, TransactionID col_trxn_id, uint32_t count);
            void AddUncompletedColumn(std::unordered_map<TransactionID, TransactionColCount> &TRXNList, TransactionID col_trxn_id, uint32_t count);
            void InsertRecoveredKeyToKeymap(Manifest *mf, ColumnInfo &colinfo, uint32_t &new_uk, uint32_t &removed_uk, uint32_t &replace_uk);
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
            uint32_t new_uk_;
            uint32_t removed_uk_;
            uint32_t replace_uk_;
            uint64_t largest_seq_;
            uint64_t largest_ikey_;
            Status status_;
            std::unordered_map<TransactionID, TransactionColCount> UncompletedTRXNList_;
            std::list<ColumnInfo> UncompletedKeyList_;

        public:
            DBRecoveryThread(DBRecovery *recovery, uint16_t worker_id) :
                recovery_(recovery), worker_id_(worker_id), thread_id_(0), new_uk_(0), removed_uk_(0), replace_uk_(0), largest_seq_(0), largest_ikey_(0), status_() { }
            static void DBRecoveryThread_(void* recovery)
            { reinterpret_cast<DBRecoveryThread*>(recovery)->ThreadFn(); }
            void ThreadFn();

            bool StartThread() {
                thread_id_ = recovery_->GetEnv()->StartThread(&DBRecoveryThread_, this);
                return thread_id_;
            }
            void JointThread() { if(thread_id_) recovery_->GetEnv()->ThreadJoin(thread_id_); }
            Status GetStatus() { return status_; }
            uint64_t GetLargestIKey() { return largest_ikey_; }
            uint64_t GetLargestSeq() { return largest_seq_; }
            uint32_t GetNewUK() { return new_uk_;}
            uint32_t GetRemovedUK() { return removed_uk_;}
            uint32_t GetReplaceUK() { return replace_uk_;}
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

    template <class T>
    class ObjectPoolShard {
    public:
        ObjectPoolShard(unsigned num_entries) :
            entryc_(num_entries),
            mem_alloc_(0),
            mem_alloc_buf_(0),
            pendingc_(0) {}
        ~ObjectPoolShard() {
            while (!free_list_.empty()) {
                T* obj = free_list_.front();
                free_list_.pop_front();
                delete obj;
                pendingc_--;
                DecMetaSize(sizeof (T));
            }
            //TODO: J.L.: to be uncommented once KBPM issue is fixed
            //assert(!pendingc_);
            //assert(!mem_alloc_);
            //assert(!mem_alloc_buf_);
        }

        T* Getlocal(uint32_t size) {
            T* obj = nullptr;

            if (!free_list_.empty() && !lock_.TryLock()) {
                if (!free_list_.empty()) {
                    obj = free_list_.front();
                    free_list_.pop_front();
                }
                lock_.Unlock();
            }
            if (obj) {
                size = obj->Realloc(size);
            } else {
                obj = new T(size, this);
                pendingc_++;
                IncMetaSize(sizeof (T));
            }

            return obj;
        }

        void Putlocal(T* obj) {
            if (free_list_.size() < entryc_) {
                lock_.Lock();
                free_list_.push_back(obj);
                lock_.Unlock();
            } else {
                T* tmp_obj;
                lock_.Lock();
                free_list_.push_back(obj);
                tmp_obj = free_list_.front();
                free_list_.pop_front();
                lock_.Unlock();
                delete tmp_obj;
                pendingc_--;
                DecMetaSize(sizeof (T));
            }
        }

        void* AllocBuf(unsigned size) {
            IncBufSize(size);
            return malloc(size);
        }
        void FreeBuf(void* p, unsigned size) {
            free(p);
            DecBufSize(size);
        }

        uint64_t MemStats() {
            return mem_alloc_.load(std::memory_order_relaxed) +
                   mem_alloc_buf_.load(std::memory_order_relaxed);
        }

        void IncMetaSize(unsigned size) {
            mem_alloc_.fetch_add(size, std::memory_order_relaxed);
        }
        void DecMetaSize(unsigned size) {
            mem_alloc_.fetch_sub(size, std::memory_order_relaxed);
        }
        void IncBufSize(unsigned size) {
            mem_alloc_buf_.fetch_add(size, std::memory_order_relaxed);
            if (mem_alloc_buf_ & (1ULL << 63))
                abort();
        }
        void DecBufSize(unsigned size) {
            mem_alloc_buf_.fetch_sub(size, std::memory_order_relaxed);
            if (mem_alloc_buf_ & (1ULL << 63))
                abort();
        }

    private:
        __attribute__((aligned(64)))
        Spinlock lock_;
        std::list<T*> free_list_;
        std::atomic<uint64_t> mem_alloc_;
        std::atomic<uint64_t> mem_alloc_buf_;
        std::atomic<uint32_t> pendingc_;        /* for debug only */
        unsigned entryc_;
    };

    template <class T>
    class ObjectPool {
    public:
        ObjectPool(size_t num_pools, size_t num_entries, uint64_t lim, std::string name):
            shardc_(num_pools), mem_lim_(lim), mem_sleep_(0), mem_total_(0), curr_idx_(0)
        {
            name_ = new std::string(name.c_str());
            if (pthread_key_create(&thr_key_, NULL))
                abort();

            int ret = posix_memalign((void**)&shards_, 64, shardc_ * sizeof (ObjectPoolShard<T>*));
            if (ret)
                abort();

            for (int i=0; i<shardc_; i++)
                shards_[i] = new ObjectPoolShard<T>(num_entries);
        }

        ~ObjectPool() {
            for (int i=0; i<shardc_; i++)
                delete shards_[i];

            free(shards_);

            if (pthread_key_delete(thr_key_))
                abort();

            delete name_;
        }

        /* Never fail */
        T* Get(uint32_t size = 0) {
            int cnt = 0;
            T* obj = nullptr;
            ObjectPoolShard<T>* shard;


#define unlikely(x) __builtin_expect(!!(x), 0)
            while (unlikely(mem_sleep_)) {
                printf("Sleep..\n");
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }

            shard = reinterpret_cast<ObjectPoolShard<T>*>(pthread_getspecific(thr_key_));
            if (!shard) {
                uint64_t i_next, i = curr_idx_;
                do {
                    i_next = (i + 1) % shardc_;
                } while (!curr_idx_.compare_exchange_weak(i, i_next));
                shard = shards_[i];
                pthread_setspecific(thr_key_, reinterpret_cast<void*>(shard));
            }

            return shard->Getlocal(size);
        }

        void Put(T* obj) {
            ObjectPoolShard<T>* shard = static_cast<insdb::ObjectPoolShard<T>*>(obj->GetAllocator());
            obj->CleanUp();
            shard->Putlocal(obj);
        }

        uint64_t PoolMemStats() {
            uint64_t sum = 0;

            for (int i=0; i<shardc_; i++)
                sum += shards_[i]->MemStats();
            mem_sleep_ = (sum > mem_lim_) ? 1 : 0;

            return mem_total_ = sum;
        }

    private:
        __attribute__((aligned(64)))
        pthread_key_t thr_key_;
        ObjectPoolShard<T>** shards_;
        uint64_t mem_lim_;
        uint32_t mem_sleep_;
        uint64_t mem_total_;  /* for debug */

        std::atomic<uint64_t> curr_idx_;
        unsigned shardc_;
        Manifest *mf_;
        std::string* name_;
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

    class Manifest {
        private:
            typedef SkipList<SKTableMem*, KeySlice, UserKeyComparator> SKTList;
#ifdef INSDB_USE_HASHMAP
            typedef folly::ConcurrentHashMap<SequenceNumber, KeyBlockPrimaryMeta*, KBMHash> KBPMHashMap;
#endif
            typedef LRU_Cache  KBCache;
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
            std::atomic<int64_t> mem_use_;
            int64_t mem_use_cached_;
            std::atomic<int64_t> skt_memory_usage_;
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
            std::atomic<uint32_t> kbpm_cnt_;
            std::atomic<uint32_t> kb_cnt_;
            std::atomic<uint32_t> flushed_key_cnt_;
            bool enable_blocking_read_;
#endif
            uint32_t kbpm_pad_size_;
            std::atomic<KeyBlockPrimaryMeta*> *kbpm_pad_;
            std::atomic<KeyBlock*> *kb_pad_;

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
            Spinlock uctrxn_group_lock_;
            std::unordered_map<TrxnGroupID, uint32_t> uncompleted_trxn_group_;
            //////////////////***************************///////////////////
            Spinlock ctrxn_group_lock_;
            std::list<TrxnGroupRange> completed_trxn_group_list_;
            ////////////////////////////////////////////////////////////////
            uint32_t max_iter_key_buffer_size_;
            uint32_t max_iter_key_offset_buffer_size_;

            ObjectPool<UserKey>*     uk_pool_;
            ObjectPool<UserValue>*   uv_pool_;
            ObjectPool<ColumnNode>*  cn_pool_;
            ObjectPool<RequestNode>* req_pool_;
            ObjectPool<KeyBlock>*    kb_pool_;
            ObjectPool<KeyBlockPrimaryMeta>* kbpm_pool_;
            //Spinlock *free_request_node_spinlock_;
            //std::deque<RequestNode*> *free_request_node_;

            /* LogCleanup thread queues */
            WorkerData *logcleanup_data_; /* need to create as worker count */

            Spinlock free_skt_buffer_spinlock_;
            std::deque<Slice> free_skt_buffer_;

            KBCache * kb_cache_;
            KBPM_Cache *kbpm_cache_;
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

            bool AddKBCache(SequenceNumber key, KeyBlock* kb);
            KeyBlock* RefKBCache(SequenceNumber key);
            void RemoveDummyKB(KeyBlock* kb);
            void DummyKBToActive(SequenceNumber key, KeyBlock *kb);
            KeyBlock* FindKBCache(SequenceNumber key);
            KeyBlock* FindKBCacheWithoutRef(SequenceNumber key);
            void EvictKBCache();
            void CleanupKBCache();
            void EvictKBPMCache();
            void AddKBPMCache(SequenceNumber key, KeyBlockPrimaryMeta* meta);


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

            const static uint64_t def_uk_entry_cnt          = 64 * 1024;
            const static uint64_t def_req_entry_cnt         = 1024 * 1024;
            const static uint64_t def_uv_entry_cnt          = 8 * 1024;
            const static uint64_t def_cn_entry_cnt          = 1024 * 1024;

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
            void SetLastSequenceNumber(uint64_t seq) { sequence_number_.store(seq); }
#if 0
            void SetNextInSDBKeySeqNum(uint64_t ikey_seq) { next_InSDBKey_seq_num_.store(ikey_seq, std::memory_order_seq_cst); }
#else
            void SetNextInSDBKeySeqNum(uint64_t seq) { next_InSDBKey_seq_num_.store(seq); }
#endif
            uint32_t GenerateNextSKTableID() { return next_sktable_id_.fetch_add(1, std::memory_order_relaxed) + 1; }
            void SetNextSKTableID(uint32_t skt_id) { next_sktable_id_.store(skt_id); }
            uint32_t GetCurNextSKTableID() { return next_sktable_id_.load(); }
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
#ifdef USE_HASHMAP_RANDOMREAD
            Status BuildFirstSKTable(int32_t skt_id, uint32_t &io_size, uint32_t &hash_data_size);
#else
            Status BuildFirstSKTable(int32_t skt_id, uint32_t &io_size);
#endif
            Status CreateNewInSDB();
            int ReadSKTableMeta(char* buffer, InSDBMetaType type, uint32_t skt_id, GetType req_type, int buffer_size);
#ifdef USE_HASHMAP_RANDOMREAD
            Arena* LoadKeymapFromDevice(uint32_t skt_id, uint32_t keymap_size, uint32_t keymap_hash_size, KeyMapHashMap*& hashmap,  GetType type);
#else
            Arena* LoadKeymapFromDevice(uint32_t skt_id, uint32_t keymap_size, GetType type);
#endif
            Status LoadColInfoFromDevice(uint32_t skt_id, uint32_t &cur_col_id, std::string &col_info);
            Status ReadAheadSKTableMeta(InSDBMetaType type, uint32_t skt_id, int buffer_size = kMaxSKTableSize);
            Status DeleteSKTableMeta(InSDBMetaType type, uint32_t skt_id, uint16_t nr_key = 1);
            Status ReadSKTableInfo(uint32_t skt_id, SKTableInfo* reocver);
            Status CleanupPreallocSKTable(uint32_t prealloc_skt, uint32_t next_skt_id);
            Status RecoverInSDB(bool &bNeedInitManifest, bool create_if_missing);
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
            int64_t MemUseUpdate() {
                mem_use_cached_ = mem_use_ +
                                  uk_pool_->PoolMemStats() + uv_pool_->PoolMemStats() +
                                  cn_pool_->PoolMemStats() + req_pool_->PoolMemStats() +
                                  kb_pool_->PoolMemStats() + kbpm_pool_->PoolMemStats();
                return mem_use_cached_;
            }
            int64_t MemUse() { return mem_use_cached_; }

            /* Try your best NOT to use update version in hot path */
            bool MemUseHigh() { return MemUse() > kMaxCacheSize; }
            bool MemUseLow() { return MemUse() < kCacheSizeLowWatermark; }

            bool MemUseHighUpdate() { return MemUseUpdate() > kMaxCacheSize; }
            bool MemUseLowUpdate() { return MemUseUpdate() < kCacheSizeLowWatermark; }

            int64_t SKTMemoryUsage(){
                return skt_memory_usage_.load(std::memory_order_relaxed);
            }
            void IncreaseMemoryUsage(int32_t size){
#ifdef MEM_TRACE
                int64_t old_val = mem_use_.fetch_add(size, std::memory_order_relaxed);
                if ((old_val >> 27) !=  ((old_val + size) >> 27)) {
                    printf("MEM: cur mem %ld: skt %ld, uk %ld, col %ld, sktd %ld, kv %ld, iterpad %ld\n"
                            "FLUSH & SKT: miss(%ld):hit(%ld):ToClean(%ld):ToDirt(%ld):new skt(%ld), Log:ToClean(%ld),Evict:evicted_skt(%ld)\n"
                            "PR: iter(%ld) next(%ld) clear(%ld) skip(%ld) evicted_with_pr(%ld)"
                            "evict_iter(%ld) evict_skt_iter(%ld) evict_stop(%ld) evict_reclaim(%ld)\n\n",
                            mem_use_.load(std::memory_order_relaxed),
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
                mem_use_.fetch_add(size, std::memory_order_relaxed);
#endif
            }
            void DecreaseMemoryUsage(int32_t size){
#ifdef MEM_TRACE
                uint64_t old_val = mem_use_.fetch_sub(size, std::memory_order_relaxed);
                if ((old_val >> 27) !=  ((old_val - size) >> 27)) {
                    printf("MEM: cur mem %ld: skt %ld, uk %ld, col %ld, sktd %ld, kv %ld, iterpad %ld\n"
                            "FLUSH & SKT: miss(%ld):hit(%ld):ToClean(%ld):ToDirt(%ld):new skt(%ld), Log:ToClean(%ld),Evict:evicted_skt(%ld)\n"
                            "PR: iter(%ld) next(%ld) clear(%ld) skip(%ld) evicted_with_pr(%ld)"
                            "evict_iter(%ld) evict_skt_iter(%ld) evict_stop(%ld) evict_reclaim(%ld)\n\n",
                            mem_use_.load(std::memory_order_relaxed),
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
                mem_use_.fetch_sub(size, std::memory_order_relaxed);
#endif
            }

            void IncreaseSKTDFNodeSize(int32_t size){
                IncreaseMemoryUsage(size);
                skt_memory_usage_.fetch_add(size, std::memory_order_relaxed);
#ifdef MEM_TRACE
                IncSKTDF_MEM_Usage(size);
#endif
            }

            void DecreaseSKTDFNodeSize(int32_t size){
                DecreaseMemoryUsage(size);
                skt_memory_usage_.fetch_sub(size, std::memory_order_relaxed);
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
#if 0
            void DecreaseUpdateCount(uint32_t size){
                update_cnt_.fetch_sub(size, std::memory_order_relaxed);
            }
#endif
            bool CheckKeyEvictionThreshold(void) { return MemUseHigh(); }
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
                if(cache_thread_shutdown_.load(std::memory_order_relaxed) && IsAllSKTCacheListEmpty() && !IsCacheThreadsBusy() && IsWorkerLogCleanupQueueEmpty() && flushed_key_hashmap_->empty() && IsSKTKBMUpdateListEmpty())
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
            bool SKTableReclaim(SKTableMem *skt);
            void SKTableReclaim(SKTableMem *skt, int64_t &reclaim_size, bool force_reclaim, std::list<SKTableMem*> &clean_list, std::list<SKTableMem*> &dirty_list );
            //Eviction
            void SKTableCleanup(SKTableMem *skt, int64_t &reclaim_size, std::list<SKTableMem*> &clean_list, std::list<SKTableMem*> &dirty_list);
            void EvictUserKeys(std::list<SKTableMem*> &skt_list, int64_t &reclaim_size, bool cleanup);

            uint16_t GetOffloadFlushCount();
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

            KeyBlock *AllocKB();
            void FreeKB(KeyBlock *node);
            KeyBlockPrimaryMeta *AllocKBPM() { return kbpm_pool_->Get(); }
            void FreeKBPM(KeyBlockPrimaryMeta * node) { kbpm_pool_->Put(node); }

            void ClearKBPMPad(KeyBlockPrimaryMeta *kbpm);
            void ClearKBPad(KeyBlock *kb);

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

            /////////////////////////////////////////// END  : Memory Allocation/Deallocation ///////////////////////////////////////////

            /////////////////////////////////////////// START: User Key & Value[UserKey/SKTDF/KBPM/KBML] Manageement ///////////////////////////////////////////

            bool LoadValue(uint64_t insdb_key_seq, uint32_t ivalue_size, bool *from_readahead, KeyBlock* kb, bool report_error = false);
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

            void FlushSingleSKTableMem(SKTableMem* sktable, std::string &col_info_table, std::string &comp_col_info_table);
            void WorkerThreadFlushSKTableMem(std::string &col_info_table, std::string &comp_col_info_table);
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

            Status GetValueFromKeymap(const KeySlice& key, std::string* value, PinnableSlice* pin_slice, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, NonBlkState &non_blk_flag_);
            void GetCurrentOrderKBPMList(std::map<SequenceNumber, KeyBlockPrimaryMeta*> &kbpm_order_list);
            void ResetKBPMHashMap();
            void ResetTrxn();
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
#ifdef USE_HASHMAP_RANDOMREAD
            Status CleanUpSKTable(int skt_id, int skt_size, int hash_size,  DeleteInSDBCtx *Delctx, std::set<uint64_t> &keylist);
#else
            Status CleanUpSKTable(int skt_id, int skt_size,  DeleteInSDBCtx *Delctx, std::set<uint64_t> &keylist);
#endif
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
#define TTL_FLAG_LOC 1
    class ColumnNode{// 64 Byte
        private:
            friend class UserKey;

            volatile UserKey* uk_;                                       //1byte
            volatile RequestNode* req_node_;                                       //1byte
            TrxnGroupID tgid_;                                       //1byte
            uint64_t ikey_seq_;
            uint64_t end_seq_;
            uint32_t ivalue_size_;
            /*
             * col_info_ : It will be copied to node in keymap
             * ||userkey(1bit) used in keymap | del type(1bit) col_info size(7bit) | ttl flag(1bit) | col_id(7bit)|
             *     | KB offset(8bit) | iter seq #(varint64) | (TTL(varint64)) | ikey(varint64) | KB Size(varint32)||
             */
            std::string col_info_;
            uint64_t uvalue_or_kbm_ ;                                       //8byte
            volatile uint8_t flags_;                                       //1byte
            SimpleLinkedNode col_list_node_;                                //16byte
            /*Set the ikey_seq_ with ColumnLock*/
            ObjectPoolShard<ColumnNode>* alloc_;

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

            ColumnNode(uint32_t size, ObjectPoolShard<ColumnNode>* alloc) :
                    uk_(NULL), req_node_(NULL), tgid_(0), ikey_seq_(0), end_seq_(0),
                    ivalue_size_(0), col_info_(), uvalue_or_kbm_(0), flags_(0),
                    col_list_node_(), alloc_(alloc) {}
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

                col_list_node_.next_ = col_list_node_.prev_ = nullptr;
            }
            /* Call from Store function(New column)  */
            void NewColumnInit(UserKey* uk, RequestType type, uint8_t col_id, uint64_t ttl, SequenceNumber seq,
                               uint32_t data_size/*For estimating KB size : key size + value size*/) {
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
                if(ttl) col_id |= TTL_BIT;
                col_info_.append(1, col_id);
                col_info_.append(1, 0); /* kb offset */
                if(ttl){
                    size = PutVarint64(&col_info_, seq);
                    PutVarint64(&col_info_, ttl);
                    data_size += 8/*sizeof(ttl)*/;
                }else{
                    size = PutVarint64(&col_info_, seq);
                }

                //uvalue_or_kbm_ = 0; // UserValue has been set
                flags_ = flags_pending_request;

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

            /* col_info_ is not accounted */
            unsigned Realloc(unsigned size) { return 0; }
            unsigned Size() { return sizeof (*this); }

            void* GetAllocator() { return alloc_; }

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
                col_info.remove_prefix(KB_OFFSET_LOC+1);
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

#define SEQNUM_LOC (KB_OFFSET_LOC+1)
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
                bool b_ttl = *(col_info.data() + TTL_FLAG_LOC) & TTL_BIT;
                if (b_ttl) {
                    col_info.remove_prefix(KB_OFFSET_LOC+1);
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
                bool b_ttl = *(col_info.data() + TTL_FLAG_LOC) & TTL_BIT;
                col_info.remove_prefix(KB_OFFSET_LOC+1);
                if (b_ttl) {
                    GetVarint64(&col_info, &temp);//to skip iter seq number
                    GetVarint64(&col_info, &temp);//to skip TTL
                }else{
                    GetVarint64(&col_info, &temp);//to skip iter seq number
                }
                GetVarint64(&col_info, &temp);//to skip ikey seq number
                GetVarint32(&col_info, &ivalue_size_);//Get KB size
                return ivalue_size_;

            }

            uint8_t GetKeyBlockOffset(){
                return (uint8_t) (*(col_info_.data()+KB_OFFSET_LOC));
            }
            void SetKeyBlockOffset(uint8_t offset){
                assert(offset < kMaxRequestPerKeyBlock);
                col_info_[KB_OFFSET_LOC] = offset;
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

            void CleanUp() {;}
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
        public:
            class ColumnData { // 40 Byte( Valid 38 Byte)
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


            SimpleLinkedNode link_;

            /* For Free Key Registration */
            UserKey(uint32_t key_size, ObjectPoolShard<UserKey>* alloc) :
                col_lock_(),
                ref_cnt_(0), // Increase reference count for new key
                key_buffer_size_(0),
                key_buffer_(NULL),
                user_key_(),
                latest_ikey_(0),
                in_hash(false),
                col_data_(NULL),
                link_(),
                alloc_(alloc)
            {
                col_data_ = new ColumnData[kMaxColumnCount];
                key_buffer_ = (char*)alloc_->AllocBuf(key_size);
                key_buffer_size_ = (uint16_t)key_size;
            }

            ~UserKey() {
                //assert(!IsDirty());
                /* UserKey::Cleanup() must be executed before calling UserKey Destructor */
                assert(col_data_);
                assert(key_buffer_);
                alloc_->FreeBuf(key_buffer_, key_buffer_size_);
                delete [] col_data_;
            }
#ifndef CODE_TRACE
            void ScanColumnInfo(std::string &str);
#endif
            void SetLatestiKey(SequenceNumber ikey){ latest_ikey_ = ikey; }
            SequenceNumber GetLatestiKey(void ){return  latest_ikey_; }
            bool BuildColumnInfo(Manifest *mf, uint16_t &size, Slice buffer, SequenceNumber &largest_ikey, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue, bool &has_del);
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
            SequenceNumber GetSmallestSequence();
            ColumnNode* GetColumnNode(uint16_t col_id, bool& deleted, uint64_t cur_time = -1, SequenceNumber seq_num = -1, uint64_t ttl = 0);
#define TRXN_DEL_REQ 0x80
            void DoVerticalMerge(RequestNode *req_node, UserKey* uk, uint16_t col_id, uint8_t worker_id, uint8_t idx, Manifest *mf, KeyBlockPrimaryMeta* kbml);
            bool InsertColumnNode(ColumnNode* input_col_node, uint16_t col_id, bool &ignored, bool front_merge = false/*For the UserKey in iterator hash*/);
            bool InsertColumnNodes(Manifest *mf, uint8_t nr_column, Slice &col_info, bool front_merge = false);
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


            uint32_t GetReferenceCount(){ return (uint32_t)(ref_cnt_.load(std::memory_order_relaxed)); }
            void IncreaseReferenceCount(uint32_t cnt = 1)
            {
#ifndef REFCNT_SANITY_CHECK
                ref_cnt_.fetch_add(cnt, std::memory_order_relaxed);
#else
                uint32_t old = ref_cnt_.fetch_add(cnt, std::memory_order_relaxed);
                if (old+1==0) abort();
#endif
            }
            uint32_t DecreaseReferenceCount()
            {
#ifndef REFCNT_SANITY_CHECK
                return (ref_cnt_.fetch_sub(1, std::memory_order_relaxed));
#else
                uint32_t ref_cnt = (ref_cnt_.fetch_sub(1, std::memory_order_seq_cst));
                if (ref_cnt==0) abort();
                return ref_cnt;
#endif
            }

            uint32_t DecreaseReferenceCount(uint32_t dec)
            {
#ifndef REFCNT_SANITY_CHECK
                return (ref_cnt_.fetch_sub(dec, std::memory_order_relaxed));
#else
                uint32_t ref_cnt = (ref_cnt_.fetch_sub(dec, std::memory_order_seq_cst));
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

            uint16_t GetKeyInfoSize(Manifest *mf, bool &submitted){
                in_hash.store(false, std::memory_order_seq_cst);
                if(GetReferenceCount()){
                    in_hash.store(true, std::memory_order_seq_cst);
                    submitted = false;
                }
                ColumnNode* col_node = NULL;
                SimpleLinkedList* col_list = NULL;
                SimpleLinkedNode* col_list_node = NULL;
                uint16_t col_info_size = 0;
                for( uint8_t col_id = 0 ; col_id < kMaxColumnCount ; col_id++){
                    if(col_node = col_data_[col_id].latest_col_node_){
                        col_info_size += col_node->GetColInfoSize();
                        if(!col_node->IsSubmitted() || (!col_node->IsTRXNCommitted() && col_node->GetTGID() && !mf->IsTrxnGroupCompleted(col_node->GetTGID()))){
                            in_hash.store(true, std::memory_order_seq_cst);
                            submitted = false;
                        }
                    }
                }
                return col_info_size + 1/*To store nr column*/;
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
                    if(col_node = col_data_[col_id].latest_col_node_){
                        col_info_size += col_node->GetColInfoSize();
                        if(!col_node->IsSubmitted() || (!col_node->IsTRXNCommitted() && col_node->GetTGID() && !mf->IsTrxnGroupCompleted(col_node->GetTGID()))){
                            in_hash.store(true, std::memory_order_seq_cst);
                            return 0; // If TRXN is not completed yet, does not move the key to flushed_key_hash
                        }
                    }
                }
                return col_info_size+1/*To store nr column*/;
            }

            /*
             * In key remove/key insert function, The caller always knows whether the key is begin key or not.
             * return UserKey : No eviction in pregress
             * return NULL    : eviction in progress
             */
            size_t GetSize();
            void* GetAllocator() { return alloc_; }

            void CleanUp() {
                for (int i=0; i<kMaxColumnCount; i++) {
                    assert(!col_data_[i].col_list_.Size());
                }
            }
            unsigned Realloc(unsigned size) {
                unsigned sz = 0;

                if (size > key_buffer_size_) {
                    sz = size - key_buffer_size_;
                    alloc_->FreeBuf(key_buffer_, key_buffer_size_);
                    key_buffer_ = static_cast<char*>(alloc_->AllocBuf(size));
                    key_buffer_size_ = size;
                }

                return sz;
            }
        private:
            /*  Colocating a column's data into adjacent memory space.
             *  For CPU Cache Prefetch Optimization
             *  Access seq : latest_col_node_ => col_lock_ => col_list_
             */
            Spinlock col_lock_;             // 1 Byte
            std::atomic<uint32_t> ref_cnt_;                             //8byte
            uint16_t key_buffer_size_;
            char* key_buffer_;
            KeySlice user_key_;                                                         //8byte
            SequenceNumber latest_ikey_;
            std::atomic<bool> in_hash;
            ObjectPoolShard<UserKey>* alloc_;
            /*
             * Make a structure for (col_date_ ~ lock_) to secure KeySlice Space within 32 byte UserKey
             * If we use "user_key_and_keysize_" instead of "KeySlice", we have to allocate & copy 16 byte KeySlice for GetKey()
             * for every accesses.
             *
             * But the user will get the pointer of "col_data & skt" before use it. and than accessing the object using the pointer
             */
            ColumnData *col_data_;                                                                  // 8byte
            // No copying allowed
            UserKey(const UserKey&);
            void operator=(const UserKey&);
    }/*UserKey */;

    /*
     * Allocate fixed size memory during DB initialization
     * And then reuse the allocated memory until DB termination
     */

#define COLUMN_NODE_ID_MASK      0x7F
#define COLUMN_NODE_HAS_TTL_BIT 0x80

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
            SequenceNumber key_queue_smallest_seq_[2];

            folly::SharedMutex keymap_mu_;
#if 1
            folly::SharedMutex keymap_random_read_mu_;
#endif
            std::atomic<uint8_t> random_read_wait_cnt_;                             //8byte
            //GCInfo gcinfo_;
            Keymap *keymap_;
            int32_t keymap_node_size_; /* size of keymap on device */
#ifdef USE_HASHMAP_RANDOMREAD
            int32_t keymap_hash_size_; /* size of keymap hash on device */
#endif
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
            std::atomic<uint8_t> flags_;                    // 1 Byte
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
#ifdef USE_HASHMAP_RANDOMREAD
            SKTableMem(Manifest* mf, KeySlice& key, uint32_t skt_id, uint32_t keymap_size, uint32_t keymap_hash_size);
#else
            SKTableMem(Manifest* mf, KeySlice& key, uint32_t skt_id, uint32_t keymap_size);
#endif
            SKTableMem(Manifest *mf, Keymap *kemap);
            ~SKTableMem();

            uint32_t GetBufferSize(){
                return keymap_->GetBufferSize();
            }
            void SetSKTableID(uint32_t skt_id);
            void SetPreAllocSKTableID(uint32_t skt_id);
            void SetNextSKTableID(uint32_t skt_id);
            uint32_t  IncCurColInfoID();
            void  ResetCurColInfoID(uint32_t prev_col_id, uint32_t next_col_id);
            void SetPrevColInfoID();
            uint32_t GetSKTableID() { return skt_id_; }
            uint32_t GetPreAllocSKTableID();
            uint32_t GetNextSKTableID();
            uint32_t GetPrevColInfoID();
            uint32_t GetCurColInfoID();

#ifdef USE_HASHMAP_RANDOMREAD
            void SetKeymapNodeSize(uint32_t size, uint32_t hash_data_size) {
                keymap_node_size_ = size;
                keymap_hash_size_ = hash_data_size;
            }
#else
            void SetKeymapNodeSize(uint32_t size) {
                keymap_node_size_ = size;
            }
#endif
            uint32_t GetKeymapNodeSize() { return keymap_node_size_; /* size of keymap on device */ }
#ifdef USE_HASHMAP_RANDOMREAD
            uint32_t GetKeymapHashSize() { return keymap_hash_size_; /* size of keymap hash on device */ }
#endif
            KeySlice GetBeginKeySlice() { return keymap_->GetKeymapBeginKey(); }
            void DeleteSKTDeviceFormatCache(Manifest* mf, bool skip = false);

            bool NeedToSplit() {
                assert(keymap_->HasArena());
                // If size of keymap is bigger than 2 times of kMaxSKTableSize, split the SKTable
                if(keymap_->GetAllocatedSize() - keymap_->GetFreedMemorySize() > kMaxSKTableSize * 2 || keymap_->GetAllocatedSize() > kDeviceRquestMaxSize) return true;
                return false;
            }

            bool NeedToCleanup() {
                if (!keymap_->HasArena()) return false;
                if (keymap_->GetFreedMemorySize() > kMaxSKTableSize/4) return true;
                return false;
            }

            uint32_t GetCount() {
                assert(keymap_->HasArena());
                return keymap_->GetCount();
            }

            void InsertArena(Arena* arena) {
                keymap_->InsertArena(arena);
            }
#ifdef USE_HASHMAP_RANDOMREAD
            void InsertArena(Arena* arena, KeyMapHashMap* hashmap) {
                keymap_->InsertArena(arena, hashmap);
            }
            Arena* MemoryCleanupSKTDeviceFormat(Manifest* mf, KeyMapHashMap* hashmap) {
                return keymap_->MemoryCleanup(mf, hashmap);
            }
#else
            Arena* MemoryCleanupSKTDeviceFormat(Manifest* mf) {
                return keymap_->MemoryCleanup(mf);
            }
#endif
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
            void PushUserKeyToActiveKeyQueue(UserKey* uk, SequenceNumber seq){
                keyqueue_lock_.Lock();
                key_queue_[active_key_queue_].InsertBack(uk->GetLink());
                // update smallest sequence number
                if (seq < key_queue_smallest_seq_[active_key_queue_])
                    key_queue_smallest_seq_[active_key_queue_] = seq;
                keyqueue_lock_.Unlock();
            }
            // caller should maintain smallest sequence number
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
            void UpdateKeyInfoUsingUserKeyFromKeyQueue(Manifest *mf, SequenceNumber seq, bool acquirelock, std::string &col_info_table, SequenceNumber &largest_ikey, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue);
            bool InsertUserKeyToKeyMapFromInactiveKeyQueue(Manifest *mf, bool inactive, SequenceNumber seq, bool acquirelock = true);
            SequenceNumber GetSmallestSequenceInKeyQueue(uint8_t idx) { return key_queue_smallest_seq_[idx]; }
            void SetSmallestSequenceInKeyQueue(uint8_t idx, SequenceNumber seq) { if(key_queue_smallest_seq_[idx] > seq) key_queue_smallest_seq_[idx] = seq; }
            void ResetSmallestSequenceInKeyQueue(uint8_t idx) { key_queue_smallest_seq_[idx] = kMaxSequenceNumber; }
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

            bool PrefetchSKTable(Manifest* mf, bool dirtycache = true, bool no_cachecheck = false, bool nio = false);
            void DiscardSKTablePrefetch(Manifest* mf);
            bool LoadSKTableDeviceFormat(Manifest *mf, GetType type = kSyncGet);

            SequenceNumber GetLatestSequenceNumberFromSKTableMem(Manifest* mf, const KeySlice& key, uint8_t col_id, bool &deleted);
            bool GetColumnInfo(Manifest* mf, const Slice& key, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, ColumnScratchPad& column,  NonBlkState &non_blk_flag_);
            uint16_t LookupColumnInfo(Manifest *mf, Slice& col_info_slice, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, SequenceNumber &biggest_begin_key, bool force_prefetch = false);

            ColumnNode* FindNextValidColumnNode(Manifest *mf, UserKey*&  ukey, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, KeySlice* end_key);
            //UserKey* MoveToNextKeyInKeyMap(Manifest* mf, UserKey* ukey);
            //void EvictKBPM(Manifest *mf);
            bool SKTDFReclaim(Manifest *mf);
            bool SKTDFReclaim(Manifest *mf, int64_t &reclaim_size, bool &active, bool force_evict, uint32_t &put_col, uint32_t &del_col);
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
            const static uint8_t flags_in_dirty_cache          = 0x01;
            const static uint8_t flags_in_clean_cache          = 0x02;
            /* If one of above status is set, the SKT is in cache(flushing/kbm updating/evicting is considered as in-cache)*/
            const static uint8_t flags_cached                  = 0x03;

            const static uint8_t flags_caching_in_progress     = 0x04;
            const static uint8_t flags_fetch_in_progress       = 0x08;
            const static uint8_t flags_deleted_skt             = 0x10;
            const static uint8_t flags_reference               = 0x20;
            const static uint8_t flags_split_sub               = 0x40;
            const static uint8_t flags_eviction_in_progress    = 0x80;
            //Evicting in progress
            bool IsSet(uint8_t bit){
                return (flags_.load(std::memory_order_relaxed) & bit);
            }
            bool IsCached() { return (flags_.load(std::memory_order_relaxed) & flags_cached); }
            bool IsCachedOrPrefetched() { return (flags_.load(std::memory_order_relaxed) & (flags_cached | flags_fetch_in_progress)); }
            void ClearCached() { flags_.fetch_and(~flags_cached, std::memory_order_relaxed); }

            bool SetCacheBit(uint8_t cache){
                if(IsCached())
                    return false;
                flags_.fetch_or(cache, std::memory_order_relaxed);
                return true;
            }

            void ChangeStatus(uint8_t from, uint8_t to){
                if(from && !IsSet(from)) abort();
                if(to && IsSet(to)) abort();
                flags_.fetch_xor((from|to), std::memory_order_relaxed);
                assert(IsSet(to));
                assert(!IsSet(from));
            }
            uint8_t GetFlags() { return flags_.load(std::memory_order_relaxed);}

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

            bool SetReference() { return (flags_.fetch_or(flags_reference, std::memory_order_relaxed) & flags_reference); }
            bool ClearReference() { return flags_.fetch_and(~flags_reference, std::memory_order_relaxed) & flags_reference; }
            bool HasReference() { return (flags_.load(std::memory_order_relaxed) & flags_reference); }

            bool SetSplitSub() { return (flags_.fetch_or(flags_split_sub, std::memory_order_relaxed) & flags_split_sub); }
            bool ClearSplitSub() { return flags_.fetch_and(~flags_split_sub, std::memory_order_relaxed) & flags_split_sub; }
            bool IsSplitSub() { return (flags_.load(std::memory_order_relaxed) & flags_split_sub); }


            bool IsEvictionInProgress() { return (flags_.load(std::memory_order_relaxed)& flags_eviction_in_progress); }
            void SetEvictionInProgress() { flags_.fetch_or(flags_eviction_in_progress, std::memory_order_relaxed); }
            void ClearEvictionInProgress() { flags_.fetch_and((uint8_t)(~flags_eviction_in_progress), std::memory_order_relaxed); }

            void UpdateKeyInfo(Manifest* mf, std::string &col_info_table, SequenceNumber &largest_ikey, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue);

            bool IsKeymapLoaded(){ return keymap_->HasArena(); }
            int32_t GetAllocatedSize() { return keymap_->GetAllocatedSize(); }
            int32_t GetFreedMemorySize() { return keymap_->GetFreedMemorySize(); }

            Status FlushSKTableMem(Manifest *mf, std::string &col_info_table, std::string &comp_col_info_table, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue);

            void SplitInactiveKeyQueue(Manifest* mf, std::vector<std::pair<SKTableMem*, Slice>> &sub_skt);

            bool SplitByPrefix(const Slice& a, const Slice& b);

#define COL_INFO_TABLE_CRC_LOC 4
    /* 4B(comp size) + 4B(CRC)*/
#define COL_INFO_TABLE_META 8
#define COMP_COL_INFO_TABLE_SIZE 4
            InSDBKey StoreColInfoTable(Manifest *mf, std::string &col_info_table, std::string &comp_col_info_table);
#ifdef USE_HASHMAP_RANDOMREAD
            Status StoreKeymap(Manifest *mf, Keymap* sub, uint32_t &io_size, uint32_t &hash_data_size);
#else
            Status StoreKeymap(Manifest *mf, Keymap* sub, uint32_t &io_size);
#endif

            SKTableMem* Next(Manifest* mf);
            SKTableMem* Prev(Manifest* mf);

            SimpleLinkedNode* GetSKTableSortedListNode(){return &skt_sorted_node_;}

            bool IsSKTDirty(Manifest *mf)
            {
                return (!flush_candidate_key_deque_.empty() || mf->IsNextSKTableDeleted(GetSKTableSortedListNode()));
            }

            bool FlushCandidateDequeEmpty() { return flush_candidate_key_deque_.empty();}


            void IncSKTRefCnt(Manifest *mf){
                uint32_t old = ref_cnt_.fetch_add(1, std::memory_order_relaxed);
#ifdef REFCNT_SANITY_CHECK
                if ((old+1)==0) abort();
#endif
                while(IsEvictionInProgress());
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
    class KeyBlock {
        private:
            uint64_t ikey_seq_num_; /* For Discard*/
            std::atomic<uint32_t> kb_ref_cnt_;
            char* raw_data_;
            uint32_t raw_data_size_;
            uint32_t data_size_;
            uint8_t key_cnt_;
            std::atomic<uint8_t> flags_;
            Spinlock lock_;
            ObjectPoolShard<KeyBlock>* alloc_;

            const static uint8_t flags_reference        = 0x01;
            const static uint8_t flags_prefetched       = 0x02;
            const static uint8_t flags_dummy            = 0x04;
            const static uint8_t flags_evicting         = 0x08;

        public:
            KeyBlock(unsigned size, ObjectPoolShard<KeyBlock>* alloc) :
                kb_ref_cnt_(0), raw_data_size_(0), data_size_(0),
                raw_data_(NULL), key_cnt_(0), alloc_(alloc) {}
            ~KeyBlock() {
                if (raw_data_)
                    alloc_->FreeBuf(raw_data_, raw_data_size_);
            }
            void InitKB() {
                ikey_seq_num_ = 0;
                kb_ref_cnt_ = 1;
                flags_ = 0;
            }

            void KeyBlockInit(uint32_t size) {
                data_size_ = size = SizeAlignment(size);
                if (size > raw_data_size_) {
                    if (raw_data_size_)
                        alloc_->FreeBuf(raw_data_, raw_data_size_);
                    raw_data_ = static_cast<char*>(alloc_->AllocBuf(size));
                    raw_data_size_ = size;
                }
            }

            void SetIKeySeqNum(SequenceNumber seq) { ikey_seq_num_ = seq; }
            SequenceNumber GetIKeySeqNum() { return ikey_seq_num_;}

            void KBLock(){ lock_.Lock(); }
            void KBUnlock(){ lock_.Unlock(); }

            bool SetReference() { return (flags_.fetch_or(flags_reference, std::memory_order_relaxed) & flags_reference); }
            bool ClearReference() { return (flags_.fetch_and(~flags_reference, std::memory_order_relaxed) & flags_reference); }

            void SetDummy() { assert(!IsDummy()); flags_.fetch_or(flags_dummy, std::memory_order_seq_cst); }
            void ClearDummy() { flags_.fetch_and(~flags_dummy, std::memory_order_seq_cst); }
            bool IsDummy() { return (flags_.load(std::memory_order_seq_cst) & flags_dummy); }

            bool SetKBPrefetched() { return flags_.fetch_or(flags_prefetched, std::memory_order_relaxed) & flags_prefetched; }
            bool ClearKBPrefetched() { return flags_.fetch_and(~flags_prefetched, std::memory_order_relaxed) & flags_prefetched; }
            bool IsKBPrefetched() { return (flags_.load(std::memory_order_relaxed) & flags_prefetched); }


            bool SetEvicting() { return flags_.fetch_or(flags_evicting, std::memory_order_seq_cst) & flags_evicting; }
            void ClearEvicting() { flags_.fetch_and(~flags_evicting, std::memory_order_seq_cst); }
            bool IsEvicting() { return (flags_.load(std::memory_order_seq_cst) & flags_evicting); }

            bool DecKBRefCnt(){ return (kb_ref_cnt_.fetch_sub(1, std::memory_order_relaxed) == 1);}
            void IncKBRefCnt(){ kb_ref_cnt_.fetch_add(1, std::memory_order_relaxed);}
            uint32_t GetKBRefCnt(){ return kb_ref_cnt_.load(std::memory_order_relaxed);}
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

            void SetKeyCnt(uint8_t cnt) { key_cnt_ = cnt; }
            uint8_t GetKeyCnt() { return key_cnt_; }
            void CleanUp() { }
            bool DecodeAllKeyMeta(KeyBlockPrimaryMeta *kbpm, std::list<ColumnInfo> &key_list/*, uint64_t &largest_seq */);

            //TODO: need to take data_size_ into account!!!
            unsigned Realloc(unsigned size) { return 0; }
            void* GetAllocator() { return alloc_; }
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
            std::atomic<uint8_t> pending_updated_value_cnt_;                      // 8 Byte

            /* lock contention may be low because of fine grained locking. */
            //port::Mutex mu_;                                        //40 Byte
            Spinlock lock_;                                        //1 Byte
            ObjectPoolShard<KeyBlockPrimaryMeta>* alloc_;

            /* Next ikey seq num will be added to end of data_ */
            //uint64_t next_ikey_seq_num_;                                            //8 Byte

            const static uint8_t flags_write_protection         = 0x01;
            const static uint8_t flags_in_updated_kbm_map       = 0x02;
            const static uint8_t flags_dummy                    = 0x04;
            /* The Key Block only contains delete commands. In this case, KeyBlock is not created. only create KeyBlockMetas*/
            const static uint8_t flags_evicting                 = 0x08;
            const static uint8_t flags_in_log                   = 0x10;/* 1 is for in-log */
            const static uint8_t flags_in_cache                 = 0x20;/* 1 is for in-log */
            const static uint8_t flags_kbm_prefetched           = 0x40;
            const static uint8_t flags_need_to_update           = 0x80;//set in FlushSKTableMem, clean in KBMUpdateThread
        public:
#define KBPM_HEADER_SIZE 7
            KeyBlockPrimaryMeta(unsigned size, ObjectPoolShard<KeyBlockPrimaryMeta>* alloc):
                flags_(0),
                nrs_key_cnt_(0),
                kb_bitmap_(0UL),
                ref_bitmap_(0UL),
                org_key_cnt_(0),
                ikey_seq_num_(0),
                alloc_(alloc) {}

            void KBPMLock(){ lock_.Lock(); }
            void KBPMUnlock(){ lock_.Unlock(); }
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
                org_key_cnt_ = 0;
                pending_updated_value_cnt_ = 0;
                flags_.store(0, std::memory_order_relaxed);
                ikey_seq_num_ = ikey_seq_num;
            }

            void NewKBPMInit(uint8_t put_cnt, uint8_t del_cnt, uint64_t *put_iter_seq_num, uint64_t next_ikey_seq_num, uint8_t nr_trxn, KeyBlock *kb, char* kbm_dev_buf, bool set_refbits = true) {
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
                dev_data_size_ += (KB_BITMAP_SIZE+4/*to save KBM size*/);
                EncodeFixed64((dev_data_+dev_data_size_), next_ikey_seq_num);
                dev_data_size_ += 8;
                for( int i = 0 ; i < put_cnt ; i++){
                    dev_data_size_ = (EncodeVarint64((dev_data_ + dev_data_size_), put_iter_seq_num[i]) - dev_data_);
                }
#if 0
                buf = buffer;
                buffer = EncodeVarint64(buf, ts);
                buffer_size+= (buffer-buf);
                //////////////////////////////////////////////////////////////////////////////////////////////
                dev_data_size_ = (EncodeVarint64((dev_data_ + dev_data_size_), next_ikey_seq_num) - dev_data_);
#endif
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
            void AddVarint64ToDeviceFormat(uint64_t data){
                dev_data_size_ = (EncodeVarint64((dev_data_ + dev_data_size_), data) - dev_data_);
            }
            uint8_t GetOriginalKeyCount() { assert(!IsDummy()); return org_key_cnt_; }

            bool SetWriteProtection() { return (flags_.fetch_or(flags_write_protection, std::memory_order_acquire) & flags_write_protection); }
            void ClearWriteProtection() { flags_.fetch_and((~flags_write_protection), std::memory_order_relaxed); }

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


            void SetCache() { assert(!IsCache()); flags_.fetch_or(flags_in_cache, std::memory_order_seq_cst); }
            void ClearCache() { flags_.fetch_and(~flags_in_cache, std::memory_order_seq_cst); }
            bool IsCache() { return (flags_.load(std::memory_order_seq_cst) & flags_in_cache); }


            /* If it is evictable, return true*/
#if 0
            bool SetOutLogAndCheckEvicable() {
                flags_.fetch_and(~flags_in_log, std::memory_order_seq_cst);
                return (!ref_cnt_ && !CheckRefBitmap());
            }
#endif
            bool IsInlog() { return (flags_.load(std::memory_order_seq_cst) & flags_in_log); }

            /* Under lock */
            bool SetKBMPrefetched() { return (flags_.fetch_or(flags_kbm_prefetched, std::memory_order_relaxed) & flags_kbm_prefetched); }
            bool IsKBMPrefetched() { return (flags_.load(std::memory_order_relaxed) & flags_kbm_prefetched); }
            bool ClearKBMPrefetched() { return (flags_.fetch_and((uint8_t)~flags_kbm_prefetched, std::memory_order_relaxed) & flags_kbm_prefetched); }

            bool SetNeedToUpdate() { return (flags_.fetch_or(flags_need_to_update, std::memory_order_seq_cst) & flags_need_to_update); }
            bool ClearNeedToUpdate() { return (flags_.fetch_and((uint8_t)~flags_need_to_update, std::memory_order_relaxed) & flags_need_to_update); }
            bool IsNeedToUpdate() { return (flags_.load(std::memory_order_relaxed) & flags_need_to_update); }
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
                Status s;
                bool from_readahead = false;
                /*Always kNonblokcingRead*/
                if(prefetched)
                    s = mf->GetEnv()->Get(GetKBKey(mf->GetDBHash(), kKBMeta, ikey_seq_num_), kbm_buffer, &buffer_size, kNonblokcingRead, mf->IsIOSizeCheckDisabled()?Env::GetFlag_NoReturnedSize:0, &from_readahead);
                else
                    s = mf->GetEnv()->Get(GetKBKey(mf->GetDBHash(), kKBMeta, ikey_seq_num_), kbm_buffer, &buffer_size, kSyncGet, mf->IsIOSizeCheckDisabled()?Env::GetFlag_NoReturnedSize:0, &from_readahead);
/*
#ifndef NDEBUG
                if (!mf->IsIOSizeCheckDisabled())
                    assert(buffer_size <= max_size);
#endif
*/
#ifdef TRACE_READ_IO
                if(s.ok()) {
                    g_kbm_sync++;
                    if (from_readahead)
                        g_kbm_sync_hit++;
                }
#endif
                if (prefetched) return from_readahead;
                else if (s.ok()) return true;
                else return false;
            }
            void InitWithDeviceFormat(Manifest  *mf, const char *buffer, uint32_t buffer_size);
            void InitWithDeviceFormatForInLog(Manifest  *mf, char *buffer, uint32_t buffer_size);
            void ClearAllKeyBlockBitmap() { kb_bitmap_ = 0;}
            char* GetRawData() { return dev_data_; }
            uint32_t GetRawDataSize() { return dev_data_size_; }
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

            void* GetAllocator() { return alloc_; }
            unsigned Realloc(unsigned size) { return 0; }

            void CleanUp() {
                assert((flags_.load(std::memory_order_seq_cst)&(~(flags_evicting | flags_dummy))) == 0);
                kb_bitmap_.store(0UL,std::memory_order_relaxed);
                org_key_cnt_ = 0;
           }

            bool EvictColdKBPM(Manifest *mf)
            {
                if(CheckRefBitmap() == 0 && !IsCache()){
                    /* If it is last one */
                    if(!SetEvicting()){
                        if(CheckRefBitmap() == 0) {
                            mf->ClearKBPMPad(this);
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(CheckRefBitmap() != 0);/* Wait until deref */
#ifdef MEM_TRACE
                            mf->DecKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif
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

            bool CheckColdKBPM(Manifest *mf)
            {
                if(CheckRefBitmap() == 0 && !IsCache()){
                    /* If it is last one */
                    if(!SetEvicting()){
                        if (TestKeyBlockBitmap()){
                            mf->AddKBPMCache(GetiKeySequenceNumber(), this);
                            ClearEvicting();
                            return false;
                        } else if(CheckRefBitmap() == 0) {
                            mf->ClearKBPMPad(this);
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(CheckRefBitmap() != 0);/* Wait until deref */
#ifdef MEM_TRACE
                            mf->DecKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif
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





            bool EvictKBPMFromCache(Manifest *mf)
            {
                assert(IsCache());
                if(CheckRefBitmap() == 0 && !IsNeedToUpdate() && !IsInlog()){
                    /* If it is last one */
                    if(!SetEvicting()){
                        if(CheckRefBitmap() == 0 && !IsNeedToUpdate() && !IsInlog()){
                            mf->ClearKBPMPad(this);
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(CheckRefBitmap() != 0);/* Wait until deref */
#ifdef MEM_TRACE
                            mf->DecKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif
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
                            mf->ClearKBPMPad(this);
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(CheckRefBitmap() != 0);/* Wait until deref */
#ifdef MEM_TRACE
                            mf->DecKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif
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
                            mf->ClearKBPMPad(this);
                            mf->RemoveKBPMFromHashMap(GetiKeySequenceNumber());
                            while(CheckRefBitmap() != 0);/* Wait until deref */
#ifdef MEM_TRACE
                            mf->DecKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif
                            reclaim_size-= sizeof(KeyBlockPrimaryMeta);
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
            uint64_t GetNextIKeySeqNumFromDeviceFormat() {
                uint64_t next_ikey = 0;
                assert(IsInlog());
                assert(dev_data_);
                next_ikey = (*((uint64_t*)(dev_data_+ NEXT_IKEY_SEQ_LOC)));
                return next_ikey;
            }

            void CleanupDeviceFormat() {
                if(dev_data_ && dev_data_size_ != USE_DEV_DATA_FOR_NEXT_IKEY_SEQ) {
                    /* cleanup buffer allocated for recovery */
                    free(dev_data_);
                    dev_data_ = nullptr;
                    dev_data_size_ = 0;
                }
                SetOutLog();
            }
            uint8_t GetNRSCount(){//Number of keys not recorded in table
                return nrs_key_cnt_.load(std::memory_order_seq_cst);
            }
            uint8_t DecNRSCount(){//Number of keys not recorded in table
                uint8_t nrs_cnt = nrs_key_cnt_.fetch_sub(1, std::memory_order_seq_cst);
                assert(nrs_cnt);
                return  nrs_cnt - 1;//return new count
            }

            bool DecodeAllDeleteKeyMeta(std::list<ColumnInfo> &key_list/*, uint64_t &largest_seq */);
    };

    class KBPM_Cache {
        typedef std::list<std::pair<SequenceNumber, KeyBlockPrimaryMeta*>>     KBPMList;
        KBPMList    _data;
        size_t      _max_size;
        size_t      _cum_size;
        size_t      _low_bound;
        size_t      _insert_count;
        size_t      _evict_count;
        Spinlock    _lock;
        public:

        explicit KBPM_Cache(size_t max_size)
            : _max_size(max_size),
            _insert_count(0),
            _evict_count(0),
            _cum_size(0),
            _low_bound(0),
            _lock()
        {
            assert(max_size);
            _low_bound = (max_size - (max_size /8u));
        }

        KBPM_Cache(const KBPM_Cache&) = delete;
        KBPM_Cache& operator=(const KBPM_Cache&) = delete;

        void Insert(SequenceNumber key, KeyBlockPrimaryMeta* kbpm) {
            if (kbpm->IsCache()) return;
            _lock.Lock();
            kbpm->SetCache();
            _data.push_back(std::make_pair(key, kbpm));
            _cum_size ++;
            _insert_count++;
            _lock.Unlock();
        }

        void Evict(Manifest* mf)
        {
            KBPMList  tmp_data;
            KBPMList  ref_data;
            int64_t evicted_size = 0;
            int64_t reclaim_size = 0;
            if (_cum_size <  _max_size ) return;
            _lock.Lock();
            _data.swap(tmp_data);
            reclaim_size = _cum_size - _low_bound;
            _lock.Unlock();
#ifdef CODE_TRACE
            size_t old_count = _evict_count;
            printf("< KBPM reclaim size %ld: - ", reclaim_size);
#endif
            for (auto it = tmp_data.cbegin(); it != tmp_data.cend() && reclaim_size > 0 ; ) {
                KeyBlockPrimaryMeta* kbpm = it->second;
                assert(kbpm);
                if (!kbpm->EvictKBPMFromCache(mf)) {
                    ref_data.push_back(std::make_pair(it->first, kbpm));
                    it = tmp_data.erase(it);
                } else {
                    _evict_count++;
                    evicted_size++;
                    reclaim_size--;
                    it = tmp_data.erase(it);
                    kbpm->ClearCache();
                    mf->FreeKBPM(kbpm);
                }
            }
#ifdef CODE_TRACE
            printf(" evicted_size %ld - evicted_count %ld >\n", evicted_size, _evict_count - old_count);
#endif
            _lock.Lock();
            _cum_size -= evicted_size;
            _data.splice(_data.cbegin(), tmp_data);
            _data.splice(_data.cend(), ref_data);
            _lock.Unlock();
        }


        void Clear(Manifest *mf) noexcept
        {
            _lock.Lock();
            const size_t old_size = _data.size();
            if (!_data.empty()) {
                for (auto it = _data.cbegin(); it != _data.cend(); ) {
                    KeyBlockPrimaryMeta* kbpm = it->second;
                    assert(kbpm);
                    mf->RemoveKBPMFromHashMap(it->first);
                    kbpm->ClearCache();
                    mf->FreeKBPM(kbpm);
                    it = _data.erase(it);
                }
            }
            //_evict_count -= old_size;
            _cum_size = 0;
            _lock.Unlock();
        }

        size_t insert_count()   const noexcept { return _insert_count; }
        size_t evict_count()    const noexcept { return _evict_count; }
        size_t max_size()       const noexcept { return _max_size; }
        size_t size()           const noexcept { return _data.size(); }
        bool empty()            const noexcept { return _data.empty(); }


        std::string to_string() const
        {
            std::stringstream KBPM_cache_string;

            KBPM_cache_string << "KBPM_cache: { address: " << this << ", max_size: " << max_size() <<
                ", size: " << size() << ", insert_count: " << insert_count() << ", evict_count: " << evict_count() << " }\n";
            return KBPM_cache_string.str();
        }
    };



    class LRU_Cache
    {
        typedef std::list<std::pair<SequenceNumber, KeyBlock*>>     KBList;
        typedef folly::ConcurrentHashMap<SequenceNumber, KeyBlock*> KBHashmap;

        size_t                      _max_size;
        KBList                      _data;
        KBHashmap                   *_keys;

        std::atomic<size_t>         _hit_count;
        std::atomic<size_t>         _miss_count;
        std::atomic<size_t>         _ref_hit_count;
        std::atomic<size_t>         _ref_miss_count;
        std::atomic<size_t>         _insert_count;
        std::atomic<size_t>         _evict_count;
        size_t                      _cum_size;
        size_t                      _low_bound;
        Spinlock                    _lock;
        public:

        explicit LRU_Cache(size_t max_size)
            : _max_size(max_size),
            _hit_count(0),
            _miss_count(0),
            _ref_hit_count(0),
            _ref_miss_count(0),
            _insert_count(0),
            _evict_count(0),
            _cum_size(0),
            _low_bound(0),
            _lock()
        {
            assert(max_size);
            _low_bound = (max_size - (max_size /8u));
            _keys = new KBHashmap(1024*1024);
        }

        ~LRU_Cache()
        {
            delete _keys;
        }





        LRU_Cache(const LRU_Cache&) = delete;


        LRU_Cache& operator=(const LRU_Cache&) = delete;


        bool Insert(SequenceNumber key, KeyBlock* value)
        {
            auto it = _keys->insert(key, value);
            if (it.second) {
                _lock.Lock();
                _data.push_back(std::make_pair(key, value));
                _cum_size += value->GetSize();
                _lock.Unlock();
                _insert_count.fetch_add(1, std::memory_order_relaxed);
            }
            return it.second;
        }

        bool InsertHashOnly(SequenceNumber key, KeyBlock* value)
        {
            auto it = _keys->insert(key, value);
            if (it.second) {
                _insert_count.fetch_add(1, std::memory_order_relaxed);
            }
            return it.second;
        }

        void RemoveHashOnly(SequenceNumber key) {
            _keys->erase(key);
            _evict_count.fetch_add(1, std::memory_order_relaxed);
        }

        void AppendDataOnly(SequenceNumber key, KeyBlock* value) {
            _lock.Lock();
            _data.push_back(std::make_pair(key, value));
            _cum_size += value->GetSize();
            _lock.Unlock();
        }

        KeyBlock* Find(SequenceNumber key)
        {
            auto it = _keys->find(key);
            if (it == _keys->end())
            {
                _miss_count.fetch_add(1, std::memory_order_relaxed);
                return nullptr;
            }
            assert(it->second);
            it->second->IncKBRefCnt();
            it->second->SetReference();
            _hit_count.fetch_add(1, std::memory_order_relaxed);

            return it->second;
        }

        KeyBlock* FindWithoutRef(SequenceNumber key)
        {
            auto it = _keys->find(key);
            if (it == _keys->end())
            {
                _miss_count.fetch_add(1, std::memory_order_relaxed);
                return nullptr;
            }
            assert(it->second);
            it->second->SetReference();
            _hit_count.fetch_add(1, std::memory_order_relaxed);

            return it->second;
        }


        KeyBlock* Reference(SequenceNumber key)
        {
            auto it = _keys->find(key);
            if (it == _keys->end())
            {
                _ref_miss_count.fetch_add(1, std::memory_order_relaxed);
                return nullptr;
            }
            assert(it->second);
            it->second->SetReference();
            _ref_hit_count.fetch_add(1, std::memory_order_relaxed);
            return it->second;
        }

        void Evict(Manifest* mf)
        {
            KBList  tmp_data;
            KBList  ref_data;
            int64_t evicted_size = 0;
            int64_t reclaim_size = 0;
            if (_cum_size <  _max_size ) return;
            _lock.Lock();
            _data.swap(tmp_data);
            reclaim_size = _cum_size - _low_bound;
            _lock.Unlock();
#ifdef CODE_TRACE
            size_t old_count = _evict_count.load(std::memory_order_relaxed);
            printf("< reclaim size %ld: - ", reclaim_size);
#endif
            for (auto it = tmp_data.cbegin(); it != tmp_data.cend() && reclaim_size > 0 ; ) {
                KeyBlock* kb = it->second;
                assert(kb);
                if (kb->ClearReference()) { /* has reference before */
                    ref_data.push_back(std::make_pair(it->first, kb));
                    it = tmp_data.erase(it);
                } else {
                    kb->SetEvicting();
                    mf->ClearKBPad(kb);
                    _evict_count.fetch_add(1, std::memory_order_relaxed);
                    evicted_size += kb->GetSize();
                    reclaim_size -= kb->GetSize();
                    _keys->erase(it->first);
                    it = tmp_data.erase(it);
                    mf->FreeKB(kb);
#ifdef MEM_TRACE
                    mf->DecKBPM_MEM_Usage(kb->GetSize());
#endif
                }
            }
#ifdef CODE_TRACE
            printf(" evicted_size %ld - evicted_count %ld >\n", evicted_size, _evict_count.load(std::memory_order_relaxed) - old_count);
#endif
            _lock.Lock();
            _cum_size -= evicted_size;
            _data.splice(_data.cbegin(), tmp_data);
            _data.splice(_data.cend(), ref_data);
            _lock.Unlock();
        }


        void Clear(Manifest *mf) noexcept
        {
            _lock.Lock();
            const size_t old_size = size();
            if (!_keys->empty()) {
                for (auto it = _keys->cbegin(); it != _keys->cend(); ) {
                    KeyBlock *kb = it->second;
                    assert(kb);
                    kb->SetEvicting();
                    mf->ClearKBPad(kb);
                    it = _keys->erase(it);
                    mf->FreeKB(kb);
                }
            }
            _data.clear();
            //_evict_count.fetch_add(old_size, std::memory_order_relaxed);
            _cum_size = 0;
            _lock.Unlock();
        }


        size_t hit_count()      const noexcept { return _hit_count.load(std::memory_order_relaxed); }
        size_t miss_count()     const noexcept { return _miss_count.load(std::memory_order_relaxed); }
        size_t ref_hit_count()  const noexcept { return _ref_hit_count.load(std::memory_order_relaxed); }
        size_t ref_miss_count() const noexcept { return _ref_miss_count.load(std::memory_order_relaxed); }
        size_t insert_count()   const noexcept { return _insert_count.load(std::memory_order_relaxed); }
        size_t evict_count()    const noexcept { return _evict_count.load(std::memory_order_relaxed); }


        size_t max_size()       const noexcept { return _max_size; }
        size_t size()           const noexcept { return _keys->size(); }
        bool empty()            const noexcept { return _keys->empty(); }


        std::string to_string() const
        {
            std::stringstream lru_cache_string;

            lru_cache_string << "lru_cache: { address: " << this << ", max_size: " << max_size() <<
                ", size: " << size() << ", hit_count: " << hit_count() << ", miss_count: " << miss_count() <<
                ", reference hit_count: " << ref_hit_count() << ", reference miss_count: " << ref_miss_count() <<
                ", insert_count: " << insert_count() << ", evict_count: " << evict_count() << " }\n";

            return lru_cache_string.str();
        }
    };


#define UV_BUF_SIZE_SHIFT 5
#define UV_BUF_SIZE_MASK 0x000000000000FFFF
#define PINMASK 0x0FFFFFFF
#define UVAlign(s)  P2ROUNDUP((s), 1ULL << UV_BUF_SIZE_SHIFT)

    class UserValue { //16 Byte
        private:
            /* Maximum Buffer size is 2MB(21bit)
             *  => 32Byte Alignment(16 bit) */
            /* | 6 Byte = buffer address || 2 Byte = size | */
            char* buf_;
            uint32_t buf_size_;
            uint32_t value_size_;// max 21 bit
            /*
             * First 4 bits are used for flags
             * the least 28 bits are for pin.
             */
            std::atomic<int16_t> pinned_;
            ObjectPoolShard<UserValue>* alloc_;

        public:
            UserValue(uint32_t size, ObjectPoolShard<UserValue>* alloc) :
                buf_(NULL), buf_size_(0), value_size_(0), pinned_(1), alloc_(alloc) {
                if (size) {
                    size = UVAlign(size);
                    buf_ = static_cast<char*>(alloc_->AllocBuf(size));
                    buf_size_ = size;
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
                if (buf_) {
                    alloc_->FreeBuf(buf_, buf_size_);
                    buf_ = nullptr;
                }
            }

            void InitUserValue(const Slice& val, const Slice& key, uint8_t col_id, uint64_t ttl) {
                char* dst = buf_;
                value_size_ = val.size();

                assert(pinned_ == 1);
                assert(buf_size_ >= (val.size() + key.size() + 1/*sizeof(col_id)*/ + (ttl?8:0)));
                assert(val.size() <= 1ULL << 21);

                /* Copy "User value" + "User Key" + "Column ID" + ("TTL") for data compression in BuildKeyBlock() */
                memcpy((void*)dst, val.data(), val.size());
                dst += val.size();
                memcpy((void*)dst, key.data(), key.size());
                dst += key.size();
                memcpy((void*)dst, &col_id, 1);
                dst += 1;/* size of col id */
                if (ttl)
                    memcpy((void*)dst, &ttl, sizeof (ttl));
            }

            unsigned Realloc(unsigned size) {
                unsigned sz = 0;
                unsigned aligned_sz = UVAlign(size);

                if (aligned_sz > buf_size_) {
                    sz = aligned_sz - buf_size_;
                    alloc_->FreeBuf(buf_, buf_size_);
                    buf_ = static_cast<char*>(alloc_->AllocBuf(aligned_sz));
                    buf_size_ = aligned_sz;
                }

                return sz;
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
                assert(value_size_ + key_size <= buf_size_);
                return Slice((buf_ + value_size_), key_size);
            }
            Slice GetValueSlice() { return Slice(buf_, value_size_); }
            uint32_t GetValueSize() { return value_size_; }
            char* GetBuffer() { return buf_; }

            void CleanUp() {;}
            void* GetAllocator() { return alloc_; }

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
            Iterator* CreateNewIterator(uint16_t col_id, const ReadOptions options);
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
        KeyBlock* iter_kb;
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
            iter_kb = nullptr;
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
        void CleanupKB(Manifest* mf) {
            if (iter_kb) {
                if (iter_kb->IsDummy()) mf->RemoveDummyKB(iter_kb);
                mf->FreeKB(iter_kb);
            }
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

            SnapshotIterator(Manifest* mf,  SnapshotImpl *snap, uint16_t col_id = 0, bool nio = false, Slice lkey = 0, Slice skey = 0) :
                mf_(mf),
                snap_(snap),
                column_id_(col_id),
                non_blocking_(nio),
                start_key_(skey.data(),
                skey.size()),
                cur_{0},
                last_key_(lkey.data(), lkey.size()),
                comparator_(const_cast<Comparator*>(mf_->GetComparator())),
                last_kb_ikey_seq_(0),
                prefix_size_(0),
                prefix_{},
                iter_history_(0), prefetch_window_shift_(0), prefetch_scanned_queue_(), prefetched_skt_(nullptr), free_pf_node_(), pf_hit_history_(0), kb_(NULL)
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
            bool non_blocking_;
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
            mutable KeyBlock* kb_;
            /* prefix optimization */
            mutable uint16_t prefix_size_;
            mutable char prefix_[16];

            //No copying allowed
            SnapshotIterator(const SnapshotIterator &);
            void operator=(const SnapshotIterator &);
    };
} // namespace insdb
#endif //STORAGE_INSDB_INTERNAL_H_
