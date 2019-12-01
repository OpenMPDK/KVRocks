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

#ifndef STORAGE_INSDB_DB_DB_IMPL_H_
#define STORAGE_INSDB_DB_DB_IMPL_H_

#include <deque>
#include <queue>
#include <set>
#include <list>
#include <stdint.h>
#include "db/insdb_internal.h"
#include "insdb/db.h"
#include "insdb/env.h"
#include "insdb/kv_trace.h"
#include "insdb/write_batch.h"
#include "insdb/merge_operator.h"
//#include "db/write_batch_internal.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include <folly/MPMCQueue.h>
#include <folly/detail/MPMCPipelineDetail.h>
//#include <folly/concurrency/ConcurrentHashMap.h>

namespace insdb {
    using namespace folly;

    struct CallbackReq
    {
        public:
            Slice UserKey_;
            UserValue *UserValue_;
            uint64_t Sequence_;
            RequestType Type_;
            uint16_t col_id_;
            bool new_key_;
            DBImpl *DBImpl_;
            Manifest *mf_;

            CallbackReq(Slice &UserKey, UserValue *UserValue, RequestType Type, uint64_t Sequence, uint16_t col_id, bool new_key, DBImpl *DBImpl, Manifest *mf)
                : UserKey_(UserKey), UserValue_(UserValue), Sequence_(Sequence), Type_(Type), col_id_(col_id), new_key_(new_key), DBImpl_(DBImpl), mf_(mf) { }
            ~CallbackReq();
    };

    class WriteBatchInternal : public WriteBatch::Handler {
        private:
            DBImpl *dbimpl_;
            Manifest *mf_;
            Transaction trxn_;
            uint64_t ttl_;
            void Store(const KeySlice& key, const Slice& value, const uint16_t col_id, RequestType type );
        public:
            WriteBatchInternal(DBImpl *dbimpl, Manifest *mf, int32_t count, uint64_t ttl);
            void InitWBI(DBImpl *dbimpl, Manifest *mf, int32_t count, uint64_t ttl);
            void Put(const Slice& key, const Slice& value, const uint16_t col_id);
            Status Merge(const Slice& key, const Slice& value, const uint16_t col_id);
            void Delete(const Slice& key, const uint16_t col_id);
    };

    /* WBIBuffer : Per Thread Memory for WriteBatchInternal  */
    class WBIBuffer{
        private:
            pthread_key_t pool_id_;
            static void DestroyBuffer(void *arg){
                WriteBatchInternal *wbi = reinterpret_cast<WriteBatchInternal*>(arg);
                delete wbi;
            }
        public:
            explicit WBIBuffer() { 
                if(pthread_key_create(&pool_id_, DestroyBuffer)) abort(); 
            } 
            ~WBIBuffer(){
                if (pthread_key_delete(pool_id_)) abort();
            }
            WriteBatchInternal* GetWBIBuffer(DBImpl *dbimpl, Manifest *mf, int32_t count, uint64_t ttl);
    };

    /** @ DBImpl class */
    /**
      It provides DB interface to user.
      */
    class DBImpl : public DB {
        public:
            DBImpl(const Options& options, const std::string& dbname);
            ~DBImpl();
            /** Implementations of the DB interface */

            /**
              @brief store key-value pair in database.
              @param  in  options  WriteOptins&  write option.
              @param  in  key Slice&.
              @param  in  value Slice&.
              @return Status::OK() if success, else error reason.

Note: Given key-value pair passes to internal write function through WriteBatch
class.
*/
            virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);

            /**
              @brief delete key-value pair from database.
              @param  in  options  WriteOptins&  write option.
              @param  in  key Slice&.
              @return Status::OK() if success, else error reason.

Note: Deleting for unsaved key does not report error, it is allowed action.
Given key passes to internal write function through WriteBatch.
*/
            virtual Status Delete(const WriteOptions&, const Slice& key);

            /**
              @brief parse WriteBatch store key-value pairs in database.
              @param  in  options  WriteOptins&  write option.
              @param  in  update  WriteBatch*.
              @return Status::OK() if success, else error reason.

              WriteBatch is kind of container which can deliver put/delete
              key-value pairs to internal database process.
              */
            virtual Status Write(const WriteOptions& options, WriteBatch* updates);

            /**
              @brief retrieve key-value pair from database.
              @param  in  options  ReadOptions&  read option.
              @param  in  key Slice&.
              @param  out  value std::string *.
              @return Status::OK() if success, else error reason.

Note: Value string will be located in "value" if success.
*/
            virtual Status Get(const ReadOptions& options,
                    const Slice& key, std::string* value, uint16_t col_id = 0, PinnableSlice* pin_slice=NULL);

            virtual Status Get(const ReadOptions& options,
                    const Slice& key, PinnableSlice* value, uint16_t col_id = 0) override;

            virtual Status CompactRange(const Slice *begin, const Slice *end, const uint16_t col_id = 0);

            virtual Status Flush(const FlushOptions& options, uint16_t col_id = 0) override;


            /**
              @brief retrieve latest key-value sequence nubmer from DB.
              @param  in  key Slice&.
              @param  in  col_id column id.
              @param  out sequence lastest key-value sequence.
              @param  out found result of key
              */
            virtual Status GetLatestSequenceForKey(const Slice& key, uint64_t* sequence, bool* found, uint8_t col_id=0);

            /**
              @brief The sequence number of the most recent transaction.
              */
            virtual uint64_t GetLatestSequenceNumber() const override;

            /**
              @brief create new iterator for database.
              @param  in  options ReadOptions&
              @return Iterator* if success, else NULL.

Note: Iterator support API, which navigates all key ranges.
It provides database image in calling time. So put/delete operations, which
issues afater this method call will not be covered by Iterator.
*/
            virtual Iterator* NewIterator(const ReadOptions& options, uint16_t col_id = 0);

            /**
              @brief create snapshot for database.
              @return Snapshot * if success, else NULL

              Snapshot provide database image in calling time. Same as Iterator, It will
              not cover put/delete operation which are issued after this method call.
              */
            virtual const Snapshot* GetSnapshot();

            /**
              @brief release snapshot from database.
              @param  in snapshot Snapshot*

              This function release snapshot resouce from database.
              */
            virtual void ReleaseSnapshot(const Snapshot* snapshot);
            WriteBatchInternal* GetWBIBuffer(int32_t count, uint64_t ttl){
                return wbi_buffer_.GetWBIBuffer(this, mf_, count, ttl);
            }

            /**
              @brief get proverty information from database.
              @param  in  property  Slice&
              @param  out value std::string*
              @return true if success, else false.

Note: It reports proverty value which supported by InSDB.
*/
            virtual bool GetProperty(const Slice& property, std::string* value);

            /**
              @brief report space usages of given #n ranges.
              @param  in  range Range*
              @param  in  n int (#of ranges)
              @param  out sizes uint64_t  (size for each ranges)

Note: It provides size information for give #n ranges. assume sizes are
pointer for uint64_t[] array.
*/
            virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes, uint16_t col_id = 0);

            //   void PutColumnNodeToRequestQueue(RequestNode*);
            void PutColumnNodeToRequestQueue(RequestNode *req_node/*, SKTableMem *skt*/);

            void BuildKeyBlockMeta(int worker_id, std::list<RequestNode*> &submit_list, KeyBlockPrimaryMeta* kbml, uint8_t total_req_count);
            uint32_t BuildKeyBlock(int , std::list<RequestNode*> &, KeyBlock*, KeyBlockPrimaryMeta*, uint8_t, char *, uint32_t, uint8_t);
            void WorkerThread();

            static void WorkerWrapper(void* db)
            { reinterpret_cast<DBImpl*>(db)->WorkerThread(); }

            void CallbackThread();
            static void CallbackSender(void* db)
            { reinterpret_cast<DBImpl*>(db)->CallbackThread(); }

            /**
              Create NewDB, it must be called when need to New DB inserted.
              */
            Status NewDB();

            Status DeleteDB();

            /**
              Recover DB(), it read DB meta and build internal on memory data structure for DB.
              */
            Status Recover(const Options& options);

            bool Is_DB_exist() { return db_exist_; }
            void SetNormalStart() { normal_start_ = true; }
            Status ValidateOption();
            bool CallMergeOperator(const Slice& key,
                    const Slice* existing_value,
                    const Slice& value,
                    std::string* new_value)
            {
                if(options_.merge_operator == nullptr)
                    return false;
                return options_.merge_operator->Merge(key, existing_value, value, new_value, options_.info_log);
            }
            inline bool CallbackAvailable() { return options_.AddUserKey != nullptr; }
            void CallAddUserKey(
                    UserKey *key,
                    UserValue *value,
                    RequestType type,
                    uint64_t seq,
                    uint16_t col_id,
                    bool new_key);

            virtual void CancelAllBackgroundWork(bool wait);

            Status SetTtlList(uint64_t* TtlList, bool* TtlEnabledList) override {
              return mf_->SetTtlList(TtlList, TtlEnabledList);
            }
            Status SetTtlInTtlList(uint16_t id, uint64_t ttl) override {
              return mf_->SetTtlInTtlList(id, ttl);
            }
            uint64_t GetTtlFromTtlList(uint16_t id) override { return mf_->GetTtlFromTtlList(id); }
            bool TtlIsEnabled(uint16_t id) override { return mf_->TtlIsEnabled(id); }

        private:
            friend class DB;
            // No copying allowed
            DBImpl(const DBImpl&);
            void operator=(const DBImpl&);

            /**
              Member variables....
              */
            Options options_;
            Env* const env_;
            std::string dbname_;  /** store database name */
            Manifest *mf_;        /** manifest pointer, used reference key-map. insdb_internal.h */
            WBIBuffer wbi_buffer_; /* WriteBatchInternal buffer */
            std::list<SnapshotImpl *> snaplist_;  /** store snapshot list */
            uint64_t* TtlList_;

            /**
             * Slowdown when all workers are woring on flush
             */
            bool slowdown_;
            port::Mutex slowdown_mu_;
            port::CondVar slowdown_cv_;

            /**
             * for Worker and pending request queue
             */
            std::queue<RequestNode*> pending_req_;

            boost::lockfree::queue<CallbackReq *> pending_callbacks_;
            std::atomic<uint32_t> nr_pending_callbacks_;
            port::Mutex callback_mu_;
            port::CondVar callback_cv_;
            uint64_t *callback_cf_flush_timers_;
            uint64_t *callback_file_sizes_;
            port::CondVar callback_shutdown_cv_;
            uint16_t callback_pending_flush_timers_;
            bool bgwork_shutting_down_;

            bool shutting_down_;
            bool db_exist_;
            bool normal_start_;
#ifdef KV_TIME_MEASURE
            std::atomic<uint64_t> prefetch_hit_;
            std::atomic<uint64_t> cache_hit_;
            std::atomic<uint64_t> cache_miss_;
#endif
    };

    /**
      @brief Destroy the contents of the specified database.
      @param  in name  std::string&   name of database.
      @param  in options Options&     options for database.
      @return Status::OK().

Note:Be very careful using this method.
For backwards compatibility, if DestroyDB is unable to list the
database files, Status::OK() will still be returned masking this failure.
*/
    Status DestroyDB(const std::string& name,
            const Options& options);
    /**
      @brief Repair broken database.
      @param  in  dbname  std::string name of databse.
      @param  in options Options&     options for database.
      @return Status::OK() if success, else error reason.

      If a DB cannot be opened, you may attempt to call this method to
      resurrect as much of the contents of the database as possible.
      Some data may be lost, so be careful when calling this function
      on a database that contains important information.
      */
    Status RepairDB(const std::string& dbname,
            const Options& options);

    extern Options SanitizeOptions(const Options& src, const std::string& dbname);

}  // namespace insdb

#endif  // STORAGE_INSDB_DB_DB_IMPL_H_
