//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/insdb_wrapper.h"
#include "rocksdb/db.h"
#include "db/version_edit.h"
#include "db/column_family.h"
#include "db/job_context.h"
#include "db/write_callback.h"
#include "db/snapshot_impl.h"

class insdb_t;

namespace rocksdb {

class DBImpl : public DB {
public:

  DBImpl(const DBOptions& db_options, const std::string& dbname);
  virtual ~DBImpl();

  // Persist and retrieve manifest
  static const char kKvdbMeta_Manifest[];
  static const char kKvdbMeta_Statistics[];
  static const char kKvdbMeta_Manifest_Signature[];
  static const char kKvdbMeta_ManifestVersion = 1;

  struct Manifest_ColumnFamily {
      uint32_t ID;
      uint32_t ttl;
      std::string name;
      std::string comparator;
  };

  struct Manifest {
      Manifest() : manifest_version(0), next_file_number(0), cf_count(0) {}
      uint8_t manifest_version;
      uint64_t next_file_number;
      uint32_t cf_count;
      std::unordered_map<std::string, Manifest_ColumnFamily> column_families;
  };

  Status WriteManifest(Manifest &manifest);
  Status ReadManifest(Manifest &manifest);

  Status OpenImpl(const std::vector<ColumnFamilyDescriptor>& column_families,
          std::vector<ColumnFamilyHandle*>* handles);

  Status AllocateColumnFamilyID(uint32_t& cf_id);
  ColumnFamilyHandleImpl* FindColumnFamilyID(uint32_t cf_id);

  virtual Status SetColumnFamilyTtl(ColumnFamilyHandle* column_family, uint32_t ttl) override;
  Status SetColumnFamilyTtlImpl(ColumnFamilyHandle* column_family, uint32_t ttl);

  Status CreateColumnFamilyImpl(Manifest& manifest, const ColumnFamilyOptions& options,
                                    const std::string& column_family_name,
                                    ColumnFamilyHandle** handle);

  virtual Status CreateColumnFamily(const ColumnFamilyOptions& options,
                                    const std::string& column_family_name,
                                    ColumnFamilyHandle** handle) override;
  virtual Status CreateColumnFamilies(
      const ColumnFamilyOptions& cf_options,
      const std::vector<std::string>& column_family_names,
      std::vector<ColumnFamilyHandle*>* handles) override;
  virtual Status CreateColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles) override;

  Status DropColumnFamilyImpl(Manifest& manifest, ColumnFamilyHandle* column_family);
  virtual Status DropColumnFamily(ColumnFamilyHandle* column_family) override;
  virtual Status DropColumnFamilies(const std::vector<ColumnFamilyHandle*>& column_families) override;

  using DB::Write;
  virtual Status Write(const WriteOptions& options,
                       WriteBatch* updates) override;
  using DB::Get;
  virtual Status Get(const ReadOptions& options,
                     const Slice& key, 
                     std::string* value) override;

  using DB::Delete;
  virtual Status Delete(const WriteOptions& options, const Slice& key) override;

  using DB::GetName;
  virtual const std::string& GetName() const override;

  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options) {   return NewIterator(options, default_cf_handle_); }

  const SnapshotList& snapshots() const { return snapshots_; }

  const ImmutableDBOptions& immutable_db_options() const {
    return immutable_db_options_;
  }

  void CancelAllBackgroundWork(bool wait);

  // Version classes are not applicable to InSDB
#if 0
  // Find Super version and reference it. Based on options, it might return
  // the thread local cached one.
  // Call ReturnAndCleanupSuperVersion() when it is no longer needed.
  SuperVersion* GetAndRefSuperVersion(ColumnFamilyData* cfd)
  {
      return &superversion_;
  }

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.
  SuperVersion* GetAndRefSuperVersion(uint32_t column_family_id)
  {
      throw std::runtime_error("Not supported API");
  }
#endif

  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into the current
  // memtables. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  //
  // If include_history=true, will also search Memtables in MemTableList
  // History.
  SequenceNumber GetEarliestMemTableSequenceNumber(ColumnFamilyHandle* cfh,
                                                   bool include_history)
  {
      return kMaxSequenceNumber;
  }

  // For a given key, check to see if there are any records for this key
  // in the memtables, including memtable history.  If cache_only is false,
  // SST files will also be checked.
  //
  // If a key is found, *found_record_for_key will be set to true and
  // *seq will be set to the stored sequence number for the latest
  // operation on this key or kMaxSequenceNumber if unknown.
  // If no key is found, *found_record_for_key will be set to false.
  //
  // Note: If cache_only=false, it is possible for *seq to be set to 0 if
  // the sequence number has been cleared from the record.  If the caller is
  // holding an active db snapshot, we know the missing sequence must be less
  // than the snapshot's sequence number (sequence numbers are only cleared
  // when there are no earlier active snapshots).
  //
  // If NotFound is returned and found_record_for_key is set to false, then no
  // record for this key was found.  If the caller is holding an active db
  // snapshot, we know that no key could have existing after this snapshot
  // (since we do not compact keys that have an earlier snapshot).
  //
  // Returns OK or NotFound on success,
  // other status on unexpected error.
  // TODO(andrewkr): this API need to be aware of range deletion operations
  Status GetLatestSequenceForKey(ColumnFamilyHandle* cfh, const Slice& key,
                                 bool cache_only, SequenceNumber* seq,
                                 bool* found_record_for_key,
                                 bool* is_blob_index = nullptr);

#if 0
  // Un-reference the super version and return it to thread local cache if
  // needed. If it is the last reference of the super version. Clean it up
  // after un-referencing it.
  void ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd, SuperVersion* sv)
  {
      throw std::runtime_error("Not supported API");
  }

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread.
  void ReturnAndCleanupSuperVersion(uint32_t colun_family_id, SuperVersion* sv);
#endif

protected:
  MutableDBOptions mutable_db_options_;
  ImmutableDBOptions immutable_db_options_;
  insdb::Options insdb_options_;
  // The options to access storage files
  const EnvOptions env_options_;

  Env* const env_;
  Statistics* stats_;
  const std::string dbname_;
  ColumnFamilyHandleImpl* default_cf_handle_;
  SnapshotList snapshots_;
  std::unordered_map<std::string, ColumnFamilyData*> column_families_;
  std::unordered_map<uint32_t, ColumnFamilyData*> column_family_ids_;

//////////////////////////////////////
/// not implemented APIs listed below
//////////////////////////////////////
public:
  using DB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) override;

  using DB::Delete;
  virtual Status Delete(const WriteOptions& options,
                        ColumnFamilyHandle* column_family,
                        const Slice& key) override;

  using DB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key) override;

  virtual Status DeleteRange(const WriteOptions& options,
                             ColumnFamilyHandle* column_family,
                             const Slice& begin_key, const Slice& end_key) override;

  using DB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override;


  using DB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     std::string* value) override;

  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;

  using DB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys, std::vector<std::string>* values) override;

  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;

  using DB::NewIterators;
  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators) override;

  const Snapshot* GetSnapshotImpl(bool is_write_conflict_boundary);

  using DB::GetSnapshot;
  virtual const Snapshot* GetSnapshot() override;

  using DB::ReleaseSnapshot;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;

  using DB::GetProperty;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const Slice& property, std::string* value) override;

  using DB::GetMapProperty;
  virtual bool GetMapProperty(ColumnFamilyHandle* column_family,
                              const Slice& property,
                              std::map<std::string, std::string>* value) override;

  using DB::GetIntProperty;
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const Slice& property, uint64_t* value) override;

  using DB::GetAggregatedIntProperty;
  virtual bool GetAggregatedIntProperty(const Slice& property,
                                        uint64_t* value) override;

  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(ColumnFamilyHandle* column_family,
                                   const Range* range, int n, uint64_t* sizes,
                                   uint8_t include_flags
                                   = INCLUDE_FILES) override;

  using DB::GetApproximateMemTableStats;
  virtual void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                           const Range& range,
                                           uint64_t* const count,
                                           uint64_t* const size) override;

  using DB::CompactRange;
  virtual Status CompactRange(const CompactRangeOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end) override;

  using DB::SetDBOptions;
  virtual Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& new_options) override;

  using DB::CompactFiles;
  virtual Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names,
      const int output_level, const int output_path_id = -1) override;

  using DB::PauseBackgroundWork;
  virtual Status PauseBackgroundWork() override;

  using DB::ContinueBackgroundWork;
  virtual Status ContinueBackgroundWork() override;

  using DB::EnableAutoCompaction;
  virtual Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override;

  using DB::NumberLevels;
  virtual int NumberLevels(ColumnFamilyHandle* column_family) override;

  using DB::MaxMemCompactionLevel;
  virtual int MaxMemCompactionLevel(ColumnFamilyHandle* column_family) override;

  using DB::Level0StopWriteTrigger;
  virtual int Level0StopWriteTrigger(ColumnFamilyHandle* column_family) override;

  using DB::GetEnv;
  virtual Env* GetEnv() const override;

  using DB::GetOptions;
  virtual Options GetOptions(ColumnFamilyHandle* column_family) const override;

  using DB::GetDBOptions;
  virtual DBOptions GetDBOptions() const override;

  using DB::Flush;
  virtual Status Flush(const FlushOptions& options,
                       ColumnFamilyHandle* column_family) override;

  using DB::SyncWAL;
  virtual Status SyncWAL() override;

  virtual Status FlushWAL(bool sync);

  using DB::GetLatestSequenceNumber;
  virtual SequenceNumber GetLatestSequenceNumber() const override;

#ifndef ROCKSDB_LITE
  using DB::DisableFileDeletions;
  virtual Status DisableFileDeletions() override;

  using DB::EnableFileDeletions;
  virtual Status EnableFileDeletions(bool force = true) override;

  using DB::GetLiveFiles;
  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true) override;

  using DB::GetSortedWalFiles;
  virtual Status GetSortedWalFiles(VectorLogPtr& files) override;

  using DB::GetUpdatesSince;
  virtual Status GetUpdatesSince(
      SequenceNumber seq_number, unique_ptr<TransactionLogIterator>* iter,
      const TransactionLogIterator::ReadOptions&
          read_options = TransactionLogIterator::ReadOptions()) override;

// Windows API macro interference
#undef DeleteFile
  using DB::DeleteFile;
  virtual Status DeleteFile(std::string name) override;
  Status DeleteFilesInRange(ColumnFamilyHandle* column_family,
                            const Slice* begin, const Slice* end);

  using DB::IngestExternalFile;
  virtual Status IngestExternalFile(
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& external_files,
      const IngestExternalFileOptions& options) override;

  virtual Status VerifyChecksum();

  using DB::GetDbIdentity;
  virtual Status GetDbIdentity(std::string& identity) const override;

  using DB::DefaultColumnFamily;
  virtual ColumnFamilyHandle* DefaultColumnFamily() const override;

  using DB::GetPropertiesOfAllTables;
  virtual Status GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                          TablePropertiesCollection* props) override;

  using DB::GetPropertiesOfTablesInRange;
  virtual Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      TablePropertiesCollection* props) override;

  // Returns the list of live files in 'live' and the list
  // of all files in the filesystem in 'candidate_files'.
  // If force == false and the last call was less than
  // db_options_.delete_obsolete_files_period_micros microseconds ago,
  // it will not fill up the job_context
  void FindObsoleteFiles(JobContext* job_context, bool force,
                         bool no_full_scan = false)
  {
	   // Do nothing for InSDB because it's not file based
  }

  // Diffs the files listed in filenames and those that do not
  // belong to live files are posibly removed. Also, removes all the
  // files in sst_delete_files and log_delete_files.
  // It is not necessary to hold the mutex when invoking this method.
  void PurgeObsoleteFiles(const JobContext& background_contet,
                          bool schedule_only = false)
  {
      // Do nothing for InSDB because it's not file based
  }

  // hollow transactions shell used for recovery.
  // these will then be passed to TransactionDB so that
  // locks can be reacquired before writing can resume.
  struct RecoveredTransaction {
    uint64_t log_number_;
    std::string name_;
    WriteBatch* batch_;
    // The seq number of the first key in the batch
    SequenceNumber seq_;
    // Number of sub-batched. A new sub-batch is created if we txn attempts to
    // inserts a duplicate key,seq to memtable. This is currently used in
    // WritePrparedTxn
    size_t batch_cnt_;
    explicit RecoveredTransaction(const uint64_t log, const std::string& name,
                                  WriteBatch* batch, SequenceNumber seq,
                                  size_t batch_cnt)
        : log_number_(log),
          name_(name),
          batch_(batch),
          seq_(seq),
          batch_cnt_(batch_cnt) {}

    ~RecoveredTransaction() { delete batch_; }
  };

  std::unordered_map<std::string, RecoveredTransaction*>
      recovered_transactions_;

  bool allow_2pc() const { return immutable_db_options_.allow_2pc; }

  std::unordered_map<std::string, RecoveredTransaction*>
  recovered_transactions() {
      return recovered_transactions_;
  }
  void DeleteAllRecoveredTransactions() {
    for (auto it = recovered_transactions_.begin();
         it != recovered_transactions_.end(); it++) {
      delete it->second;
    }
    recovered_transactions_.clear();
  }
  void MarkLogAsContainingPrepSection(uint64_t log) {
#if 0
      throw std::runtime_error("Not supported API");
#endif
  }
  void MarkLogAsHavingPrepSectionFlushed(uint64_t log) {
#if 0
      throw std::runtime_error("Not supported API");
#endif
  }
  // If disable_memtable is set the application logic must guarantee that the
  // batch will still be skipped from memtable during the recovery. In
  // WriteCommitted it is guarnateed since disable_memtable is used for prepare
  // batch which will be written to memtable later during the commit, and in
  // WritePrepared it is guaranteed since it will be used only for WAL markers
  // which will never be written to memtable.
  // batch_cnt is expected to be non-zero in seq_per_batch mode and indicates
  // the number of sub-patches. A sub-patch is a subset of the write batch that
  // does not have duplicate keys.
  Status WriteImpl(const WriteOptions& options, WriteBatch* updates,
                   WriteCallback* callback = nullptr,
                   uint64_t* log_used = nullptr, uint64_t log_ref = 0,
                   bool disable_memtable = false, uint64_t* seq_used = nullptr,
                   size_t batch_cnt = 0);

  // Function that Get and KeyMayExist call with no_io true or false
  // Note: 'value_found' from KeyMayExist propagates here
  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, PinnableSlice* value,
                 bool* value_found = nullptr, ReadCallback* callback = nullptr,
                 bool* is_blob_index = nullptr)
  {
      throw std::runtime_error("Not supported API");
  }

  // Similar to GetSnapshot(), but also lets the db know that this snapshot
  // will be used for transaction write-conflict checking.  The DB can then
  // make sure not to compact any keys that would prevent a write-conflict from
  // being detected.
  const Snapshot* GetSnapshotForWriteConflictBoundary();

  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.  Return value only valid until next call to this function or
  // mutex is released.
  ColumnFamilyHandle* GetColumnFamilyHandle(uint32_t column_family_id) {
      std::unordered_map<uint32_t,ColumnFamilyData*>::const_iterator cf_ids_itr = column_family_ids_.find(column_family_id);
      if(cf_ids_itr != column_family_ids_.end())
      {
          return nullptr;
      }
      return new ColumnFamilyHandleImpl(cf_ids_itr->second, this, &mutex_);
  }

  void DeleteColumnFamilyHandle(ColumnFamilyHandleImpl *col_handle) {  }

  // Except in DB::Open(), WriteOptionsFile can only be called when:
  // Persist options to options file.
  // If need_mutex_lock = false, the method will lock DB mutex.
  Status WriteOptionsFile(bool need_mutex_lock);

  std::atomic<uint64_t> next_file_number_;
  uint64_t options_file_number_;
  Status DeleteObsoleteOptionsFiles();
  Status RenameTempFileToOptionsFile(const std::string& file_name);
  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1); }

  insdb::DB *GetInSDB() { return insdb_; }
  void SetInSDB(insdb::DB *insdb) { insdb_ = insdb; }
  Logger *GetLogger() { return immutable_db_options_.info_log.get(); }

  InstrumentedMutex* mutex() { return &mutex_; }

  mutable InstrumentedMutex mutex_;

private:
  insdb::DB* insdb_;
  uint32_t max_nr_column_family_;
  std::string metadata_manifest_path;
  SuperVersion superversion_;

#endif  // ROCKSDB_LITE

}; // DB_Impl

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

}  // namespace rocksdb
