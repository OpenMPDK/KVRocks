//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring
//    kTypeDeletion varstring
//    kTypeSingleDeletion varstring
//    kTypeMerge varstring varstring
//    kTypeColumnFamilyValue varint32 varstring varstring
//    kTypeColumnFamilyDeletion varint32 varstring varstring
//    kTypeColumnFamilySingleDeletion varint32 varstring varstring
//    kTypeColumnFamilyMerge varint32 varstring varstring
//    kTypeBeginPrepareXID varstring
//    kTypeEndPrepareXID
//    kTypeCommitXID varstring
//    kTypeRollbackXID varstring
//    kTypeNoop
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "insdb/write_batch.h"
#include "db/insdb_wrapper.h"
#include "rocksdb/write_batch.h"

#include <map>
#include <stack>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/snapshot_impl.h"
#include "db/write_batch_internal.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "rocksdb/merge_operator.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

// anon namespace for file-local types
namespace {

enum ContentFlags : uint32_t {
  DEFERRED = 1 << 0,
  HAS_PUT = 1 << 1,
  HAS_DELETE = 1 << 2,
  HAS_SINGLE_DELETE = 1 << 3,
  HAS_MERGE = 1 << 4,
  HAS_BEGIN_PREPARE = 1 << 5,
  HAS_END_PREPARE = 1 << 6,
  HAS_COMMIT = 1 << 7,
  HAS_ROLLBACK = 1 << 8,
  HAS_DELETE_RANGE = 1 << 9,
};

struct BatchContentClassifier : public WriteBatch::Handler {
  uint32_t content_flags = 0;

  Status PutCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_PUT;
    return Status::OK();
  }

  Status DeleteCF(uint32_t, const Slice&) override {
    content_flags |= ContentFlags::HAS_DELETE;
    return Status::OK();
  }

  Status SingleDeleteCF(uint32_t, const Slice&) override {
    content_flags |= ContentFlags::HAS_SINGLE_DELETE;
    return Status::OK();
  }

  Status DeleteRangeCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_DELETE_RANGE;
    return Status::OK();
  }

  Status MergeCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_MERGE;
    return Status::OK();
  }

  Status MarkBeginPrepare() override {
    content_flags |= ContentFlags::HAS_BEGIN_PREPARE;
    return Status::OK();
  }

  Status MarkEndPrepare(const Slice&) override {
    content_flags |= ContentFlags::HAS_END_PREPARE;
    return Status::OK();
  }

  Status MarkCommit(const Slice&) override {
    content_flags |= ContentFlags::HAS_COMMIT;
    return Status::OK();
  }

  Status MarkRollback(const Slice&) override {
    content_flags |= ContentFlags::HAS_ROLLBACK;
    return Status::OK();
  }
};

}  // anon namespace

struct SavePoints {
  std::stack<SavePoint> stack;
};

// FIX: WriteBatch Memory Limit fix
// Add set max_bytes_
WriteBatch::WriteBatch(size_t reserved_bytes, size_t max_bytes)
    : save_points_(nullptr), content_flags_(0), max_bytes_(max_bytes) {

  wrapper_context = new insdb::WriteBatch();
}
// FIX: end

WriteBatch::WriteBatch(const std::string& rep)
    : save_points_(nullptr),
      content_flags_(ContentFlags::DEFERRED) {

    wrapper_context = new insdb::WriteBatch();
}

WriteBatch::WriteBatch(const WriteBatch& src)
    : save_points_(src.save_points_),
      wal_term_point_(src.wal_term_point_),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)) {

	throw std::runtime_error("Not supported API");
}

WriteBatch::WriteBatch(WriteBatch&& src) noexcept
    : save_points_(std::move(src.save_points_)),
      wal_term_point_(std::move(src.wal_term_point_)),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)) {

	throw std::runtime_error("Not supported API");
}

WriteBatch& WriteBatch::operator=(const WriteBatch& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(src);
  }
  return *this;
}

WriteBatch& WriteBatch::operator=(WriteBatch&& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(std::move(src));
  }
  return *this;
}

WriteBatch::~WriteBatch() { delete save_points_; delete WrapperToInsdbWriteBatch(wrapper_context); }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Handler::LogData(const Slice& blob) {
  // If the user has not specified something to do with blobs, then we ignore
  // them.
}

bool WriteBatch::Handler::Continue() {
  return true;
}

void WriteBatch::Clear() {
	WrapperToInsdbWriteBatch(wrapper_context)->Clear();

  content_flags_.store(0, std::memory_order_relaxed);

  if (save_points_ != nullptr) {
    while (!save_points_->stack.empty()) {
      save_points_->stack.pop();
    }
  }

  wal_term_point_.clear();
}

int WriteBatch::Count() const {
  return WrapperToInsdbWriteBatch(wrapper_context)->Count();
}

const std::string& WriteBatch::Data() const {
  return WrapperToInsdbWriteBatch(wrapper_context)->Data();
}

// Retrieve data size of the batch.
size_t WriteBatch::GetDataSize() const {
  return WrapperToInsdbWriteBatch(wrapper_context)->Data().size();
}

// FIX: WriteBatch Memory Limit fix
// Resize data
void WriteBatch::ResizeData(size_t size) const {
  WrapperToInsdbWriteBatch(wrapper_context)->ResizeData(size);
}
// FIX: end

uint32_t WriteBatch::ComputeContentFlags() const {
  auto rv = content_flags_.load(std::memory_order_relaxed);
  if ((rv & ContentFlags::DEFERRED) != 0) {
    BatchContentClassifier classifier;
    Iterate(&classifier);
    rv = classifier.content_flags;

    // this method is conceptually const, because it is performing a lazy
    // computation that doesn't affect the abstract state of the batch.
    // content_flags_ is marked mutable so that we can perform the
    // following assignment
    content_flags_.store(rv, std::memory_order_relaxed);
  }
  return rv;
}

void WriteBatch::MarkWalTerminationPoint() {
  wal_term_point_.size = GetDataSize();
  wal_term_point_.count = Count();
  wal_term_point_.content_flags = content_flags_;
}

bool WriteBatch::HasPut() const {
  return (ComputeContentFlags() & ContentFlags::HAS_PUT) != 0;
}

bool WriteBatch::HasDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_DELETE) != 0;
}

bool WriteBatch::HasSingleDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_SINGLE_DELETE) != 0;
}

bool WriteBatch::HasDeleteRange() const {
  return (ComputeContentFlags() & ContentFlags::HAS_DELETE_RANGE) != 0;
}

bool WriteBatch::HasMerge() const {
  return (ComputeContentFlags() & ContentFlags::HAS_MERGE) != 0;
}

bool ReadKeyFromWriteBatchEntry(Slice* input, Slice* key, bool cf_record) {
    insdb::Slice insdb_input(input->data(), input->size());
    insdb::Slice insdb_key(key->data(), key->size());
    bool success = insdb::ReadKeyFromWriteBatchEntry(&insdb_input, &insdb_key, cf_record);
    if (success)
    {
        key->data_ = insdb_key.data();
        key->size_ = insdb_key.size();
    }
    return success;
}

bool WriteBatch::HasBeginPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_BEGIN_PREPARE) != 0;
}

bool WriteBatch::HasEndPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_END_PREPARE) != 0;
}

bool WriteBatch::HasCommit() const {
  return (ComputeContentFlags() & ContentFlags::HAS_COMMIT) != 0;
}

bool WriteBatch::HasRollback() const {
  return (ComputeContentFlags() & ContentFlags::HAS_ROLLBACK) != 0;
}

class InsdbWriteBatchHandler : public insdb::WriteBatch::Handler {
public:
	InsdbWriteBatchHandler(WriteBatch::Handler *handler) : rocksdb_handler(handler) {}
    virtual void Put(const insdb::Slice& key, const insdb::Slice& value, const uint16_t col_id = 0) {
    	Slice rocksdb_key(key.data(), key.size());
    	Slice rocksdb_value(value.data(), value.size());
    	rocksdb_handler->PutCF(col_id, rocksdb_key, rocksdb_value);
    }
    virtual insdb::Status Merge(const insdb::Slice& key, const insdb::Slice& value, const uint16_t col_id = 0) {
    	Slice rocksdb_key(key.data(), key.size());
    	Slice rocksdb_value(value.data(), value.size());
    	Status s = rocksdb_handler->MergeCF(col_id, rocksdb_key, rocksdb_value);
    	return convert_to_insdb_status(s);
    }
    virtual void Delete(const insdb::Slice& key, const uint16_t col_id = 0) {
    	Slice rocksdb_key(key.data(), key.size());
    	rocksdb_handler->DeleteCF(col_id, rocksdb_key);
    }
    // FIX: Continue feature fix
    // Continue is called by WriteBatch::Iterate. If it returns false,
    // iteration is halted. Otherwise, it continues iterating. The default
    // implementation always returns true.
    virtual bool Continue() {
        return rocksdb_handler->Continue();
    }
    // FIX: end
private:
    WriteBatch::Handler *rocksdb_handler;
};

Status WriteBatch::Iterate(Handler* handler) const {
	InsdbWriteBatchHandler wrapper_hander(handler);
	insdb::Status s = WrapperToInsdbWriteBatch(wrapper_context)->Iterate(&wrapper_hander);
  return convert_insdb_status(s);
}

int WriteBatchInternal::Count(const WriteBatch* b) {
	return b->Count();
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
    //Feat: For implementing RollbackToSavePoint feature
    //Set the count for WriteBatch
    WrapperToInsdbWriteBatch(b->wrapper_context)->SetCount(n);
    //Feat end
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
	throw std::runtime_error("Not supported API");
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
	throw std::runtime_error("Not supported API");
}

size_t WriteBatchInternal::GetFirstOffset(WriteBatch* b) {
    return WrapperToInsdbWriteBatch(b->wrapper_context)->GetFirstOffset();
}

Status WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                             const Slice& key, const Slice& value) {
  // FIX: WriteBatch Memory Limit fix
  // Record start WriteBatch size and count
  LocalSavePoint save(b);
  // FIX: end
  insdb::Slice insdb_key(key.data(), key.size());
  insdb::Slice insdb_value(value.data(), value.size());
  WrapperToInsdbWriteBatch(b->wrapper_context)->Put(insdb_key, insdb_value, column_family_id);

#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("PUT IN BATCH",column_family_id,key.data(),key.size(),value.size());
#endif

  b->content_flags_.store(
      b->content_flags_.load(std::memory_order_relaxed) | ContentFlags::HAS_PUT,
      std::memory_order_relaxed);

  // FIX: WriteBatch Memory Limit fix
  // commit check if back to start SavePoint
  return save.commit();
  // FIX: end
}

Status WriteBatch::Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) {
  return WriteBatchInternal::Put(this, column_family?column_family->GetID():0, key, value);
}

Status WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                             const SliceParts& key, const SliceParts& value) {
  // FIX: WriteBatch Memory Limit fix
  // Record start WriteBatch size and count
  LocalSavePoint save(b);
  // FIX: end
  std::string key_buf;
  std::string value_buf;
  Slice slice_key(key, &key_buf);
  Slice slice_value(value, &value_buf);

#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("PUT PARTS IN BATCH",column_family_id,slice_key.data(),slice_key.size(),slice_value.size());
#endif

  insdb::Slice insdb_key(slice_key.data(), slice_key.size());
  insdb::Slice insdb_value(slice_value.data(), slice_value.size());
  WrapperToInsdbWriteBatch(b->wrapper_context)->Put(insdb_key, insdb_value, column_family_id);

  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                          ContentFlags::HAS_PUT,
                          std::memory_order_relaxed);

  // FIX: WriteBatch Memory Limit fix
  // commit check if back to start SavePoint
  return save.commit();
  // FIX: end
}

Status WriteBatch::Put(ColumnFamilyHandle* column_family, const SliceParts& key,
                     const SliceParts& value) {
  return WriteBatchInternal::Put(this, column_family?column_family->GetID():0, key, value);
}

Status WriteBatchInternal::InsertNoop(WriteBatch* b) {
  // TODO: Call InSDB
  // Noop is often placeholder for BeginPrepare. See MarkEndPrepare()

  // Return success for now
  return Status::OK();
}

Status WriteBatchInternal::MarkEndPrepare(WriteBatch* b, const Slice& xid) {
    // a manually constructed batch can only contain one prepare section
    //assert(b->rep_[12] == static_cast<char>(kTypeNoop));

    // all savepoints up to this point are cleared
    if (b->save_points_ != nullptr) {
      while (!b->save_points_->stack.empty()) {
        b->save_points_->stack.pop();
      }
    }

    // TODO: Call InSDB

    // rewrite noop as begin marker
    //b->rep_[12] = static_cast<char>(kTypeBeginPrepareXID);
    //b->rep_.push_back(static_cast<char>(kTypeEndPrepareXID));
    //PutLengthPrefixedSlice(&b->rep_, xid);
    b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                            ContentFlags::HAS_END_PREPARE |
                            ContentFlags::HAS_BEGIN_PREPARE,
                            std::memory_order_relaxed);
    return Status::OK();
}

Status WriteBatchInternal::MarkCommit(WriteBatch* b, const Slice& xid) {
    //b->rep_.push_back(static_cast<char>(kTypeCommitXID));
    //PutLengthPrefixedSlice(&b->rep_, xid);
    b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                            ContentFlags::HAS_COMMIT,
                            std::memory_order_relaxed);
    return Status::OK();
}

Status WriteBatchInternal::MarkRollback(WriteBatch* b, const Slice& xid) {
  throw std::runtime_error("Not supported API");
}

Status WriteBatchInternal::Delete(WriteBatch* b, uint32_t column_family_id,
                                const Slice& key) {
  // FIX: WriteBatch Memory Limit fix
  // Record start WriteBatch size and count
  LocalSavePoint save(b);
  // FIX: end
  insdb::Slice insdb_key(key.data(), key.size());
#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("DELETE IN BATCH",column_family_id,key.data(),key.size(),0);
#endif

  WrapperToInsdbWriteBatch(b->wrapper_context)->Delete(insdb_key, column_family_id);

  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                          ContentFlags::HAS_DELETE,
                          std::memory_order_relaxed);

  // FIX: WriteBatch Memory Limit fix
  // commit check if back to start SavePoint
  return save.commit();
  // FIX: end
}

Status WriteBatch::Delete(ColumnFamilyHandle* column_family, const Slice& key) {
  return WriteBatchInternal::Delete(this, column_family?column_family->GetID():0, key);
}

Status WriteBatchInternal::Delete(WriteBatch* b, uint32_t column_family_id,
                                const SliceParts& key) {
	  std::string key_buf;
	  Slice slice_key(key, &key_buf);
	  return WriteBatchInternal::Delete(b, column_family_id, slice_key);
}

Status WriteBatch::Delete(ColumnFamilyHandle* column_family,
                        const SliceParts& key) {
  return WriteBatchInternal::Delete(this, column_family?column_family->GetID():0, key);
}

Status WriteBatchInternal::SingleDelete(WriteBatch* b, uint32_t column_family_id,
                                      const Slice& key) {
  // FIX: WriteBatch Memory Limit fix
  // Record start WriteBatch size and count
  LocalSavePoint save(b);
  // FIX: end
#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("SINGLE DELETE IN BATCH",column_family_id,key.data(),key.size(),0);
#endif
  // insdb treats SingleDelete as Delete without performance penalty
  insdb::Slice insdb_key(key.data(), key.size());
  WrapperToInsdbWriteBatch(b->wrapper_context)->Delete(insdb_key, column_family_id);

  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                          ContentFlags::HAS_SINGLE_DELETE,
                          std::memory_order_relaxed);

  // FIX: WriteBatch Memory Limit fix
  // commit check if back to start SavePoint
  return save.commit();
  // FIX: end
}

Status WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                              const Slice& key) {
  return WriteBatchInternal::SingleDelete(this, column_family?column_family->GetID():0, key);
}

Status WriteBatchInternal::SingleDelete(WriteBatch* b, uint32_t column_family_id,
                                      const SliceParts& key) {
	  std::string key_buf;
	  Slice slice_key(key, &key_buf);
	return SingleDelete(b, column_family_id, slice_key);
}

Status WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                              const SliceParts& key) {
  return WriteBatchInternal::SingleDelete(this, column_family?column_family->GetID():0, key);
}

Status WriteBatchInternal::DeleteRange(WriteBatch* b, uint32_t column_family_id,
                                     const Slice& begin_key,
                                     const Slice& end_key) {
	throw std::runtime_error("Not supported API");
}

Status WriteBatch::DeleteRange(ColumnFamilyHandle* column_family,
                             const Slice& begin_key, const Slice& end_key) {
  return WriteBatchInternal::DeleteRange(this, 0,
                                  begin_key, end_key);
}

Status WriteBatchInternal::DeleteRange(WriteBatch* b, uint32_t column_family_id,
                                     const SliceParts& begin_key,
                                     const SliceParts& end_key) {
	throw std::runtime_error("Not supported API");
}

Status WriteBatch::DeleteRange(ColumnFamilyHandle* column_family,
                             const SliceParts& begin_key,
                             const SliceParts& end_key) {
  return WriteBatchInternal::DeleteRange(this, 0,
                                  begin_key, end_key);
}

Status WriteBatchInternal::Merge(WriteBatch* b, uint32_t column_family_id,
                               const Slice& key, const Slice& value) {
  // FIX: WriteBatch Memory Limit fix
  // Record start WriteBatch size and count
  LocalSavePoint save(b);
  // FIX: end
#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("MERGE IN BATCH",column_family_id,key.data(),key.size(),value.size());
#endif
  insdb::Slice insdb_key(key.data(), key.size());
  insdb::Slice insdb_value(value.data(), value.size());
  WrapperToInsdbWriteBatch(b->wrapper_context)->Merge(insdb_key, insdb_value, column_family_id);

  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                          ContentFlags::HAS_MERGE,
                          std::memory_order_relaxed);

  // FIX: WriteBatch Memory Limit fix
  // commit check if back to start SavePoint
  return save.commit();
  // FIX: end
}

Status WriteBatch::Merge(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) {
  return WriteBatchInternal::Merge(this, column_family?column_family->GetID():0, key, value);
}

Status WriteBatchInternal::Merge(WriteBatch* b, uint32_t column_family_id,
                               const SliceParts& key,
                               const SliceParts& value) {
	throw std::runtime_error("Not supported API");
}

Status WriteBatch::Merge(ColumnFamilyHandle* column_family,
                       const SliceParts& key,
                       const SliceParts& value) {
  return WriteBatchInternal::Merge(this, 0,
                            key, value);
}

Status WriteBatch::PutLogData(const Slice& blob) {
    throw std::runtime_error("Not supported API");
}

void WriteBatch::SetSavePoint() {
  if (save_points_ == nullptr) {
    save_points_ = new SavePoints();
  }
  // Record length and count of current batch of writes.
  save_points_->stack.push(SavePoint(
      GetDataSize(), Count(), content_flags_.load(std::memory_order_relaxed)));
}

Status WriteBatch::RollbackToSavePoint() {
  if (save_points_ == nullptr || save_points_->stack.size() == 0) {
    return Status::NotFound();
  }

  // Pop the most recent savepoint off the stack
  SavePoint savepoint = save_points_->stack.top();
  save_points_->stack.pop();

  assert(savepoint.size <= GetDataSize());
  assert(savepoint.count <= Count());

  if (savepoint.size == GetDataSize()) {
    // No changes to rollback
  } else if (savepoint.size == 0) {
    // Rollback everything
    Clear();
  } else {
      WrapperToInsdbWriteBatch(wrapper_context)->ResizeData(savepoint.size);
      WriteBatchInternal::SetCount(this, savepoint.count);
      content_flags_.store(savepoint.content_flags, std::memory_order_relaxed);
  }

  return Status::OK();
}
Status WriteBatch::PopSavePoint() {
  if (save_points_ == nullptr || save_points_->stack.size() == 0) {
    return Status::NotFound();
  }

  // Pop the most recent savepoint off the stack
  save_points_->stack.pop();

  return Status::OK();
}

#ifdef _ORIG_ROCKSDB_

Status WriteBatchInternal::InsertInto(
    const WriteBatch* batch, ColumnFamilyMemTables* memtables,
    FlushScheduler* flush_scheduler, bool ignore_missing_column_families,
    uint64_t log_number, DB* db, bool concurrent_memtable_writes,
    SequenceNumber* last_seq_used, bool* has_valid_writes) {
  return s.NotSupported();
}

#endif

Status WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
	throw std::runtime_error("Not supported API");
}

Status WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src,
                                const bool wal_only) {
    //size_t src_len;
    //int src_count;
    uint32_t src_flags;

#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("BATCH APPEND",0,"",0,0);
#endif

    const SavePoint& batch_end = src->GetWalTerminationPoint();

    if (wal_only && !batch_end.is_cleared()) {
      //src_len = batch_end.size - WriteBatchInternal::kHeader;
      //src_count = batch_end.count;
      src_flags = batch_end.content_flags;
    } else {
      //src_len = src->rep_.size() - WriteBatchInternal::kHeader;
      //src_count = Count(src);
      src_flags = src->content_flags_.load(std::memory_order_relaxed);
    }

    insdb::Status insdb_s = WrapperToInsdbWriteBatch(dst->wrapper_context)->Append(WrapperToInsdbWriteBatch(src->wrapper_context), wal_only);
    if(!insdb_s.ok())
    {
        return convert_insdb_status(insdb_s);;
    }

    //SetCount(dst, Count(dst) + src_count);
    //assert(src->rep_.size() >= WriteBatchInternal::kHeader);
    //dst->rep_.append(src->rep_.data() + WriteBatchInternal::kHeader, src_len);
    dst->content_flags_.store(
        dst->content_flags_.load(std::memory_order_relaxed) | src_flags,
        std::memory_order_relaxed);
    return Status::OK();
}

size_t WriteBatchInternal::AppendedByteSize(size_t leftByteSize,
                                            size_t rightByteSize) {
	throw std::runtime_error("Not supported API");
}

}  // namespace rocksdb
