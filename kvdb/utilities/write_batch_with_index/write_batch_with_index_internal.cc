//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

#include "insdb/write_batch.h"
#include "db/insdb_wrapper.h"
#include "db/column_family.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

class Env;
class Logger;
class Statistics;

Status ReadableWriteBatch::GetEntryFromDataOffset(size_t data_offset,
                                                  WriteType* type, Slice* Key,
                                                  Slice* value, Slice* blob,
                                                  Slice* xid) const {

  insdb::WriteType insdb_writeType;
  insdb::Slice insdb_Key(Key->data(), Key->size());
  insdb::Slice insdb_value(value->data(), value->size());
  insdb::Slice insdb_blob(blob->data(), blob->size());
  insdb::Slice insdb_xid(xid->data(), xid->size());

  insdb::Status s = WrapperToInsdbWriteBatch(wrapper_context)->GetEntryFromDataOffset(
          data_offset, &insdb_writeType,
          &insdb_Key, &insdb_value, &insdb_blob, &insdb_xid);
  if (!s.ok())
      return convert_insdb_status(s);

  // convert InSDB result to RocksDB
  *type = convert_insdb_write_type(insdb_writeType);
  //Fix bug：Can not get the record entry in WriteBatch which is not committed yet in transaction
  //Convert the InSDB data result to KVDB result
  Key->data_ = insdb_Key.data();
  value->data_ = insdb_value.data();
  blob->data_ = insdb_blob.data();
  xid->data_ = insdb_xid.data();
  //Fix end
  Key->size_ = insdb_Key.size();
  value->size_ = insdb_value.size();
  blob->size_ = insdb_blob.size();
  xid->size_ = insdb_xid.size();

  return Status::OK();
}

int WriteBatchEntryComparator::operator()(
    const WriteBatchIndexEntry* entry1,
    const WriteBatchIndexEntry* entry2) const {
  if (entry1->column_family > entry2->column_family) {
    return 1;
  } else if (entry1->column_family < entry2->column_family) {
    return -1;
  }

  if (entry1->offset == WriteBatchIndexEntry::kFlagMin) {
    return -1;
  } else if (entry2->offset == WriteBatchIndexEntry::kFlagMin) {
    return 1;
  }

  Slice key1, key2;
  if (entry1->search_key == nullptr) {
    key1 = Slice(write_batch_->Data().data() + entry1->key_offset,
                 entry1->key_size);
  } else {
    key1 = *(entry1->search_key);
  }
  if (entry2->search_key == nullptr) {
    key2 = Slice(write_batch_->Data().data() + entry2->key_offset,
                 entry2->key_size);
  } else {
    key2 = *(entry2->search_key);
  }

  int cmp = CompareKey(entry1->column_family, key1, key2);
  if (cmp != 0) {
    return cmp;
  } else if (entry1->offset > entry2->offset) {
    return 1;
  } else if (entry1->offset < entry2->offset) {
    return -1;
  }
  return 0;
}

int WriteBatchEntryComparator::CompareKey(uint32_t column_family,
                                          const Slice& key1,
                                          const Slice& key2) const {
  if (column_family < cf_comparators_.size() &&
      cf_comparators_[column_family] != nullptr) {
    return cf_comparators_[column_family]->Compare(key1, key2);
  } else {
    return default_comparator_->Compare(key1, key2);
  }
}

WriteBatchWithIndexInternal::Result WriteBatchWithIndexInternal::GetFromBatch(
    const ImmutableDBOptions& immuable_db_options, WriteBatchWithIndex* batch,
    ColumnFamilyHandle* column_family, const Slice& key,
    MergeContext* merge_context, WriteBatchEntryComparator* cmp,
    std::string* value, bool overwrite_key, Status* s) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  *s = Status::OK();
  WriteBatchWithIndexInternal::Result result =
      WriteBatchWithIndexInternal::Result::kNotFound;

  std::unique_ptr<WBWIIterator> iter =
      std::unique_ptr<WBWIIterator>(batch->NewIterator(column_family));

  // We want to iterate in the reverse order that the writes were added to the
  // batch.  Since we don't have a reverse iterator, we must seek past the end.
  // TODO(agiardullo): consider adding support for reverse iteration
  iter->Seek(key);
  while (iter->Valid()) {
    const WriteEntry entry = iter->Entry();
    if (cmp->CompareKey(cf_id, entry.key, key) != 0) {
      break;
    }

    iter->Next();
  }

  if (!(*s).ok()) {
    return WriteBatchWithIndexInternal::Result::kError;
  }

  if (!iter->Valid()) {
    // Read past end of results.  Reposition on last result.
    iter->SeekToLast();
  } else {
    iter->Prev();
  }

  Slice entry_value;
  while (iter->Valid()) {
    const WriteEntry entry = iter->Entry();
    if (cmp->CompareKey(cf_id, entry.key, key) != 0) {
      // Unexpected error or we've reached a different next key
      break;
    }

    switch (entry.type) {
      case kPutRecord: {
        result = WriteBatchWithIndexInternal::Result::kFound;
        entry_value = entry.value;
        break;
      }
      case kMergeRecord: {
        result = WriteBatchWithIndexInternal::Result::kMergeInProgress;
        merge_context->PushOperand(entry.value);
        break;
      }
      case kDeleteRecord:
      case kSingleDeleteRecord: {
        result = WriteBatchWithIndexInternal::Result::kDeleted;
        break;
      }
      case kLogDataRecord:
      case kXIDRecord: {
        // ignore
        break;
      }
      default: {
        result = WriteBatchWithIndexInternal::Result::kError;
        (*s) = Status::Corruption("Unexpected entry in WriteBatchWithIndex:",
                                  ToString(entry.type));
        break;
      }
    }
    if (result == WriteBatchWithIndexInternal::Result::kFound ||
        result == WriteBatchWithIndexInternal::Result::kDeleted ||
        result == WriteBatchWithIndexInternal::Result::kError) {
      // We can stop iterating once we find a PUT or DELETE
      break;
    }
    if (result == WriteBatchWithIndexInternal::Result::kMergeInProgress &&
        overwrite_key == true) {
      // Since we've overwritten keys, we do not know what other operations are
      // in this batch for this key, so we cannot do a Merge to compute the
      // result.  Instead, we will simply return MergeInProgress.
      break;
    }

    iter->Prev();
  }

  if ((*s).ok()) {
    if (result == WriteBatchWithIndexInternal::Result::kFound ||
        result == WriteBatchWithIndexInternal::Result::kDeleted) {
      // Found a Put or Delete.  Merge if necessary.
      if (merge_context->GetNumOperands() > 0) {
        const MergeOperator* merge_operator;

        if (column_family != nullptr) {
          auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
          merge_operator = cfh->cfd()->ioptions()->merge_operator;
        } else {
          *s = Status::InvalidArgument("Must provide a column_family");
          result = WriteBatchWithIndexInternal::Result::kError;
          return result;
        }
        Statistics* statistics = immuable_db_options.statistics.get();
        Env* env = immuable_db_options.env;
        Logger* logger = immuable_db_options.info_log.get();

        if (merge_operator) {
          *s = MergeHelper::TimedFullMerge(merge_operator, key, &entry_value,
                                           merge_context->GetOperands(), value,
                                           logger, statistics, env);
        } else {
          *s = Status::InvalidArgument("Options::merge_operator must be set");
        }
        if ((*s).ok()) {
          result = WriteBatchWithIndexInternal::Result::kFound;
        } else {
          result = WriteBatchWithIndexInternal::Result::kError;
        }
      } else {  // nothing to merge
        if (result == WriteBatchWithIndexInternal::Result::kFound) {  // PUT
          value->assign(entry_value.data(), entry_value.size());
        }
      }
    }
  }

  return result;
}

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
