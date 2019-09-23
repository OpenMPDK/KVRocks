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


// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_INSDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_INSDB_INCLUDE_WRITE_BATCH_H_

#include <string>
#include "insdb/export.h"
#include "insdb/status.h"

namespace insdb {

    class Slice;
    class DBImpl;

    enum WriteType {
      kPutRecord,
      kMergeRecord,
      kDeleteRecord,
      kSingleDeleteRecord,
      kDeleteRangeRecord,
      kLogDataRecord,
      kXIDRecord,
    };

    class INSDB_EXPORT WriteBatch {
        public:
            WriteBatch();
            ~WriteBatch();

            // Store the mapping "key->value" in the database.
            void Put(const Slice& key, const Slice& value, const uint16_t col_id = 0);

            // Merge the mapping "key->value" in the database.
            void Merge(const Slice& key, const Slice& value, const uint16_t col_id = 0);

            // If the database contains a mapping for "key", erase it.  Else do nothing.
            void Delete(const Slice& key, const uint16_t col_id = 0);

            // Clear all updates buffered in this batch.
            void Clear();

            // The size of the database changes caused by this batch.
            //
            // This number is tied to implementation details, and may change across
            // releases. It is intended for LevelDB usage metrics.
            size_t ApproximateSize();

            // Return the number of entries in the batch.
            size_t Count();

            // Retrieve the serialized version of this batch.
            const std::string& Data() const { return rep_; }

            // Retrieve data size of the batch.
            size_t GetDataSize() const { return rep_.size(); }

            void ResizeData(size_t size) { rep_.resize(size); }

            //Feat: For implementing RollbackToSavePoint feature
            //Set the count for WriteBatch
            void SetCount(int n);
            //Feat end

            Status Append(WriteBatch* src, const bool wal_only);

            // Retrieve some information from a write entry in the write batch, given
            // the start offset of the write entry.
            // it supports ReadableWriteBatch class in RocksDB
            Status GetEntryFromDataOffset(size_t data_offset,
                                          WriteType* type, Slice* Key,
                                          Slice* value, Slice* blob,
                                          Slice* xid) const;

            // Returns the offset of the first entry in the batch.
            // This offset is only valid if the batch is not empty.
            size_t GetFirstOffset();

            // Support for iterating over the contents of a batch.
            class Handler {
                public:
                    virtual ~Handler();
                    virtual void Put(const Slice& key, const Slice& value, const uint16_t col_id = 0) = 0;
                    virtual Status Merge(const Slice& key, const Slice& value, const uint16_t col_id = 0) = 0;
                    virtual void Delete(const Slice& key, const uint16_t col_id = 0) = 0;
                    // FIX: Continue feature fix
                    // Continue is called by WriteBatch::Iterate. If it returns false,
                    // iteration is halted. Otherwise, it continues iterating. The default
                    // implementation always returns true.
                    virtual bool Continue();
                    // FIX: end
            };
            Status Iterate(Handler* handler);
        private:
            std::string rep_;  // See comment in write_batch.cc for the format of rep_

            // Intentionally copyable
    };

    // Read the key of a record from a write batch.
    // if this record represent the default column family then cf_record
    // must be passed as false, otherwise it must be passed as true.
    INSDB_EXPORT bool ReadKeyFromWriteBatchEntry(Slice* input, Slice* key, bool cf_record);

    // Read record from a write batch piece from input.
    // tag, column_family, key, value and blob are return values. Callers own the
    // Slice they point to.
    // Tag is defined as ValueType.
    // input will be advanced to after the record.
    INSDB_EXPORT Status ReadRecordFromWriteBatch(Slice* input, char* tag,
                                    uint16_t* column_family, Slice* key,
                                    Slice* value, Slice* blob, Slice* xid);
}  // namespace insdb

#endif  // STORAGE_INSDB_INCLUDE_WRITE_BATCH_H_
