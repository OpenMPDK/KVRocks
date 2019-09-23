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


#ifndef STORAGE_INSDB_INCLUDE_DB_H_
#define STORAGE_INSDB_INCLUDE_DB_H_

#include <stdint.h>
#include <stdio.h>
#include <unordered_map>
#include "insdb/export.h"
#include "insdb/iterator.h"
#include "insdb/options.h"

namespace insdb {

    // Update Makefile if you change these
    static const int kMajorVersion = 1;
    static const int kMinorVersion = 20;

    struct Options;
    struct ReadOptions;
    struct WriteOptions;
    class WriteBatch;

    // Abstract handle to particular state of a DB.
    // A Snapshot is an immutable object and can therefore be safely
    // accessed from multiple threads without any external synchronization.
    class INSDB_EXPORT Snapshot {
        public:
            // returns Snapshot's sequence number
            virtual uint64_t GetSequenceNumber() const = 0;

        protected:
            virtual ~Snapshot();
    };

    // A range of keys
    struct INSDB_EXPORT Range {
        Slice start;          // Included in the range
        Slice limit;          // Not included in the range

        Range() { }
        Range(const Slice& s, const Slice& l) : start(s), limit(l) { }
    };

    // A DB is a persistent ordered map from keys to values.
    // A DB is safe for concurrent access from multiple threads without
    // any external synchronization.
    class INSDB_EXPORT DB {
        public:
            // Open the database with the specified "name".
            // Stores a pointer to a heap-allocated database in *dbptr and returns
            // OK on success.
            // Stores NULL in *dbptr and returns a non-OK status on error.
            // Caller should delete *dbptr when it is no longer needed.
            static Status Open(const Options& options,
                    const std::string& name,
                    DB** dbptr);

            static Status OpenWithTtl(const Options& options, const std::string& name,
                                      DB** dbptr, uint64_t* TtlList, bool* TtlEnabledList);

            DB() { }
            virtual ~DB();

            virtual Status SetTtlList(uint64_t* TtlList, bool* TtlEnabledList) {
              return Status::NotSupported("Not implemented");
            }
            virtual Status SetTtlInTtlList(uint16_t id, uint64_t ttl) {
              return Status::NotSupported("Not implemented");
            }
            virtual uint64_t GetTtlFromTtlList(uint16_t id) { return 0; }
            virtual bool TtlIsEnabled(uint16_t id) { return false; }

            // Set the database entry for "key" to "value".  Returns OK on success,
            // and a non-OK status on error.
            // Note: consider setting options.sync = true.
            virtual Status Put(const WriteOptions& options,
                    const Slice& key,
                    const Slice& value) = 0;

            // Remove the database entry (if any) for "key".  Returns OK on
            // success, and a non-OK status on error.  It is not an error if "key"
            // did not exist in the database.
            // Note: consider setting options.sync = true.
            virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

            // Apply the specified updates to the database.
            // Returns OK on success, non-OK on failure.
            // Note: consider setting options.sync = true.
            virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

            // If the database contains an entry for "key" store the
            // corresponding value in *value and return OK.
            //
            // If there is no entry for "key" leave *value unchanged and return
            // a status for which Status::IsNotFound() returns true.
            //
            // May return some other Status on an error.
            virtual Status Get(const ReadOptions& options,
                    const Slice& key, std::string* value, uint16_t col_id = 0, PinnableSlice* pin_slice = NULL) = 0;

            virtual Status Get(const ReadOptions& options,
                    const Slice& key, PinnableSlice* value, uint16_t col_id = 0) = 0;

            virtual Status CompactRange(const Slice *begin, const Slice *end, const uint16_t col_id = 0) = 0;


            // Return a heap-allocated iterator over the contents of the database.
            // The result of NewIterator() is initially invalid (caller must
            // call one of the Seek methods on the iterator before using it).
            //
            // Caller should delete the iterator when it is no longer needed.
            // The returned iterator should be deleted before this db is deleted.
            virtual Iterator* NewIterator(const ReadOptions& options, uint16_t col_id = 0) = 0;

            // Return a handle to the current DB state.  Iterators created with
            // this handle will all observe a stable snapshot of the current DB
            // state.  The caller must call ReleaseSnapshot(result) when the
            // snapshot is no longer needed.
            virtual const Snapshot* GetSnapshot() = 0;

            // Release a previously acquired snapshot.  The caller must not
            // use "snapshot" after this call.
            virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

            // Flush all mem-table data.
            virtual Status Flush(const FlushOptions& options,
                    uint16_t col_id = 0) = 0;

            // DB implementations can export properties about their state
            // via this method.  If "property" is a valid property understood by this
            // DB implementation, fills "*value" with its current value and returns
            // true.  Otherwise returns false.
            //
            //
            // Valid property names include:
            //
            //  "insdb.num-files-at-level<N>" - return the number of files at level <N>,
            //     where <N> is an ASCII representation of a level number (e.g. "0").
            //  "insdb.stats" - returns a multi-line string that describes statistics
            //     about the internal operation of the DB.
            //  "insdb.sstables" - returns a multi-line string that describes all
            //     of the sstables that make up the db contents.
            //  "insdb.approximate-memory-usage" - returns the approximate number of
            //     bytes of memory in use by the DB.
            virtual bool GetProperty(const Slice& property, std::string* value) = 0;

            // For each i in [0,n-1], store in "sizes[i]", the approximate
            // file system space used by keys in "[range[i].start .. range[i].limit)".
            //
            // Note that the returned sizes measure file system space usage, so
            // if the user data compresses by a factor of ten, the returned
            // sizes will be one-tenth the size of the corresponding user data size.
            //
            // The results may not include the sizes of recently written data.
            virtual void GetApproximateSizes(const Range* range, int n,
                    uint64_t* sizes, uint16_t col_id=0) = 0;
            /*
            // Compact the underlying storage for the key range [*begin,*end].
            // In particular, deleted and overwritten versions are discarded,
            // and the data is rearranged to reduce the cost of operations
            // needed to access the data.  This operation should typically only
            // be invoked by users who understand the underlying implementation.
            //
            // begin==NULL is treated as a key before all keys in the database.
            // end==NULL is treated as a key after all keys in the database.
            // Therefore the following call will compact the entire database:
            //    db->CompactRange(NULL, NULL);
            virtual void CompactRange(const Slice* begin, const Slice* end) = 0;
            */

            // Search given key and set sequencenumber.
            // if if found, it also set found value as true.
            virtual Status GetLatestSequenceForKey(const Slice& key,uint64_t* sequence, bool* found, uint16_t col_id=0) = 0;

            // The sequence number of the most recent transaction.
            virtual uint64_t GetLatestSequenceNumber() const = 0;

            virtual void CancelAllBackgroundWork(bool wait) = 0;

        private:
            // No copying allowed
            DB(const DB&);
            void operator=(const DB&);
    };

    // Destroy the contents of the specified database.
    // Be very careful using this method.
    //
    // Note: For backwards compatibility, if DestroyDB is unable to list the
    // database files, Status::OK() will still be returned masking this failure.
    INSDB_EXPORT Status DestroyDB(const std::string& name,
            const Options& options);

    // If a DB cannot be opened, you may attempt to call this method to
    // resurrect as much of the contents of the database as possible.
    // Some data may be lost, so be careful when calling this function
    // on a database that contains important information.
    INSDB_EXPORT Status RepairDB(const std::string& dbname,
            const Options& options);

}  // namespace insdb

#endif  // STORAGE_INSDB_INCLUDE_DB_H_
