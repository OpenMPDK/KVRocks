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



// An Env is an interface used by the insdb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#ifndef STORAGE_INSDB_INCLUDE_ENV_H_
#define STORAGE_INSDB_INCLUDE_ENV_H_

#include <stdarg.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "insdb/export.h"
#include "insdb/status.h"
//#include "util/linux_nvme_ioctl.h"

namespace insdb {

    class FileLock;
    class Logger;
    class RandomAccessFile;
    class SequentialFile;
    class Slice;
    class WritableFile;

    enum FlushType{
        kWriteFlush = 0x81, // nvme_cmd_kv_store,
        kReadFlush = 0x90, // nvme_cmd_kv_retrieve,
    };
    enum GetType{
        kSyncGet = 0x1,
        kReadahead = 0x2,
        kNonblockingRead = 0x3,
    };
    union INSDB_EXPORT InSDBKey {
        struct {
            uint64_t    first;
            uint64_t    second;
        };
        struct {
            uint32_t    db_id;
            uint16_t    type;
            uint16_t    split_seq;
            union { 
                uint64_t    seq;
                uint64_t    meta;
                struct{
                    uint32_t meta_id;
                    uint32_t meta_info;
                };
                struct{
                    uint32_t meta_word0;
                    uint32_t meta_word1;
                };
            };
        };
    } __attribute__((packed)) ;

    static_assert (sizeof(InSDBKey) == 16, "Invalid InSDBKey size");

    inline bool operator==(const InSDBKey& x, const InSDBKey& y) {
        return ((x.first == y.first) && (x.second == y.second));
    }

    inline bool operator!=(const InSDBKey& x, const InSDBKey& y) {
        return !(x == y);
    }

    InSDBKey EncodeInSDBKey(InSDBKey);

    class INSDB_EXPORT Env {
        public:
            Env() { }
            virtual ~Env();

            // Return a default environment suitable for the current operating
            // system.  Sophisticated users may wish to provide their own Env
            // implementation instead of relying on this default environment.
            //
            // The result of Default() belongs to insdb and must never be deleted.
            static Env* Default();

            /**
              @brief Open KV SSD device.
              @param  in  kv_ssd_name std::string&
              @return Status::OK() if success, else error messages.

              Open a KV SSD device. Because single device open enough during DB
              life cycle, It hide to expose to upper.
              For the operation withoug opeinning device, will report 
              - We may need a mechanism, which maniging multiple KV SSD.
              Current implementation just assume a single KV SSD.

              [Note]
                kv_dev : KV SSD device name.
*/
            virtual Status Open(std::vector<std::string> &kv_ssd) = 0;

            /**
              Close a KV SSD device
              */
            virtual void Close() = 0;

            /**
              @brief acquire database lock.
              @param  in  dbname std::string&
              @return true if success, false.

              AcauireDBLock(). Used to protect DB from multiple-acess.
              Kernel must support this feature and also cover the case, which
              application leave without ReleaseDBLock() call.
              Use internel DB handler which is Opened by Open() call.
              */
            virtual bool AcquireDBLock(uint16_t db_name) = 0;

            /**
              @brief release database lock.
              @param  in  dbname std::string&
              @return true if success, false.

              ReleaseDBLock(), Kernel must free internal data structure for DB lock(),
              Use internel DB handler which is Opened by Open() call.
              */
            virtual void ReleaseDBLock(uint16_t db_name) = 0;

            /**
              @brief save key-value pair synchronously.
              @param  in  key Slice&
              @param  in  value Sliece&
              @param  in  async bool
              @return Status::OK() if success, else error messages.

              (A)Synchronous Put operation with Key-value pair.
              Use internel DB handler which is Opened by Open() call.
              */
            virtual Status Put(InSDBKey key, const Slice& value, bool async = false) = 0;

            /**
              @brief flush all keys.
              @param  in  key Slice&
              @return Status::OK() if success, else error messages.

              Flush single or all Key. it guarantees of writng keyes to KV SSD, which maybe exist on
              kernel write buffer.
              Use internel DB handler which is Opened by Open() call.
              */
            virtual Status Flush(FlushType type, int dev_idx = -1, InSDBKey key = {0, 0}) = 0;

            /**
              @brief  retrieve key-value pair synchronously.
              @param  in  key Slice&
              @param  out  buf char*
              @param  in/out  size  int
              @param  in  readahead
              @return Status::OK() if success, else error messages.

              Synchronous Get operation with Key, contents of key will be saved on value.
              Value defined as Slice, it must provide pre-allocated buffer.
              Use internel DB handler which is Opened by Open() call.
              */
            static const uint8_t GetFlag_NoReturnedSize = 0x80;
            static const uint8_t GetFlag_PrefetchLevel_1 = 0x20;
            static const uint8_t GetFlag_PrefetchLevel_2 = 0x40;
            static const uint8_t GetFlag_PrefetchLevel_3 = 0x60;
            virtual Status Get(InSDBKey key, char *buf, int *size, GetType type = kSyncGet, uint8_t flags = 0, bool *from_readahead = NULL) = 0;

            enum DevStatus {
                Success = 0,
                KeyNotExist = 1,
                ReadaheadHit = 2,
                Unknown,
            };

            virtual Status MultiPut(std::vector<InSDBKey> &key, std::vector<char*> &buf, std::vector<int> &size, InSDBKey blocking_key, bool async) = 0;
            virtual Status MultiGet(std::vector<InSDBKey> &key, std::vector<char*> &buf, std::vector<int> &size, InSDBKey blocking_key, bool readahead, uint8_t flags, std::vector<DevStatus> &status) = 0;
            virtual Status MultiDel(std::vector<InSDBKey> &key, InSDBKey blocking_key, bool async, std::vector<DevStatus> &status) = 0;
            /**
              @brief ask discard a single key in kernel buffer
              @param  in  key Slice&
              @return Status::OK() if success, else error messages.

              It removes a key or all keys from kernel read-ahread buffer.
              Use internel DB handler which is Opened by Open() call.
              */
            virtual Status DiscardPrefetch(InSDBKey key = {0, 0}) = 0;

            /**
              @brief Delete existing key.
              @param  in  key Slice&
              @param  in  async bool
              @return Status::OK() if success, else error messages.

              It delete existing key.
              */
            virtual Status Delete(InSDBKey key, bool async = true) = 0;



            /**
              @brief check whether key is in database or not.
              @param  in  key Slice&
              @return true if success, else false.

              It check whetehr key exist or not.
              */
            virtual bool KeyExist(InSDBKey key) = 0;

            virtual Status IterReq(uint32_t mask, uint32_t iter_val, unsigned char *iter_handle, int dev_idx, bool open = false, bool rm = false) = 0;
            virtual Status IterRead(char* buf, int *size, unsigned char iter_handle, int dev_idx) = 0;
            virtual Status GetLog(char pagecode, char* buf, int bufflen, int dev_idx) = 0;
            // Arrange to run "(*function)(arg)" once in a background thread.
            //
            // "function" may run in an unspecified thread.  Multiple functions
            // added to the same Env may run concurrently in different threads.
            // I.e., the caller may not assume that background work items are
            // serialized.
            virtual void Schedule(
                    void (*function)(void* arg),
                    void* arg) = 0;

            // Start a new thread, invoking "function(arg)" within the new thread.
            // When "function(arg)" returns, the thread will be destroyed.
            virtual uint64_t StartThread(void (*function)(void* arg), void* arg) = 0;

            // Wait for a thread to terminate.
            virtual void ThreadJoin(uint64_t tid) {}

            // Create and return a log file for storing informational messages.
            virtual Status NewLogger(const std::string& fname, Logger** result) = 0;

            // Returns the number of micro-seconds since some fixed point in time. Only
            // useful for computing deltas of time.
            virtual uint64_t NowMicros() = 0;

            virtual uint64_t NowSecond() = 0;

            // Sleep/delay the thread for the prescribed number of micro-seconds.
            virtual void SleepForMicroseconds(int micros) = 0;

        private:
            // No copying allowed
            Env(const Env&);
            void operator=(const Env&);
    };

    // An interface for writing log messages.
    class INSDB_EXPORT Logger {
        public:
            Logger() { }
            virtual ~Logger();

            // Write an entry to the log file with the specified format.
            virtual void Logv(const char* format, va_list ap) = 0;

        private:
            // No copying allowed
            Logger(const Logger&);
            void operator=(const Logger&);
    };

    // Log the specified data to *info_log if info_log is non-NULL.
    extern void Log(Logger* info_log, const char* format, ...)
#   if defined(__GNUC__) || defined(__clang__)
        __attribute__((__format__ (__printf__, 2, 3)))
#   endif
        ;

    // An implementation of Env that forwards all calls to another Env.
    // May be useful to clients who wish to override just part of the
    // functionality of another Env.
    class INSDB_EXPORT EnvWrapper : public Env {
        public:
            // Initialize an EnvWrapper that delegates all calls to *t
            explicit EnvWrapper(Env* t) : target_(t) { }
            virtual ~EnvWrapper();

            // Return the target to which this Env forwards all calls
            Env* target() const { return target_; }

            // The following text is boilerplate that forwards all methods to target()
            Status Open(std::vector<std::string>& kv_ssd_name) { return target_->Open(kv_ssd_name); }
            void Close() { return target_->Close(); }
            bool AcquireDBLock(const uint16_t db_name) { return target_->AcquireDBLock(db_name); }
            void ReleaseDBLock(const uint16_t db_name) { return target_->ReleaseDBLock(db_name); }
            Status Put(InSDBKey key, const Slice& value, bool async = false) { return target_->Put(key, value, async); }
            Status Flush(FlushType type, int dev_idx = -1, InSDBKey key = {0,0}) { return target_->Flush(type, dev_idx, key); }
            Status Get(InSDBKey key, char *buf, int *size, GetType type = kSyncGet, uint8_t flags = 0, bool *from_readahead = NULL) { return target_->Get(key, buf, size, type, flags, from_readahead); }
            Status MultiPut(std::vector<InSDBKey> &key, std::vector<char*> &buf, std::vector<int> &size, InSDBKey blocking_key, bool async) { return target_->MultiPut(key, buf, size, blocking_key, async); }
            Status MultiGet(std::vector<InSDBKey> &key, std::vector<char*> &buf, std::vector<int> &size, InSDBKey blocking_key, bool readahead, uint8_t flags, std::vector<DevStatus> &status) { return target_->MultiGet(key, buf, size, blocking_key, readahead, flags, status); }
            Status MultiDel(std::vector<InSDBKey> &key, InSDBKey blocking_key, bool async, std::vector<DevStatus> &status) { return target_->MultiDel(key, blocking_key, async, status); }
            Status DiscardPrefetch(InSDBKey key = {0,0}) {return target_->DiscardPrefetch(key); }
            Status Delete(InSDBKey key, bool async = true) { return target_->Delete(key, async); }
            bool KeyExist(InSDBKey key) { return target_->KeyExist(key); }


            virtual Status IterReq(uint32_t mask, uint32_t iter_val, unsigned char *iter_handle, int dev_idx, bool open = false, bool rm = false) {
                return target_->IterReq(mask, iter_val, iter_handle, open, rm, dev_idx);
            }
            virtual Status IterRead(char* buf, int *size, unsigned char iter_handle, int dev_idx) {
                return target_->IterRead(buf, size, iter_handle, dev_idx);
            }
            virtual Status GetLog(char pagecode, char* buf, int bufflen, int dev_idx) {
                return target_->GetLog(pagecode, buf, bufflen, dev_idx);
            }
            uint64_t StartThread(void (*f)(void*), void* a) {
                return target_->StartThread(f, a);
            }
            virtual void ThreadJoin(uint64_t tid) {
                return target_->ThreadJoin(tid);
            }
            virtual Status NewLogger(const std::string& fname, Logger** result) {
                return target_->NewLogger(fname, result);
            }
            uint64_t NowMicros() {
                return target_->NowMicros();
            }

            uint64_t NowSecond() {
                return target_->NowSecond();
            }
            void SleepForMicroseconds(int micros) {
                target_->SleepForMicroseconds(micros);
            }
        private:
            Env* target_;
    };

}  // namespace insdb

#endif  // STORAGE_INSDB_INCLUDE_ENV_H_
