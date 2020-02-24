//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

//#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/env.h"

#include "CommonType.h"



#include "util/sync_point.h"


#include <fcntl.h>
#include <inttypes.h>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/db_impl.h"
#include "db/dbformat.h"
// #include "env/mock_env.h"
#include "memtable/hash_linklist_rep.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/options_util.h"
#include "table/block_based_table_factory.h"
// #include "table/mock_table.h"
#include "table/plain_table_factory.h"
#include "table/scoped_arena_iterator.h"
#include "util/compression.h"
#include "util/filename.h"
#include "util/mutexlock.h"

#include "util/string_util.h"
#include "util/sync_point.h"
#include "utilities/merge_operators.h"

#include "Common.h"
#include "db/column_family.h"
#include "env.h"
#include "iterator.h"
#include "util/coding.h"
#include "util/testutil.h"
#include "rocksdb/snapshot.h"

//#include "db/write_controller.h"
// #include "db/version_set.h"

namespace rocksdb {

const Snapshot* ManagedSnapshot::snapshot() {
	return snapshot_;
}

ManagedSnapshot::ManagedSnapshot(DB* db) : db_(db),
	snapshot_(db->GetSnapshot()) {}
ManagedSnapshot::~ManagedSnapshot() {
	db_->ReleaseSnapshot(snapshot_);
}

enum SkipPolicy { kSkipNone = 0, kSkipNoSnapshot = 1, kSkipNoPrefix = 2 };
struct OptionsOverride {
	std::shared_ptr<const FilterPolicy> filter_policy = nullptr;
	// These will be used only if filter_policy is set
	bool partition_filters = false;
	uint64_t metadata_block_size = 1024;
	BlockBasedTableOptions::IndexType index_type =
	    BlockBasedTableOptions::IndexType::kBinarySearch;

	// Used as a bit mask of individual enums in which to skip an XF test point
	int skip_policy = 0;
};

class AtomicCounter {
public:
	explicit AtomicCounter(Env* env = NULL)
		: env_(env), cond_count_(&mu_), count_(0) {}

	void Increment() {
		MutexLock l(&mu_);
		count_++;
		cond_count_.SignalAll();
	}

	int Read() {
		MutexLock l(&mu_);
		return count_;
	}

	bool WaitFor(int count) {
		MutexLock l(&mu_);

		uint64_t start = env_->NowMicros();
		while (count_ < count) {
			uint64_t now = env_->NowMicros();
			cond_count_.TimedWait(now + /*1s*/ 1 * 1000 * 1000);
			if (env_->NowMicros() - start > /*10s*/ 10 * 1000 * 1000) {
				return false;
			}
			if (count_ < count) {
				GTEST_LOG_(WARNING) << "WaitFor is taking more time than usual";
			}
		}

		return true;
	}

	void Reset() {
		MutexLock l(&mu_);
		count_ = 0;
		cond_count_.SignalAll();
	}

private:
	Env* env_;
	port::Mutex mu_;
	port::CondVar cond_count_;
	int count_;
};

// Special Env used to delay background operations
class SpecialEnv : public EnvWrapper {
public:
	explicit SpecialEnv(Env* base);

	Status NewWritableFile(const std::string& f, unique_ptr<WritableFile>* r,
	                       const EnvOptions& soptions) override {
		class SSTableFile : public WritableFile {
		private:
			SpecialEnv* env_;
			unique_ptr<WritableFile> base_;

		public:
			SSTableFile(SpecialEnv* env, unique_ptr<WritableFile>&& base)
				: env_(env), base_(std::move(base)) {}
			Status Append(const Slice& data) override {
				if (env_->table_write_callback_) {
					(*env_->table_write_callback_)();
				}
				if (env_->drop_writes_.load(std::memory_order_acquire)) {
					// Drop writes on the floor
					return Status::OK();
				} else if (env_->no_space_.load(std::memory_order_acquire)) {
					return Status::NoSpace("No space left on device");
				} else {
					env_->bytes_written_ += data.size();
					return base_->Append(data);
				}
			}
			Status PositionedAppend(const Slice& data, uint64_t offset) override {
				if (env_->table_write_callback_) {
					(*env_->table_write_callback_)();
				}
				if (env_->drop_writes_.load(std::memory_order_acquire)) {
					// Drop writes on the floor
					return Status::OK();
				} else if (env_->no_space_.load(std::memory_order_acquire)) {
					return Status::NoSpace("No space left on device");
				} else {
					env_->bytes_written_ += data.size();
					return base_->PositionedAppend(data, offset);
				}
			}
			Status Truncate(uint64_t size) override { return base_->Truncate(size); }
 			Status Close() override {
// SyncPoint is not supported in Released Windows Mode.
//#if !(defined NDEBUG) || !defined(OS_WIN)
				// Check preallocation size
				// preallocation size is never passed to base file.
				size_t preallocation_size = preallocation_block_size();
/* 				TEST_SYNC_POINT_CALLBACK("DBTestWritableFile.GetPreallocationStatus",
				&preallocation_size) */;
//#endif  // !(defined NDEBUG) || !defined(OS_WIN)
				return base_->Close();
			} 
			Status Flush() override { return base_->Flush(); }
			Status Sync() override {
				++env_->sync_counter_;
				while (env_->delay_sstable_sync_.load(std::memory_order_acquire)) {
					env_->SleepForMicroseconds(100000);
				}
				return base_->Sync();
			}
			void SetIOPriority(Env::IOPriority pri) override {
				base_->SetIOPriority(pri);
			}
			Env::IOPriority GetIOPriority() override {
				return base_->GetIOPriority();
			}
			bool use_direct_io() const override {
				return base_->use_direct_io();
			}
			Status Allocate(uint64_t offset, uint64_t len) override {
				return base_->Allocate(offset, len);
			}
		};
		class ManifestFile : public WritableFile {
		public:
			ManifestFile(SpecialEnv* env, unique_ptr<WritableFile>&& b)
				: env_(env), base_(std::move(b)) {}
			Status Append(const Slice& data) override {
				if (env_->manifest_write_error_.load(std::memory_order_acquire)) {
					return Status::IOError("simulated writer error");
				} else {
					return base_->Append(data);
				}
			}
			Status Truncate(uint64_t size) override { return base_->Truncate(size); }
			Status Close() override { return base_->Close(); }
			Status Flush() override { return base_->Flush(); }
			Status Sync() override {
				++env_->sync_counter_;
				if (env_->manifest_sync_error_.load(std::memory_order_acquire)) {
					return Status::IOError("simulated sync error");
				} else {
					return base_->Sync();
				}
			}
			uint64_t GetFileSize() override { return base_->GetFileSize(); }

		private:
			SpecialEnv* env_;
			unique_ptr<WritableFile> base_;
		};
		class WalFile : public WritableFile {
		public:
			WalFile(SpecialEnv* env, unique_ptr<WritableFile>&& b)
				: env_(env), base_(std::move(b)) {
				env_->num_open_wal_file_.fetch_add(1);
			}
			virtual ~WalFile() {
				env_->num_open_wal_file_.fetch_add(-1);
			}
			Status Append(const Slice& data) override {
#if !(defined NDEBUG) || !defined(OS_WIN)
				//TEST_SYNC_POINT("SpecialEnv::WalFile::Append:1");
#endif
				Status s;
				if (env_->log_write_error_.load(std::memory_order_acquire)) {
					s = Status::IOError("simulated writer error");
				} else {
					int slowdown =
					env_->log_write_slowdown_.load(std::memory_order_acquire);
					if (slowdown > 0) {
						env_->SleepForMicroseconds(slowdown);
					}
					s = base_->Append(data);
				}
//#if !(defined NDEBUG) || !defined(OS_WIN)
				//TEST_SYNC_POINT("SpecialEnv::WalFile::Append:2");
//#endif
				return s;
			}
			Status Truncate(uint64_t size) override { return base_->Truncate(size); }
			Status Close() override {
// SyncPoint is not supported in Released Windows Mode.
//#if !(defined NDEBUG) || !defined(OS_WIN)
				// Check preallocation size
				// preallocation size is never passed to base file.
				size_t preallocation_size = preallocation_block_size();
/* 				TEST_SYNC_POINT_CALLBACK("DBTestWalFile.GetPreallocationStatus",
				&preallocation_size); */
//#endif  // !(defined NDEBUG) || !defined(OS_WIN)

				return base_->Close();
			}
			Status Flush() override { return base_->Flush(); }
			Status Sync() override {
				++env_->sync_counter_;
				return base_->Sync();
			}
			bool IsSyncThreadSafe() const override {
				return env_->is_wal_sync_thread_safe_.load();
			}

		private:
			SpecialEnv* env_;
			unique_ptr<WritableFile> base_;
		};

		if (non_writeable_rate_.load(std::memory_order_acquire) > 0) {
			uint32_t random_number;
			{
				MutexLock l(&rnd_mutex_);
				random_number = rnd_.Uniform(100);
			}
			if (random_number < non_writeable_rate_.load()) {
				return Status::IOError("simulated random write error");
			}
		}

		new_writable_count_++;

		if (non_writable_count_.load() > 0) {
			non_writable_count_--;
			return Status::IOError("simulated write error");
		}

		EnvOptions optimized = soptions;
		if (strstr(f.c_str(), "MANIFEST") != nullptr ||
		        strstr(f.c_str(), "log") != nullptr) {
			optimized.use_mmap_writes = false;
			optimized.use_direct_writes = false;
		}

		Status s = target()->NewWritableFile(f, r, optimized);
		if (s.ok()) {
			if (strstr(f.c_str(), ".sst") != nullptr) {
				r->reset(new SSTableFile(this, std::move(*r)));
			} else if (strstr(f.c_str(), "MANIFEST") != nullptr) {
				r->reset(new ManifestFile(this, std::move(*r)));
			} else if (strstr(f.c_str(), "log") != nullptr) {
				r->reset(new WalFile(this, std::move(*r)));
			}
		}
		return s;
	}

	Status NewRandomAccessFile(const std::string& f,
	                           unique_ptr<RandomAccessFile>* r,
	                           const EnvOptions& soptions) override {
		class CountingFile : public RandomAccessFile {
		public:
			CountingFile(unique_ptr<RandomAccessFile>&& target,
			AtomicCounter* counter,
			std::atomic<size_t>* bytes_read)
				: target_(std::move(target)),
				counter_(counter),
				bytes_read_(bytes_read) {}
			virtual Status Read(uint64_t offset, size_t n, Slice* result,
			char* scratch) const override {
				counter_->Increment();
				Status s = target_->Read(offset, n, result, scratch);
				*bytes_read_ += result->size();
				return s;
			}

		private:
			unique_ptr<RandomAccessFile> target_;
			AtomicCounter* counter_;
			std::atomic<size_t>* bytes_read_;
		};

		Status s = target()->NewRandomAccessFile(f, r, soptions);
		random_file_open_counter_++;
		if (s.ok() && count_random_reads_) {
			r->reset(new CountingFile(std::move(*r), &random_read_counter_,
			                          &random_read_bytes_counter_));
		}
		return s;
	}

	Status NewSequentialFile(const std::string& f, unique_ptr<SequentialFile>* r,
	                         const EnvOptions& soptions) override {
		class CountingFile : public SequentialFile {
		public:
			CountingFile(unique_ptr<SequentialFile>&& target,
			AtomicCounter* counter)
			// anon::AtomicCounter* counter)
				: target_(std::move(target)), counter_(counter) {}
			virtual Status Read(size_t n, Slice* result, char* scratch) override {
				counter_->Increment();
				return target_->Read(n, result, scratch);
			}
			virtual Status Skip(uint64_t n) override { return target_->Skip(n); }

		private:
			unique_ptr<SequentialFile> target_;
			AtomicCounter* counter_;
			// anon::AtomicCounter* counter_;
		};

		Status s = target()->NewSequentialFile(f, r, soptions);
		if (s.ok() && count_sequential_reads_) {
			r->reset(new CountingFile(std::move(*r), &sequential_read_counter_));
		}
		return s;
	}

	virtual void SleepForMicroseconds(int micros) override {
		sleep_counter_.Increment();
		if (no_slowdown_ || time_elapse_only_sleep_) {
			addon_time_.fetch_add(micros);
		}
		if (!no_slowdown_) {
			target()->SleepForMicroseconds(micros);
		}
	}

	virtual Status GetCurrentTime(int64_t* unix_time) override {
		Status s;
		if (!time_elapse_only_sleep_) {
			s = target()->GetCurrentTime(unix_time);
		}
		if (s.ok()) {
			*unix_time += addon_time_.load();
		}
		return s;
	}

	virtual uint64_t NowNanos() override {
		return (time_elapse_only_sleep_ ? 0 : target()->NowNanos()) +
		addon_time_.load() * 1000;
	}

	virtual uint64_t NowMicros() override {
		return (time_elapse_only_sleep_ ? 0 : target()->NowMicros()) +
		addon_time_.load();
	}

	virtual Status DeleteFile(const std::string& fname) override {
		delete_count_.fetch_add(1);
		return target()->DeleteFile(fname);
	}
	Random rnd_;
	port::Mutex rnd_mutex_;  // Lock to pretect rnd_
	std::function<void()>* table_write_callback_;
	// Drop writes on the floor while this pointer is non-nullptr.
	std::atomic<bool> drop_writes_;
	// Simulate no-space errors while this pointer is non-nullptr.
	std::atomic<bool> no_space_;
	std::atomic<int64_t> bytes_written_;
	std::atomic<int> sync_counter_;
	// sstable Sync() calls are blocked while this pointer is non-nullptr.
	std::atomic<bool> delay_sstable_sync_;
	// Force sync of manifest files to fail while this pointer is non-nullptr
	std::atomic<bool> manifest_sync_error_;
	// Number of WAL files that are still open for write.
	std::atomic<int> num_open_wal_file_;
	// Force write to log files to fail while this pointer is non-nullptr
	std::atomic<bool> log_write_error_;

	// Slow down every log write, in micro-seconds.
	std::atomic<int> log_write_slowdown_;
	std::atomic<bool> is_wal_sync_thread_safe_ {true};
	std::atomic<bool> manifest_write_error_;
	std::atomic<uint32_t> non_writeable_rate_;
	std::atomic<uint32_t> new_writable_count_;
	std::atomic<uint32_t> non_writable_count_;
	std::atomic<int> random_file_open_counter_;
	bool count_random_reads_;
	AtomicCounter random_read_counter_;
	AtomicCounter sleep_counter_;
	std::atomic<size_t> random_read_bytes_counter_;
	bool no_slowdown_;
	bool time_elapse_only_sleep_;
	std::atomic<int64_t> addon_time_;
	std::atomic<int> delete_count_;
	bool count_sequential_reads_;
	AtomicCounter sequential_read_counter_;
};

SpecialEnv::SpecialEnv(Env* base)
    : EnvWrapper(base),
      rnd_(301),
      sleep_counter_(this),
      addon_time_(0),
      time_elapse_only_sleep_(false),
      no_slowdown_(false) {
  delay_sstable_sync_.store(false, std::memory_order_release);
  drop_writes_.store(false, std::memory_order_release);
  no_space_.store(false, std::memory_order_release);
  //non_writable_.store(false, std::memory_order_release);
  count_random_reads_ = false;
  count_sequential_reads_ = false;
  manifest_sync_error_.store(false, std::memory_order_release);
  manifest_write_error_.store(false, std::memory_order_release);
  log_write_error_.store(false, std::memory_order_release);
  random_file_open_counter_.store(0, std::memory_order_relaxed);
  delete_count_.store(0, std::memory_order_relaxed);
  num_open_wal_file_.store(0);
  log_write_slowdown_ = 0;
  bytes_written_ = 0;
  sync_counter_ = 0;
  non_writeable_rate_ = 0;
  new_writable_count_ = 0;
  non_writable_count_ = 0;
  table_write_callback_ = nullptr;
}

class DBTestBase : public testing::Test {
public:
	// Sequence of option configurations to try
enum OptionConfig :
	int {
		kDefault = 0,
		kBlockBasedTableWithPrefixHashIndex = 1,
		kBlockBasedTableWithWholeKeyHashIndex = 2,
		kPlainTableFirstBytePrefix = 3,
		kPlainTableCappedPrefix = 4,
		kPlainTableCappedPrefixNonMmap = 5,
		kPlainTableAllBytesPrefix = 6,
		kVectorRep = 7,
		kHashLinkList = 8,
		kHashCuckoo = 9,
		kMergePut = 10,
		kFilter = 11,
		kFullFilterWithNewTableReaderForCompactions = 12,
		kUncompressed = 13,
		kNumLevel_3 = 14,
		kDBLogDir = 15,
		kWalDirAndMmapReads = 16,
		kManifestFileSize = 17,
		kPerfOptions = 18,
		kHashSkipList = 19,
		kUniversalCompaction = 20,
		kUniversalCompactionMultiLevel = 21,
		kCompressedBlockCache = 22,
		kInfiniteMaxOpenFiles = 23,
		kxxHashChecksum = 24,
		kFIFOCompaction = 25,
		kOptimizeFiltersForHits = 26,
		kRowCache = 27,
		kRecycleLogFiles = 28,
		kConcurrentSkipList = 29,
		kPipelinedWrite = 30,
		kEnd = 31,
		kDirectIO = 32,
		kLevelSubcompactions = 33,
		kUniversalSubcompactions = 34,
		kBlockBasedTableWithIndexRestartInterval = 35,
		kBlockBasedTableWithPartitionedIndex = 36,
		kPartitionedFilterWithNewTableReaderForCompactions = 37,
		kDifferentDataDir,
		kWalDir,
		kSyncWal,
		kWalDirSyncWal,
		kMultiLevels,

	};

public:
	std::string dbname_;
	std::string alternative_wal_dir_;
	std::string alternative_db_log_dir_;
//  MockEnv* mem_env_;
	SpecialEnv* env_;
	DB* db_;
	std::vector<ColumnFamilyHandle*> handles_;

	int option_config_;
	Options last_options_;

	// Skip some options, as they may not be applicable to a specific test.
	// To add more skip constants, use values 4, 8, 16, etc.
	enum OptionSkip {
		kNoSkip = 0,
		kSkipDeletesFilterFirst = 1,
		kSkipUniversalCompaction = 2,
		kSkipMergePut = 4,
		kSkipPlainTable = 8,
		kSkipHashIndex = 16,
		kSkipNoSeekToLast = 32,
		kSkipHashCuckoo = 64,
		kSkipFIFOCompaction = 128,
		kSkipMmapReads = 256,
	};

	// Do n memtable compactions, each of which produces an sstable
// covering the range [small,large].
	void MakeTables(int n, const std::string& small,
	                const std::string& large, int cf) {
		for (int i = 0; i < n; i++) {
			ASSERT_OK(Put(cf, small, "begin"));
			ASSERT_OK(Put(cf, large, "end"));
			ASSERT_OK(Flush(cf));
			MoveFilesToLevel(n - i - 1, cf);
		}
	}

//modify: need to complete the function
	void MoveFilesToLevel(int level, int cf) {
		/*   for (int l = 0; l < level; ++l) {
		    if (cf > 0) {
		      dbfull()->TEST_CompactRange(l, nullptr, nullptr, handles_[cf]);
		    } else {
		      dbfull()->TEST_CompactRange(l, nullptr, nullptr);
		    }
		  } */
	}

// Prevent pushing of new sstables into deeper levels by adding
// tables that cover a specified range to all levels.
	void FillLevels(const std::string& smallest,
	                const std::string& largest, int cf) {
		(db_->NumberLevels(handles_[cf]), smallest, largest, cf);
	}
	void Close() {
        for (auto h : handles_) {
			db_->DestroyColumnFamilyHandle(h);
		}
		handles_.clear();
		delete db_;
		db_ = nullptr;
	}
	// Return the current option configuration.
	Options CurrentOptions() {
		Options options;
		//options.env = env_;
		options.create_if_missing = true;
		return options;
	}

	Options GetDefaultOptions() {
		Options options;
		options.write_buffer_size = 4090 * 4096;
		options.target_file_size_base = 2 * 1024 * 1024;
		options.max_bytes_for_level_base = 10 * 1024 * 1024;
		options.max_open_files = 5000;
		options.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
		options.compaction_pri = CompactionPri::kByCompensatedSize;
		return options;
	}

	Options CurrentOptions(const OptionsOverride& options_override = OptionsOverride()) const {
		return GetOptions(option_config_, GetDefaultOptions(), options_override);
	}

	Options CurrentOptions(const Options& default_options, const OptionsOverride& options_override = OptionsOverride()) const {
		default_options = GetDefaultOptions();
		return GetOptions(option_config_, default_options, options_override);
	}

	Options GetOptions(
	    int option_config, const Options& default_options,
	    const OptionsOverride& options_override) const {
		// this redundant copy is to minimize code change w/o having lint error.
		default_options = GetDefaultOptions();
		Options options = default_options;
		BlockBasedTableOptions table_options;
		bool set_block_based_table_factory = true;
//#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) &&  \
  !defined(OS_AIX)
/* 		SyncPoint::GetInstance()->ClearCallBack(
		    "NewRandomAccessFile:O_DIRECT");
		SyncPoint::GetInstance()->ClearCallBack(
		    "NewWritableFile:O_DIRECT"); */
//#endif

		switch (option_config) {
#ifndef ROCKSDB_LITE
		case kHashSkipList:
			options.prefix_extractor.reset(NewFixedPrefixTransform(1));
			options.memtable_factory.reset(NewHashSkipListRepFactory(16));
			options.allow_concurrent_memtable_write = false;
			break;
		case kPlainTableFirstBytePrefix:
			options.table_factory.reset(new PlainTableFactory());
			options.prefix_extractor.reset(NewFixedPrefixTransform(1));
			options.allow_mmap_reads = true;
			options.max_sequential_skip_in_iterations = 999999;
			set_block_based_table_factory = false;
			break;
		case kPlainTableCappedPrefix:
			options.table_factory.reset(new PlainTableFactory());
			options.prefix_extractor.reset(NewCappedPrefixTransform(8));
			options.allow_mmap_reads = true;
			options.max_sequential_skip_in_iterations = 999999;
			set_block_based_table_factory = false;
			break;
		case kPlainTableCappedPrefixNonMmap:
			options.table_factory.reset(new PlainTableFactory());
			options.prefix_extractor.reset(NewCappedPrefixTransform(8));
			options.allow_mmap_reads = false;
			options.max_sequential_skip_in_iterations = 999999;
			set_block_based_table_factory = false;
			break;
		case kPlainTableAllBytesPrefix:
			options.table_factory.reset(new PlainTableFactory());
			options.prefix_extractor.reset(NewNoopTransform());
			options.allow_mmap_reads = true;
			options.max_sequential_skip_in_iterations = 999999;
			set_block_based_table_factory = false;
			break;
		case kVectorRep:
			options.memtable_factory.reset(new VectorRepFactory(100));
			options.allow_concurrent_memtable_write = false;
			break;
		case kHashLinkList:
			options.prefix_extractor.reset(NewFixedPrefixTransform(1));
			options.memtable_factory.reset(
			    NewHashLinkListRepFactory(4, 0, 3, true, 4));
			options.allow_concurrent_memtable_write = false;
			break;
		case kHashCuckoo:
			options.memtable_factory.reset(
			    NewHashCuckooRepFactory(options.write_buffer_size));
			options.allow_concurrent_memtable_write = false;
			break;
#endif  // ROCKSDB_LITE
		case kMergePut:
			options.merge_operator = MergeOperators::CreatePutOperator();
			break;
		case kFilter:
			table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
			break;
		case kFullFilterWithNewTableReaderForCompactions:
			table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
			options.new_table_reader_for_compaction_inputs = true;
			options.compaction_readahead_size = 10 * 1024 * 1024;
			break;
		case kPartitionedFilterWithNewTableReaderForCompactions:
			table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
			table_options.partition_filters = true;
			table_options.index_type =
			    BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
			options.new_table_reader_for_compaction_inputs = true;
			options.compaction_readahead_size = 10 * 1024 * 1024;
			break;
		case kUncompressed:
			options.compression = kNoCompression;
			break;
		case kNumLevel_3:
			options.num_levels = 3;
			break;
		case kDBLogDir:
			options.db_log_dir = alternative_db_log_dir_;
			break;
		case kWalDirAndMmapReads:
			options.wal_dir = alternative_wal_dir_;
			// mmap reads should be orthogonal to WalDir setting, so we piggyback to
			// this option config to test mmap reads as well
			options.allow_mmap_reads = true;
			break;
		case kManifestFileSize:
			options.max_manifest_file_size = 50;  // 50 bytes
			break;
		case kPerfOptions:
			options.soft_rate_limit = 2.0;
			options.delayed_write_rate = 8 * 1024 * 1024;
			options.report_bg_io_stats = true;
			// TODO(3.13) -- test more options
			break;
		case kUniversalCompaction:
			options.compaction_style = kCompactionStyleUniversal;
			options.num_levels = 1;
			break;
		case kUniversalCompactionMultiLevel:
			options.compaction_style = kCompactionStyleUniversal;
			options.max_subcompactions = 4;
			options.num_levels = 8;
			break;
		case kCompressedBlockCache:
			options.allow_mmap_writes = true;
			table_options.block_cache_compressed = NewLRUCache(8 * 1024 * 1024);
			break;
		case kInfiniteMaxOpenFiles:
			options.max_open_files = -1;
			break;
		case kxxHashChecksum: {
			table_options.checksum = kxxHash;
			break;
		}
		case kFIFOCompaction: {
			options.compaction_style = kCompactionStyleFIFO;
			break;
		}
		case kBlockBasedTableWithPrefixHashIndex: {
			table_options.index_type = BlockBasedTableOptions::kHashSearch;
			options.prefix_extractor.reset(NewFixedPrefixTransform(1));
			break;
		}
		case kBlockBasedTableWithWholeKeyHashIndex: {
			table_options.index_type = BlockBasedTableOptions::kHashSearch;
			options.prefix_extractor.reset(NewNoopTransform());
			break;
		}
		case kBlockBasedTableWithPartitionedIndex: {
			table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
			options.prefix_extractor.reset(NewNoopTransform());
			break;
		}
		case kBlockBasedTableWithIndexRestartInterval: {
			table_options.index_block_restart_interval = 8;
			break;
		}
		case kOptimizeFiltersForHits: {
			options.optimize_filters_for_hits = true;
			set_block_based_table_factory = true;
			break;
		}
		case kRowCache: {
			options.row_cache = NewLRUCache(1024 * 1024);
			break;
		}
		case kRecycleLogFiles: {
			options.recycle_log_file_num = 2;
			break;
		}
		case kLevelSubcompactions: {
			options.max_subcompactions = 4;
			break;
		}
		case kUniversalSubcompactions: {
			options.compaction_style = kCompactionStyleUniversal;
			options.num_levels = 8;
			options.max_subcompactions = 4;
			break;
		}
		case kConcurrentSkipList: {
			options.allow_concurrent_memtable_write = true;
			options.enable_write_thread_adaptive_yield = true;
			break;
		}
		case kDirectIO: {
			options.use_direct_reads = true;
			options.use_direct_io_for_flush_and_compaction = true;
			options.compaction_readahead_size = 2 * 1024 * 1024;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
!defined(OS_AIX)
			rocksdb::SyncPoint::GetInstance()->SetCallBack(
			"NewWritableFile:O_DIRECT", [&](void* arg) {
				int* val = static_cast<int*>(arg);
				*val &= ~O_DIRECT;
			});
			rocksdb::SyncPoint::GetInstance()->SetCallBack(
			"NewRandomAccessFile:O_DIRECT", [&](void* arg) {
				int* val = static_cast<int*>(arg);
				*val &= ~O_DIRECT;
			});
			rocksdb::SyncPoint::GetInstance()->EnableProcessing();
#endif
			break;
		}
		case kPipelinedWrite: {
			options.enable_pipelined_write = true;
			break;
		}

		default:
			break;
		}

		if (options_override.filter_policy) {
			table_options.filter_policy = options_override.filter_policy;
			table_options.partition_filters = options_override.partition_filters;
			table_options.metadata_block_size = options_override.metadata_block_size;
		}
		if (set_block_based_table_factory) {
			options.table_factory.reset(NewBlockBasedTableFactory(table_options));
		}
		options.env = env_;
		options.create_if_missing = true;
		options.fail_if_options_file_error = true;
		return options;
	}

	Status TryReopen(const Options& options) {
		Close();
		last_options_.table_factory.reset();
		// Note: operator= is an unsafe approach here since it destructs shared_ptr in
		// the same order of their creation, in contrast to destructors which
		// destructs them in the opposite order of creation. One particular problme is
		// that the cache destructor might invoke callback functions that use Option
		// members such as statistics. To work around this problem, we manually call
		// destructor of table_facotry which eventually clears the block cache.
		last_options_ = options;
		return DB::Open(options, dbname_, &db_);
	}

	void Reopen(const Options& options) {
		ASSERT_OK(TryReopen(options));
	}
	DBTestBase(const std::string path)
		: env_(new SpecialEnv(Env::Default())), 
		option_config_(kDefault) {
		Options options;
	    options.create_if_missing = true;		
		// env_->SetBackgroundThreads(1, Env::LOW);
		// env_->SetBackgroundThreads(1, Env::HIGH);
 		//dbname_ = "/tmp";
		//dbname_ += path; 
		dbname_ = test::TmpDir(env_) + path;
		alternative_wal_dir_ = dbname_ + "/wal";
	     alternative_db_log_dir_ = dbname_ + "/db_log_dir";
		//auto options = CurrentOptions();
		options.env = env_;
		auto delete_options = options;
		delete_options.wal_dir = alternative_wal_dir_;  

		db_ = nullptr;
		Reopen(options);
		//Random::GetTLSInstance()->Reset(0xdeadbeef);
	}

	~DBTestBase() {
		/* 		rocksdb::SyncPoint::GetInstance()->DisableProcessing();
				rocksdb::SyncPoint::GetInstance()->LoadDependency( {});
			rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks(); */
			
		Close();
		Options options;
/*  		options.db_paths.emplace_back(dbname_, 0);
		options.db_paths.emplace_back(dbname_ + "_2", 0);
		options.db_paths.emplace_back(dbname_ + "_3", 0);
		options.db_paths.emplace_back(dbname_ + "_4", 0);
		options.env = env_;  */
     
 		if (getenv("KEEP_DB")) {
			printf("DB is still at %s\n", dbname_.c_str());
		} else {
			EXPECT_OK(DestroyDB(dbname_, options));
		} 
		//delete env_;
	};

	Status Flush(int cf = 0) {
		if (cf == 0) {
			return db_->Flush(FlushOptions());
		} else {
			return db_->Flush(FlushOptions(), handles_[cf]);
		}
	}

	Status DBTestBase::Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions()) {
		return db_->Put(wo, k, v);
	}

	Status DBTestBase::Put(int cf, const Slice& k, const Slice& v,
	           WriteOptions wo = WriteOptions()) {
	    cout << "the put cf is " <<cf<<endl;
		return db_->Put(wo, handles_[cf], k, v);
	}

	Status Delete(const std::string& k) {
		return db_->Delete(WriteOptions(), k);
	}

	Status Delete(int cf, const std::string& k) {
		return db_->Delete(WriteOptions(), handles_[cf], k);
	}

	std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
		ReadOptions options;
		options.verify_checksums = true;
		options.snapshot = snapshot;
		std::string result;
		Status s = db_->Get(options, k, &result);
		if (s.IsNotFound()) {
			result = "NOT_FOUND";
		} else if (!s.ok()) {
			result = s.ToString();
		}
		return result;
	}

	std::string Get(int cf, const std::string& k,
	                const Snapshot* snapshot = nullptr) {
		ReadOptions options;
		options.verify_checksums = true;
		options.snapshot = snapshot;
		std::string result;
		Status s = db_->Get(options, handles_[cf], k, &result);
		if (s.IsNotFound()) {
			result = "NOT_FOUND";
		} else if (!s.ok()) {
			result = s.ToString();
		}
		return result;
	}
	Status ReadOnlyReopen(const Options& options) {
		return DB::OpenForReadOnly(options, dbname_, &db_);
	}

	std::string DummyString(size_t len, char c) {
		return std::string(len, c);
	}

	int NumTableFilesAtLevel(int level, int cf) {
		std::string property;
		if (cf == 0) {
			// default cfd
			EXPECT_TRUE(db_->GetProperty(
			                "rocksdb.num-files-at-level" + NumberToString(level), &property));
		} else {
			EXPECT_TRUE(db_->GetProperty(
			                handles_[cf], "rocksdb.num-files-at-level" + NumberToString(level),
			                &property));
		}
		return atoi(property.c_str());
	}

	DBImpl* dbfull() {
		return reinterpret_cast<DBImpl*>(db_);
	}

	Status TryReopenWithColumnFamilies(
	    const std::vector<std::string>& cfs, const std::vector<Options>& options) {
		Close();
		EXPECT_EQ(cfs.size(), options.size());
		std::vector<ColumnFamilyDescriptor> column_families;
		for (size_t i = 0; i < cfs.size(); ++i) {
			column_families.push_back(ColumnFamilyDescriptor(cfs[i], options[i]));
		}
		DBOptions db_opts = DBOptions(options[0]);
		return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
	}

	Status TryReopenWithColumnFamilies(
	    const std::vector<std::string>& cfs, const Options& options) {
		Close();
		std::vector<Options> v_opts(cfs.size(), options);
		return TryReopenWithColumnFamilies(cfs, v_opts);
	}

	void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
	                              const std::vector<Options>& options) {
		ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
	}

	void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
	                              const Options& options) {
		ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
	}

	void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
	                           const Options& options) {
		CreateColumnFamilies(cfs, options);
		std::vector<std::string> cfs_plus_default = cfs;
		cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
		ReopenWithColumnFamilies(cfs_plus_default, options);
	}

	void CreateColumnFamilies(const std::vector<std::string>& cfs,
	                          const Options& options) {
		ColumnFamilyOptions cf_opts(options);
		size_t cfi = handles_.size();
		handles_.resize(cfi + cfs.size());
        for (auto cf : cfs) {
			cout << "all cf is  "	<<cf<<endl;	
			ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
		}
	}
	
	Status DBTestBase::SingleDelete(const std::string& k) {
  return db_->SingleDelete(WriteOptions(), k);
}

Status DBTestBase::SingleDelete(int cf, const std::string& k) {
  return db_->SingleDelete(WriteOptions(), handles_[cf], k);
}

	bool ShouldSkipOptions(int option_config, int skip_mask) {
#ifdef ROCKSDB_LITE
		// These options are not supported in ROCKSDB_LITE
		if (option_config == kHashSkipList ||
		        option_config == kPlainTableFirstBytePrefix ||
		        option_config == kPlainTableCappedPrefix ||
		        option_config == kPlainTableCappedPrefixNonMmap ||
		        option_config == kPlainTableAllBytesPrefix ||
		        option_config == kVectorRep || option_config == kHashLinkList ||
		        option_config == kHashCuckoo || option_config == kUniversalCompaction ||
		        option_config == kUniversalCompactionMultiLevel ||
		        option_config == kUniversalSubcompactions ||
		        option_config == kFIFOCompaction ||
		        option_config == kConcurrentSkipList) {
			return true;
		}
#endif

		if ((skip_mask & kSkipUniversalCompaction) &&
		        (option_config == kUniversalCompaction ||
		         option_config == kUniversalCompactionMultiLevel)) {
			return true;
		}
		if ((skip_mask & kSkipMergePut) && option_config == kMergePut) {
			return true;
		}
		if ((skip_mask & kSkipNoSeekToLast) &&
		        (option_config == kHashLinkList || option_config == kHashSkipList)) {
			return true;
		}
		if ((skip_mask & kSkipPlainTable) &&
		        (option_config == kPlainTableAllBytesPrefix ||
		         option_config == kPlainTableFirstBytePrefix ||
		         option_config == kPlainTableCappedPrefix ||
		         option_config == kPlainTableCappedPrefixNonMmap)) {
			return true;
		}
		if ((skip_mask & kSkipHashIndex) &&
		        (option_config == kBlockBasedTableWithPrefixHashIndex ||
		         option_config == kBlockBasedTableWithWholeKeyHashIndex)) {
			return true;
		}
		if ((skip_mask & kSkipHashCuckoo) && (option_config == kHashCuckoo)) {
			return true;
		}
		if ((skip_mask & kSkipFIFOCompaction) && option_config == kFIFOCompaction) {
			return true;
		}
		if ((skip_mask & kSkipMmapReads) && option_config == kWalDirAndMmapReads) {
			return true;
		}
		return false;
	}

	std::unique_ptr<Env> base_env_;
	
	bool ChangeOptions() {
		option_config_++;
		if (option_config_ >= kEnd) {
			return false;
		} else {
			if (option_config_ == kMultiLevels) {
				base_env_.reset(Env::Default());
				// base_env_.reset(new MockEnv(Env::Default()));
			}
			return true;
		}
	}
// Switch to a fresh database with the next option configuration to
// test.  Return false if there are no more configurations to test.
	bool ChangeOptions(int skip_mask) {
		cout <<"********************the changed op is : "<<skip_mask<<endl;
		for (option_config_++; option_config_ < kEnd; option_config_++) {
			if (ShouldSkipOptions(option_config_, skip_mask)) {
				continue;
			}
			break;
		}

		if (option_config_ >= kEnd) {
			Destroy(last_options_);
			return false;
		} else {
			auto options = CurrentOptions();
			options.create_if_missing = true;
			DestroyAndReopen(options);
			return true;
		}
	}

	void Destroy(const Options& options) {
		Close();
		ASSERT_OK(DestroyDB(dbname_, options));
	}

	void DestroyAndReopen(const Options& options) {
		// Destroy using last options
		Destroy(last_options_);
		ASSERT_OK(TryReopen(options));
	}

	std::string AllEntriesFor(const Slice& user_key, int cf) {
		//Arena arena;
		cout <<"we are here now 1 "<<endl;
		auto options = CurrentOptions();
		InternalKeyComparator icmp(options.comparator);
		RangeDelAggregator range_del_agg(icmp, {} /* snapshots */);
		cout <<"we are here now 2 "<<endl;
		ScopedArenaIterator iter;
		InternalKey target(user_key, kMaxSequenceNumber, kTypeValue);
		cout <<"we are here now 3 "<<endl;
		iter->Seek(target.Encode());
		cout <<"we are here now 4 "<<endl;
		std::string result;
		if (!iter->status().ok()) {
			result = iter->status().ToString();
		} else {
			result = "[ ";
			bool first = true;
			while (iter->Valid()) {
				ParsedInternalKey ikey(Slice(), 0, kTypeValue);
				if (!ParseInternalKey(iter->key(), &ikey)) {
					result += "CORRUPTED";
				} else {
					if (!last_options_.comparator->Equal(ikey.user_key, user_key)) {
						break;
					}
					if (!first) {
						result += ", ";
					}
					first = false;
					switch (ikey.type) {
					case kTypeValue:
						result += iter->value().ToString();
						break;
					case kTypeMerge:
						// keep it the same as kTypeValue for testing kMergePut
						result += iter->value().ToString();
						break;
					case kTypeDeletion:
						result += "DEL";
						break;
					case kTypeSingleDeletion:
						result += "SDEL";
						break;
					default:
						assert(false);
						break;
					}
				}
				iter->Next();
			}
			if (!first) {
				result += " ";
			}
			result += "]";
		}
		return result;
	}

	// Switch between different compaction styles.
	bool ChangeCompactOptions() {
		cout << "the OP config is : +++++++"<<option_config_<<endl;
		if (option_config_ == kDefault) {
			option_config_ = kUniversalCompaction;
			Destroy(last_options_);
			auto options = CurrentOptions();
			options.create_if_missing = true;
			TryReopen(options);
			return true;
		} else if (option_config_ == kUniversalCompaction) {
			option_config_ = kUniversalCompactionMultiLevel;
			Destroy(last_options_);
			auto options = CurrentOptions();
			options.create_if_missing = true;
			TryReopen(options);
			return true;
		} else if (option_config_ == kUniversalCompactionMultiLevel) {
			
			option_config_ = kLevelSubcompactions;
			Destroy(last_options_);
			auto options = CurrentOptions();
			assert(options.max_subcompactions >= 1);
			TryReopen(options);
			return true;
		} else if (option_config_ == kLevelSubcompactions) {
			option_config_ = kUniversalSubcompactions;
			Destroy(last_options_);
			auto options = CurrentOptions();
			assert(options.max_subcompactions >= 1);
			TryReopen(options);
			return true;
		} else {
			return false;
		}
	}

	uint64_t GetNumSnapshots() {
		uint64_t int_num;
		dbfull()->GetIntProperty("rocksdb.num-snapshots", &int_num);
		cout << "the snap number is :   "<<int_num<<endl;
		//EXPECT_TRUE(dbfull()->GetIntProperty("rocksdb.num-snapshots", &int_num));
		return int_num;
	}

	uint64_t GetTimeOldestSnapshots() {
		uint64_t int_num;
		EXPECT_TRUE(
		    dbfull()->GetIntProperty("rocksdb.oldest-snapshot-time", &int_num));
		return int_num;
	}

	std::vector<std::uint64_t> ListTableFiles(Env* env,
	        const std::string& path) {
		std::vector<std::string> files;
		std::vector<uint64_t> file_numbers;
		env->GetChildren(path, &files);
		uint64_t number;
		FileType type;
		cout <<"~~~~~~~~~~~~~~~~ListTableFiles~~~~~~"<<files.size()<<endl;
		for (size_t i = 0; i < files.size(); ++i) {
			if (ParseFileName(files[i], &number, &type)) {
				if (type == kTableFile) {
					file_numbers.push_back(number);
				}
			}
		}
		return file_numbers;
	}

};

class DBBasicTest : public DBTestBase {
public:
	DBBasicTest() : DBTestBase("/ddtest") {}
};


TEST_F(DBBasicTest, OpenWhenOpen) {
	Options options = CurrentOptions();
	//options.env = env_;
	DB* db2 = nullptr;
	Status s = DB::Open(options, dbname_, &db2);
	ASSERT_EQ(Status::Code::kOk, s.code());
   cout << "the 2nd open status is " <<s.ToString()<<endl;
	delete db2;
} 

TEST_F(DBBasicTest, PutSingleDeleteGet) {
		CreateAndReopenWithCF( {"pikachu"}, CurrentOptions());
		ASSERT_OK(Put(1, "foo", "v1"));
		ASSERT_EQ("v1", Get(1, "foo"));
		ASSERT_OK(Put(1, "foo2", "v2"));
		ASSERT_EQ("v2", Get(1, "foo2"));
		ASSERT_OK(SingleDelete(1, "foo"));
		ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
}

TEST_F(DBBasicTest, PutDeleteGet) {
    Options options;
	std::vector<ColumnFamilyDescriptor> column_families;
	 std::vector<std::string> column_families_names;	
	 
	CreateColumnFamilies({"pikachu"}, options);
	Status status = LoadLatestOptions(dbname_, rocksdb::Env::Default(), &options,
                            &column_families, false);
    cout << "LoadLatestOptions " << status.ToString() << endl;
	status = DB::ListColumnFamilies(static_cast<DBOptions>(options), dbname_, &column_families_names);
    if(!status.ok())
        cout << "DB ListColumnFamilies " << status.ToString() << " errno " << errno << endl;
    else
    {
        cout << "DB ListColumnFamilies " << status.ToString() << " count " << column_families_names.size() << endl;
        for (auto each_cf : column_families_names)
        {
            cout << each_cf.c_str() << endl;
        }
        cout << endl;
    } 
    status = DB::Open(options, dbname_, column_families, &handles_, &db_);
	cout << "DB open with pikachu column family " << status.ToString() << endl;
	cout <<"the handle 0 name is : "<< handles_[0]->GetName()<<endl;
//hainan	cout <<"the handle 0 name is : "<< handles_[1]->GetName()<<endl;
	ASSERT_OK(Put(0, "foo0", "v1"));
//hainan	ASSERT_OK(Put(1, "foo1", "v1"));
	ASSERT_EQ("v1", Get(0, "foo0"));
//hainan	ASSERT_EQ("v1", Get(1, "foo1"));
//hainan	ASSERT_EQ("NOT_FOUND", Get(1, "foo0"));
	ASSERT_OK(Flush(1));
	const Snapshot* s1 = db_->GetSnapshot();
	ASSERT_OK(Put(0, "foo0", "v2"));
//hainan	ASSERT_OK(Put(1, "foo1", "v2"));
//hainan    ASSERT_EQ("v1", Get(1, "foo1", s1));
	ASSERT_EQ("v1", Get(0, "foo0", s1));
//hainan    ASSERT_OK(SingleDelete(1, "foo1"));
//hainan	ASSERT_EQ("NOT_FOUND", Get(1, "foo1"));
	db_->ReleaseSnapshot(s1);
}
 
TEST_F(DBBasicTest, EmptyFlush) {

		Random rnd(301);
		Options options = CurrentOptions();
		options.disable_auto_compactions = true;
		CreateAndReopenWithCF( {"pikachu"}, options);
		Put(1, "a", Slice());
		ASSERT_OK(Flush(1));
         // delete key
        Status status = db_->Delete(WriteOptions(), handles_[1], "a");
        cout << "Delete in CF " << status.ToString() << endl;
        assert(status.ok());
} 

TEST_F(DBBasicTest, GetFromVersions) {
		CreateAndReopenWithCF( {"pikachu"}, CurrentOptions());
		ASSERT_OK(Put(1, "foo", "v1"));
		ASSERT_OK(Flush(1));
		ASSERT_EQ("v1", Get(1, "foo"));
		ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
} 

TEST_F(DBBasicTest, GetSnapshot) {
		CreateAndReopenWithCF( {"pikachu"}, CurrentOptions());
		for (int i = 0; i < 2; i++) {
			std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
			ASSERT_OK(Put(1, key, "v1"));
			const Snapshot* s1 = db_->GetSnapshot();
			if (option_config_ == kHashCuckoo) {
				// Unsupported case.
				ASSERT_TRUE(s1 == nullptr);
				break;
			}
			ASSERT_OK(Put(1, key, "v2"));
			ASSERT_EQ("v2", Get(1, key));
			ASSERT_EQ("v1", Get(1, key, s1));
			ASSERT_OK(Flush(1));
			ASSERT_EQ("v2", Get(1, key));
			ASSERT_EQ("v1", Get(1, key, s1));
			db_->ReleaseSnapshot(s1);
		}
} 

TEST_F(DBBasicTest, CheckLock) {
	do {
		DB* localdb;
		Options options = CurrentOptions();
		ASSERT_OK(TryReopen(options));

		// second open should fail
		ASSERT_TRUE((DB::Open(options, dbname_, &localdb)).ok());
	} while (ChangeOptions());
} 
 
TEST_F(DBBasicTest, FlushMultipleMemtable) {
	 do {
		Options options = CurrentOptions();
		WriteOptions writeOpt = WriteOptions();
		writeOpt.disableWAL = true;
		options.max_write_buffer_number = 4;
		options.min_write_buffer_number_to_merge = 3;
		options.max_write_buffer_number_to_maintain = -1;
		CreateAndReopenWithCF( {"pikachu"}, options);
		ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
		ASSERT_OK(Flush(1));
		ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

		ASSERT_EQ("v1", Get(1, "foo"));
		ASSERT_EQ("v1", Get(1, "bar"));
		ASSERT_OK(Flush(1));
	 } while (ChangeCompactOptions());
} 

TEST_F(DBBasicTest, FlushEmptyColumnFamily) {
	// Block flush thread and disable compaction thread
	Options options = CurrentOptions();
	// disable compaction
	options.disable_auto_compactions = true;
	WriteOptions writeOpt = WriteOptions();
	writeOpt.disableWAL = true;
	options.max_write_buffer_number = 2;
	options.min_write_buffer_number_to_merge = 1;
	options.max_write_buffer_number_to_maintain = 1;
	CreateAndReopenWithCF( {"pikachu"}, options);

	// Compaction can still go through even if no thread can flush the
	// mem table.
	ASSERT_OK(Flush(0));
	ASSERT_OK(Flush(1));

	// Insert can go through
	ASSERT_OK(dbfull()->Put(writeOpt, handles_[0], "foo", "v1"));
	ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

	ASSERT_EQ("v1", Get(0, "foo"));
	ASSERT_EQ("v1", Get(1, "bar"));
	ASSERT_OK(Flush(0));
	ASSERT_OK(Flush(1));
} 

 TEST_F(DBBasicTest, FLUSH) {
	do {
		CreateAndReopenWithCF( {"pikachu"}, CurrentOptions());
		WriteOptions writeOpt = WriteOptions();
		writeOpt.disableWAL = true;
		SetPerfLevel(kEnableTime);
		;
		ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
		// this will now also flush the last 2 writes
		ASSERT_OK(Flush(1));
		ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

		get_perf_context()->Reset();
		Get(1, "foo");
		
		ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time >= 0);

		ReopenWithColumnFamilies( {"default", "pikachu"}, CurrentOptions());
		ASSERT_EQ("v1", Get(1, "foo"));
		ASSERT_EQ("v1", Get(1, "bar"));

		writeOpt.disableWAL = true;
		ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
		ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));
		ASSERT_OK(Flush(1));

		ReopenWithColumnFamilies( {"default", "pikachu"}, CurrentOptions());
		ASSERT_EQ("v2", Get(1, "bar"));
		get_perf_context()->Reset();
		ASSERT_EQ("v2", Get(1, "foo"));
		ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time >= 0);

		writeOpt.disableWAL = false;
		ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
		ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));
		ASSERT_OK(Flush(1));

		ReopenWithColumnFamilies( {"default", "pikachu"}, CurrentOptions());
		// 'foo' should be there because its put
		// has WAL enabled.
		ASSERT_EQ("v3", Get(1, "foo"));
		ASSERT_EQ("v3", Get(1, "bar"));

		SetPerfLevel(kDisable);
	} while (ChangeCompactOptions());
}
 
TEST_F(DBBasicTest, ManifestRollOver) {
	do {
		Options options;
		options.max_manifest_file_size = 10;  // 10 bytes
		//modify no find function CurrentOptions(options)
		// options = CurrentOptions(options);
		CreateAndReopenWithCF( {"pikachu"}, options);
		{
			ASSERT_OK(Put(1, "manifest_key1", std::string(1000, '1')));
			ASSERT_OK(Put(1, "manifest_key2", std::string(1000, '2')));
			ASSERT_OK(Put(1, "manifest_key3", std::string(1000, '3')));
			//modify db_impl has no TEST_Current_Manifest_FileNo
			// uint64_t manifest_before_flush = dbfull()->TEST_Current_Manifest_FileNo();
			ASSERT_OK(Flush(1));  // This should trigger LogAndApply.
			// uint64_t manifest_after_flush = dbfull()->TEST_Current_Manifest_FileNo();
			// ASSERT_GT(manifest_after_flush, manifest_before_flush);
			ReopenWithColumnFamilies( {"default", "pikachu"}, options);
			// ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(), manifest_after_flush);
			// check if a new manifest file got inserted or not.
			ASSERT_EQ(std::string(1000, '1'), Get(1, "manifest_key1"));
			ASSERT_EQ(std::string(1000, '2'), Get(1, "manifest_key2"));
			ASSERT_EQ(std::string(1000, '3'), Get(1, "manifest_key3"));
		}
	} while (ChangeCompactOptions());
} 

TEST_F(DBBasicTest, IdentityAcrossRestarts) {
	do {
		std::string id1;
		ASSERT_OK(db_->GetDbIdentity(id1));	
		Options options = CurrentOptions();
		Reopen(options);
		std::string id2;
		ASSERT_OK(db_->GetDbIdentity(id2));
		// id1 should match id2 because identity was not regenerated
		ASSERT_EQ(id1.compare(id2), 0);

		std::string idfilename = IdentityFileName(dbname_);
		ASSERT_OK(env_->DeleteFile(idfilename));
		Reopen(options);
		std::string id3;
		ASSERT_OK(db_->GetDbIdentity(id3));
		// id1 should NOT match id3 because identity was regenerated
		ASSERT_NE(id1.compare(id3), 0);
	} while (ChangeCompactOptions());
} 

TEST_F(DBBasicTest, Snapshot) {
	OptionsOverride options_override;
	options_override.skip_policy = kSkipNoSnapshot;
	//do {
		CreateAndReopenWithCF( {"pikachu"}, CurrentOptions(options_override));
		Put(0, "foo", "0v1");
		
		Put(1, "foo", "1v1");
     	const Snapshot* s1 = db_->GetSnapshot();
		Put(0, "foo", "0v2");
		Put(1, "foo", "1v2");

		env_->addon_time_.fetch_add(1);

		const Snapshot* s2 = db_->GetSnapshot();

		Put(0, "foo", "0v3");
		Put(1, "foo", "1v3");

		{
			ManagedSnapshot s3(db_);
			Put(0, "foo", "0v4");
			Put(1, "foo", "1v4");
			ASSERT_EQ("0v1", Get(0, "foo", s1));
			ASSERT_EQ("1v1", Get(1, "foo", s1));
			ASSERT_EQ("0v2", Get(0, "foo", s2));
			ASSERT_EQ("1v2", Get(1, "foo", s2));
			ASSERT_EQ("0v3", Get(0, "foo", s3.snapshot()));
			ASSERT_EQ("1v3", Get(1, "foo", s3.snapshot()));
			ASSERT_EQ("0v4", Get(0, "foo"));
			ASSERT_EQ("1v4", Get(1, "foo"));
		}

		ASSERT_EQ("0v1", Get(0, "foo", s1));
		ASSERT_EQ("1v1", Get(1, "foo", s1));
		ASSERT_EQ("0v2", Get(0, "foo", s2));
		ASSERT_EQ("1v2", Get(1, "foo", s2));
		ASSERT_EQ("0v4", Get(0, "foo"));
		ASSERT_EQ("1v4", Get(1, "foo"));

		db_->ReleaseSnapshot(s1);
		ASSERT_EQ("0v2", Get(0, "foo", s2));
		ASSERT_EQ("1v2", Get(1, "foo", s2));
		ASSERT_EQ("0v4", Get(0, "foo"));
		ASSERT_EQ("1v4", Get(1, "foo"));

		db_->ReleaseSnapshot(s2);
		ASSERT_EQ("0v4", Get(0, "foo"));
		ASSERT_EQ("1v4", Get(1, "foo"));
	//} while (ChangeOptions(kSkipHashCuckoo));
}
TEST_F(DBBasicTest, DBOpen_Options) {
	Options options = CurrentOptions();
	std::string dbname = test::TmpDir(env_) + "/db_options_test";
	//std::string dbname = "/tmp/db_options_test";
	//ASSERT_OK(DestroyDB(dbname, options));

	// Does not exist, and create_if_missing == false: error
	// issue here, db will be created even with create_if_missing == false: error
	DB* db = nullptr;
 	// options.create_if_missing = false;
	// Status s = DB::Open(options, dbname, &db);
	// ASSERT_TRUE(strstr(s.ToString().c_str(), "does not exist") != nullptr);
	// ASSERT_TRUE(db == nullptr); 

	// Does not exist, and create_if_missing == true: OK
	options.create_if_missing = true;
	Status s = DB::Open(options, dbname, &db);
	ASSERT_OK(s);
	ASSERT_TRUE(db != nullptr);

	delete db;
	db = nullptr;

	// Does exist, and error_if_exists == true: error
	options.create_if_missing = false;
	options.error_if_exists = true;
	s = DB::Open(options, dbname, &db);
	cout <<"~~~~~~~~error_if_exists == true  " <<s.ToString()<<endl;
	ASSERT_TRUE(strstr(s.ToString().c_str(), "exists") != nullptr);
	ASSERT_TRUE(db == nullptr);

	// Does exist, and error_if_exists == false: OK
	options.create_if_missing = true;
	options.error_if_exists = false;
	s = DB::Open(options, dbname, &db);
	ASSERT_OK(s);
	ASSERT_TRUE(db != nullptr);

	delete db;
	db = nullptr;
} 

TEST_F(DBBasicTest, FlushOneColumnFamily) {
	Options options = CurrentOptions();
	CreateAndReopenWithCF( {"pikachu"
	                       },
	                       options);

	ASSERT_OK(Put(0, "Default", "Default"));
	ASSERT_OK(Put(1, "pikachu", "pikachu"));
	//ASSERT_OK(Put(2, "ilya", "ilya"));
	// ASSERT_OK(Put(3, "muromec", "muromec"));
	// ASSERT_OK(Put(4, "dobrynia", "dobrynia"));
	// ASSERT_OK(Put(5, "nikitich", "nikitich"));
	// ASSERT_OK(Put(6, "alyosha", "alyosha"));
	// ASSERT_OK(Put(7, "popovich", "popovich"));

	for (int i = 0; i < 2; ++i) {
		Flush(i);
		// the filename size is diff from rocksdb 
		auto tables = ListTableFiles(env_, dbname_);
		ASSERT_EQ(tables.size(), 0);
	}
}
 
TEST_F(DBBasicTest, ChecksumTest) {
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Flush());  // table with crc checksum

  table_options.checksum = kxxHash;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_OK(Put("e", "f"));
  ASSERT_OK(Put("g", "h"));
  ASSERT_OK(Flush());  // table with xxhash checksum

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_EQ("b", Get("a"));
  ASSERT_EQ("d", Get("c"));
  ASSERT_EQ("f", Get("e"));
  ASSERT_EQ("h", Get("g"));

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_EQ("b", Get("a"));
  ASSERT_EQ("d", Get("c"));
  ASSERT_EQ("f", Get("e"));
  ASSERT_EQ("h", Get("g"));
}
 

}

	
