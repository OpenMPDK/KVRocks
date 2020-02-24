/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file kvdb_Newiterator.cc
  *  \brief Test cases code of the KBDB NewIterator().
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
//#include <time.h>
#include <fcntl.h>
//#include "gtest/gtest.h"
//#include "db.h"
//#include "status.h"
//#include "options.h"
//#include "env.h"
//#include "include/rocksdb/perf_context.h"
//#include "slice.h"
//#include "slice_transform.h"
//#include "thread_status.h"
//#include "CommonType.h"
//#include "Common.h"
//#include "utilities/merge_operators.h"
////for cftest
//#include "util/sync_point.h"
//#include "util/string_util.h"
//#include "db/db_impl.h"
//#include "util/testutil.h"
////for iterator
//#include <stdio.h>
//
#include "kvdb_test.h"

using namespace std;

namespace rocksdb {

enum SkipPolicy { kSkipNone = 0, kSkipNoSnapshot = 1, kSkipNoPrefix = 2 };

static uint64_t TestGetTickerCount(const Options& options,
                                   Tickers ticker_type) {
  return options.statistics->getTickerCount(ticker_type);
}

namespace anon {
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

}  // namespace anon



namespace cftest_wal {

//hainan.liu 
// counts how many operations were performed
class EnvCounter : public EnvWrapper {
 public:
  explicit EnvCounter(Env* base)
      : EnvWrapper(base), num_new_writable_file_(0) {}
  int GetNumberOfNewWritableFileCalls() {
    return num_new_writable_file_;
  }
  Status NewWritableFile(const std::string& f, unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    ++num_new_writable_file_;
    return EnvWrapper::NewWritableFile(f, r, soptions);
  }
  bool count_random_reads_ = false;
  std::atomic<size_t> random_read_bytes_counter_;
 private:
  std::atomic<int> num_new_writable_file_;
};



//hainan.liu 
class DBWALTest: public KVDBTest{
 public:
  DBWALTest() {
    env_ = new EnvCounter(Env::Default());
    //env_ = new SpecialEnv(Env::Default());
    dbname_ = test::TmpDir(env_) + "/wal_test";
    //db_options_.create_if_missing = true;
    //db_options_.fail_if_options_file_error = true;
    //db_options_.env = env_;
    //DestroyDB(dbname_, Options(db_options_, column_family_options_));
    //EXPECT_OK(DestroyDB(dbname_, db_options_));
    cout << "---------------DBWALTest--------------------"<< endl; 
    auto options = CurrentOptions();
    db_options_.env = env_;
    db_options_.create_if_missing = true;
    db_options_.num_cols = 8;
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
    //EXPECT_OK(DestroyDB(dbname_, options));
    //DestroyDB(dbname_, options);
    //db_ = nullptr;
    //Reopen(options);
    //Random::GetTLSInstance()->Reset(0xdeadbeef);
 }
   public:
  // Sequence of option configurations to try
  enum OptionConfig : int {
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
  };
  
  int option_config_;
  Options last_options_;

  std::string alternative_wal_dir_;
  std::string alternative_db_log_dir_;

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

 
  ~DBWALTest() {
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->LoadDependency({});
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
  cout << "---------------~DBWALTest--------------------"<< endl;
  Options options;
  options.env = env_;
  //Close();
//  if (getenv("KEEP_DB")) {
//    printf("DB is still at %s\n", dbname_.c_str());
//  } else {
//    EXPECT_OK(DestroyDB(dbname_, options));
//  // Destroy();
//  }  
  if (db_)
   {Destroy();}
  delete env_;
}

std::string IterStatus(Iterator* iter) {
  std::string result;
  if (iter->Valid()) {
    result = iter->key().ToString() + "->" + iter->value().ToString();
  } else {
    result = "(invalid)";
  }
  return result;
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



// Switch to a fresh database with the next option configuration to
// test.  Return false if there are no more configurations to test.
bool ChangeOptions(int skip_mask= kNoSkip) {
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
    //db_options_ = options;
    cout <<"DestroyAndReopen"<<endl;
    DestroyAndReopen(options);
    cout <<"DestroyAndReopen"<<endl;
    return true;
  }
}

// Switch between different compaction styles.
bool ChangeCompactOptions() {
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
    assert(options.max_subcompactions > 1);
    TryReopen(options);
    return true;
  } else if (option_config_ == kLevelSubcompactions) {
    option_config_ = kUniversalSubcompactions;
    Destroy(last_options_);
    auto options = CurrentOptions();
    assert(options.max_subcompactions > 1);
    TryReopen(options);
    return true;
  } else {
    return false;
  }
}

void VerifyIterLast(std::string expected_key, int cf=0) {
  Iterator* iter;
  ReadOptions ro;
  if (cf == 0) {
    iter = db_->NewIterator(ro);
  } else {
    iter = db_->NewIterator(ro, handles_[cf]);
  }
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), expected_key);
  delete iter;
}

// Return the current option configuration.
Options CurrentOptions(
    const anon::OptionsOverride& options_override = anon::OptionsOverride()) const {
  return GetOptions(option_config_, GetDefaultOptions(), options_override);
}

Options CurrentOptions(
    const Options& default_options,
    const anon::OptionsOverride& options_override = anon::OptionsOverride()) const {
  return GetOptions(option_config_, default_options, options_override);
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


Options GetOptions(
    int option_config, const Options& default_options,
    const anon::OptionsOverride& options_override) const {
  // this redundant copy is to minimize code change w/o having lint error.
  Options options = default_options;
  BlockBasedTableOptions table_options;
  bool set_block_based_table_factory = true;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) &&  \
  !defined(OS_AIX)
  rocksdb::SyncPoint::GetInstance()->ClearCallBack(
      "NewRandomAccessFile:O_DIRECT");
  rocksdb::SyncPoint::GetInstance()->ClearCallBack(
      "NewWritableFile:O_DIRECT");
#endif

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
};
/*
TEST_F(DBWALTest, WAL) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // Both value's should be present.
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v2", Get(1, "foo"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // again both values should be present.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));
  } while (ChangeWalOptions());
}

*/
TEST_F(DBWALTest, 1204_DeleteAndFlush) {
  Open();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  //Reopen();
  //Flush();
  //delete db_;
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(0, "foo1", "v1"));
  ASSERT_OK(Put(0, "foo2", "v1"));
  ASSERT_OK(Put(0, "foo3", "v1"));
  ASSERT_OK(Put(0, "foo4", "v1"));
  ASSERT_OK(Put(0, "foo5", "v1"));
  //ASSERT_OK(Put(1, "foo", "v1"));
  //ASSERT_OK(Put(2, "foo", "v1"));
  ASSERT_OK(Delete(0, "foo"));
  ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  //Flush();
  ASSERT_OK(db_->Flush(FlushOptions()));
  //ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  ASSERT_EQ("v1", Get(0, "foo1"));
  //ASSERT_EQ("v1", Get(1, "foo"));
  //ASSERT_EQ("v1", Get(2, "foo"));
  Close();
}
TEST_F(DBWALTest, 1133_ReadFromPersistedTier) {
//  do {
   // for (int disableWAL = 0; disableWAL <= 1; ++disableWAL) {
      Open();
      CreateColumnFamiliesAndReopen({"one"});      
//CreateColumnFamilies({"one"});
//      Reopen();
      //delete db_;
      //TryOpen({"default", "one"});
      WriteOptions wopt;
      ReadOptions ropt;
      std::string value;
      ropt.read_tier = kPersistedTier;
//      wopt.disableWAL = (disableWAL == 1);
      // 1st round: put but not flush
      ASSERT_OK(db_->Put(wopt, handles_[1], "foo", "first"));
      ASSERT_OK(db_->Put(wopt, handles_[1], "bar", "one"));
      ASSERT_TRUE(
            db_->Get(ropt, handles_[1], "foo", &value).IsNotFound());
      ASSERT_TRUE(
            db_->Get(ropt, handles_[1], "bar", &value).IsNotFound());
      ASSERT_OK(db_->Flush(FlushOptions()));
      cout<<"After flush"<<endl;
      sleep(2);  //should be deleted after 0701 version.
      ASSERT_OK(
            db_->Get(ropt, handles_[1], "foo", &value));
      ASSERT_EQ(value, "first");
      ASSERT_OK(
            db_->Get(ropt, handles_[1], "bar", &value));
      ASSERT_EQ(value, "one");
      Close();
//      DestroyDB(dbname_, options);
//      Destroy();
  //    }
// } while (ChangeOptions(kSkipHashCuckoo));
}

/*
TEST_F(DBWALTest, ReadFromPersistedTier) {
  //do {
    Open();
    Random rnd(301);
    Options options = CurrentOptions();
    for (int disableWAL = 0; disableWAL <= 1; ++disableWAL) {
      //CreateAndReopenWithCF({"pikachu"}, options);
      CreateColumnFamilies({"one"});
      Reopen();
      WriteOptions wopt;
      wopt.disableWAL = (disableWAL == 1);
      // 1st round: put but not flush
      ASSERT_OK(db_->Put(wopt, handles_[1], "foo", "first"));
      ASSERT_OK(db_->Put(wopt, handles_[1], "bar", "one"));
      sleep(1); //kvdb
      ASSERT_EQ("first", Get(1, "foo"));
      ASSERT_EQ("one", Get(1, "bar"));
      ASSERT_OK(db_->Flush(FlushOptions()));
      ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
      ASSERT_EQ("NOT_FOUND", Get(1, "bar"));

      // Read directly from persited data.
      ReadOptions ropt;
      ropt.read_tier = kPersistedTier;
      std::string value;
      if (wopt.disableWAL) {
        // as data has not yet being flushed, we expect not found.
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "foo", &value).IsNotFound());
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).IsNotFound());
      } else {
        cout <<"Get" <<endl;
        ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
        ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
      }

      // Multiget
      std::vector<ColumnFamilyHandle*> multiget_cfs;
      multiget_cfs.push_back(handles_[1]);
      multiget_cfs.push_back(handles_[1]);
      std::vector<Slice> multiget_keys;
      multiget_keys.push_back("foo");
      multiget_keys.push_back("bar");
      std::vector<std::string> multiget_values;
      auto statuses =
          db_->MultiGet(ropt, multiget_cfs, multiget_keys, &multiget_values);
      if (wopt.disableWAL) {
        ASSERT_TRUE(statuses[0].IsNotFound());
        ASSERT_TRUE(statuses[1].IsNotFound());
      } else {
        ASSERT_OK(statuses[0]);
        ASSERT_OK(statuses[1]);
      }

      // 2nd round: flush and put a new value in memtable.
      ASSERT_OK(db_->Flush(FlushOptions()));
      ASSERT_OK(db_->Put(wopt, handles_[1], "rocksdb", "hello"));

      // once the data has been flushed, we are able to get the
      // data when kPersistedTier is used.
      ASSERT_TRUE(db_->Get(ropt, handles_[1], "foo", &value).ok());
      ASSERT_EQ(value, "first");
      ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).ok());
      ASSERT_EQ(value, "one");
      if (wopt.disableWAL) {
        ASSERT_TRUE(
            db_->Get(ropt, handles_[1], "rocksdb", &value).IsNotFound());
      } else {
        ASSERT_OK(db_->Get(ropt, handles_[1], "rocksdb", &value));
        ASSERT_EQ(value, "hello");
      }
 
      // Expect same result in multiget
      multiget_cfs.push_back(handles_[1]);
      multiget_keys.push_back("rocksdb");
      statuses =
          db_->MultiGet(ropt, multiget_cfs, multiget_keys, &multiget_values);
      ASSERT_TRUE(statuses[0].ok());
      ASSERT_EQ("first", multiget_values[0]);
      ASSERT_TRUE(statuses[1].ok());
      ASSERT_EQ("one", multiget_values[1]);
      if (wopt.disableWAL) {
        ASSERT_TRUE(statuses[2].IsNotFound());
      } else {
        ASSERT_OK(statuses[2]);
      }

      // 3rd round: delete and flush
      ASSERT_OK(db_->Delete(wopt, handles_[1], "foo"));
      //Flush(1);
      ASSERT_OK(db_->Flush(FlushOptions()));
      ASSERT_OK(db_->Delete(wopt, handles_[1], "bar"));

      ASSERT_TRUE(db_->Get(ropt, handles_[1], "foo", &value).IsNotFound());
      if (wopt.disableWAL) {
        // Still expect finding the value as its delete has not yet being
        // flushed.
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).ok());
        ASSERT_EQ(value, "one");
      } else {
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).IsNotFound());
      }
      ASSERT_TRUE(db_->Get(ropt, handles_[1], "rocksdb", &value).ok());
      ASSERT_EQ(value, "hello");

      statuses =
          db_->MultiGet(ropt, multiget_cfs, multiget_keys, &multiget_values);
      ASSERT_TRUE(statuses[0].IsNotFound());
      if (wopt.disableWAL) {
        ASSERT_TRUE(statuses[1].ok());
        ASSERT_EQ("one", multiget_values[1]);
      } else {
        ASSERT_TRUE(statuses[1].IsNotFound());
      }
      ASSERT_TRUE(statuses[2].ok());
      ASSERT_EQ("hello", multiget_values[2]);
      if (wopt.disableWAL == 0) {
        DestroyAndReopen(options);
      }
    }
    Close();
 // } while (ChangeOptions(kSkipHashCuckoo));
}
*/


//SyncWAL not supported
TEST_F(DBWALTest, DISABLED_1119_SyncWALNotSupported){
   Open();
   Status s = db_->SyncWAL();
   cout << s.ToString() <<endl;
   Close(); 
}

TEST_F(DBWALTest, 1120_FlushWALNotSupported){
   Open();
   Status s = db_->FlushWAL(true);
   cout << s.ToString() <<endl;
   s = db_->FlushWAL(false);
   cout << s.ToString() <<endl;
   Close();
}


TEST_F(DBWALTest, 1121_NoFlushAndGet_Default) {
  Open();
  //Reopen();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  //delete db_;
  ASSERT_OK(Put(0, "foo", "v1"));
//  Flush();
  //sleep(2);
 // ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(0, "foo"));
  Close();
}

TEST_F(DBWALTest, 1122_ReopenNoFlushAndGet_Default) {
  Open();
  Reopen();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  //delete db_;
  ASSERT_OK(Put(0, "foo", "v1"));
//  Flush();
  //sleep(2);
 // ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(0, "foo"));
  Close();
}


TEST_F(DBWALTest, 1123_NoFlushAndGet_CF){
  Open();
  CreateColumnFamilies({"one"});
  // Generate log file A.
  //delete db_;
  //Status s = TryOpen({"one","default"});
  //cout << s.ToString() <<endl;
  //Reopen();
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1
  //Reopen();
  //sleep(1);
  ASSERT_EQ("v1", Get(1, "foo"));
  Close();
 }

TEST_F(DBWALTest, 1124_ReopenNoFlushAndGet_CF){
  Open();
  CreateColumnFamilies({"one"});
  // Generate log file A.
  //delete db_;
  //Status s = TryOpen({"one","default"});
  //cout << s.ToString() <<endl;
  Reopen();
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1
  //Reopen();
  //sleep(1);
  ASSERT_EQ("v1", Get(1, "foo"));
  Close();
 }

TEST_F(DBWALTest, 1205_FlushEmptyDB) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  Reopen();
  Flush();
  //delete db_;
  ASSERT_OK(Put(0, "foo", "v1"));
  //  Flush();
  ASSERT_OK(db_->Flush(FlushOptions()));
 // ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(0, "foo"));
  Close();
}
/*
TEST_F(DBWALTest, DeleteAndFlush) {
  Open();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  //Reopen();
  //Flush();
  //delete db_;
  ASSERT_OK(Put(0, "foo", "v1"));
  //ASSERT_OK(Put(1, "foo", "v1"));
  //ASSERT_OK(Put(2, "foo", "v1"));
  ASSERT_OK(Delete(0, "foo"));
  //ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  //  Flush();
  //ASSERT_OK(db_->Flush(FlushOptions()));
 // ASSERT_EQ("v1", Get(0, "foo"));
  //ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  //ASSERT_EQ("v1", Get(1, "foo"));
  //ASSERT_EQ("v1", Get(2, "foo"));
  Close();
}
*/

TEST_F(DBWALTest, 1125_FlushAndGet) {
  Open();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  //delete db_;
  ASSERT_OK(Put(0, "foo", "v1"));
  //  Flush();
  ASSERT_OK(db_->Flush(FlushOptions()));
 // ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(0, "foo"));
  Close();
}



TEST_F(DBWALTest, 1126_FlushAndGet_CFs) {
  Open();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  CreateColumnFamiliesAndReopen({"one", "two"});
  //delete db_;

  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(0, "bar", "v2"));
  ASSERT_OK(Put(1, "mirko", "v3"));
  ASSERT_OK(Put(0, "foo", "v2"));
  ASSERT_OK(Put(2, "fodor", "v5"));
  ASSERT_OK(db_->Flush(FlushOptions()));
 // ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v2", Get(0, "bar"));
  ASSERT_EQ("v3", Get(1, "mirko"));
  ASSERT_EQ("v2", Get(0, "foo"));
  ASSERT_EQ("v5", Get(2, "fodor"));
  Close();
}

TEST_F(DBWALTest, 1127_FlushAndGet_NoWait) {
  Open();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  CreateColumnFamiliesAndReopen({"one", "two"});
  //delete db_;
  FlushOptions flush_options;
  flush_options.wait=false;
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
 // ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(0, "foo"));
  Close();
}

TEST_F(DBWALTest, 1128_FlushAndGet_Wait) {
  Open();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  CreateColumnFamiliesAndReopen({"one", "two"});
  //delete db_;
  FlushOptions flush_options;
  flush_options.wait=false;
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
 // ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(0, "foo"));
  Close();
}


/*
TEST_F(DBWALTest, FlushOneColumnFamily) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "pikachu", "pikachu"));
  ASSERT_OK(Put(2, "ilya", "ilya"));
  ASSERT_OK(Put(3, "muromec", "muromec"));
  ASSERT_OK(Put(4, "dobrynia", "dobrynia"));
  ASSERT_OK(Put(5, "nikitich", "nikitich"));
  ASSERT_OK(Put(6, "alyosha", "alyosha"));
  ASSERT_OK(Put(7, "popovich", "popovich"));

  for (int i = 0; i < 8; ++i) {
    Flush(i);
    //auto tables = ListTableFiles(env_, dbname_);
    //ASSERT_EQ(tables.size(), i + 1U);
  }
}
*/

/*
TEST_F(DBWALTest, FlushOnDestroy) {
  WriteOptions wo;
  wo.disableWAL = true;
  ASSERT_OK(Put("foo", "v1", wo));
  CancelAllBackgroundWork(db_);
}
*/

/*
TEST_F(DBWALTest, PreShutdownFlush) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(1, "key", "value"));
  CancelAllBackgroundWork(db_);
  Status s =
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr);
  ASSERT_TRUE(s.IsShutdownInProgress());
}
*/

//core dump
/*
TEST_F(DBWALTest, IteratorPropertyVersionNumber) {
  Open();
  Put("", "");
  Iterator* iter1 = db_->NewIterator(ReadOptions());
  std::string prop_value;
  ASSERT_OK(
      iter1->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number1 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  Put("", "");
//  Flush();

  Iterator* iter2 = db_->NewIterator(ReadOptions());
  ASSERT_OK(
      iter2->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number2 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  ASSERT_GT(version_number2, version_number1);

  Put("", "");

  Iterator* iter3 = db_->NewIterator(ReadOptions());
  ASSERT_OK(
      iter3->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number3 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  ASSERT_EQ(version_number2, version_number3);

  iter1->SeekToFirst();
  ASSERT_OK(
      iter1->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number1_new =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));
  ASSERT_EQ(version_number1, version_number1_new);

  delete iter1;
  delete iter2;
  delete iter3;
  Close();
}
*/


TEST_F(DBWALTest, 1129_FirstSnapshotTest) {
  Open();
  Options options = CurrentOptions();
  ReadOptions roptions;
  std::string result;
  options.write_buffer_size = 100000;  // Small write buffer
  //options = CurrentOptions(options);
  //CreateAndReopenWithCF({"pikachu"}, options);
  CreateColumnFamilies({"pikachu"});
  options.env = env_;
  options.create_if_missing = true;
  options.num_cols = 8;
  db_options_=options;
  Reopen();
  // This snapshot will have sequence number 0 what is expected behaviour.
  const Snapshot* s1 = db_->GetSnapshot();

  Put(1, "k1", std::string(100000, 'x'));  // Fill memtable
  Put(1, "k2", std::string(100000, 'y'));  // Trigger flush

  const Snapshot* s2 = db_->GetSnapshot();
  //Status s = db_->Get(roptions, "foo11", &result);
  ASSERT_EQ("NOT_FOUND", Get(1,"k1", s1));
  sleep(1);
  ASSERT_EQ(std::string(100000, 'y'), Get(1,"k2", s2));
  db_->ReleaseSnapshot(s2);
  db_->ReleaseSnapshot(s1);
  Close();
}


/*
TEST_F(DBWALTest, OptimizeForPointLookup) {
  Options options = CurrentOptions();
  Close();
  options.OptimizeForPointLookup(2);
  ASSERT_OK(DB::Open(options, dbname_, &db_));

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_EQ("v1", Get("foo"));
  Flush();
  ASSERT_EQ("v1", Get("foo"));
}
*/

/*
TEST_F(DBWALTest, DirectIO) {
  if (!IsDirectIOSupported()) {
    return;
  }
  Options options = CurrentOptions();
  options.use_direct_reads = options.use_direct_io_for_flush_and_compaction =
      true;
  options.allow_mmap_reads = options.allow_mmap_writes = false;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  Reopen(options);
}
*/

/*
TEST_P(DBWALTest, DirectIO) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.use_direct_io_for_flush_and_compaction = GetParam();
  options.env = new MockEnv(Env::Default());
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:create_file", [&](void* arg) {
        bool* use_direct_writes = static_cast<bool*>(arg);
        ASSERT_EQ(*use_direct_writes,
                  options.use_direct_io_for_flush_and_compaction);
      });

  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_OK(Put("foo", "v"));
  FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_OK(dbfull()->Flush(flush_options));
  Destroy(options);
  delete options.env;
}
*/

/*
TEST_F(DBWALTest, FlushWhileWritingManifest) {
  Options options;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.env = env_;
  Reopen(options);
  FlushOptions no_wait;
  no_wait.wait = false;

  SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply:WriteManifest",
        "DBFlushTest::FlushWhileWritingManifest:1"},
       {"MemTableList::InstallMemtableFlushResults:InProgress",
        "VersionSet::LogAndApply:WriteManifestDone"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("foo", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  TEST_SYNC_POINT("DBFlushTest::FlushWhileWritingManifest:1");
  ASSERT_OK(Put("bar", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  // If the issue is hit we will wait here forever.
  dbfull()->TEST_WaitForFlushMemTable();
#ifndef ROCKSDB_LITE
  ASSERT_EQ(2, TotalTableFiles());
#endif  // ROCKSDB_LITE
}
*/

/*
TEST_F(DBWALTest, SyncFail) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options;
  options.disable_auto_compactions = true;
  options.env = fault_injection_env.get();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBFlushTest::SyncFail:1", "DBImpl::SyncClosedLogs:Start"},
       {"DBImpl::SyncClosedLogs:Failed", "DBFlushTest::SyncFail:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);
  Put("key", "value");
  auto* cfd =
      reinterpret_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
          ->cfd();
  int refs_before = cfd->current()->TEST_refs();
  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_options));
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT("DBFlushTest::SyncFail:1");
  TEST_SYNC_POINT("DBFlushTest::SyncFail:2");
  fault_injection_env->SetFilesystemActive(true);
  dbfull()->TEST_WaitForFlushMemTable();
#ifndef ROCKSDB_LITE
  ASSERT_EQ("", FilesPerLevel());  // flush failed.
#endif                             // ROCKSDB_LITE
  // Flush job should release ref count to current version.
  ASSERT_EQ(refs_before, cfd->current()->TEST_refs());
  Destroy(options);
}
*/

/*
TEST_F(DBWALTest, FlushInLowPriThreadPool) {
  // Verify setting an empty high-pri (flush) thread pool causes flushes to be
  // scheduled in the low-pri (compaction) thread pool.
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(new SpecialSkipListFactory(1));
  Reopen(options);
  env_->SetBackgroundThreads(0, Env::HIGH);

  std::thread::id tid;
  int num_flushes = 0, num_compactions = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkFlush", [&](void* arg) {
        if (tid == std::thread::id()) {
          tid = std::this_thread::get_id();
        } else {
          ASSERT_EQ(tid, std::this_thread::get_id());
        }
        ++num_flushes;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkCompaction", [&](void* arg) {
        ASSERT_EQ(tid, std::this_thread::get_id());
        ++num_compactions;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key", "val"));
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key", "val"));
    dbfull()->TEST_WaitForFlushMemTable();
  }
  dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(4, num_flushes);
  ASSERT_EQ(1, num_compactions);
}
*/


/*
TEST_F(DBWALTest, MemtableOnlyIterator) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "first"));
  ASSERT_OK(Put(1, "bar", "second"));

  ReadOptions ropt;
  ropt.read_tier = kMemtableTier;
  std::string value;
  Iterator* it = nullptr;

  // Before flushing
  // point lookups
  ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
  ASSERT_EQ("first", value);
  ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
  ASSERT_EQ("second", value);

  // Memtable-only iterator (read_tier=kMemtableTier); data not flushed yet.
  it = db_->NewIterator(ropt, handles_[1]);
  int count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(2, count);
  delete it;

  Flush(1);

  // After flushing
  // point lookups
  ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
  ASSERT_EQ("first", value);
  ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
  ASSERT_EQ("second", value);
  // nothing should be returned using memtable-only iterator after flushing.
  it = db_->NewIterator(ropt, handles_[1]);
  count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(0, count);
  delete it;

  // Add a key to memtable
  ASSERT_OK(Put(1, "foobar", "third"));
  it = db_->NewIterator(ropt, handles_[1]);
  count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("foobar", it->key().ToString());
    ASSERT_EQ("third", it->value().ToString());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(1, count);
  delete it;
}
*/

/*
TEST_F(DBWALTest, RecoverWithoutFlushMultipleCF) {
  const std::string kSmallValue = "v";
  const std::string kLargeValue = DummyString(1024);
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;
  options.create_if_missing = false;
  options.disable_auto_compactions = true;

  auto countWalFiles = [this]() {
    VectorLogPtr log_files;
    dbfull()->GetSortedWalFiles(log_files);
    return log_files.size();
  };

  // Create DB with multiple column families and multiple log files.
  CreateAndReopenWithCF({"one", "two"}, options);
  ASSERT_OK(Put(0, "key1", kSmallValue));
  ASSERT_OK(Put(1, "key2", kLargeValue));
  Flush(1);
  ASSERT_EQ(1, countWalFiles());
  ASSERT_OK(Put(0, "key3", kSmallValue));
  ASSERT_OK(Put(2, "key4", kLargeValue));
  Flush(2);
  ASSERT_EQ(2, countWalFiles());

  // Reopen, insert and flush.
  options.db_write_buffer_size = 64 * 1024 * 1024;
  ReopenWithColumnFamilies({"default", "one", "two"}, options);
  ASSERT_EQ(Get(0, "key1"), kSmallValue);
  ASSERT_EQ(Get(1, "key2"), kLargeValue);
  ASSERT_EQ(Get(0, "key3"), kSmallValue);
  ASSERT_EQ(Get(2, "key4"), kLargeValue);
  // Insert more data.
  ASSERT_OK(Put(0, "key5", kLargeValue));
  ASSERT_OK(Put(1, "key6", kLargeValue));
  ASSERT_EQ(3, countWalFiles());
  Flush(1);
  ASSERT_OK(Put(2, "key7", kLargeValue));
  ASSERT_EQ(4, countWalFiles());

  // Reopen twice and validate.
  for (int i = 0; i < 2; i++) {
    ReopenWithColumnFamilies({"default", "one", "two"}, options);
    ASSERT_EQ(Get(0, "key1"), kSmallValue);
    ASSERT_EQ(Get(1, "key2"), kLargeValue);
    ASSERT_EQ(Get(0, "key3"), kSmallValue);
    ASSERT_EQ(Get(2, "key4"), kLargeValue);
    ASSERT_EQ(Get(0, "key5"), kLargeValue);
    ASSERT_EQ(Get(1, "key6"), kLargeValue);
    ASSERT_EQ(Get(2, "key7"), kLargeValue);
    ASSERT_EQ(4, countWalFiles());
  }
}
*/
/*
TEST_F(DBWALTest, FlushEmptyCFTest) {
//TEST_P(FlushEmptyCFTestWithParam, FlushEmptyCFTest) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  db_options_.env = fault_env.get();
  db_options_.allow_2pc = allow_2pc_;
  Open();
  CreateColumnFamilies({"one", "two"});
  // Generate log file A.
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1

  Reopen();
  // Log file A is not dropped after reopening because default column family's
  // min log number is 0.
  // It flushes to SST file X
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 2
  ASSERT_OK(Put(1, "bar", "v2"));  // seqID 3
  // Current log file is file B now. While flushing, a new log file C is created
  // and is set to current. Boths' min log number is set to file C in memory, so
  // after flushing file B is deleted. At the same time, the min log number of
  // default CF is not written to manifest. Log file A still remains.
  // Flushed to SST file Y.
  //Flush(1);
  //Flush(0);
  ASSERT_OK(Put(1, "bar", "v3"));  // seqID 4
  ASSERT_OK(Put(1, "foo", "v4"));  // seqID 5

  // Preserve file system state up to here to simulate a crash condition.
  fault_env->SetFilesystemActive(false);
  std::vector<std::string> names;
  for (auto name : names_) {
    if (name != "") {
      names.push_back(name);
    }
  }

  Close();
  fault_env->ResetState();

  // Before opening, there are four files:
  //   Log file A contains seqID 1
  //   Log file C contains seqID 4, 5
  //   SST file X contains seqID 1
  //   SST file Y contains seqID 2, 3
  // Min log number:
  //   default CF: 0
  //   CF one, two: C
  // When opening the DB, all the seqID should be preserved.
  Open(names, {});
  ASSERT_EQ("v4", Get(1, "foo"));
  ASSERT_EQ("v3", Get(1, "bar"));
  Close();

  db_options_.env = env_;
}
*/


//如果写入1024*16次，则会出发flush
TEST_F(DBWALTest, 1130_MassPutTest) {
  Open();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  //string k[16][1024]={0};
  //string v[16][1024]={0};
//  int s=0;
  //char temp[64];
  //int kv[1026*16]={0};
//put 1024*16 kvs
//  std::string test(17, 'x');
//  cout<<test<<endl;
  ASSERT_OK(Put(0, "1", "1"));
  for(int i=0; i<16; i++)
    {
       for (int j=0;j<1024;j++)
        {
        //kv[s++]=s++;
        //s++;
        //std::string Value(s, '1');
        //std::string Key(s, 'k');
        //Status s=Put(0, Key, Value);
        //cout<<s.ToString()<<endl;
        std::vector<std::string> source_strings;
        Random rnd(32);
        std::string Key = test::RandomKey(&rnd, 32);      
        std::string Value = test::RandomKey(&rnd, 8);  
        ASSERT_OK(Put(0, Key, Value));        
        } 
   }
   //std::string k1(50, 'k');
   //std::string v1(50, '1');
   //std::string k2(1024*16, 'k');
   //std::string v2(1024*16, '1');
   ASSERT_EQ(Get(0, "1"), "1");
   //ASSERT_EQ(Get(0, k1), v1);
   //ASSERT_EQ(Get(0, k2), v2);
   Close();
}

/*
TEST_F(DBWALTest, 1131_FlushIterTest) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(0, "bar", "v2"));
  ASSERT_OK(Put(0, "mirko", "v3"));
  ASSERT_OK(Put(0, "foo1", "v2"));
  ASSERT_OK(Put(1, "foo", "v4"));
  ASSERT_OK(Put(1, "mirko", "v3"));
  ASSERT_OK(Put(1, "more", "v2"));
    
 
  ASSERT_OK(db_->Flush(FlushOptions())); 
//flush();
  Iterator* iter0 = db_->NewIterator(ReadOptions(), handles_[0]);  
  Iterator* iter1 = db_->NewIterator(ReadOptions(), handles_[1]);
  //Iterator* iter2 = db_->NewIterator(ReadOptions(), handles_[2]);
  iter0->SeekToFirst();
  ASSERT_EQ(IterStatus(iter0), "bar->v2");
  iter0->Next();
  ASSERT_EQ(IterStatus(iter0), "foo->v1");
  iter0->SeekToLast();
  ASSERT_EQ(IterStatus(iter0), "mirko->v3");
  iter0->Next();
  ASSERT_EQ(IterStatus(iter0), "(invalid)");

  iter1->SeekToLast();
  ASSERT_EQ(IterStatus(iter1), "more->v2");
  iter1->Prev();
  ASSERT_EQ(IterStatus(iter1), "mirko->v3");
  iter1->Prev();
  ASSERT_EQ(IterStatus(iter1), "foo->v4");
  iter1->Prev();
  ASSERT_EQ(IterStatus(iter1), "(invalid)");  

  iter1->Seek("mirko");
  ASSERT_EQ(IterStatus(iter1), "mirko->v3");
  iter0->SeekForPrev(Slice("d"));
  ASSERT_EQ(IterStatus(iter0), "bar->v2");
  iter1->Seek("m");
  //ASSERT_EQ(IterStatus(iter1), "(invalid)");
  ASSERT_EQ(IterStatus(iter1), "mirko->v3");

  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.env = env_;
  options.create_if_missing = true;
  options.num_cols = 8;
  db_options_ = options;   
  //Reopen();
  delete iter0;
  delete iter1;
  delete db_;
  db_ = nullptr;
  Status s=TryOpen({"default","one", "two"});
  cout<<s.ToString()<<endl;
  ASSERT_OK(Put(2, "fodor", "v1"));
  ASSERT_OK(Put(2, "foo", "v2"));
  ASSERT_OK(Put(2, "dar", "v3"));
  ASSERT_OK(Put(2, "ace", "v4"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  Iterator* iter2 = db_->NewIterator(ReadOptions(), handles_[2]);
  iter2->Seek("four");
//  ASSERT_EQ(IterStatus(iter2), "fodor->v1");


  delete iter2;
  Close();
}

*/
TEST_F(DBWALTest, 1132_FlushTest) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(0, "bar", "v2"));
  ASSERT_OK(Put(1, "mirko", "v3"));
  ASSERT_OK(Put(0, "foo", "v2"));
  ASSERT_OK(Put(2, "fodor", "v5"));
  cout <<"after put"<<endl;
  for (int j = 0; j < 2; j++) {
    ReadOptions ro;
    std::vector<Iterator*> iterators;
    // Hold super version.
    if (j == 0) {
      ASSERT_OK(db_->NewIterators(ro, handles_, &iterators));
    }
/*
    for (int i = 0; i < 3; ++i) {
      uint64_t max_total_in_memory_state =
          MaxTotalInMemoryState();
      Flush(i);
      AssertMaxTotalInMemoryState(max_total_in_memory_state);
    }
*/
    cout <<"after iterators"<<endl;
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(Put(1, "foofoo", "bar"));
    ASSERT_OK(Put(0, "foofoo", "bar"));
    cout <<"after flush"<<endl;
    for (auto* it : iterators) {
      delete it;
    }
  }
  Reopen();

  for (int iter = 0; iter <= 2; ++iter) {
    ASSERT_EQ("v2", Get(0, "foo"));
    ASSERT_EQ("v2", Get(0, "bar"));
    ASSERT_EQ("v3", Get(1, "mirko"));
    ASSERT_EQ("v5", Get(2, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(0, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(2, "foo"));
    if (iter <= 1) {
      Reopen();
    }
  }
  Close();
}


/*
TEST_F(DBWALTest, CrashAfterFlush) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  db_options_.env = fault_env.get();
  Open();
  CreateColumnFamilies({"one"});

  WriteBatch batch;
  batch.Put(handles_[0], Slice("foo"), Slice("bar"));
  batch.Put(handles_[1], Slice("foo"), Slice("bar"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  Flush(0);
  fault_env->SetFilesystemActive(false);

  std::vector<std::string> names;
  for (auto name : names_) {
    if (name != "") {
      names.push_back(name);
    }
  }
  Close();
  fault_env->DropUnsyncedFileData();
  fault_env->ResetState();
  Open(names, {});

  // Write batch should be atomic.
  ASSERT_EQ(Get(0, "foo"), Get(1, "foo"));

  Close();
  db_options_.env = env_;
}
*/
/*
TEST_F(DBBasicTest, FlushEmptyColumnFamily) {
  // Block flush thread and disable compaction thread
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  Options options = CurrentOptions();
  // disable compaction
  options.disable_auto_compactions = true;
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.max_write_buffer_number = 2;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number_to_maintain = 1;
  CreateAndReopenWithCF({"pikachu"}, options);

  // Compaction can still go through even if no thread can flush the
  // mem table.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  // Insert can go through
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[0], "foo", "v1"));
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

  ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(1, "bar"));

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();

  // Flush can still go through.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}*/


TEST_F(DBWALTest, 1144_FLUSH) {
//  do {
    Open();    
  Options options;
  CreateColumnFamilies({"pikachu"});
  options.env = env_;
  options.create_if_missing = true;
  options.num_cols = 8;
  db_options_=options;
  Reopen();

  //  CreateAndReopenWithCF({"pikachu"});
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    //SetPerfLevel(kEnableTime);
    ;
    ASSERT_OK(db_->Put(writeOpt, handles_[1], "foo", "v1"));
    // this will now also flush the last 2 writes
    //ASSERT_OK(Flush());
    ASSERT_OK(db_->Flush(FlushOptions()));    
    ASSERT_OK(db_->Put(writeOpt, handles_[1], "bar", "v1"));

//    get_perf_context()->Reset();
    Get(1, "foo");
    cout <<"get foo"<<endl;
//    ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time > 0);
    delete db_;
    Status s = TryOpen({"default", "pikachu"});
    cout <<s.ToString()<<endl;
    //ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = true;
    ASSERT_OK(db_->Put(writeOpt, handles_[1], "bar", "v2"));
    ASSERT_OK(db_->Put(writeOpt, handles_[1], "foo", "v2"));
    ASSERT_OK(db_->Flush(FlushOptions()));

    delete db_;
    s = TryOpen({"default", "pikachu"});
    //ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v2", Get(1, "bar"));
   // get_perf_context()->Reset();
    ASSERT_EQ("v2", Get(1, "foo"));
 //   ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time > 0);

    writeOpt.disableWAL = false;
    ASSERT_OK(db_->Put(writeOpt, handles_[1], "bar", "v3"));
    ASSERT_OK(db_->Put(writeOpt, handles_[1], "foo", "v3"));
    ASSERT_OK(db_->Flush(FlushOptions()));

    delete db_;
    s = TryOpen({"default", "pikachu"});
    //ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // 'foo' should be there because its put
    // has WAL enabled.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));
    Close();
  //  SetPerfLevel(kDisable);
//  } while (ChangeCompactOptions());
}

/*
TEST_F(DBTest, FlushSchedule) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_stop_writes_trigger = 1 << 10;
  options.level0_slowdown_writes_trigger = 1 << 10;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number_to_maintain = 1;
  options.max_write_buffer_number = 2;
  options.write_buffer_size = 120 * 1024;
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<port::Thread> threads;

  std::atomic<int> thread_num(0);
  // each column family will have 5 thread, each thread generating 2 memtables.
  // each column family should end up with 10 table files
  std::function<void()> fill_memtable_func = [&]() {
    int a = thread_num.fetch_add(1);
    Random rnd(a);
    WriteOptions wo;
    // this should fill up 2 memtables
    for (int k = 0; k < 5000; ++k) {
      ASSERT_OK(db_->Put(wo, handles_[a & 1], RandomString(&rnd, 13), ""));
    }
  };

  for (int i = 0; i < 10; ++i) {
    threads.emplace_back(fill_memtable_func);
  }

  for (auto& t : threads) {
    t.join();
  }

  auto default_tables = GetNumberOfSstFilesForColumnFamily(db_, "default");
  auto pikachu_tables = GetNumberOfSstFilesForColumnFamily(db_, "pikachu");
  ASSERT_LE(default_tables, static_cast<uint64_t>(10));
  ASSERT_GT(default_tables, static_cast<uint64_t>(0));
  ASSERT_LE(pikachu_tables, static_cast<uint64_t>(10));
  ASSERT_GT(pikachu_tables, static_cast<uint64_t>(0));
}*/

}
}
