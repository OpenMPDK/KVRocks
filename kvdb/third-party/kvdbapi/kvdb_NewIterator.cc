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
#include "include/rocksdb/perf_context.h"
#include "kvdb_test.h"

using namespace std;

namespace rocksdb {

//static const int kValueSize = 1000;
enum SkipPolicy { kSkipNone = 0, kSkipNoSnapshot = 1, kSkipNoPrefix = 2 };

static uint64_t TestGetTickerCount(const Options& options,
                                   Tickers ticker_type) {
  return options.statistics->getTickerCount(ticker_type);
}


namespace {
std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}
}  // anonymous namespace

namespace anon {
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


namespace cftest_iter {

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
class DBIteratorTest: public KVDBTest {
 public:
  DBIteratorTest() {
    env_ = new EnvCounter(Env::Default());
    dbname_ = test::TmpDir(env_) + "/iterator_test";
    cout << "---------------DBIteratorTest--------------------"<< endl; 
    auto options = CurrentOptions();
    db_options_.env = env_;
    db_options_.create_if_missing = true;
    db_options_.num_cols = 8;
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
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
 
  ~DBIteratorTest() {
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->LoadDependency({});
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
  cout << "---------------~DBIteratorTest--------------------"<< endl;
    //Close();
    //rocksdb::SyncPoint::GetInstance()->DisableProcessing();
   /// Destroy();
  Options options;
//  options.db_paths.emplace_back(dbname_, 0);
//  options.db_paths.emplace_back(dbname_ + "_2", 0);
//  options.db_paths.emplace_back(dbname_ + "_3", 0);
//  options.db_paths.emplace_back(dbname_ + "_4", 0);
  options.env = env_;
   for (auto h : handles_) {
    //  if (h) {
    if (h && db_){ 
       db_->DestroyColumnFamilyHandle(h);
      }
    }
    handles_.clear();
    names_.clear();


  if(db_)
    {  
      cout<<"destroy:"<<endl;
      Destroy();
    }
///  Close();
///  if (getenv("KEEP_DB")) {
///    printf("DB is still at %s\n", dbname_.c_str());
///  } else if(db_) {
///    EXPECT_OK(DestroyDB(dbname_, options));
///  // Destroy();
///  }  
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


//TEST_CompactRange not declared in db_impl.cc
//void MoveFilesToLevel(int level, int cf) {
//  for (int l = 0; l < level; ++l) {
//    if (cf > 0) {
//      dbfull()->TEST_CompactRange(l, nullptr, nullptr, handles_[cf]);
//    } else {
//      dbfull()->TEST_CompactRange(l, nullptr, nullptr);
//    }
//  }
//}
//
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

void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                                       const Options& options) {
  CreateColumnFamilies(cfs, options);
  std::vector<std::string> cfs_plus_default = cfs;
  cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
  ReopenWithColumnFamilies(cfs_plus_default, options);
}

void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const std::vector<Options>& options) {
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
}

void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const Options& options) {
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
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



void DestroyAndReopen(const Options& options) {
  // Destroy using last options
  Destroy(last_options_);
  ASSERT_OK(TryReopen(options));
}
//void Destroy(const Options& options) {
//  Close();
//  ASSERT_OK(DestroyDB(dbname_, options));
//}

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
    //cout <<"DestroyAndReopen"<<endl;
    DestroyAndReopen(options);
    //cout <<"DestroyAndReopen"<<endl;
    return true;
  }
}
/*
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
*/
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
  options.num_cols = 8;
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

  // Return the value to associate with the specified key
  Slice Value(int k, std::string* storage) {
    if (k == 0) {
      // Ugh.  Random seed of 0 used to produce no entropy.  This code
      // preserves the implementation that was in place when all of the
      // magic values in this file were picked.
      *storage = std::string(kValueSize, ' ');
      return Slice(*storage);
    } else {
      Random r(k);
      return test::RandomString(&r, kValueSize, storage);
    }
  }

};

  // Property "rocksdb.iterator.is-key-pinned":
  //   If returning "1", this means that the Slice returned by key() is valid
  //   as long as the iterator is not deleted.
  //   It is guaranteed to always return "1" if
  //      - Iterator created with ReadOptions::pin_data = true
  //      - DB tables were created with
  //        BlockBasedTableOptions::use_delta_encoding = false.
  // Property "rocksdb.iterator.super-version-number":
  //   LSM version used by the iterator. The same format as DB Property
  //   kCurrentSuperVersionNumber. See its comment for more information.
TEST_F(DBIteratorTest, 990_IteratorProperty) {
  // The test needs to be changed if kPersistedTier is supported in iterator.
  Options options;
  options.create_if_missing = true;  
  options.max_sequential_skip_in_iterations = 3;
  options.num_cols = 8;
  // options.statistics = rocksdb::CreateDBStatistics();
   db_options_ = options;
 Open();
 std::vector<std::string> cf_names;
 cf_names.push_back("pikachu");
 CreateColumnFamilies(cf_names);
 cf_names.push_back("default");
 delete db_;
 Status s = TryOpen(cf_names);
 cout <<"--------TryOpen----------"<<endl;
 cout <<s.ToString()<<endl;
//  cout <<"--------opened----------"<<endl;
  //CreateColumnFamiliesAndReopen({"one"});
  //CreateAndReopenWithCF({"one"}, options);
  Put(1,"1", "2");
  ReadOptions ropt;
  ropt.pin_data = false;
  {
    unique_ptr<Iterator> iter(db_->NewIterator(ropt, handles_[1]));
    iter->SeekToFirst();
    std::string prop_value;
    ASSERT_NOK(iter->GetProperty("non_existing.value", &prop_value));  //get一个不存在的property
    cout << prop_value << endl;
  //  ASSERT_OK(iter->GetProperty("rocksdb.iterator.super-version-number", &prop_value));  //get一个不存在的property
  //  cout << prop_value << endl;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value)); 
    ASSERT_EQ("1", prop_value); //kvdb always returns 1.
    iter->Next();
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("Iterator is not valid.", prop_value);
  }
 Close();
}

TEST_F(DBIteratorTest, DISABLED_IteratorPropertyVersionNumber) {
  Open();
  Put("", "");
  Iterator* iter1 = db_->NewIterator(ReadOptions());
  std::string prop_value;
  ASSERT_OK(
      iter1->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number1 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  Put("", "");
  Flush();

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
}

TEST_F(DBIteratorTest, DISABLED_Empty) {
  Open();
  do {
    Options options;
    options.env = env_;
    options.num_cols = 8; 
    options.write_buffer_size = 100000;  // Small write buffer
    options.allow_concurrent_memtable_write = false;
    options = CurrentOptions(options);
    db_options_ = options;
    //CreateAndReopenWithCF({"pikachu"}, options);
    CreateColumnFamiliesAndReopen({"one"});
    std::string num;
    ASSERT_TRUE(db_->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("0", num);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_TRUE(db_->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("1", num);

  } while (ChangeOptions());
  Close();
}

//increments upon any change to the LSM tree.
    //  "rocksdb.current-super-version-number" - returns number of current LSM
    //  version. It is a uint64_t integer number, incremented after there is
    //  any change to the LSM tree. The number is not preserved after restarting
    //  the DB. After DB restart, it will start from 0 again.
TEST_F(DBIteratorTest, DISABLED_CurrentVersionNumber) {
  Open();
  uint64_t v1, v2, v3;
  ASSERT_TRUE(
      db_->GetIntProperty("rocksdb.current-super-version-number", &v1));
  Put("12345678", "");
  ASSERT_TRUE(
      db_->GetIntProperty("rocksdb.current-super-version-number", &v2));
  Flush();
  ASSERT_TRUE(
      db_->GetIntProperty("rocksdb.current-super-version-number", &v3));

  ASSERT_EQ(v1, v2);
  ASSERT_GT(v3, v2);
  delete db_;
  Open();
    ASSERT_TRUE(
      db_->GetIntProperty("rocksdb.current-super-version-number", 0));
  Close();
}

// already tested in column family feature.
//TEST_F(DBIteratorTest, 991_NewIteratorsTest) {
//  // iter == 0 -- no tailing
//  // iter == 2 -- tailing
//  for (int iter = 0; iter < 1; ++iter) {
//    Open();
//    CreateColumnFamiliesAndReopen({"one", "two"});
//    ASSERT_OK(Put(0, "a", "b"));
//    ASSERT_OK(Put(1, "b", "a"));
//    ASSERT_OK(Put(2, "c", "m"));
//    ASSERT_OK(Put(2, "v", "t"));
//    std::vector<Iterator*> iterators;
//    ReadOptions options;
//    options.tailing = (iter == 1);
//    cout << "-------------------------------------" <<endl;
//    ASSERT_OK(db_->NewIterators(options, handles_, &iterators));
//    cout << "-------------------------------------" <<endl;
//    for (auto it : iterators) {
//      it->SeekToFirst();
//    }
//    ASSERT_EQ(IterStatus(iterators[0]), "a->b");
//    ASSERT_EQ(IterStatus(iterators[1]), "b->a");
//    ASSERT_EQ(IterStatus(iterators[2]), "c->m");
//
//    ASSERT_OK(Put(1, "x", "x"));
//
//    for (auto it : iterators) {
//      it->Next();
//    }
//
//    ASSERT_EQ(IterStatus(iterators[0]), "(invalid)");
//    if (iter == 0) {
//      // no tailing
//      ASSERT_EQ(IterStatus(iterators[1]), "(invalid)");
//    } else {
//      // tailing
//      ASSERT_EQ(IterStatus(iterators[1]), "x->x");
//    }
//    ASSERT_EQ(IterStatus(iterators[2]), "v->t");
//
//    for (auto it : iterators) {
//      delete it;
//    }
//    //Destroy();
//    Close();
//    //DestroyDB(dbname_,Options(db_options_, column_family_options_));
//  }
//}
//
TEST_F(DBIteratorTest, 992_IterSeekBeforePrev) {
  Open();
  ASSERT_OK(Put(0,"a", "b"));
  ASSERT_OK(Put(0,"c", "d"));
  //dbfull()->Flush(FlushOptions());
  db_->Flush(FlushOptions());
  ASSERT_OK(Put(0, "0", "f"));
  ASSERT_OK(Put(0, "1", "h"));
  db_->Flush(FlushOptions());
  ASSERT_OK(Put(0, "2", "j"));
  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  iter->Prev();
  iter->Seek(Slice("a"));
  iter->Prev();
  delete iter;
  Close();
}

TEST_F(DBIteratorTest, 993_IterSeekForPrevBeforeNext) {
  Open();
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  db_->Flush(FlushOptions());
  ASSERT_OK(Put("0", "f"));
  ASSERT_OK(Put("1", "h"));
  db_->Flush(FlushOptions());
  ASSERT_OK(Put("2", "j"));
  auto iter = db_->NewIterator(ReadOptions());
  iter->SeekForPrev(Slice("0"));
  iter->Next();
  iter->SeekForPrev(Slice("1"));
  iter->Next();
  delete iter;
  Close();
}

namespace {
std::string MakeLongKey(size_t length, char c) {
  return std::string(length, c);
}
}  // namespace

TEST_F(DBIteratorTest, 994_IterLongKeys) {
  Open();
  ASSERT_OK(Put(MakeLongKey(20, 0), "0"));
  ASSERT_OK(Put(MakeLongKey(32, 2), "2"));
  ASSERT_OK(Put("a", "b"));
  db_->Flush(FlushOptions());
  ASSERT_OK(Put(MakeLongKey(50, 1), "1"));
  ASSERT_OK(Put(MakeLongKey(127, 3), "3"));
  ASSERT_OK(Put(MakeLongKey(64, 4), "4"));
  auto iter = db_->NewIterator(ReadOptions());

  // Create a key that needs to be skipped for Seq too new
  iter->Seek(MakeLongKey(20, 0));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(20, 0) + "->0");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(64, 4) + "->4");

  iter->SeekForPrev(MakeLongKey(127, 3));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  delete iter;

  iter = db_->NewIterator(ReadOptions());
  iter->Seek(MakeLongKey(50, 1));
  ASSERT_EQ(IterStatus(iter), MakeLongKey(50, 1) + "->1");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(32, 2) + "->2");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), MakeLongKey(127, 3) + "->3");
  delete iter;
  Close();
}

TEST_F(DBIteratorTest, 995_IterNextWithNewerSeq) {
  Open();
  ASSERT_OK(Put("0", "0"));
  db_->Flush(FlushOptions());
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = db_->NewIterator(ReadOptions());
  Options last_options_;  //hainan.liu
   
  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("a"));
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->SeekForPrev(Slice("b"));
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->d");

  delete iter;
  Close();
}

TEST_F(DBIteratorTest, 996_IterPrevWithNewerSeq) {
  Open();
  ASSERT_OK(Put("0", "0"));
  db_->Flush(FlushOptions());
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("d", "e"));
  auto iter = db_->NewIterator(ReadOptions());
  Options last_options_;
  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Seek(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "d->e");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  iter->SeekForPrev(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "d->e");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->d");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  delete iter;
  Close();
}

/* 
//dump  kvdb doesn't support reseek
TEST_F(DBIteratorTest, IterReseek) {
  //Open();
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  options.max_sequential_skip_in_iterations = 3;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  db_options_ = options;
  //DestroyAndReopen(db_options_);
  delete db_;
  Open();
 std::vector<std::string> cf_names;
 cf_names.push_back("pikachu");
 CreateColumnFamilies(cf_names);
 cf_names.push_back("default");

 Status s = TryOpen(cf_names);
  //CreateAndReopenWithCF({"pikachu"}, options);
  
  // insert three keys with same userkey and verify that
  // reseek is not invoked. For each of these test cases,
  // verify that we can find the next key "b".
  ASSERT_OK(Put(1, "a", "zero"));
  ASSERT_OK(Put(1, "a", "one"));
  ASSERT_OK(Put(1, "a", "two"));
  ASSERT_OK(Put(1, "b", "bone"));
  //NUMBER_OF_RESEEKS_IN_ITERATION = 68;
  Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(TestGetTickerCount(db_options_, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "a->two");
  iter->Next();
  //ASSERT_EQ(TestGetTickerCount(db_options_, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of three keys with same userkey and verify
  // that reseek is still not invoked.
  ASSERT_OK(Put(1, "a", "three"));
  iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->three");
  iter->Next();
  //ASSERT_EQ(TestGetTickerCount(db_options_, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // insert a total of four keys with same userkey and verify
  // that reseek is invoked.
  ASSERT_OK(Put(1, "a", "four"));
  iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->four");
  //ASSERT_EQ(TestGetTickerCount(db_options_, NUMBER_OF_RESEEKS_IN_ITERATION), 0);
  iter->Next();
  cout << NUMBER_OF_RESEEKS_IN_ITERATION << endl;
  //ASSERT_EQ(TestGetTickerCount(db_options_, 68), 1);
  ASSERT_EQ(IterStatus(iter), "b->bone");
  delete iter;

  // Testing reverse iterator
  // At this point, we have three versions of "a" and one version of "b".
  // The reseek statistics is already at 1.
  int num_reseeks = static_cast<int>(
      TestGetTickerCount(db_options_, NUMBER_OF_RESEEKS_IN_ITERATION));

  // Insert another version of b and assert that reseek is not invoked
  ASSERT_OK(Put(1, "b", "btwo"));
  iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->btwo");
  //ASSERT_EQ(TestGetTickerCount(db_options_, NUMBER_OF_RESEEKS_IN_ITERATION),
  //          num_reseeks);
  iter->Prev();
  //ASSERT_EQ(TestGetTickerCount(db_options_, NUMBER_OF_RESEEKS_IN_ITERATION),
  //          num_reseeks + 1);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;

  // insert two more versions of b. This makes a total of 4 versions
  // of b and 4 versions of a.
  ASSERT_OK(Put(1, "b", "bthree"));
  ASSERT_OK(Put(1, "b", "bfour"));
  iter = db_->NewIterator(ReadOptions(), handles_[1]);
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "b->bfour");
  //ASSERT_EQ(TestGetTickerCount(db_options_, NUMBER_OF_RESEEKS_IN_ITERATION),
  //          num_reseeks + 2);
  iter->Prev();

  // the previous Prev call should have invoked reseek
  //ASSERT_EQ(TestGetTickerCount(db_options_, NUMBER_OF_RESEEKS_IN_ITERATION),
  //          num_reseeks + 3);
  ASSERT_EQ(IterStatus(iter), "a->four");
  delete iter;
  //Close();
}

*/ 

TEST_F(DBIteratorTest, 997_IterPrevWithNewerSeq2) {
  Open();
  ASSERT_OK(Put("0", "0"));
  db_->Flush(FlushOptions());
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Put("e", "f"));
  auto iter = db_->NewIterator(ReadOptions());
  auto iter2 = db_->NewIterator(ReadOptions());
  iter->Seek(Slice("c"));
  iter2->SeekForPrev(Slice("d"));
  ASSERT_EQ(IterStatus(iter), "c->d");
  ASSERT_EQ(IterStatus(iter2), "c->d");
  Options last_options_;
  // Create a key that needs to be skipped for Seq too new
  for (uint64_t i = 0; i < last_options_.max_sequential_skip_in_iterations + 1;
       i++) {
    ASSERT_OK(Put("b", "f"));
  }

  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->b");
  iter->Prev();
  iter2->Prev();
  ASSERT_EQ(IterStatus(iter2), "a->b");
  iter2->Prev();
  delete iter;
  delete iter2;
  Close();
}


//need to add options in DBBase
TEST_F(DBIteratorTest, 998_IterEmpty) {
  //do {
    Open(); 
    CreateColumnFamiliesAndReopen({"pikachu"});
    // CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
    
    ASSERT_EQ(IterStatus(iter), "(invalid)");
 
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("foo");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekForPrev("foo");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
  //} while (ChangeCompactOptions());
    Close();
}

TEST_F(DBIteratorTest, DISABLED_IterPrefix) {
    Open();
    Options options;
    WriteOptions wropt;
    ReadOptions ropt;
    std::vector<std::string> cf_names;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
    
    cf_names.push_back("one");
    CreateColumnFamilies(cf_names,options);
    cf_names.push_back("default");
    Close();
    ASSERT_OK(TryOpen(cf_names));
    ASSERT_OK(db_->Put(wropt, handles_[0], "abc", "11"));
    ASSERT_OK(db_->Put(wropt, handles_[0], "ac2", "22"));
    ASSERT_OK(db_->Put(wropt, handles_[0], "bc3", "33"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "b1", "44"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "b4", "55"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "bb", "77"));
    cout<<"start to seek"<<endl;
    auto iter = db_->NewIterator(ropt, handles_[0]);
    
    iter -> Seek("bc");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bc3",iter->key().ToString());
    cout<<"iter seek next"<<endl;
    iter -> Next();
    ASSERT_TRUE(!iter->Valid());

    delete iter;
    auto iter1 = db_->NewIterator(ropt, handles_[1]);
    
    iter1 -> Seek("b3");
    ASSERT_TRUE(iter1->Valid());
    ASSERT_EQ("b4",iter1->key().ToString());
        cout<<"iter1 seek next"<<endl;
    iter1 -> Next();
    ASSERT_TRUE(iter1->Valid());
    ASSERT_EQ("bb",iter1->key().ToString());
    iter1 -> Next();
    ASSERT_TRUE(!iter1->Valid());
    delete iter1;
    Close();
}


TEST_F(DBIteratorTest, 1206_InvalidCFHandle) {
  Open();
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

    ASSERT_OK(Put(1, "ka", "va"));
    ASSERT_OK(Put(1, "kb", "vb"));
    ASSERT_OK(Put(1, "kc", "vc"));
    ASSERT_OK(Delete(1, "kb"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kb"));

    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[0]);
    iter->Seek("kb");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    cout <<"kb invalid"<<endl;
    iter->Seek("kc");
    cout <<"kv invalid"<<endl;
    //iter->Prev();
    cout <<"prev"<<endl;
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
    Close();
}



TEST_F(DBIteratorTest, 1207_IterAfterDelete) {
  Open();
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

    ASSERT_OK(Put(1, "ka", "va"));
    ASSERT_OK(Put(1, "kb", "vb"));
    ASSERT_OK(Put(1, "kc", "vc"));
    ASSERT_OK(Delete(1, "kb"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kb"));

    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
    iter->Seek("kb");
    ASSERT_EQ(IterStatus(iter), "kc->vc");
    iter->Seek("kc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "ka->va");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "ka->va"); 
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "kc->vc");

    delete iter;
    Close();
}

/*
TEST_F(DBIteratorTest, IterAfterDelete2) {
  Open();
  CreateAndReopenWithCF({"pikachu","pika", "pipi"}, CurrentOptions());

    ASSERT_OK(Put(1, "ka", "va"));
    ASSERT_OK(Put(1, "kb", "vb"));
    ASSERT_OK(Put(1, "kb1", "vb1"));
    ASSERT_OK(Put(1, "kc", "vc"));
    ASSERT_OK(Delete(1, "kc"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kc"));

    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
    iter->SeekToLast();
    cout<<"seek to first"<<endl;
    ASSERT_EQ(IterStatus(iter), "kb1->vb1");
    ASSERT_OK(Delete(1, "kb1"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kb1"));
//    Iterator* iter1 = db_->NewIterator(ReadOptions(), handles_[1]);  //不管有没有重新new，都不会再检查有没有被delete
    iter->SeekToLast();
    cout<<"seek to first"<<endl;
    ASSERT_EQ(IterStatus(iter), "kb->vb");
    delete iter;

    ASSERT_OK(Put(2, "ka", "va"));
    ASSERT_OK(Put(2, "kb", "vb"));
    ASSERT_OK(Put(2, "kb1", "vb1"));
    ASSERT_OK(Put(2, "kc", "vc"));
    ASSERT_OK(Delete(2, "kc"));
    ASSERT_EQ("NOT_FOUND", Get(2, "kc"));

    Iterator* iter1 = db_->NewIterator(ReadOptions(), handles_[2]);
    iter1->SeekToFirst();
    ASSERT_EQ(IterStatus(iter1), "ka->va");
    iter1->Next();
    ASSERT_EQ(IterStatus(iter1), "kb->vb");
    ASSERT_OK(Delete(2, "kb1"));
    iter1->Next();
    ASSERT_EQ(IterStatus(iter1), "invalid");
    delete iter1;

    ASSERT_OK(Put(3, "ka", "va"));
    ASSERT_OK(Put(3, "kb", "vb"));
    ASSERT_OK(Put(3, "kb1", "vb1"));
    ASSERT_OK(Put(3, "kc", "vc"));
    ASSERT_OK(Delete(3, "ka"));
    ASSERT_EQ("NOT_FOUND", Get(3, "ka"));

    Iterator* iter2 = db_->NewIterator(ReadOptions(), handles_[3]);
    iter2->SeekToFirst();
    ASSERT_EQ(IterStatus(iter2), "kb->vb");
    iter2->SeekToLast();
    ASSERT_EQ(IterStatus(iter2), "kc->vc");
    ASSERT_OK(Delete(3, "kb1"));
    iter2->Prev();
    ASSERT_EQ(IterStatus(iter2), "kb->vb");
    iter2->SeekForPrev("kb1");
    ASSERT_EQ(IterStatus(iter2), "kb->vb");
    delete iter2;

    Close();
}
*/
TEST_F(DBIteratorTest, 1208_IterBeforeDelete) {
  Open();
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
    ASSERT_OK(Put(1, "ka", "va"));
    ASSERT_OK(Put(1, "kb", "vb"));
    ASSERT_OK(Put(1, "kc", "vc"));
    ASSERT_OK(Delete(1, "kb"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kb"));

//    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
    iter->Seek("kb");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->Seek("kc");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
 Close();
}


TEST_F(DBIteratorTest, 999_IterMultiWithDelete) {
  Open();
  do {
 //Open();
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "ka", "va"));
    ASSERT_OK(Put(1, "kb", "vb"));
    ASSERT_OK(Put(1, "kc", "vc"));
    ASSERT_OK(Delete(1, "kb"));
    ASSERT_EQ("NOT_FOUND", Get(1, "kb"));

    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
    iter->Seek("kc");
    ASSERT_EQ(IterStatus(iter), "kc->vc");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ &&
          kHashSkipList != option_config_) {  // doesn't support SeekToLast
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "ka->va");
      }
    }
    delete iter;
  } while (ChangeOptions());
 // } while (ChangeCompactOptions());
 Close();
}


TEST_F(DBIteratorTest, SeekToLastTest){
  Open();
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  ASSERT_OK(Put(0, "key1", "v1"));
  ASSERT_OK(Put(0, "key2", "v2"));
  ASSERT_OK(Put(0, "key3", "v3"));
  ASSERT_OK(Put(0, "key4", "v4"));
  ASSERT_OK(Put(0, "key5", "v5"));
  ReadOptions ro;
  Iterator* iter1 = db_->NewIterator(ro, handles_[0]);
  iter1->SeekToLast();
  ASSERT_EQ(IterStatus(iter1), "key5->v5");
  delete iter1;
  
  ASSERT_OK(Put(1, "key1", "v1"));
  ASSERT_OK(Put(1, "key2", "v2"));
  ASSERT_OK(Put(1, "key3", "v3"));
  ASSERT_OK(Put(1, "key4", "v4"));
  ASSERT_OK(Put(1, "key5", "v5"));
  Iterator* iter2 = db_->NewIterator(ro, handles_[1]);
  iter2->SeekToLast();
  ASSERT_EQ(IterStatus(iter2), "key5->v5");
  delete iter2;

  Close(); 
} 

TEST_F(DBIteratorTest, 1000_IterPrevMaxSkip) {
 Open(); 
// do {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  for (int i = 0; i < 2; i++) {
      ASSERT_OK(Put(1, "key1", "v1"));
      ASSERT_OK(Put(1, "key2", "v2"));
      ASSERT_OK(Put(1, "key3", "v3"));
      ASSERT_OK(Put(1, "key4", "v4"));
      ASSERT_OK(Put(1, "key5", "v5"));
    }
    cout <<"-------------verify key5-----------"<<endl;
    VerifyIterLast("key5->v5", 1);
    cout <<"-------------verify key5-----------"<<endl;
    ASSERT_OK(Delete(1, "key5"));
    VerifyIterLast("key4->v4", 1);

    ASSERT_OK(Delete(1, "key4"));
    VerifyIterLast("key3->v3", 1);

    ASSERT_OK(Delete(1, "key3"));
    VerifyIterLast("key2->v2", 1);

    ASSERT_OK(Delete(1, "key2"));
    VerifyIterLast("key1->v1", 1);

    ASSERT_OK(Delete(1, "key1"));
    VerifyIterLast("(invalid)", 1);
    cout <<"-------------finished------------"<<endl;
    //Close();
//  } while (ChangeOptions(kSkipMergePut | kSkipNoSeekToLast));
    Close();
}

TEST_F(DBIteratorTest, 1001_IterSingle) {
//  do {
    Open();
    //CreateColumnFamiliesAndReopen({"pikachu"});
    std::vector<std::string> cf_names;
    cf_names.push_back("pikachu");
    CreateColumnFamilies(cf_names);
    cf_names.push_back("default");
    Status s = TryOpen(cf_names);
    ASSERT_OK(Put(1, "a", "va"));
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("b");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
//  } while (ChangeCompactOptions());
    Close();
}


TEST_F(DBIteratorTest, 1002_IterMulti) {
  //do {
    Open();
    //CreateColumnFamiliesAndReopen({"pikachu"});
    std::vector<std::string> cf_names;
    cf_names.push_back("pikachu");
    CreateColumnFamilies(cf_names);
    cf_names.push_back("default");
    Status s = TryOpen(cf_names);    
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", "vb"));
    ASSERT_OK(Put(1, "c", "vc"));
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->Seek("");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("a");
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("ax");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->SeekForPrev("d");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->SeekForPrev("c");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->SeekForPrev("bx");
    ASSERT_EQ(IterStatus(iter), "b->vb");

    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Seek("z");
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekForPrev("b");
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->SeekForPrev("");
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    // Switch from reverse to forward
    iter->SeekToLast();
    iter->Prev();
    iter->Prev();
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Switch from forward to reverse
    iter->SeekToFirst();
    iter->Next();
    iter->Next();
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");

    // Make sure iter stays at snapshot
    ASSERT_OK(Put(1, "a", "va2"));
    ASSERT_OK(Put(1, "a2", "va3"));
    ASSERT_OK(Put(1, "b", "vb2"));
    ASSERT_OK(Put(1, "c", "vc2"));
    ASSERT_OK(Delete(1, "b"));
    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->vb");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");
   
    delete iter;
 // } while (ChangeCompactOptions());
   // Close();
}

TEST_F(DBIteratorTest, 1003_IterSmallAndLargeMix) {
  //do {
    Open();
    CreateColumnFamiliesAndReopen({"pikachu"});
    ASSERT_OK(Put(1, "a", "va"));
    ASSERT_OK(Put(1, "b", std::string(100000, 'b')));
    ASSERT_OK(Put(1, "c", "vc"));
    ASSERT_OK(Put(1, "d", std::string(100000, 'd')));
    ASSERT_OK(Put(1, "e", std::string(100000, 'e')));

    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);

    iter->SeekToFirst();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Next();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    iter->SeekToLast();
    ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "(invalid)");

    delete iter;
 // } while (ChangeCompactOptions());
    Close();
}

TEST_F(DBIteratorTest, 1004_IterWithSnapshot) {
  Open();
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
 do {
 //   Open();
    CreateAndReopenWithCF({"pikachu"},CurrentOptions(options_override));
    //CreateColumnFamiliesAndReopen({"pikachu"});
 //Options db_options;
 //db_options.create_if_missing = true; 
 //db_options_ = db_options;
// Open();
// std::vector<std::string> cf_names;
// cf_names.push_back("pikachu");
// CreateColumnFamilies(cf_names);
// cf_names.push_back("default");
// Status s = TryOpen(cf_names);
 
    ASSERT_OK(Put(1, "key1", "val1"));
    ASSERT_OK(Put(1, "key2", "val2"));
    ASSERT_OK(Put(1, "key3", "val3"));
    ASSERT_OK(Put(1, "key4", "val4"));
    ASSERT_OK(Put(1, "key5", "val5"));

    const Snapshot* snapshot = db_->GetSnapshot();
    ReadOptions options;
    options.snapshot = snapshot;
    Iterator* iter = db_->NewIterator(options, handles_[1]);

    ASSERT_OK(Put(1, "key0", "val0"));
    // Put more values after the snapshot
    ASSERT_OK(Put(1, "key100", "val100"));
    ASSERT_OK(Put(1, "key101", "val101"));

    iter->Seek("key5");
    ASSERT_EQ(IterStatus(iter), "key5->val5");
    if (!CurrentOptions().merge_operator) {
      // TODO: merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ && kHashSkipList != option_config_) {
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key3->val3");

        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key4->val4");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key5->val5");
      }
      iter->Next();
      ASSERT_TRUE(!iter->Valid());
    }

    if (!CurrentOptions().merge_operator) {
      // TODO(gzh): merge operator does not support backward iteration yet
      if (kPlainTableAllBytesPrefix != option_config_ &&
          kBlockBasedTableWithWholeKeyHashIndex != option_config_ &&
          kHashLinkList != option_config_ && kHashSkipList != option_config_) {
        iter->SeekForPrev("key1");
        ASSERT_EQ(IterStatus(iter), "key1->val1");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key2->val2");
        iter->Next();
        ASSERT_EQ(IterStatus(iter), "key3->val3");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key2->val2");
        iter->Prev();
        ASSERT_EQ(IterStatus(iter), "key1->val1");
        iter->Prev();
        ASSERT_TRUE(!iter->Valid());
      }
    }
    db_->ReleaseSnapshot(snapshot);
    delete iter;
    //Close();
    // skip as HashCuckooRep does not support snapshot
  } while (ChangeOptions(kSkipHashCuckoo));
  Close();
}


//iter->status()
  // If an error has occurred, return it.  Else return an ok status.
  // If non-blocking IO is requested and this operation cannot be
  // satisfied without doing some IO, then this returns Status::Incomplete().
TEST_F(DBIteratorTest, 1005_IteratorStatusOKTest) {
  //Open();
 // do {
    Open();
    //CreateColumnFamiliesAndReopen({"pikachu"}); //CurrentOptions());
  std::vector<std::string> cf_names;
  cf_names.push_back("pikachu");
  CreateColumnFamilies(cf_names);
  cf_names.push_back("default");
  Status s = TryOpen(cf_names);
    Put(1, "foo", "hello");
    // Get iterator that will yield the current contents of the DB.
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
    // Write to force compactions
    Put(1, "foo", "newvalue1");
    for (int i = 0; i < 100; i++) {
      // 100K values
      ASSERT_OK(Put(1, Key(i), Key(i) + std::string(100000, 'v')));
    }
    Put(1, "foo1", "var1");
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ("hello", iter->value().ToString());
    iter->SeekToLast();
    iter->Next();
    ASSERT_OK(iter->status());
    iter->SeekForPrev("foo");
    ASSERT_OK(iter->status());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ("hello", iter->value().ToString());
    delete iter;
    Close();
}



TEST_F(DBIteratorTest, 1006_IteratorPinsRef) {
  //Open();
 // do {
    Open();
    //CreateColumnFamiliesAndReopen({"pikachu"}); //CurrentOptions());
  std::vector<std::string> cf_names;
  cf_names.push_back("pikachu");
  CreateColumnFamilies(cf_names);
  cf_names.push_back("default");
  Status s = TryOpen(cf_names);    
    Put(1, "foo", "hello");
    // Get iterator that will yield the current contents of the DB.
    Iterator* iter = db_->NewIterator(ReadOptions(), handles_[1]);
    // Write to force compactions
    Put(1, "foo", "newvalue1");
    for (int i = 0; i < 100; i++) {
      // 100K values
      ASSERT_OK(Put(1, Key(i), Key(i) + std::string(100000, 'v')));
    }
    Put(1, "foo", "newvalue2");

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ("hello", iter->value().ToString());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
 // } while (ChangeCompactOptions());
    Close();
}


TEST_F(DBIteratorTest, 1007_DBIteratorBoundTest) {
  Open();
  Options options = CurrentOptions();
  
  options.env = env_;
  options.create_if_missing = true;

  options.prefix_extractor = nullptr;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing basic case with no iterate_upper_bound and no prefix_extractor
  {
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("g1")), 0);

    iter->SeekForPrev("g1");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("g1")), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);
  }
  cout <<"---------1---------------------"<<endl;
  // testing iterate_upper_bound and forward iterator
  // to make sure it stops at bound
  {
    ReadOptions ro;
    // iterate_upper_bound points beyond the last expected entry
    Slice prefix("foo2");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("foo")), 0);
   cout <<"---------1-1---------------------"<<endl;
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("foo1")), 0);
   cout <<"---------1-2---------------------"<<endl;
    iter->Next();
    // should stop here...
   cout <<"---------1-3---------------------"<<endl;
    ASSERT_TRUE(!iter->Valid());
  }
  cout <<"---------2---------------------"<<endl;
  // Testing SeekToLast with iterate_upper_bound set
  {
    ReadOptions ro;

    Slice prefix("foo");
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("a")), 0);
  }
  cout <<"---------3---------------------"<<endl;
  // prefix is the first letter of the key
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));

  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "0"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("g1", "0"));

  // testing with iterate_upper_bound and prefix_extractor
  // Seek target and iterate_upper_bound are not is same prefix
  // This should be an error
  {
    ReadOptions ro;
    Slice upper_bound("g");
    ro.iterate_upper_bound = &upper_bound;

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek("foo");

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo1", iter->key().ToString());

    iter->Next();
    ASSERT_TRUE(!iter->Valid());
  }
  cout <<"---------4---------------------"<<endl;
  // testing that iterate_upper_bound prevents iterating over deleted items
  // if the bound has already reached
  {
    options.prefix_extractor = nullptr;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_OK(Put("b", "0"));
    ASSERT_OK(Put("b1", "0"));
    ASSERT_OK(Put("c", "0"));
    ASSERT_OK(Put("d", "0"));
    ASSERT_OK(Put("e", "0"));
    ASSERT_OK(Delete("c"));
    ASSERT_OK(Delete("d"));

    // base case with no bound
    ReadOptions ro;
    ro.iterate_upper_bound = nullptr;

    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    get_perf_context()->Reset();
    iter->Next();

    ASSERT_TRUE(iter->Valid());
  //???  ASSERT_EQ(static_cast<int>(get_perf_context()->internal_delete_skipped_count), 2);

    // now testing with iterate_bound
    Slice prefix("c");
    ro.iterate_upper_bound = &prefix;

    iter.reset(db_->NewIterator(ro));

    get_perf_context()->Reset();

    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Slice("b")), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(("b1")), 0);

    iter->Next();
    // the iteration should stop as soon as the bound key is reached
    // even though the key is deleted
    // hence internal_delete_skipped_count should be 0
    ASSERT_TRUE(!iter->Valid());
    //???ASSERT_EQ(static_cast<int>(get_perf_context()->internal_delete_skipped_count), 0);
  }
  Close();
}

/*
////need to add FlushBlockEveryKeyPolicyFactory related code
TEST_F(DBIteratorTest, 1008_DBIteratorBoundOptimizationTest) {
  int upper_bound_hits = 0;
  Open();
  Options options = CurrentOptions();
//  rocksdb::SyncPoint::GetInstance()->SetCallBack(
//      "BlockBasedTable::BlockEntryIteratorState::KeyReachedUpperBound",
//      [&upper_bound_hits](void* arg) {
//        assert(arg != nullptr);
//        upper_bound_hits += (*static_cast<bool*>(arg) ? 1 : 0);
//      });
//  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  options.env = env_;
  options.create_if_missing = true;
  options.prefix_extractor = nullptr;
  BlockBasedTableOptions table_options;
//  table_options.flush_block_policy_factory =
//    std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("foo2", "bar2"));
  ASSERT_OK(Put("foo4", "bar4"));
  ASSERT_OK(Flush());
  cout <<"After flush"<<endl;
  Slice ub("foo3");
  ReadOptions ro;
  ro.iterate_upper_bound = &ub;

  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("foo1")), 0);
  //ASSERT_EQ(upper_bound_hits, 0);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().compare(Slice("foo2")), 0);
  //ASSERT_EQ(upper_bound_hits, 0);

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  //ASSERT_EQ(upper_bound_hits, 1);
  //delete iter;
  //Close();
}
*/
//dump 
// 7月份讨论
TEST_F(DBIteratorTest, 1209_NonBlockingIteration){
 Open();
 int count=0;
 do {
    ReadOptions non_blocking_opts, regular_opts;
    //Open();
    Options options;
    options.statistics = rocksdb::CreateDBStatistics();
    non_blocking_opts.read_tier = kBlockCacheTier;   //// read from block cache only.data in memtable or block cache
    options.create_if_missing = true;
    db_options_ = options;
 std::vector<std::string> cf_names;
 cf_names.push_back("pikachu");
 CreateColumnFamilies(cf_names);
 cf_names.push_back("default");
 //delete db_;
 Status s = TryOpen(cf_names);
    //DestroyAndReopen(options);
    //CreateAndReopenWithCF({"pikachu"}, options);
    //CreateColumnFamiliesAndReopen({"pikachu"});   
    // write one kv to the database.
     ASSERT_OK(Put(1, "a", "b"));

    // scan using non-blocking iterator. We should find it because
    // it is in memtable.
    Iterator* iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_EQ(count, 1);
    cout <<"----------------1-------------------"<<endl;
    delete iter;
    cout <<"----------------2-------------------"<<endl;
    // flush memtable to storage. Now, the key should not be in the
    // memtable neither in the block cache.
    ASSERT_OK(Flush());
    cout <<"-----------------3------------------"<<endl;
    // verify that a non-blocking iterator does not find any
    // kvs. Neither does it do any IOs to storage.
    uint64_t numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    uint64_t cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      count++;
    }
    cout <<"------------------4-----------------"<<endl;
    //ASSERT_EQ(count, 0);  //dump
    cout <<"-------------------5----------------"<<endl;
   // ASSERT_TRUE(iter->status().IsIncomplete());
    ASSERT_TRUE(iter->status().ok());

    cout <<"-------------------6----------------"<<endl;
    //ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    //ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // read in the specified block via a regular get
    ASSERT_EQ(Get(1, "a"), "b");
    
    // verify that we can find it via a non-blocking scan
    //numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    //cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    cout <<"-------------------7----------------"<<endl;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    cout <<"-------------------8----------------"<<endl;
    ASSERT_EQ(count, 1);
    //ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    //ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;
    //Close();
    //DestroyDB(dbname_, options); 
    // This test verifies block cache behaviors, which is not used by plain
    // table format.
    // Exclude kHashCuckoo as it does not support iteration currently
  } while (ChangeOptions(kSkipPlainTable | kSkipNoSeekToLast | kSkipHashCuckoo |
                         kSkipMmapReads));
  Close();
}


//////dump managed option not supported in kvdb. Also not supported in Rocksdb any more.
#ifndef ROCKSDB_LITE
TEST_F(DBIteratorTest, DISABLED_ManagedNonBlockingIteration) {
  Open();
  do {
    ReadOptions non_blocking_opts, regular_opts;
    Options options = CurrentOptions();
    options.statistics = rocksdb::CreateDBStatistics();
    non_blocking_opts.read_tier = kBlockCacheTier;
    non_blocking_opts.managed = true;
    options.create_if_missing = true;
    db_options_ = options;
 std::vector<std::string> cf_names;
 cf_names.push_back("pikachu");
 CreateColumnFamilies(cf_names);
 cf_names.push_back("default");
 Status s = TryOpen(cf_names);
    //CreateAndReopenWithCF({"pikachu"}, options);
    // write one kv to the database.
    ASSERT_OK(Put(1, "a", "b"));

    // scan using non-blocking iterator. We should find it because
    // it is in memtable.
    Iterator* iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    int count = 0;
    //for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    //  ASSERT_OK(iter->status());
    //  count++;
    //}
    iter->SeekToFirst();   //for debug
    ASSERT_OK(iter->status()); //for debug :Invalid argument: Managed Iterators not supported in KVDB
    ASSERT_EQ("a", iter->key().ToString()); //for debug
    
    cout <<"----------------1---------------"<<endl;
    ASSERT_EQ(count, 1);
    delete iter;

    // flush memtable to storage. Now, the key should not be in the
    // memtable neither in the block cache.
    ASSERT_OK(Flush());

    // verify that a non-blocking iterator does not find any
    // kvs. Neither does it do any IOs to storage.
    int64_t numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    int64_t cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      count++;
    }
    cout <<"----------------2---------------"<<endl;
    ASSERT_EQ(count, 0);
    ASSERT_TRUE(iter->status().IsIncomplete());
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;
   // read in the specified block via a regular get
    ASSERT_EQ(Get(1, "a"), "b");

    // verify that we can find it via a non-blocking scan
    numopen = TestGetTickerCount(options, NO_FILE_OPENS);
    cache_added = TestGetTickerCount(options, BLOCK_CACHE_ADD);
    iter = db_->NewIterator(non_blocking_opts, handles_[1]);
    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    cout <<"----------------3---------------"<<endl;
    ASSERT_EQ(count, 1);
    ASSERT_EQ(numopen, TestGetTickerCount(options, NO_FILE_OPENS));
    ASSERT_EQ(cache_added, TestGetTickerCount(options, BLOCK_CACHE_ADD));
    delete iter;

    // This test verifies block cache behaviors, which is not used by plain
    // table format.
    // Exclude kHashCuckoo as it does not support iteration currently
  } while (ChangeOptions(kSkipPlainTable | kSkipNoSeekToLast | kSkipHashCuckoo |
                         kSkipMmapReads));
  Close();
}
#endif  // ROCKSDB_LITE
////dump

//This option allows Get and MultiGet to read only the persited data and skip mem-tables if writes were done with disableWAL = true.
TEST_F(DBIteratorTest, 1009_PersistedTierOnIterator) {
  // The test needs to be changed if kPersistedTier is supported in iterator.
  //Options options = CurrentOptions();
  Open();
  //CreateColumnFamiliesAndReopen({"pikachu"});
  std::vector<std::string> cf_names;
cf_names.push_back("pikachu");
CreateColumnFamilies(cf_names);
cf_names.push_back("default");
Status s = TryOpen(cf_names);
  ReadOptions ropt;
  ropt.read_tier = kPersistedTier;

  auto* iter = db_->NewIterator(ropt, handles_[1]);
  ASSERT_TRUE(iter->status().IsNotSupported());  //7月份要实现
  cout <<iter->status().ToString() <<endl;
  //iter->status().IsNotSupported();
  delete iter;
  cout <<"---------------------------------" <<endl;
  std::vector<Iterator*> iters;
  //s = db_->NewIterators(ropt, {handles_[1]}, &iters);
  //cout <<s.ToString() <<endl;
  //ASSERT_OK(db_->NewIterators(ropt, {handles_[1]}, &iters));
  ASSERT_TRUE(db_->NewIterators(ropt, {handles_[1]}, &iters).IsNotSupported());//7月份要实现
  for (auto it : iters) {
      delete it;
  }
  Close();
}

//
TEST_F(DBIteratorTest, 1010_PrevAfterAndNextAfterMerge) {
  Open();
  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.env = env_;
  DestroyAndReopen(options);

  // write three entries with different keys using Merge()
  WriteOptions wopts;
 Status s1 = db_->Merge(wopts, "1", "data1");
 Status s2 = db_->Merge(wopts, "2", "data2");
 Status s3 = db_->Merge(wopts, "3", "data3");
 cout <<s1.ToString() << endl;
 cout <<s2.ToString() << endl; 
 cout <<s3.ToString() << endl;
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));

  it->Seek("2");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("2", it->key().ToString());

  it->Prev();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("1", it->key().ToString());

  it->SeekForPrev("1");
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("1", it->key().ToString());

  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ("2", it->key().ToString());
cout <<"---------------------------------" <<endl;
  //delete it;
  //Close();
}

/*
//DUMP
TEST_F(DBIteratorTest, 1238_PinnedDataIteratorRandomized) {
  enum TestConfig {
    NORMAL,
    CLOSE_AND_OPEN,
  //  COMPACT_BEFORE_READ, //not supported by kvdb
    FLUSH_EVERY_1000,
    MAX
  };

  // Generate Random data
  Random rnd(301);

  int puts = 100; //原来是 100000;
  int key_pool = static_cast<int>(puts * 0.7);
  int key_size = 100;
  int val_size = 1000;
  int seeks_percentage = 20;   // 20% of keys will be used to test seek()
  int delete_percentage = 20;  // 20% of keys will be deleted
  int merge_percentage = 20;   // 20% of keys will be added using Merge()
  Open();
  for (int run_config = 0; run_config < TestConfig::MAX; run_config++) {
    Options options = CurrentOptions();
    BlockBasedTableOptions table_options;
    table_options.use_delta_encoding = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.merge_operator = MergeOperators::CreatePutOperator();
    DestroyAndReopen(options);
cout <<"----------------Reopened---------------"<<endl;
    std::vector<std::string> generated_keys(key_pool);
    for (int i = 0; i < key_pool; i++) {
      generated_keys[i] = RandomString(&rnd, key_size);
    }
cout <<"----------------Random---------------"<<endl;

    std::map<std::string, std::string> true_data;
    std::vector<std::string> random_keys;
    std::vector<std::string> deleted_keys;
    for (int i = 0; i < puts; i++) {
      auto& k = generated_keys[rnd.Next() % key_pool];
      auto v = RandomString(&rnd, val_size);

      // Insert data to true_data map and to DB
      true_data[k] = v;
      if (rnd.OneIn(static_cast<int>(100.0 / merge_percentage))) {
//cout <<"----------------merge---------------"<<endl;  
      ASSERT_OK(db_->Merge(WriteOptions(), k, v));
      } else {
        ASSERT_OK(Put(k, v));
      }
//cout <<"----------------merged---------------"<<endl;
      // Pick random keys to be used to test Seek()
      if (rnd.OneIn(static_cast<int>(100.0 / seeks_percentage))) {
        random_keys.push_back(k);
      }
//cout <<"----------------pushback---------------"<<endl;
      // Delete some random keys
      if (rnd.OneIn(static_cast<int>(100.0 / delete_percentage))) {
        deleted_keys.push_back(k);
        true_data.erase(k);
        ASSERT_OK(Delete(k));
      }
//cout <<"----------------deleted---------------"<<endl;
      if (run_config == TestConfig::FLUSH_EVERY_1000) {
        if (i && i % 1000 == 0) {

cout <<"----------------flush---------------"<<endl;

          Flush();
        }
      }
    }
cout <<"----------------0---------------"<<endl;
    if (run_config == TestConfig::CLOSE_AND_OPEN) {
      Close();
      Reopen(options);
    } //else if (run_config == TestConfig::COMPACT_BEFORE_READ) {
   //cout <<"----------------compact---------------"<<endl;
      //db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
   // }

    ReadOptions ro;
    ro.pin_data = true;
    auto iter = db_->NewIterator(ro);

    {
      // Test Seek to random keys
      std::vector<Slice> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->Seek(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.lower_bound(k), true_data.end());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key());
        true_keys.push_back(true_data.lower_bound(k)->first);
      }

      for (size_t i = 0; i < keys_slices.size(); i++) {
        ASSERT_EQ(keys_slices[i].ToString(), true_keys[i]);
      }
    }
    cout <<"----------------1---------------"<<endl;
    {
      // Test SeekForPrev to random keys
      std::vector<Slice> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->SeekForPrev(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.upper_bound(k), true_data.begin());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key());
        true_keys.push_back((--true_data.upper_bound(k))->first);
      }

      for (size_t i = 0; i < keys_slices.size(); i++) {
        ASSERT_EQ(keys_slices[i].ToString(), true_keys[i]);
      }
    }
    cout <<"----------------2---------------"<<endl;
    {
      // Test iterating all data forward
      std::vector<Slice> all_keys;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key());
      }
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid
      auto data_iter = true_data.begin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i].ToString(), data_iter->first);
        data_iter++;
      }
    }
    cout <<"----------------3---------------"<<endl;
    {
      // Test iterating all data backward
      std::vector<Slice> all_keys;
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key());
      }
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid (backward)
      auto data_iter = true_data.rbegin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i].ToString(), data_iter->first);
        data_iter++;
      }
    }

    delete iter;
  }
  Close();
}
*/

TEST_F(DBIteratorTest, 1084_PinnedDataIteratorRandomized) {
  enum TestConfig {
    NORMAL,
    CLOSE_AND_OPEN,
  //  COMPACT_BEFORE_READ, //not supported by kvdb
    FLUSH_EVERY_1000,
    MAX
  };

  Random rnd(301);

  int puts = 100; //鍘熸潵鏄?100000;
  int key_pool = static_cast<int>(puts * 0.7);
  int key_size = 100;
  int val_size = 1000;
  int seeks_percentage = 20;   // 20% of keys will be used to test seek()
  int delete_percentage = 20;  // 20% of keys will be deleted
  int merge_percentage = 20;   // 20% of keys will be added using Merge()
  Open();
  for (int run_config = 0; run_config < TestConfig::MAX; run_config++) { 
    Options options = CurrentOptions();
    BlockBasedTableOptions table_options;
    table_options.use_delta_encoding = false;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.merge_operator = MergeOperators::CreatePutOperator();
    DestroyAndReopen(options);
    std::vector<std::string> generated_keys(key_pool);
    for (int i = 0; i < key_pool; i++) {
      generated_keys[i] = RandomString(&rnd, key_size);
    }

    std::map<std::string, std::string> true_data;
    std::vector<std::string> random_keys;
    std::vector<std::string> deleted_keys;
    for (int i = 0; i < puts; i++) {
      auto& k = generated_keys[rnd.Next() % key_pool];
      auto v = RandomString(&rnd, val_size);

      // Insert data to true_data map and to DB
      true_data[k] = v;
      if (rnd.OneIn(static_cast<int>(100.0 / merge_percentage))) {
        
      ASSERT_OK(db_->Merge(WriteOptions(), k, v));
      } else {
        ASSERT_OK(Put(k, v));
      }

      // Pick random keys to be used to test Seek()
      if (rnd.OneIn(static_cast<int>(100.0 / seeks_percentage))) {
        random_keys.push_back(k);
      }
      // Delete some random keys
      if (rnd.OneIn(static_cast<int>(100.0 / delete_percentage))) {
        deleted_keys.push_back(k);
        true_data.erase(k);
        ASSERT_OK(Delete(k));
      }
      if (run_config == TestConfig::FLUSH_EVERY_1000) {
        if (i && i % 1000 == 0) {
         cout <<"----------------flush---------------"<<endl;
          Flush();
        }
      }
    }

      if (run_config == TestConfig::CLOSE_AND_OPEN) {
	   
      Status s = TryReopen();
	  //cout <<"----------------TryReopen() is --------------"<<s.ToString()<<endl;
    }   
      
    ReadOptions ro;
    ro.pin_data = true;
    auto iter = db_->NewIterator(ro);

    {
      // Test Seek to random keys
      //std::vector<Slice> keys_slices;
	  std::vector<std::string> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->Seek(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.lower_bound(k), true_data.end());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key().ToString());
        true_keys.push_back(true_data.lower_bound(k)->first);
      }
      int t = keys_slices.size();
      for (size_t i = 0; i < t; i++) {
           ASSERT_EQ(keys_slices[i], true_keys[i]);
      }
    }
    cout <<"----------------1---------------"<<endl;
    {
      // Test SeekForPrev to random keys
      std::vector<std::string> keys_slices;
      std::vector<std::string> true_keys;
      for (auto& k : random_keys) {
        iter->SeekForPrev(k);
        if (!iter->Valid()) {
          ASSERT_EQ(true_data.upper_bound(k), true_data.begin());
          continue;
        }
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        keys_slices.push_back(iter->key().ToString());
        true_keys.push_back((--true_data.upper_bound(k))->first);
      }

      for (size_t i = 0; i < keys_slices.size(); i++) {
        ASSERT_EQ(keys_slices[i], true_keys[i]);
      }
    }
    cout <<"----------------2---------------"<<endl;
    {
      // Test iterating all data forward
      std::vector<std::string> all_keys;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key().ToString());
      }
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid
      auto data_iter = true_data.begin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i], data_iter->first);
        data_iter++;
      }
    }
    cout <<"----------------3---------------"<<endl;
    {
      // Test iterating all data backward
      std::vector<std::string> all_keys;
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        std::string prop_value;
        ASSERT_OK(
            iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
        ASSERT_EQ("1", prop_value);
        all_keys.push_back(iter->key().ToString());
      }
      ASSERT_EQ(all_keys.size(), true_data.size());

      // Verify that all keys slices are valid (backward)
      auto data_iter = true_data.rbegin();
      for (size_t i = 0; i < all_keys.size(); i++) {
        ASSERT_EQ(all_keys[i], data_iter->first);
        data_iter++;
      }
    }

    delete iter;
  }
  Close();
}

TEST_F(DBIteratorTest, 1084_PinnedDataIteratorReadAfterUpdate) {
  Open();
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.write_buffer_size = 100000;
  DestroyAndReopen(options);

  Random rnd(301);

  std::map<std::string, std::string> true_data;
  for (int i = 0; i < 1000; i++) {
    std::string k = RandomString(&rnd, 10);
    std::string v = RandomString(&rnd, 1000);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = db_->NewIterator(ro);

  // Delete 50% of the keys and update the other 50%
  for (auto& kv : true_data) {
    if (rnd.OneIn(2)) {
      ASSERT_OK(Delete(kv.first));
    } else {
      std::string new_val = RandomString(&rnd, 1000);
      ASSERT_OK(Put(kv.first, new_val));
    }
  }

  std::vector<std::pair<std::string, std::string>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key().ToString(), iter->value().ToString());
  }

  auto data_iter = true_data.begin();
  for (size_t i = 0; i < results.size(); i++, data_iter++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, data_iter->first);
    ASSERT_EQ(kv.second, data_iter->second);
  }

  delete iter;
}

TEST_F(DBIteratorTest, 1210_PinnedDataIteratorMergeOperator) {
  Open();
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  DestroyAndReopen(options);

  std::string numbers[7];
  for (int val = 0; val <= 6; val++) {
    PutFixed64(numbers + val, val);
  }

  // +1 all keys in range [ 0 => 999]
  for (int i = 0; i < 1000; i++) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[1]));
  }

  // +2 all keys divisible by 2 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 2) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[2]));
  }

  // +3 all keys divisible by 5 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 5) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[3]));
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = db_->NewIterator(ro);

  std::vector<std::pair<std::string, std::string>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key().ToString(), iter->value().ToString());
  }

  ASSERT_EQ(results.size(), 1000);
  for (size_t i = 0; i < results.size(); i++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, Key(static_cast<int>(i)));
    int expected_val = 1;
    if (i % 2 == 0) {
      expected_val += 2;
    }
    if (i % 5 == 0) {
      expected_val += 3;
    }
    ASSERT_EQ(kv.second, numbers[expected_val]);
  }

  delete iter;
}




/*
//
//
//DUMP need to check whether Merge is supported later 
TEST_F(DBIteratorTest, 1210_PinnedDataIteratorMergeOperator) {
  Open();
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  DestroyAndReopen(options);

  std::string numbers[7];
  for (int val = 0; val <= 6; val++) {
    PutFixed64(numbers + val, val);
  }

  // +1 all keys in range [ 0 => 999]
  for (int i = 0; i < 1000; i++) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[1]));
  }

  // +2 all keys divisible by 2 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 2) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[2]));
  }

  // +3 all keys divisible by 5 in range [ 0 => 999]
  for (int i = 0; i < 1000; i += 5) {
    WriteOptions wo;
    ASSERT_OK(db_->Merge(wo, Key(i), numbers[3]));
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = db_->NewIterator(ro);

  std::vector<std::pair<Slice, std::string>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key(), iter->value().ToString());
  }

  ASSERT_EQ(results.size(), 1000);
  for (size_t i = 0; i < results.size(); i++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, Key(static_cast<int>(i)));
    int expected_val = 1;
    if (i % 2 == 0) {
      expected_val += 2;
    }
    if (i % 5 == 0) {
      expected_val += 3;
    }
    ASSERT_EQ(kv.second, numbers[expected_val]);
  }

  delete iter;
}

*/
//
////Dump

/*
TEST_F(DBIteratorTest, 1084_PinnedDataIteratorReadAfterUpdate) {
  Open();
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.use_delta_encoding = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.write_buffer_size = 100000;
  DestroyAndReopen(options);

  Random rnd(301);

  std::map<std::string, std::string> true_data;
  for (int i = 0; i < 1000; i++) {
    std::string k = RandomString(&rnd, 10);
    std::string v = RandomString(&rnd, 1000);
    ASSERT_OK(Put(k, v));
    true_data[k] = v;
  }

  ReadOptions ro;
  ro.pin_data = true;
  auto iter = db_->NewIterator(ro);

  // Delete 50% of the keys and update the other 50%
  for (auto& kv : true_data) {
    if (rnd.OneIn(2)) {
      ASSERT_OK(Delete(kv.first));
    } else {
      std::string new_val = RandomString(&rnd, 1000);
      ASSERT_OK(Put(kv.first, new_val));
    }
  }

  std::vector<std::pair<Slice, std::string>> results;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string prop_value;
    ASSERT_OK(iter->GetProperty("rocksdb.iterator.is-key-pinned", &prop_value));
    ASSERT_EQ("1", prop_value);
    results.emplace_back(iter->key(), iter->value().ToString());
  }

  auto data_iter = true_data.begin();
  for (size_t i = 0; i < results.size(); i++, data_iter++) {
    auto& kv = results[i];
    ASSERT_EQ(kv.first, data_iter->first);
    ASSERT_EQ(kv.second, data_iter->second);
  }

  delete iter;
}
*/


TEST_F(DBIteratorTest, 1011_IteratorWithLocalStatistics) {
  Open();
  Options options = CurrentOptions();
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 1000; i++) {
    // Key 10 bytes / Value 10 bytes
    ASSERT_OK(Put(RandomString(&rnd, 10), RandomString(&rnd, 10)));
  }

  std::atomic<uint64_t> total_next(0);
  std::atomic<uint64_t> total_next_found(0);
  std::atomic<uint64_t> total_prev(0);
  std::atomic<uint64_t> total_prev_found(0);
  std::atomic<uint64_t> total_bytes(0);

  std::vector<port::Thread> threads;
  std::function<void()> reader_func_next = [&]() {
    Iterator* iter = db_->NewIterator(ReadOptions());

    iter->SeekToFirst();
    // Seek will bump ITER_BYTES_READ
    total_bytes += iter->key().size();
    total_bytes += iter->value().size();
    while (true) {
      iter->Next();
      total_next++;

      if (!iter->Valid()) {
        break;
      }
      total_next_found++;
      total_bytes += iter->key().size();
      total_bytes += iter->value().size();
    }

    delete iter;
  };

  std::function<void()> reader_func_prev = [&]() {
    Iterator* iter = db_->NewIterator(ReadOptions());

    iter->SeekToLast();
    // Seek will bump ITER_BYTES_READ
    total_bytes += iter->key().size();
    total_bytes += iter->value().size();
    while (true) {
      iter->Prev();
      total_prev++;

      if (!iter->Valid()) {
        break;
      }
      total_prev_found++;
      total_bytes += iter->key().size();
      total_bytes += iter->value().size();
    }

    delete iter;
  };

  for (int i = 0; i < 10; i++) {
    threads.emplace_back(reader_func_next);
  }
  for (int i = 0; i < 15; i++) {
    threads.emplace_back(reader_func_prev);
  }

  for (auto& t : threads) {
    t.join();
  }

  //ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_NEXT), (uint64_t)total_next);
  //ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_NEXT_FOUND),
  //          (uint64_t)total_next_found);
  //ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_PREV), (uint64_t)total_prev);
  //ASSERT_EQ(TestGetTickerCount(options, NUMBER_DB_PREV_FOUND),
  //          (uint64_t)total_prev_found);
  //ASSERT_EQ(TestGetTickerCount(options, ITER_BYTES_READ), (uint64_t)total_bytes);

}

//
//
////MoveFilesToLevel(2) cannot be compiled
TEST_F(DBIteratorTest, 1012_ReadAhead) {
  Open();
  Options options = CurrentOptions();
  //env_->count_random_reads_ = true;  //编译失败
  options.env = env_;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 4 << 20;
  //options.statistics = rocksdb::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  table_options.no_block_cache = true;
  //options.table_factory.reset(new BlockBasedTableFactory(table_options));
  db_options_ = options;
  //Reopen();
  DestroyAndReopen(options);

  std::string value(1024, 'a');
  for (int i = 0; i < 100; i++) {
    Put(Key(i), value);
  }
  ASSERT_OK(Flush());      //ASSERT_OK(Flush());will core dump 
//  MoveFilesToLevel(2);
  cout << "---------flush 1-----------" << endl;
  for (int i = 0; i < 100; i++) {
    Put(Key(i), value);
  }
  ASSERT_OK(Flush());
//  MoveFilesToLevel(1);
  cout << "---------flush 2-----------" << endl;
  for (int i = 0; i < 100; i++) {
    Put(Key(i), value);
  }
  cout << "---------PUT 100-----------" << endl;
  ASSERT_OK(Flush());
#ifndef ROCKSDB_LITE
//  ASSERT_EQ("1,1,1", FilesPerLevel());
#endif  // !ROCKSDB_LITE
//  cout << "---------flush 3-----------" << endl;
//  env_->random_read_bytes_counter_ = 0;
//  cout << "---------set ticker-----------" << endl;
//  options.statistics->setTickerCount(NO_FILE_OPENS, 0);  //core dump
//  cout << "---------set ticker-----------" << endl;
    ReadOptions read_options;
//  auto* iter = db_->NewIterator(read_options);
//  iter->SeekToFirst();
//  cout << "---------get ticker-----------" << endl;
//  int64_t num_file_opens = TestGetTickerCount(options, NO_FILE_OPENS);
//  cout << "---------get ticker-----------" << endl;
//  size_t bytes_read = env_->random_read_bytes_counter_;
//  delete iter;
//  cout << "---------NewIterator-----------" << endl;
//  env_->random_read_bytes_counter_ = 0;
//  options.statistics->setTickerCount(NO_FILE_OPENS, 0);
//  read_options.readahead_size = 1024 * 10;
//  iter = db_->NewIterator(read_options);
//  iter->SeekToFirst();
//  int64_t num_file_opens_readahead = TestGetTickerCount(options, NO_FILE_OPENS);
//  size_t bytes_read_readahead = env_->random_read_bytes_counter_;
//  delete iter;
// // ASSERT_EQ(num_file_opens + 3, num_file_opens_readahead);
//  //ASSERT_GT(bytes_read_readahead, bytes_read);
//  //ASSERT_GT(bytes_read_readahead, read_options.readahead_size * 3);
//  cout << "---------NewIterator 2-----------" << endl;
//  // Verify correctness.
  auto* iter = db_->NewIterator(read_options);
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(value, iter->value());
    count++;
  }
  ASSERT_EQ(100, count);
  for (int i = 0; i < 100; i++) {
    iter->Seek(Key(i));
    ASSERT_EQ(value, iter->value());
  }
  delete iter;
}

//
//// Insert a key, create a snapshot iterator, overwrite key lots of times,
//// seek to a smaller key. Expect DBIter to fall back to a seek instead of
//// going through all the overwrites linearly.
TEST_F(DBIteratorTest, 1013_DBIteratorSkipRecentDuplicatesTest) {
  Open();
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_sequential_skip_in_iterations = 3;
  options.prefix_extractor = nullptr;
  options.write_buffer_size = 1 << 27;  // big enough to avoid flush
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(options);

  // Insert.
  ASSERT_OK(Put("b", "0"));

  // Create iterator.
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));

  // Insert a lot.
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("b", std::to_string(i + 1).c_str()));
  }
 cout<<"Data put"<<endl;
#ifndef ROCKSDB_LITE
  // Check that memtable wasn't flushed.
//  std::string val;
//  ASSERT_TRUE(db_->GetProperty("rocksdb.num-files-at-level0", &val));
//  EXPECT_EQ("0", val);
#endif

  // Seek iterator to a smaller key.
  get_perf_context()->Reset();
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("b", iter->key().ToString());
  EXPECT_EQ("0", iter->value().ToString());
 cout<<"Data seek"<<endl;
  // Check that the seek didn't do too much work.
  // Checks are not tight, just make sure that everything is well below 100.
  EXPECT_LT(get_perf_context()->internal_key_skipped_count, 4);
  //EXPECT_LT(get_perf_context()->internal_recent_skipped_count, 8);
  //EXPECT_LT(get_perf_context()->seek_on_memtable_count, 10);
  //EXPECT_LT(get_perf_context()->next_on_memtable_count, 10);
  //EXPECT_LT(get_perf_context()->prev_on_memtable_count, 10);
 cout<<"Check that the seek didn't do too much work"<<endl;

  // Check that iterator did something like what we expect.
 // EXPECT_EQ(get_perf_context()->internal_delete_skipped_count, 0);
 // EXPECT_EQ(get_perf_context()->internal_merge_count, 0);
  //EXPECT_GE(get_perf_context()->internal_recent_skipped_count, 2);
  //EXPECT_GE(get_perf_context()->seek_on_memtable_count, 2);
  //EXPECT_EQ(1, options.statistics->getTickerCount(
  //               NUMBER_OF_RESEEKS_IN_ITERATION)); //lining.dou
//Close();
}

TEST_F(DBIteratorTest, 1014_NewIteratorGreater_CF){
  Open();
  //CreateColumnFamiliesAndReopen({"one"});

  std::vector<std::string> cf_names;
  cf_names.push_back("pikachu");
  CreateColumnFamilies(cf_names);
  cf_names.push_back("default");
  //delete db_;
  Status s = TryOpen(cf_names); 
  // ClearEnv(db_);
  int k = 16;
  int v = 64;
  int num = 3;
  InsertDB(db_,k, v, num);  //插入长度为16个1，16个2，16个3的三个key， 64个1，64个2，64个3的三个value
  int tar_len = 16;
  std::string target(tar_len, '3');
  int cnt = CntNewIterator(db_, target); 
  ClearEnv(db_);
  ASSERT_EQ(0, cnt);
  Close(); 
}

TEST_F(DBIteratorTest, 1015_NewIteratorGreater){
  Open();
  //CreateColumnFamiliesAndReopen({"one"});
  int k = 16;
  int v = 64;
  int num = 3;
  InsertDB(db_,k, v, num);   //插入长度为16个0，16个1，16个2的三个key， 64个1，64个2，64个0的三个value
  int tar_len = 16;
  std::string target(tar_len, '3');
  int cnt = CntNewIterator(db_, target);  //查找到target之后，执行next并且计数，直到invalid key
  ClearEnv(db_);
  ASSERT_EQ(0, cnt);  //16个3的key是最大值，所以下一个就是invalid，cnt=0
  Close();
}


TEST_F(DBIteratorTest, 1016_NewIteratorEqual){
  Open();
 // CreateAndReopenWithCF({"one"});
  //CreateColumnFamiliesAndReopen({"one"});
  int k = 16;
  int v = 64;
  int num = 2;
  InsertDB(db_,k, v, num); 
  int tar_len = 16;
  std::string target(tar_len, '1');
  int cnt = CntNewIterator(db_, target);
  ClearEnv(db_);
  ASSERT_EQ(1, cnt);
  Close();
}

TEST_F(DBIteratorTest, 1017_NewIteratorGreaterEqual){
  Open();
  //CreateAndReopenWithCF({"one"});
  //CreateColumnFamiliesAndReopen({"one"});
  int k = 16;
  int v = 64;
  int num = 3;
  InsertDB(db_,k, v, num);
  int tar_len = 16;
  std::string target(tar_len, '1');
  int cnt = CntNewIterator(db_, target);
  ClearEnv(db_);
  ASSERT_EQ(2, cnt);
  Close();
}

TEST_F(DBIteratorTest, 1018_NewIteratorCom){
  Open();
  //CreateAndReopenWithCF({"one"});
  //CreateColumnFamiliesAndReopen({"one"});
  int v = 64;
  std::string testkey = "ABCaaaaaaaaaaaaa";
  std::string testvalue(v, 'a');
  std::string testkey1 = "ACBbbbbbbbbbbbbb";
  std::string testkey2 = "CBAbbbbbbbbbbbbbbb";
  std::string targetKEY = "ACB";

  ASSERT_OK(db_->Put(WriteOptions(), testkey, testvalue));
  ASSERT_OK(db_->Put(WriteOptions(), testkey1, testvalue));
  ASSERT_OK(db_->Put(WriteOptions(), testkey2, testvalue));
  int cnt = CntNewIterator(db_, targetKEY);
  ClearEnv(db_);
  ASSERT_EQ(2, cnt);
  Close();
}

TEST_F(DBIteratorTest, 1019_NewIteratorMaxKey){
  Open();
  //CreateAndReopenWithCF({"one"});
  //CreateColumnFamiliesAndReopen({"one"});
  int k = 255;
  int v = 64;
  int num = 3;
  InsertDB(db_,k, v, num);
  int tar_len = 16;
  std::string target(tar_len, '1');
  int cnt = CntNewIterator(db_, target);
  ClearEnv(db_);
  ASSERT_EQ(2, cnt);
  Close();
}

TEST_F(DBIteratorTest, 1020_NewIteratorMaxTarget){
  Open();
  //CreateAndReopenWithCF({"one"});
  //CreateColumnFamiliesAndReopen({"one"});
  int k = 16;
  int v = 64;
  int num = 3;
  InsertDB(db_,k, v, num);
  int tar_len = 256;
  std::string target(tar_len, '1');
  int cnt = CntNewIterator(db_, target);
  ClearEnv(db_);
  ASSERT_EQ(1, cnt);
  Close();
}

TEST_F(DBIteratorTest, 1022_NewIteratorNullTarget){
  Open();
  //CreateAndReopenWithCF({"one"});
  //CreateColumnFamiliesAndReopen({"one"});
  int k = 16;
  int v = 64;
  int num = 3;
  InsertDB(db_,k, v, num);
  int tar_len = 0;
  std::string target(tar_len, '1');
  int cnt = CntNewIterator(db_, target);
  ClearEnv(db_);
  ASSERT_EQ(3, cnt);
  Close();
}

TEST_F(DBIteratorTest, 1023_NewIteratorMutiSeek){
  Open();
  //CreateAndReopenWithCF({"one"});
  //CreateColumnFamiliesAndReopen({"one"});
  int k = 16;
  int v = 64;
  int num = 3;
  InsertDB(db_,k, v, num);
  int tar_len = 16;
  int res = 3;
  std::string t1(tar_len, '0');
  std::string t2(tar_len, '1');
  std::string t3(tar_len, '2');
  ASSERT_EQ(res, CntNewIterator(db_, t1));
  ASSERT_EQ((res-1), CntNewIterator(db_, t2));
  ASSERT_EQ((res-2), CntNewIterator(db_, t3));
  ClearEnv(db_);
  //ASSERT_EQ(3, cnt);
  Close();
}

TEST_F(DBIteratorTest, 1024_NewIteratorSpecTarget){
  Open();
  //CreateAndReopenWithCF({"one"});
  //CreateColumnFamiliesAndReopen({"one"});
  int k = 16;
  int v = 64;
  int num = 3;
  InsertDB(db_,k, v, num);
  int tar_len = 16;
  std::string target(tar_len, '!');
  int cnt = CntNewIterator(db_, target);
  ClearEnv(db_);
  ASSERT_EQ(3, cnt);
  Close();
}

TEST_F(DBIteratorTest, 1026_NewIteratorIn1000KV){
  Open();
  int k = 16;
  int v = 64;
  int num = 1000;
  int res = 1000;
  int tar_len = 16;
  InsertRandom(db_, k, v, num);
  std::string t1= SetRandString(tar_len);
  std::string t2= SetRandString(tar_len);
  std::string t3(tar_len, ' ');
  CntNewIterator(db_, t1);
  CntNewIterator(db_, t2);
  int cnt = CntNewIterator(db_, t3);
  //sleep(10);
  ASSERT_EQ(res, cnt);
  ClearEnv(db_);
  Close();
}

TEST_F(DBIteratorTest, 1028_NewIteratorIn10000KV){
  Open();
  int k = 16;
  int v = 64;
  int num = 10000;
  int res = 10000;
  int tar_len = 16;
  InsertRandom(db_, k, v, num);
  std::string t1= SetRandString(tar_len);
  std::string t2= SetRandString(tar_len);
  std::string t3(tar_len, ' ');
  CntNewIterator(db_, t1);
  CntNewIterator(db_, t2);
  int cnt = CntNewIterator(db_, t3);
  //sleep(10);
  ASSERT_EQ(res, cnt);
  ClearEnv(db_);
  Close();
}

  TEST_F(DBIteratorTest, 1029_NewIteratorIn100000KV){
  Open();
  int k = 16;
  int v = 64;
  int num = 100000;
  int res = 100000;
  int tar_len = 16;
  cout <<"before insert"<<endl;
  InsertRandom(db_, k, v, num);
  cout <<"after insert"<<endl;
  std::string t1= SetRandString(tar_len);
  std::string t2= SetRandString(tar_len);
  std::string t3(tar_len, ' ');
  cout <<"before iter1"<<endl;
  CntNewIterator(db_, t1);
  cout <<"before iter2"<<endl;
  CntNewIterator(db_, t2);
  cout <<"before iter3"<<endl;
  int cnt = CntNewIterator(db_, t3);
  //sleep(10);
  ASSERT_EQ(res, cnt);
  cout <<"after assert cnt"<<endl;
  ClearEnv(db_);
  cout <<"after clear"<<endl;
  Close();
}


//没有实现


}

}

