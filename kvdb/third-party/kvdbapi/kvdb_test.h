/*#
# Copyright (c) Software R&D Center, Samsung Electronics
# File    :     kvdb_test.h
# Description : This file is for kvdb UT test,including some functions for others feature
# Team    :     SRCX_SEG_MSG_SQA
# Author  :     ll66.liu
# History :     2019-07-25  - ll66.liu - Created
#               2019-08-02  - ll66.liu - Add random number in constructor
# Version :     1.00
#*/

#include "CommonType.h"
#include "Common.h"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>
#include <fcntl.h>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "memtable/hash_linklist_rep.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "table/block_based_table_factory.h"
#include "table/plain_table_factory.h"
#include "table/scoped_arena_iterator.h"
#include "util/compression.h"
#include "util/filename.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

namespace rocksdb {
static const int kValueSize = 1000;

class KVDBTest: public testing::Test {
public:
     KVDBTest():rnd_(139){}

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
  Env* env_ ;
  DB* db_;
  Random rnd_;
  
  std::vector<ColumnFamilyHandle*> handles_;
  std::vector<std::string> names_;
  std::set<std::string> keys_;
  ColumnFamilyOptions column_family_options_;
  std::vector<ColumnFamilyDescriptor> column_families;
  

  int option_config_;
  DBOptions db_options_;
  Options last_options_;
  Options options;
  
Options* GetOptions() { return &last_options_; }

static std::string RandomString(Random* rnd, int len) {
    std::string r;
    test::RandomString(rnd, len, &r);
    return r;
  }

static std::string Key(int i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "key%07d", i);
    return std::string(buf);
  }
Slice Value(int k, std::string* storage) {
    if (k == 0) {
      *storage = std::string(kValueSize, ' ');
      return Slice(*storage);
    } else {
      Random r(k);
      return test::RandomString(&r, kValueSize, storage);
    }
  }
void PutRandomData(int cf, int num, int key_value_size, bool save = false) {
    for (int i = 0; i < num; ++i) {
      // 10 bytes for key, rest is value
      if (!save) {
        ASSERT_OK(Put(cf, test::RandomKey(&rnd_, 11),
                      RandomString(&rnd_, key_value_size - 10)));
      } else {
        std::string key = test::RandomKey(&rnd_, 11);
        keys_.insert(key);
        ASSERT_OK(Put(cf, key, RandomString(&rnd_, key_value_size - 10)));
      }
    }
  }

void Open(std::vector<std::string> cf,
            std::vector<ColumnFamilyOptions> options = {}) {
    ASSERT_OK(TryOpen(cf, options));
  }

void Open() {
    Open({"default"});
  }

Status Open(const Options& options) {
    return DB::Open(options, dbname_, &db_);
  }

Status TryReopen() {
    delete db_;
    db_ = nullptr;
    last_options_.create_if_missing = true;
    return DB::Open(last_options_, dbname_, &db_);
}
Status TryOpen(std::vector<std::string> cf,
                 std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<ColumnFamilyDescriptor> column_families;
    names_.clear();
    for (size_t i = 0; i < cf.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(
          cf[i], options.size() == 0 ? column_family_options_ : options[i]));
      names_.push_back(cf[i]);
    }
    db_options_.create_if_missing = true;
    return DB::Open(db_options_, dbname_, column_families, &handles_, &db_);
  }
Status TryReopen(const Options& options) {
    Close();
    last_options_.table_factory.reset();
    last_options_ = options;
    return DB::Open(options, dbname_, &db_);
}

void Reopen(const Options& options) {
  ASSERT_OK(TryReopen(options));
}
void Reopen(const std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<std::string> names;
    for (auto name : names_) {
      if (name != "") {
        names.push_back(name);
      }
    }
    Close();
    assert(options.size() == 0 || names.size() == options.size());
    Open(names, options);
  }

/*
void Close() {
    for (auto h : handles_) {
    //  if (h) {
    if (h && db_){ 
       db_->DestroyColumnFamilyHandle(h);
      }
    }
    handles_.clear();
    names_.clear();
    if(db_)
       delete db_;
    db_ = nullptr;
  }
*/


void Close() {
 //   for (auto h : handles_) {
 //   //  if (h) {
 //   if (h && db_){ 
 //      db_->DestroyColumnFamilyHandle(h);
 //     }
 //   }
 //   handles_.clear();
 //   names_.clear();
    if(db_)
       delete db_;
    db_ = nullptr;
  }


void Destroy() {
    Close();
    ASSERT_OK(DestroyDB(dbname_, Options(db_options_, column_family_options_)));
  }

void Destroy(const Options& options) {
    ASSERT_OK(DestroyDB(dbname_, options));
}

void DestroyAndReopen() {
    //Destroy using last options
   Destroy();
   ASSERT_OK(TryReopen());
  }

void DestroyAndReopen(const Options& options) {
    // Destroy using last options
    Destroy(last_options_);
    last_options_.create_if_missing = true;
    ASSERT_OK(TryReopen(options));
 }

void CreateColumnFamilies(const std::vector<std::string>& cfs,
                                      const Options& options) {
    ColumnFamilyOptions cf_opts(options);
    size_t cfi = handles_.size();
//    cout<<cfi<<endl;
    handles_.resize(cfi + cfs.size());
//    cout<<handles_.size()<<endl;
    for (auto cf : cfs) {
        ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
  }
}
void CreateColumnFamilies(
      const std::vector<std::string>& cfs,
      const std::vector<ColumnFamilyOptions> options = {}) {
    int cfi = static_cast<int>(handles_.size());
    handles_.resize(cfi + cfs.size());
    names_.resize(cfi + cfs.size());
    for (size_t i = 0; i < cfs.size(); ++i) {
        const auto& current_cf_opt =
          options.size() == 0 ? column_family_options_ : options[i];
        ASSERT_OK(
          db_->CreateColumnFamily(current_cf_opt, cfs[i], &handles_[cfi]));
      //  cout << "The cf name is :"<< cfs[i] <<endl;
        names_[cfi] = cfs[i];
        cfi++;
    }
  }

// Status ReadOnlyReopen(const Options& options) {
  // return DB::OpenForReadOnly(options, dbname_, &db_);
// }



void CreateColumnFamiliesAndReopen(const std::vector<std::string>& cfs) {
    CreateColumnFamilies(cfs);
    Reopen();
  }

void DropColumnFamilies(const std::vector<int>& cfs) {
    for (auto cf : cfs) {
      ASSERT_OK(db_->DropColumnFamily(handles_[cf]));
      db_->DestroyColumnFamilyHandle(handles_[cf]);
      handles_[cf] = nullptr;
      names_[cf] = "";
    }
  }

Status Put(const Slice& k, const Slice& v, WriteOptions wo) {
  if (kMergePut == option_config_) {
    return db_->Merge(wo, k, v);
  } else {
    return db_->Put(wo, k, v);
  }
}

Status Put(int cf, const Slice& k, const Slice& v,
                       WriteOptions wo) {
  if (kMergePut == option_config_) {
    return db_->Merge(wo, handles_[cf], k, v);
  } else {
    return db_->Put(wo, handles_[cf], k, v);
  }
}

Status Put(int cf, const std::string& key, const std::string& value) {
    return db_->Put(WriteOptions(), handles_[cf], Slice(key), Slice(value));
  }
  Status Put(const std::string& key, const std::string& value) {
    return db_->Put(WriteOptions(), Slice(key), Slice(value));
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
Status Merge(int cf, const std::string& key, const std::string& value) {
    return db_->Merge(WriteOptions(), handles_[cf], Slice(key), Slice(value));
  }

Status Delete(int cf, const std::string& key) {
    return db_->Delete(WriteOptions(), handles_[cf], Slice(key));
  }
  
Status Delete(const std::string& key) {
    return db_->Delete(WriteOptions(), Slice(key));
  }

Status SingleDelete(int cf, const std::string& k) {
   return db_->SingleDelete(WriteOptions(), handles_[cf], k);
 }
 
Status SingleDelete(const std::string& k) {
   return db_->SingleDelete(WriteOptions(), k);
 }

Status Flush(int cf=0) {
  if (cf == 0) {
    return db_->Flush(FlushOptions());
  } else {
    return db_->Flush(FlushOptions(), handles_[cf]);
  }
}
};
}
