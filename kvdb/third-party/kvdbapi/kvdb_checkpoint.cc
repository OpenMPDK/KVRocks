/**
 * @ Hainan Liu (hainan.liu@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file kvdb_snapshot.cc
  *  \brief Test cases code of the KVDB Snapshot().
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////

#ifndef ROCKSDB_LITE

//#include <iostream>
//#include <thread>
//#include <utility>
//#include <assert.h>
//#include "gtest/gtest.h"
////#include "db/db_impl.h"
//#include "../../kvdb/port/stack_trace.h"
//#include "../../kvdb/port/port.h"
//#include "../../kvdb/port/port_posix.h"
//#include "utilities/checkpoint.h"
//#include "db.h"
//#include "env.h"
//#include "Common.h"
//#include "utilities/transaction_db.h"
//#include "sync_point.h"
//#include "../../kvdb/kvdb/util/testharness.h" //not find this file


//for cftest
//#include "util/sync_point.h"
//#include "util/string_util.h"
//#include "db/db_impl.h"
//#include "util/testutil.h"
#include "kvdb_test.h"

using namespace std;



namespace rocksdb {

const Snapshot* ManagedSnapshot::snapshot() {
	return snapshot_;
}

ManagedSnapshot::ManagedSnapshot(DB* db) : db_(db),
	snapshot_(db->GetSnapshot()) {}
ManagedSnapshot::~ManagedSnapshot() {
	db_->ReleaseSnapshot(snapshot_);
}


namespace cftest_cp {

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

 private:
  std::atomic<int> num_new_writable_file_;
};

class CFTest_CP : public KVDBTest {
 public:
  CFTest_CP() {
    env_ = new EnvCounter(Env::Default());
//    env_->SetBackgroundThreads(1, Env::LOW);
//    env_->SetBackgroundThreads(1, Env::HIGH);
    dbname_ = test::TmpDir() + "/checkpoint_test";
	//alternative_wal_dir_ = dbname_ + "/wal";
    //db_options_.create_if_missing = true;
    //db_options_.fail_if_options_file_error = true;
    //db_options_.env = env_;
    //insdb::Options insdb_options;  //hainan.liu
    //insdb_options.num_column_count = 5;
    //db_options_.num_cols = 8;
    
    Options options ;
    options.create_if_missing = true;
    options.num_cols = 8;
    //db_options = options;
    //auto delete_options = options;
    //EXPECT_OK(DestroyDB(dbname_, delete_options));
    //EXPECT_OK(DestroyDB(dbname_, options));
    db_ = nullptr;
    Reopen(options);
    //DestroyDB(dbname_, Options(db_options_, column_family_options_));
  }


  ~CFTest_CP() {
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->LoadDependency({});
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
//    Close();
    Options options;
//    options.db_paths.emplace_back(dbname_, 0);
//    options.db_paths.emplace_back(dbname_ + "_2", 0);
//    options.db_paths.emplace_back(dbname_ + "_3", 0);
//    options.db_paths.emplace_back(dbname_ + "_4", 0);
//    EXPECT_OK(DestroyDB(dbname_, options));
    options.env = env_;
//    Close();
//    if (getenv("KEEP_DB")) {
//        printf("DB is still at %s\n", dbname_.c_str());
//    } else {
//        EXPECT_OK(DestroyDB(dbname_, options));
//    // Destroy();
//    }  
    Destroy();
    delete env_;
  }

//lining.dou

};

TEST_F(CFTest_CP, OpenReadOnly) {

  return false;
}
TEST_F(CFTest_CP, ExistedPath) {
  //Open();
//  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    const std::string snapshot_name = test::TmpDir(env_) + "/cp_snapshot";
    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    options ;
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    //ASSERT_OK(DestroyDB(snapshot_name, options));
    env_->DeleteDir(snapshot_name);

    env_->CreateDir(snapshot_name);
    

    // Create a database
    Status s;
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    cout << "checkpoint to be created" <<endl;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_NOK(checkpoint->CreateCheckpoint(snapshot_name));

    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));

// Restore DB name
    dbname_ = test::TmpDir(env_) + "/checkpoint_test";
    env_->DeleteDir(snapshot_name);
}

TEST_F(CFTest_CP, RelativePath) {
  //Open();
//  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    const std::string snapshot_name = "../cp_snapshot";
    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    //ASSERT_OK(DestroyDB(snapshot_name, options));
    env_->DeleteDir(snapshot_name);

// Create a database
    Status s;
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    cout << "checkpoint to be created" <<endl;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_NOK(checkpoint->CreateCheckpoint(snapshot_name));

    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));

// Restore DB name
    dbname_ = test::TmpDir(env_) + "/checkpoint_test";
    env_->DeleteDir(snapshot_name);
}

TEST_F(CFTest_CP, EmptyPath) {
  //Open();
//  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    const std::string snapshot_name = "";
    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    //options = CurrentOptions();
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    //ASSERT_OK(DestroyDB(snapshot_name, options));
//    env_->DeleteDir(snapshot_name);

// Create a database
    Status s;
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    cout << "checkpoint to be created" <<endl;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_NOK(checkpoint->CreateCheckpoint(snapshot_name));

    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));

// Restore DB name
    dbname_ = test::TmpDir(env_) + "/checkpoint_test";
//    env_->DeleteDir(snapshot_name);
}

TEST_F(CFTest_CP, DeletePathThenGet) {
  //Open();
//  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    const std::string snapshot_name = test::TmpDir(env_) + "/cp_snapshot";
    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    //options = CurrentOptions();
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    //ASSERT_OK(DestroyDB(snapshot_name, options));
//    env_->DeleteDir(snapshot_name);

// Create a database

    Status s;
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    cout << "checkpoint to be created" <<endl;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name));
    cout << "checkpoint created" <<endl;
    ASSERT_OK(Put(key, "v2"));
    ASSERT_EQ("v2", Get(key));
    ASSERT_OK(Flush());
    ASSERT_EQ("v2", Get(key));

    env_->DeleteDir(snapshot_name);

    // Open snapshot and verify contents while DB is running
    options.create_if_missing = false;
    ASSERT_OK(DB::Open(options, snapshot_name, &snapshotDB));
    ASSERT_OK(snapshotDB->Get(roptions, key, &result));
    ASSERT_EQ(result, "NOT_FOUND");
    delete snapshotDB;
    snapshotDB = nullptr;
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    delete checkpoint;

// Restore DB name
    dbname_ = test::TmpDir(env_) + "/checkpoint_test";
//    env_->DeleteDir(snapshot_name);
}

  // log_size_for_flush: if the total log file size is equal or larger than
  // this value, then a flush is triggered for all the column families. The
  // default value is 0, which means flush is always triggered. If you move
  // away from the default, the checkpoint may not contain up-to-date data
  // if WAL writing is not always enabled.
TEST_F(CFTest_CP, GetSnapshotLink) {
  //Open();
  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    const std::string snapshot_name = test::TmpDir(env_) + "/cp_snapshot";
    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    //options = CurrentOptions();
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    //ASSERT_OK(DestroyDB(snapshot_name, options));
    env_->DeleteDir(snapshot_name);

    // Create a database
    Status s;
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    cout << "checkpoint to be created" <<endl;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name, log_size_for_flush));
    cout << "checkpoint created" <<endl;
    ASSERT_OK(Put(key, "v2"));
    ASSERT_EQ("v2", Get(key));
    ASSERT_OK(Flush());
    ASSERT_EQ("v2", Get(key));
    // Open snapshot and verify contents while DB is running
    options.create_if_missing = false;
    ASSERT_OK(DB::Open(options, snapshot_name, &snapshotDB));
    ASSERT_OK(snapshotDB->Get(roptions, key, &result));
    ASSERT_EQ("v1", result);
    delete snapshotDB;
    snapshotDB = nullptr;
    delete db_;
    db_ = nullptr;

    // Destroy original DB
    ASSERT_OK(DestroyDB(dbname_, options));

    // Open snapshot and verify contents
    options.create_if_missing = false;
    dbname_ = snapshot_name;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_EQ("v1", Get(key));
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    delete checkpoint;

    // Restore DB name
    dbname_ = test::TmpDir(env_) + "/checkpoint_test";
  }
}

//if write a checkpoint , the original data should be ?
TEST_F(CFTest_CP, CheckpointWrite) {
  //Open();
  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    const std::string snapshot_name = test::TmpDir(env_) + "/cp_snapshot";
    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    //options = CurrentOptions();
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    //ASSERT_OK(DestroyDB(snapshot_name, options));
    env_->DeleteDir(snapshot_name);

    // Create a database
    Status s;
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    cout << "checkpoint to be created" <<endl;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name, log_size_for_flush));
    cout << "checkpoint created" <<endl;
    ASSERT_OK(Put(key, "v2"));
    ASSERT_EQ("v2", Get(key));
    ASSERT_OK(Flush());
    ASSERT_EQ("v2", Get(key));
    // Open snapshot and verify contents while DB is running
    options.create_if_missing = false;
    ASSERT_OK(DB::Open(options, snapshot_name, &snapshotDB));
    ASSERT_OK(snapshotDB->Get(roptions, key, &result));
    ASSERT_EQ("v1", result);
    ASSERT_OK(snapshotDB->Put(WriteOptions(), key, "v11"));
    delete snapshotDB;
    snapshotDB = nullptr;
    delete db_;
    db_ = nullptr;
    // Destroy original DB
    ASSERT_OK(DestroyDB(dbname_, options));

    // Open snapshot and original DB to verify contents
    options.create_if_missing = false;
    dbname_ = snapshot_name;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_EQ("v11", Get(key));
    delete db_;
    db_ = nullptr;
    ASSERT_OK(DestroyDB(dbname_, options));
    delete checkpoint;

    // Restore DB name
    dbname_ = test::TmpDir(env_) + "/checkpoint_test";
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_EQ("v11", Get(key)); 
    
  }
}


/*
TEST_F(CFTest_CP, CheckpointCF) {
  Options options = CurrentOptions();
  //CreateAndReopenWithCF({"one", "two", "three", "four", "five"}, options);
  CreateColumnFamilies({"one","two", "three", "four", "five"});
  delete db_;
  options.create_if_missing = true;
  options.num_cols = 8;
  db_options_=options;
  Status s = TryOpen({"default","one","two", "three", "four", "five"});
  cout << s.ToString() <<endl;
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointTest::CheckpointCF:2", "DBImpl::GetLiveFiles:2"},
       {"DBImpl::GetLiveFiles:1", "CheckpointTest::CheckpointCF:1"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  ASSERT_OK(Put(2, "two", "two"));
  ASSERT_OK(Put(3, "three", "three"));
  ASSERT_OK(Put(4, "four", "four"));
  ASSERT_OK(Put(5, "five", "five"));

  const std::string snapshot_name = test::TmpDir(env_) + "/snapshot";
  DB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;

  //ASSERT_OK(DestroyDB(snapshot_name, options));
  env_->DeleteDir(snapshot_name);

  //Status s;
  // Take a snapshot
  rocksdb::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name));
    delete checkpoint;
  });
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:1");
  ASSERT_OK(Put(0, "Default", "Default1"));
  ASSERT_OK(Put(1, "one", "eleven"));
  ASSERT_OK(Put(2, "two", "twelve"));
  ASSERT_OK(Put(3, "three", "thirteen"));
  ASSERT_OK(Put(4, "four", "fourteen"));
  ASSERT_OK(Put(5, "five", "fifteen"));
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:2");
  t.join();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(Put(1, "one", "twentyone"));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  ASSERT_OK(Put(3, "three", "twentythree"));
  ASSERT_OK(Put(4, "four", "twentyfour"));
  ASSERT_OK(Put(5, "five", "twentyfive"));
  ASSERT_OK(Flush());

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs=  {kDefaultColumnFamilyName, "one", "two", "three", "four", "five"};
  std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(cfs[i], options));
    }
  ASSERT_OK(DB::Open(options, snapshot_name,
        column_families, &cphandles, &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default1", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("eleven", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  for (auto h : cphandles) {
      delete h;
  }
  cphandles.clear();
  delete snapshotDB;
  snapshotDB = nullptr;
  ASSERT_OK(DestroyDB(snapshot_name, options));
}
*/
TEST_F(CFTest_CP, CheckpointCFNoFlush) {
  Options options;
  //CreateAndReopenWithCF({"one", "two", "three", "four", "five"}, options);
  CreateColumnFamilies({"one","two", "three", "four", "five"});
  delete db_;
  options.create_if_missing = true;
  options.num_cols = 8;
  db_options_=options;
  Status s = TryOpen({"default","one","two", "three", "four", "five"});
  cout << s.ToString() <<endl;
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  Flush();
  ASSERT_OK(Put(2, "two", "two"));

  const std::string snapshot_name = test::TmpDir(env_) + "/snapshot";
  DB* snapshotDB;
  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;

  //ASSERT_OK(DestroyDB(snapshot_name, options));
  env_->DeleteDir(snapshot_name);

  //`Status s;
  // Take a snapshot
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void* arg) {
        // Flush should never trigger.
        ASSERT_TRUE(false);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name, 1000000));
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  delete checkpoint;
  ASSERT_OK(Put(1, "one", "two"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  Close();
  EXPECT_OK(DestroyDB(dbname_, options));

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs = {kDefaultColumnFamilyName, "one", "two", "three", "four", "five"};
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.push_back(ColumnFamilyDescriptor(cfs[i], options));
  }
  ASSERT_OK(DB::Open(options, snapshot_name, column_families, &cphandles,
                     &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("one", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  ASSERT_EQ("two", result);
  for (auto h : cphandles) {
    delete h;
  }
  cphandles.clear();
  delete snapshotDB;
  snapshotDB = nullptr;
  ASSERT_OK(DestroyDB(snapshot_name, options));
}



}//namespace cftest




}  // namespace rocksdb


#endif  // !ROCKSDB_LITE

