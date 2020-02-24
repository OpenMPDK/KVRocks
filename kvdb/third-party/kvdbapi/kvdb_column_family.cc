//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "kvdb_test.h"

using namespace std;

namespace rocksdb {

namespace {
std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}
}  // anonymous namespace
namespace cftest_cf {
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

class ColumnFamilyTest : public KVDBTest {
 public:
  ColumnFamilyTest() {
    env_ = new EnvCounter(Env::Default());
    dbname_ = test::TmpDir() + "/column_family_test";
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
    db_options_.env = env_;
    db_options_.num_cols = 8;
    //insdb_options.num_column_count = 256; 
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
  }

  ~ColumnFamilyTest() {
  if(db_)
   { Destroy();}
  delete env_;  
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


//
//void PutRandomData(int cf, int num, int key_value_size, bool save = false) {
//    for (int i = 0; i < num; ++i) {
//      // 10 bytes for key, rest is value
//      if (!save) {
//        ASSERT_OK(Put(cf, test::RandomKey(&rnd_, 11),
//                      RandomString(&rnd_, key_value_size - 10)));
//      } else {
//        std::string key = test::RandomKey(&rnd_, 11);
//        keys_.insert(key);
//        ASSERT_OK(Put(cf, key, RandomString(&rnd_, key_value_size - 10)));
//      }
//    }
//  }
//
  void CheckMissed() {
    uint64_t next_expected = 0;
    uint64_t missed = 0;
    int bad_keys = 0;
    int bad_values = 0;
    int correct = 0;
    std::string value_space;
    for (int cf = 0; cf < 3; cf++) {
      next_expected = 0;
      Iterator* iter = db_->NewIterator(ReadOptions(false, true), handles_[cf]);
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        uint64_t key;
        Slice in(iter->key());
        in.remove_prefix(3);
        if (!ConsumeDecimalNumber(&in, &key) || !in.empty() ||
            key < next_expected) {
          bad_keys++;
          continue;
        }
        missed += (key - next_expected);
        next_expected = key + 1;
        if (iter->value() != Value(static_cast<int>(key), &value_space)) {
          bad_values++;
        } else {
          correct++;
        }
      }
      delete iter;
    }

    ASSERT_EQ(0, bad_keys);
    ASSERT_EQ(0, bad_values);
    ASSERT_EQ(0, missed);
    (void)correct;
  }

  Status Merge(int cf, const std::string& key, const std::string& value) {
    return db_->Merge(WriteOptions(), handles_[cf], Slice(key), Slice(value));
  }


//#ifndef ROCKSDB_LITE  // ReadOnlyDB is not supported
  void AssertOpenReadOnly(std::vector<std::string> cf,
                    std::vector<ColumnFamilyOptions> options = {}) {
    ASSERT_OK(OpenReadOnly(cf, options));
  }


  Status OpenReadOnly(std::vector<std::string> cf,
                         std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<ColumnFamilyDescriptor> column_families;
    names_.clear();
    for (size_t i = 0; i < cf.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(
          cf[i], options.size() == 0 ? column_family_options_ : options[i]));
      names_.push_back(cf[i]);
    }
    return DB::OpenForReadOnly(db_options_, dbname_, column_families, &handles_,
                               &db_);
  }


 // Random rnd_;
};

TEST_F(ColumnFamilyTest, 977_DoNotReuseColumnFamilyID) {
  for (int iter = 0; iter < 3; ++iter) {
    Open();
    Status s;
    cout<<iter<<endl;
    CreateColumnFamilies({"one", "two", "three"});
    for (size_t i = 0; i < handles_.size(); ++i) {
      auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(i, cfh->GetID());
    }
    if (iter == 1) {
      Reopen();
    }
    cout<<"before drop 3"<<endl;
    DropColumnFamilies({3});
    cout<<"after drop 3"<<endl;
     Reopen();
    if (iter == 2) {
      // this tests if max_column_family is correctly persisted with
      // WriteSnapshot()
      Reopen();
    }
    CreateColumnFamilies({"three2"});
     Reopen();
    cout <<"three2 created"<<endl;
    // ID 3 that was used for dropped column family "three" should not be reused
    auto cfh3 = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[3]);
    ASSERT_EQ(4U, cfh3->GetID());
    ASSERT_EQ("three2", cfh3->GetName());
    //DropColumnFamilies({4});
    Close();
    Destroy();
  }
   Open();
   Close();
   //Close();
   //Destroy();
}

#ifndef ROCKSDB_LITE
TEST_F(ColumnFamilyTest, 978_CreateCFRaceWithGetAggProperty) {
  Open();

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WriteOptionsFile:1",
        "ColumnFamilyTest.CreateCFRaceWithGetAggProperty:1"},
       {"ColumnFamilyTest.CreateCFRaceWithGetAggProperty:2",
        "DBImpl::WriteOptionsFile:2"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread thread([&] { CreateColumnFamilies({"one"}); });

  TEST_SYNC_POINT("ColumnFamilyTest.CreateCFRaceWithGetAggProperty:1");
  uint64_t pv;
  db_->GetAggregatedIntProperty(DB::Properties::kEstimateTableReadersMem, &pv);
  TEST_SYNC_POINT("ColumnFamilyTest.CreateCFRaceWithGetAggProperty:2");

  thread.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  Close();
}
#endif  // !ROCKSDB_LITE


TEST_F(ColumnFamilyTest, 979_AddDrop) {
  Open();
  CreateColumnFamilies({"one", "two", "three"});
  ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
  ASSERT_EQ("NOT_FOUND", Get(2, "fodor"));
  DropColumnFamilies({2});
  ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
  CreateColumnFamilies({"four"});
  ASSERT_EQ("NOT_FOUND", Get(3, "fodor"));
  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  ASSERT_EQ("NOT_FOUND", Get(3, "fodor"));
  Reopen();
  DropColumnFamilies({1});
  Reopen();
  Close();

  std::vector<std::string> families;
  ASSERT_OK(DB::ListColumnFamilies(db_options_, dbname_, &families));
  std::sort(families.begin(), families.end());
  for (auto family : families) {
   cout << "Cf is " << endl; 
   cout << family << endl;
 }   
ASSERT_TRUE(families ==
              std::vector<std::string>({"default", "four", "three"}));
}

//创建cf10-cf11000,每个put数据OK， 先drop cf在删除handle，最后检测只剩下default
//dumped hainan.liu
/*联调时会core dump
TEST_F(ColumnFamilyTest, 980_BulkAddDrop) {
  constexpr int kNumCF = 64;
  ColumnFamilyOptions cf_options;
  WriteOptions write_options;
  db_options_.num_cols = 65;
  Open();
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyHandle*> cf_handles;
  for (int i = 1; i <= kNumCF; i++) {
    cf_names.push_back("cf1-" + ToString(i));
  }
  ASSERT_OK(db_->CreateColumnFamilies(cf_options, cf_names, &cf_handles));
  for (int i = 1; i <= kNumCF; i++) {
    ASSERT_OK(db_->Put(write_options, cf_handles[i - 1], "foo", "bar"));
  }
  cout <<"----------after put---------------"<<endl;
  ASSERT_OK(db_->DropColumnFamilies(cf_handles));
  cout <<"----------dropped---------------"<<endl;
  std::vector<ColumnFamilyDescriptor> cf_descriptors;
  for (auto* handle : cf_handles) {
  //  delete handle;
  }
  cf_handles.clear();
  cout <<"----------after drop---------------"<<endl;
  for (int i = 1; i <= kNumCF; i++) {
    cf_descriptors.emplace_back("cf2-" + ToString(i), ColumnFamilyOptions());
  }
  ASSERT_OK(db_->CreateColumnFamilies(cf_descriptors, &cf_handles));
  for (int i = 1; i <= kNumCF; i++) {
    ASSERT_OK(db_->Put(write_options, cf_handles[i - 1], "foo", "bar"));
  }
  ASSERT_OK(db_->DropColumnFamilies(cf_handles));
  for (auto* handle : cf_handles) {
  //  delete handle;
  }
  db_options_.num_cols = 8;
  Close();
  std::vector<std::string> families;
  ASSERT_OK(DB::ListColumnFamilies(db_options_, dbname_, &families));
  std::sort(families.begin(), families.end());
  for (int i=0; i<families.size();i++)  
   {
     cout << families[i]<<endl;
   } 
  ASSERT_TRUE(families == std::vector<std::string>({"default"}));
  sleep(10);
}
*/
TEST_F(ColumnFamilyTest, 981_WriteBatchFailure) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  WriteBatch batch;
  batch.Put(handles_[0], Slice("existing"), Slice("column-family"));
  //auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[0]);
  //cout << "The CF name is :" << endl;
  //cout << cfh->GetName() << endl;
  batch.Put(handles_[1], Slice("non-existing"), Slice("column-family"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  DropColumnFamilies({1});
  WriteOptions woptions_ignore_missing_cf;
  woptions_ignore_missing_cf.ignore_missing_column_families = true;
  batch.Put(handles_[0], Slice("still here"), Slice("column-family"));
  ASSERT_OK(db_->Write(woptions_ignore_missing_cf, &batch));
  ASSERT_EQ("column-family", Get(0, "still here"));
  Status s = db_->Write(WriteOptions(), &batch);
  //cout << s.ToString()<<endl;
  ASSERT_TRUE(s.IsInvalidArgument());
  Close();
}

//put data to each cf, then get 
TEST_F(ColumnFamilyTest, 982_ReadWrite) {
  Open();
  //CreateColumnFamiliesAndReopen({"one", "two"});
  CreateColumnFamiliesAndReopen({"one", "two"});
  //delete db_;
  
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(0, "bar", "v2"));
  ASSERT_OK(Put(1, "mirko", "v3"));
  ASSERT_OK(Put(0, "foo", "v2"));
  ASSERT_OK(Put(2, "fodor", "v5"));
  //sleep(2);
  for (int iter = 0; iter <= 3; ++iter) {
    cout <<iter<<endl;
    ASSERT_EQ("v2", Get(0, "foo"));
    ASSERT_EQ("v2", Get(0, "bar"));
    ASSERT_EQ("v3", Get(1, "mirko"));
    ASSERT_EQ("v5", Get(2, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(0, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(2, "foo"));
    if (iter <= 1) {
       Reopen();
       cout << "Reopened" <<endl;
    }
  }
  Close();
}

TEST_F(ColumnFamilyTest, 983_CrashAfterFlush) {
  Open();
  CreateColumnFamilies({"one"});
  WriteBatch batch;
  batch.Put(handles_[0], Slice("foo"), Slice("bar"));
  batch.Put(handles_[1], Slice("foo"), Slice("bar"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  Flush(0);
  //fault_env->SetFilesystemActive(false);

  std::vector<std::string> names;
  for (auto name : names_) {
    if (name != "") {
      names.push_back(name);
    }
  }
  delete db_;
  Open(names, {});
  // Write batch should be atomic.
  ASSERT_EQ(Get(0, "foo"), Get(1, "foo"));
  Close();
}


//hainan.liu dump
TEST_F(ColumnFamilyTest, 1142_OpenNonexistentColumnFamily) {
  //ASSERT_OK(TryOpen({"default"}));
  //Close();
  std::vector<ColumnFamilyDescriptor> column_families;
  std::vector<ColumnFamilyOptions> options = {};
  std::vector<std::string> names_;
  std::vector<std::string> cf ={"default", "dne"};
  names_.clear();
  for (size_t i = 0; i < cf.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(
          cf[i], options.size() == 0 ? column_family_options_ : options[i]));
      names_.push_back(cf[i]);
  }
  Status s = DB::Open(db_options_, dbname_, column_families, &handles_, &db_);
  cout <<s.ToString()<<endl;
  ASSERT_TRUE(s.IsInvalidArgument());
}

class TestComparator : public Comparator {
  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
    return 0;
  }
  const char* Name() const override { return "Test"; }
  void FindShortestSeparator(std::string* start,
                             const rocksdb::Slice& limit) const override {}
  void FindShortSuccessor(std::string* key) const override {}
};

static TestComparator third_comparator;
static TestComparator fourth_comparator;

// Test that we can retrieve the comparator from a created CF
TEST_F(ColumnFamilyTest, 985_GetComparator) {
  Open();
  // Add a column family with no comparator specified
  CreateColumnFamilies({"first"});
  const Comparator* comp = handles_[0]->GetComparator();
  ASSERT_EQ(comp, BytewiseComparator());

  // Add three column families - one with no comparator and two
  // with comparators specified
  ColumnFamilyOptions second, third, fourth;
  second.comparator = &third_comparator;
  third.comparator = &fourth_comparator;
  CreateColumnFamilies({"second", "third", "fourth"}, {second, third, fourth});
  ASSERT_EQ(handles_[1]->GetComparator(), BytewiseComparator());
  ASSERT_EQ(handles_[2]->GetComparator(), &third_comparator);
  ASSERT_EQ(handles_[3]->GetComparator(), &fourth_comparator);
  Close();
}

TEST_F(ColumnFamilyTest, 1106_MergeOperators_Add) {
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = MergeOperators::CreateUInt64AddOperator();
  first.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen({default_cf, first});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_OK(Merge(0, "foo", two));
  ASSERT_EQ(Get(0, "foo"), three);
/* 
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_OK(Merge(1, "foo", two));
  ASSERT_EQ(Get(1, "foo"), three);
*/
  Close();
}

TEST_F(ColumnFamilyTest, 1107_MergeOperators_Append) {
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = MergeOperators::CreateStringAppendOperator();
  first.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen({default_cf, first});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_OK(Merge(0, "foo", two));
  ASSERT_EQ(Get(0, "foo"), one + "," + two);
/*
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_OK(Merge(1, "foo", two));
  ASSERT_EQ(Get(1, "foo"), one + "," + two);
*/
  Close();
}

TEST_F(ColumnFamilyTest, 1108_MergeOperators_Put) {
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = MergeOperators::CreatePutOperator();
  first.merge_operator = MergeOperators::CreatePutOperator();
  Reopen({default_cf, first});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_OK(Merge(0, "foo", two));
  ASSERT_EQ(Get(0, "foo"),two);
/*
  ASSERT_OK(Put(1, "foo1", "one"));
  ASSERT_OK(Put(1, "foo1", "two"));
  ASSERT_OK(Merge(1, "foo1", "three"));
  ASSERT_EQ(Get(1, "foo1"),"three");
*/
  Close();
}


TEST_F(ColumnFamilyTest, 1143_MergeOperators_Max) {
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = MergeOperators::CreateMaxOperator();
  first.merge_operator = MergeOperators::CreateMaxOperator();
  Reopen({default_cf, first});
  ASSERT_OK(Put(0, "foo", "1"));
  ASSERT_OK(Put(0, "foo", "3"));
  ASSERT_OK(Merge(0, "foo", "5"));
  ASSERT_EQ(Get(0, "foo"), "5");
  ASSERT_OK(Merge(0, "foo", "2"));
  ASSERT_EQ(Get(0, "foo"), "5");
/*
  ASSERT_OK(Put(1, "foo", "1"));
  ASSERT_OK(Put(1, "foo", "3"));
  ASSERT_OK(Merge(1, "foo", "5"));
  ASSERT_EQ(Get(1, "foo"), "5");
  ASSERT_OK(Merge(1, "foo", "2"));
  ASSERT_EQ(Get(1, "foo"), "5");
*/

  Close();
}
TEST_F(ColumnFamilyTest, 1109_MergeOperators_IDPut) {
  Open();
  ColumnFamilyOptions default_cf, first;
  CreateColumnFamilies({"first"});
  first.merge_operator = MergeOperators::CreateFromStringId("put");
  default_cf.merge_operator = MergeOperators::CreateFromStringId("put");
  Reopen({default_cf, first});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_OK(Merge(0, "foo", two));
  ASSERT_EQ(Get(0, "foo"),two);
/*
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_OK(Merge(1, "foo", two));
  ASSERT_EQ(Get(1, "foo"),two);
*/
  Close();
}

TEST_F(ColumnFamilyTest, 1203_MergeOperators_IDPutv1) {
  Open();
  ColumnFamilyOptions default_cf, first;
  CreateColumnFamilies({"first"});
  first.merge_operator = MergeOperators::CreateFromStringId("put_v1");
  default_cf.merge_operator = MergeOperators::CreateFromStringId("put_v1");
  Reopen({default_cf, first});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_OK(Merge(0, "foo", two));
  ASSERT_EQ(Get(0, "foo"),two);
/*
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_OK(Merge(1, "foo", two));
  ASSERT_EQ(Get(1, "foo"),two);
*/
  Close();
}

TEST_F(ColumnFamilyTest, 1110_MergeOperators_IDAppend) {
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = MergeOperators::CreateFromStringId("stringappend");
  first.merge_operator = MergeOperators::CreateFromStringId("stringappend");
  Reopen({default_cf, first});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_OK(Merge(0, "foo", two));
  ASSERT_EQ(Get(0, "foo"), one + "," + two);
/* 
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_OK(Merge(1, "foo", two));
  ASSERT_EQ(Get(1, "foo"), one + "," + two);
*/
  Close();
}

TEST_F(ColumnFamilyTest, 1111_MergeOperators_IDMax) {
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = MergeOperators::CreateFromStringId("max");
  first.merge_operator = MergeOperators::CreateFromStringId("max");
  Reopen({default_cf, first});
  ASSERT_OK(Put(0, "foo", "1"));
  ASSERT_OK(Put(0, "foo", "3"));
  ASSERT_OK(Merge(0, "foo", "5"));
  ASSERT_EQ(Get(0, "foo"), "5");
  ASSERT_OK(Merge(0, "foo", "2"));
  ASSERT_EQ(Get(0, "foo"), "5");
/*
  ASSERT_OK(Put(1, "foo", "1"));
  ASSERT_OK(Put(1, "foo", "3"));
  ASSERT_OK(Merge(1, "foo", "5"));
  ASSERT_EQ(Get(1, "foo"), "5");
  ASSERT_OK(Merge(1, "foo", "2"));
  ASSERT_EQ(Get(1, "foo"), "5");
*/

  Close();
}


//Empty or unknown, just return nullptr
TEST_F(ColumnFamilyTest, 1112_MergeOperators_IDUnknown) {
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = MergeOperators::CreateFromStringId("xxxxxx");
  first.merge_operator = MergeOperators::CreateFromStringId("xxxxxx");
  Reopen({default_cf, first});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_NOK(Merge(0, "foo", two));
  //ASSERT_EQ(Get(0, "foo"), one + "," + two);
/*
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_NOK(Merge(1, "foo", two));
*/
  Close();
}

TEST_F(ColumnFamilyTest, 1113_MergeOperators_IDEmpty) {
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = MergeOperators::CreateFromStringId("");
  first.merge_operator = MergeOperators::CreateFromStringId("");
  Reopen({default_cf, first});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_NOK(Merge(0, "foo", two));
  //ASSERT_EQ(Get(0, "foo"), one + "," + two);
/*
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_NOK(Merge(1, "foo", two));
*/
  Close();
}


TEST_F(ColumnFamilyTest, 1114_MergeOperators_NULL){
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = NULL;
  first.merge_operator = NULL;
  Reopen({default_cf, first});

  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
//  ASSERT_OK(Merge(0, "foo", two));
  ASSERT_TRUE(Merge(0, "foo", two).IsNotSupported()); 

/*
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
//  ASSERT_OK(Merge(0, "foo", two));
  ASSERT_TRUE(Merge(1, "foo", two).IsNotSupported());
*/
  Close(); 
}

TEST_F(ColumnFamilyTest, 1115_DefaultCFMergeOperators_Switch) {
  Open();
  ColumnFamilyOptions default_cf;
  default_cf.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen({default_cf});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_OK(Merge(0, "foo", two));
  ASSERT_EQ(Get(0, "foo"), one + "," + two);
  
  default_cf.merge_operator = MergeOperators::CreateUInt64AddOperator();
  Reopen({default_cf});  
  ASSERT_OK(Put(0, "foo1", two));
  ASSERT_OK(Put(0, "foo1", one));
  ASSERT_OK(Merge(0, "foo1", two));
  ASSERT_EQ(Get(0, "foo1"), three);
  Close();
}

/*
//目前不支持default CF 之外的操作
TEST_F(ColumnFamilyTest, 1116_MergeOperators_Switch) {
  Open();
  CreateColumnFamilies({"first"});
  ColumnFamilyOptions default_cf, first;
  first.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen({default_cf, first});
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_OK(Merge(1, "foo", two));
  ASSERT_EQ(Get(1, "foo"), one + "," + two);
  cout <<"first merge"<<endl;
  first.merge_operator = MergeOperators::CreateUInt64AddOperator();
  Reopen({default_cf,first});
  ASSERT_OK(Put(1, "foo1", two));
  ASSERT_OK(Put(1, "foo1", one));
  ASSERT_OK(Merge(1, "foo1", two));
  ASSERT_EQ(Get(1, "foo1"), three);
  Close();
}
*/

TEST_F(ColumnFamilyTest, 1117_MaxSuccessiveMergesChangeWithDBRecovery) {
  Open();

  Options options;
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.max_successive_merges = 3;
  options.num_cols = 8;
//  options.merge_operator = MergeOperators::CreatePutOperator();
  
  //options.disable_auto_compactions = true;
  //DestroyAndReopen(options);
  db_options_ = options;
//  delete db_;
  ColumnFamilyOptions default_cf;
  default_cf.merge_operator = MergeOperators::CreatePutOperator();
  Reopen({default_cf});

//  Status s = TryOpen({"default"});
//  cout <<s.ToString()<< endl;
//  cout <<"1st reopen" << endl;
  //Reopen(options);
  Put(0,"poi", "Finch");
  cout <<"after put" << endl;
  db_->Merge(WriteOptions(), "poi", "Reese");
  db_->Merge(WriteOptions(), "poi", "Shaw");
  db_->Merge(WriteOptions(), "poi", "Root");
  cout <<"after merge" << endl;
  options.max_successive_merges = 2;
  //Reopen(options);
  db_options_ = options;
  delete db_;
  TryOpen({"default"});
  cout <<"2nd reopen" << endl;
  ASSERT_EQ(Get(0, "poi"), "Root");
  Close();
}

/*
//  7月份讨论
TEST_F(ColumnFamilyTest, 986_DifferentMergeOperators) {
  Open();
  CreateColumnFamilies({"first", "second"});
  ColumnFamilyOptions default_cf, first, second;
  first.merge_operator = MergeOperators::CreateUInt64AddOperator();
  second.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen({default_cf, first, second});

  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);

  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  cout <<"1st merge" <<endl;
  //ASSERT_OK(Merge(0, "foo", two));
  //cout <<"1st merge" <<endl;

  ASSERT_TRUE(Merge(0, "foo", two).IsNotSupported());  //failed
  ASSERT_EQ(Get(0, "foo"), one);
  cout <<Get(0, "foo") <<endl;

  cout <<"2nd merge" <<endl;
  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_OK(Merge(1, "foo", two)); //dump
  ASSERT_EQ(Get(1, "foo"), three);

  ASSERT_OK(Put(2, "foo", two));
  ASSERT_OK(Put(2, "foo", one));
  cout <<"3rd merge" <<endl;
  ASSERT_OK(Merge(2, "foo", two));
  cout <<Get(2, "foo") <<endl;
  ASSERT_EQ(Get(2, "foo"), one + "," + two);

  Close();
}
*/

#ifndef ROCKSDB_LITE  // Tailing interator not supported
namespace {
std::string IterStatus(Iterator* iter) {
  std::string result;
  if (iter->Valid()) {
    result = iter->key().ToString() + "->" + iter->value().ToString();
  } else {
    result = "(invalid)";
  }
  return result;
}
}  // anonymous namespace


//kvdb 不支持tailing和mananged option. 7月份讨论
TEST_F(ColumnFamilyTest, 987_NewIteratorsTest) {
  // iter == 0 -- no tailing
  // iter == 2 -- tailing
  for (int iter = 0; iter < 1 ; ++iter) {
    Open();
    CreateColumnFamiliesAndReopen({"one", "two"});
    ASSERT_OK(Put(0, "a", "b"));
    ASSERT_OK(Put(1, "b", "a"));
    ASSERT_OK(Put(2, "c", "m"));
    ASSERT_OK(Put(2, "v", "t"));
    std::vector<Iterator*> iterators;
    ReadOptions options;
    options.tailing = (iter == 1);
    ASSERT_OK(db_->NewIterators(options, handles_, &iterators));

    for (auto it : iterators) {
      it->SeekToFirst();
    }
    cout <<iter<<endl;
    ASSERT_EQ(IterStatus(iterators[0]), "a->b");
    ASSERT_EQ(IterStatus(iterators[1]), "b->a");
    ASSERT_EQ(IterStatus(iterators[2]), "c->m");

    ASSERT_OK(Put(1, "x", "x"));

    for (auto it : iterators) {
      it->Next();
    }

    ASSERT_EQ(IterStatus(iterators[0]), "(invalid)");
    if (iter == 0) {
      // no tailing
      ASSERT_EQ(IterStatus(iterators[1]), "(invalid)");
    } else {
      // tailing
      ASSERT_EQ(IterStatus(iterators[1]), "x->x");
    }
    ASSERT_EQ(IterStatus(iterators[2]), "v->t");

    for (auto it : iterators) {
      delete it;
    }
    Close();
    //Destroy();
  }
}
#endif  // !ROCKSDB_LITE

/*
#ifndef ROCKSDB_LITE  // ReadOnlyDB is not supported
TEST_F(ColumnFamilyTest, ReadOnlyDBTest) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two", "three", "four"});
  ASSERT_OK(Put(0, "a", "b"));
  ASSERT_OK(Put(1, "foo", "bla"));
  ASSERT_OK(Put(2, "foo", "blabla"));
  ASSERT_OK(Put(3, "foo", "blablabla"));
  ASSERT_OK(Put(4, "foo", "blablablabla"));

  DropColumnFamilies({2});
  Close();
  // open only a subset of column families
  AssertOpenReadOnly({"default", "one", "four"});
  ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  ASSERT_EQ("bla", Get(1, "foo"));
  ASSERT_EQ("blablablabla", Get(2, "foo"));


  // test newiterators
  {
    std::vector<Iterator*> iterators;
    ASSERT_OK(db_->NewIterators(ReadOptions(), handles_, &iterators));
    for (auto it : iterators) {
      it->SeekToFirst();
    }
    ASSERT_EQ(IterStatus(iterators[0]), "a->b");
    ASSERT_EQ(IterStatus(iterators[1]), "foo->bla");
    ASSERT_EQ(IterStatus(iterators[2]), "foo->blablablabla");
    for (auto it : iterators) {
      it->Next();
    }
    ASSERT_EQ(IterStatus(iterators[0]), "(invalid)");
    ASSERT_EQ(IterStatus(iterators[1]), "(invalid)");
    ASSERT_EQ(IterStatus(iterators[2]), "(invalid)");

    for (auto it : iterators) {
      delete it;
    }
  }

  Close();
  // can't open dropped column family
  Status s = OpenReadOnly({"default", "one", "two"});
  ASSERT_TRUE(!s.ok());

  // Can't open without specifying default column family
  s = OpenReadOnly({"one", "four"});
  ASSERT_TRUE(!s.ok());
}
#endif  // !ROCKSDB_LITE
*/

TEST_F(ColumnFamilyTest, 988_CreateMissingColumnFamilies) {
  Status s = TryOpen({"one", "two"});
  ASSERT_TRUE(!s.ok());
  //delete db_;
  db_options_.create_missing_column_families = true;
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
    db_options_.env = env_;
    db_options_.num_cols = 8;
  s = TryOpen({"default", "one", "two"});
  ASSERT_TRUE(s.ok());
  Close();
}

/*
TEST_F(ColumnFamilyTest, 989_ReadDroppedColumnFamily) {
  // iter 0 -- drop CF, don't reopen
  // iter 1 -- delete CF, reopen
  int count = 0;
  for (int iter = 0; iter < 1; ++iter) {
    db_options_.create_missing_column_families = true;
    db_options_.max_open_files = 20;
    // delete obsolete files always
    db_options_.delete_obsolete_files_period_micros = 0;
    cout << "Open" <<endl;
    Open({"default", "one", "two"});
    ColumnFamilyOptions options;
    options.level0_file_num_compaction_trigger = 100;
    options.level0_slowdown_writes_trigger = 200;
    options.level0_stop_writes_trigger = 200;
    options.write_buffer_size = 100000;  // small write buffer size
    cout <<"before reopen" <<endl;
    Reopen({options, options, options});
    cout << iter<<endl;
    // 1MB should create ~10 files for each CF
    int kKeysNum = 10000;
    PutRandomData(0, kKeysNum, 100);
    PutRandomData(1, kKeysNum, 100);
    PutRandomData(2, kKeysNum, 100);

    {
      std::unique_ptr<Iterator> iterator(
          db_->NewIterator(ReadOptions(), handles_[2]));
      iterator->SeekToFirst();

      if (iter == 0) {
        // Drop CF two
        ASSERT_OK(db_->DropColumnFamily(handles_[2]));
      } else {
        // delete CF two
        db_->DestroyColumnFamilyHandle(handles_[2]);
        handles_[2] = nullptr;
      }
      // Make sure iterator created can still be used.
      count = 0;
      for (; iterator->Valid(); iterator->Next()) {
        ASSERT_OK(iterator->status());
        ++count;
      }
    cout <<iterator->status().ToString() <<endl;
     ASSERT_OK(iterator->status());   //failed
      ASSERT_EQ(count, kKeysNum);
    }

    // Add bunch more data to other CFs
    PutRandomData(0, kKeysNum, 100);
    PutRandomData(1, kKeysNum, 100);

    if (iter == 1) {
       cout << "into  reopen"<<endl;
       Reopen();
    }
    cout << "after reopen"<<endl;
    // Since we didn't delete CF handle, RocksDB's contract guarantees that
    // we're still able to read dropped CF
    for (int i = 0; i < 3; ++i) {
      cout <<i<<endl;
      std::unique_ptr<Iterator> iterator(
          db_->NewIterator(ReadOptions(), handles_[i]));
      int count = 0;
      cout <<"after new iterator"<<endl;
      for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
         //cout <<iterator->status().ToString()  <<endl;
         ASSERT_OK(iterator->status());
        ++count;
      }
     
      ASSERT_OK(iterator->status());
      ASSERT_EQ(count, kKeysNum * ((i == 2) ? 1 : 2));
    }
    cout <<"before close" <<endl;
    Close();
//    Destroy();
  }
}
*/

}
}  // namespace rocksdb

