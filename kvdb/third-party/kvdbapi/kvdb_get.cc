/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file kvdb_get.cc
  *  \brief Test cases code of the KBDB get().
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
//using namespace std;
#include "kvdb_test.h"
namespace rocksdb {

namespace cftest_get {

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


//hainan.liu 
class CFTest_GET: public KVDBTest {
 public:
  CFTest_GET(){
    env_ = new EnvCounter(Env::Default());
    dbname_ = test::TmpDir() + "/get_test";
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
    db_options_.env = env_;
    db_options_.num_cols = 8;
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
  }


  ~CFTest_GET() {
 // Close();
  //rocksdb::SyncPoint::GetInstance()->LoadDependency({});
  //rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
  //rocksdb::SyncPoint::GetInstance()->DisableProcessing();
   if(db_) 
   {Destroy();}
//   Options options;
//  options.env = env_;
//  if (getenv("KEEP_DB")) {
//    printf("DB is still at %s\n", dbname_.c_str());
//  } else if(db_){
//    EXPECT_OK(DestroyDB(dbname_, options));
//    }
    delete env_;
  }

  std::string Get_Without_Checksums(int cf,  const std::string& key) {
    ReadOptions options;
    options.verify_checksums = false;
    std::string result;
    Status s = db_->Get(options, handles_[cf], Slice(key), &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

};


 TEST_F(CFTest_GET, 912_GetKV){
  Open();
  //CreateColumnFamilies({"one"});
  // Generate log file A.
  //Reopen();
  ASSERT_OK(Put(0, "foo", "v1"));  // seqID 1
  //sleep(1);
  ASSERT_EQ("v1", Get(0, "foo"));
  Close();
 }


 TEST_F(CFTest_GET, 929_GetKVWithCF){
  Open();
  CreateColumnFamilies({"one"});
  // Generate log file A.
  Reopen();
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1
  //sleep(1);
  ASSERT_EQ("v1", Get(1, "foo"));
  Close();
 }

 TEST_F(CFTest_GET, 1184_GetKVMultiTimes){
  Open();
  CreateColumnFamilies({"one"});
  // Generate log file A.
  Reopen();
  for (int i=0;i<10;i++){
     cout <<i<<endl;
     ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1
     cout <<"after put"<<endl;
//     sleep(1);
     //Flush();  
     ASSERT_EQ("v1", Get(1, "foo"));
     cout <<"after get"<<endl;
}
  Close();
 }



 TEST_F(CFTest_GET, 1067_GetWithoutChecksums){
  Open();
  // Generate log file A.
  ASSERT_OK(Put(0, "foo", "v1"));  // seqID 1
  ASSERT_EQ("v1", Get_Without_Checksums(0, "foo"));
  Close();
 }

 TEST_F(CFTest_GET, 1068_GetWithoutChecksums_CF){
  Open();
  CreateColumnFamilies({"one"});
  // Generate log file A.
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1
  //Reopen();
  ASSERT_EQ("v1", Get_Without_Checksums(1, "foo"));
  cout <<"Get_Without_Checksums"<<endl;
  Close();
 }


 TEST_F(CFTest_GET, 930_GetSameKVInDiffCF){
  Open();
  CreateColumnFamilies({"one","two"});
  delete db_;
  Status s = TryOpen({"one","default","two"});
  cout << s.ToString() <<endl;  
  // Generate log file A.
  ASSERT_EQ("NOT_FOUND", Get(1,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(2,"foo"));
  
  ASSERT_OK(Put(1,"foo", "v1"));  // seqID 1
  ASSERT_OK(Put(2,"foo", "v2"));
 // Flush();
  ASSERT_EQ("v1", Get(1,"foo"));
  ASSERT_EQ("v2", Get(2, "foo"));
  Close();
 }
 

 TEST_F(CFTest_GET, 1069_ReopenThenGet){
  Open();
//  CreateColumnFamilies({"one","two"});
  ASSERT_EQ("NOT_FOUND", Get(0,"foo"));
  ASSERT_OK(Put(0,"foo", "v1"));
  ASSERT_EQ("v1", Get(0,"foo"));
  delete db_;
  Status s = TryOpen({"default"});
  cout << s.ToString() <<endl;
  // Generate log file A.
  ASSERT_EQ("v1", Get(0, "foo"));
  Close();
 }


 TEST_F(CFTest_GET, 1070_ReopenThenGet_CFs){
  Open();
  CreateColumnFamilies({"one","two"});
  delete db_;
  Status s = TryOpen({"default","one","two"});
  cout << s.ToString() <<endl;
  ASSERT_EQ("NOT_FOUND", Get(0,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(1,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(2,"foo2"));
  ASSERT_OK(Put(0,"foo", "v"));
  ASSERT_OK(Put(1,"foo", "v1"));
  ASSERT_OK(Put(2,"foo2", "v2"));
  ASSERT_EQ("v", Get(0,"foo"));
  ASSERT_EQ("v1", Get(1,"foo"));
  ASSERT_EQ("v2", Get(2,"foo2"));
  delete db_;
  s = TryOpen({"default","one","two"});
  cout << s.ToString() <<endl;
  // Generate log file A.
  ASSERT_EQ("v", Get(0, "foo"));
  ASSERT_EQ("v1", Get(1,"foo"));
  ASSERT_EQ("v2", Get(2,"foo2"));
  Close();
 }


//dump 如果drop掉two之后再去Get会core dump，这样就达不到测试的目的了，只能测试drop掉某个CF之后其他的CF还能get到数据。
TEST_F(CFTest_GET, 1071_GetFromDropedCF){
  Open();
  CreateColumnFamilies({"one","two"});
  // Generate log file A.
  ASSERT_EQ("NOT_FOUND", Get(1,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(2,"foo"));

  ASSERT_OK(Put(1,"foo", "v1"));  // seqID 1
  ASSERT_OK(Put(2,"foo", "v2"));
  cout <<"before drop 2"<<endl;
//  DropColumnFamilies({2});
//  cout <<"aftre drop 2"<<endl;
  ASSERT_OK(db_->DropColumnFamily(handles_[2]));
  db_->DestroyColumnFamilyHandle(handles_[2]);
  cout << "cf is dropped"<< endl;
  delete db_;
  db_=nullptr;
  Status s = TryOpen({"default","one","two"});
//   Reopen();
  cout <<s.ToString()<<endl;   //Invalid argument: Column family not found:two
  s = TryOpen({"default","one"});
  cout <<s.ToString()<<endl;
  auto cfh0 = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[0]);
  //ASSERT_EQ(4U, cfh0->GetID());
  cout << "The CF handle ID is :" << endl;
  cout<<  cfh0->GetID() << endl;
  cout << "The CF name is :" << endl;
  cout << cfh0->GetName() << endl;
  auto cfh1 = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[1]);
  //ASSERT_EQ(4U, cfh0->GetID());
  cout << "The CF handle ID is :" << endl;
  cout<<  cfh1->GetID() << endl;
  cout << "The CF name is :" << endl;
  cout << cfh1->GetName() << endl;
  auto cfh2 = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[2]);
  //ASSERT_EQ(4U, cfh0->GetID());
  cout << "The CF handle ID is :" << endl;
  cout<<  cfh2->GetID() << endl;
  cout << "The CF name is :" << endl;
  cout << cfh2->GetName() << endl;
  ASSERT_EQ("v1", Get(1,"foo"));  //如果添加reopen的操作则会NotFound
  cout << Get(1,"foo")<< endl;
  ReadOptions roptions;
  roptions.verify_checksums = true;
  std::string result;
  s = db_->Get(roptions, handles_[2],  "foo", &result);
  cout<<s.ToString()<<endl;
  Close();
 }



 TEST_F(CFTest_GET, 931_GetSameKVInMaxCF){
  Open();
  std::vector<std::string> cf_names;
  constexpr int kNumCF = 7;
  for (int i = 1; i <= kNumCF; i++) {
    cf_names.push_back("cf1-" + ToString(i));
  }
  CreateColumnFamilies(cf_names);  
  delete db_;
  cf_names.push_back("default");
  Status s=TryOpen(cf_names);
  cout <<s.ToString()<<endl;
  ASSERT_OK(s);
  // Generate log file A.
  ASSERT_EQ("NOT_FOUND", Get(0,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(1,"foo"));
  for (int i = 1; i <= kNumCF; i++) {
     ASSERT_OK(Put(i,"foo", "v1"));
  }
  for (int i = 1; i <= kNumCF; i++) {
     ASSERT_EQ("v1", Get(i, "foo"));
  }
  Close();
 }

 TEST_F(CFTest_GET, 1187_SameKeyMultiCF){
  Open();
  std::vector<std::string> cf_names;
  constexpr int kNumCF = 7;
  for (int i = 1; i <= kNumCF; i++) {
    cf_names.push_back("cf1-" + ToString(i));
  }
  CreateColumnFamilies(cf_names);
  delete db_;
  cf_names.push_back("default");
  Status s=TryOpen(cf_names);
  cout <<s.ToString()<<endl;
  ASSERT_OK(s);
  for (int i = 0; i <= kNumCF; i++) {
     ASSERT_EQ("NOT_FOUND",Get(i,"foo"));
     ASSERT_EQ("NOT_FOUND",Get(i,"foo"));
  }
  string v1="v1";
  string v2="v2";
  for (int i = 0; i <= kNumCF; i++) {
     ASSERT_OK(Put(i,"foo", "v1"+std::to_string(i)));
  }
  for (int i = 0; i <= kNumCF; i++) {
     ASSERT_EQ("v1"+std::to_string(i), Get(i, "foo"));
  }
  for (int i = 0; i <= kNumCF; i++) {
     ASSERT_OK(Put(i,"foo", "v2"+std::to_string(i)));
  }
  for (int i = 0; i <= kNumCF; i++) {
     ASSERT_EQ("v2"+std::to_string(i), Get(i, "foo"));
  }
  Close();
 }


 TEST_F(CFTest_GET, 1188_SameKeyMultiPut){
  Open();
  std::vector<std::string> cf_names;
  constexpr int kNumCF = 7;
  int kNum = 5;
  for (int i = 1; i <= kNumCF; i++) {
    cf_names.push_back("cf1-" + ToString(i));
  }
  CreateColumnFamilies(cf_names);
  delete db_;
  cf_names.push_back("default");
  Status s=TryOpen(cf_names);
  cout <<s.ToString()<<endl;
  ASSERT_OK(s);
  for (int i = 0; i <= kNumCF; i++) {
     ASSERT_EQ("NOT_FOUND",Get(i,"foo"));
     ASSERT_EQ("NOT_FOUND",Get(i,"foo"));
  }
//  string v1="v1";
//  string v2="v2";
  for (int i = 0; i <= kNumCF; i++) {
     ASSERT_OK(Put(i,"foo", "v1"+std::to_string(i)));
     ASSERT_EQ("v1"+std::to_string(i), Get(i, "foo"));
     ASSERT_OK(Put(i,"foo", "v2"+std::to_string(i)));
     ASSERT_EQ("v2"+std::to_string(i), Get(i, "foo"));
     ASSERT_OK(Put(i,"foo", "v3"+std::to_string(i)));
     ASSERT_EQ("v3"+std::to_string(i), Get(i, "foo"));
     ASSERT_OK(Put(i,"foo", "v4"+std::to_string(i)));
     ASSERT_EQ("v4"+std::to_string(i), Get(i, "foo"));
     ASSERT_OK(Put(i,"foo", "v5"+std::to_string(i)));
     ASSERT_EQ("v5"+std::to_string(i), Get(i, "foo"));
    }
  Close();
 }


 TEST_F(CFTest_GET, 1186_GetNullKey){
  Open();
  CreateColumnFamilies({"one"});
  // Generate log file A.
  Reopen();
  ASSERT_OK(Put(1, "", "v1"));  // seqID 1
  ASSERT_OK(Put(0, "", "v0"));
  //sleep(1);
  ASSERT_EQ("v0", Get(0, ""));
  ASSERT_EQ("v1", Get(1, ""));
  Close();
 }

TEST_F(CFTest_GET, 1185_GetMultiDB){
  Open();
  Options option_test;
  option_test.create_if_missing = true;
  option_test.fail_if_options_file_error = true;
  option_test.env = env_;
  ColumnFamilyOptions column_family_options_test;
  std::string multi_dbname= test::TmpDir() + "/multiDB_test";
  DestroyDB(multi_dbname, Options(option_test, column_family_options_test));

  DB* multi_db_ = nullptr;
  Status s = DB::Open(option_test,multi_dbname,&multi_db_);
  ASSERT_OK(s);
  int Num = 10;
  cout <<"delete db"<<endl; 
  for (int i = 1; i <= Num; i++) {
        ASSERT_OK(db_->Put(WriteOptions(), handles_[0],"key"+ToString(i), "value"+ToString(i)));
        ASSERT_OK(multi_db_->Put(WriteOptions(), handles_[0],"key_m"+ToString(i), "value_m"+ToString(i)));
  //      sleep(1);
        std::string result, result_m;
        db_->Get(ReadOptions(), handles_[0],"key"+ToString(i),&result);
        ASSERT_EQ("value"+ToString(i), result);
        multi_db_->Get(ReadOptions(), handles_[0],"key_m"+ToString(i),&result_m);
        ASSERT_EQ("value_m"+ToString(i), result_m);
  }
  cout <<"delete db"<<endl;
  delete multi_db_;
//  multi_db_ = nullptr;
  ASSERT_OK(DestroyDB(multi_dbname, Options(option_test, column_family_options_test)));
}

TEST_F(CFTest_GET, 913_GetKV_NonExist){
  Open();
  //CreateColumnFamilies({"one"});
  // Generate log file A.
  //Reopen();
  //ASSERT_OK(Put(0, "foo", "v1"));  // seqID 1
  //sleep(1);
  ASSERT_EQ("NOT_FOUND", Get(0, "foo")); //status.code() != Status::Code::kNotFound
  Close();
 }


 TEST_F(CFTest_GET, 1189_GetKVWithCF_NonExist){
  Open();
  CreateColumnFamilies({"one"});
  // Generate log file A.
  Reopen();
  //ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1
  //sleep(1);
  cout <<Get(1, "foo")<<endl;
  ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
  Close();
 }

 TEST_F(CFTest_GET, 1190_NonBlocking_Get){
   Open();
   ReadOptions non_blocking_opts, regular_opts;
   non_blocking_opts.read_tier = kBlockCacheTier;
   regular_opts.read_tier = kReadAllTier;
   std::string result;
   std::string key;
   std::string value;
   ASSERT_OK(Put(0, "a", "b"));
   for (int i=0; i<1024; i++){
     key = "key"+std::to_string(i);
     value = "value"+std::to_string(i);
     ASSERT_OK(Put(0, key, value));

   }
   Status s = db_->Get(non_blocking_opts, handles_[0], "a", &result);
   ASSERT_OK(s);
   ASSERT_EQ(result, "b");
   s = db_->Get(regular_opts, handles_[0], "a", &result);
   ASSERT_OK(s);
   ASSERT_EQ(result, "b");
   Flush();
   Reopen();
 // delete db_;
 // db_ = nullptr;
 // std::vector<std::string> cf_names;
 // cf_names.push_back("default");
 // s=TryOpen(cf_names);
 // cout <<s.ToString()<<endl;
   s = db_->Get(non_blocking_opts, handles_[0], "a", &result);
   cout <<s.ToString()<<endl;
//   ASSERT_TRUE(s.IsNotFound());
   ASSERT_TRUE(s.IsIncomplete());
   s = db_->Get(regular_opts, handles_[0], "a", &result);
   ASSERT_OK(s);
   s = db_->Get(non_blocking_opts, handles_[0], "a", &result);
   ASSERT_OK(s);
   Close();
}

 TEST_F(CFTest_GET, 1191_NonBlocking_GetCF){
   Open();
   CreateColumnFamilies({"one"});
   delete db_;
   Status s = TryOpen({"default","one"});
   ASSERT_OK(s);
   
   ReadOptions non_blocking_opts, regular_opts;
   non_blocking_opts.read_tier = kBlockCacheTier;
   std::string result;
   ASSERT_OK(Put(1, "a", "b"));
   s = db_->Get(non_blocking_opts, handles_[1], "a", &result);
   ASSERT_OK(s);
   s = db_->Get(regular_opts, handles_[1], "a", &result);
   ASSERT_OK(s);
   
   Flush();
   Reopen();
   s = db_->Get(non_blocking_opts, handles_[1], "a", &result);
   //ASSERT_TRUE(s.IsNotFound());
   cout <<s.ToString()<<endl;
   ASSERT_TRUE(s.IsIncomplete());
   s = db_->Get(regular_opts, handles_[1], "a", &result);
   ASSERT_OK(s);
   s = db_->Get(non_blocking_opts, handles_[1], "a", &result);
   ASSERT_OK(s);
   Close();
}

TEST_F(CFTest_GET, DISABLED_1192_MultiGetSimple) {
//  do {
    Open();
    CreateColumnFamilies({"one"});
    delete db_;
    Status stat = TryOpen({"default","one"});
    ASSERT_OK(stat);
    
//    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    std::vector<Slice> keys({"k1", "k2", "k3", "k4", "k5", "no_key"});

    std::vector<std::string> values(20, "Temporary data to be overwritten");
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);

    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(values[0], "v1");
    ASSERT_EQ(values[1], "v2");
    ASSERT_EQ(values[2], "v3");
    ASSERT_EQ(values[4], "v5");

    ASSERT_OK(s[0]);
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_TRUE(s[3].IsNotFound());
    ASSERT_OK(s[4]);
    ASSERT_TRUE(s[5].IsNotFound());
//  } while (ChangeCompactOptions());
    Close();

}

TEST_F(CFTest_GET, DISABLED_1193_MultiGetEmpty) {
//  do {
//    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    Open();
    CreateColumnFamilies({"one"});
    delete db_;
    Status stat = TryOpen({"default","one"});
    ASSERT_OK(stat);

    // Empty Key Set
    std::vector<Slice> keys;
    std::vector<std::string> values;
    std::vector<ColumnFamilyHandle*> cfs;
    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Empty Key Set
    Options options;
    options.create_if_missing = true;
    DestroyAndReopen(options);
    CreateColumnFamilies({"one"});
    delete db_;
    stat = TryOpen({"default","one"});
    ASSERT_OK(stat);

//    CreateAndReopenWithCF({"pikachu"}, options);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Search for Keys
    keys.resize(2);
    keys[0] = "a";
    keys[1] = "b";
    cfs.push_back(handles_[0]);
    cfs.push_back(handles_[1]);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(static_cast<int>(s.size()), 2);
    ASSERT_TRUE(s[0].IsNotFound() && s[1].IsNotFound());
//  } while (ChangeCompactOptions());
}


}  //end of namespace cf_test


}
