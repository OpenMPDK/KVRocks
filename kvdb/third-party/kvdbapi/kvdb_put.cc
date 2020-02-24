/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file kvdb_put.cc
  *  \brief Test cases code of the KBDB put().
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////

#include "kvdb_test.h"
#include "util/stop_watch.h"

namespace rocksdb {

namespace cftest_put {

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
class CFTest_PUT: public KVDBTest {
 public:
  CFTest_PUT() {
    env_ = new EnvCounter(Env::Default());
    dbname_ = test::TmpDir() + "/put_test";
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
    db_options_.env = env_;
    db_options_.num_cols = 8;
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
  }


  ~CFTest_PUT() {
  Close();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->LoadDependency({});
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
  cout << "---------------~DBPutTest--------------------"<< endl;
  Options options;
  options.env = env_;
//  if (getenv("KEEP_DB")) {
//    printf("DB is still at %s\n", dbname_.c_str());
//  } else if(db_){
//    EXPECT_OK(DestroyDB(dbname_, options));
//    }
  if(db_){  
    Destroy();
  }
    delete env_;
  }
 
int PutMultiKV(int cf, int num){
  Status result;
  for (int i = 0; i <= num; i++) {
    result = Put(cf,"foo"+std::to_string(i), "v"+std::to_string(i));
    if (result.code()!= Status::Code::kOk )
       return FAILURE;
   }
  for (int i = 0; i <= num; i++) {
    if (("v"+std::to_string(i))!=Get(cf, "foo"+std::to_string(i)))
       return FAILURE;
  }

  return SUCCESS;
}

int PutMultiKVSync(int cf, int num){
  WriteOptions woptions;
  woptions.sync = true;
  Status result;
  for (int i = 0; i <= num; i++) {
    result = db_->Put(woptions, handles_[cf],"foo"+std::to_string(i), "v"+std::to_string(i));
    if (result.code()!= Status::Code::kOk )
       return FAILURE;
   }
  for (int i = 0; i <= num; i++) {
    if (("v"+std::to_string(i))!=Get(cf, "foo"+std::to_string(i)))
       return FAILURE;
  }

  return SUCCESS;
}

  void VerifyDB(const std::map<std::string, std::string> &data) {
    Iterator *iter = db_->NewIterator(ReadOptions());
    iter->SeekToFirst();
    for (auto &p : data) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(p.first, iter->key().ToString());
      ASSERT_EQ(p.second, iter->value().ToString());
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }


};


TEST_F(CFTest_PUT, 958_PutKVWithCF){
  Open();
  CreateColumnFamilies({"one"});
  // Generate log file A.
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1
  //Reopen();
  ASSERT_EQ("v1", Get(1, "foo"));
  //DropColumnFamilies({1});
  Close(); 
 }

TEST_F(CFTest_PUT, 1083_WriteEmptyBatch) {
  Open();
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;
  options.num_cols = 8;
  db_options_ = options;
  //CreateAndReopenWithCF({"pikachu"}, options);
  CreateColumnFamilies({"pikachu"});
  delete db_;
  TryOpen({"default","pikachu"});
  
  ASSERT_OK(Put(1, "foo", "bar"));
  WriteOptions wo;
  wo.sync = true;
  wo.disableWAL = false;
  WriteBatch empty_batch;
  ASSERT_OK(db_->Write(wo, &empty_batch));

  // make sure we can re-open it.
  //ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
  Reopen();
  ASSERT_EQ("bar", Get(1, "foo"));
  Close();
}

TEST_F(CFTest_PUT, 1174_DeleteBatch) {
  //BlobDBOptionsImpl bdb_options;
  //bdb_options.disable_background_tasks = true;
  Open();
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;
  options.num_cols = 8;
//  options.disable_background_tasks = true;
  db_options_ = options;
  delete db_;
  Status s = TryOpen({"default"});
  cout <<s.ToString()<<endl;
  for (size_t i = 0; i < 10; i++) {
    Put(0, "key" + ToString(i), ToString(i));
  }
  ASSERT_EQ("1",Get(0, "key1"));
  ASSERT_EQ("5",Get(0, "key5"));
  ASSERT_EQ("9",Get(0, "key9"));
  WriteBatch batch;
  for (size_t i = 0; i < 10; i++) {
    batch.Delete("key" + ToString(i));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  // DB should be empty.
  ASSERT_EQ("NOT_FOUND",Get(0, "key1"));
  ASSERT_EQ("NOT_FOUND",Get(0, "key5"));
  ASSERT_EQ("NOT_FOUND",Get(0, "key9"));
  Close();
 }

TEST_F(CFTest_PUT, 1175_WriteBatchAcrossCF) {
  //BlobDBOptionsImpl bdb_options;
  //bdb_options.disable_background_tasks = true;
  Open();
  CreateColumnFamilies({"one", "two"});
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;
  options.num_cols = 8;
//  options.disable_background_tasks = true;
  db_options_ = options;
  delete db_;
  Status s = TryOpen({"default","one","two"});
  cout <<s.ToString()<<endl;
  WriteBatch batch;
  batch.Put(handles_[0], "key0", "value0");
  batch.Put(handles_[1], "key1", "value1");
  batch.Put(handles_[2], "key2", "value2");
  batch.Put(handles_[2], "key22", "value22");
  batch.Delete(handles_[2],"key22");
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_EQ("value0",Get(0, "key0"));
  ASSERT_EQ("value1",Get(1, "key1"));
  ASSERT_EQ("value2",Get(2, "key2"));
  ASSERT_EQ("NOT_FOUND",Get(2, "key22"));
  Close();
 }

TEST_F(CFTest_PUT, 1176_WriteBatchTest_Default) {
  Open();
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;
  options.num_cols = 8;
  ColumnFamilyOptions default_cf;
  default_cf.merge_operator = MergeOperators::CreatePutOperator();
  //options.merge_operator = MergeOperators::CreateStringAppendOperator();
  db_options_ = options;
  //CreateAndReopenWithCF({"pikachu"}, options);
//  CreateColumnFamilies({"pikachu"});
  delete db_;
  TryOpen({"default"},{default_cf});
 

  ASSERT_OK(Put(0, "foo", "bar")); 
  ASSERT_OK(Put(0, "bee", "bao"));
  WriteOptions wo;
  //wo.sync = true;
  //wo.disableWAL = false;
  WriteBatch batch;
  batch.Put(handles_[0],Slice("k1"), Slice("v1"));
  batch.Put(handles_[0],Slice("k2"), Slice("v2"));
  cout<<"before merge"<<endl;
  batch.Merge(handles_[0],Slice("k2"), Slice("v22"));
  cout<<"after merge"<<endl;
  batch.Put(handles_[0],Slice("k3"), Slice("v3"));
//  batch.PutLogData(Slice("blob1"));
  batch.Delete(handles_[0],Slice("bee"));
  batch.SingleDelete(handles_[0],Slice("k3"));
  cout<<"before write"<<endl;
  ASSERT_OK(db_->Write(wo, &batch));
  cout<<"after write"<<endl;
  // make sure we can re-open it.
  Reopen();
  ASSERT_EQ("bar", Get(0, "foo"));
  ASSERT_EQ("NOT_FOUND", Get(0, "bee"));
  ASSERT_EQ("v1", Get(0, "k1"));
  ASSERT_EQ("v22", Get(0, "k2"));
  ASSERT_EQ("NOT_FOUND", Get(0, "k3"));

  Close();
}


TEST_F(CFTest_PUT, 1177_WriteBatchTest) {
  Open();
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;
  options.num_cols = 8;
  db_options_ = options;
  CreateColumnFamilies({"pikachu"});
  ColumnFamilyOptions default_cf, first;
  default_cf.merge_operator = MergeOperators::CreatePutOperator();
  Reopen({default_cf, first});

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "bee", "bao"));
  WriteOptions wo;
  //wo.sync = true;
  //wo.disableWAL = false;
  WriteBatch batch;
  batch.Put(handles_[1],Slice("k1"), Slice("v1"));
  batch.Put(handles_[1],Slice("k2"), Slice("v2"));
  batch.Merge(handles_[1],Slice("k2"), Slice("v22"));
  batch.Put(handles_[1],Slice("k3"), Slice("v3"));
//  batch.PutLogData(Slice("blob1"));
  batch.Delete(handles_[1],Slice("bee"));
  batch.SingleDelete(handles_[1],Slice("k3"));
  ASSERT_OK(db_->Write(wo, &batch));
  // make sure we can re-open it.
  Reopen();
  ASSERT_EQ("bar", Get(1, "foo"));
  ASSERT_EQ("NOT_FOUND", Get(1, "bee"));
  ASSERT_EQ("v1", Get(1, "k1"));
  ASSERT_EQ("v22", Get(1, "k2"));
  ASSERT_EQ("NOT_FOUND", Get(1, "k3"));
  Close();
}

 TEST_F(CFTest_PUT, 959_PutKVWithDropedCF){
  Open();
  CreateColumnFamilies({"one", "two", "three"});
  for (size_t i = 0; i < handles_.size(); ++i) {
      auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(i, cfh->GetID());
      cout << "The CF handle ID is :" << endl;
      cout<<  cfh->GetID() << endl;
       cout << "The CF name is :" << endl;
       cout << cfh->GetName() << endl;
  }
  // Generate log file A.
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1
  //Reopen();
  ASSERT_EQ("v1", Get(1, "foo"));
  // DropColumnFamilies({3}); //会把handle_[3]和names_[3]置为空，并且清除掉handles_[3]对应的CF
  ASSERT_OK(db_->DropColumnFamily(handles_[3]));
  db_->DestroyColumnFamilyHandle(handles_[3]); 
  CreateColumnFamilies({"four"});
  //Reopen();
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[4]);
  cout << "The CF four handle ID is :" << endl;  //3
  cout<<  cfh->GetID() << endl;
  cout<<  cfh->GetName() << endl;
  auto cfh3 = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[3]);
  cout << "The CF three handle ID is :" << endl;  //3
  cout<<  cfh3->GetID() << endl;
  cout<<  cfh3->GetName() << endl;
  ASSERT_OK(Put(4, "foo", "v4"));//给handle_[4]中写入
  ASSERT_OK(Put(3, "foo", "v3")); //应该失败
  //db_->Put(WriteOptions(), "", "foo", "v3");
  ASSERT_EQ("v4", Get(4, "foo")); //get from handles_[4]
  ASSERT_EQ("NOT_FOUND", Get(3, "foo"));//应该返回not found

  DropColumnFamilies({1,2,4});
  Close();
 }


 TEST_F(CFTest_PUT, 1178_PutKVWithRecreatedCF){
  Open();
  CreateColumnFamilies({"one", "two", "three"});
  for (size_t i = 0; i < handles_.size(); ++i) {
      auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(i, cfh->GetID());
      cout << "The CF handle ID is :" << endl;
      cout<<  cfh->GetID() << endl;
       cout << "The CF name is :" << endl;
       cout << cfh->GetName() << endl;
  }
  // Generate log file A.
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1
  //Reopen();
  ASSERT_EQ("v1", Get(1, "foo"));
  //DropColumnFamilies({3}); //会把handle_[3]和names_[3]置为空，并且清除掉handles_[3]对应的CF
  ASSERT_OK(Put(1, "foo", "v1"));
  ASSERT_EQ("v1", Get(1, "foo"));

  ASSERT_OK(Put(3, "foo3", "v3"));
  ASSERT_EQ("v3", Get(3, "foo3"));

  ASSERT_OK(db_->DropColumnFamily(handles_[3]));
  db_->DestroyColumnFamilyHandle(handles_[3]);
  handles_[3] = nullptr;
  names_[3] = "";  
  CreateColumnFamilies({"three"});
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[4]);
  cout << "The CF four handle ID is :" << endl;  //3
  cout<<  cfh->GetID() << endl;
  cout<<  cfh->GetName() << endl;
//  auto cfh3 = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[3]);
//  cout << "The CF three handle ID is :" << endl;  //3
//  cout<<  cfh3->GetID() << endl;
//  cout<<  cfh3->GetName() << endl;
 // ASSERT_EQ("NOT_FOUND", Get(4, "foo3"));
  Reopen();
  ASSERT_EQ("NOT_FOUND", Get(4, "foo3")); 
  ASSERT_OK(Put(4, "foo", "v4"));//给handle_[4]中写入
//  Put(3, "foo", "v3"); //will dump
  //db_->Put(WriteOptions(), "", "foo", "v3");
  ASSERT_EQ("v4", Get(4, "foo")); //get from handles_[4]
//  ASSERT_EQ("v3", Get(3, "foo"));
  DropColumnFamilies({1,2,4});
  Close();
 }

//dump
TEST_F(CFTest_PUT, 1140_PutKVWithInvalidCF){
  Open();
  CreateColumnFamilies({"one","two"});
  // Generate log file A.
  ASSERT_OK(Put(1, "foo1", "v1"));
  ASSERT_OK(Put(2, "foo2", "v2"));  // seqID 1
  //Reopen();
  //DropColumnFamilies({"two"});
  ASSERT_OK(db_->DropColumnFamily(handles_[2]));
  Status s = db_->DestroyColumnFamilyHandle(handles_[2]);
  delete db_;
  TryOpen({"default","one"});
  ASSERT_NOK(Put(2, "foo2", "v22")); 
  ASSERT_EQ("v1", Get(1, "foo1"));
  handles_[2] = nullptr;
  names_[2] = "";
  Close();
 }

 TEST_F(CFTest_PUT, 960_PutNullCF){
  Open();
  CreateColumnFamilies({"one",""});
  // Generate log file A.
  ASSERT_OK(Put(2, "foo1", "v1"));  // seqID 1
  //Reopen();
  //sleep(1); //should be deleted next version
  ASSERT_EQ("v1", Get(2, "foo1"));
  Close();
 }

//PutWithoutKey
 TEST_F(CFTest_PUT, 1064_PutSpecialKV){
  Open();
//  CreateColumnFamilies({"one",""});
  // Generate log file A.
  ASSERT_OK(Put(0, "", ""));  // seqID 1
  //Reopen();
  ASSERT_EQ("", Get(0, ""));
  ASSERT_OK(Put(0, "A_b#2(%^*&)@三星", "A_b#2(%^*&)@三星"));
  ASSERT_EQ("A_b#2(%^*&)@三星", Get(0, "A_b#2(%^*&)@三星"));
  ASSERT_OK(Put(0, " ", " "));
  ASSERT_EQ(" ", Get(0, " "));

  Close();
 }



 TEST_F(CFTest_PUT, 961_PutSpecialKVWithCF){
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_OK(Put(1, "", ""));  // seqID 1
  ASSERT_EQ("", Get(1, ""));
  ASSERT_OK(Put(1, "A_b#2(%^*&)@三星", "A_b#2(%^*&)@三星"));
  ASSERT_EQ("A_b#2(%^*&)@三星", Get(1, "A_b#2(%^*&)@三星"));
  ASSERT_OK(Put(1, " ", " "));
  ASSERT_EQ(" ", Get(1, " "));
  Close();
 }

 TEST_F(CFTest_PUT, 962_PutSameKeyInDiffCF){
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_EQ("NOT_FOUND", Get(0,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(1,"foo"));
  ASSERT_OK(Put(0,"foo", "v1"));  // seqID 1
  ASSERT_EQ("v1", Get(0,"foo"));
  ASSERT_OK(Put(1,"foo", "v11"));
  ASSERT_EQ("v11", Get(1, "foo"));
  Close();
 }
 
 TEST_F(CFTest_PUT, 963_PutSameKeyInFiveCF){
  Open();
  CreateColumnFamilies({"one","two","three","four","five"});
  ASSERT_EQ("NOT_FOUND", Get(0,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(1,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(2,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(3,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(4,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(5,"foo"));

  ASSERT_OK(Put(0,"foo", "v1"));  // seqID 1
  ASSERT_EQ("v1", Get(0,"foo"));
  //Reopen();
  ASSERT_OK(Put(1,"foo", "v11"));
  ASSERT_EQ("v11", Get(1, "foo"));
  ASSERT_OK(Put(3,"foo", "v111"));
  ASSERT_EQ("v111", Get(3, "foo"));
  ASSERT_OK(Put(5,"foo", "v11111"));
  ASSERT_EQ("v11111", Get(5, "foo"));
  Close();
 }

 TEST_F(CFTest_PUT, 1065_PutDiffKeyInFiveCF){
  Open();
  CreateColumnFamilies({"one","two","three","four","five"});
  // Generate log file A.
  ASSERT_EQ("NOT_FOUND", Get(0,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(1,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(2,"foo2"));
  ASSERT_EQ("NOT_FOUND", Get(3,"foo3"));
  ASSERT_EQ("NOT_FOUND", Get(4,"foo4"));
  ASSERT_EQ("NOT_FOUND", Get(5,"foo5"));

  ASSERT_OK(Put(0,"foo", "v"));  // seqID 1
  ASSERT_EQ("v", Get(0,"foo"));
  //Reopen();
  ASSERT_OK(Put(1,"foo", "v"));
  ASSERT_EQ("v", Get(1, "foo"));
  ASSERT_OK(Put(2,"foo2", "v"));
  ASSERT_EQ("v", Get(2, "foo2"));
  ASSERT_OK(Put(3,"foo3", "v"));
  ASSERT_EQ("v", Get(3, "foo3"));
  ASSERT_OK(Put(4,"foo4", "v"));
  ASSERT_EQ("v", Get(4, "foo4"));
  ASSERT_OK(Put(5,"foo5", "v"));
  ASSERT_EQ("v", Get(5, "foo5"));
  Close();
 }



 TEST_F(CFTest_PUT, 964_PutSameKVInMaxCF){
  //insdb::Options insdb_options;
  //insdb_options.num_column_count = 5;
  //Options options = CurrentOptions();
  Open();
  std::vector<std::string> cf_names;
  constexpr int kNumCF = 7;
  for (int i = 1; i <= kNumCF; i++) {
    cf_names.push_back("cf1-" + ToString(i));
  } 
  //CreateColumnFamiliesAndReopen(cf_names); 
  CreateColumnFamilies(cf_names);
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

TEST_F(CFTest_PUT, 1179_No_SlowDown) {
  Open();
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;
  options.num_cols = 8;
  db_options_ = options;
  //CreateAndReopenWithCF({"pikachu"}, options);
  CreateColumnFamilies({"pikachu"});
  delete db_;
  TryOpen({"default","pikachu"});
//  ASSERT_OK(Put(1, "foo", "bar"));
  WriteOptions wo;
  wo.sync = true;    //If this flag is true, writes will be slower
  wo.no_slowdown =true;   //If true and we need to wait or sleep for the write request, fails immediately with Status::Incomplete().
  //Status s = db_->Put(wo, "k1", "v1");
  //ASSERT_OK(s);
  string Key[3] = {"k1","k2","k3"};
  for (int i = 0; i < 3; i++) {
    Put(Key[i], std::string(i+1, 'x'));
    Flush();
  }  
  std::string result = Get(0, "k3");
  cout <<result<<endl; 
  ASSERT_EQ("xxx", result);
 }
//PutRightData
TEST_F(CFTest_PUT, 932_PutKVWithDefaultCF){
  Open();
  // Generate log file A.
  ASSERT_OK(Put(0, "foo", "v1"));  // seqID 1
  //Reopen();
  ASSERT_EQ("v1", Get(0, "foo"));
  //DropColumnFamilies({1});
  Close();
 }

//PutWithSameKey
TEST_F(CFTest_PUT, 935_PutSameKeyWithDefaultCF){
  Open();
  // Generate log file A.
  ASSERT_OK(Put(0, "foo", "v1"));  // seqID 1
  //Reopen();
  ASSERT_OK(Put(0, "foo", "v2")); 
  ASSERT_EQ("v2", Get(0, "foo"));
  //DropColumnFamilies({1});
  Close();
 }

//PutWithSameKV
TEST_F(CFTest_PUT, 936_PutSameKVWithDefaultCF){
  Open();
  // Generate log file A.
  ASSERT_OK(Put(0, "foo", "v1"));  // seqID 1
  //Reopen();
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_EQ("v1", Get(0, "foo"));
  //DropColumnFamilies({1});
  Close();
 }

//PutToDeletedkey
TEST_F(CFTest_PUT, 949_PutDeletedKeyWithDefaultCF){
  Open();
  // Generate log file A.
  ASSERT_OK(Put(0, "foo", "v1"));  // seqID 1
  //Reopen();
  ASSERT_OK(Delete(0,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  ASSERT_OK(Put(0, "foo", "v2"));
  ASSERT_EQ("v2", Get(0, "foo"));
  //DropColumnFamilies({1});
  Close();
 }


//Put100KV
TEST_F(CFTest_PUT, 951_Put100KVWithDefaultCF){
  Open();
  // Generate log file A.
  ASSERT_EQ(0, PutMultiKV(0, 100));
  Close();
 }

//Put1000KV
TEST_F(CFTest_PUT, 952_Put1000KVWithDefaultCF){
  Open();
  // Generate log file A.
  ASSERT_EQ(0, PutMultiKV(0, 1000));
  Close();
 }

//Put10000KV
TEST_F(CFTest_PUT, 1180_Put10000KVWithDefaultCF){
  Open();
  // Generate log file A.
  ASSERT_EQ(0, PutMultiKV(0, 10000));
  Close();
}

TEST_F(CFTest_PUT, 1181_Put100000KVWithDefaultCF){
  Open();
  // Generate log file A.
  ASSERT_EQ(0, PutMultiKV(0, 100000));
  Close();
 }
//PutSync
TEST_F(CFTest_PUT, DISABLED_956_PutSync){
  StopWatchNano timer(Env::Default());
  Open();
  CreateColumnFamilies({"one"});
  timer.Start();
  WriteOptions woptions1;
  for (int i = 1; i<=100000; i++){ 
     ASSERT_OK(db_->Put(woptions1, handles_[1], "key"+ToString(i), "value"+ToString(i)));
  }
  uint64_t elapsed = timer.ElapsedNanos();
  cout <<std::to_string(elapsed)<<endl;
  
  timer.Start();
  WriteOptions woptions;
  woptions.sync=true;
  for (int i = 1; i<=100000; i++){
     ASSERT_OK(db_->Put(woptions,handles_[1],"k"+ToString(i), "v"+ToString(i)));
  }
  uint64_t elapsed_1 = timer.ElapsedNanos();
  cout <<std::to_string(elapsed_1)<<endl;
  ASSERT_LT(elapsed-elapsed_1, 20000000);
  cout <<std::to_string(elapsed-elapsed_1)<<endl;
  Close();
}
//PutValue4G
TEST_F(CFTest_PUT, 957_PutLargeKVWithDefaultCF){
  Open();
  std::string testkey(100*1024+1024*100, '1');
  std::string testvalue(100*1024, 'a');
  //std::string testkey(1024+1024, '1');
  //std::string testvalue(1024+1024, 'a'); 


 // int len;
 // len = testkey.max_size(); 
 // cout <<"max size :" << len<<endl;
  // Generate log file A.
  ASSERT_OK(Put(0,testkey ,testvalue));  // seqID 1
  //cout <<testkey<<endl;

  ASSERT_EQ(testvalue, Get(0, testkey));
  //DropColumnFamilies({1});
  Close();
 }

/*
TEST_F(CFTest_PUT, 1182_PutMultiCFInRowSeq){
  Open();
  CreateColumnFamilies({"one","two","three","four"});
  //Reopen();
  ASSERT_EQ("NOT_FOUND", Get(0,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(1,"foo"));
  int i,j;
  int kv_num = 99999;
  //row put
  cout<<"row put"<<endl;
    for (j=0; j< kv_num; j++){
     for (i=0; i<4; i++)   
     { ASSERT_OK(Put(i,"foo"+std::to_string(j), "v"+std::to_string(j)));}
  }
  //column get
  cout<<"column get"<<endl;
  for (i=0; i<4; i++){
   for (j=0; j< kv_num; j++)
     { ASSERT_EQ(Get(i,"foo"+std::to_string(j)), "v"+std::to_string(j));}
  }
  //row get
  cout<<"row get"<<endl;
  for (j=0; j< kv_num; j++){
     for (i=0; i<4; i++)
     { ASSERT_EQ(Get(i,"foo"+std::to_string(j)), "v"+std::to_string(j));}
  }
  Close();
 }

TEST_F(CFTest_PUT, 1183_PutMultiCFInColSeq){
  Open();
  CreateColumnFamilies({"one","two","three","four"});
  ASSERT_EQ("NOT_FOUND", Get(0,"foo"));
  ASSERT_EQ("NOT_FOUND", Get(1,"foo"));
  int i,j;
  int kv_num = 99999;
  //column put
  cout<<"column put"<<endl;
  for (i=0; i<4; i++){
   for (j=0; j< kv_num; j++)
     { ASSERT_OK(Put(i,"foo"+std::to_string(j), "v"+std::to_string(j)));}
  }
  //column get
  cout<<"column get"<<endl;
  for (i=0; i<4; i++){
   for (j=0; j< kv_num; j++)
     { ASSERT_EQ(Get(i,"foo"+std::to_string(j)), "v"+std::to_string(j));}
  }
  //row get
  cout<<"row get"<<endl;
  for (j=0; j< kv_num; j++){
     for (i=0; i<4; i++)
     { ASSERT_EQ(Get(i,"foo"+std::to_string(j)), "v"+std::to_string(j));}
  }
  Close();
 }
*/
//  const uint64_t k32KB = 1 << 15;
//  const uint64_t k64KB = 1 << 16;
//  const uint64_t k128KB = 1 << 17;
//  const uint64_t k1MB = 1 << 20;
//  const uint64_t k4KB = 1 << 12;
TEST_F(CFTest_PUT, SmallWriteBuffer) {
  Open();
  Options options;
  options.env = env_;
  //options.write_buffer_size = 100000;
  options.num_cols = 8;
  options.write_buffer_size = 100;
  db_options_ = options;
  //CreateAndReopenWithCF({"pikachu"}, options);
  CreateColumnFamilies({"pikachu"});
  delete db_;
  TryOpen({"default","pikachu"});
//  ASSERT_OK(Put(1, "foo", "bar"));
  WriteOptions wo;
  wo.sync = true;    //If this flag is true, writes will be slower
//  wo.no_slowdown =true;   //If true and we need to wait or sleep for the write request, fails immediately with Status::Incomplete().
  //Status s = db_->Put(wo, "k1", "v1");
  //ASSERT_OK(s);
  Put(0,"key", std::string(1, 'x'));
  string Key[3] = {"k1","k2","k3"};
  for (int i = 0; i < 10000; i++) {
    Put(1,"key", std::string(i, 'x'));
    //cout<<i<<endl; 
   //Flush();
  }
  Put(1,"key", std::string(2, 'x'));
  ASSERT_EQ("x",Get(0, "key"));
  ASSERT_EQ("xx",Get(1, "key"));
  //cout <<result<<endl;
  Close();
 }
 
}  //end of namespace cf_test


}
