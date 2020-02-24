/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file kvdb_open.cc
  *  \brief Test cases code of the KVDB open().
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
#include "rocksdb/utilities/options_util.h"   //hainan.liu
#include "kvdb_test.h"

using namespace std;

namespace rocksdb {

namespace cftest_open {

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
class CFTest_OPEN: public KVDBTest {
 public:
  CFTest_OPEN() {
    env_ = new EnvCounter(Env::Default());
    dbname_ = test::TmpDir() + "/open_test";
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
    db_options_.env = env_;
    db_options_.num_cols = 8; 
    //DestroyDB(dbname_, Options(db_options_, column_family_options_));
    
    Options options;
    DestroyDB(dbname_,options);
    db_ = nullptr;
    }


  ~CFTest_OPEN() {
  rocksdb::SyncPoint::GetInstance()->LoadDependency({});
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
  cout << "---------------~DBOpenTest--------------------"<< endl;
  Options options;
    options.env = env_;

  if(db_)
   {
    Destroy();
  }
//  if (getenv("KEEP_DB")) {
//    printf("DB is still at %s\n", dbname_.c_str());
//  } else if(db_){
//    EXPECT_OK(DestroyDB(dbname_, options));
//    } 
  delete env_;
}
};
//OpenIsOk
TEST_F(CFTest_OPEN, 1139_OpenWithoutCF){  
  cout << "open" <<endl;
  Options option_test;
  option_test.create_if_missing = true;
  option_test.fail_if_options_file_error = true;
  option_test.env = env_;
  ColumnFamilyOptions column_family_options_test;
  std::string dbname_test=test::TmpDir() + "/NoCF_test";;
  DB* db_test = nullptr;
  Status s = DB::Open(option_test,dbname_test,&db_test);
  ASSERT_OK(s);
  cout << "opened" <<endl;
  //CreateColumnFamilies({"one"});
  delete db_test;
  ASSERT_OK(DestroyDB(dbname_test, Options(option_test, column_family_options_test)));
}

TEST_F(CFTest_OPEN, 1137_OpenMultiDB){
  Open();
  Options option_test;
  option_test.create_if_missing = true;
  option_test.fail_if_options_file_error = true;
  option_test.env = env_;
  ColumnFamilyOptions column_family_options_test;
  std::string multi_dbname= test::TmpDir() + "/multiDB_test";
  DB* multi_db_ = nullptr;
  Status s = DB::Open(option_test,multi_dbname,&multi_db_);
  ASSERT_OK(s);
  delete multi_db_;
  ASSERT_OK(DestroyDB(multi_dbname, Options(option_test, column_family_options_test)));
}

TEST_F(CFTest_OPEN, 1138_DefaultCFNum){
  cout << "open" <<endl;
  constexpr int kNumCF = 2;
  Options option_test;
  option_test.create_if_missing = true;
  option_test.fail_if_options_file_error = true;
  option_test.env = env_;
  std::string dbname_test= test::TmpDir() + "/defaultCF_test";
  DB* db_test = nullptr;
  ColumnFamilyOptions column_family_options_test;
  std::vector<ColumnFamilyDescriptor> column_families_test;
  std::vector<std::string> names_test;
  std::vector<ColumnFamilyHandle*> handles_test;
  //const std::vector<std::string>& cfs_test={"one"};
  DB::Open(option_test,dbname_test,&db_test);
  cout << "opened" <<endl;
  for (int i = 1; i <= kNumCF; i++) {
    names_test.push_back("cf1-" + ToString(i));
  }
  Status s = db_test->CreateColumnFamilies(column_family_options_test, names_test, &handles_test);
  cout << "created "+s.ToString() <<endl;
  delete db_test;
  column_families_test.push_back(ColumnFamilyDescriptor({"default"},column_family_options_test));
  for (size_t i = 0; i < names_test.size(); ++i) {
      column_families_test.push_back(ColumnFamilyDescriptor(names_test[i],column_family_options_test));
     // names_test.push_back([i]);
    }
  s = DB::Open(option_test, dbname_test, column_families_test, &handles_test, &db_test);
  cout << "The open status is " <<endl;
  cout << s.ToString() <<endl;
  //ASSERT_OK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  db_test = nullptr;
  ASSERT_OK(DestroyDB(dbname_test, Options(option_test, column_family_options_test)));
 }



TEST_F(CFTest_OPEN, 893_NullNameCF){
  cout << "open" <<endl;
  Open();
  cout << "opened" <<endl;
  CreateColumnFamilies({"one",""});
  delete db_;
  Status s = TryOpen({"default","one",""});
  cout << "The open status is " <<endl;
  cout << s.ToString() <<endl;
  ASSERT_OK(s);  
  Close();
 }


TEST_F(CFTest_OPEN, 894_CreateNullCFAndNotOpen){
  Open();
  CreateColumnFamilies({"one",""});
  delete db_;
  Status s = TryOpen({"one","default"});
  cout << s.ToString() <<endl;
  //Invalid argument: You have to open all column families. Column families not opened:one
  ASSERT_TRUE(s.IsInvalidArgument());
  TryOpen({"default","one",""});
  Close(); 
}

TEST_F(CFTest_OPEN, 895_OpenWithInvalidCF){
  Options options ;
  Status s = TryOpen({"default","Invalid_CF"});
  cout << s.ToString() <<endl;
  ASSERT_TRUE(s.IsInvalidArgument());  
}


TEST_F(CFTest_OPEN, 896_closeBeforeOpen){
  delete db_;
  Open();
  Close();
}

TEST_F(CFTest_OPEN, 1063_DropCFAndReopen){
  Open();
  Options options ;
  CreateColumnFamilies({"one","two","three","four"});
  delete db_;
  Status s = TryOpen({"default","one","two","three","four"});
  ASSERT_OK(s);
  cout << s.ToString() <<endl;
  DropColumnFamilies({2, 4});
  delete db_;
  s = TryOpen({"default","one","three"});
  ASSERT_OK(s);
  Close();
}



 //should fail???
TEST_F(CFTest_OPEN, 897_OpenWith2Of4CF){
  Open();
  CreateColumnFamilies({"one","two","three","four"});
  delete db_;
  Status s=TryOpen({"default","one","two"});
  cout <<s.ToString()<<endl;
  ASSERT_TRUE(s.IsInvalidArgument());
  TryOpen({"default","one","two","three","four"});
  Close();
 }

TEST_F(CFTest_OPEN, 898_OpenWithoutDefaultCF){
  Open();
  CreateColumnFamilies({"one","two","three","four"});
  Close();
  Status s=TryOpen({"one","two","three","four"});
  cout <<s.ToString()<<endl;
  //ASSERT_OK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(s.ToString(), "Invalid argument: Default column family not specified");
  Close();
 }

TEST_F(CFTest_OPEN, 899_CreateDefaultCF){
  Open();
//  CreateColumnFamilies({"default"});
  int cfi = static_cast<int>(handles_.size());
  handles_.resize(cfi + 1);
  Status s = db_->CreateColumnFamily(column_family_options_, "default", &handles_[cfi]);
  cout <<s.ToString()<<endl; //Invalid argument: duplicate column name
  ASSERT_EQ(s.ToString(), "Invalid argument: duplicate column name");  
  Close();
 }

TEST_F(CFTest_OPEN, 1230_CreateExistCF){
  Open();
  CreateColumnFamilies({"one"});
  int cfi = static_cast<int>(handles_.size());
  handles_.resize(cfi + 1);
  Status s = db_->CreateColumnFamily(column_family_options_, "one", &handles_[cfi]);
  cout <<s.ToString()<<endl; //Invalid argument: duplicate column name
  ASSERT_EQ(s.ToString(), "Invalid argument: duplicate column name");
  Close();
 }


 
 TEST_F(CFTest_OPEN, 900_OpenMaxCF){
 // db_options_=options;
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
  Close();
 }

 
TEST_F(CFTest_OPEN, 901_OpenMaxMoreCF){
  //Options options = CurrentOptions();
  Open();
  std::vector<std::string> cf_names;
  constexpr int kNumCF = 7;
  for (int i = 1; i <= kNumCF; i++) {
    cf_names.push_back("cf1-" + ToString(i));
  }
  CreateColumnFamilies(cf_names);
  int cfi = static_cast<int>(handles_.size());
  handles_.resize(cfi + cf_names.size()+1);
  Status s = db_->CreateColumnFamily(column_family_options_, "cf-8", &handles_[cfi]);
  cout <<s.ToString()<<endl;
  //IO error: No space left on deviceReached maximum of column families
  ASSERT_EQ(s.ToString(), "IO error: No space left on deviceReached maximum of column families");
  Close();
 }

TEST_F(CFTest_OPEN, 902_ResetNumCol){
  Open();
  CreateColumnFamilies({"one","two"});
  delete db_;
  db_=nullptr;
  Options options;
  options.num_cols = 3;
  options.create_if_missing = true;
  options.env = env_;
  db_options_=options;
  Status s=TryOpen({"one","two","default"});
  cout <<s.ToString()<<endl;    //column count mismatched
  ASSERT_NE(strstr(s.ToString().c_str(), "column count mismatched"),NULL);
  ASSERT_EQ(db_,nullptr);
  //Close();
 }


TEST_F(CFTest_OPEN, 904_No_Create_If_Missing){
  Options option_test;
  option_test.create_if_missing = false;
  option_test.fail_if_options_file_error = true;
  option_test.env = env_;
  std::string dbname_test= test::TmpDir() + "/defaultCF_test";
  DB* db_test = nullptr;
  ColumnFamilyOptions column_family_options_test;
  std::vector<ColumnFamilyDescriptor> column_families_test;
  std::vector<std::string> names_test;
  std::vector<ColumnFamilyHandle*> handles_test;
  //const std::vector<std::string>& cfs_test={"one"};
  Status s = DB::Open(option_test,dbname_test,&db_test);
  cout << s.ToString() <<endl;  //db does not exists
  ASSERT_NE(strstr(s.ToString().c_str(), "db does not exists"),NULL);
  ASSERT_EQ(db_test,nullptr);
}
//OpenWithDBNameNull
TEST_F(CFTest_OPEN, 905_OpenWithDBNameNull){
  Options option_test;
  option_test.create_if_missing = true;
  option_test.fail_if_options_file_error = true;
  option_test.env = env_;
  std::string dbname_test= "";
  DB* db_test = nullptr;
  ColumnFamilyOptions column_family_options_test;
  std::vector<ColumnFamilyDescriptor> column_families_test;
  std::vector<std::string> names_test;
  std::vector<ColumnFamilyHandle*> handles_test;
  //const std::vector<std::string>& cfs_test={"one"};
  Status s = DB::Open(option_test,dbname_test,&db_test);
  cout << s.ToString() <<endl;  //IO error: While mkdir if missing: No such file or directory
  //ASSERT_OK(s); 
  ASSERT_NE(strstr(s.ToString().c_str(), "While mkdir if missing"),NULL);

}

TEST_F(CFTest_OPEN, 1172_DestroyEmptyDB){
  cout << "open" <<endl;
  Open();
  cout << "opened" <<endl;
  Close();
 }

TEST_F(CFTest_OPEN, 1173_DestroyEmptyDB_2){
  cout << "open" <<endl;
  //Open();
  cout << "opened" <<endl;
  //Close();
  DestroyDB(dbname_,options);
 }


TEST_F(CFTest_OPEN, 906_OpenAgain){
  Open();
  delete db_;
  Open();
  Close();
}

TEST_F(CFTest_OPEN, 907_OpenTwice){
  Open();
  //delete db_;
  Options options;
  options.num_cols = 8;
  options.create_if_missing = true;
  Status s = DB::Open(options, dbname_, &db_);
  cout << s.ToString() <<endl;   //db exists but corrupted
  ASSERT_NE(strstr(s.ToString().c_str(), "db exists but corrupted"),NULL);  
  ASSERT_NE(db_,nullptr);
  Close();
}

TEST_F(CFTest_OPEN, 908_ErrorIfExists){
  Open();
  delete db_;
  Options options;
  options.create_if_missing = false;
  options.error_if_exists = true;
  options.num_cols = 8;
  Status s = DB::Open(options, dbname_, &db_);
  cout << s.ToString() <<endl;   //db exists but corrupted
  ASSERT_NE(strstr(s.ToString().c_str(), "db exists (error_if_exists is true)"),NULL);
  ASSERT_NE(db_,nullptr);
  //Close();

  //DestroyDB(dbname_,options);
  //db_=nullptr;
  options.create_if_missing = false;
  options.error_if_exists = false;
  options.num_cols = 8;
  s = DB::Open(options, dbname_, &db_);
  cout << s.ToString() <<endl;   //db exists but corrupted
  ASSERT_OK(s); 
  Close();

}

TEST_F(CFTest_OPEN, 909_OpenWithNonExistPath){
  //Open();
  Options option_test;
  option_test.create_if_missing = true;
  option_test.fail_if_options_file_error = true;
  option_test.env = env_;
  std::string dbname_test= test::TmpDir() + "/nonexist/test";
  DB* db_test = nullptr;
  //const std::vector<std::string>& cfs_test={"one"};
  Status s = DB::Open(option_test,dbname_test,&db_test);
  cout << s.ToString() <<endl;  //db does not exists
  ASSERT_NE(strstr(s.ToString().c_str(), "While mkdir if missing"),NULL);
  ASSERT_EQ(db_,nullptr);
}

//"1 Aa&^%$#@!~&*()+|?/':-,.?测试
TEST_F(CFTest_OPEN, 910_OpenWithSpecialPath){
  //Open();
  Options option_test;
  option_test.create_if_missing = true;
  option_test.fail_if_options_file_error = true;
  option_test.env = env_;
  std::string dbname_test= test::TmpDir() + "/1 Aa&^%$#@!~&*()+|?/':-,.?测试";
  DB* db_test = nullptr;
  //const std::vector<std::string>& cfs_test={"one"};
  Status s = DB::Open(option_test,dbname_test,&db_test);
  cout << s.ToString() <<endl;  //db does not exists
  ASSERT_NE(strstr(s.ToString().c_str(), "While mkdir if missing"),NULL);
  ASSERT_EQ(db_,nullptr);
}

TEST_F(CFTest_OPEN, 911_OpenWithCF){
     Options options;
     DB* db = NULL;
     options.create_if_missing = true;
     options.create_missing_column_families = true;
     std::vector<ColumnFamilyHandle*> handles;
     std::vector<ColumnFamilyDescriptor> column_families;
     std::vector<std::string> column_families_names;
     //std::string path = test::TmpDir()+"/open_test";
     //std::string path = "/tmp/OpenWithCF";
     //options.num_column_count=6; 
    // insdb::Options insdb_options;
    // insdb_options.num_column_count = 6;
    // List column
    std::string path = dbname_;
    Status s = DB::Open(options, path, &db);
    delete db;
    column_families_names.clear();
    Status status = DB::ListColumnFamilies(static_cast<DBOptions>(options), path, &column_families_names);
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
    //load options 
     DBOptions dboptions(options);
     status = LoadLatestOptions(path, rocksdb::Env::Default(), &options,
                            &column_families, false);
     cout << "LoadLatestOptions " << status.ToString() << endl;

     //open with default cf
     status = DB::Open(options, path, column_families, &handles, &db);
     cout << "DB open with default column family " << status.ToString() << endl;

     db->DestroyColumnFamilyHandle(handles[0]);
     delete db;
     cout << "DB closed" << endl;

     //open with cf one
     ColumnFamilyOptions cfoptions;
     ColumnFamilyDescriptor one("one", cfoptions);
     column_families.push_back(one);
     options.create_missing_column_families = true;
     //options.num_cols = 5;
     status = DB::Open(options, path, column_families, &handles, &db);
     cout << "DB open with column family one:" << status.ToString() << endl; 
     //if (status.code() != Status::Code::kOk)
     //   return FAILURE;
     ASSERT_OK(status);
     ColumnFamilyDescriptor two("two", cfoptions);
     column_families.push_back(two);
     //options.num_column_count = 6;
     options.create_missing_column_families = true;
     status = DB::Open(options, path, column_families, &handles, &db);
     cout << "DB open with column family two:" << status.ToString() << endl;
     //if (status.code() != Status::Code::kOk)
     //   return FAILURE;
     ASSERT_OK(status);
         // Delete cf1
     status = db->DropColumnFamily(handles[1]);
     //assert(status.ok());
     cout << "cf one is deleted " << status.ToString() << endl;
     ///if (status.code() != Status::Code::kOk)
     //   return FAILURE;
     ASSERT_OK(status);
     status = db->DestroyColumnFamilyHandle(handles[1]);
     cout << "cf one is destroyed " << status.ToString() << endl; 
     //if (status.code() != Status::Code::kOk)
     //   return FAILURE;   
     ASSERT_OK(status);
     status = db->DropColumnFamily(handles[2]);
     //assert(status.ok());
     cout << "cf two is deleted " << status.ToString() << endl;
     //if (status.code() != Status::Code::kOk)
     //   return FAILURE;
     ASSERT_OK(status);
     status = db->DestroyColumnFamilyHandle(handles[2]);
     cout << "cf two is destroyed " << status.ToString() << endl;
     //if (status.code() != Status::Code::kOk)
     //   return FAILURE;
     ASSERT_OK(status);
     delete db;
     cout << "DB closed" << endl;
     DestroyDB(path, options);
}     



}

}

