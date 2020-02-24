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
#include "kvdb_test.h"

namespace rocksdb {

namespace cftest_ss {
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

class CFTest_SS : public KVDBTest {
 public:
  CFTest_SS() {
    //env_ = new EnvCounter(Env::Default());
    dbname_ = test::TmpDir() + "/ss_test";
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
    //db_options_.env = env_;
    //insdb::Options insdb_options;  //hainan.liu
    //insdb_options.num_column_count = 5;
    db_options_.num_cols = 8;
    db_ = nullptr;
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
  }


  ~CFTest_SS() {
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->LoadDependency({});
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
    Options options;
    //options.env = env_;
    if (getenv("KEEP_DB")) {
       printf("DB is still at %s\n", dbname_.c_str());
    } else if(db_){
       Close();
       EXPECT_OK(DestroyDB(dbname_, options));
    }
    //Destroy();
    //delete env_;
  }


};
TEST_F(CFTest_SS, 965_ReadFromOneSnapshot_CF) {
    //const std::string snapshot_name_CF = test::TmpDir(env_) + "/snapshotCF";
    Open();
    //CreateColumnFamiliesAndReopen( {"CF1"});
    CreateColumnFamilies({"CF1"});
    std::string key1 = std::string("foo1_CF");
    ASSERT_OK(Put(1,key1, "cf1"));
    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;	
    std::string key2 = std::string("foo2_Def");
    ASSERT_OK(Put(key2, "cf2_def")); 
    //read key foo1 from snapshot
    //roptions.snapshot = ss;
    ASSERT_EQ("cf1", Get(1,"foo1_CF", ss));
    cout << Get(1,"foo1_CF", ss) << endl;	
    ASSERT_EQ("NOT_FOUND", Get("foo2_Def", ss));
    cout << Get("foo2_Def", ss) << endl;	
    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
}
TEST_F(CFTest_SS, 966_ReadFromOneSnapshot_CFs) {
    //const std::string snapshot_name_CF = test::TmpDir(env_) + "/snapshotCF";
    Open();
    //CreateColumnFamiliesAndReopen( {"CF1","CF2"});
    CreateColumnFamilies({"CF1","CF2"});
    std::string key1 = std::string("foo_CF");
    std::string key_new = std::string("foo_CF_new");
    ASSERT_OK(Put(1,key1, "cf1"));
    ASSERT_OK(Put(2,key1, "cf2"));
    // Take a snapshot
    const Snapshot *ss1 = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss1->GetSequenceNumber() << endl;
    ASSERT_EQ("cf1", Get(1,"foo_CF", ss1));
    ASSERT_EQ("cf2", Get(2,"foo_CF", ss1));

    ASSERT_OK(Put(0,key1, "cf0"));
    ASSERT_OK(Put(1,key1, "cf11"));    
    ASSERT_OK(Put(2,key_new, "cf2"));
    const Snapshot *ss2 = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss2->GetSequenceNumber() << endl;
    ASSERT_EQ("cf1", Get(1,"foo_CF", ss1));
    ASSERT_EQ("cf11", Get(1,"foo_CF", ss2));
    ASSERT_EQ("cf0", Get(0,"foo_CF", ss2));
    ASSERT_EQ("cf2", Get(2,"foo_CF_new", ss2));


    db_->ReleaseSnapshot(ss1);
    db_->ReleaseSnapshot(ss2);
    ClearEnv(db_);
}
TEST_F(CFTest_SS, 967_UpdateKeyThenReadFromSnapshot_CFs) {
    //const std::string snapshot_name_CF = test::TmpDir(env_) + "/snapshotCF";
    Open();
    //CreateColumnFamiliesAndReopen( {"CF1","CF2"});
    CreateColumnFamilies( {"CF1","CF2"});
    std::string key1 = std::string("foo_CF");
    ASSERT_OK(Put(1,key1, "cf1"));
    ASSERT_OK(Put(2,key1, "cf2"));
    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    //update the values of key
    ASSERT_OK(Put(1,key1, "cf11"));
    ASSERT_OK(Put(2,key1, "cf22"));

    ASSERT_EQ("cf1", Get(1,"foo_CF", ss));
    ASSERT_EQ("cf2", Get(2,"foo_CF", ss));
    ASSERT_EQ("cf11", Get(1,"foo_CF"));
    ASSERT_EQ("cf22", Get(2,"foo_CF"));
    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 968_GetSnapshot) {
    Open();
    std::string key1 = std::string("foo1");
    ASSERT_OK(Put(key1, "v1"));
    std::string key2 = std::string("foo2");
        ASSERT_OK(Put(key2, "v2"));

    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
}


TEST_F(CFTest_SS, DISABLED_GetSnapshotNum) {
    Open();
    std::string key1 = std::string("foo3");
    ASSERT_OK(Put(key1, "v3"));
    std::string key2 = std::string("foo4");
        ASSERT_OK(Put(key2, "v4"));

    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
    EXPECT_TRUE(db_->GetIntProperty("rocksdb.num-snapshots", 1));
        //read key foo1 from snapshot
        //roptions.snapshot = ss;
        ASSERT_EQ("v3", Get("foo3", ss));
    cout << Get("foo3", ss) << endl;
    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 1198_TwoSnapshotSameTime) {
    Open();
    std::string key1 = std::string("foo3");
    ASSERT_OK(Put(key1, "v3"));
    std::string key2 = std::string("foo4");
        ASSERT_OK(Put(key2, "v4"));

    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);

    const Snapshot *ss1 = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss1->GetSequenceNumber() << endl;
    assert(ss1);

        //read key foo1 from snapshot
        //roptions.snapshot = ss;
    ASSERT_EQ("v3", Get("foo3", ss));
    ASSERT_EQ("v3", Get("foo3", ss1));
    cout << Get("foo3", ss) << endl;
    db_->ReleaseSnapshot(ss);
    db_->ReleaseSnapshot(ss1);
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 1199_LargeAmountSnapshot) {
    Open();
    std::string key1 = std::string("foo3");
    ASSERT_OK(Put(key1, "v3"));
    std::string key2 = std::string("foo4");
        ASSERT_OK(Put(key2, "v4"));
    int i; 
    //std::string ss;
    const Snapshot *ss[1024];
    // Take a snapshot
    for (i=0; i<1024; i++)
    {
    ss[i] = db_->GetSnapshot();
    //cout << "Snapshot: seq " << ss[i]->GetSequenceNumber() << endl;
    assert(ss[i]);
    }
    //EXPECT_TRUE(db_->GetIntProperty("rocksdb.num-snapshots", 1));
        //read key foo1 from snapshot
        //roptions.snapshot = ss;
    ASSERT_EQ("v3", Get("foo3", ss[0]));
    cout << Get("foo3", ss[0]) << endl;

   for (i=0; i<1024; i++){
      db_->ReleaseSnapshot(ss[i]);
    }
    ClearEnv(db_);
}



TEST_F(CFTest_SS, 969_ReadFromOneSnapshot) {
    Open();
    std::string key1 = std::string("foo3");
    ASSERT_OK(Put(key1, "v3"));
    std::string key2 = std::string("foo4");
        ASSERT_OK(Put(key2, "v4"));

    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
        //read key foo1 from snapshot
        //roptions.snapshot = ss;
        ASSERT_EQ("v3", Get("foo3", ss));
    cout << Get("foo3", ss) << endl;
    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
}

//release之后不允许使用
TEST_F(CFTest_SS, DISABLED_970_ReleaseSnapshotThenRead) {
    ReadOptions roptions;
    Open();
    std::string key1 = std::string("foo3");
    ASSERT_OK(Put(key1, "v3"));
    std::string key2 = std::string("foo4");
        ASSERT_OK(Put(key2, "v4"));

    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
    //cout <<"The foo3 before release is :"<< Get("foo3", ss) << endl;
    db_->ReleaseSnapshot(ss);
    //read key foo1 from snapshot
    ASSERT_EQ("v3", Get("foo3", ss));
    cout << "The foo3 after release is :"<<Get("foo3", ss) << endl;
    ASSERT_EQ("v3", Get("foo3"));
    ASSERT_EQ("v4", Get("foo4"));
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 971_Create3SnapshotAndRead) {
    ReadOptions roptions;
    Open();
    std::string key1 = std::string("foo3");
    ASSERT_OK(Put(key1, "v3"));
    std::string key2 = std::string("foo4");
    ASSERT_OK(Put(key2, "v4"));
    // Take a snapshot
    const Snapshot *ss1 = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss1->GetSequenceNumber() << endl;
    std::string key3 = std::string("foo5");
    ASSERT_OK(Put(key3, "v5"));
        const Snapshot *ss2 = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss2->GetSequenceNumber() << endl;
        std::string key4 = std::string("foo6");
    ASSERT_OK(Put(key4, "v6"));
    const Snapshot *ss3 = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss3->GetSequenceNumber() << endl;
    //read key foo1 from snapshot1
//    roptions.snapshot = ss1;
    ASSERT_EQ("v3", Get("foo3", ss1));
    cout << Get("foo3", ss1) << endl;
    ASSERT_EQ("v4", Get("foo4", ss1));
    cout << Get("foo4", ss1) << endl;
    ASSERT_EQ("NOT_FOUND", Get("foo5", ss1));
    cout << Get("foo5", ss1) << endl;
    ASSERT_EQ("NOT_FOUND", Get("foo6", ss1));
    cout << Get("foo6", ss1) << endl;
    //read key foo5 from snapshot2
//    roptions.snapshot = ss2;
    ASSERT_EQ("v3", Get("foo3", ss2));
    cout << Get("foo3", ss2) << endl;
    ASSERT_EQ("v4", Get("foo4", ss2));
    cout << Get("foo4", ss2) << endl;
    ASSERT_EQ("v5", Get("foo5", ss2));
    cout << Get("foo5", ss2) << endl;
    ASSERT_EQ("NOT_FOUND", Get("foo6", ss2));
    cout << Get("foo6", ss2) << endl;
    //read key foo6 from snapshot3
//    roptions.snapshot = ss3;
    ASSERT_EQ("v3", Get("foo3", ss3));
    cout << Get("foo3", ss3) << endl;
    ASSERT_EQ("v4", Get("foo4", ss3));
    cout << Get("foo4", ss3) << endl;
    ASSERT_EQ("v5", Get("foo5", ss3));
    cout << Get("foo5", ss3) << endl;
    ASSERT_EQ("v6", Get("foo6", ss3));
    cout << Get("foo6", ss3) << endl;

    db_->ReleaseSnapshot(ss1);
    db_->ReleaseSnapshot(ss2);
    db_->ReleaseSnapshot(ss3);
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 972_SnapshotOptionNull) {
    ReadOptions roptions;
    std::string result;
//    cout << "DB open " << endl;
    Open();
    std::string key1 = std::string("foo1");
    ASSERT_OK(Put(key1, "v1"));
    std::string key2 = std::string("foo2");
    ASSERT_OK(Put(key2, "v2"));
    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
    ASSERT_OK(Put(key1, "v111"));
    ASSERT_OK(Put(key2, "v222"));
    //read key foo1 from snapshot1
    roptions.snapshot = nullptr;
    Status s = db_->Get(roptions, "foo1", &result);
    cout <<"DB Get NULL Snapshot " << s.ToString()  << endl;
    cout <<"the foo1 value is  " << result  << endl;
    ASSERT_EQ("v111", result);
    //change the value of foo1
    ASSERT_OK(Put("foo1", "vv"));
    s = db_->Get(roptions, "foo2", &result);
    cout <<"DB Get NULL Snapshot " << s.ToString()  << endl;
    cout <<"the foo2 value is  " << result  << endl;
    ASSERT_EQ("v222", result);
    ASSERT_EQ("v1",Get("foo1", ss));
    ASSERT_EQ("v2",Get("foo2", ss));
    db_->ReleaseSnapshot(ss);
    //restore the foo1 value
    ASSERT_OK(Put("foo1", "v1"));
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 973_UpdateKeyThenReadFromSnapshot) {
//    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
//    cout << "DB open " << endl;
    Open();
    std::string key1 = std::string("foo1");
    ASSERT_OK(Put(key1, "v1"));
        // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
            //get the foo1 from db
    Status s = db_->Get(roptions, "foo1", &result);
    ASSERT_EQ("v1", result);
    cout << "foo1 in the db is " << result << endl;
        //PUT foo1 and foo2
    std::string key111 = std::string("foo1");
    ASSERT_OK(Put(key111, "vvv"));
            //get the foo1 from db again
    s = db_->Get(roptions, "foo1", &result);
    ASSERT_EQ("vvv", result);
    cout << "foo1 in the db is " << result << endl;
    //get foo1 from the snapshot
    ASSERT_EQ("v1", Get("foo1", ss));
    cout << "foo1 in the snapshot is " << Get("foo1", ss) << endl;
    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 974_PutNewKVAfterSnapshot) {
//    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
//    ASSERT_OK(DB::Open(options, snapshot_name, &db_));
//    cout << "DB open " << endl;
    Open();
     //get the foo1 from db
    ASSERT_EQ("NOT_FOUND", Get("foo7"));
    cout << "foo7 in the db is " << Get("foo7") << endl;
        //PUT foo7 and foo8
    std::string key7 = std::string("foo7");
    ASSERT_OK(Put(key7, "v7"));
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;

    std::string key8 = std::string("foo8");
    ASSERT_OK(Put(key8, "v8"));
            //get the foo7,foo8 from db
    Status s = db_->Get(roptions, "foo7", &result);
    ASSERT_EQ("v7", result);
    cout << "foo7 in the db is " << result << endl;
    s = db_->Get(roptions, "foo8", &result);
    ASSERT_EQ("v8", result);
    cout << "foo8 in the db is " << result << endl;
    //get foo7 from the snapshot
    result = Get("foo7", ss);
    ASSERT_EQ("v7", result);
    cout << "foo7 in the snapshot is " << result << endl;
    result = Get("foo8", ss);
    ASSERT_EQ("NOT_FOUND", result);
    cout << "foo8 in the snapshot is " << result << endl;

    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
//    EXPECT_OK(DestroyDB(snapshot_name, options));
}
                                           
TEST_F(CFTest_SS, 975_SnapshotDeleteGet) {
//    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
//    cout << "DB open " << endl;
    Open();
    //PUT foo9
    std::string key9 = std::string("foo9");
    ASSERT_OK(Put(key9, "v9"));
    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
            //get the foo9 from db
    Status s = db_->Get(roptions, "foo9", &result);
    ASSERT_EQ("v9", result);
    cout << "foo9 in the db is " << result << endl;
//    ASSERT_OK(Delete("foo9"));
   Status status_delete = db_->Delete(WriteOptions(), "foo9");
    cout << "The deletion is " << status_delete.ToString() << endl;
            //get the foo9 from db
    s = db_->Get(roptions, "foo9", &result);
    ASSERT_EQ(1, s.IsNotFound());
    cout << "foo9 in the db is " << s.ToString() << endl;
    //get foo9 from the snapshot
    ASSERT_EQ("v9", Get("foo9", ss));
    cout << "foo9 in the snapshot is " << Get("foo9", ss) << endl;
    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 976_ReadUpdatedKeyFromRealesedSnapshot) {
//    DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
//    cout << "DB open " << endl;
    Open();
        //PUT foo11
    std::string key11 = std::string("foo11");
    ASSERT_OK(Put(key11, "v11"));
        // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
        //get the foo11 from db
    Status s = db_->Get(roptions, "foo11", &result);
    ASSERT_EQ("v11", result);
    cout << "foo11 in the db is " << result << endl;
    //get foo11 from the snapshot
    ASSERT_EQ("v11", Get("foo11", ss));
    cout << "foo11 in the snapshot is " << Get("foo11", ss) << endl;
     //edit the value of foo11
    ASSERT_OK(Put(key11, "v1111"));
    ASSERT_EQ("v1111", Get("foo11"));
    ASSERT_EQ("v11", Get("foo11", ss));
    //release snapshot;
    db_->ReleaseSnapshot(ss);
    //ASSERT_TRUE(ss == nullptr);
    //get foo11 from the released snapshot
    cout << "ss is " << ss << endl;
    cout << "foo11 in the released snapshot is " << Get("foo11", ss) << endl;
    ASSERT_EQ("v11", Get("foo11", ss));
        cout <<"tets is ok"<<endl;
        ASSERT_EQ("v1111", Get("foo11"));
    ClearEnv(db_);
 //   EXPECT_OK(DestroyDB(snapshot_name, options));
}

TEST_F(CFTest_SS, 1141_SnapshotEmptyDB) {
//  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    const std::string snapshot_name_empty = test::TmpDir() + "/snapshotEmpty";
    //DB* snapshotDB;
    ReadOptions roptions;
    std::string result;
    //Checkpoint* checkpoint;
    //ClearEnv(db_);
    //delete db_;
    DB* db_empty_ = nullptr;
    options.create_if_missing = true;
    //Open a database
    //EXPECT_OK(DestroyDB(snapshot_name_empty, options));
    ASSERT_OK(DB::Open(options, snapshot_name_empty, &db_empty_));
    cout << "DB open " << endl;
        // Take a snapshot
    const Snapshot *ss = db_empty_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
        //PUT foo1 and foo2
    std::string key1 = std::string("foo1");
    ASSERT_OK(db_empty_->Put(WriteOptions(), "foo1", "v1"));
    std::string key2 = std::string("foo2");
    ASSERT_OK(db_empty_->Put(WriteOptions(), "foo2", "v2"));
    //get the foo1 from db
    Status s = db_empty_->Get(roptions, "foo1", &result);
    ASSERT_EQ("v1", result);
    cout << "foo1 in the db is " << result << endl;

    //get foo1 from the snapshot
    roptions.snapshot = ss;
    s = db_empty_->Get(roptions, "foo1", &result);
    cout << "foo1 in the snapshot is " << result << endl;
    //ASSERT_EQ("NOT_FOUND",s.ToString());
    ASSERT_TRUE(s.IsNotFound());
    db_empty_->ReleaseSnapshot(ss);
    ClearEnv(db_empty_);
    //delete db_;
    EXPECT_OK(DestroyDB(snapshot_name_empty, options));
}   //end of TEST_F

TEST_F(CFTest_SS, 1200_TwoSnapshotOneHandle) {
    ReadOptions roptions;
    Open();
    std::string key1 = std::string("foo3");
    ASSERT_OK(Put(key1, "v3"));
    std::string key2 = std::string("foo4");
    ASSERT_OK(Put(key2, "v4"));

    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
    ASSERT_EQ("v3", Get("foo3", ss));
    //cout <<"The foo3 before release is :"<< Get("foo3", ss) << endl;
//    db_->ReleaseSnapshot(ss);
    ASSERT_OK(Put(key1, "v33"));
    ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
    //read key foo1 from snapshot
    //roptions.snapshot = ss;
    ASSERT_EQ("v33", Get("foo3", ss));
    cout << "The foo3 after release is :"<<Get("foo3", ss) << endl;
    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 1201_ReleaseThenCreate) {
    ReadOptions roptions;
    Open();
    std::string key1 = std::string("foo3");
    ASSERT_OK(Put(key1, "v3"));
    std::string key2 = std::string("foo4");
    ASSERT_OK(Put(key2, "v4"));

    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
    ASSERT_EQ("v3", Get("foo3", ss));
    //cout <<"The foo3 before release is :"<< Get("foo3", ss) << endl;
    db_->ReleaseSnapshot(ss);
    ASSERT_OK(Put(key1, "v33"));
    ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
    //read key foo1 from snapshot
    //roptions.snapshot = ss;
    ASSERT_EQ("v33", Get("foo3", ss));
    cout << "The foo3 after release is :"<<Get("foo3", ss) << endl;
    db_->ReleaseSnapshot(ss);
    ClearEnv(db_);
}

TEST_F(CFTest_SS, 1202_MergeTest) {
    ReadOptions roptions;
    Open();
    ColumnFamilyOptions default_cf;
    default_cf.merge_operator = MergeOperators::CreateStringAppendOperator();
    Reopen({default_cf});
    //delete db_;
    //TryOpen({"default"}, {default_cf});
    std::string key1 = std::string("foo3");
    ASSERT_OK(Put(key1, "v3"));
    std::string key2 = std::string("foo4");
    ASSERT_OK(Put(key2, "v4"));

    // Take a snapshot
    const Snapshot *ss = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    assert(ss);
    ASSERT_EQ("v3", Get("foo3", ss));
    ASSERT_EQ("v4", Get("foo4", ss));
    //cout <<"The foo3 before release is :"<< Get("foo3", ss) << endl;
    
//    ColumnFamilyOptions default_cf;
//    default_cf.merge_operator = MergeOperators::CreateStringAppendOperator();
//    Reopen({default_cf});
    ASSERT_OK(Merge(0, key1, "v33"));
    const Snapshot *ss1 = db_->GetSnapshot();
    cout << "Snapshot: seq " << ss1->GetSequenceNumber() << endl;
    assert(ss1);
    //read key foo1 from snapshot
    //roptions.snapshot = ss;
    ASSERT_EQ("v3", Get("foo3", ss));
    ASSERT_EQ("v3,v33", Get("foo3", ss1));
    db_->ReleaseSnapshot(ss);
    db_->ReleaseSnapshot(ss1);
    ClearEnv(db_);
}

}//namespace cftest


}  // namespace rocksdb



