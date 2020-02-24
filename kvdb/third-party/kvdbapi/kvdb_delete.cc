/*! \file kvdb_delete.cc
 *  \brief Test cases code of the KVDB delete feature.
*/
#include "kvdb_test.h"
#include<time.h>
using namespace std;

namespace rocksdb {

class DeleteTest: public KVDBTest {
public:
   Options options;
public:
  DeleteTest() {
    dbname_ = test::TmpDir() + "/delete_test";
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
    
  }
  ~DeleteTest() {
    Destroy();
  }
};
/********************************************************************************************/
/******************************************Test cases****************************************/
/********************************************************************************************/
/*
 * @brief     Use DeleteKeyNormal to test delete KV
 * @param     DeleteTest --- Class name. \n
 *            DeleteKeyNormal --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteKeyNormal){
    Open();
    std::string testkey(NORMALKEY, '1');
    std::string testvalue(NORMALVALUE,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_EQ(testvalue, Get(testkey));
    ASSERT_OK(Delete(testkey));
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }
 /**
 * @brief     Use DeleteNullKey to test delete the key which has 0 byte
 * @param     DeleteTest --- Class name. \n
 *            DeleteKey15B --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteNullKey){
    Open();
    std::string testkey(0, '1');
    std::string testvalue(NORMALVALUE,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_EQ(testvalue, Get(testkey));
    ASSERT_OK(Delete(testkey));
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }
 
/**
 * @brief     Use DeleteLessMaxKey to test delete the key which has 39 byte
 * @param     DeleteTest --- Class name. \n
 *            DeleteLessMaxKey --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteLessMaxKey){
    Open();
    std::string testkey(39, '1');
    std::string testvalue(NORMALVALUE,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_EQ(testvalue, Get(testkey));
    ASSERT_OK(Delete(testkey));
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }
 /**
 * @brief     Use DeleteMaxKey to test delete the key which has 40 byte
 * @param     DeleteTest --- Class name. \n
 *            DeleteMaxKey --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteDefaultMaxKey){
    Open();
    std::string testkey(40, '1');
    std::string testvalue(NORMALVALUE,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_EQ(testvalue, Get(testkey));
    ASSERT_OK(Delete(testkey));
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }
/**
 * @brief     Use DeleteMoreMaxKey to test delete the key which has 41 byte
 * @param     DeleteTest --- Class name. \n
 *            DeleteMoreMaxKey --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteMoreMaxKey){
    Open();
    std::string testkey(41, '1');
    std::string testvalue(NORMALVALUE,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_EQ(testvalue, Get(testkey));
    ASSERT_OK(Delete(testkey));
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }
/**
 * @brief     Use DeleteSetMaxKey to test delete the key which has 255 byte
 * @param     DeleteTest --- Class name. \n
 *            DeleteSetMaxKey --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*
TEST_F(DeleteTest, DeleteSetMaxKey){
    Open();
    options.max_key_size = 255;
    Reopen(options);
    std::string testkey(255, '1');
    std::string testvalue(NORMALVALUE,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_EQ(testvalue, Get(testkey));
    ASSERT_OK(Delete(testkey));
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }
 */
/**
 * @brief     Use DeleteSpecialKey to test delete the Special key
 * @param     DeleteTest --- Class name. \n
 *            DeleteSetMaxKey --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteSpecialKey){
    Open();
    std::string testkey = "'~!@#$%^&*()_+`5')";
    std::string testvalue = "'~!@#$%^&*()_+`5')";
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_EQ(testvalue, Get(testkey));
    ASSERT_OK(Delete(testkey));
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }

 /**
 * @brief     Use DeleteKeyWithoutValue to test delete the key which has no value
 * @param     DeleteTest --- Class name. \n
 *            DeleteKeyWithoutValue --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteKeyWithoutValue){
    Open();
    std::string testkey(NORMALKEY, '1');
    std::string testvalue(0,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_EQ(testvalue, Get(testkey));
    ASSERT_OK(Delete(testkey));
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }
 /**
 * @brief     Use DeleteOneKey to test delete the key in default CF
 * @param     DeleteTest --- Class name. \n
 *            DeleteOneKey --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteOneKey){
    Open();
    std::string testkey(MINKEY, '1');
    std::string testkey1(MINKEY+1, '1');
    std::string testvalue(NORMALVALUE,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_OK(Put(testkey1, testvalue));
    ASSERT_EQ(testvalue, Get(testkey));
    ASSERT_EQ(testvalue, Get(testkey1));
    ASSERT_OK(Delete(testkey1));
    ASSERT_EQ("NOT_FOUND", Get(testkey1));
    ASSERT_EQ(testvalue, Get(testkey));
    Close();
 }
 /**
 * @brief     Use DeleteNoneExistKey to test delete the none existed Key
 * @param     DeleteTest --- Class name. \n
 *            DeleteNoneExistKey --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */ 
TEST_F(DeleteTest, DeleteNoneExistKey){
    Open();
    std::string testkey(MINKEY, '1');
    ASSERT_OK(Delete(testkey));
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }
 /**
 * @brief     Use SingleDeleteKV to test single delete the Key
 * @param     DeleteTest --- Class name. \n
 *            SingleDeleteKV --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, SingleDeleteKV){
    Open();
    std::string testkey(NORMALKEY, '1');
    std::string testvalue(NORMALVALUE,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_OK(Put(testkey, "111"));
    Status s = SingleDelete(testkey);
    ASSERT_EQ("NOT_FOUND", Get(testkey));
    Close();
 }
  /**
 * @brief     Use SingleDeleteInvalidKV to test single delete the invalid Key
 * @param     DeleteTest --- Class name. \n
 *            SingleDeleteInvalidKV --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, SingleDeleteInvalidKV){
    Open();
    std::string testkey(NORMALKEY, '1');
    std::string testvalue(NORMALVALUE,'a');
    ASSERT_OK(Put(testkey, testvalue));
    ASSERT_OK(SingleDelete("abcd"));
    ASSERT_EQ("NOT_FOUND", Get("abcd"));
    Close();
 }
/**
 * @brief     Use DeleteOneKVWithCF to test delete one KV in the CF
 * @param     DeleteTest --- Class name. \n
 *            DeleteOneKVWithCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteOneKVWithCF){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_OK(Put(1, "test", "v1"));
    ASSERT_OK(Put(1, "test12", "v2"));
    //Before delete can get the kv
    ASSERT_EQ("v1", Get(1, "test"));
    ASSERT_EQ("v2", Get(1, "test12"));
    ASSERT_OK(Delete(1, "test"));
    //After delete can't get kv
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    ASSERT_EQ("v2", Get(1, "test12"));
    Close(); 
 }
/**
 * @brief     Use DeleteWithDroppedCF to test delete one KV in the dropped CF
 * @param     DeleteTest --- Class name. \n
 *            DeleteWithDroppedCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteWithDroppedCF){
    db_options_.num_cols = 5;
    Open();
    CreateColumnFamilies({"one", "two", "three"});
    ASSERT_OK(Put(1, "test1", "v1"));
    ASSERT_EQ("v1", Get(1, "test1"));
    ASSERT_OK(Put(2, "test1", "v2"));
    ASSERT_EQ("v2", Get(2, "test1"));
    ASSERT_OK(Put(3, "test1", "v3"));
    ASSERT_EQ("v3", Get(3, "test1"));
    db_->DestroyColumnFamilyHandle(handles_[3]);
    //DropColumnFamilies({3});
    ASSERT_OK(Delete(3, "test1"));
    ASSERT_EQ("NOT_FOUND", Get(3, "test1"));
    Close();
}
/**
 * @brief     Use DeleteKVWithMultiCF to test delete some KVs in CFs
 * @param     DeleteTest --- Class name. \n
 *            DeleteKVWithMultiCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteKVWithMultiCF){
    db_options_.num_cols = 3;
    Open();
    CreateColumnFamilies({"one","two"});
    ASSERT_OK(Put(1, "test", "v1"));
    ASSERT_OK(Put(2, "test12", "v2"));
    ASSERT_EQ("v1", Get(1, "test"));
    ASSERT_EQ("v2", Get(2, "test12"));
    ASSERT_OK(Delete(1, "test"));
    ASSERT_OK(Delete(2, "test12"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    ASSERT_EQ("NOT_FOUND", Get(2, "test12"));
    Close();
}
/**
 * @brief     Use DeleteSameKVInMaxCF to test delete KVs in max CFs
 * @param     DeleteTest --- Class name. \n
 *            DeleteSameKVInMaxCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteSameKVInMaxCF){
    db_options_.num_cols = 65;
    Open();
    std::vector<std::string> cf_names;
    constexpr int kNumCF = 64;
    for (int i = 1; i <= kNumCF; i++) {
        cf_names.push_back("cf1-" + ToString(i));
    } 
    CreateColumnFamiliesAndReopen(cf_names); 
    ASSERT_EQ("NOT_FOUND", Get(0, "test1"));
    ASSERT_EQ("NOT_FOUND", Get(64,"test1"));
    cout <<"------check finished"<<endl;
    for (int i = 1; i <= kNumCF; i++) {
        ASSERT_OK(Put(i,"test1", "v1"));
    }
    for (int i = 1; i <= kNumCF; i++) {
        ASSERT_EQ("v1", Get(i, "test1"));
    }
    for (int i = 1; i <= kNumCF; i++) {
        ASSERT_OK(Delete(i, "test1")); 
    }
    for (int i = 1; i <= kNumCF; i++) {
        ASSERT_EQ("NOT_FOUND", Get(i, "test1"));
    }
    Close();
} 

/**
 * @brief     Use ReopenDeleteKVWithCF to test delete KVs after reopen DB
 * @param     DeleteTest --- Class name. \n
 *            ReopenDeleteKVWithCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*
TEST_F(DeleteTest, ReopenDeleteKVWithCF){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test12"));

    ASSERT_OK(Put(0, "test0", "v0"));
    ASSERT_OK(Put(1, "test1", "v1"));
    ASSERT_EQ("v0", Get(0, "test0"));
    ASSERT_EQ("v1", Get(1, "test1"));
    ASSERT_OK(Delete(1, "test1"));
    Reopen();

    ASSERT_EQ("NOT_FOUND", Get(1, "test1"));
    ASSERT_OK(Delete(0, "test0"));
    ASSERT_EQ("NOT_FOUND", Get(0, "test0"));
    Close();
}
*/
 /**
 * @brief     Use PutSameKeyTwiceDeleteCF to test delete same Key
 * @param     DeleteTest --- Class name. \n
 *            PutSameKeyTwiceDeleteCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, PutSameKeyTwiceDeleteCF){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_OK(Put(1, "test", "v1"));
    ASSERT_OK(Put(1, "test", "v11"));
    ASSERT_EQ("v11", Get(1, "test"));
    ASSERT_OK(Delete(1, "test"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    Close();
}

 /**
 * @brief     Use DeleteInvalidKeyCF to test delete invalid KV in the CF
 * @param     DeleteTest --- Class name. \n
 *            DeleteInvalidKeyCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteInvalidKeyCF){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    ASSERT_OK(Delete(1, "test"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    Close();
}
 /**
 * @brief     Use DeleteSameKeyInDiffCF to test delete same KV in the different CFs
 * @param     DeleteTest --- Class name. \n
 *            DeleteSameKeyInDiffCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteSameKeyInDiffCF){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_EQ("NOT_FOUND", Get(0, "test"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    ASSERT_OK(Put(0, "test", "v1"));
    ASSERT_OK(Put(1, "test", "v11"));
    ASSERT_EQ("v1", Get(0, "test"));
    ASSERT_EQ("v11", Get(1, "test"));
    ASSERT_OK(Delete(1, "test"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    ASSERT_EQ("v1", Get(0, "test"));
    ASSERT_OK(Delete(0, "test"));
    ASSERT_EQ("NOT_FOUND", Get(0, "test"));
    Close();
}
 /**
 * @brief     Use DeleteKeyWithoutValueCF to test delete key without value in the CF
 * @param     DeleteTest --- Class name. \n
 *            DeleteKeyWithoutValueCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteKeyWithoutValueCF){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    ASSERT_OK(Put(1, "test", ""));
    ASSERT_EQ("", Get(1, "test"));
    ASSERT_OK(Delete(1, "test"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test"));
    Close();
 }
 /**
 * @brief     Use PutSingleDeleteCF to test singleDelete KV in the CF
 * @param     DeleteTest --- Class name. \n
 *            PutSingleDeleteCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, PutSingleDeleteCF){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_OK(Put(1, "test1", "v1"));
    ASSERT_EQ("v1", Get(1, "test1"));
    ASSERT_OK(SingleDelete(1, "test1"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test1"));
    Close();
}
 /**
 * @brief     Use PutSingleDeletePutSingleDeleteCF to test singleDelete KV twice in the CF
 * @param     DeleteTest --- Class name. \n
 *            PutSingleDeletePutSingleDeleteCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, PutSingleDeletePutSingleDeleteCF){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_OK(Put(1, "test1", "v1"));
    ASSERT_EQ("v1", Get(1, "test1"));
    ASSERT_OK(SingleDelete(1, "test1"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test1"));
 
    ASSERT_OK(Put(1, "test1", "v1"));
    ASSERT_EQ("v1", Get(1, "test1"));
    ASSERT_OK(SingleDelete(1, "test1"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test1"));
    Close();
}
 /**
 * @brief     Use SingleDeleteInvalidKeyCF to test singleDelete invalid KV in the CF
 * @param     DeleteTest --- Class name. \n
 *            SingleDeleteInvalidKeyCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, SingleDeleteInvalidKeyCF){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_OK(Put(1, "test1", "v1"));
    ASSERT_EQ("v1", Get(1, "test1"));
    ASSERT_OK(SingleDelete(1, "test2"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test2"));
    Close();
}
 /**
 * @brief     Use DeleteKeyWithNullCF to test delete KV in the null CF
 * @param     DeleteTest --- Class name. \n
 *            DeleteKeyWithNullCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteKeyWithNullCF){
    db_options_.num_cols = 3;
    Open();
    CreateColumnFamilies({"one",""});
    ASSERT_OK(Put(2, "test1", "v2"));
    ASSERT_EQ("v2", Get(2, "test1"));
    ASSERT_OK(Delete(2, "test1"));
    ASSERT_EQ("NOT_FOUND", Get(2, "test1"));
    Close();
}
/**
 * @brief     Use DeleteAndSingleDeleteCF to test delete and singledelete KV
 * @param     DeleteTest --- Class name. \n
 *            DeleteKeyWithNullCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteAndSingleDeleteCF){
    db_options_.num_cols = 3;
    Open();
    CreateColumnFamilies({"one","two"});
    ASSERT_OK(Put(1, "test1", "v1"));
    ASSERT_OK(Put(2, "test2", "v2"));
    ASSERT_EQ("v1", Get(1, "test1"));
    ASSERT_EQ("v2", Get(2, "test2"));
    ASSERT_OK(SingleDelete(1, "test1"));
    ASSERT_OK(Delete(2, "test2"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test1"));
    ASSERT_EQ("NOT_FOUND", Get(2, "test2"));
    Close();
}
/**
 * @brief     Use DeleteKeyAfterFlush to test delete KV after flush
 * @param     DeleteTest --- Class name. \n
 *            DeleteKeyWithNullCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteKeyAfterFlush){
    Open();
    CreateColumnFamilies({"one"});
    ASSERT_OK(Put(1, "test1", "v1"));
    ASSERT_OK(Flush());
    ASSERT_EQ("v1", Get(1, "test1"));
    ASSERT_OK(Delete(1, "test1"));
    ASSERT_EQ("NOT_FOUND", Get(1, "test1"));
    Close();
}
/**
 * @brief     Use MultiDeleteinCF to test delete KV
 * @param     DeleteTest --- Class name. \n
 *            MultiDeleteinCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, MultiDeleteinCF){
    Open();
    CreateColumnFamilies({"one"});
    int Num = 10000;
	cout<<"The test num is "+ToString(Num)<<endl;
    for (int i = 1; i <= Num; i++) {
        ASSERT_OK(Put(1,"key"+ToString(i), "value"+ToString(i)));
    }
    cout<<"Put is ok"<<endl;
    sleep(1);
    for (int i = 1; i <= Num; i++) {
        ASSERT_EQ("value"+ToString(i), Get(1,"key"+ToString(i)));
    }
    cout<<"Get is ok"<<endl;
    for (int i = 1; i <= Num; i++) {
        ASSERT_OK(Delete(1,"key"+ToString(i)));
    }
    cout<<"delete is ok"<<endl;
    for (int i = 1; i <= Num; i++) {
        ASSERT_EQ("NOT_FOUND", Get(1,"key"+ToString(i)));
    }
    cout<<"Get is ok"<<endl;
    Close();
}

/**
 * @brief     Use MultiDeleteinCF1 to test delete 100000 KV
 * @param     DeleteTest --- Class name. \n
 *            MultiDeleteinCF1 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*
TEST_F(DeleteTest, MultiDeleteinCF1){
    Open();
    CreateColumnFamilies({"one"});
    int Num = 100000;
    cout<<"The test num is "+ToString(Num)<<endl;
    for (int i = 1; i <= Num; i++) {
        ASSERT_OK(Put(1,"key"+ToString(i), "value"+ToString(i)));
    }
    cout<<"Put is ok"<<endl;
    sleep(1);
    for (int i = 1; i <= Num; i++) {
        ASSERT_EQ("value"+ToString(i), Get(1,"key"+ToString(i)));
    }
    cout<<"Get is ok"<<endl;
    for (int i = 1; i <= Num; i++) {
        ASSERT_OK(Delete(1,"key"+ToString(i)));
    }
    cout<<"delete is ok"<<endl;
    for (int i = 1; i <= Num; i++) {
        ASSERT_EQ("NOT_FOUND", Get(1,"key"+ToString(i)));
    }
    cout<<"Get is ok"<<endl;
    Close();
}

*/


/**
 * @brief     Use SyncDeleteinCF to test delete KV by sync options is true
 * @param     DeleteTest --- Class name. \n
 *            DeleteKeyWithNullCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(DeleteTest, SyncDeleteinCF){
    WriteOptions wropt;
    Open();
    clock_t start,finish;
    double time = 0, time1 = 0,time2 = 0;
    int Num = 10000;
    for (int j=1;j<=100;j++){
        start=clock();
        for (int i = 1; i <= Num; i++) {
            ASSERT_OK(db_->Put(wropt, handles_[0],"key"+ToString(i), "value"+ToString(i)));
            ASSERT_EQ("value"+ToString(i), Get(0,"key"+ToString(i)));
        }
    
        for (int i = 1; i <= Num; i++) {
            ASSERT_OK(db_->Delete(wropt,handles_[0],"key"+ToString(i)));
            ASSERT_EQ("NOT_FOUND", Get(0,"key"+ToString(i)));
        }
        finish=clock();
        time=(double)(finish-start)/CLOCKS_PER_SEC;
        time1 = time1+time;
		j++;
    }
    cout<<"The time1 is "<<time1<<endl;

    wropt.sync = true;
    TryReopen();
    for (int j=1;j<=100;j++){
        start=clock();
        for (int i = 1; i <= Num; i++) {
            ASSERT_OK(db_->Put(wropt, handles_[0],"key"+ToString(i), "value1"+ToString(i)));
            ASSERT_EQ("value1"+ToString(i), Get(0,"key"+ToString(i)));
        }
    
        for (int i = 1; i <= Num; i++) {
            ASSERT_OK(db_->Delete(wropt,handles_[0],"key"+ToString(i)));
            ASSERT_EQ("NOT_FOUND", Get(0,"key"+ToString(i)));
        }
        finish=clock();
        time=(double)(finish-start)/CLOCKS_PER_SEC;
        time2 = time2+time;
		j++;
    }
    cout<<"The time2 is "<<time2<<endl;
    
    ASSERT_LT(time1,time2);
    Close();
}
/**
 * @brief     Use DeleteDifKVInMaxCF to test delete KVs in max CFs
 * @param     DeleteTest --- Class name. \n
 *            DeleteDifKVInMaxCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(DeleteTest, DeleteDifKVInMaxCF){
    db_options_.num_cols = 65;
    Open();
    std::vector<std::string> cf_names;
    constexpr int kNumCF = 64;
    for (int i = 1; i <= kNumCF; i++) {
        cf_names.push_back("cf1-" + ToString(i));
    } 
    CreateColumnFamiliesAndReopen(cf_names); 
    ASSERT_EQ("NOT_FOUND", Get(0, "test1"));
    ASSERT_EQ("NOT_FOUND", Get(64,"test1"));

    //Put diff KV in every CF
    for (int j = 1;j<=kNumCF;j++){
        for (int i = 1; i <= kNumCF; i++) {
            ASSERT_OK(Put(j,"key"+ToString(i), "value"+ToString(i)));
            ASSERT_EQ("value"+ToString(i), Get(j,"key"+ToString(i)));
        }
    }
    //Delete some CF's some kvs
    for (int j = 1;j<=20;j++){
        for (int i = 10; i <= 40; i++) {
            ASSERT_OK(Delete(j,"key"+ToString(i)));
            ASSERT_EQ("NOT_FOUND", Get(j,"key"+ToString(i)));
        }
    }
    //Delete some CF's all KVs
    for (int j = 30;j<=kNumCF;j++){
        for (int i = 1; i <= kNumCF; i++) {
            ASSERT_OK(Delete(j,"key"+ToString(i)));
            ASSERT_EQ("NOT_FOUND", Get(j,"key"+ToString(i)));
        }
    }
    Close();
}

}
