//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "kvdb_test.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/transaction_test_util.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/transactions/pessimistic_transaction_db.h"


#define UINT64_MAX  18446744073709551616

using std::string;

namespace rocksdb {

//INSTANTIATE_TEST_CASE_P(
 //   DBAsBaseDB, TransactionTest,
 //   ::testing::Values(std::make_tuple(false,  WRITE_COMMITTED)));
                      //std::make_tuple(false,  WRITE_COMMITTED),
                      //std::make_tuple(false,  WRITE_PREPARED),
                      //std::make_tuple(false,  WRITE_PREPARED),
                      //std::make_tuple(false,  WRITE_UNPREPARED),
                      //std::make_tuple(false,  WRITE_UNPREPARED)));

//Add by 66

namespace transaction {
class TransactionFunTest: public testing::Test {
 public:
  std::string dbname="";
  Options options;
  TransactionDB* db;
  TransactionDBOptions txn_db_options;
  std::string value="";
  WriteOptions write_options;
  ReadOptions read_options;
  
public:
  TransactionFunTest() {
  dbname = test::TmpDir() + "/transaction_test";
  options.create_if_missing = true;
  options.num_cols = 3;
  options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
  DestroyDB(dbname, options);
  txn_db_options.transaction_lock_timeout = 0;
  txn_db_options.default_lock_timeout = 0;
  txn_db_options.write_policy = WRITE_COMMITTED;
  TransactionDB::Open(options, txn_db_options, dbname, &db);

  }


  ~TransactionFunTest() {
    delete db;
    db = nullptr;
    DestroyDB(dbname, options);
  }
    Status ReOpenNoDelete() {
    delete db;
    db = nullptr;
    Status s;
    s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    assert(!s.ok() || db != nullptr);
    return s;
  }
  Status ReOpen() {
    delete db;
    db = nullptr;
    DestroyDB(dbname, options);
    Status s;
    s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    assert(db != nullptr);
    return s;
  }
}; 
/**
 * @brief     Use BeginTest to test Begin Transaction,and verify the actions of BeginTransaction API.
 * @param     TransactionFunTest --- Class name. \n
 *            BeginTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, BeginTest) {
  WriteOptions write_options;

  Transaction* txn = db->BeginTransaction(write_options);
  delete txn;
  
  TransactionOptions txn_options;
  Transaction* txn1 = db->BeginTransaction(write_options,txn_options);
  //reopen transaction txn1 and rename txn1->txn2
  Transaction* txn2 = db->BeginTransaction(write_options,txn_options,txn1);
  //delete txn1;
  delete txn2;
  
}
/**
 * @brief     Use BeginTwiceTest to test begin Transaction two times,and verify the actions of BeginTransaction API.
 * @param     TransactionFunTest --- Class name. \n
 *            BeginTwiceTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, BeginTwiceTest) {
  WriteOptions write_options;

  ASSERT_OK(db->Put(write_options, "66", "test"));

  Transaction* txn = db->BeginTransaction(write_options);
  //ASSERT_OK(txn->Commit());
  delete txn;

  txn = db->BeginTransaction(write_options);
  delete txn;
}

/**
 * @brief     Use WriteTest to test Put kv in Transaction,and verify the Put actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            WriteTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WriteTest) {
  WriteOptions write_options;

  // Start a transaction
  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_OK(txn->Put("abc", "def"));
  ASSERT_OK(txn->Commit());
  delete txn;
}
/**
 * @brief     Use MultiTimesTest to test Start a transaction 100000 times
 * @param     TransactionFunTest --- Class name. \n
 *            MultiTimesTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, MultiTimesTest) {
  WriteOptions write_options;

  // Start a transaction
  for (int i=0;i <= 100000;i++){
  
  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_OK(txn->Put("key"+ToString(i), "value"+ToString(i)));
  ASSERT_OK(txn->Commit());
  delete txn;
  db->Get(read_options, "key"+ToString(i), &value);
  ASSERT_EQ(value,"value"+ToString(i));
  }
}
/**
 * @brief     Use WriteLargeData to test Put 1024*1 kv in Transaction
 * @param     TransactionFunTest --- Class name. \n
 *            WriteLargeData --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WriteLargeData) {
  WriteOptions write_options;

  // Start a transaction
  Transaction* txn = db->BeginTransaction(write_options);
  std::string testkey(1024*1, '1');
  std::string testvalue(1024*10,'a');
  ASSERT_OK(txn->Put(testkey, testvalue));
  ASSERT_OK(txn->Commit());
  db->Get(read_options, testkey, &value);
  ASSERT_EQ(value,testvalue);
  delete txn;
}
/**
 * @brief     Use WriteLargeData1 to test Put 1024*10 kv in Transaction
 * @param     TransactionFunTest --- Class name. \n
 *            WriteLargeData1 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(TransactionFunTest, WriteLargeData1) {
  WriteOptions write_options;

  // Start a transaction
  Transaction* txn = db->BeginTransaction(write_options);
  std::string testkey(1024*10, '1');
  std::string testvalue(1024*10,'a');
  ASSERT_OK(txn->Put(testkey, testvalue));
  ASSERT_OK(txn->Commit());
  db->Get(read_options, testkey, &value);
  ASSERT_EQ(value,testvalue);
  delete txn;
}
/**
 * @brief     Use WriteMultiTimes to test PUT KV in a transaction 100000 times
 * @param     TransactionFunTest --- Class name. \n
 *            WriteMultiTimes --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WriteMultiTimes) {
  WriteOptions write_options;
  // Start a transaction
  Transaction* txn = db->BeginTransaction(write_options);
  int Num = 100000;
  cout<<"The test num is "+ToString(Num)<<endl;
  for (int i = 1; i <= Num; i++) {
    ASSERT_OK(txn->Put("key"+ToString(i), "value"+ToString(i)));
  }
  cout<<"Put is ok"<<endl;
  ASSERT_OK(txn->Commit());
  
  for (int i = 1; i <= Num; i++) {
    ASSERT_OK(db->Get(read_options, "key"+ToString(i), &value));
    ASSERT_EQ(value,"value"+ToString(i));
  }
  cout<<"Get is ok"<<endl;
  delete txn;
}
/**
 * @brief     Use WriteTest2 to test Transaction, and verify the actions of PUT API.
 * @param     TransactionFunTest --- Class name. \n
 *            WriteTest2 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WriteTest2) {
  WriteOptions write_options;

  Transaction* txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  // Test conflict checking against the very first write to a db.
  ASSERT_OK(db->Put(write_options, "A", "a"));
  
  Status s = txn->Put("A", "b");
  ASSERT_TRUE(s.IsBusy());
  delete txn;
}
/**
 * @brief     Use WriteTestWithoutSnapshot to test put in Transaction withoutSnapshot
 * @param     TransactionFunTest --- Class name. \n
 *            WriteTestWithoutSnapshot --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WriteTestWithoutSnapshot) {
  WriteOptions write_options;

  Transaction* txn = db->BeginTransaction(write_options);

  ASSERT_OK(db->Put(write_options, "A", "a"));
  
  Status s = txn->Put("A", "b");
  ASSERT_OK(s);
  ASSERT_OK(txn->Commit());
  
  s = db->Get(read_options, "A", &value);
  ASSERT_EQ(value,"b");
  delete txn;
}
/**
 * @brief     Use WriteTest3 to test Transaction, and verify the actions of PUT API.
 * @param     TransactionFunTest --- Class name. \n
 *            WriteTest3 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WriteTest3) {
  WriteOptions write_options;
  
  Status s = db->Put(write_options, "A", "a");
  ASSERT_OK(s);
  Transaction* txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  s = txn->Put("A", "b");
  ASSERT_OK(s);
  ASSERT_OK(txn->Commit());
  
  db->Get(read_options, "A", &value);
  ASSERT_EQ(value,"b");
  delete txn;
}
/**
 * @brief     Use WritetoSamekey to test Transaction, and verify the Put actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            WritetoSamekey --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WritetoSamekey) {
  WriteOptions write_options;
  TransactionOptions txn_options;

  // Test 2 transactions writing to the same key in multiple orders
  Transaction* txn1 = db->BeginTransaction(write_options,txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options,txn_options);

  Status s = txn1->Put("1", "1");
  ASSERT_OK(s);
  s = txn2->Put("1", "2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("2", "2");
  ASSERT_OK(s);
  ASSERT_OK(txn1->Commit());
  ASSERT_OK(txn2->Commit());
  
  db->Get(read_options, "1", &value);
  ASSERT_EQ(value,"1");
  db->Get(read_options, "2", &value);
  ASSERT_EQ(value,"2");
  delete txn1;
  delete txn2;
}
/**
 * @brief     Use WritetoSamekeyWithSnapshot to Test 2 transactions writing to the same key in multiple orders
 * @param     TransactionFunTest --- Class name. \n
 *            WritetoSamekeyWithSnapshot --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WritetoSamekeyWithSnapshot) {
  WriteOptions write_options;
  TransactionOptions txn_options;

  // Test 2 transactions writing to the same key in multiple orders
  Transaction* txn1 = db->BeginTransaction(write_options,txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options,txn_options);
  txn1->SetSnapshot();
  Status s = txn1->Put("1", "1");
  ASSERT_OK(s);
  s = txn2->Put("1", "2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("2", "2");
  ASSERT_OK(s);
  ASSERT_OK(txn1->Commit());
  ASSERT_OK(txn2->Commit());
  
  db->Get(read_options, "1", &value);
  ASSERT_EQ(value,"1");
  db->Get(read_options, "2", &value);
  ASSERT_EQ(value,"2");
  delete txn1;
  delete txn2;
}
/**
 * @brief     Use WriteConflictTest to test Transaction, and verify the actions of  Put API.
 * @param     TransactionFunTest --- Class name. \n
 *            WriteConflictTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    "txn->PUT" at first in conflict with "db->Put"
 */
 
TEST_F(TransactionFunTest, WriteConflictTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  db->Put(write_options, "foo", "A");
  db->Put(write_options, "foo2", "B");
  
  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);
  s = txn->Put("foo", "A2");
  ASSERT_OK(s);
  
  s = txn->Put("foo2", "B2");
  ASSERT_OK(s);


  // This Put outside of a transaction will conflict with the previous write
  s = db->Put(write_options, "foo", "xxx");
  ASSERT_TRUE(s.IsTimedOut());

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "A");

  s = txn->Commit();
  ASSERT_OK(s);

  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "A2");
  db->Get(read_options, "foo2", &value);
  ASSERT_EQ(value, "B2");

  delete txn;
}
/**
 * @brief     Use WriteConflictTest2 to test Transaction, and verify the Put actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            WriteConflictTest2 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    "db->PUT" at first in conflict with "txn->Put"
 */
TEST_F(TransactionFunTest, WriteConflictTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  db->Put(write_options, "foo", "bar");

  txn_options.set_snapshot = true;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  // This Put outside of a transaction will conflict with a later write
  s = db->Put(write_options, "foo", "barz");
  ASSERT_OK(s);
  cout << "txn->put is OK" << endl;
  s = txn->Put("foo2", "X");
  ASSERT_OK(s);

  s = txn->Put("foo","bar2");  // Conflicts with write done after snapshot taken
  ASSERT_TRUE(s.IsBusy());

  s = txn->Put("foo3", "Y");
  ASSERT_OK(s);

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");

  ASSERT_EQ(2, txn->GetNumKeys());

  s = txn->Commit();
  ASSERT_OK(s);  // Txn should commit, but only write foo2 and foo3

  // Verify that transaction wrote foo2 and foo3 but not foo
  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "barz");

  db->Get(read_options, "foo2", &value);
  ASSERT_EQ(value, "X");

  db->Get(read_options, "foo3", &value);
  ASSERT_EQ(value, "Y");

  delete txn;
}
/**
 * @brief     Use WriteOptionsTest to test Transaction, and verify the actions of *WriteOptions API.
 * @param     TransactionFunTest --- Class name. \n
 *            WriteOptionsTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WriteOptionsTest) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = true;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  ASSERT_TRUE(txn->GetWriteOptions()->sync);

  write_options.sync = false;
  txn->SetWriteOptions(write_options);
  ASSERT_FALSE(txn->GetWriteOptions()->sync);
  //ASSERT_TRUE(txn->GetWriteOptions()->disableWAL);

  delete txn;
}
/**
 * @brief     Use ReadTest to test Transaction, and verify the actions of Get API.
 * @param     TransactionFunTest --- Class name. \n
 *            ReadTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    "txn->PUT" at first in conflict with "db->Put"
 */
 
TEST_F(TransactionFunTest, ReadTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  txn->Put("foo", "bar2");
  txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  s = txn->Commit();
  ASSERT_OK(s);
  // Verify that transaction did not write anything
  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  delete txn;
}


/**
 * @brief     Use ReadTest to test Transaction, and verify the actions of MultiGet API.
 * @param     TransactionFunTest --- Class name. \n
 *            ReadTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
 
TEST_F(TransactionFunTest, MultiReadTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);
  
  ASSERT_OK(txn->Put("1", "a"));
  ASSERT_OK(txn->Put("2", "b"));
  ASSERT_OK(txn->Put("3", "c"));
  
  std::vector<Slice> multi_keys = {"1", "2", "3"};
  std::vector<std::string> values;
  txn->MultiGet(read_options, multi_keys, &values);
  ASSERT_EQ(values[0], "a");
  ASSERT_EQ(values[1], "b");
  ASSERT_EQ(values[2], "c");

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;
}
/**
 * @brief     Use ReadConflictTest to test Transaction, and verify the Get actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            ReadConflictTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, ReadConflictTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  db->Put(write_options, "foo", "bar");
  db->Put(write_options, "foo2", "bar");

  txn_options.set_snapshot = true;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  snapshot_read_options.snapshot = txn->GetSnapshot();
  
  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  // This Put outside of a transaction will conflict with the previous read
  s = db->Put(write_options, "foo", "barz");
  ASSERT_TRUE(s.IsTimedOut());
  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  s = txn->Commit();
  ASSERT_OK(s);

  // Verify that transaction did not write anything
  txn->GetForUpdate(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");
  txn->GetForUpdate(read_options, "foo2", &value);
  ASSERT_EQ(value, "bar");

  delete txn;
}
/**
 * @brief     Use ReadConflictTest2 to test Transaction, and verify the actions of Get API.
 * @param     TransactionFunTest --- Class name. \n
 *            ReadConflictTest2 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, ReadConflictTest2) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  db->Put(write_options, "foo", "bar");
  db->Put(write_options, "foo2", "bar");

  txn_options.set_snapshot = true;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  // This Put outside of a transaction will conflict with the previous read
  s = db->Put(write_options, "foo", "barz");
  ASSERT_TRUE(s.IsTimedOut());

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  s = txn->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
}
/**
 * @brief     Use EmptyTest to test no txn->put and commit transaction
 * @param     TransactionFunTest --- Class name. \n
 *            EmptyTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, EmptyTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  s = db->Put(write_options, "aaa", "aaa");
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  txn = db->BeginTransaction(write_options);
  txn->Rollback();
  delete txn;

  txn = db->BeginTransaction(write_options);
  s = txn->GetForUpdate(read_options, "aaa", &value);
  ASSERT_EQ(value, "aaa");

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  s = txn->GetForUpdate(read_options, "aaa", &value);
  ASSERT_EQ(value, "aaa");

  // Conflicts with previous GetForUpdate
  s = db->Put(write_options, "aaa", "xxx");
  ASSERT_TRUE(s.IsTimedOut());

  // transaction expired!
  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;
}
/**
 * @brief     Use LostUpdate to test write to the same key with/without snapshots
 * @param     TransactionFunTest --- Class name. \n
 *            LostUpdate --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, LostUpdate) {
  WriteOptions write_options;
  ReadOptions read_options, read_options1, read_options2;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  // Test 2 transactions writing to the same key in multiple orders and
  // with/without snapshots

  Transaction* txn1 = db->BeginTransaction(write_options);
  Transaction* txn2 = db->BeginTransaction(write_options);

  s = txn1->Put("1", "1");
  ASSERT_OK(s);

  s = txn2->Put("1", "2");  // conflict
  ASSERT_TRUE(s.IsTimedOut());

  s = txn2->Commit();
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("1", value);

  delete txn1;
  delete txn2;

  txn_options.set_snapshot = true;
  txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  s = txn1->Put("1", "3");
  ASSERT_OK(s);
  s = txn2->Put("1", "4");  // conflict
  ASSERT_TRUE(s.IsTimedOut());

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("3", value);

  delete txn1;
  delete txn2;

  txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  s = txn1->Put("1", "5");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Put("1", "6");
  ASSERT_TRUE(s.IsBusy());
  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  delete txn1;
  delete txn2;

  txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  s = txn1->Put("1", "7");
  ASSERT_OK(s);
  s = txn1->Commit();
  ASSERT_OK(s);

  txn2->SetSnapshot();
  s = txn2->Put("1", "8");
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("8", value);

  delete txn1;
  delete txn2;

  txn1 = db->BeginTransaction(write_options);
  txn2 = db->BeginTransaction(write_options);

  s = txn1->Put("1", "9");
  ASSERT_OK(s);
  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Put("1", "10");
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  delete txn1;
  delete txn2;

  s = db->Get(read_options, "1", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "10");
}
/**
 * @brief     Use GetForUpdateTest to test Transaction, and verify GetForUpdate actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            GetForUpdateTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, GetForUpdateTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  string value;
  Status s;

  db->Put(write_options, "foo", "bar");
  db->Put(write_options, "foo2", "bar");

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  ASSERT_OK(txn->Put("foo", "bar2"));

  txn->GetForUpdate(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  ASSERT_OK(txn->Commit());

  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  delete txn;
}

/**
 * @brief     Use MultiGetForUpdateTest to test Transaction, and verify actions of MultiGetForUpdate API.
 * @param     TransactionFunTest --- Class name. \n
 *            MultiGetForUpdateTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, MultiGetForUpdateTest) {
  WriteOptions write_options;
  ReadOptions read_options1, read_options2;
  TransactionOptions txn_options;
  string value;
  Status s;

  txn_options.set_snapshot = true;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  Transaction* txn2 = db->BeginTransaction(write_options);
  txn2->SetSnapshot();
  read_options2.snapshot = txn2->GetSnapshot();

  std::vector<Slice> multiget_keys = {"1", "2", "3"};
  std::vector<std::string> multiget_values;

  std::vector<Status> results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_TRUE(results[1].IsNotFound());
  cout<<results[1].ToString()<<endl;

  //Conflict's with txn1's MultiGetForUpdate for key "2"
  s = txn2->Put("2", "x");
  ASSERT_TRUE(s.IsTimedOut());
  
  //txn-> put can't write data to multiget_keys
  s = txn2->Get(read_options2, "2", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn2->Commit();
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);
  
  delete txn1;
  delete txn2;
    
  txn1 = db->BeginTransaction(write_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = db->BeginTransaction(write_options);
  read_options2.snapshot = txn2->GetSnapshot();
  
  multiget_values.clear();
  txn1->Put("1", "66");
  txn1->Put("2", "67");
  txn1->Put("3", "68");
  
  results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_OK(results[0]);
  cout<<results[0].ToString()<<endl;
  ASSERT_OK(results[1]);
  cout<<results[1].ToString()<<endl;
  ASSERT_OK(results[2]);
  cout<<results[2].ToString()<<endl;
  ASSERT_EQ(multiget_values[0], "66");
  ASSERT_EQ(multiget_values[1], "67");
  ASSERT_EQ(multiget_values[2], "68");
  
  s = txn1->Put("1", "666");
  ASSERT_OK(s);
  
  s = txn2->Put("4", "69");
  ASSERT_OK(s);
    
  txn2->MultiGet(read_options2, multiget_keys, &multiget_values);
  ASSERT_EQ(multiget_values[0], "66");
  ASSERT_EQ(multiget_values[1], "67");
  ASSERT_EQ(multiget_values[2], "68");
  
  s = txn2->Get(read_options2, "4", &value);
  ASSERT_EQ(value, "69");
 
  s = txn2->Commit();
  ASSERT_OK(s);
  
  s = txn1->Commit();
  ASSERT_OK(s);
  ReadOptions read_options;
  
  db -> Get(read_options, "1", &value);
  ASSERT_EQ(value, "666");
  
  db -> Get(read_options, "4", &value);
  ASSERT_EQ(value, "69");
  
  delete txn1;
  delete txn2;
}

/**
 * @brief     Use UndoGetForUpdateTest to test Transaction, and verify actions of UndoGetForUpdate API.
 * @param     TransactionFunTest --- Class name. \n
 *            UndoGetForUpdateTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, UndoGetForUpdateTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  txn1->UndoGetForUpdate("A");

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;

  txn1 = db->BeginTransaction(write_options, txn_options);

  txn1->UndoGetForUpdate("A");
  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Verify that A is locked
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  s = txn2->Put("A", "a");
  ASSERT_TRUE(s.IsTimedOut());

  txn1->UndoGetForUpdate("A");

  // Verify that A is now unlocked
  s = txn2->Put("A", "a2");
  ASSERT_OK(s);
  txn2->Commit();
  delete txn2;
  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a2", value);

  s = txn1->Delete("A");
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->Put("B", "b3");
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "B", &value);
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");

  // Verify that A and B are still locked
  txn2 = db->BeginTransaction(write_options, txn_options);
  s = txn2->Put("A", "a4");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b4");
  ASSERT_TRUE(s.IsTimedOut());

  txn1->Rollback();
  delete txn1;

  // Verify that A and B are no longer locked
  s = txn2->Put("A", "a5");
  ASSERT_OK(s);
  s = txn2->Put("B", "b5");
  ASSERT_OK(s);
  s = txn2->Commit();
  delete txn2;
  ASSERT_OK(s);

  txn1 = db->BeginTransaction(write_options, txn_options);

  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->GetForUpdate(read_options, "B", &value);
  ASSERT_OK(s);
  s = txn1->Put("B", "b5");
  s = txn1->GetForUpdate(read_options, "B", &value);
  ASSERT_OK(s);
  //If call GetForUpdate N time,Undo also need call N times
  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("X");

  // Verify A,B,C are locked
  txn2 = db->BeginTransaction(write_options, txn_options);
  s = txn2->Put("A", "a6");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Delete("B");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c6");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("X", "x6");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("X");

  // Verify A,B are locked and C is not
  s = txn2->Put("A", "a6");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Delete("B");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c6");
  ASSERT_OK(s);
  s = txn2->Put("X", "x6");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("X");

  // Verify B is locked and A and C are not
  s = txn2->Put("A", "a7");
  ASSERT_OK(s);
  s = txn2->Delete("B");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c7");
  ASSERT_OK(s);
  s = txn2->Put("X", "x7");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;
}
/**
 * @brief     Use UndoGetForUpdateTest2 to test Transaction, and verify actions of UndoGetForUpdate API.
 * @param     TransactionFunTest --- Class name. \n
 *            UndoGetForUpdateTest2 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, UndoGetForUpdateTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  s = db->Put(write_options, "A", "");
  ASSERT_OK(s);

  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->Put("F", "f");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 1

  txn1->UndoGetForUpdate("A");

  s = txn1->GetForUpdate(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->GetForUpdate(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->Put("E", "e");
  ASSERT_OK(s);
  s = txn1->GetForUpdate(read_options, "E", &value);
  ASSERT_OK(s);

  s = txn1->GetForUpdate(read_options, "F", &value);
  ASSERT_OK(s);

  // Verify A,B,C,D,E,F are still locked
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  s = txn2->Put("A", "a1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("D", "d1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e1");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f1");
  ASSERT_TRUE(s.IsTimedOut());

  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("E");

  // Verify A,B,D,E,F are still locked and C is not.
  s = txn2->Put("A", "a2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("D", "d2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c2");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 2

  s = txn1->Put("H", "h");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("D");
  txn1->UndoGetForUpdate("E");
  txn1->UndoGetForUpdate("F");
  txn1->UndoGetForUpdate("G");
  txn1->UndoGetForUpdate("H");

  // Verify A,B,D,E,F,H are still locked and C,G are not.
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("D", "d3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("H", "h3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);

  txn1->RollbackToSavePoint();  // rollback to 2

  // Verify A,B,D,E,F are still locked and C,G,H are not.
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("D", "d3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);
  s = txn2->Put("H", "h3");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("D");
  txn1->UndoGetForUpdate("E");
  txn1->UndoGetForUpdate("F");
  txn1->UndoGetForUpdate("G");
  txn1->UndoGetForUpdate("H");

  // Verify A,B,E,F are still locked and C,D,G,H are not.
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("E", "e3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("D", "d3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);
  s = txn2->Put("H", "h3");
  ASSERT_OK(s);

  txn1->RollbackToSavePoint();  // rollback to 1

  // Verify A,B,F are still locked and C,D,E,G,H are not.
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("D", "d3");
  ASSERT_OK(s);
  s = txn2->Put("E", "e3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);
  s = txn2->Put("H", "h3");
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("B");
  txn1->UndoGetForUpdate("C");
  txn1->UndoGetForUpdate("D");
  txn1->UndoGetForUpdate("E");
  txn1->UndoGetForUpdate("F");
  txn1->UndoGetForUpdate("G");
  txn1->UndoGetForUpdate("H");

  // Verify F is still locked and A,B,C,D,E,G,H are not.
  s = txn2->Put("F", "f3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("A", "a3");
  ASSERT_OK(s);
  s = txn2->Put("B", "b3");
  ASSERT_OK(s);
  s = txn2->Put("C", "c3");
  ASSERT_OK(s);
  s = txn2->Put("D", "d3");
  ASSERT_OK(s);
  s = txn2->Put("E", "e3");
  ASSERT_OK(s);
  s = txn2->Put("G", "g3");
  ASSERT_OK(s);
  s = txn2->Put("H", "h3");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);

  delete txn1;
  delete txn2;
}
/**
 * @brief     Use NoSnapshotTest to test Transaction, and verify the actions of GetSnapshot API.
 * @param     TransactionFunTest --- Class name. \n
 *            NoSnapshotTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */

TEST_F(TransactionFunTest, NoSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  db->Put(write_options, "AAA", "bar");

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  // Modify key after transaction start
  db->Put(write_options, "AAA", "bar1");

  // Read and write without a snapshot
  txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_EQ(value, "bar1");
  s = txn->Put("AAA", "bar2");
  ASSERT_OK(s);

  // Should commit since read/write was done after data changed
  s = txn->Commit();
  ASSERT_OK(s);

  txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_EQ(value, "bar2");

  delete txn;
}
/**
 * @brief     Use SetSnapshot to test Transaction, and verify the actions of SetSnapshot API.
 * @param     TransactionFunTest --- Class name. \n
 *            SetSnapshot --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */ 
TEST_F(TransactionFunTest, SetSnapshot) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  TransactionOptions txn_options;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
// Set a snapshot at start of transaction
  txn_options.set_snapshot = true;

  // Do some reads and writes to key "x"
  read_options.snapshot = db->GetSnapshot();
  ASSERT_OK(txn->Put("x", "x"));
  txn->Get(read_options, "x", &value);
  ASSERT_EQ(value, "x");
  // Do a write outside of the transaction to key "y"
  db->Put(write_options, "y", "y");

  // Set a new snapshot in the transaction
  txn->SetSnapshot();
  txn->SetSavePoint();
  read_options.snapshot = db->GetSnapshot();

  // Do some reads and writes to key "y"
  // Since the snapshot was advanced, the write done outside of the
  // transaction does not conflict.
  txn->GetForUpdate(read_options, "y", &value);
  txn->Put("y", "y");

  // Decide we want to revert the last write from this transaction.
  txn->RollbackToSavePoint();

  // Commit.
  txn->Commit();
  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;
}
/**
 * @brief     Use GetSnapshot to test Transaction, and verify the actions of GetSnapshot API.
 * @param     TransactionFunTest --- Class name. \n
 *            GetSnapshot --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, GetSnapshot) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  TransactionOptions txn_options;
  // Set a snapshot at start of transaction by setting set_snapshot=true
  txn_options.set_snapshot = true;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);

  const Snapshot* snapshot = txn->GetSnapshot();

  // Write a key OUTSIDE of transaction
  ASSERT_OK(db->Put(write_options, "abc", "xyz"));
  // Attempt to read a key using the snapshot.  This will fail since
  // the previous write outside this txn conflicts with this read.
  read_options.snapshot = snapshot;
  txn->GetForUpdate(read_options, "abc", &value);
  
  // Clear snapshot from read options since it is no longer valid
  delete txn;
  read_options.snapshot = nullptr;
  snapshot = nullptr;
  //txn->Rollback(); 
}

/**
 * @brief     Use MultipleSnapshotTest to test Transaction, and verify the actions of GetForUpdate API.
 * @param     TransactionFunTest --- Class name. \n
 *            MultipleSnapshotTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, MultipleSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
  Status s;

  ASSERT_OK(db->Put(write_options, "AAA", "bar"));
  ASSERT_OK(db->Put(write_options, "BBB", "bar"));
  ASSERT_OK(db->Put(write_options, "CCC", "bar"));

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  db->Put(write_options, "AAA", "bar1");

  // Read and write without a snapshot
  ASSERT_OK(txn->GetForUpdate(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar1");
  s = txn->Put("AAA", "bar2");
  ASSERT_OK(s);

  // Modify BBB before snapshot is taken
  ASSERT_OK(db->Put(write_options, "BBB", "bar1"));

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "BBB", &value));
  ASSERT_EQ(value, "bar1");
  s = txn->Put("BBB", "bar2");
  ASSERT_OK(s);

  ASSERT_OK(db->Put(write_options, "CCC", "bar1"));

  // Set a new snapshot
  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  txn->GetForUpdate(snapshot_read_options, "CCC", &value);
  ASSERT_EQ(value, "bar1");
  s = txn->Put("CCC", "bar2");
  ASSERT_OK(s);

  s = txn->GetForUpdate(read_options, "AAA", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = txn->GetForUpdate(read_options, "BBB", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = txn->GetForUpdate(read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  s = db->Get(read_options, "AAA", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar1");
  s = db->Get(read_options, "BBB", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar1");
  s = db->Get(read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar1");

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "AAA", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = db->Get(read_options, "BBB", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");
  s = db->Get(read_options, "CCC", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar2");

  // verify that we track multiple writes to the same key at different snapshots
  delete txn;
  txn = db->BeginTransaction(write_options);

  // Potentially conflicting writes
  db->Put(write_options, "ZZZ", "zzz");
  db->Put(write_options, "XXX", "xxx");

  txn->SetSnapshot();

  TransactionOptions txn_options;
  txn_options.set_snapshot = true;
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  txn2->SetSnapshot();

  // This should not conflict in txn since the snapshot is later than the
  // previous write (spoiler alert:  it will later conflict with txn2).
  s = txn->Put("ZZZ", "zzzz");
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;

  // This will conflict since the snapshot is earlier than another write to ZZZ
  s = txn2->Put("ZZZ", "xxxxx");
  ASSERT_TRUE(s.IsBusy());

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "ZZZ", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "zzzz");

  delete txn2;
}
/**
 * @brief     Use UntrackedTest to test Transaction, and verify the actions of Untracked* API.
 * @param     TransactionFunTest --- Class name. \n
 *            UntrackedTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, UntrackedTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  // Verify transaction rollback works for untracked keys.
  Transaction* txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  s = txn->PutUntracked("untracked", "0");
  ASSERT_OK(s);
  txn->Rollback();
  //fail at this 
  s = db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();

  s = db->Put(write_options, "untracked", "x");
  ASSERT_OK(s);

  // Untracked writes should succeed even though key was written after snapshot
  s = txn->PutUntracked("untracked", "1");
  ASSERT_OK(s);
  //s = txn->MergeUntracked("untracked", "2");
  //ASSERT_OK(s);
  s = txn->DeleteUntracked("untracked");
  ASSERT_OK(s);

  // Conflict
  s = txn->Put("untracked", "3");
  ASSERT_TRUE(s.IsBusy());
  
  //Add by 66 to test SingleDeleteUntracked API
   s = txn->PutUntracked("untracked1", "11");
  ASSERT_OK(s);
  s = txn->SingleDeleteUntracked("untracked1");
  ASSERT_OK(s);
  
  s = txn->Put("untracked", "4");
  ASSERT_TRUE(s.IsBusy());
  //Add end
  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}
/**
 * @brief     Use IteratorTest to test Transaction, and verify the actions of GetIterator API.
 * @param     TransactionFunTest --- Class name. \n
 *            IteratorTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, IteratorTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
  Status s;

  // Write some keys to the db
  s = db->Put(write_options, "A", "a");
  ASSERT_OK(s);

  s = db->Put(write_options, "G", "g");
  ASSERT_OK(s);

  s = db->Put(write_options, "F", "f");
  ASSERT_OK(s);

  s = db->Put(write_options, "C", "c");
  ASSERT_OK(s);

  s = db->Put(write_options, "D", "d");
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  // Write some keys in a txn
  s = txn->Put("B", "b");
  ASSERT_OK(s);

  s = txn->Put("H", "h");
  ASSERT_OK(s);

  s = txn->Delete("D");
  ASSERT_OK(s);

  s = txn->Put("E", "e");
  ASSERT_OK(s);

  txn->SetSnapshot();
  const Snapshot* snapshot = txn->GetSnapshot();

  // Write some keys to the db after the snapshot
  s = db->Put(write_options, "BB", "xx");
  ASSERT_OK(s);

  s = db->Put(write_options, "C", "xx");
  ASSERT_OK(s);

  read_options.snapshot = snapshot;
  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_OK(iter->status());
  iter->SeekToFirst();

  // Read all keys via iter and lock them all
  std::string results[] = {"a", "b", "c", "e", "f", "g", "h"};
  for (int i = 0; i < 7; i++) {
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(results[i], iter->value().ToString());

    s = txn->GetForUpdate(read_options, iter->key(), nullptr);
    if (i == 2) {
      // "C" was modified after txn's snapshot
      ASSERT_TRUE(s.IsBusy());
    } else {
      ASSERT_OK(s);
    }

    iter->Next();
  }
  ASSERT_FALSE(iter->Valid());

  iter->Seek("G");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("g", iter->value().ToString());

  iter->Prev();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("f", iter->value().ToString());

  iter->Seek("D");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("e", iter->value().ToString());

  iter->Seek("C");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("c", iter->value().ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("e", iter->value().ToString());

  iter->Seek("");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("a", iter->value().ToString());

  iter->Seek("X");
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());

  iter->SeekToLast();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("h", iter->value().ToString());

  s = txn->Commit();
  ASSERT_OK(s);

  delete iter;
  delete txn;
}
/**
 * @brief     Use SavepointTest to test Transaction, and verify the actions of GetNum* API.
 * @param     TransactionFunTest --- Class name. \n
 *            SavepointTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */

TEST_F(TransactionFunTest, SavepointTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  ASSERT_EQ(0, txn->GetNumPuts());

  s = txn->RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());

  txn->SetSavePoint();  // 1

  ASSERT_OK(txn->RollbackToSavePoint());  // Rollback to beginning of txn
  s = txn->RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Put("B", "b");
  ASSERT_OK(s);

  ASSERT_EQ(1, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Put("B", "bb");
  ASSERT_OK(s);

  s = txn->Put("C", "c");
  ASSERT_OK(s);

  txn->SetSavePoint();  // 2

  s = txn->Delete("B");
  ASSERT_OK(s);

  s = txn->Put("C", "cc");
  ASSERT_OK(s);

  s = txn->Put("D", "d");
  ASSERT_OK(s);

  ASSERT_EQ(5, txn->GetNumPuts());
  ASSERT_EQ(1, txn->GetNumDeletes());

  ASSERT_OK(txn->RollbackToSavePoint());  // Rollback to 2

  ASSERT_EQ(3, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  s = txn->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("bb", value);

  s = txn->Get(read_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c", value);

  s = txn->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Put("E", "e");
  ASSERT_OK(s);

  ASSERT_EQ(5, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  // Rollback to beginning of txn
  s = txn->RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  txn->Rollback();

  ASSERT_EQ(0, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  s = txn->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Get(read_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Put("A", "aa");
  ASSERT_OK(s);

  s = txn->Put("F", "f");
  ASSERT_OK(s);

  ASSERT_EQ(2, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  txn->SetSavePoint();  // 3
  txn->SetSavePoint();  // 4

  s = txn->Put("G", "g");
  ASSERT_OK(s);

  s = txn->SingleDelete("F");
  ASSERT_OK(s);

  s = txn->Delete("B");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("aa", value);

  s = txn->Get(read_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_EQ(3, txn->GetNumPuts());
  ASSERT_EQ(2, txn->GetNumDeletes());

  ASSERT_OK(txn->RollbackToSavePoint());  // Rollback to 3

  ASSERT_EQ(2, txn->GetNumPuts());
  ASSERT_EQ(0, txn->GetNumDeletes());

  s = txn->Get(read_options, "F", &value);
  ASSERT_OK(s);
  ASSERT_EQ("f", value);

  s = txn->Get(read_options, "G", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "F", &value);
  ASSERT_OK(s);
  ASSERT_EQ("f", value);

  s = db->Get(read_options, "G", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("aa", value);

  s = db->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b", value);

  s = db->Get(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}
/**
 * @brief     Use SavepointTest2 to test Transaction, and verify the actions of RollbackToSavePoint API.
 * @param     TransactionFunTest --- Class name. \n
 *            SavepointTest2 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, SavepointTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  Status s;

  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  s = txn1->Put("A", "");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 1

  s = txn1->Put("A", "a");
  ASSERT_OK(s);

  s = txn1->Put("C", "c");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 2

  s = txn1->Put("A", "a");
  ASSERT_OK(s);
  s = txn1->Put("B", "b");
  ASSERT_OK(s);

  ASSERT_OK(txn1->RollbackToSavePoint());  // Rollback to 2

  // Verify that "A" and "C" is still locked while "B" is not
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn2->Put("A", "a2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b2");
  ASSERT_OK(s);

  s = txn1->Put("A", "aa");
  ASSERT_OK(s);
  s = txn1->Put("B", "bb");
  ASSERT_TRUE(s.IsTimedOut());

  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn1->Put("A", "aaa");
  ASSERT_OK(s);
  s = txn1->Put("B", "bbb");
  ASSERT_OK(s);
  s = txn1->Put("C", "ccc");
  ASSERT_OK(s);

  txn1->SetSavePoint();                    // 3
  ASSERT_OK(txn1->RollbackToSavePoint());  // Rollback to 3

  // Verify that "A", "B", "C" are still locked
  txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn2->Put("A", "a2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b2");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("C", "c2");
  ASSERT_TRUE(s.IsTimedOut());

  ASSERT_OK(txn1->RollbackToSavePoint());  // Rollback to 1

  // Verify that only "A" is locked
  s = txn2->Put("A", "a3");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put("B", "b3");
  ASSERT_OK(s);
  s = txn2->Put("C", "c3po");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;

  // Verify "A" "C" "B" are no longer locked
  s = txn2->Put("A", "a4");
  ASSERT_OK(s);
  s = txn2->Put("B", "b4");
  ASSERT_OK(s);
  s = txn2->Put("C", "c4");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;
}
/**
 * @brief     Use WaitingTxn to test Transaction, and verify the actions of GetWaitingTxns API.
 * @param     TransactionFunTest --- Class name. \n
 *            WaitingTxn --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, WaitingTxn) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  txn_options.lock_timeout = 1;
  s = db->Put(write_options, Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  /* create second cf */
  ColumnFamilyHandle* cfa;
  ColumnFamilyOptions cf_options;
  s = db->CreateColumnFamily(cf_options, "CFA", &cfa);
  ASSERT_OK(s);
  s = db->Put(write_options, cfa, Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  TransactionID id1 = txn1->GetID();
  ASSERT_TRUE(txn1);
  ASSERT_TRUE(txn2);

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "TransactionLockMgr::AcquireWithTimeout:WaitingTxn", [&](void* /*arg*/) {
        std::string key;
        uint32_t cf_id;
        std::vector<TransactionID> wait = txn2->GetWaitingTxns(&cf_id, &key);
        ASSERT_EQ(key, "foo");
        ASSERT_EQ(wait.size(), 1);
        ASSERT_EQ(wait[0], id1);
        ASSERT_EQ(cf_id, 0);
      });

  //get_perf_context()->Reset();
  // lock key in default cf
  s = txn1->GetForUpdate(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");
  //ASSERT_EQ(get_perf_context()->key_lock_wait_count, 0);

  // lock key in cfa
  s = txn1->GetForUpdate(read_options, cfa, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "bar");
  //ASSERT_EQ(get_perf_context()->key_lock_wait_count, 0);

  auto lock_data = db->GetLockStatusData();
  // Locked keys exist in both column family.
  ASSERT_EQ(lock_data.size(), 2);

  auto cf_iterator = lock_data.begin();

  // The iterator points to an unordered_multimap
  // thus the test can not assume any particular order.

  // Column family is 1 or 0 (cfa).
  if (cf_iterator->first != 1 && cf_iterator->first != 0) {
    FAIL();
  }
  // The locked key is "foo" and is locked by txn1
  ASSERT_EQ(cf_iterator->second.key, "foo");
  ASSERT_EQ(cf_iterator->second.ids.size(), 1);
  ASSERT_EQ(cf_iterator->second.ids[0], txn1->GetID());

  cf_iterator++;

  // Column family is 0 (default) or 1.
  if (cf_iterator->first != 1 && cf_iterator->first != 0) {
    FAIL();
  }
  // The locked key is "foo" and is locked by txn1
  ASSERT_EQ(cf_iterator->second.key, "foo");
  ASSERT_EQ(cf_iterator->second.ids.size(), 1);
  ASSERT_EQ(cf_iterator->second.ids[0], txn1->GetID());

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  s = txn2->GetForUpdate(read_options, "foo", &value);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");
  //ASSERT_EQ(get_perf_context()->key_lock_wait_count, 1);
  //ASSERT_GE(get_perf_context()->key_lock_wait_time, 0);

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

  delete cfa;
  delete txn1;
  delete txn2;
}
/**
 * @brief     Use TransactionFunTest to test Transaction, and verify the actions of deadlock_detect API.
 * @param     TransactionFunTest --- Class name. \n
 *            DeadlockStress --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, DeadlockStress) {
  const uint32_t NUM_TXN_THREADS = 10;
  const uint32_t NUM_KEYS = 100;
  const uint32_t NUM_ITERS = 10000;

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;

  txn_options.lock_timeout = 1000000;
  txn_options.deadlock_detect = true;
  std::vector<std::string> keys;

  for (uint32_t i = 0; i < NUM_KEYS; i++) {
    db->Put(write_options, Slice(ToString(i)), Slice(""));
    keys.push_back(ToString(i));
  }

  size_t tid = std::hash<std::thread::id>()(std::this_thread::get_id());
  Random rnd(static_cast<uint32_t>(tid));
  std::function<void(uint32_t)> stress_thread = [&](uint32_t seed) {
    std::default_random_engine g(seed);

    Transaction* txn;
    for (uint32_t i = 0; i < NUM_ITERS; i++) {
      txn = db->BeginTransaction(write_options, txn_options);
      auto random_keys = keys;
      std::shuffle(random_keys.begin(), random_keys.end(), g);

      // Lock keys in random order.
      for (const auto& k : random_keys) {
        // Lock mostly for shared access, but exclusive 1/4 of the time.
        auto s =
            txn->GetForUpdate(read_options, k, nullptr, txn->GetID() % 4 == 0);
        if (!s.ok()) {
          ASSERT_TRUE(s.IsDeadlock());
          txn->Rollback();
          break;
        }
      }

      delete txn;
    }
  };

  std::vector<port::Thread> threads;
  for (uint32_t i = 0; i < NUM_TXN_THREADS; i++) {
    threads.emplace_back(stress_thread, rnd.Next());
  }

  for (auto& t : threads) {
    t.join();
  }
}
/**
 * @brief     Use SuccessTest to test Transaction, and verify the actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            SuccessTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */ 
TEST_F(TransactionFunTest, SuccessTest) {
  //ASSERT_OK(db->ResetStats());
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  ASSERT_OK(db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_OK(db->Put(write_options, Slice("foo2"), Slice("bar")));

  Transaction* txn = db->BeginTransaction(write_options, TransactionOptions());
  ASSERT_TRUE(txn);

  ASSERT_EQ(0, txn->GetNumPuts());
  ASSERT_LE(0, txn->GetID());

  ASSERT_OK(txn->GetForUpdate(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  ASSERT_OK(txn->Put(Slice("foo"), Slice("bar2")));

  ASSERT_EQ(1, txn->GetNumPuts());

  ASSERT_OK(txn->GetForUpdate(read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  ASSERT_OK(txn->Commit());

  ASSERT_OK(db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  delete txn;
}
/**
 * @brief     Use GetNumTest to test Transaction, and verify the actions of GetNum* API.
 * @param     TransactionFunTest --- Class name. \n
 *            GetNumTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, GetNumTest) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  ASSERT_EQ(0, txn->GetNumPuts());
  ASSERT_LE(0, txn->GetID());

  s = txn->Put("66", "test");
  ASSERT_OK(s);
  ASSERT_EQ(1, txn->GetNumPuts());  
  ASSERT_EQ(0, txn->GetNumDeletes());

  s = txn->Put("77", "test1");
  ASSERT_OK(s);
  ASSERT_EQ(2, txn->GetNumPuts());
  
  s = txn->Put("78", "test");
  ASSERT_OK(s);
  ASSERT_EQ(3, txn->GetNumPuts());
  
  ASSERT_OK(txn->Delete("78"));
  ASSERT_EQ(1, txn->GetNumDeletes());
  
  s = txn->Merge("77", "test2");
  ASSERT_OK(s);
  ASSERT_EQ(1, txn->GetNumMerges());
  
  delete txn;
}


/**
 * @brief     Use LogNumTest to test Transaction, and verify the actions of *LogNum API.
 * @param     TransactionFunTest --- Class name. \n
 *            LogNumTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, LogNumTest) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  std::string value;
  Status s;
  uint64_t  current_log;
  
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);
  
  //set number is null
  txn->SetLogNumber("");
  
  current_log = txn->GetLogNumber();
  ASSERT_EQ("",current_log);
  //set number is 1
  txn->SetLogNumber(1);
  current_log = txn->GetLogNumber();
  ASSERT_EQ(1,current_log);
  
  //set number is uint64_t's max = 2^64
  txn->SetLogNumber(UINT64_MAX);
  
  current_log = txn->GetLogNumber();
  ASSERT_EQ(UINT64_MAX,current_log);
  
  delete txn;
}

/**
 * @brief     Use StateTest to test Transaction, and verify the actions of Set/GetState API.
 * @param     TransactionFunTest --- Class name. \n
 *            StateTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, StateTest) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  Status s;
  Transaction::TransactionState txn_state;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);
  
  ASSERT_EQ(0,txn->GetState());
  
  txn_state = 1;
  txn->SetState(txn_state);
  ASSERT_EQ(1,txn->GetState());
  
  
  txn_state = 3;
  txn->SetState(txn_state);
  ASSERT_EQ(3,txn->GetState());
  
  txn_state = 7;
  txn->SetState(txn_state);
  ASSERT_EQ(7,txn->GetState());
  
  txn_state = 10;
  txn->SetState(txn_state);
  ASSERT_EQ(10,txn->GetState());
  
  delete txn;
}

/**
 * @brief     Use PutLogDataTest to test Transaction, and verify the actions of PutLogData API.
 * @param     TransactionFunTest --- Class name. \n
 *            PutLogDataTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    This Test case is deleted,because kvdb has no log
 */
/*TEST_F(TransactionFunTest, PutLogDataTest) {
  TransactionOptions txn_options;
  Status s;
  Transaction* txn = db->BeginTransaction(WriteOptions(), txn_options);
  ASSERT_TRUE(txn);
  cout <<"put start"<<endl;
  s = txn->Put("66", "test");
  ASSERT_OK(s);
  cout <<"put stop"<<endl;
  cout <<"PutLogData start"<<endl;
  txn->PutLogData("test66");
  cout <<"PutLogData stop"<<endl;
  
  ASSERT_OK(txn->Commit());
  
  delete txn;
}*/
/**
 * @brief     Use CommitTimeBatchFailTest to test Transaction, and verify the actions of GetCommitTimeWriteBatch API.
 * @param     TransactionFunTest --- Class name. \n
 *            CommitTimeBatchFailTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */

TEST_F(TransactionFunTest, CommitTimeBatchFailTest) {
  WriteOptions write_options;
  TransactionOptions txn_options;

  std::string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);

  ASSERT_OK(txn1->GetCommitTimeWriteBatch()->Put("cat", "dog"));

  s = txn1->Put("foo", "bar");
  
  ASSERT_OK(s);

  // fails due to non-empty commit-time batch
  s = txn1->Commit();
  ASSERT_EQ(s, Status::InvalidArgument());

  delete txn1;
  
}

/**
 * @brief     Use FlushTest to test Transaction, and verify the actions of Flush API.
 * @param     TransactionFunTest --- Class name. \n
 *            FlushTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, FlushTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
  Status s;

  db->Put(write_options, Slice("foo"), Slice("bar"));
  db->Put(write_options, Slice("foo2"), Slice("bar"));

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar");

  s = txn->Put(Slice("foo"), Slice("bar2"));
  ASSERT_OK(s);

  txn->GetForUpdate(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  // Put a random key 
  s = db->Put(write_options, "dummy", "dummy");
  ASSERT_OK(s);

  //  flush
  FlushOptions flush_ops;
  db->Flush(flush_ops);

  s = txn->Commit();
  ASSERT_OK(s);

  db->Get(read_options, "foo", &value);
  ASSERT_EQ(value, "bar2");

  delete txn;
}

/**
 * @brief     Use FlushTest2 to test Transaction, and verify the actions of Flush API.
 * @param     TransactionFunTest --- Class name. \n
 *            FlushTest2 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, FlushTest2) {
  const size_t num_tests = 3;

  for (size_t n = 0; n < num_tests; n++) {
    // Test different table factories
    switch (n) {
      case 0:
        break;
      case 1:
        //options.table_factory.reset(new mock::MockTableFactory());
        break;
      case 2: {
        PlainTableOptions pt_opts;
        pt_opts.hash_table_ratio = 0;
        options.table_factory.reset(NewPlainTableFactory(pt_opts));
        break;
      }
    }
    Status s = ReOpen();
    ASSERT_OK(s);
    assert(db != nullptr);

    WriteOptions write_options;
    ReadOptions read_options, snapshot_read_options;
    TransactionOptions txn_options;
    string value;

    //DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());

    db->Put(write_options, Slice("foo"), Slice("bar"));
    db->Put(write_options, Slice("foo2"), Slice("bar2"));
    db->Put(write_options, Slice("foo3"), Slice("bar3"));

    txn_options.set_snapshot = true;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    ASSERT_TRUE(txn);

    snapshot_read_options.snapshot = txn->GetSnapshot();

    txn->GetForUpdate(snapshot_read_options, "foo", &value);
    ASSERT_EQ(value, "bar");

    s = txn->Put(Slice("foo"), Slice("bar2"));
    ASSERT_OK(s);

    txn->GetForUpdate(snapshot_read_options, "foo", &value);
    ASSERT_EQ(value, "bar2");
    // verify foo is locked by txn
    s = db->Delete(write_options, "foo");
    ASSERT_TRUE(s.IsTimedOut());

    s = db->Put(write_options, "Z", "z");
    ASSERT_OK(s);
    s = db->Put(write_options, "dummy", "dummy");
    ASSERT_OK(s);

    s = db->Put(write_options, "S", "s");
    ASSERT_OK(s);
    s = db->Delete(write_options, "S");
    ASSERT_OK(s);

    s = txn->Delete("S");
    // Should fail after encountering a write to S in memtable
    ASSERT_TRUE(s.IsBusy());
    ASSERT_OK(db->Flush(FlushOptions()));
    // force a memtable flush
    //s = db_impl->TEST_FlushMemTable(true);
    //ASSERT_OK(s);

    // Put a random key so we have a MemTable to flush
    s = db->Put(write_options, "dummy", "dummy2");
    ASSERT_OK(s);
    ASSERT_OK(db->Flush(FlushOptions()));
    // force a memtable flush
    //ASSERT_OK(db_impl->TEST_FlushMemTable(true));

    s = db->Put(write_options, "dummy", "dummy3");
    ASSERT_OK(s);
    ASSERT_OK(db->Flush(FlushOptions()));
    // force a memtable flush
    // Since our test db has max_write_buffer_number=2, this flush will cause
    // the first memtable to get purged from the MemtableList history.
    //ASSERT_OK(db_impl->TEST_FlushMemTable(true));

    s = txn->Put("X", "Y");
    // Should succeed after verifying there is no write to X in SST file
    ASSERT_OK(s);

    s = txn->Put("Z", "zz");
    // Should fail after encountering a write to Z in SST file
    ASSERT_TRUE(s.IsBusy());

    s = txn->GetForUpdate(read_options, "foo2", &value);
    // should succeed since key was written before txn started
    ASSERT_OK(s);
    // verify foo2 is locked by txn
    s = db->Delete(write_options, "foo2");
    ASSERT_TRUE(s.IsTimedOut());

    s = txn->Delete("S");
    // Should fail after encountering a write to S in SST file
    ASSERT_TRUE(s.IsBusy());

    // Write a bunch of keys to db to force a compaction
    Random rnd(47);
    for (int i = 0; i < 1000; i++) {
      s = db->Put(write_options, std::to_string(i),
                  test::CompressibleString(&rnd, 0.8, 100, &value));
      ASSERT_OK(s);
    }

    s = txn->Put("X", "yy");
    // Should succeed after verifying there is no write to X in SST file
    ASSERT_OK(s);

    s = txn->Put("Z", "zzz");
    // Should fail after encountering a write to Z in SST file
    ASSERT_TRUE(s.IsBusy());

    s = txn->Delete("S");
    // Should fail after encountering a write to S in SST file
    ASSERT_TRUE(s.IsBusy());

    s = txn->GetForUpdate(read_options, "foo3", &value);
    // should succeed since key was written before txn started
    ASSERT_OK(s);
    // verify foo3 is locked by txn
    s = db->Delete(write_options, "foo3");
    ASSERT_TRUE(s.IsTimedOut());

    //db_impl->TEST_WaitForCompact();

    s = txn->Commit();
    ASSERT_OK(s);

    // Transaction should only write the keys that succeeded.
    s = db->Get(read_options, "foo", &value);
    ASSERT_EQ(value, "bar2");

    s = db->Get(read_options, "X", &value);
    ASSERT_OK(s);
    ASSERT_EQ("yy", value);

    s = db->Get(read_options, "Z", &value);
    ASSERT_OK(s);
    ASSERT_EQ("z", value);

  delete txn;
  }
}

/**
 * @brief     Use ColumnFamiliesTest to test Transaction, and verify the actions of  API.
 * @param     TransactionFunTest --- Class name. \n
 *            ColumnFamiliesTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
 
TEST_F(TransactionFunTest, ColumnFamiliesTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  ColumnFamilyHandle *cfa, *cfb;
  ColumnFamilyOptions cf_options;
  // Create 2 new column families
  s = db->CreateColumnFamily(cf_options, "CFA", &cfa);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "CFB", &cfb);
  ASSERT_OK(s);

  delete cfa;
  delete cfb;
  delete db;
  db = nullptr;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new column families
  column_families.push_back(
      ColumnFamilyDescriptor("CFA", ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("CFB", ColumnFamilyOptions()));

  std::vector<ColumnFamilyHandle*> handles;

  s = TransactionDB::Open(options, txn_db_options, dbname, column_families,
                          &handles, &db);
  assert(db != nullptr);
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn_options.set_snapshot = true;
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  // Write some data to the db
  WriteBatch batch;
  batch.Put("foo", "foo");
  batch.Put(handles[1], "AAA", "bar");
  batch.Put(handles[1], "AAAZZZ", "bar");
  s = db->Write(write_options, &batch);
  ASSERT_OK(s);
  ASSERT_OK(db->Delete(write_options, handles[1], "AAAZZZ"));

  // These keys do not conflict with existing writes since they're in
  // different column families
  s = txn->Delete("AAA");
  ASSERT_OK(s);
  s = txn->GetForUpdate(snapshot_read_options, handles[1], "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  Slice key_slice("AAAZZZ");
  Slice value_slices[2] = {Slice("bar"), Slice("bar")};
  s = txn->Put(handles[2], SliceParts(&key_slice, 1),
               SliceParts(value_slices, 2));
  ASSERT_OK(s);
  ASSERT_EQ(3, txn->GetNumKeys());

  s = txn->Commit();
  ASSERT_OK(s);
  s = db->Get(read_options, "AAA", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = db->Get(read_options, handles[2], "AAAZZZ", &value);
  ASSERT_EQ(value, "barbar");

  Slice key_slices[3] = {Slice("AAA"), Slice("ZZ"), Slice("Z")};
  Slice value_slice("barbarbar");

  s = txn2->Delete(handles[2], "XXX");
  ASSERT_OK(s);
  s = txn2->Delete(handles[1], "XXX");
  ASSERT_OK(s);

  // This write will cause a conflict with the earlier batch write
  s = txn2->Put(handles[1], SliceParts(key_slices, 3),
                SliceParts(&value_slice, 1));
  ASSERT_TRUE(s.IsBusy());

  s = txn2->Commit();
  ASSERT_OK(s);
  // In the above the latest change to AAAZZZ in handles[1] is delete.
  s = db->Get(read_options, handles[1], "AAAZZZ", &value);
  ASSERT_TRUE(s.IsNotFound());
  delete txn;
  delete txn2;
  txn = db->BeginTransaction(write_options, txn_options);
  snapshot_read_options.snapshot = txn->GetSnapshot();
  txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);
  std::vector<ColumnFamilyHandle*> multiget_cfh = {handles[1], handles[2],
                                                   handles[0], handles[2]};
  std::vector<Slice> multiget_keys = {"AAA", "AAAZZZ", "foo", "foo"};
  std::vector<std::string> values(4);
  std::vector<Status> results = txn->MultiGetForUpdate(
      snapshot_read_options, multiget_cfh, multiget_keys, &values);
  ASSERT_OK(results[0]);
  ASSERT_OK(results[1]);
  ASSERT_OK(results[2]);
  ASSERT_TRUE(results[3].IsNotFound());
  ASSERT_EQ(values[0], "bar");
  ASSERT_EQ(values[1], "barbar");
  ASSERT_EQ(values[2], "foo");
  s = txn->SingleDelete(handles[2], "ZZZ");
  ASSERT_OK(s);
  s = txn->Put(handles[2], "ZZZ", "YYY");
  ASSERT_OK(s);
  s = txn->Put(handles[2], "ZZZ", "YYYY");
  ASSERT_OK(s);
  s = txn->Delete(handles[2], "ZZZ");
  ASSERT_OK(s);
  s = txn->Put(handles[2], "AAAZZZ", "barbarbar");
  ASSERT_OK(s);

  ASSERT_EQ(5, txn->GetNumKeys());
  // Txn should commit
  s = txn->Commit();
  ASSERT_OK(s);
  s = db->Get(read_options, handles[2], "ZZZ", &value);
  ASSERT_TRUE(s.IsNotFound());
  // Put a key which will conflict with the next txn using the previous snapshot
  db->Put(write_options, handles[2], "foo", "000");
  results = txn2->MultiGetForUpdate(snapshot_read_options, multiget_cfh,
                                    multiget_keys, &values);
  // All results should fail since there was a conflict
  ASSERT_TRUE(results[0].IsBusy());
  ASSERT_TRUE(results[1].IsBusy());
  ASSERT_TRUE(results[2].IsBusy());
  ASSERT_TRUE(results[3].IsBusy());
  s = db->Get(read_options, handles[2], "foo", &value);
  ASSERT_EQ(value, "000");
  s = txn2->Commit();
  ASSERT_OK(s);
  s = db->DropColumnFamily(handles[1]);
  ASSERT_OK(s);
  s = db->DropColumnFamily(handles[2]);
  ASSERT_OK(s);
  delete txn;
  delete txn2;
  for (auto handle : handles) {
    delete handle;
  }
  db = nullptr;
  delete db;
}
/**
 * @brief     Use ColumnFamiliesTest2 to test Transaction, and verify the actions of  API.
 * @param     TransactionFunTest --- Class name. \n
 *            ColumnFamiliesTest2 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, ColumnFamiliesTest2) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  string value;
  Status s;

  ColumnFamilyHandle *one, *two;
  ColumnFamilyOptions cf_options;

  // Create 2 new column families
  s = db->CreateColumnFamily(cf_options, "ONE", &one);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "TWO", &two);
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn1);
  Transaction* txn2 = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn2);

  s = txn1->Put(one, "X", "1");
  ASSERT_OK(s);
  s = txn1->Put(two, "X", "2");
  ASSERT_OK(s);
  s = txn1->Put("X", "0");
  ASSERT_OK(s);

  s = txn2->Put(one, "X", "11");
  ASSERT_TRUE(s.IsTimedOut());

  s = txn1->Commit();
  ASSERT_OK(s);

  // Drop first column family
  s = db->DropColumnFamily(one);
  ASSERT_OK(s);

  // Should fail since column family was dropped.
  s = txn2->Commit();
  ASSERT_OK(s);

  delete txn1;
  txn1 = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn1);

  // Should fail since column family was dropped
  s = txn1->Put(one, "X", "111");
  ASSERT_TRUE(s.IsInvalidArgument());

  s = txn1->Put(two, "X", "222");
  ASSERT_OK(s);

  s = txn1->Put("X", "000");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, two, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("222", value);

  s = db->Get(read_options, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("000", value);

  s = db->DropColumnFamily(two);
  ASSERT_OK(s);

  delete txn1;
  delete txn2;

  delete one;
  delete two;
}
/**
 * @brief     Use PredicateManyPreceders to test Transaction, and verify the actions of MultiGetForUpdate API.
 * @param     TransactionFunTest --- Class name. \n
 *            PredicateManyPreceders --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, PredicateManyPreceders) {
  WriteOptions write_options;
  ReadOptions read_options1, read_options2;
  TransactionOptions txn_options;
  string value;
  Status s;

  txn_options.set_snapshot = true;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  Transaction* txn2 = db->BeginTransaction(write_options);
  txn2->SetSnapshot();
  read_options2.snapshot = txn2->GetSnapshot();

  std::vector<Slice> multiget_keys = {"1", "2", "3"};
  std::vector<std::string> multiget_values;

  std::vector<Status> results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_TRUE(results[1].IsNotFound());

  s = txn2->Put("2", "x");  // Conflict's with txn1's MultiGetForUpdate
  ASSERT_TRUE(s.IsTimedOut());

  txn2->Rollback();

  multiget_values.clear();
  results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_TRUE(results[1].IsNotFound());

  s = txn1->Commit();
  ASSERT_OK(s);

  delete txn1;
  delete txn2;

  txn1 = db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  s = txn1->Put("4", "x");
  ASSERT_OK(s);

  s = txn2->Delete("4");  // conflict
  ASSERT_TRUE(s.IsTimedOut());

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options2, "4", &value);
  ASSERT_TRUE(s.IsBusy());

  txn2->Rollback();

  delete txn1;
  delete txn2;
}
/**
 * @brief     Use ExpiredTransaction to test Transaction, and verify the actions of expiration API.
 * @param     TransactionFunTest --- Class name. \n
 *            ExpiredTransaction --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */

TEST_F(TransactionFunTest, ExpiredTransaction) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  // Set txn expiration timeout to 0 microseconds (expires instantly)
  txn_options.expiration = 0;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  s = txn1->Put("X", "1");
  ASSERT_OK(s);

  s = txn1->Put("Y", "1");
  ASSERT_OK(s);

  Transaction* txn2 = db->BeginTransaction(write_options);

  // txn2 should be able to write to X since txn1 has expired
  s = txn2->Put("X", "2");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);
  s = db->Get(read_options, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("2", value);

  s = txn1->Put("Z", "1");
  ASSERT_OK(s);

  // txn1 should fail to commit since it is expired
  s = txn1->Commit();
  ASSERT_TRUE(s.IsExpired());

  s = db->Get(read_options, "Y", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "Z", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn1;
  delete txn2;
}
/**
 * @brief     Use ExpiredTransactionDataRace1 to test Transaction, and verify the actions of expiration API.
 * @param     TransactionFunTest --- Class name. \n
 *            ExpiredTransactionDataRace1 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */

TEST_F(TransactionFunTest, ExpiredTransactionDataRace1) {
  // In this test, txn1 should succeed committing,
  // as the callback is called after txn1 starts committing.
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"TransactionFunTest::ExpirableTransactionDataRace:1"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "TransactionFunTest::ExpirableTransactionDataRace:1", [&](void* /*arg*/) {
        WriteOptions write_options;
        TransactionOptions txn_options;

        // Force txn1 to expire
        /* sleep override */
        std::this_thread::sleep_for(std::chrono::milliseconds(150));

        Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
        Status s;
        s = txn2->Put("X", "2");
        ASSERT_TRUE(s.IsTimedOut());
        s = txn2->Commit();
        ASSERT_OK(s);
        delete txn2;
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions write_options;
  TransactionOptions txn_options;

  txn_options.expiration = 100;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  Status s;
  s = txn1->Put("X", "1");
  ASSERT_OK(s);
  s = txn1->Commit();
  ASSERT_OK(s);

  ReadOptions read_options;
  string value;
  s = db->Get(read_options, "X", &value);
  ASSERT_EQ("1", value);

  delete txn1;
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

/**
 * @brief     Use ReinitializeTest to test Transaction, and verify the actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            ReinitializeTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, ReinitializeTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  // Set txn expiration timeout to 0 microseconds (expires instantly)
  txn_options.expiration = 0;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // Reinitialize transaction to no long expire
  txn_options.expiration = -1;
  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  s = txn1->Put("Z", "z");
  ASSERT_OK(s);

  // Should commit since not expired
  s = txn1->Commit();
  ASSERT_OK(s);

  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  s = txn1->Put("Z", "zz");
  ASSERT_OK(s);

  // Reinitilize txn1 and verify that Z gets unlocked
  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  Transaction* txn2 = db->BeginTransaction(write_options, txn_options, nullptr);
  s = txn2->Put("Z", "zzz");
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = db->Get(read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "zzz");

  // Verify snapshots get reinitialized correctly
  txn1->SetSnapshot();
  s = txn1->Put("Z", "zzzz");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "zzzz");

  txn1 = db->BeginTransaction(write_options, txn_options, txn1);
  const Snapshot* snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot);

  txn_options.set_snapshot = true;
  txn1 = db->BeginTransaction(write_options, txn_options, txn1);
  snapshot = txn1->GetSnapshot();
  ASSERT_TRUE(snapshot);

  s = txn1->Put("Z", "a");
  ASSERT_OK(s);

  txn1->Rollback();

  s = txn1->Put("Y", "y");
  ASSERT_OK(s);

  txn_options.set_snapshot = false;
  txn1 = db->BeginTransaction(write_options, txn_options, txn1);
  snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot);

  s = txn1->Put("X", "x");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "zzzz");

  s = db->Get(read_options, "Y", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  s = txn1->SetName("name");
  ASSERT_OK(s);

  s = txn1->Prepare();
  ASSERT_OK(s);
  s = txn1->Commit();
  ASSERT_OK(s);

  txn1 = db->BeginTransaction(write_options, txn_options, txn1);

  s = txn1->SetName("name");
  ASSERT_OK(s);

  delete txn1;
}
/**
 * @brief     Use RollbackTest to test Transaction, and verify the actions of Rollback API.
 * @param     TransactionFunTest --- Class name. \n
 *            RollbackTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, RollbackTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  ASSERT_OK(s);

  s = txn1->Put("X", "1");
  ASSERT_OK(s);

  Transaction* txn2 = db->BeginTransaction(write_options);

  // txn2 should not be able to write to X since txn1 has it locked
  s = txn2->Put("X", "2");
  ASSERT_TRUE(s.IsTimedOut());

  txn1->Rollback();
  delete txn1;

  // txn2 should now be able to write to X
  s = txn2->Put("X", "3");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("3", value);

  delete txn2;
}
/**
 * @brief     Use LockLimitTest to test Transaction, and verify the actions of Rollback API.
 * @param     TransactionFunTest --- Class name. \n
 *            LockLimitTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, LockLimitTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  delete db;
  db = nullptr;

  // Open DB with a lock limit of 3
  txn_db_options.max_num_locks = 3;
  s = TransactionDB::Open(options, txn_db_options, dbname, &db);
  assert(db != nullptr);
  ASSERT_OK(s);

  // Create a txn and verify we can only lock up to 3 keys
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);

  s = txn->Put("X", "x");
  ASSERT_OK(s);

  s = txn->Put("Y", "y");
  ASSERT_OK(s);

  s = txn->Put("Z", "z");
  ASSERT_OK(s);

  // lock limit reached
  s = txn->Put("W", "w");
  ASSERT_TRUE(s.IsBusy());

  // re-locking same key shouldn't put us over the limit
  s = txn->Put("X", "xx");
  ASSERT_OK(s);

  s = txn->GetForUpdate(read_options, "W", &value);
  ASSERT_TRUE(s.IsBusy());
  s = txn->GetForUpdate(read_options, "V", &value);
  ASSERT_TRUE(s.IsBusy());

  // re-locking same key shouldn't put us over the limit
  s = txn->GetForUpdate(read_options, "Y", &value);
  ASSERT_OK(s);
  ASSERT_EQ("y", value);

  s = txn->Get(read_options, "W", &value);
  ASSERT_TRUE(s.IsNotFound());

  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  // "X" currently locked
  s = txn2->Put("X", "x");
  ASSERT_TRUE(s.IsTimedOut());

  // lock limit reached
  s = txn2->Put("M", "m");
  ASSERT_TRUE(s.IsBusy());

  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "X", &value);
  ASSERT_OK(s);
  ASSERT_EQ("xx", value);

  s = db->Get(read_options, "W", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Committing txn should release its locks and allow txn2 to proceed
  s = txn2->Put("X", "x2");
  ASSERT_OK(s);

  s = txn2->Delete("X");
  ASSERT_OK(s);

  s = txn2->Put("M", "m");
  ASSERT_OK(s);

  s = txn2->Put("Z", "z2");
  ASSERT_OK(s);

  // lock limit reached
  s = txn2->Delete("Y");
  ASSERT_TRUE(s.IsBusy());

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "Z", &value);
  ASSERT_OK(s);
  ASSERT_EQ("z2", value);

  s = db->Get(read_options, "Y", &value);
  ASSERT_OK(s);
  ASSERT_EQ("y", value);

  s = db->Get(read_options, "X", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  delete txn2;
}

/**
 * @brief     Use DisableIndexingTest to test Transaction, and verify the actions of EnableIndexing API.
 * @param     TransactionFunTest --- Class name. \n
 *            DisableIndexingTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, DisableIndexingTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  txn->DisableIndexing();

  s = txn->Put("B", "b");
  ASSERT_OK(s);

  s = txn->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_OK(iter->status());

  iter->Seek("B");
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());

  s = txn->Delete("A");
  ASSERT_OK(s);
  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  txn->EnableIndexing();

  s = txn->Put("B", "bb");
  ASSERT_OK(s);

  iter->Seek("B");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bb", iter->value().ToString());

  s = txn->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("bb", value);

  s = txn->Put("A", "aa");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("aa", value);

  delete iter;
  delete txn;
}

/**
 * @brief     Use TimeoutTest to test Transaction, and verify the actions of SetLockTimeout API.
 * @param     TransactionFunTest --- Class name. \n
 *            TimeoutTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*
TEST_F(TransactionFunTest, TimeoutTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  delete db;
  db = nullptr;

  // transaction writes have an infinite timeout,
  // but we will override this when we start a txn
  // db writes have infinite timeout
  txn_db_options.transaction_lock_timeout = -1;
  txn_db_options.default_lock_timeout = -1;

  s = TransactionDB::Open(options, txn_db_options, dbname, &db);
  assert(db != nullptr);
  ASSERT_OK(s);

  s = db->Put(write_options, "aaa", "aaa");
  ASSERT_OK(s);

  TransactionOptions txn_options0;
  txn_options0.expiration = 100;  // 100ms
  txn_options0.lock_timeout = 50;  // txn timeout no longer infinite
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options0);

  s = txn1->GetForUpdate(read_options, "aaa", nullptr);
  ASSERT_OK(s);

  // Conflicts with previous GetForUpdate.
  // Since db writes do not have a timeout, this should eventually succeed when
  // the transaction expires.
  s = db->Put(write_options, "aaa", "xxx");
  ASSERT_OK(s);

  ASSERT_GE(txn1->GetElapsedTime(),
            static_cast<uint64_t>(txn_options0.expiration));

  s = txn1->Commit();
  ASSERT_TRUE(s.IsExpired());  // expired!

  s = db->Get(read_options, "aaa", &value);
  ASSERT_OK(s);
  ASSERT_EQ("xxx", value);

  delete txn1;
  delete db;

  // transaction writes have 10ms timeout,
  // db writes have infinite timeout
  txn_db_options.transaction_lock_timeout = 50;
  txn_db_options.default_lock_timeout = -1;

  s = TransactionDB::Open(options, txn_db_options, dbname, &db);
  ASSERT_OK(s);

  s = db->Put(write_options, "aaa", "aaa");
  ASSERT_OK(s);

  TransactionOptions txn_options;
  txn_options.expiration = 100;  // 100ms
  txn1 = db->BeginTransaction(write_options, txn_options);

  s = txn1->GetForUpdate(read_options, "aaa", nullptr);
  ASSERT_OK(s);

  // Conflicts with previous GetForUpdate.
  // Since db writes do not have a timeout, this should eventually succeed when
  // the transaction expires.
  s = db->Put(write_options, "aaa", "xxx");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_NOK(s);  // expired!

  s = db->Get(read_options, "aaa", &value);
  ASSERT_OK(s);
  ASSERT_EQ("xxx", value);

  delete txn1;
  txn_options.expiration = 6000000;  // 100 minutes
  txn_options.lock_timeout = 1;      // 1ms
  txn1 = db->BeginTransaction(write_options, txn_options);
  txn1->SetLockTimeout(100);

  TransactionOptions txn_options2;
  txn_options2.expiration = 10;  // 10ms
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options2);
  ASSERT_OK(s);

  s = txn2->Put("a", "2");
  ASSERT_OK(s);

  // txn1 has a lock timeout longer than txn2's expiration, so it will win
  s = txn1->Delete("a");
  ASSERT_OK(s);

  s = txn1->Commit();
  ASSERT_OK(s);

  // txn2 should be expired out since txn1 waiting until its timeout expired.
  s = txn2->Commit();
  ASSERT_TRUE(s.IsExpired());

  delete txn1;
  delete txn2;
  txn_options.expiration = 6000000;  // 100 minutes
  txn1 = db->BeginTransaction(write_options, txn_options);
  txn_options2.expiration = 100000000;
  txn2 = db->BeginTransaction(write_options, txn_options2);

  s = txn1->Delete("asdf");
  ASSERT_OK(s);

  // txn2 has a smaller lock timeout than txn1's expiration, so it will time out
  s = txn2->Delete("asdf");
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Put("asdf", "asdf");
  ASSERT_OK(s);

  s = txn2->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "asdf", &value);
  ASSERT_OK(s);
  ASSERT_EQ("asdf", value);

  delete txn1;
  delete txn2;
}
*/

/**
 * @brief     Use SingleDeleteTest to test Transaction, and verify the actions of SingleDelete API.
 * @param     TransactionFunTest --- Class name. \n
 *            SingleDeleteTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, SingleDeleteTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = txn->SingleDelete("A");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  txn = db->BeginTransaction(write_options);

  s = txn->SingleDelete("A");
  ASSERT_OK(s);

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  txn = db->BeginTransaction(write_options);

  s = txn->SingleDelete("A");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  s = db->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  txn = db->BeginTransaction(write_options);
  Transaction* txn2 = db->BeginTransaction(write_options);
  txn2->SetSnapshot();

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Put("A", "a2");
  ASSERT_OK(s);

  s = txn->SingleDelete("A");
  ASSERT_OK(s);

  s = txn->SingleDelete("B");
  ASSERT_OK(s);

  // According to db.h, doing a SingleDelete on a key that has been
  // overwritten will have undefinied behavior.  So it is unclear what the
  // result of fetching "A" should be. The current implementation will
  // return NotFound in this case.
  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn2->Put("B", "b");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  // According to db.h, doing a SingleDelete on a key that has been
  // overwritten will have undefinied behavior.  So it is unclear what the
  // result of fetching "A" should be. The current implementation will
  // return NotFound in this case.
  s = db->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = db->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());
}
/**
 * @brief     Use MergeTest to test Transaction, and verify the actions of Merge API.
 * @param     TransactionFunTest --- Class name. \n
 *            MergeTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, MergeTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options, TransactionOptions());
  ASSERT_TRUE(txn);

  s = db->Put(write_options, "A", "a0");
  ASSERT_OK(s);

  s = txn->Merge("A", "1");
  ASSERT_OK(s);

  s = txn->Merge("A", "2");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsMergeInProgress());

  s = txn->Put("A", "a");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a", value);

  s = txn->Merge("A", "3");
  ASSERT_OK(s);

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsMergeInProgress());

  TransactionOptions txn_options;
  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  // verify that txn has "A" locked
  s = txn2->Merge("A", "4");
  ASSERT_TRUE(s.IsTimedOut());

  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn->Commit();
  ASSERT_OK(s);
  delete txn;

  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a,3", value);
}
/**
 * @brief     Use DeferSnapshotTest to test Transaction, and verify the actions of SetSnapshotOnNextOperation API.
 * @param     TransactionFunTest --- Class name. \n
 *            DeferSnapshotTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, DeferSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  s = db->Put(write_options, "A", "a0");
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options);
  Transaction* txn2 = db->BeginTransaction(write_options);

  txn1->SetSnapshotOnNextOperation();
  auto snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot);

  s = txn2->Put("A", "a2");
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;

  s = txn1->GetForUpdate(read_options, "A", &value);
  // Should not conflict with txn2 since snapshot wasn't set until
  // GetForUpdate was called.
  ASSERT_OK(s);
  ASSERT_EQ("a2", value);

  s = txn1->Put("A", "a1");
  ASSERT_OK(s);

  s = db->Put(write_options, "B", "b0");
  ASSERT_OK(s);

  // Cannot lock B since it was written after the snapshot was set
  s = txn1->Put("B", "b1");
  ASSERT_TRUE(s.IsBusy());

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;

  s = db->Get(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a1", value);

  s = db->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_EQ("b0", value);
}
/**
 * @brief     Use DeferSnapshotTest2 to test Transaction, and verify the actions of SetSnapshotOnNextOperation API.
 * @param     TransactionFunTest --- Class name. \n
 *            DeferSnapshotTest2 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, DeferSnapshotTest2) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options);

  txn1->SetSnapshot();

  s = txn1->Put("A", "a1");
  ASSERT_OK(s);

  s = db->Put(write_options, "C", "c0");
  ASSERT_OK(s);
  s = db->Put(write_options, "D", "d0");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();

  txn1->SetSnapshotOnNextOperation();

  s = txn1->Get(snapshot_read_options, "C", &value);
  // Snapshot was set before C was written
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->Get(snapshot_read_options, "D", &value);
  // Snapshot was set before D was written
  ASSERT_TRUE(s.IsNotFound());

  // Snapshot should not have changed yet.
  snapshot_read_options.snapshot = txn1->GetSnapshot();

  s = txn1->Get(snapshot_read_options, "C", &value);
  // Snapshot was set before C was written
  ASSERT_TRUE(s.IsNotFound());
  s = txn1->Get(snapshot_read_options, "D", &value);
  // Snapshot was set before D was written
  ASSERT_TRUE(s.IsNotFound());

  s = txn1->GetForUpdate(read_options, "C", &value);
  ASSERT_OK(s);
  ASSERT_EQ("c0", value);

  s = db->Put(write_options, "D", "d00");
  ASSERT_OK(s);

  // Snapshot is now set
  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "D", &value);
  ASSERT_OK(s);
  ASSERT_EQ("d0", value);

  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;
}
/**
 * @brief     Use DeferSnapshotSavePointTest to test Transaction, and verify the actions of RollbackToSavePoint API.
 * @param     TransactionFunTest --- Class name. \n
 *            DeferSnapshotSavePointTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, DeferSnapshotSavePointTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
  Status s;

  Transaction* txn1 = db->BeginTransaction(write_options);

  txn1->SetSavePoint();  // 1

  s = db->Put(write_options, "T", "1");
  ASSERT_OK(s);

  txn1->SetSnapshotOnNextOperation();

  s = db->Put(write_options, "T", "2");
  ASSERT_OK(s);
  txn1->SetSavePoint();  // 2

  s = db->Put(write_options, "T", "3");
  ASSERT_OK(s);

  s = txn1->Put("A", "a");
  ASSERT_OK(s);

  txn1->SetSavePoint();  // 3

  s = db->Put(write_options, "T", "4");
  ASSERT_OK(s);

  txn1->SetSnapshot();
  txn1->SetSnapshotOnNextOperation();

  txn1->SetSavePoint();  // 4

  s = db->Put(write_options, "T", "5");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("4", value);

  s = txn1->Put("A", "a1");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->RollbackToSavePoint();  // Rollback to 4
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("4", value);

  s = txn1->RollbackToSavePoint();  // Rollback to 3
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("3", value);

  s = txn1->Get(read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->RollbackToSavePoint();  // Rollback to 2
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot_read_options.snapshot);
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->Delete("A");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  ASSERT_TRUE(snapshot_read_options.snapshot);
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->RollbackToSavePoint();  // Rollback to 1
  ASSERT_OK(s);

  s = txn1->Delete("A");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn1->GetSnapshot();
  ASSERT_FALSE(snapshot_read_options.snapshot);
  s = txn1->Get(snapshot_read_options, "T", &value);
  ASSERT_OK(s);
  ASSERT_EQ("5", value);

  s = txn1->Commit();
  ASSERT_OK(s);

  delete txn1;
}
/**
 * @brief     Use SetSnapshotOnNextOperationWithNotification to test Transaction, and verify the actions of SetSnapshotOnNextOperation API.
 * @param     TransactionFunTest --- Class name. \n
 *            SetSnapshotOnNextOperationWithNotification --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, SetSnapshotOnNextOperationWithNotification) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  class Notifier : public TransactionNotifier {
   private:
    const Snapshot** snapshot_ptr_;

   public:
    explicit Notifier(const Snapshot** snapshot_ptr)
        : snapshot_ptr_(snapshot_ptr) {}

    void SnapshotCreated(const Snapshot* newSnapshot) {
      *snapshot_ptr_ = newSnapshot;
    }
  };

  std::shared_ptr<Notifier> notifier =
      std::make_shared<Notifier>(&read_options.snapshot);
  Status s;

  s = db->Put(write_options, "B", "0");
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options);

  txn1->SetSnapshotOnNextOperation(notifier);
  ASSERT_FALSE(read_options.snapshot);

  s = db->Put(write_options, "B", "1");
  ASSERT_OK(s);

  // A Get does not generate the snapshot
  s = txn1->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_FALSE(read_options.snapshot);
  ASSERT_EQ(value, "1");

  // Any other operation does
  s = txn1->Put("A", "0");
  ASSERT_OK(s);

  // Now change "B".
  s = db->Put(write_options, "B", "2");
  ASSERT_OK(s);

  // The original value should still be read
  s = txn1->Get(read_options, "B", &value);
  ASSERT_OK(s);
  ASSERT_TRUE(read_options.snapshot);
  ASSERT_EQ(value, "1");

  s = txn1->Commit();
  ASSERT_OK(s);

  delete txn1;
}
/**
 * @brief     Use ClearSnapshotTest to test Transaction, and verify the actions of ClearSnapshot API.
 * @param     TransactionFunTest --- Class name. \n
 *            ClearSnapshotTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, ClearSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  std::string value;
  Status s;

  s = db->Put(write_options, "foo", "0");
  ASSERT_OK(s);

  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn);

  s = db->Put(write_options, "foo", "1");
  ASSERT_OK(s);

  snapshot_read_options.snapshot = txn->GetSnapshot();
  ASSERT_FALSE(snapshot_read_options.snapshot);

  // No snapshot created yet
  s = txn->Get(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "1");

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();
  ASSERT_TRUE(snapshot_read_options.snapshot);

  s = db->Put(write_options, "foo", "2");
  ASSERT_OK(s);

  // Snapshot was created before change to '2'
  s = txn->Get(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "1");

  txn->ClearSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();
  ASSERT_FALSE(snapshot_read_options.snapshot);

  // Snapshot has now been cleared
  s = txn->Get(snapshot_read_options, "foo", &value);
  ASSERT_EQ(value, "2");

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;
}

/**
 * @brief     Use MemoryLimitTest to test Transaction, and verify the actions of IsMemoryLimit API.
 * @param     TransactionFunTest --- Class name. \n
 *            MemoryLimitTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, MemoryLimitTest) {
  TransactionOptions txn_options;
  // Header (4 bytes) + 4 * 8 bytes for data.
  txn_options.max_write_batch_size = 36;
  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(WriteOptions(), txn_options);
  ASSERT_TRUE(txn);

  ASSERT_EQ(0, txn->GetNumPuts());
  ASSERT_LE(0, txn->GetID());

  s = txn->Put(Slice("a"), Slice("...."));
  ASSERT_OK(s);
  ASSERT_EQ(1, txn->GetNumPuts());

  s = txn->Put(Slice("b"), Slice("...."));
  ASSERT_OK(s);
  ASSERT_EQ(2, txn->GetNumPuts());

  s = txn->Put(Slice("b"), Slice("...."));
  auto pdb = reinterpret_cast<PessimisticTransactionDB*>(db);
  // For write unprepared, write batches exceeding max_write_batch_size will
  // just flush to DB instead of returning a memory limit error.
  if (pdb->GetTxnDBOptions().write_policy != WRITE_UNPREPARED) {
    ASSERT_TRUE(s.IsMemoryLimit());
    ASSERT_EQ(2, txn->GetNumPuts());
  } else {
    ASSERT_OK(s);
    ASSERT_EQ(3, txn->GetNumPuts());
  }

  txn->Rollback();
  delete txn;
}
/**
 * @brief     Use DoubleEmptyWrite to test Transaction, and verify the Get actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            DoubleEmptyWrite --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, DoubleEmptyWrite) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;

  WriteBatch batch;

  ASSERT_OK(db->Write(write_options, &batch));
  ASSERT_OK(db->Write(write_options, &batch));

  // Also test committing empty transactions in 2PC
  TransactionOptions txn_options;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn0->SetName("xid"));
  ASSERT_OK(txn0->Prepare());
  ASSERT_OK(txn0->Commit());
  delete txn0;

  // Also test that it works during recovery
  /*txn0 = db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn0->SetName("xid2"));
  txn0->Put(Slice("foo0"), Slice("bar0a"));
  ASSERT_OK(txn0->Prepare());
  delete txn0;
  ASSERT_OK(ReOpen());
  assert(db != nullptr);
  txn0 = db->GetTransactionByName("xid2");
  ASSERT_OK(txn0->Commit());

  delete txn0;*/
}


/**
 * @brief     Use SharedLocks to test Transaction, and verify the actions of exclusive API.
 * @param     TransactionFunTest --- Class name. \n
 *            SharedLocks --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
 
TEST_F(TransactionFunTest, SharedLocks) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  Status s;
  bool exclusive = false;

  txn_options.lock_timeout = 1;
  s = db->Put(write_options, Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn3 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);
  ASSERT_TRUE(txn2);
  ASSERT_TRUE(txn3);

  // Test shared access between txns
  s = txn1->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  s = txn3->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  auto lock_data = db->GetLockStatusData();
  ASSERT_EQ(lock_data.size(), 1);

  auto cf_iterator = lock_data.begin();
  ASSERT_EQ(cf_iterator->second.key, "foo");

  // We compare whether the set of txns locking this key is the same. To do
  // this, we need to sort both vectors so that the comparison is done
  // correctly.
  std::vector<TransactionID> expected_txns = {txn1->GetID(), txn2->GetID(),
                                              txn3->GetID()};
  std::vector<TransactionID> lock_txns = cf_iterator->second.ids;
  ASSERT_EQ(expected_txns, lock_txns);
  ASSERT_FALSE(cf_iterator->second.exclusive);

  txn1->Rollback();
  txn2->Rollback();
  txn3->Rollback();

  // Test txn1 and txn2 sharing a lock and txn3 trying to obtain it.
  s = txn1->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive );
  ASSERT_OK(s);

  s = txn3->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn1->UndoGetForUpdate("foo");
  s = txn3->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn2->UndoGetForUpdate("foo");
  s = txn3->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_OK(s);

  txn1->Rollback();
  txn2->Rollback();
  txn3->Rollback();

  // Test txn1 and txn2 sharing a lock and txn2 trying to upgrade lock.
  s = txn1->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn1->UndoGetForUpdate("foo");
  s = txn2->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_OK(s);

  ASSERT_OK(txn1->Rollback());
  ASSERT_OK(txn2->Rollback());

  // Test txn1 trying to downgrade its lock.
  exclusive = true;
  s = txn1->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);
  exclusive = false;
  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  // Should still fail after "downgrading".
  s = txn1->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn1->Rollback();
  txn2->Rollback();

  // Test txn1 holding an exclusive lock and txn2 trying to obtain shared
  // access.
  s = txn1->GetForUpdate(read_options, "foo", nullptr);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  txn1->UndoGetForUpdate("foo");
  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  delete txn1;
  delete txn2;
  delete txn3;
}
/**
 * @brief     Use LogMarkLeakTest to test Transaction, and verify the actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            LogMarkLeakTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */

TEST_F(TransactionFunTest, LogMarkLeakTest) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  options.write_buffer_size = 1024;
  ASSERT_OK(ReOpen());
  assert(db != nullptr);
  Random rnd(47);
  std::vector<Transaction*> txns;
  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  for (size_t i = 0; i < 100; i++) {
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    ASSERT_OK(txn->SetName("xid" + ToString(i)));
    ASSERT_OK(txn->Put(Slice("foo" + ToString(i)), Slice("bar")));
    Status s = txn->Prepare();
    ASSERT_OK(s);

    if (rnd.OneIn(5)) {
      txns.push_back(txn);
    } else {
      ASSERT_OK(txn->Commit());
      delete txn;
    }
  }
  for (auto txn : txns) {
    ASSERT_OK(txn->Commit());
    delete txn;
  }
}
/**
 * @brief     Use ToggleAutoCompactionTest to test disable_auto_compactions options
 * @param     TransactionFunTest --- Class name. \n
 *            ToggleAutoCompactionTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, ToggleAutoCompactionTest) {
  Status s;

  ColumnFamilyHandle *cfa, *cfb;
  ColumnFamilyOptions cf_options;
  
  // Create 2 new column families
  s = db->CreateColumnFamily(cf_options, "CFA", &cfa);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "CFB", &cfb);
  ASSERT_OK(s);

  delete cfa;
  delete cfb;
  delete db;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new column families
  column_families.push_back(
      ColumnFamilyDescriptor("CFA", ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("CFB", ColumnFamilyOptions()));

  ColumnFamilyOptions* cf_opt_default = &column_families[0].options;
  ColumnFamilyOptions* cf_opt_cfa = &column_families[1].options;
  ColumnFamilyOptions* cf_opt_cfb = &column_families[2].options;
  //Disable automatic compactions for the given column families
  cf_opt_default->disable_auto_compactions = false;
  cf_opt_cfa->disable_auto_compactions = true;
  cf_opt_cfb->disable_auto_compactions = false;

  std::vector<ColumnFamilyHandle*> handles;

  s = TransactionDB::Open(options, txn_db_options, dbname, column_families,
                          &handles, &db);
  ASSERT_OK(s);

  auto cfh_default = reinterpret_cast<ColumnFamilyHandleImpl*>(handles[0]);
  auto opt_default = *cfh_default->cfd()->GetLatestMutableCFOptions();
  auto cfh_a = reinterpret_cast<ColumnFamilyHandleImpl*>(handles[1]);
  auto opt_a = *cfh_a->cfd()->GetLatestMutableCFOptions();

  auto cfh_b = reinterpret_cast<ColumnFamilyHandleImpl*>(handles[2]);
  auto opt_b = *cfh_b->cfd()->GetLatestMutableCFOptions();

  ASSERT_EQ(opt_default.disable_auto_compactions, false);
  ASSERT_EQ(opt_a.disable_auto_compactions, true);
  ASSERT_EQ(opt_b.disable_auto_compactions, false);

  for (auto handle : handles) {
    delete handle;
  }
}
/*TEST_F(TransactionFunTest, ValidateSnapshotTest) {
  for (bool with_2pc : {true, false}) {
    ASSERT_OK(ReOpen());
    WriteOptions write_options;
    ReadOptions read_options;
    std::string value;

    assert(db != nullptr);
    Transaction* txn1 =
        db->BeginTransaction(write_options, TransactionOptions());
    ASSERT_TRUE(txn1);
    ASSERT_OK(txn1->Put(Slice("foo"), Slice("bar1")));
    if (with_2pc) {
      ASSERT_OK(txn1->SetName("xid1"));
      ASSERT_OK(txn1->Prepare());
    }

    Transaction* txn2 =
        db->BeginTransaction(write_options, TransactionOptions());
    ASSERT_TRUE(txn2);
    txn2->SetSnapshot();

    ASSERT_OK(txn1->Commit());
    delete txn1;

    auto pes_txn2 = dynamic_cast<PessimisticTransaction*>(txn2);
    // Test the simple case where the key is not tracked yet
    auto trakced_seq = kMaxSequenceNumber;
    auto s = pes_txn2->ValidateSnapshot(db->DefaultColumnFamily(), "foo",
                                        &trakced_seq);
    ASSERT_TRUE(s.IsBusy());
    delete txn2;
  }
}*/

/**
 * @brief     Use DeadlockCycle to test Special case for a deadlock path that exceeds the maximum depth
 * @param     TransactionFunTest --- Class name. \n
 *            DeadlockCycle --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, DeadlockCycle) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;

  // offset by 2 from the max depth to test edge case
  const uint32_t kMaxCycleLength = 52;

  txn_options.lock_timeout = 1000000;
  txn_options.deadlock_detect = true;

  for (uint32_t len = 2; len < kMaxCycleLength; len++) {
    // Set up a long wait for chain like this:
    //
    // T1 -> T2 -> T3 -> ... -> Tlen

    std::vector<Transaction*> txns(len);

    for (uint32_t i = 0; i < len; i++) {
      txns[i] = db->BeginTransaction(write_options, txn_options);
      ASSERT_TRUE(txns[i]);
      auto s = txns[i]->GetForUpdate(read_options, ToString(i), nullptr);
      ASSERT_OK(s);
    }

    std::atomic<uint32_t> checkpoints(0);
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "TransactionLockMgr::AcquireWithTimeout:WaitingTxn",
        [&](void* /*arg*/) { checkpoints.fetch_add(1); });
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();

    // We want the last transaction in the chain to block and hold everyone
    // back.
    std::vector<port::Thread> threads;
    for (uint32_t i = 0; i < len - 1; i++) {
      std::function<void()> blocking_thread = [&, i] {
        auto s = txns[i]->GetForUpdate(read_options, ToString(i + 1), nullptr);
        ASSERT_OK(s);
        txns[i]->Rollback();
        delete txns[i];
      };
      threads.emplace_back(blocking_thread);
    }

    // Wait until all threads are waiting on each other.
    while (checkpoints.load() != len - 1) {
      // sleep override
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

    // Complete the cycle Tlen -> T1
    auto s = txns[len - 1]->GetForUpdate(read_options, "0", nullptr);
    ASSERT_TRUE(s.IsDeadlock());

    const uint32_t dlock_buffer_size_ = (len - 1 > 5) ? 5 : (len - 1);
    uint32_t curr_waiting_key = 0;
    TransactionID curr_txn_id = txns[0]->GetID();

    auto dlock_buffer = db->GetDeadlockInfoBuffer();
    ASSERT_EQ(dlock_buffer.size(), dlock_buffer_size_);
    uint32_t check_len = len;
    bool check_limit_flag = false;

    // Special case for a deadlock path that exceeds the maximum depth.
    if (len > 50) {
      check_len = 0;
      check_limit_flag = true;
    }
    auto dlock_entry = dlock_buffer[0].path;
    ASSERT_EQ(dlock_entry.size(), check_len);
    ASSERT_EQ(dlock_buffer[0].limit_exceeded, check_limit_flag);

    /*int64_t pre_deadlock_time = dlock_buffer[0].deadlock_time;
    int64_t cur_deadlock_time = 0;
    for (auto const& dl_path_rec : dlock_buffer) {
      cur_deadlock_time = dl_path_rec.deadlock_time;
      ASSERT_NE(cur_deadlock_time, 0);
      ASSERT_TRUE(cur_deadlock_time <= pre_deadlock_time);
      pre_deadlock_time = cur_deadlock_time;
    }*/

    // Iterates backwards over path verifying decreasing txn_ids.
    for (auto it = dlock_entry.rbegin(); it != dlock_entry.rend(); it++) {
      auto dl_node = *it;
      ASSERT_EQ(dl_node.m_txn_id, len + curr_txn_id - 1);
      ASSERT_EQ(dl_node.m_cf_id, 0);
      ASSERT_EQ(dl_node.m_waiting_key, ToString(curr_waiting_key));
      ASSERT_EQ(dl_node.m_exclusive, true);

      curr_txn_id--;
      if (curr_waiting_key == 0) {
        curr_waiting_key = len;
      }
      curr_waiting_key--;
    }

    // Rollback the last transaction.
    txns[len - 1]->Rollback();
    delete txns[len - 1];

    for (auto& t : threads) {
      t.join();
    }
  }
}

/**
 * @brief     Use UndoGetForUpdateInTwoTxn to test Transaction, and verify actions of UndoGetForUpdate API.
 * @param     TransactionFunTest --- Class name. \n
 *            UndoGetForUpdateInTwoTxn --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, UndoGetForUpdateInTwoTxn) {
  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;
  Status s;

  txn_options.lock_timeout = 1;  // 1 ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  s = txn1->Put("A", "a");
  ASSERT_OK(s);
  //lock A by txn1
  s = txn1->GetForUpdate(read_options, "A", &value);
  ASSERT_OK(s);
  ASSERT_EQ("a",value);
  //Unlock A by txn2
  txn2->UndoGetForUpdate("A");
  //
  s = txn1->Put("A", "a1");
  ASSERT_OK(s);
  s = txn2->Put("A", "a2");
  ASSERT_TRUE(s.IsTimedOut());

  s = txn2->Put("B", "b1");
  ASSERT_OK(s);
  s = txn2->GetForUpdate(read_options, "B", &value);
  ASSERT_OK(s);

  txn1->UndoGetForUpdate("B");

  s = txn2->Put("B", "b2");
  ASSERT_OK(s);


  s = txn1->Delete("B");
  ASSERT_TRUE(s.IsTimedOut());

  s = txn2->Commit();
  ASSERT_OK(s);
  delete txn2;
  
  s = txn1->Commit();
  ASSERT_OK(s);
  delete txn1;
}

/**
 * @brief     Use UntrackMultiTimes to test Transaction, and verify actions of Untrack API.
 * @param     TransactionFunTest --- Class name. \n
 *            UntrackMultiTimes --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, UntrackMultiTimes) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options);
  txn->SetSnapshot();
  s = db->Put(write_options, "untracked", "x");
  ASSERT_OK(s);
  
  s = txn->PutUntracked("untracked", "0");
  ASSERT_OK(s);

  // Untracked writes should succeed even though key was written after snapshot
  s = txn->PutUntracked("untracked", "1");
  ASSERT_OK(s);

  // Conflict
  s = txn->Put("untracked", "2");
  ASSERT_TRUE(s.IsBusy());

  s = txn->PutUntracked("untracked", "3");
  ASSERT_OK(s);
  
  //Add end
  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "untracked", &value);
  ASSERT_OK(s);
  ASSERT_EQ("3",value);

  delete txn;
}

/**
 * @brief     Use StatesTest to test Transaction, and verify actions of State API.
 * @param     TransactionFunTest --- Class name. \n
 *            StatesTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, StatesTest) {
  TransactionOptions txn_options;
  WriteOptions write_options;
  Status s;
  Transaction::TransactionState txn_state;
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn);
  
  ASSERT_EQ(0,txn->GetState());
  
  
  s = txn->Put("A", "a1");
  ASSERT_OK(s);
  
  s = txn->Commit();
  ASSERT_OK(s);
  
  ASSERT_EQ(4,txn->GetState());
  
}

/**
 * @brief     Use RollbackinMultiCF to test Transaction, and verify the actions of Rollback in multi keys
 * @param     TransactionFunTest --- Class name. \n
 *            RollbackinMultiCF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */

TEST_F(TransactionFunTest, RollbackinMultiCF) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  ColumnFamilyHandle *cf1, *cf2;
  ColumnFamilyOptions cf_options;
  // Create 2 new column families
  s = db->CreateColumnFamily(cf_options, "CF1", &cf1);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "CF2", &cf2);
  ASSERT_OK(s);

  delete cf1;
  delete cf2;
  delete db;
  db = nullptr;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new column families
  column_families.push_back(
      ColumnFamilyDescriptor("CF1", ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("CF2", ColumnFamilyOptions()));

  std::vector<ColumnFamilyHandle*> handles;

  s = TransactionDB::Open(options, txn_db_options, dbname, column_families,
                          &handles, &db);
  assert(db != nullptr);
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn1);
  
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);
  
  txn1->SetSavePoint();   //1
  s = txn1->Put(handles[0], "cf0_key","cf0_value");
  ASSERT_OK(s);
  s = txn2->Put(handles[0], "cf0_key0","cf0_value0");
  ASSERT_OK(s);
  
  s = txn1->Put(handles[1], "cf1_key","cf1_value");
  ASSERT_OK(s);
  s = txn2->Put(handles[1], "cf1_key0","cf1_value0");
  ASSERT_OK(s);
  
  s = txn1->Put(handles[2], "cf2_key","cf2_value");
  ASSERT_OK(s);
  s = txn2->Put(handles[2], "cf2_key0","cf2_value0");
  ASSERT_OK(s);
  
  ASSERT_OK(txn1->RollbackToSavePoint());  //Rollback to 1
  //check default cf
  s = txn1->Get(read_options, handles[0], "cf0_key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn2->Get(read_options, handles[0], "cf0_key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf0_value0", value);
  //check cf1 
  s = txn1->Get(read_options, handles[1], "cf1_key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn2->Get(read_options, handles[1], "cf1_key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf1_value0", value);
  
  //check cf2 
  s = txn1->Get(read_options, handles[2], "cf2_key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn2->Get(read_options, handles[2], "cf2_key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf2_value0", value);
  
  txn2->SetSavePoint();   //2
  s = txn1->Put(handles[0], "cf0_key","cf0_value0");
  ASSERT_OK(s);
  s = txn2->Put(handles[0], "cf0_key0","cf0_value66");
  ASSERT_OK(s);
  s = txn1->Put(handles[1], "cf1_key","cf1_value66");
  ASSERT_OK(s);
  s = txn2->Put(handles[1], "cf1_key0","cf1_value77");
  ASSERT_OK(s);
  s = txn1->Put(handles[2], "cf2_key","cf2_value");
  ASSERT_OK(s);
  s = txn2->Put(handles[2], "cf2_key0","cf2_value66");
  ASSERT_OK(s);
  
  ASSERT_OK(txn2->RollbackToSavePoint());  //Rollback to 2
  //check default cf
  s = txn1->Get(read_options, handles[0], "cf0_key", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf0_value0", value);
  s = txn2->Get(read_options, handles[0], "cf0_key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf0_value0", value);
  //check cf1
  s = txn1->Get(read_options, handles[1], "cf1_key", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf1_value66", value);
  s = txn2->Get(read_options, handles[1], "cf1_key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf1_value0", value);
  //check cf2
  s = txn1->Get(read_options, handles[2], "cf2_key", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf2_value", value);
  s = txn2->Get(read_options, handles[2], "cf2_key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf2_value0", value);
  
  txn1->Rollback();
  //check default cf
  s = txn1->Get(read_options, handles[0], "cf0_key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn2->Get(read_options, handles[0], "cf0_key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf0_value0", value);
  
  //check cf1
  s = txn1->Get(read_options, handles[1], "cf1_key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn2->Get(read_options, handles[1], "cf1_key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf1_value0", value);
  
  //check cf2
  s = txn1->Get(read_options, handles[2], "cf2_key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn2->Get(read_options, handles[2], "cf2_key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf2_value0", value);
  
  s = txn1->Put(handles[0], "cf0_key66","cf0_value66");
  ASSERT_OK(s);
  s = txn1->Put(handles[1], "cf1_key66","cf1_value66");
  ASSERT_OK(s);
  s = txn1->Put(handles[2], "cf2_key66","cf2_value66");
  ASSERT_OK(s);
  
  txn2->Rollback();
    //check default cf
  s = txn1->Get(read_options, handles[0], "cf0_key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn2->Get(read_options, handles[0], "cf0_key0", &value);
  ASSERT_TRUE(s.IsNotFound());
  
  //check cf1
  s = txn1->Get(read_options, handles[1], "cf1_key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn2->Get(read_options, handles[1], "cf1_key0", &value);
  ASSERT_TRUE(s.IsNotFound());
  
  //check cf2
  s = txn1->Get(read_options, handles[2], "cf2_key", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn2->Get(read_options, handles[2], "cf2_key0", &value);
  ASSERT_TRUE(s.IsNotFound());
  
  s = txn1->Commit();
  ASSERT_OK(s);
  s = txn2->Commit();
  ASSERT_OK(s);
  
  s = db->Get(read_options, handles[0], "cf0_key66", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf0_value66", value);
  s = db->Get(read_options, handles[1], "cf1_key66", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf1_value66", value);
  s = db->Get(read_options, handles[2], "cf2_key66", &value);
  ASSERT_OK(s);
  ASSERT_EQ("cf2_value66", value);
  
  delete txn1;
  delete txn2;
}
/**
 * @brief     Use ExpiredTest to test Transaction, and verify the actions of expiration
 * @param     TransactionFunTest --- Class name. \n
 *            ExpiredTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TransactionFunTest, ExpiredTestinCF) {
   WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  ColumnFamilyHandle *cf1, *cf2;
  ColumnFamilyOptions cf_options;
  // Create 2 new column families
  s = db->CreateColumnFamily(cf_options, "CF1", &cf1);
  ASSERT_OK(s);
  s = db->CreateColumnFamily(cf_options, "CF2", &cf2);
  ASSERT_OK(s);

  delete cf1;
  delete cf2;
  delete db;
  db = nullptr;

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new column families
  column_families.push_back(
      ColumnFamilyDescriptor("CF1", ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("CF2", ColumnFamilyOptions()));

  std::vector<ColumnFamilyHandle*> handles;

  s = TransactionDB::Open(options, txn_db_options, dbname, column_families,
                          &handles, &db);
  assert(db != nullptr);
  ASSERT_OK(s);

  s = db->Put(write_options, handles[0],"key0","value0");
  ASSERT_OK(s);
  s = db->Put(write_options, handles[1],"key1","value1");
  ASSERT_OK(s);
  
  // Set txn expiration timeout to 1000 ms
  txn_options.expiration = 1000 ;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  Transaction* txn2 = db->BeginTransaction(write_options);
  ASSERT_TRUE(txn2);
  
  s = txn1->Put(handles[0],"key0", "0");
  ASSERT_OK(s);
  s = txn1->Put(handles[1],"key1", "1");
  ASSERT_OK(s);
  s = txn2->Put(handles[0],"key0", "value66");
  ASSERT_TRUE(s.IsTimedOut());
  s = txn2->Put(handles[1],"key1", "value");
  ASSERT_TRUE(s.IsTimedOut());
  sleep(1);
  // txn1 should fail to commit since it is expired
  s = txn1->Commit();
  ASSERT_TRUE(s.IsExpired());
  s = txn1->Get(read_options, handles[0], "key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("0", value);
  delete txn1;
  
  // txn2 should be able to write to X since txn1 has expired
  s = txn2->Put(handles[0],"key0", "value66");
  ASSERT_OK(s);
  s = txn2->Put(handles[1],"key1", "value77");
  ASSERT_OK(s);
  sleep(1);
  s = txn2->Get(read_options, handles[1], "key1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value77", value);
  
  s = txn2->Commit();
  ASSERT_OK(s);
 
  s = db->Get(read_options, handles[0], "key0", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value66", value);
  s = txn2->Get(read_options, handles[1], "key1", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value77", value);

  delete txn2;
}
/**
 * @brief     Use ExclusiveTest2 to test Transaction, and verify the actions of exclusive API.
 * @param     TransactionFunTest --- Class name. \n
 *            ExclusiveTest2 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
 
TEST_F(TransactionFunTest, ExclusiveTest2) {
  WriteOptions write_options;
  ReadOptions read_options, snapshot_read_options;
  TransactionOptions txn_options;
  string value;
  Status s;

  //Set the Lock whether can be shared
  bool exclusive = false;

  txn_options.lock_timeout = 1;
  s = db->Put(write_options, Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn3 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);
  ASSERT_TRUE(txn2);
  ASSERT_TRUE(txn3);

  // Test shared access between txns
  s = txn1->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);
  s = txn3->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  auto lock_data = db->GetLockStatusData();
  ASSERT_EQ(lock_data.size(), 1);

  s = txn1 -> Put("foo", "value");
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");
  txn1->UndoGetForUpdate("foo");
  txn2->UndoGetForUpdate("foo");
  txn3->UndoGetForUpdate("foo");
  
  s = txn1 -> Put("foo", "value");
  ASSERT_OK(s);
  
  s = txn1->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ("value", value);
  
  s = txn2->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ("bar", value);

  s = txn3->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ("bar", value);


  ASSERT_OK(txn1->Rollback());
  ASSERT_OK(txn2->Rollback());
  ASSERT_OK(txn3->Rollback());

  // Test txn1 trying to downgrade its lock.
  exclusive = true;
  s = txn1->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);
  exclusive = false;
  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");

  // Should still fail after "downgrading".
  s = txn1->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_OK(s);

  s = txn2->GetForUpdate(read_options, "foo", nullptr, exclusive);
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_EQ(s.ToString(), "Operation timed out: Timeout waiting to lock key");
  
  s = txn1->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ("bar", value);
  
  s = txn2->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ("bar", value);

  s = txn3->Get(read_options, "foo", &value);
  ASSERT_OK(s);
  ASSERT_EQ("bar", value);

  delete txn1;
  delete txn2;
  delete txn3;
}
//2PC has not been implemented
/**
 * @brief     Use TwoPhaseNameTest to test Transaction, and verify the actions of SetName API.
 * @param     TransactionFunTest --- Class name. \n
 *            TwoPhaseNameTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(TransactionFunTest, TwoPhaseNameTest) {
  Status s;

  WriteOptions write_options;
  TransactionOptions txn_options;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn3 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn3);
  delete txn3;

  // cant prepare txn without name
  cout<<"Prepare start"<<endl;
  s = txn1->Prepare();
    cout<<"Prepare stop"<<endl;
  ASSERT_EQ(s, Status::InvalidArgument());

  // name too short
  s = txn1->SetName("");
  ASSERT_EQ(s, Status::InvalidArgument());

  // name too long
  s = txn1->SetName(std::string(513, 'x'));
  ASSERT_EQ(s, Status::InvalidArgument());

  // valid set name
  s = txn1->SetName("name1");
  ASSERT_OK(s);

  // cant have duplicate name
  s = txn2->SetName("name1");
  ASSERT_EQ(s, Status::InvalidArgument());

  // shouldn't be able to prepare
  s = txn2->Prepare();
  ASSERT_EQ(s, Status::InvalidArgument());

  // valid name set
  s = txn2->SetName("name2");
  ASSERT_OK(s);

  // cant reset name
  s = txn2->SetName("name3");
  ASSERT_EQ(s, Status::InvalidArgument());

  ASSERT_EQ(txn1->GetName(), "name1");
  ASSERT_EQ(txn2->GetName(), "name2");

  s = txn1->Prepare();
  ASSERT_OK(s);

  // can't rename after prepare
  s = txn1->SetName("name4");
  ASSERT_EQ(s, Status::InvalidArgument());

  txn1->Rollback();
  txn2->Rollback();
  delete txn1;
  delete txn2;
}

/*TEST_F(TransactionFunTest, TwoPhaseEmptyWriteTest) {
      ASSERT_OK(ReOpen());
      Status s;
      std::string value;
      WriteOptions write_options;
      ReadOptions read_options;
      TransactionOptions txn_options;
      //txn_options.use_only_the_last_commit_time_batch_for_recovery =
      //    cwb4recovery;
      Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
      ASSERT_TRUE(txn1);
      Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
      ASSERT_TRUE(txn2);

      s = txn1->SetName("joe");
      ASSERT_OK(s);

      s = txn2->SetName("bob");
      ASSERT_OK(s);

      s = txn1->Prepare();
      ASSERT_OK(s);

      s = txn1->Commit();
      ASSERT_OK(s);

      delete txn1;

      txn2->GetCommitTimeWriteBatch()->Put(Slice("foo"), Slice("bar"));

      s = txn2->Prepare();
      ASSERT_OK(s);

      s = txn2->Commit();
      ASSERT_OK(s);

      delete txn2;
      s = db->Get(read_options, "foo", &value);
      ASSERT_OK(s);
      ASSERT_EQ(value, "bar");
   
      s = ReOpen();
      ASSERT_OK(s);
      assert(db != nullptr);
      s = db->Get(read_options, "foo", &value);
      ASSERT_OK(s);
      ASSERT_EQ(value, "bar"); 
}

/**
 * @brief     Use TwoPhaseExpirationTest to test Transaction, and verify the actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            TwoPhaseExpirationTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */

/*TEST_F(TransactionFunTest, TwoPhaseExpirationTest) {
  Status s;

  WriteOptions write_options;
  TransactionOptions txn_options;
  txn_options.expiration = 500;  // 500ms
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn1);
  ASSERT_TRUE(txn1);

  s = txn1->SetName("joe");
  ASSERT_OK(s);
  s = txn2->SetName("bob");
  ASSERT_OK(s);
  cout<<"Prepare start"<<endl;
  s = txn1->Prepare();
  cout<<"Prepare stop"<<endl;
  ASSERT_OK(s);

  // sleep override
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  s = txn1->Commit();
  ASSERT_OK(s);

  s = txn2->Prepare();
  ASSERT_EQ(s, Status::Expired());

  delete txn1;
  delete txn2;
}
/**
 * @brief     Use TwoPhaseRollbackTest to test Transaction, and verify the actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            TwoPhaseRollbackTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */

/*TEST_F(TransactionFunTest, TwoPhaseRollbackTest) {
  WriteOptions write_options;
  ReadOptions read_options;

  TransactionOptions txn_options;

  std::string value;
  Status s;

  DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);

  // transaction put
  s = txn->Put(Slice("tfoo"), Slice("tbar"));
  ASSERT_OK(s);

  // value is readable form txn
  s = txn->Get(read_options, Slice("tfoo"), &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "tbar");

  // issue rollback
  s = txn->Rollback();
  ASSERT_OK(s);

  // value is nolonger readable
  s = txn->Get(read_options, Slice("tfoo"), &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(txn->GetNumPuts(), 0);

  // put new txn values
  s = txn->Put(Slice("tfoo2"), Slice("tbar2"));
  ASSERT_OK(s);

  // new value is readable from txn
  s = txn->Get(read_options, Slice("tfoo2"), &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "tbar2");

  s = txn->Prepare();
  ASSERT_OK(s);

  // flush to next wal
  s = db->Put(write_options, Slice("foo"), Slice("bar"));
  ASSERT_OK(s);
  FlushOptions flush_options;
  //db_impl->FlushMemTable(flush_options);

  // issue rollback (marker written to WAL)
  s = txn->Rollback();
  ASSERT_OK(s);

  // value is nolonger readable
  s = txn->Get(read_options, Slice("tfoo2"), &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(txn->GetNumPuts(), 0);

  // make commit
  s = txn->Commit();
  ASSERT_EQ(s, Status::InvalidArgument());

  // try rollback again
  s = txn->Rollback();
  ASSERT_EQ(s, Status::InvalidArgument());

  delete txn;
}
/**
 * @brief     Use TwoPhaseLongPrepareTest to test Transaction, and verify the actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            TwoPhaseLongPrepareTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(TransactionFunTest, TwoPhaseLongPrepareTest) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;
  ReadOptions read_options;
  TransactionOptions txn_options;

  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("bob");
  ASSERT_OK(s);

  // transaction put
  ASSERT_OK(txn->Put("foo", "bar"));

  // prepare
  cout<<"Prepare start"<<endl;
  ASSERT_OK(txn->Prepare());
  cout<<"Prepare stop"<<endl;
  delete txn;

  for (int i = 0; i < 1000; i++) {
    std::string key(i, 'k');
    std::string val(1000, 'v');
    assert(db != nullptr);
    s = db->Put(write_options, key, val);
    ASSERT_OK(s);

    if (i % 29 == 0) {
      // crash
      //env->SetFilesystemActive(false);
      //reinterpret_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
      ReOpen();
    } else if (i % 37 == 0) {
      // close
      ReOpen();
    }
  }

  // commit old txn
  txn = db->GetTransactionByName("bob");
  ASSERT_TRUE(txn);
  s = txn->Commit();
  ASSERT_OK(s);

  // verify data txn data
  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar");

  // verify non txn data
  for (int i = 0; i < 1000; i++) {
    std::string key(i, 'k');
    std::string val(1000, 'v');
    s = db->Get(read_options, key, &value);
    ASSERT_EQ(s, Status::OK());
    ASSERT_EQ(value, val);
  }

  delete txn;
}
/**
 * @brief     Use TwoPhaseSequenceTest to test Transaction, and verify the actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            TwoPhaseSequenceTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(TransactionFunTest, TwoPhaseSequenceTest) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;
  ReadOptions read_options;

  TransactionOptions txn_options;

  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);

  // transaction put
  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);
  //s = txn->Put(Slice("foo2"), Slice("bar2"));
  //ASSERT_OK(s);
  //s = txn->Put(Slice("foo3"), Slice("bar3"));
  ///ASSERT_OK(s);
  //s = txn->Put(Slice("foo4"), Slice("bar4"));
  //ASSERT_OK(s);

  // prepare
    cout<<"Prepare start"<<endl;
  s = txn->Prepare();
  ASSERT_OK(s);
    cout<<"Prepare stop"<<endl;

  // make commit
  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;

  // kill and reopen
  //env->SetFilesystemActive(false);
  ReOpen();
  assert(db != nullptr);

  // value is now available
  s = db->Get(read_options, "foo4", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar4");
}

/**
 * @brief     Use TwoPhaseDoubleRecoveryTest to test Transaction, and verify the actions of API.
 * @param     TransactionFunTest --- Class name. \n
 *            TwoPhaseDoubleRecoveryTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(TransactionFunTest, TwoPhaseDoubleRecoveryTest) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = false;
  ReadOptions read_options;

  TransactionOptions txn_options;

  std::string value;
  Status s;

  Transaction* txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("a");
  ASSERT_OK(s);

  // transaction put
  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);

  // prepare
    cout<<"Prepare start"<<endl;
  s = txn->Prepare();
    cout<<"Prepare stop"<<endl;
  ASSERT_OK(s);

  delete txn;

  // kill and reopen
  //env->SetFilesystemActive(false);
  //reinterpret_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
  ReOpen();

  // commit old txn
  txn = db->GetTransactionByName("a");
  s = txn->Commit();
  ASSERT_OK(s);

  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar");

  delete txn;

  txn = db->BeginTransaction(write_options, txn_options);
  s = txn->SetName("b");
  ASSERT_OK(s);

  s = txn->Put(Slice("foo2"), Slice("bar2"));
  ASSERT_OK(s);

  s = txn->Prepare();
  ASSERT_OK(s);

  s = txn->Commit();
  ASSERT_OK(s);

  delete txn;

  // kill and reopen
  //env->SetFilesystemActive(false);
  ReOpen();
  assert(db != nullptr);

  // value is now available
  s = db->Get(read_options, "foo", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar");

  s = db->Get(read_options, "foo2", &value);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(value, "bar2");
}*/
} //namespace transaction
}  // namespace rocksdb
