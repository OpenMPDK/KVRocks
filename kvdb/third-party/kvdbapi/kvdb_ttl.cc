#include <map>
#include <memory>
#include "rocksdb/compaction_filter.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/string_util.h"
#include <unistd.h>
#include <gtest/gtest.h>
#include <string>
#include "rocksdb/env.h"
#include "CommonType.h"
#include "Common.h"

using namespace std;

namespace rocksdb {
namespace {
typedef std::map<std::string, std::string> KVMap;
enum BatchOperation { OP_PUT = 0, OP_DELETE = 1 };
}

class SpecialTimeEnv : public EnvWrapper {
 public:
    explicit SpecialTimeEnv(Env* base) : EnvWrapper(base) {
       base->GetCurrentTime(&current_time_);
    }

    void Sleep(int64_t sleep_time) { current_time_ += sleep_time; }
    Status GetCurrentTime(int64_t* current_time) override {
       *current_time = current_time_;
       return Status::OK();
    }
 private:
    int64_t current_time_ = 0;
};

class TTLTest : public testing::Test {
 public:
    TTLTest() {
       env_.reset(new SpecialTimeEnv(Env::Default()));
       dbname_ = test::TmpDir() + "/TTL_test";
       options_.create_if_missing = true;
       options_.env = env_.get();
       // ensure that compaction is kicked in to always strip timestamp from kvs
       options_.max_compaction_bytes = 1;
       // compaction should take place always from level0 for determinism
       db_ttl = nullptr;
       DestroyDB(dbname_, Options());
    }

    ~TTLTest() override {
       CloseTtl();
       DestroyDB(dbname_, Options());
    }

    void MakeKVMap(int64_t num_entries) {
       kvmap_.clear();
       int digits = 1;
       for (int64_t dummy = num_entries; dummy /= 10; ++digits) {
       }
       int digits_in_i = 1;
       for (int64_t i = 0; i < num_entries; i++) {
          std::string key = "key";
          std::string value = "value";
          if (i % 10 == 0) {
             digits_in_i++;
          }
          for(int j = digits_in_i; j < digits; j++) {
             key.append("0");
             value.append("0");
          }
          AppendNumberTo(&key, i);
          AppendNumberTo(&value, i);
          kvmap_[key] = value;
       }
       ASSERT_EQ(static_cast<int64_t>(kvmap_.size()),
                       num_entries);    //check all insertions done
    }

    void OpenTtl() {
       ASSERT_TRUE(db_ttl == nullptr);    //db should be closed before opening again
       ASSERT_OK(DBWithTTL::Open(options_, dbname_, &db_ttl));
    }
    // Open database with TTL support when TTL provided with db_ttl pointer
    void OpenTtl(int32_t TTL) {
       ASSERT_TRUE(db_ttl == nullptr);
       ASSERT_OK(DBWithTTL::Open(options_, dbname_, &db_ttl, TTL));
    }
    
    // Makes a write-batch with key-values from kvmap_ and Write it
    void MakePutWriteBatch(const BatchOperation* batch_ops, int64_t num_ops) {
       ASSERT_LE(num_ops, static_cast<int64_t>(kvmap_.size()));
       static WriteOptions wopts;
       static FlushOptions flush_opts;
       WriteBatch batch;
       kv_it_ = kvmap_.begin();
       for (int64_t i = 0; i < num_ops && kv_it_ != kvmap_.end(); i++, ++kv_it_) {
          switch (batch_ops[i]) {
             case OP_PUT:
                batch.Put(kv_it_->first, kv_it_->second);
                break;
             case OP_DELETE:
                batch.Delete(kv_it_->first);
                break;
             default:
                FAIL();
          }
       }
       db_ttl->Write(wopts, &batch);
       db_ttl->Flush(flush_opts);
    }
    // Puts num_entries starting from start_pos_map from kvmap_ into the database
    void PutValues(int64_t start_pos_map, int64_t num_entries, bool flush = true,
                           ColumnFamilyHandle* cf = nullptr) {
       ASSERT_TRUE(db_ttl);
       ASSERT_LE(start_pos_map + num_entries, static_cast<int64_t>(kvmap_.size()));
       
       static WriteOptions wopts;
       static FlushOptions flush_opts;

       kv_it_ = kvmap_.begin();
       advance(kv_it_, start_pos_map);
       for (int64_t i = 0; kv_it_ != kvmap_.end() && i < num_entries;
              i++, ++kv_it_) {
      //printf("test put\n");
          ASSERT_OK(cf == nullptr ? db_ttl->Put(wopts, kv_it_->first, kv_it_->second)
                             : db_ttl->Put(wopts, cf, kv_it_->first, kv_it_->second));
       if (flush) {
          if (cf == nullptr) {
             db_ttl->Flush(flush_opts);
          } else {
             db_ttl->Flush(flush_opts, cf);
          }
       }
    }
}
    void OpenReadOnlyTtl(int32_t TTL) {
       ASSERT_TRUE(db_ttl == nullptr);
       ASSERT_OK(DBWithTTL::Open(options_, dbname_, &db_ttl, TTL, true));
}
 // checks the whole kvmap_ to return correct values using KeyMayExist
    void SimpleKeyMayExistCheck() {
       static ReadOptions ropts;
       bool value_found;
       std::string val;
       for(auto &kv : kvmap_) {
          bool ret = db_ttl->KeyMayExist(ropts, kv.first, &val, &value_found);
          if (ret == false || value_found == false) {
             fprintf(stderr, "KeyMayExist could not find key=%s in the database but"
                                       " should have\n", kv.first.c_str());
             FAIL();
          } else if (val.compare(kv.second) != 0) {
             fprintf(stderr, " value for key=%s present in database is %s but"
                                       " should be %s\n", kv.first.c_str(), val.c_str(),
                                       kv.second.c_str());
             FAIL();
          }
       }
    }

    // checks the whole kvmap_ to return correct values using MultiGet
    void SimpleMultiGetTest() {
       static ReadOptions ropts;
       std::vector<Slice> keys;
       std::vector<std::string> values;
       for (auto& kv : kvmap_) {
          keys.emplace_back(kv.first);
       }
       auto statuses = db_ttl->MultiGet(ropts, keys, &values);
       size_t i = 0;
       for (auto& kv : kvmap_) {
          ASSERT_OK(statuses[i]);
          ASSERT_EQ(values[i], kv.second);
          ++i;
       }
    }
    // Sleeps for slp_tim then runs a manual compaction
    // Checks span starting from st_pos from kvmap_ in the db and
    // Gets should return true if check is true and false otherwise
    // Also checks that value that we got is the same as inserted; and =kNewValue
    //    if test_compaction_change is true
    void SleepCompactCheck(int slp_tim, int64_t st_pos, int64_t span,
                       bool check = true, bool test_compaction_change = false,
                       ColumnFamilyHandle* cf = nullptr) {
       ASSERT_TRUE(db_ttl);
       sleep(slp_tim);
       static ReadOptions ropts;
       kv_it_ = kvmap_.begin();
       advance(kv_it_, st_pos);
       std::string v;
       for (int64_t i = 0; kv_it_ != kvmap_.end() && i < span; i++, ++kv_it_) {
          Status s = (cf == nullptr) ? db_ttl->Get(ropts, kv_it_->first, &v)
                                                    : db_ttl->Get(ropts, cf, kv_it_->first, &v);
          if (s.ok() != check) {
             printf("key=%s ", kv_it_->first.c_str());
             if (!s.ok()) {
                printf("is absent from db but was expected to be present\n");
             } else {
                printf("is present in db but was expected to be absent\n");
             }
             FAIL();
          } else if (s.ok()) {
             if (test_compaction_change && v.compare(kNewValue_) != 0) {
                printf(" value for key=%s present in database is %s but "
                           " should be %s\n",
                           kv_it_->first.c_str(), v.c_str(),
                           kNewValue_.c_str());
             } else if (!test_compaction_change && v.compare(kv_it_->second) != 0) {
                printf(" value for key=%s present in database is %s but "
                           " should be %s\n",
                           kv_it_->first.c_str(), v.c_str(),
                           kv_it_->second.c_str());
                FAIL();
             }
              //printf("value for key=%s is %s\n", kv_it_->first.c_str(), v.c_str());
          }
       }
    }
    // Similar as SleepCompactCheck but uses TtlIterator to read from db
    void SleepCompactCheckIter(int slp, int st_pos, int64_t span,
                                              bool check = true) {
       ASSERT_TRUE(db_ttl);
       sleep(slp);
       static ReadOptions ropts;
       Iterator *dbiter = db_ttl->NewIterator(ropts);
       kv_it_ = kvmap_.begin();
       advance(kv_it_, st_pos);
       dbiter->Seek(kv_it_->first);
       if (!check) {
          if (dbiter->Valid()) {
             printf(" sleep_iter value.data: %s\n", dbiter->value().data());
             printf(" sleep_iter value.status: %s\n", dbiter->status());
             printf(" sleep_iter second: %s\n", kv_it_->second);
             //ASSERT_NE(dbiter->value().compare(kv_it_->second), 0);
          }
       } else {    // dbiter should have found out kvmap_[st_pos]
          for (int64_t i = st_pos; kv_it_ != kvmap_.end() && i < st_pos + span;
                 i++, ++kv_it_) {
             ASSERT_TRUE(dbiter->Valid());
             ASSERT_EQ(dbiter->value().compare(kv_it_->second), 0);
             dbiter->Next();
        }
       }
       delete dbiter;
 }

    // Set TTL on open db
    void SetTtl(int32_t TTL, ColumnFamilyHandle* cf = nullptr) {
       ASSERT_TRUE(db_ttl);
       cf == nullptr ? db_ttl->SetTtl(TTL) : db_ttl->SetTtl(cf, TTL);
    }

    void CloseTtl() {
       for (auto h : handles_) {
             cout <<h<<endl;
             if (h) {
                    db_ttl->DestroyColumnFamilyHandle(h);
             }
       }
       handles_.clear();
       cfnames_.clear();
       delete db_ttl;
       db_ttl = nullptr;
 }
    std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
       ReadOptions options;
       options.verify_checksums = true;
       options.snapshot = snapshot;
       std::string result;
       Status s = db_ttl->Get(options, k, &result);
       if (s.IsNotFound()) {
          result = "NOT_FOUND";
       } else if (!s.ok()) {
          result = s.ToString();
       }
       return result;
    }
    std::string Get(int cf, std::vector<ColumnFamilyHandle*> handles,const std::string& key,const Snapshot* snapshot = nullptr) {
       ReadOptions options;
       options.snapshot = snapshot;
       std::string result;
       Status s = db_ttl->Get(options, handles[cf], Slice(key), &result);
       if (s.IsNotFound()) {
          result = "NOT_FOUND";
       } else if (!s.ok()) {
          result = s.ToString();
       }
       return result;
    }
    Status Put(const Slice& k, const Slice& v) {
       return db_ttl->Put(wopts, k, v);
    }
       Status Put(int cf,std::vector<ColumnFamilyHandle*> handles, const std::string& key, const std::string& value) {
       return db_ttl->Put(wopts, handles[cf], Slice(key), Slice(value));
    }
    Status Delete(const std::string& key) {
       return db_ttl->Delete(WriteOptions(), Slice(key));
    }
public:
    std::vector<ColumnFamilyHandle*> handles_;
    std::vector<std::string> cfnames_;
    std::set<std::string> keys_;
    ColumnFamilyOptions column_family_options_;
    DBOptions db_options_;
    DB* db_;
// Choose carefully so that Put, Gets & Compaction complete in 1 second buffer
    static const int64_t kSampleSize_ = 100;
    std::string dbname_;
    DBWithTTL* db_ttl;
    std::unique_ptr<SpecialTimeEnv> env_;
    ReadOptions ropts;
    WriteOptions wopts;
 private:
    Options options_;
    KVMap kvmap_;
    KVMap::iterator kv_it_;
    const std::string kNewValue_ = "new_value";
    std::unique_ptr<CompactionFilter> test_comp_filter_;
};

/**
 * @brief    Use PutLargeData to put 1024 key in the ttl time
 * @param    TTLTest --- Class name. \n
 *           PutLargeData --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest,PutLargeData) {
  OpenTtl(1);
  cout<<"put large data"<<endl;
  std::string testkey(1024*1, '1');
  std::string testvalue(1024*1024,'a');
  cout<<"start to put large data"<<endl;
  ASSERT_OK(Put(testkey, testvalue));
  cout<<"start to get large data"<<endl;
  ASSERT_EQ(testvalue, Get(testkey));
  cout<<"end to get large data"<<endl;
  sleep(1);
  ASSERT_EQ("NOT_FOUND", Get(testkey));
  cout<<"start to close"<<endl;
  CloseTtl();
  cout<<"end to close"<<endl;
}

/**
 * @brief    Use PutLargeData1 to put 1024*10 key in the ttl time
 * @param    TTLTest --- Class name. \n
 *           PutLargeData1 --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(TTLTest,PutLargeData1) {
  OpenTtl(1);
  cout<<"put large data"<<endl;
  std::string testkey(1024*10, '1');
  std::string testvalue(1024*1024,'a');
  cout<<"start to put large data"<<endl;
  ASSERT_OK(Put(testkey, testvalue));
  cout<<"start to get large data"<<endl;
  ASSERT_EQ(testvalue, Get(testkey));
  cout<<"end to get large data"<<endl;
  sleep(1);
  ASSERT_EQ("NOT_FOUND", Get(testkey));
  cout<<"start to close"<<endl;
  CloseTtl();
  cout<<"end to close"<<endl;
}
/**
 * @brief    Use PutDelete to call SetTtl API to test TTL
 * @param    TTLTest --- Class name. \n
 *           PutDelete --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest,PutDelete) {
  OpenTtl(1);
  ASSERT_OK(Put("foo", "v"));
  ASSERT_OK(Put("foo1", "v1"));
  ASSERT_OK(Delete("foo"));
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ("v1", Get("foo1"));
  sleep(1);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ("NOT_FOUND", Get("foo1"));
  CloseTtl();
}
/**
 * @brief    Use PutAfterTTL to test after ttl time is missing,then put kv to db
 * @param    TTLTest --- Class name. \n
 *           PutAfterTTL --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, PutAfterTTL) {
    OpenTtl();
    SetTtl(5);
    ASSERT_OK(Put("a", "1"));
    ASSERT_EQ("1", Get("a"));
    sleep(5);
    ASSERT_EQ("NOT_FOUND", Get("a"));
	ASSERT_OK(Put("b", "1"));
    ASSERT_EQ("1", Get("b"));
    CloseTtl();
}
/**
 * @brief    Use NoPutTest to call SetTtl API to test TTL
 * @param    TTLTest --- Class name. \n
 *           NoPutTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest,NoPutTest) {
  OpenTtl(6);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  sleep(6);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  CloseTtl();
}
/**
 * @brief    Use SetTTL to call SetTtl API to test TTL
 * @param    TTLTest --- Class name. \n
 *           SetTTL --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, SetTTL) {
    OpenTtl();
    SetTtl(6);
    ASSERT_OK(Put("a", "1"));
    ASSERT_EQ("1", Get("a"));
    sleep(6);
    ASSERT_EQ("NOT_FOUND", Get("a"));
    CloseTtl();
}
/**
 * @brief    Use TTL_Zero to Set ttl is 0
 * @param    TTLTest --- Class name. \n
 *           TTL_Zero --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, TTL_Zero) {
    OpenTtl();
    SetTtl(0);
    ASSERT_OK(Put("a", "1"));
    ASSERT_EQ("1", Get("a"));
    sleep(2);
    ASSERT_EQ("1", Get("a"));
    CloseTtl();
}
/**
 * @brief    Use TTL_invalid to Set ttl is invalid
 * @param    TTLTest --- Class name. \n
 *          TTL_invalid --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, TTL_invalid) {
    OpenTtl();
    SetTtl(-1);
    ASSERT_OK(Put("a", "1"));
    ASSERT_EQ("1", Get("a"));
    sleep(2);
    ASSERT_EQ("1", Get("a"));
    CloseTtl();
}
/**
 * @brief    Use SetTTL_twice to Set ttl twice
 * @param    TTLTest --- Class name. \n
 *          SetTTL_twice --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, SetTTL_twice) {
    OpenTtl();
    SetTtl(3);
    ASSERT_OK(Put("a", "1"));
    ASSERT_EQ("1", Get("a"));
    SetTtl(5);
    ASSERT_OK(Put("b", "2"));
	ASSERT_EQ("1", Get("a"));
    sleep(3);
    ASSERT_EQ("2", Get("b"));
	ASSERT_EQ("1", Get("a"));
    sleep(2);
    ASSERT_EQ("NOT_FOUND", Get("b"));
	ASSERT_EQ("NOT_FOUND", Get("a"));
    CloseTtl();
}
/**
 * @brief    Use ReopenTTL to Set ttl after reopen db
 * @param    TTLTest --- Class name. \n
 *          ReopenTTL --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, ReopenTTL) {
    OpenTtl();
    SetTtl(3);
    ASSERT_OK(Put("a", "1"));
    ASSERT_EQ("1", Get("a"));
    sleep(3);
    ASSERT_EQ("NOT_FOUND", Get("a"));
    CloseTtl();
    //After reopen,the ttl is 0
    OpenTtl();
    ASSERT_OK(Put("b", "2"));
    sleep(1);
    ASSERT_EQ("2", Get("b"));
    sleep(3);
    ASSERT_EQ("2", Get("b"));
    CloseTtl();
}
/**
 * @brief    Use DeleteBeforeTTL to delete key before set ttl
 * @param    TTLTest --- Class name. \n
 *          DeleteBeforeTTL --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, DeleteBeforeTTL) {
    OpenTtl();
    SetTtl(6);
    ASSERT_OK(Put("a", "1"));
    ASSERT_EQ("1", Get("a"));
    ASSERT_OK(Delete("a"));
    ASSERT_EQ("NOT_FOUND", Get("a"));
    sleep(6);
    ASSERT_EQ("NOT_FOUND", Get("a"));
    CloseTtl();
}
/**
 * @brief    Use TTLAfterFlush to test flush data to device in the ttl time
 * @param    TTLTest --- Class name. \n
 *          TTLAfterFlush --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, TTLAfterFlush) {
    OpenTtl();
    SetTtl(6);
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(db_ttl->Flush(FlushOptions()));
    ASSERT_EQ("1", Get("a"));
    sleep(6);
    ASSERT_EQ("NOT_FOUND", Get("a"));
    CloseTtl();
}
/**
 * @brief    Use TTL_CF to Set ttl for each CF
 * @param    TTLTest --- Class name. \n
 *           TTL_CF --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, TTL_CF) {
    Options options;
    options.create_if_missing = true;
    ColumnFamilyHandle* handle;
    std::vector<ColumnFamilyDescriptor> column_families;
    OpenTtl();
    db_ttl->CreateColumnFamilyWithTtl(ColumnFamilyOptions(options), "1", &handle,5);
    CloseTtl();
    column_families.push_back(ColumnFamilyDescriptor(
        kDefaultColumnFamilyName, ColumnFamilyOptions(options)));
    column_families.push_back(ColumnFamilyDescriptor(
          "1", ColumnFamilyOptions(options)));
    std::vector<ColumnFamilyHandle*> handles;

    ASSERT_OK(DBWithTTL::Open(DBOptions(options), dbname_, column_families,
                                             &handles, &db_ttl, {3, 5}, false));
    handles.push_back(handle);
    //default cf ttl = 3,cf 1 's ttl is 5
    Status s = Put(0, handles, "a", "1");
    Put(1, handles, "b", "2");
    cout<<s.ToString()<<endl;
    sleep(1);
    ASSERT_EQ("1", Get(0,handles,"a"));
    cout<<Get(0,handles,"a")<<endl;
    ASSERT_EQ("2", Get(1,handles,"b"));
    cout<<Get(1,handles,"b")<<endl;
    sleep(3);
    ASSERT_EQ("NOT_FOUND", Get(0,handles,"a"));
    ASSERT_EQ("2", Get(1,handles,"b"));
    sleep(2);
    ASSERT_EQ("NOT_FOUND", Get(0,handles,"a"));
    ASSERT_EQ("NOT_FOUND", Get(1,handles,"b"));
    CloseTtl();
}
/**
 * @brief    Use TTL_Snapshot to Set snapshot before ttl's time
 * @param    TTLTest --- Class name. \n
 *          TTL_Snapshot --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, TTL_Snapshot) {
    Options options;
    options.create_if_missing = true;
    ColumnFamilyHandle* handle;
    std::vector<ColumnFamilyDescriptor> column_families;
    OpenTtl();
    db_ttl->CreateColumnFamilyWithTtl(ColumnFamilyOptions(options), "1", &handle,5);
    CloseTtl();
    column_families.push_back(ColumnFamilyDescriptor(
        kDefaultColumnFamilyName, ColumnFamilyOptions(options)));
    column_families.push_back(ColumnFamilyDescriptor(
          "1", ColumnFamilyOptions(options)));
    std::vector<ColumnFamilyHandle*> handles;

    ASSERT_OK(DBWithTTL::Open(DBOptions(options), dbname_, column_families,
                                             &handles, &db_ttl, {3, 5}, false));
    handles.push_back(handle);
    //default cf ttl = 3
    Status s = Put(0, handles, "a", "1");
    Put(1, handles, "b", "2");
    cout<<s.ToString()<<endl;
    sleep(1);
    ASSERT_EQ("1", Get(0,handles,"a"));
    cout<<Get(0,handles,"a")<<endl;
    ASSERT_EQ("2", Get(1,handles,"b"));
    cout<<Get(1,handles,"b")<<endl;
    //Set snapshot before sleep
    const Snapshot *ss = db_ttl->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;
    sleep(3);
    ASSERT_EQ("NOT_FOUND", Get(0,handles,"a"));
    ASSERT_EQ("2", Get(1,handles,"b"));
    sleep(2);
    ASSERT_EQ("NOT_FOUND", Get(0,handles,"a"));
    ASSERT_EQ("NOT_FOUND", Get(1,handles,"b"));
    ASSERT_EQ("1", Get(0, handles,"a",ss));
    ASSERT_EQ("2", Get(1, handles,"b",ss));
    CloseTtl();
}
/**
 * @brief    Use NoEffect to test opens the db 3 times with such default behavior
 * @param    TTLTest --- Class name. \n
 *           NoEffect --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
// If TTL is non positive or not provided, the behaviour is TTL = infinity
// This test opens the db 3 times with such default behavior and inserts a
// bunch of kvs each time. All kvs should accumulate in the db till the end
// Partitions the sample-size provided into 3 sets over boundary1 and boundary2
TEST_F(TTLTest, NoEffect) {
    MakeKVMap(kSampleSize_);
    int64_t boundary1 = kSampleSize_ / 3;
    int64_t boundary2 = 2 * boundary1;
    OpenTtl();
    PutValues(0, boundary1);                            //T=0: Set1 never deleted
    SleepCompactCheck(1, 0, boundary1);                    //T=1: Set1 still there
    CloseTtl();
    OpenTtl(0);
    PutValues(boundary1, boundary2 - boundary1);    //T=1: Set2 never deleted
    SleepCompactCheck(1, 0, boundary2);            //T=2: Sets1 & 2 still there
    CloseTtl();
    OpenTtl(-1);
    PutValues(boundary2, kSampleSize_ - boundary2); //T=3: Set3 never deleted
    SleepCompactCheck(1, 0, kSampleSize_, true);    //T=4: Sets 1,2,3 still there
    CloseTtl();
}
/**
 * @brief    Use PresentDuringTTL to test Puts a set of values and checks its presence using Get during TTL
 * @param    TTLTest --- Class name. \n
 *          PresentDuringTTL --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, PresentDuringTTL) {
    MakeKVMap(kSampleSize_);
    OpenTtl(2);                                          // T=0:Open the db with TTL = 2
    PutValues(0, kSampleSize_);                             // T=0:Insert Set1. Delete at t=2
    SleepCompactCheck(1, 0, kSampleSize_, true); // T=1:Set1 should still be there
    CloseTtl();
}
/**
 * @brief    Use AbsentAfterTTL to test Puts a set of values and checks its absence using Get after TTL
 * @param    TTLTest --- Class name. \n
 *          AbsentAfterTTL --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, AbsentAfterTTL) {
    MakeKVMap(kSampleSize_);
    OpenTtl(1);                                           // T=0:Open the db with TTL = 2
    PutValues(0, kSampleSize_);                             // T=0:Insert Set1. Delete at t=2
    SleepCompactCheck(2, 0, kSampleSize_, false); // T=2:Set1 should not be there
    CloseTtl();
}
/**
 * @brief    Use ResetTimestamp to test resets the timestamp of a set of kvs by 
 *          updating them and checks that they are not deleted according to the old timestamp
 * @param    TTLTest --- Class name. \n
 *          ResetTimestamp --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, ResetTimestamp) {
    MakeKVMap(kSampleSize_);
    OpenTtl(3);
    PutValues(0, kSampleSize_);                    // T=0: Insert Set1. Delete at t=3
    env_->Sleep(2);                                // T=2
    PutValues(0, kSampleSize_);                    // T=2: Insert Set1. Delete at t=5
    SleepCompactCheck(2, 0, kSampleSize_);         // T=4: Set1 should still be there
    CloseTtl();
}
/**
 * @brief    Use IterPresentDuringTTL to test Similar to PresentDuringTTL but uses Iterator
 * @param    TTLTest --- Class name. \n
 *          IterPresentDuringTTL --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */ 
TEST_F(TTLTest, IterPresentDuringTTL) {
    MakeKVMap(kSampleSize_);
    OpenTtl(2);
    PutValues(0, kSampleSize_);                // T=0: Insert. Delete at t=2
    SleepCompactCheckIter(1, 0, kSampleSize_);    // T=1: Set should be there
    CloseTtl();
}
/**
 * @brief    Use IterPresentDuringTTL to test Similar to AbsentAfterTTL but uses Iterator
 * @param    TTLTest --- Class name. \n
 *          IterPresentDuringTTL --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */ 
TEST_F(TTLTest, IterAbsentAfterTTL) {
    MakeKVMap(kSampleSize_);
    OpenTtl(1);
    PutValues(0, kSampleSize_);                    // T=0: Insert. Delete at t=1
    SleepCompactCheckIter(2, 0, kSampleSize_, false); // T=2: Should not be there
    CloseTtl();
}
/**
 * @brief    Use MultiOpenSamePresent to Checks presence while opening the same db more than once with the same TTL
 * @param    TTLTest --- Class name. \n
 *           MultiOpenSamePresent --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */ 
// Note: The second open will open the same db
TEST_F(TTLTest, MultiOpenSamePresent) {
    MakeKVMap(kSampleSize_);
    OpenTtl(2);
    PutValues(0, kSampleSize_);                      // T=0: Insert. Delete at t=2
    CloseTtl();
    OpenTtl(2);                                   // T=0. Delete at t=2
    SleepCompactCheck(1, 0, kSampleSize_);             // T=1: Set should be there
    SleepCompactCheck(1, 0, kSampleSize_, false);
    CloseTtl();
}
/**
 * @brief    Use MultiOpenSamePresent to Checks absence while opening the same db more than once with the same TTL
 * @param    TTLTest --- Class name. \n
 *          MultiOpenSamePresent --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
// Note: The second open will open the same db
TEST_F(TTLTest, MultiOpenSameAbsent) {
    MakeKVMap(kSampleSize_);
    OpenTtl(1);
    PutValues(0, kSampleSize_); // T=0: Insert. Delete at t=1
    CloseTtl();
    OpenTtl(1);// T=0.Delete at t=1
    SleepCompactCheck(2, 0, kSampleSize_, false); // T=2: Set should not be there
    CloseTtl();
}
/**
 * @brief    Use MultiOpenDifferent to Checks presence while opening the same db more than once with bigger TTL
 * @param    TTLTest --- Class name. \n
 *          MultiOpenDifferent --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, MultiOpenDifferent) {
    MakeKVMap(kSampleSize_);
    OpenTtl(1);
    PutValues(0, kSampleSize_);// T=0: Insert. Delete at t=1
    CloseTtl();
    OpenTtl(3);
    SleepCompactCheck(2, 0, kSampleSize_); // T=2: Set should be there
    SleepCompactCheck(1, 0, kSampleSize_,false);
    CloseTtl();
}
/**
 * @brief    Use ReadOnlyPresentForever to Checks presence during TTL in read_only mode
 * @param    TTLTest --- Class name. \n
 *          ReadOnlyPresentForever --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(TTLTest, ReadOnlyPresentForever) {
    MakeKVMap(kSampleSize_);
    OpenTtl(1);                      // T=0:Open the db normally
    PutValues(0, kSampleSize_);         // T=0:Insert Set1. Delete at t=1
    CloseTtl();
    OpenReadOnlyTtl(1);
    SleepCompactCheck(2, 0, kSampleSize_);// T=2:Set1 should still be there
    CloseTtl();
}
/**
 * @brief    Use WriteBatchTest to Checks whether WriteBatch works well with TTL
 * @param    TTLTest --- Class name. \n
 *          WriteBatchTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
// Puts all kvs in kvmap_ in a batch and writes first, then deletes first half
TEST_F(TTLTest, WriteBatchTest) {
    MakeKVMap(kSampleSize_);
    BatchOperation batch_ops[kSampleSize_];
    for (int i = 0; i < kSampleSize_; i++) {
    batch_ops[i] = OP_PUT;
    }
    OpenTtl(2);
    MakePutWriteBatch(batch_ops, kSampleSize_);
    for (int i = 0; i < kSampleSize_ / 2; i++) {
       batch_ops[i] = OP_DELETE;
    }
    MakePutWriteBatch(batch_ops, kSampleSize_ / 2);
    SleepCompactCheck(0, 0, kSampleSize_ / 2, false);
    SleepCompactCheck(0, kSampleSize_ / 2, kSampleSize_ - kSampleSize_ / 2);
    CloseTtl();
}
/**
 * @brief    Use KeyMayExist to Insert some key-values which KeyMayExist 
 *          should be able to get and check that values returned are fine
 *          be able to get and check that values returned are fine
 * @param    TTLTest --- Class name. \n
 *          KeyMayExist --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(TTLTest, KeyMayExist) {
    MakeKVMap(kSampleSize_);
    OpenTtl();
    PutValues(0, kSampleSize_, false);
    SimpleKeyMayExistCheck();
    CloseTtl();
}
/**
 * @brief    Use MultiGetTest to checks the whole kvmap_ to return correct values using MultiGet with ttl
 *          be able to get and check that values returned are fine
 * @param    TTLTest --- Class name. \n
 *          MultiGetTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
/*TEST_F(TTLTest, MultiGetTest) {
    MakeKVMap(kSampleSize_);
    OpenTtl();
    PutValues(0, kSampleSize_, false);
    SimpleMultiGetTest();
    CloseTtl();
}
/**
 * @brief    Use ChangeTtlOnOpenDb to Puts a set of values and checks its absence using Get after TTL
 *          be able to get and check that values returned are fine
 * @param    TTLTest --- Class name. \n
 *          ChangeTtlOnOpenDb --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, ChangeTtlOnOpenDb) {
    MakeKVMap(kSampleSize_);
    OpenTtl(1);                            // T=0:Open the db with TTL = 2
    SetTtl(3);
    PutValues(0, kSampleSize_);               // T=0:Insert Set1. Delete at t=2
    SleepCompactCheck(2, 0, kSampleSize_, true); // T=2:Set1 should be there
    SleepCompactCheck(1, 0, kSampleSize_, false);
    CloseTtl();
}
/**
 * @brief    Use TwoColumnFamiliesTest to Set ttl for two CFs When write a lot of dada
 *          be able to get and check that values returned are fine
 * @param    TTLTest --- Class name. \n
 *          TwoColumnFamiliesTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, TwoColumnFamiliesTest) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    MakeKVMap(kSampleSize_);
    DB::Open(options, dbname_, &db);
    ColumnFamilyHandle* handle;
    
    ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(options),
                                                        "TTL_column_family", &handle));
    delete handle;
    delete db;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.push_back(ColumnFamilyDescriptor(
        kDefaultColumnFamilyName, ColumnFamilyOptions(options)));
    column_families.push_back(ColumnFamilyDescriptor(
          "TTL_column_family", ColumnFamilyOptions(options)));
    std::vector<ColumnFamilyHandle*> handles;
    
    ASSERT_OK(DBWithTTL::Open(DBOptions(options), dbname_, column_families,
                                             &handles, &db_ttl, {3, 5}, false));
    ASSERT_EQ(handles.size(), 2U);
    PutValues(0, kSampleSize_, false, handles[0]);
    PutValues(0, kSampleSize_, false, handles[1]);
    // everything should be there after 1 second
    SleepCompactCheck(1, 0, kSampleSize_, true, false, handles[0]);
    SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[1]);

    // only column family 1 should be alive after 4 seconds
    SleepCompactCheck(3, 0, kSampleSize_, false, false, handles[0]);
    SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[1]);
    // nothing should be there after 6 seconds
    SleepCompactCheck(2, 0, kSampleSize_, false, false, handles[0]);
    SleepCompactCheck(0, 0, kSampleSize_, false, false, handles[1]);
    CloseTtl();
}
/**
 * @brief    Use MultiColumnFamiliesTest to Set ttl for three CFs When write a lot of dada
 *          be able to get and check that values returned are fine
 * @param    TTLTest --- Class name. \n
 *          MultiColumnFamiliesTest --- Function name.
 * @return    void
 * @author    Liu Liu(ll66.liu@sumsung.com)
 * @version   v0.1
 * @remark    
 */
TEST_F(TTLTest, MultiColumnFamiliesTest) {
    Options options;
    options.create_if_missing = true;
    MakeKVMap(kSampleSize_);
    options.num_cols = 3;
    DBWithTTL::Open(options, dbname_, &db_ttl);
    cout <<"open success"<<endl;
    std::vector<ColumnFamilyHandle*> handles;
    std::vector<ColumnFamilyDescriptor> column_families;
    ASSERT_OK(db_ttl->CreateColumnFamilies(options, {"ttl_cf_1", "ttl_cf_2"},&handles));
    cout <<"create success"<<endl;
    CloseTtl();
    cout <<"close success"<<endl;
    column_families.push_back(ColumnFamilyDescriptor(
          kDefaultColumnFamilyName, ColumnFamilyOptions(options)));
    column_families.push_back(ColumnFamilyDescriptor(
          "ttl_cf_1", ColumnFamilyOptions(options)));
    column_families.push_back(ColumnFamilyDescriptor(
          "ttl_cf_2", ColumnFamilyOptions(options)));
    cout<<"start to open all CFS"<<endl;
    ASSERT_OK(DBWithTTL::Open(DBOptions(options), dbname_, column_families,
                                             &handles, &db_ttl, {3, 4, 5}, false));
    cout<<"opened all CFS"<<endl;
    PutValues(0, kSampleSize_, false, handles[0]);
    PutValues(0, kSampleSize_, false, handles[1]);
    PutValues(0, kSampleSize_, false, handles[2]);
    cout<<"Put data in the all CFS"<<endl;
    //everything should be there after 1 second
    cout<<"everything should be there after 1 second"<<endl;
    SleepCompactCheck(1, 0, kSampleSize_, true, false, handles[0]);
    SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[1]);
    SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[2]);
    //only ttl_cf_2 should be alive after 4 seconds
    cout<<"ttl_cf_1,ttl_cf_2 should be there after 3 second"<<endl;
    SleepCompactCheck(2, 0, kSampleSize_, false, false, handles[0]);
    SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[1]);
    SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[2]);
    cout<<"ttl_cf_1 should be not there after 4 second"<<endl;
    SleepCompactCheck(1, 0, kSampleSize_, false, false, handles[0]);
    SleepCompactCheck(0, 0, kSampleSize_, false, false, handles[1]);
    SleepCompactCheck(0, 0, kSampleSize_, true, false, handles[2]);
    cout<<"nothing should be there after 5 seconds"<<endl;
    SleepCompactCheck(1, 0, kSampleSize_, false, false, handles[0]);
    SleepCompactCheck(0, 0, kSampleSize_, false, false, handles[1]);
    SleepCompactCheck(0, 0, kSampleSize_, false, false, handles[2]);
    CloseTtl();
}
}
