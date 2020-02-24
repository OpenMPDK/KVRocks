/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file kvdb_flush.cc
  *  \brief Test cases code of the KVDB flush().
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
#include <fcntl.h>
#include <algorithm>
#include <set>
#include <thread>
#include <unordered_set>
#include <utility>
#include "gtest/gtest.h"
#include "db.h"
#include "status.h"
#include "options.h"
#include "cache.h"
#include "convenience.h"

#include "env.h"
#include "experimental.h"
#include "filter_policy.h"
#include "perf_context.h"
#include "slice.h"
#include "slice_transform.h"
#include "snapshot.h"
#include "table.h"
#include "table_properties.h"
#include "thread_status.h"
#include "CommonType.h"
#include "Common.h"
#include "util/string_util.h"

#include <time.h>
#include <inttypes.h>
using namespace std;

namespace rocksdb
{

namespace
{

typedef std::map<std::string, std::string> KVMap;
}

class FlushTest : public testing::Test
{
public:
    FlushTest()
    {
        env_.reset(new EnvWrapper(Env::Default()));
        dbname_ = "db";
        fopt_.wait = true;
        options_.create_if_missing = true;
        options_.env = env_.get();
        // ensure that compaction is kicked in to always strip timestamp from kvs
        options_.max_compaction_bytes = 1;
        // compaction should take place always from level0 for determinism
        db_ = nullptr;
        DestroyDB(dbname_, Options());
    }

    ~FlushTest() override
    {
        CloseDB();
        DestroyDB(dbname_, Options());
    }

    void MakeKVMap(int64_t num_entries)
    {
        kvmap_.clear();
        int digits = 1;
        for (int64_t dummy = num_entries; dummy /= 10; ++digits)
        {
        }
        int digits_in_i = 1;
        for (int64_t i = 0; i < num_entries; i++)
        {
            std::string key = "key";
            std::string value = "value";
            if (i % 10 == 0)
            {
                digits_in_i++;
            }
            for (int j = digits_in_i; j < digits; j++)
            {
                key.append("0");
                value.append("0");
            }
            AppendNumberTo(&key, i);
            AppendNumberTo(&value, i);
            kvmap_[key] = value;
        }
        ASSERT_EQ(static_cast<int64_t>(kvmap_.size()),
                  num_entries); // check all insertions done
    }

    void Flush()
    {
        db_->Flush(fopt_);
    }

    void OpenDB()
    {
        ASSERT_TRUE(db_ ==
                    nullptr); //  db should be closed before opening again
        ASSERT_OK(DB::Open(options_, dbname_, &db_));
        ASSERT_TRUE(db_);
    }

    void PutValues(int64_t start_pos_map, int64_t num_entries,
                   ColumnFamilyHandle *cf = nullptr)
    {
        ASSERT_TRUE(db_);
        ASSERT_LE(start_pos_map + num_entries, static_cast<int64_t>(kvmap_.size()));
        static WriteOptions wopts;
        static FlushOptions flush_opts;
        kv_it_ = kvmap_.begin();
        advance(kv_it_, start_pos_map);
        for (int64_t i = 0; kv_it_ != kvmap_.end() && i < num_entries;
             i++, ++kv_it_)
        {
            ASSERT_OK(cf == nullptr
                          ? db_->Put(wopts, kv_it_->first, kv_it_->second)
                          : db_->Put(wopts, cf, kv_it_->first, kv_it_->second));
        }
    }

    void CloseDB()
    {
        delete db_;
        db_ = nullptr;
    }

    void GetKVMap(int64_t st_pos, int64_t span, ReadOptions ropts,
                  bool check = true,
                  ColumnFamilyHandle *cf = nullptr)
    {
        ASSERT_TRUE(db_);
        kv_it_ = kvmap_.begin();
        advance(kv_it_, st_pos);
        std::string v;
        for (int64_t i = 0; kv_it_ != kvmap_.end() && i < span; i++, ++kv_it_)
        {
            Status s = (cf == nullptr) ? db_->Get(ropts, kv_it_->first, &v)
                                       : db_->Get(ropts, cf, kv_it_->first, &v);

            if (!s.ok() && !s.IsIncomplete())
            {
                printf("key=%s ", kv_it_->first.c_str());
                printf("is absent from db but was expected to be present\n");
                if (!s.ok())
                {
                    printf("is absent from db but was expected to be present\n");
                }
                else
                {
                    printf("is present in db but was expected to be absent\n");
                }
                FAIL();
            }
            else if (s.ok() && check == false)
            {
                printf("key=%s ", kv_it_->first.c_str());
                printf("is present in db but was expected to be absent\n");
                FAIL();
            }
            else if (s.IsIncomplete() && check == true) {
                printf("key=%s ", kv_it_->first.c_str());
                printf("is present in db but was expected to be absent when using non-blocking get\n");
                FAIL();
            }
        }
    }
    void GetKVMapIter(int64_t st_pos, int64_t span,ReadOptions ropts,
                    bool check = true,
                    ColumnFamilyHandle *cf = nullptr)
        {
            ASSERT_TRUE(db_);
            Iterator *dbiter = db_->NewIterator(ropts);
        kv_it_ = kvmap_.begin();
        advance(kv_it_, st_pos);

        dbiter->Seek(kv_it_->first);

        if (!check)
        {
            if (dbiter->Valid())
            {
                printf(" sleep_iter value.data: %s\n", dbiter->value().data());
                printf(" sleep_iter value.status: %s\n", dbiter->status());
                printf(" sleep_iter second: %s\n", kv_it_->second);
                ASSERT_NE(dbiter->value().compare(kv_it_->second), 0);
            }
        }
        else
        { // dbiter should have found out kvmap_[st_pos]
            for (int64_t i = st_pos; kv_it_ != kvmap_.end() && i < st_pos + span;
                 i++, ++kv_it_)
            {
                ASSERT_TRUE(dbiter->Valid());
                ASSERT_EQ(dbiter->value().compare(kv_it_->second), 0);
                dbiter->Next();
            }
        }
        delete dbiter;
        }
    // Choose carefully so that Put, Gets & Compaction complete in 1 second buffer
    static const int64_t kSampleSize_ = 1;
    std::string dbname_;
    DB *db_;
    std::unique_ptr<Env> env_;

private:
    Options options_;
    FlushOptions fopt_;
    KVMap kvmap_;
    KVMap::iterator kv_it_;
};
TEST_F(FlushTest, nonBlockingGet)
{
    MakeKVMap(kSampleSize_);
    OpenDB();
    PutValues(0, kSampleSize_);

    ReadOptions options;
    options.read_tier = kBlockCacheTier;
    GetKVMap(0, kSampleSize_, options, true);//non-blocking get

    FlushOptions flush_option_;
    ASSERT_OK(db_->Flush(flush_option_));
    
    CloseDB();
    OpenDB();

    options.read_tier = kBlockCacheTier;
    GetKVMap(0, kSampleSize_, options, false);//non-blocking get

    options.read_tier = kReadAllTier;
    GetKVMap(0, kSampleSize_, options, true);//normal get

    options.read_tier = kBlockCacheTier;
    GetKVMap(0, kSampleSize_, options, true);//non-blocking get

    CloseDB();
}

TEST_F(FlushTest, nonBlockingIter)
{
    int kSampleSizeMax_ = 100;
    MakeKVMap(kSampleSizeMax_);
    OpenDB();
    PutValues(0, kSampleSizeMax_);

    ReadOptions options;
    options.read_tier = kBlockCacheTier;
    GetKVMapIter(0, kSampleSizeMax_, options, true);//non-blocking get
   
    CloseDB();
    OpenDB();
    
    options.read_tier = kBlockCacheTier;
    GetKVMapIter(0, kSampleSizeMax_, options, false);//non-blocking get
 
    options.read_tier = kReadAllTier;
    GetKVMapIter(0, kSampleSizeMax_, options, true);//non-blocking get

    options.read_tier = kBlockCacheTier;
    GetKVMapIter(0, kSampleSizeMax_, options, true);//non-blocking get
    CloseDB();
}

TEST_F(FlushTest, nonBlockingGetExceedCache)
{
    int kSampleSizeMax_ = 500000;
    MakeKVMap(kSampleSizeMax_);
    OpenDB();
    PutValues(0, kSampleSizeMax_);

    ReadOptions options;
    options.read_tier = kBlockCacheTier;
    GetKVMap(0, 1, options, false);//non-blocking get

    options.read_tier = kReadAllTier;
    GetKVMap(0, 1, options, true);//normal get

    options.read_tier = kBlockCacheTier;
    GetKVMap(0, 1, options, true);//non-blocking get

    FlushOptions flush_option_;
    ASSERT_OK(db_->Flush(flush_option_));
    CloseDB();
    OpenDB();

    options.read_tier = kBlockCacheTier;
    GetKVMap(0, 1, options, false);//non-blocking get

    options.read_tier = kReadAllTier;
    GetKVMap(0, 1, options, true);//normal get

    options.read_tier = kBlockCacheTier;
    GetKVMap(0, 1, options, true);//non-blocking get
    CloseDB();
}

} // namespace rocksdb
