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
#include "util/rate_limiter.h"
#include "include/rocksdb/rate_limiter.h"
#include <inttypes.h>
#include <limits>

namespace rocksdb {

//class RateLimiterTest : public testing::Test {};
class RateLimiterTest : public KVDBTest {
 public:
  RateLimiterTest() {
    //env_ = new EnvCounter(Env::Default());
    dbname_ = test::TmpDir() + "/ratelimiter_test";
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
    //db_options_.env = env_;
    //insdb::Options insdb_options;  //hainan.liu
    //insdb_options.num_column_count = 5;
    //db_options_.num_cols = 8;
    db_ = nullptr;
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
  }


  ~RateLimiterTest() {
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
  
  DBImpl* dbfull() { return reinterpret_cast<DBImpl*>(db_); }
 public:
  enum class OpType {
    // Limitation: we currently only invoke Request() with OpType::kRead for
    // compactions when DBOptions::new_table_reader_for_compaction_inputs is set
    kRead,
    kWrite,
  };
  enum class Mode {
    kReadsOnly,
    kWritesOnly,
    kAllIo,
  };


};

//TEST_F(RateLimiterTest, RateLimitingTest) {
//  Options options ;
//  options.write_buffer_size = 1 << 20;  // 1MB
//  //options.level0_file_num_compaction_trigger = 2;
//  //options.target_file_size_base = 1 << 20;     // 1MB
//  //options.max_bytes_for_level_base = 4 << 20;  // 4MB
//  //options.max_bytes_for_level_multiplier = 4;
//  options.compression = kNoCompression;
//  options.create_if_missing = true;
//  options.env = env_;
//  //options.statistics = rocksdb::CreateDBStatistics();
//  options.IncreaseParallelism(4);
//  DestroyAndReopen(options);
//
//  WriteOptions wo;
//  //wo.disableWAL = true;
//
//  // # no rate limiting
//  Random rnd(301);
//  uint64_t start = env_->NowMicros();
//  // Write ~96M data
//  for (int64_t i = 0; i < (96 << 10); ++i) {
//    ASSERT_OK(
//        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
//  }
//  uint64_t elapsed = env_->NowMicros() - start;
//  double raw_rate = env_->bytes_written_ * 1000000.0 / elapsed;
//  uint64_t rate_limiter_drains =
//      TestGetTickerCount(options, NUMBER_RATE_LIMITER_DRAINS);
//  ASSERT_EQ(0, rate_limiter_drains);
//  Close();
//
//  // # rate limiting with 0.7 x threshold
//  options.rate_limiter.reset(
//      NewGenericRateLimiter(static_cast<int64_t>(0.7 * raw_rate)));
//  env_->bytes_written_ = 0;
//  DestroyAndReopen(options);
//
//  start = env_->NowMicros();
//  // Write ~96M data
//  for (int64_t i = 0; i < (96 << 10); ++i) {
//    ASSERT_OK(
//        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
//  }
//  rate_limiter_drains =
//      TestGetTickerCount(options, NUMBER_RATE_LIMITER_DRAINS) -
//      rate_limiter_drains;
//  elapsed = env_->NowMicros() - start;
//  Close();
//  ASSERT_EQ(options.rate_limiter->GetTotalBytesThrough(), env_->bytes_written_);
//  // Most intervals should've been drained (interval time is 100ms, elapsed is
//  // micros)
//  ASSERT_GT(rate_limiter_drains, 0);
//  ASSERT_LE(rate_limiter_drains, elapsed / 100000 + 1);
//  double ratio = env_->bytes_written_ * 1000000 / elapsed / raw_rate;
//  fprintf(stderr, "write rate ratio = %.2lf, expected 0.7\n", ratio);
//  ASSERT_TRUE(ratio < 0.8);
//
//  // # rate limiting with half of the raw_rate
//  options.rate_limiter.reset(
//      NewGenericRateLimiter(static_cast<int64_t>(raw_rate / 2)));
//  env_->bytes_written_ = 0;
//  DestroyAndReopen(options);
//
//  start = env_->NowMicros();
//  // Write ~96M data
//  for (int64_t i = 0; i < (96 << 10); ++i) {
//    ASSERT_OK(
//        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
//  }
//  elapsed = env_->NowMicros() - start;
//  rate_limiter_drains =
//      TestGetTickerCount(options, NUMBER_RATE_LIMITER_DRAINS) -
//      rate_limiter_drains;
//  Close();
//  ASSERT_EQ(options.rate_limiter->GetTotalBytesThrough(), env_->bytes_written_);
//  // Most intervals should've been drained (interval time is 100ms, elapsed is
//  // micros)
//  ASSERT_GT(rate_limiter_drains, elapsed / 100000 / 2);
//  ASSERT_LE(rate_limiter_drains, elapsed / 100000 + 1);
//  ratio = env_->bytes_written_ * 1000000 / elapsed / raw_rate;
//  fprintf(stderr, "write rate ratio = %.2lf, expected 0.5\n", ratio);
//  ASSERT_LT(ratio, 0.6);
//}


TEST_F(RateLimiterTest, SanitizeDelayedWriteRate) {
  Options options;
  options.delayed_write_rate = 0;
  options.create_if_missing = true;
  options.fail_if_options_file_error = true;
  //options.num_cols = 8;
  Reopen(options);
  ASSERT_EQ(16 * 1024 * 
1024, dbfull()->GetDBOptions().delayed_write_rate);

  options.rate_limiter.reset(NewGenericRateLimiter(31 * 1024 * 1024));
  Reopen(options);
  ASSERT_EQ(31 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
}

TEST_F(RateLimiterTest, TotalRequestsTest) {
  Options options;
  options.delayed_write_rate = 0;
  options.create_if_missing = true;
  options.fail_if_options_file_error = true;
  //options.num_cols = 8;
  WriteOptions wo;
  Reopen(options);
  //ASSERT_EQ(16 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
  Random rnd(301);
  options.rate_limiter.reset(NewGenericRateLimiter(31 * 1024 * 1024));
  Reopen(options);
  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(
        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
  }
  int totalRequest = options.rate_limiter->GetTotalRequests();  
  //ASSERT_EQ(31 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
  cout << totalRequest << endl;
}

TEST_F(RateLimiterTest, BytesPerSecondTest) {
  Options options;
  options.delayed_write_rate = 0;
  options.create_if_missing = true;
  options.fail_if_options_file_error = true;
  //options.num_cols = 8;
  WriteOptions wo;
  Reopen(options);
  //ASSERT_EQ(16 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
  Random rnd(301);
  options.rate_limiter.reset(NewGenericRateLimiter(31 * 1024 * 1024));
  Reopen(options);
  
  int old_bytesPerSec = options.rate_limiter->GetBytesPerSecond();
  cout << old_bytesPerSec << endl;
  options.rate_limiter->SetBytesPerSecond(5*1024*1024);

  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(
        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
    }  
    int bytesPerSec = options.rate_limiter->GetBytesPerSecond();
    //ASSERT_EQ(31 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
    cout << bytesPerSec << endl;
}

TEST_F(RateLimiterTest, SingleBurstByteTest) {
  Options options;
  options.delayed_write_rate = 0;
  options.create_if_missing = true;
  options.fail_if_options_file_error = true;
  //options.num_cols = 8;
  WriteOptions wo;
  Reopen(options);
  //ASSERT_EQ(16 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
  Random rnd(301);
  options.rate_limiter.reset(NewGenericRateLimiter(31 * 1024 * 1024));
  Reopen(options);

  int old_SingleBurstByte = options.rate_limiter->GetSingleBurstBytes();
  cout << old_SingleBurstByte << endl;

  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(
        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
    }
    int SingleBurstByte = options.rate_limiter->GetSingleBurstBytes();
    //ASSERT_EQ(31 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
    cout << SingleBurstByte << endl;
}

TEST_F(RateLimiterTest, ModeTest) {
  Options options;
  options.delayed_write_rate = 0;
  options.create_if_missing = true;
  options.fail_if_options_file_error = true;
  //options.num_cols = 8;
  WriteOptions wo;
  Reopen(options);
  //ASSERT_EQ(16 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
  Random rnd(301);
  options.rate_limiter.reset(NewGenericRateLimiter(31 * 1024 * 1024));
  Reopen(options);
//  RateLimiter::Mode mode = options.rate_limiter->GetMode();
//  if (mode == RateLimiter::Mode::kReadsOnly)
//   { cout << "Read Only" << endl;}
//  else if (mode == RateLimiter::Mode::kWritesOnly)
//   { cout << "Write Only" << endl;}
//  else if (mode == RateLimiter::Mode::kAllIo)
//   { cout << "All IO" << endl;}
//  else 
//   { cout << "No Mode Got" << endl;}

  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(
        Put(RandomString(&rnd, 32), RandomString(&rnd, (1 << 10) + 1), wo));
    }
  bool isLimited = options.rate_limiter->IsRateLimited(RateLimiter::OpType::kRead);
  cout << isLimited << endl;
  isLimited = options.rate_limiter->IsRateLimited(RateLimiter::OpType::kWrite);
  cout << isLimited << endl;
}



TEST_F(RateLimiterTest, OverflowRate) {
  GenericRateLimiter limiter(port::kMaxInt64, 1000, 10);
  ASSERT_GT(limiter.GetSingleBurstBytes(), 1000000000ll);
}

TEST_F(RateLimiterTest, StartStop) {
  std::unique_ptr<RateLimiter> limiter(NewGenericRateLimiter(100, 100, 10));
}

TEST_F(RateLimiterTest, Rate) {
  auto* env = Env::Default();
  struct Arg {
    Arg(int32_t _target_rate, int _burst)
        : limiter(NewGenericRateLimiter(_target_rate, 100 * 1000, 10)),
          request_size(_target_rate / 10),
          burst(_burst) {}
    std::unique_ptr<RateLimiter> limiter;
    int32_t request_size;
    int burst;
  };

  auto writer = [](void* p) {
    auto* thread_env = Env::Default();
    auto* arg = static_cast<Arg*>(p);
    // Test for 2 seconds
    auto until = thread_env->NowMicros() + 2 * 1000000;
    Random r((uint32_t)(thread_env->NowNanos() %
                        std::numeric_limits<uint32_t>::max()));
    while (thread_env->NowMicros() < until) {
      for (int i = 0; i < static_cast<int>(r.Skewed(arg->burst) + 1); ++i) {
        arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1,
                              Env::IO_HIGH, nullptr /* stats */);
      }
      arg->limiter->Request(r.Uniform(arg->request_size - 1) + 1, Env::IO_LOW,
                            nullptr /* stats */);
    }
  };

  for (int i = 1; i <= 16; i *= 2) {
    int32_t target = i * 1024 * 10;
    Arg arg(target, i / 4 + 1);
    int64_t old_total_bytes_through = 0;
    for (int iter = 1; iter <= 2; ++iter) {
      // second iteration changes the target dynamically
      if (iter == 2) {
        target *= 2;
        arg.limiter->SetBytesPerSecond(target);
      }
      auto start = env->NowMicros();
      for (int t = 0; t < i; ++t) {
        env->StartThread(writer, &arg);
      }
      env->WaitForJoin();

      auto elapsed = env->NowMicros() - start;
      double rate =
          (arg.limiter->GetTotalBytesThrough() - old_total_bytes_through) *
          1000000.0 / elapsed;
      old_total_bytes_through = arg.limiter->GetTotalBytesThrough();
      fprintf(stderr,
              "request size [1 - %" PRIi32 "], limit %" PRIi32
              " KB/sec, actual rate: %lf KB/sec, elapsed %.2lf seconds\n",
              arg.request_size - 1, target / 1024, rate / 1024,
              elapsed / 1000000.0);

      ASSERT_GE(rate / target, 0.80);
      ASSERT_LE(rate / target, 1.25);
    }
  }
}

TEST_F(RateLimiterTest, LimitChangeTest) {
  // starvation test when limit changes to a smaller value
  int64_t refill_period = 1000 * 1000;
  auto* env = Env::Default();
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  struct Arg {
    Arg(int32_t _request_size, Env::IOPriority _pri,
        std::shared_ptr<RateLimiter> _limiter)
        : request_size(_request_size), pri(_pri), limiter(_limiter) {}
    int32_t request_size;
    Env::IOPriority pri;
    std::shared_ptr<RateLimiter> limiter;
  };

  auto writer = [](void* p) {
    auto* arg = static_cast<Arg*>(p);
    arg->limiter->Request(arg->request_size, arg->pri, nullptr /* stats */);
  };

  for (uint32_t i = 1; i <= 16; i <<= 1) {
    int32_t target = i * 1024 * 10;
    // refill per second
    for (int iter = 0; iter < 2; iter++) {
      std::shared_ptr<RateLimiter> limiter =
          std::make_shared<GenericRateLimiter>(target, refill_period, 10);
      rocksdb::SyncPoint::GetInstance()->LoadDependency(
          {{"GenericRateLimiter::Request",
            "RateLimiterTest::LimitChangeTest:changeLimitStart"},
           {"RateLimiterTest::LimitChangeTest:changeLimitEnd",
            "GenericRateLimiter::Refill"}});
      Arg arg(target, Env::IO_HIGH, limiter);
      // The idea behind is to start a request first, then before it refills,
      // update limit to a different value (2X/0.5X). No starvation should
      // be guaranteed under any situation
      // TODO(lightmark): more test cases are welcome.
      env->StartThread(writer, &arg);
      int32_t new_limit = (target << 1) >> (iter << 1);
      TEST_SYNC_POINT("RateLimiterTest::LimitChangeTest:changeLimitStart");
      arg.limiter->SetBytesPerSecond(new_limit);
      TEST_SYNC_POINT("RateLimiterTest::LimitChangeTest:changeLimitEnd");
      env->WaitForJoin();
      fprintf(stderr,
              "[COMPLETE] request size %" PRIi32 " KB, new limit %" PRIi32
              "KB/sec, refill period %" PRIi64 " ms\n",
              target / 1024, new_limit / 1024, refill_period / 1000);
    }
  }
}


}// namespace rocksdb


