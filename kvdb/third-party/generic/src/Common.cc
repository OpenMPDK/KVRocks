/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file common.cc
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
#include <cstring>
#include <string>
#include <cstdlib>

#include "gtest/gtest.h"

#include "CommonError.h"
#include "CommonType.h"
#include "Common.h"

/*
 *  Global variable for store the API return value and output to the user
 * */
INT32 g_nAdiRes;

 /**
 * @brief     Try to construct an inexistent key.
 * @param     pKey --- The inexistent key. \n
              len -- The key length.
 * @return    TRUE32(1) --- The function execute successfully. \n
 *            FALSE32(0) --- The function execution error.
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 */
BOOL32 MakeInexistentKey(PUINT8 pKey, UINT32 len)
{
    // To be implement
    return TRUE32;
}

namespace rocksdb {
namespace test {
 
 ::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s) {
   if (s.ok()) {
     return ::testing::AssertionSuccess();
   } else {
     return ::testing::AssertionFailure() << s_expr << std::endl
                                          << s.ToString();
   }
 }

//hainan.liu
//Slice RandomString(Random* rnd, int len, std::string* dst) {
//  dst->resize(len);
//  for (int i = 0; i < len; i++) {
//    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
//  }
//  return Slice(*dst);
//}
// 
 std::string TmpDir(Env* env) {
   std::string dir;
   Status s = env->GetTestDirectory(&dir);
   EXPECT_TRUE(s.ok()) << s.ToString();
   return dir;
 }
 
 int RandomSeed() {
   const char* env = getenv("TEST_RANDOM_SEED");
   int result = (env != nullptr ? atoi(env) : 301);
   if (result <= 0) {
     result = 301;
   }
   return result;
 }
}

//hainan.liu 2019/03/11
////namespace ColumnFamily {
//ColumnFamily::ColumnFamily(const std::string path)
////    : mem_env_(!getenv("MEM_ENV") ? nullptr : new MockEnv(Env::Default())),
////      env_(new SpecialEnv(mem_env_ ? mem_env_ : Env::Default())),
////      option_config_(kDefault) {
//{
////  env_->SetBackgroundThreads(1, Env::LOW);
////  env_->SetBackgroundThreads(1, Env::HIGH);
//  dbname_ = test::TmpDir(env_) + path;
//  alternative_wal_dir_ = dbname_ + "/wal";
//  alternative_db_log_dir_ = dbname_ + "/db_log_dir";
//  auto options = CurrentOptions();
//  //options.env = env_;
//  //auto delete_options = options;
//  Options options;
//  delete_options.wal_dir = alternative_wal_dir_;
//  EXPECT_OK(DestroyDB(dbname_, delete_options));
//  // Destroy it for not alternative WAL dir is used.
//  EXPECT_OK(DestroyDB(dbname_, options));
//  db_ = nullptr;
//  Reopen(options);
//  Random::GetTLSInstance()->Reset(0xdeadbeef);
//}
////
//ColumnFamily::~ColumnFamily() {
//  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
//  rocksdb::SyncPoint::GetInstance()->LoadDependency({});
//  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
//  Close();
//  Options options;
//  options.db_paths.emplace_back(dbname_, 0);
//  options.db_paths.emplace_back(dbname_ + "_2", 0);
//  options.db_paths.emplace_back(dbname_ + "_3", 0);
//  options.db_paths.emplace_back(dbname_ + "_4", 0);
//  options.env = env_;
//
//  if (getenv("KEEP_DB")) {
//    printf("DB is still at %s\n", dbname_.c_str());
//  } else {
//    EXPECT_OK(DestroyDB(dbname_, options));
//  }
//  delete env_;
//}
//
//
//
//
//void ColumnFamily::CreateColumnFamilies(const std::vector<std::string>& cfs,
//									   const Options& options) {
//   ColumnFamilyOptions cf_opts(options);
//   size_t cfi = handles_.size();
//   handles_.resize(cfi + cfs.size());
//   for (auto cf : cfs) {
//	 ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
//   }
// }
// 
// void ColumnFamily::CreateAndReopenWithCF(const std::vector<std::string>& cfs,
//										const Options& options) {
//   CreateColumnFamilies(cfs, options);
//   std::vector<std::string> cfs_plus_default = cfs;
//   cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
//   ReopenWithColumnFamilies(cfs_plus_default, options);
// }
// 
// void ColumnFamily::ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
//										   const std::vector<Options>& options) {
//   ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
// }
// 
// void ColumnFamily::ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
//										   const Options& options) {
//   ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
// }
// 
// Status ColumnFamily::TryReopenWithColumnFamilies(
//	 const std::vector<std::string>& cfs, const std::vector<Options>& options) {
//   Close();
//   EXPECT_EQ(cfs.size(), options.size());
//   std::vector<ColumnFamilyDescriptor> column_families;
//   for (size_t i = 0; i < cfs.size(); ++i) {
//	 column_families.push_back(ColumnFamilyDescriptor(cfs[i], options[i]));
//   }
//   DBOptions db_opts = DBOptions(options[0]);
//   return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
// }
// 
// Status ColumnFamily::TryReopenWithColumnFamilies(
//	 const std::vector<std::string>& cfs, const Options& options) {
//   Close();
//   std::vector<Options> v_opts(cfs.size(), options);
//   return TryReopenWithColumnFamilies(cfs, v_opts);
// }
//
// Status ColumnFamily::Put(int cf, const Slice& k, const Slice& v,
//						WriteOptions wo) {
//   if (kMergePut == option_config_) {
//	 return db_->Merge(wo, handles_[cf], k, v);
//   } else {
//	 return db_->Put(wo, handles_[cf], k, v);
//   }
// }
// Status ColumnFamily::Merge(int cf, const Slice& k, const Slice& v,
//						  WriteOptions wo) {
//   return db_->Merge(wo, handles_[cf], k, v);
// }
// Status ColumnFamily::Delete(int cf, const std::string& k) {
//  return db_->Delete(WriteOptions(), handles_[cf], k);
//}
// 
// Status ColumnFamily::SingleDelete(int cf, const std::string& k) {
//   return db_->SingleDelete(WriteOptions(), handles_[cf], k);
// }
//
// std::string ColumnFamily::Get(int cf, const std::string& k,
//							 const Snapshot* snapshot) {
//   ReadOptions options;
//   options.verify_checksums = true;
//   options.snapshot = snapshot;
//   std::string result;
//   Status s = db_->Get(options, handles_[cf], k, &result);
//   if (s.IsNotFound()) {
//	 result = "NOT_FOUND";
//   } else if (!s.ok()) {
//	 result = s.ToString();
//   }
//   return result;
// }
//void ColumnFamily::Reopen(const Options& options) {
//  ASSERT_OK(TryReopen(options));
//}
//
//void ColumnFamily::Close() {
//  for (auto h : handles_) {
//    db_->DestroyColumnFamilyHandle(h);
//  }
//  handles_.clear();
//  delete db_;
//  db_ = nullptr;
//}
//
//Status ColumnFamily::TryReopen(const Options& options) {
//  Close();
//  last_options_.table_factory.reset();
//  // Note: operator= is an unsafe approach here since it destructs shared_ptr in
//  // the same order of their creation, in contrast to destructors which
//  // destructs them in the opposite order of creation. One particular problme is
//  // that the cache destructor might invoke callback functions that use Option
//  // members such as statistics. To work around this problem, we manually call
//  // destructor of table_facotry which eventually clears the block cache.
//  last_options_ = options;
//  return DB::Open(options, dbname_, &db_);
//}
//
////// Return the current option configuration.
////Options ColumnFamily::CurrentOptions(
////    const anon::OptionsOverride& options_override) const {
////  return GetOptions(option_config_, GetDefaultOptions(), options_override);
////}
////
////Options ColumnFamily::CurrentOptions(
////    const Options& default_options,
////    const anon::OptionsOverride& options_override) const {
////  return GetOptions(option_config_, default_options, options_override);
////}
////// }  // namespace ColumnFamily
//

}

