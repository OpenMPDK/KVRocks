/*! \file kvdb_compression.cc
 *  \brief Test cases code of the KVDB compression feature.
*/
#include "kvdb_test.h"

using namespace std;

namespace rocksdb {
#define kNumKeysWritten  100000

class CompressionTest: public KVDBTest {
public:
    Options options;

    CompressionTest()  {
        dbname_ = test::TmpDir() + "/compression_test";
        db_options_.create_if_missing = true;
        DestroyDB(dbname_, Options(db_options_, column_family_options_));
 }

    ~CompressionTest() {
        Destroy();
}

void PutRandom(DB *db, const std::string &key, Random *rnd,
                  std::map<std::string, std::string> *data = nullptr) {
    std::string value = RandomString(rnd, 110);
    ASSERT_OK(Put(key, value));
    if (data != nullptr) {
        (*data)[key] = value;
    }
}
void VerifyData(DB *db,const std::map<std::string, std::string> &data){
    for (auto &p : data) {
        std::string value;
        ASSERT_EQ(p.second, Get(p.first));
    }
}
};

/*************************************************************************************************/
/**********************************************Compression Test Case***********************************/
/*************************************************************************************************/

/**
 * @brief    Use kNoCompression to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *           NoCompressionTest --- Function name.
 * @return   void
 * @author   Liu Liu(ll66.liu@sumsung.com)
 * @version  v0.1
 * @remark        
 */
TEST_F(CompressionTest,NoCompressionTest) {
    options.compression = CompressionType::kNoCompression;
    options.create_if_missing = true;
    Open(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put("a"+ToString(i), "0"+ToString(i)));
    }
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_EQ("0"+ToString(i), Get("a"+ToString(i)));
    }
    Close();
}
/**
 * @brief    Use kDisableCompressionOption to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *           DisableCompressionTest --- Function name.
 * @return   void
 * @author   Liu Liu(ll66.liu@sumsung.com)
 * @version  v0.1
 * @remark   
 */
TEST_F(CompressionTest,DisableCompressionTest) {

    options.compression = CompressionType::kDisableCompressionOption;
    options.create_if_missing = true;
    Open(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put("a"+ToString(i), "0"+ToString(i)));
    }
    sleep(1);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_EQ("0"+ToString(i), Get("a"+ToString(i)));
    }
    Close();
}
/**
 * @brief    Use SnappyCompressionTest to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *           SnappyCompressionTest --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(CompressionTest, SnappyCompressionTest) {
    options.compression = CompressionType::kSnappyCompression;
    options.create_if_missing = true;
    Open(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put("a"+ToString(i), "0"+ToString(i)));
    }
    sleep(1);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_EQ("0"+ToString(i), Get("a"+ToString(i)));
    }   
    Close();
}

/*TEST_F(CompressionTest, ZlibCompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kZlibCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    Close();
}

//kLZ4HCCompression
TEST_F(CompressionTest, LZ4HCompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kLZ4HCCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    Close();
}

TEST_F(CompressionTest, XpressCompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kXpressCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    Close();
}

TEST_F(CompressionTest, ZSTDCompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kZSTDNotFinalCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    Close();
}*/
/**
 * @brief    Use 0CompressionTest to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              0CompressionTest --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest, 0CompressionTest) {
    options.compression = 0;
    options.create_if_missing = true;
    Open(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put("a"+ToString(i), "0"+ToString(i)));
    }
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_EQ("0"+ToString(i), Get("a"+ToString(i)));
    }
    Close();
}
/**
 * @brief    Use 1CompressionTest to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              1CompressionTest --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest, 1CompressionTest) {
    options.compression = 1;
    options.create_if_missing = true;
    Open(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put("a"+ToString(i), "0"+ToString(i)));
    }
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_EQ("0"+ToString(i), Get("a"+ToString(i)));
    }
    Close();
}
/**
 * @brief    Use NegativeCompressionTest to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              NegativeCompressionTest --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest, NegativeCompressionTest) {
    options.compression = -1;
    options.create_if_missing = true;
    Open(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put("a"+ToString(i), "0"+ToString(i)));
    }
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_EQ("0"+ToString(i), Get("a"+ToString(i)));
    }
    Close();
}

//Add by 66
//BZip2Compression
/*TEST_F(CompressionTest, BZip2CompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kBZip2Compression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    Close();
}

//LZ4CCompression
TEST_F(CompressionTest, LZ4CCompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kLZ4Compression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    Close();
}*/

/**
 * @brief    Use SnappyUncompressionTest to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              SnappyUncompressionTest --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest, SnappyUncompressionTest) {
    options.compression = CompressionType::kSnappyCompression;
    options.create_if_missing = true;
    Open(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put("a"+ToString(i), "0"+ToString(i)));
    }
    
    options.compression = CompressionType::kNoCompression;
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put("b"+ToString(i), "0"+ToString(i)));
    }
    Reopen(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_EQ("0"+ToString(i), Get("a"+ToString(i)));
        ASSERT_EQ("0"+ToString(i), Get("b"+ToString(i)));
    }
    Close();
}


//Zlib Unompression
/*TEST_F(CompressionTest, ZlibUnompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kZlibCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    
    options.compression = CompressionType::kNoCompression;
    Reopen(options);
    ASSERT_EQ("0", Get("a"));
    Close();
}
//BZip2 UnCompression
TEST_F(CompressionTest, BZip2UncompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kBZip2Compression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    
    options.compression = CompressionType::kNoCompression;
    Reopen(options);
    ASSERT_EQ("0", Get("a"));
    Close();
}

//LZ4C Uncompression
TEST_F(CompressionTest, LZ4CUncompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kLZ4Compression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
        
    options.compression = CompressionType::kNoCompression;
    Reopen(options);
    ASSERT_EQ("0", Get("a"));
    Close();
}

//LZ4HC Uncompression
TEST_F(CompressionTest, LZ4HCUnompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kLZ4HCCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    
    options.compression = CompressionType::kNoCompression;
    Reopen(options);
    ASSERT_EQ("0", Get("a"));
    Close();
}
//Xpress Uncompression
TEST_F(CompressionTest, XpressUncompression) {
    Open();
    Options options;
    options.compression = CompressionType::kXpressCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
        
    options.compression = CompressionType::kNoCompression;
    Reopen(options);
    ASSERT_EQ("0", Get("a"));
    Close();
}
//ZSTD Unompression
TEST_F(CompressionTest, ZSTDUncompressionTest) {
    Open();
    Options options;
    options.compression = CompressionType::kZSTDNotFinalCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
        
    options.compression = CompressionType::kNoCompression;
    Reopen(options);
    ASSERT_EQ("0", Get("a"));
    Close();
}


//ZSTD bottommost_compression
TEST_F(CompressionTest, ZSTD_bottommostCompressionTest) {
    Open();
    Options options;
    options.bottommost_compression = CompressionType::kZSTDNotFinalCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    Close();
}
//Zlib bottommost_compression
TEST_F(CompressionTest, Zlib_bottommostCompressionTest) {
    Open();
    Options options;
    options.bottommost_compression = CompressionType::kZlibCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "0"));
    ASSERT_EQ("0", Get("a"));
    Close();
}*/

/**
 * @brief    Use RandomDataNoCompression to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              RandomDataNoCompression --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest, RandomDataNoCompression) {
    Random rnd(301);
    options.compression = CompressionType::kNoCompression;
    options.create_if_missing = true;
    Open(options);
    std::map<std::string, std::string> data;
    for (size_t i = 0; i < kNumKeysWritten; i++) {
        PutRandom(db_,"put-key" + ToString(i), &rnd, &data);
    }
    VerifyData(db_,data);
    Close();
}
/**
 * @brief    Use RandomDataCompression to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              RandomDataCompression --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest, RandomDataCompression) {
    Random rnd(301);
    options.compression = CompressionType::kSnappyCompression;
    options.create_if_missing = true;
    Open(options);
    std::map<std::string, std::string> data;
    for (size_t i = 0; i < kNumKeysWritten; i++) {
        PutRandom(db_,"put-key" + ToString(i), &rnd, &data);
    }
    VerifyData(db_,data);
    Close();
}
/**
 * @brief    Use ChangeSnappyCompressAfterReopen to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              ChangeSnappyCompressAfterReopen --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest, ChangeSnappyCompressAfterReopen) {
    Random rnd(301);
    options.compression = CompressionType::kSnappyCompression;
    options.create_if_missing = true;
    Open(options);
    std::map<std::string, std::string> data;
    for (size_t i = 0; i < kNumKeysWritten; i++) {
        PutRandom(db_,"put-key" + ToString(i), &rnd, &data);
        //cout <<"---"<<&data["put-key" + ToString(i)]<<"---"<<endl;
    }
    options.compression = CompressionType::kNoCompression;
    Reopen(options);
    VerifyData(db_,data);
    Close();
}
/**
 * @brief    Use ChangeNoCompressAfterReopen to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              ChangeNoCompressAfterReopen --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest, ChangeNoCompressAfterReopen) {
    Random rnd(301);
    options.compression = CompressionType::kNoCompression;
    options.create_if_missing = true;
    Open(options);
    std::map<std::string, std::string> data;
    for (size_t i = 0; i < kNumKeysWritten; i++) {
        PutRandom(db_,"put-key" + ToString(i), &rnd, &data);
        //cout <<"---"<<&data["put-key" + ToString(i)]<<"---"<<endl;
    }
    options.compression = CompressionType::kSnappyCompression;
    Reopen(options);
    VerifyData(db_,data);
    Close();
}
/**
 * @brief    Use NoCompressioninCFTest to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              NoCompressioninCFTest --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest,NoCompressioninCFTest) {
    Open();
    Options options;
    options.compression = CompressionType::kNoCompression;
    options.create_if_missing = true;
    CreateColumnFamilies({"one"});
    DestroyAndReopen(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put(0, "cf1"+ToString(i), "v1"+ToString(i)));
        ASSERT_OK(Put(1, "cf2"+ToString(i), "v2"+ToString(i)));
    }
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_EQ("v1"+ToString(i), Get(0, "cf1"+ToString(i)));
        ASSERT_EQ("v2"+ToString(i), Get(1, "cf2"+ToString(i)));
    }
    Close();
}
/**
 * @brief    Use SnappyCompressioninCFTest to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *              SnappyCompressioninCFTest --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(CompressionTest,SnappyCompressioninCFTest) {
    Open();
    Options options;
    options.compression = CompressionType::kSnappyCompression;
    options.create_if_missing = true;
    CreateColumnFamilies({"one"});
    DestroyAndReopen(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_OK(Put(0, "cf1"+ToString(i), "v1"+ToString(i)));
        ASSERT_OK(Put(1, "cf2"+ToString(i), "v2"+ToString(i)));
    }
    for (size_t i=1;i<= kNumKeysWritten;i++){
        ASSERT_EQ("v1"+ToString(i), Get(0, "cf1"+ToString(i)));
        ASSERT_EQ("v2"+ToString(i), Get(1, "cf2"+ToString(i)));
    }
    Close();
}
/**
 * @brief    Use RandomDataCompressioninCF to test CompressionType, and verify the actions of Compression API.
 * @param    CompressionTest --- Class name. \n
 *           RandomDataCompressioninCF --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(CompressionTest, RandomDataCompressioninCFTest) {
    Random rnd(166);
    std::string value;
    Open();
    options.compression = CompressionType::kSnappyCompression;
    options.create_if_missing = true;
    CreateColumnFamilies({"one"});
    DestroyAndReopen(options);
    for (size_t i=1;i<= kNumKeysWritten;i++){
        value = RandomString(&rnd, 128);
        ASSERT_OK(Put(0, "cf1"+ToString(i),value));
        ASSERT_OK(Put(1, "cf2"+ToString(i),value));
        ASSERT_EQ(value, Get(0, "cf1"+ToString(i)));
        ASSERT_EQ(value, Get(1, "cf2"+ToString(i)));
    }
    Close();
}

/*TEST_F(CompressionTest, CompressionStatsTest) {

  Options options;
  options.create_if_missing = true;
  options.compression = CompressionType::kSnappyCompression;
  options.statistics = rocksdb::CreateDBStatistics();
  //options.statistics->stats_level_ = StatsLevel::kExceptTimeForMutex;
  Open(options);

  // Check that compressions occur and are counted when compression is turned on
  Random rnd(301);
  for (int i = 0; i < kNumKeysWritten; ++i) {
    // compressible string
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 128) + std::string(128, 'a')));
  }
  //ASSERT_GT(options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED), 0);

  for (int i = 0; i < kNumKeysWritten; ++i) {
    auto r = Get(Key(i));
  }
  //ASSERT_GT(options.statistics->getTickerCount(NUMBER_BLOCK_DECOMPRESSED), 0);

  options.compression = kSnappyCompression;
  DestroyAndReopen(options);
  uint64_t currentCompressions =
            options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
			cout<<ToString(currentCompressions)<<endl;
  uint64_t currentDecompressions =
            options.statistics->getTickerCount(NUMBER_BLOCK_DECOMPRESSED);

  // Check that compressions do not occur when turned off
  for (int i = 0; i < kNumKeysWritten; ++i) {
    // compressible string
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 128) + std::string(128, 'a')));
  }
  ASSERT_EQ(options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED)
            - currentCompressions, 0);

  for (int i = 0; i < kNumKeysWritten; ++i) {
    auto r = Get(Key(i));
  }
  ASSERT_EQ(options.statistics->getTickerCount(NUMBER_BLOCK_DECOMPRESSED)
            - currentDecompressions, 0);
}*/
}
