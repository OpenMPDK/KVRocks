/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file Common.h
  *  \brief This head file defines the common functions used in the test suite.
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
#ifndef __COMMON__
#define __COMMON__

#include <algorithm>   //hainan.liu
#include <vector> //hainan.liu
#include <cstdlib>
#include <string>
#include <time.h>
#include "gtest/gtest.h"
#include "db.h"
#include "env.h"
#include "status.h"
#include "options.h"
#include "CommonType.h"
#include "db/column_family.h"     //hainan.liu
//#include "Common.h"







//#include "kv_types.h"

using namespace std;

extern INT32 g_nAdiRes; ///< used to store the return value of the tested ADI.

/**
 * @brief     Try to make random buffer value.
 * @param     pBuff --- The buffer array data. \n
              len -- The buffer length.
 * @return    void
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 * @remark    - 1. The buffer value is random.\n
 */
inline void MakeRandBuf(PUINT8 pBuff, UINT32 len)
{
    UINT32 i;

    if(pBuff == NULL)
    {
        return;
    }

    for (i = 0; i < len; i++)
    {
        pBuff[i] = (UINT8)rand();
    }
    return;
}

/**
 * @brief     Try to Create a unified string.
 * @param     i --- the string member\n
 * @return
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   20190215
 * @remark
 */
inline  static std::string Key(int i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "key%06d", i);
    return std::string(buf);
  }




/**
 * @brief     Try to make asynchronous callback function.
 * @param     void
 * @return    void
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 * @remark    - 1. The only used for asynchronous operation.\n
 */
inline void async_cb()
{
    return;
}

/**
 * @brief     Try to Create and Open DB.
 * @param     db --- the operation DB. \n
 *            path --- DB name. \n
 *            flag --- create if missing. \n
 * @return    stat --- The status of the operation.
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline rocksdb::Status CreateOpenDB(rocksdb::DB** db, string path, bool flag) {
    rocksdb::Options options;
    /**
    BlockBasedTableOptions table_options;
    table_options.block_cache = NewLRUCache(16384, 6, false, 0.0);
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    rocksdb::options.table_factory.reset(
        NewBlockBasedTableFactory(table_options));
*/
    options.create_if_missing = flag;
    rocksdb::Status stat = rocksdb::DB::Open(options, path, db);
    cout <<"Open is :"<<endl;
    cout <<stat.ToString()<<endl;
    return stat;
}

 
/**
 * @brief     Try to get random string.
 * @param     len --- the length of the string
 * @return    randString --- The function return random string.
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline string SetRandString(int len) {
    const int SIZE_CHAR = len;
    const char CCH[] = "_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#%^&*<>,.-()_";
    char randString[SIZE_CHAR + 1];
     for (int i = 0; i < SIZE_CHAR; ++i) {
            int x = rand() / (RAND_MAX / (sizeof(CCH) - 1));
            randString[i] = CCH[x];
			}		
    return randString;
}


/**
 * @brief     Try to get random string.
 * @param     len --- the length of the string. \n
 *            num --- the unti char if the string
 * @return    teststring --- The function return  string.
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline string SetString(int len, int num) {
    char teststring[len + 1] ;
    for (int i = 0; i < len+1; i++) {
        teststring[i] = num + '0';
	}
	return teststring;
}

/**
 * @brief     Try to insert string.
 * @param     db --- the operation DB. \n
 *            k_len --- the length of key. \n
 *            v_len --- the length of value. \n
 *            num --- the number of  the insert kv pair
 * @return    void
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline void InsertDB(rocksdb::DB* db, int k_len, int v_len, int num) {
    for (int i = 0; i < num; i++) {
        std::string testkey = SetString(k_len, i);
        std::string testvalue = SetString(v_len, i);
        db->Put(rocksdb::WriteOptions(), testkey, testvalue);
    }
}

/**
 * @brief     Try to insert random string.
 * @param     db --- the operation DB. \n
 *            k_len --- the length of key. \n
 *            v_len --- the length of value. \n
 *            num --- the number of  the insert kv pair
 * @return    void
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline void InsertRandom(rocksdb::DB* db, int k_len, int v_len, int num) {
    for (int i = 0; i < num; ++i) {
        std::string testkey =  SetRandString(k_len);
        std::string testvalue = SetRandString(v_len);
        db->Put(rocksdb::WriteOptions(), testkey, testvalue);
    }
}


/**
 * @brief     Try to get the count of the matched keys.
 * @param     db --- the operation DB. \n
 *            target --- the compare target. 
 * @return    count --- the number of the matched keys
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline int CntNewIterator(rocksdb::DB* db, string target) {
    int count = 0 ;
    rocksdb::Iterator* rit = db->NewIterator(rocksdb::ReadOptions());
    rocksdb::Slice startkey = rocksdb::Slice(target);
	   rit->Seek(startkey);
    while (rit->Valid()) {
        rit->Next();
        count++;
    }
    delete rit;
    return count;
}


/**
 * @brief     Try to check the operation status .
 * @param     db --- the operation DB
 * @return    void
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark 

inline int CheckStatus(rocksdb::Status status, string stat) {
	if (status.code() != rocksdb::Status::Code::stat) 
        return FAIL;
    return SUCCESS;
	
}
 */

/**
 * @brief     Try to clear the env.
 * @param     db --- the operation DB
 * @return    void
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark 
 */
inline void ClearEnv(rocksdb::DB* db) {
    rocksdb::Iterator* rit = db->NewIterator(rocksdb::ReadOptions());
    rocksdb::Slice startkey = rocksdb::Slice("");
	rit->Seek(startkey);
    while (rit->Valid()) {
        db->Delete(rocksdb::WriteOptions(), rit->key());
        rit->Next();
    }
    delete rit;
//    rocksdb::DestroyDB(path, rocksdb::WriteOptions());
}


//hainan.liu
//inline void DestroyDB(string path, const Options& option) {
//    rocksdb::DestroyDB(path, option));
//}




/*
 * Test functions definitions
 * */

/**
 * @brief     Try to clear the env.
 * @param     db --- the operation DB
 *            target --- the seek string of the NewIterator
 * @return    void
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline void ReadUpdateDB(rocksdb::DB* db, string target) {
    std::string testvalue = SetRandString(NORMALVALUE);
    rocksdb::Iterator* rit = db->NewIterator(rocksdb::ReadOptions());
    rocksdb::Slice startkey = rocksdb::Slice(target);
    rit->Seek(startkey);
    while (rit->Valid()) {
        std::string value;
        db->Get(rocksdb::ReadOptions(), rit->key(), &value);
        db->Put(rocksdb::WriteOptions(), rit->key(), testvalue);
        rit->Next();
    }
    delete rit;
}

/**
 * @brief     Try to put KV then delete, check the delete status.
 * @param     db --- the operation DB. \n
 *            k  --- the length of key. \n
 *            v --- the length of value.
 * @return    SUCCESS --- The function execute successfully. \n
   *          FAIL --- The function execution error.  
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark 
 */
inline int PutDelete(rocksdb::DB* db, int k, int v) {
    std::string testkey(k, '1');
    std::string testvalue(v,'a');
    db->Put(rocksdb::WriteOptions(), testkey, testvalue);
    rocksdb::Status status = db->Delete(rocksdb::WriteOptions(), testkey);
    if (status.code() != rocksdb::Status::Code::kOk) { 
        return FAILURE;
    }
    return SUCCESS;

}

/**
 * @brief     Try to make the unit strss to DB.
 * @param     db --- the operation DB. \n
 *            k --- the length of key. \n
 *            v --- the length of value. \n
 *            num --- the number of  the insert kv pair.
 * @return    void
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline static int32_t UnitStress(rocksdb::DB* db, int k, int v, int num) {
    ClearEnv(db);
    InsertRandom(db, k, v, num);
    ReadUpdateDB(db, "w");
    ReadUpdateDB(db, "c");
    ReadUpdateDB(db, "1");
    ClearEnv(db);
    return SUCCESS;
} 

/**
 * @brief     Try to Create and Open DB.
 * @param     db --- the operation DB. \n
 *            k_len --- the length of key. \n
 *            v_len --- the length of value. \n
 *            num --- the number of KV pair
 * @return    SUCCESS(0) --- The function execute successfully. \n
 *            FAIL --- The function execution error.
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline int GetKVS(rocksdb::DB* db, int k_len, int v_len, int num) {
    std::string testkey(k_len, '2');
    std::string testvalue(v_len,'a');
    //put key-value
    db->Put(rocksdb::WriteOptions(), testkey, testvalue);
    std::string value;
    for (int i = 0; i < num; ++i) {
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), testkey, &value);
        if (status.code() != rocksdb::Status::Code::kOk) {
            ClearEnv(db);
            delete db;
            return FAILURE;
        }
        if (value != testvalue) {
            // delete key
            ClearEnv(db);
            delete db;
            return FAILURE;
        }
    }
    ClearEnv(db);
    delete db;
    return SUCCESS;
}

/**
 * @brief     Try to Create and Open DB.
 * @param     db --- the operation DB. \n
 *            k --- the operation key. \n
 *            v --- the operation value
 * @return    SUCCESS(0) --- The function execute successfully. \n
 *            FAIL --- The function execution error.
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline int CheckKV(rocksdb::DB* db, string testkey, string testvalue) {
    std::string value;
    db->Get(rocksdb::ReadOptions(), testkey, &value);
    db->Delete(rocksdb::WriteOptions(), testkey);
//    delete db;
    if (value != testvalue)
        return FAILURE;
    return SUCCESS;
}

/**
 * @brief     Try to Create and Open DB.
 * @param     db --- the operation DB. \n
 *            k_len --- the length of key. \n
 *            v_len --- the length of value. \n
 *            num --- the number of KV pair.
 * @return    SUCCESS(0) --- The function execute successfully. \n
 *            FAIL --- The function execution error.
 * @author    Lining Dou(lining.dou@sumsung.com)
 * @version   v0.1
 * @remark
 */
inline int PutCheckKVS(rocksdb::DB* db, int k_len, int v_len, int num) {
    for (int i = 0; i < num; ++i) {
        std::string testkey = SetRandString(k_len);
		std::string testvalue = SetRandString(v_len);
        rocksdb::Status status = db->Put(rocksdb::WriteOptions(), testkey, testvalue);
        if (status.code() != rocksdb::Status::Code::kOk) {
            ClearEnv(db);
            delete db;
            return FAILURE;
        }
        std::string value;
        status = db->Get(rocksdb::ReadOptions(), testkey, &value);
		if (status.code() != rocksdb::Status::Code::kOk || value != testvalue){
			ClearEnv(db);
            delete db;    
		    return FAILURE;
		}

    }
    ClearEnv(db);
    delete db;
    return SUCCESS;
}

/**
 * @brief     Try to construct an inexistent key.
 * @param     pKey --- The inexistent key. \n
              len -- The key length.
 * @return    TRUE32(1) --- The function execute successfully. \n
 *            FALSE32(0) --- The function execution error.
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 */
extern BOOL32 MakeInexistentKey(PUINT8 pKey, UINT32 len);
 
//Status CreateOpenDB(DB** db, string path, bool flag); 
 

 
/**
 * @brief     Try to get a key-value pair.
 * @param     keyLen --- The key length. \n
              valLen -- The value length. \n
			  offLen -- The value offset length.
			  opt -- The option value.
 * @return    pair --- The key-value pair. \n
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 * @remark    - 1. The key and value are random data.\n
 */
//extern kv_pair GenKVPair(UINT16 keyLen, UINT32 valLen, UINT32 offLen, INT32 opt);

/**
 * @brief     Try to get a key-value pair.
 * @param     pkey --- The key data. \n
              valLen -- The value length. \n
			  offLen -- The value offset length. \n
			  opt -- The option value. \n
 * @return    pair --- The key-value pair. \n
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 * @remark    - 1. Caller must ensure the key is ginven.\n
 */
//extern kv_pair GenExistKVPair(kv_key pkey, UINT32 valLen, UINT32 offLen, INT32 opt);

 /**
 * @brief     Try to get an iterate key-value pair.
 * @param     iteHdl --- The iterate handle. \n
              valLen -- The value length. \n
			  offLen -- The value offset length. \n
			  opt -- The option value. \n
 * @return    kv_it --- The iterate key-value pair. \n
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 * @remark   
 */
//kv_iterate GenIterateKVPair(uint8_t iteHdl, UINT32 valLen, UINT32 offLen, INT32 opt);


namespace rocksdb {

namespace test {

// Return the directory to use for temporary storage.
std::string TmpDir(Env* env = Env::Default());

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
int RandomSeed();

//hainan.liu
// Store in *dst a random string of length "len" and return a Slice that
// references the generated data.
extern Slice RandomString(Random* rnd, int len, std::string* dst);


::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s);

#define ASSERT_OK(s) ASSERT_PRED_FORMAT1(rocksdb::test::AssertStatus, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).ok())
#define EXPECT_OK(s) EXPECT_PRED_FORMAT1(rocksdb::test::AssertStatus, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).ok())   

}

////hainan.liu for BaseTest
//class DBTestBase : public testing::Test {
//public:
//	// Sequence of option configurations to try
//enum OptionConfig :
//	int {
//		kDefault = 0,
//		kBlockBasedTableWithPrefixHashIndex = 1,
//		kBlockBasedTableWithWholeKeyHashIndex = 2,
//		kPlainTableFirstBytePrefix = 3,
//		kPlainTableCappedPrefix = 4,
//		kPlainTableCappedPrefixNonMmap = 5,
//		kPlainTableAllBytesPrefix = 6,
//		kVectorRep = 7,
//		kHashLinkList = 8,
//		kHashCuckoo = 9,
//		kMergePut = 10,
//		kFilter = 11,
//		kFullFilterWithNewTableReaderForCompactions = 12,
//		kUncompressed = 13,
//		kNumLevel_3 = 14,
//		kDBLogDir = 15,
//		kWalDirAndMmapReads = 16,
//		kManifestFileSize = 17,
//		kPerfOptions = 18,
//		kHashSkipList = 19,
//		kUniversalCompaction = 20,
//		kUniversalCompactionMultiLevel = 21,
//		kCompressedBlockCache = 22,
//		kInfiniteMaxOpenFiles = 23,
//		kxxHashChecksum = 24,
//		kFIFOCompaction = 25,
//		kOptimizeFiltersForHits = 26,
//		kRowCache = 27,
//		kRecycleLogFiles = 28,
//		kConcurrentSkipList = 29,
//		kPipelinedWrite = 30,
//		kEnd = 31,
//		kDirectIO = 32,
//		kLevelSubcompactions = 33,
//		kUniversalSubcompactions = 34,
//		kBlockBasedTableWithIndexRestartInterval = 35,
//		kBlockBasedTableWithPartitionedIndex = 36,
//		kPartitionedFilterWithNewTableReaderForCompactions = 37,
//		kDifferentDataDir,
//		kWalDir,
//		kSyncWal,
//		kWalDirSyncWal,
//		kMultiLevels,
//
//	};
//
//public:
//	std::string dbname_;
//	std::string alternative_wal_dir_;
//	std::string alternative_db_log_dir_;
////  MockEnv* mem_env_;
//	SpecialEnv* env_;
//	DB* db_;
//	std::vector<ColumnFamilyHandle*> handles_;
//
//	int option_config_;
//	Options last_options_;
//
//	// Skip some options, as they may not be applicable to a specific test.
//	// To add more skip constants, use values 4, 8, 16, etc.
//	enum OptionSkip {
//		kNoSkip = 0,
//		kSkipDeletesFilterFirst = 1,
//		kSkipUniversalCompaction = 2,
//		kSkipMergePut = 4,
//		kSkipPlainTable = 8,
//		kSkipHashIndex = 16,
//		kSkipNoSeekToLast = 32,
//		kSkipHashCuckoo = 64,
//		kSkipFIFOCompaction = 128,
//		kSkipMmapReads = 256,
//	};
//
//	// Do n memtable compactions, each of which produces an sstable
//// covering the range [small,large].
//	void MakeTables(int n, const std::string& small,
//	                const std::string& large, int cf) {
//		for (int i = 0; i < n; i++) {
//			ASSERT_OK(Put(cf, small, "begin"));
//			ASSERT_OK(Put(cf, large, "end"));
//			ASSERT_OK(Flush(cf));
//			MoveFilesToLevel(n - i - 1, cf);
//		}
//	}
//
////modify: need to complete the function
//	void MoveFilesToLevel(int level, int cf) {
//		/*   for (int l = 0; l < level; ++l) {
//		    if (cf > 0) {
//		      dbfull()->TEST_CompactRange(l, nullptr, nullptr, handles_[cf]);
//		    } else {
//		      dbfull()->TEST_CompactRange(l, nullptr, nullptr);
//		    }
//		  } */
//	}
//
//// Prevent pushing of new sstables into deeper levels by adding
//// tables that cover a specified range to all levels.
//	void FillLevels(const std::string& smallest,
//	                const std::string& largest, int cf) {
//		(db_->NumberLevels(handles_[cf]), smallest, largest, cf);
//	}
//	void Close() {
//        for (auto h : handles_) {
//			db_->DestroyColumnFamilyHandle(h);
//		}
//		handles_.clear();
//		delete db_;
//		db_ = nullptr;
//	}
//	// Return the current option configuration.
//	Options CurrentOptions() {
//		Options options;
//		//options.env = env_;
//		options.create_if_missing = true;
//		return options;
//	}
//
//	Options GetDefaultOptions() {
//		Options options;
//		options.write_buffer_size = 4090 * 4096;
//		options.target_file_size_base = 2 * 1024 * 1024;
//		options.max_bytes_for_level_base = 10 * 1024 * 1024;
//		options.max_open_files = 5000;
//		options.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
//		options.compaction_pri = CompactionPri::kByCompensatedSize;
//		return options;
//	}
//
//	Options CurrentOptions(const OptionsOverride& options_override = OptionsOverride()) const {
//		return GetOptions(option_config_, GetDefaultOptions(), options_override);
//	}
//
//	Options CurrentOptions(const Options& default_options, const OptionsOverride& options_override = OptionsOverride()) const {
//		default_options = GetDefaultOptions();
//		return GetOptions(option_config_, default_options, options_override);
//	}
//
//	Options GetOptions(
//	    int option_config, const Options& default_options,
//	    const OptionsOverride& options_override) const {
//		// this redundant copy is to minimize code change w/o having lint error.
//		default_options = GetDefaultOptions();
//		Options options = default_options;
//		BlockBasedTableOptions table_options;
//		bool set_block_based_table_factory = true;
////#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) &&  \
//  !defined(OS_AIX)
///* 		SyncPoint::GetInstance()->ClearCallBack(
//		    "NewRandomAccessFile:O_DIRECT");
//		SyncPoint::GetInstance()->ClearCallBack(
//		    "NewWritableFile:O_DIRECT"); */
////#endif
//
//		switch (option_config) {
//#ifndef ROCKSDB_LITE
//		case kHashSkipList:
//			options.prefix_extractor.reset(NewFixedPrefixTransform(1));
//			options.memtable_factory.reset(NewHashSkipListRepFactory(16));
//			options.allow_concurrent_memtable_write = false;
//			break;
//		case kPlainTableFirstBytePrefix:
//			options.table_factory.reset(new PlainTableFactory());
//			options.prefix_extractor.reset(NewFixedPrefixTransform(1));
//			options.allow_mmap_reads = true;
//			options.max_sequential_skip_in_iterations = 999999;
//			set_block_based_table_factory = false;
//			break;
//		case kPlainTableCappedPrefix:
//			options.table_factory.reset(new PlainTableFactory());
//			options.prefix_extractor.reset(NewCappedPrefixTransform(8));
//			options.allow_mmap_reads = true;
//			options.max_sequential_skip_in_iterations = 999999;
//			set_block_based_table_factory = false;
//			break;
//		case kPlainTableCappedPrefixNonMmap:
//			options.table_factory.reset(new PlainTableFactory());
//			options.prefix_extractor.reset(NewCappedPrefixTransform(8));
//			options.allow_mmap_reads = false;
//			options.max_sequential_skip_in_iterations = 999999;
//			set_block_based_table_factory = false;
//			break;
//		case kPlainTableAllBytesPrefix:
//			options.table_factory.reset(new PlainTableFactory());
//			options.prefix_extractor.reset(NewNoopTransform());
//			options.allow_mmap_reads = true;
//			options.max_sequential_skip_in_iterations = 999999;
//			set_block_based_table_factory = false;
//			break;
//		case kVectorRep:
//			options.memtable_factory.reset(new VectorRepFactory(100));
//			options.allow_concurrent_memtable_write = false;
//			break;
//		case kHashLinkList:
//			options.prefix_extractor.reset(NewFixedPrefixTransform(1));
//			options.memtable_factory.reset(
//			    NewHashLinkListRepFactory(4, 0, 3, true, 4));
//			options.allow_concurrent_memtable_write = false;
//			break;
//		case kHashCuckoo:
//			options.memtable_factory.reset(
//			    NewHashCuckooRepFactory(options.write_buffer_size));
//			options.allow_concurrent_memtable_write = false;
//			break;
//#endif  // ROCKSDB_LITE
//		case kMergePut:
//			options.merge_operator = MergeOperators::CreatePutOperator();
//			break;
//		case kFilter:
//			table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
//			break;
//		case kFullFilterWithNewTableReaderForCompactions:
//			table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
//			options.new_table_reader_for_compaction_inputs = true;
//			options.compaction_readahead_size = 10 * 1024 * 1024;
//			break;
//		case kPartitionedFilterWithNewTableReaderForCompactions:
//			table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
//			table_options.partition_filters = true;
//			table_options.index_type =
//			    BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
//			options.new_table_reader_for_compaction_inputs = true;
//			options.compaction_readahead_size = 10 * 1024 * 1024;
//			break;
//		case kUncompressed:
//			options.compression = kNoCompression;
//			break;
//		case kNumLevel_3:
//			options.num_levels = 3;
//			break;
//		case kDBLogDir:
//			options.db_log_dir = alternative_db_log_dir_;
//			break;
//		case kWalDirAndMmapReads:
//			options.wal_dir = alternative_wal_dir_;
//			// mmap reads should be orthogonal to WalDir setting, so we piggyback to
//			// this option config to test mmap reads as well
//			options.allow_mmap_reads = true;
//			break;
//		case kManifestFileSize:
//			options.max_manifest_file_size = 50;  // 50 bytes
//			break;
//		case kPerfOptions:
//			options.soft_rate_limit = 2.0;
//			options.delayed_write_rate = 8 * 1024 * 1024;
//			options.report_bg_io_stats = true;
//			// TODO(3.13) -- test more options
//			break;
//		case kUniversalCompaction:
//			options.compaction_style = kCompactionStyleUniversal;
//			options.num_levels = 1;
//			break;
//		case kUniversalCompactionMultiLevel:
//			options.compaction_style = kCompactionStyleUniversal;
//			options.max_subcompactions = 4;
//			options.num_levels = 8;
//			break;
//		case kCompressedBlockCache:
//			options.allow_mmap_writes = true;
//			table_options.block_cache_compressed = NewLRUCache(8 * 1024 * 1024);
//			break;
//		case kInfiniteMaxOpenFiles:
//			options.max_open_files = -1;
//			break;
//		case kxxHashChecksum: {
//			table_options.checksum = kxxHash;
//			break;
//		}
//		case kFIFOCompaction: {
//			options.compaction_style = kCompactionStyleFIFO;
//			break;
//		}
//		case kBlockBasedTableWithPrefixHashIndex: {
//			table_options.index_type = BlockBasedTableOptions::kHashSearch;
//			options.prefix_extractor.reset(NewFixedPrefixTransform(1));
//			break;
//		}
//		case kBlockBasedTableWithWholeKeyHashIndex: {
//			table_options.index_type = BlockBasedTableOptions::kHashSearch;
//			options.prefix_extractor.reset(NewNoopTransform());
//			break;
//		}
//		case kBlockBasedTableWithPartitionedIndex: {
//			table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
//			options.prefix_extractor.reset(NewNoopTransform());
//			break;
//		}
//		case kBlockBasedTableWithIndexRestartInterval: {
//			table_options.index_block_restart_interval = 8;
//			break;
//		}
//		case kOptimizeFiltersForHits: {
//			options.optimize_filters_for_hits = true;
//			set_block_based_table_factory = true;
//			break;
//		}
//		case kRowCache: {
//			options.row_cache = NewLRUCache(1024 * 1024);
//			break;
//		}
//		case kRecycleLogFiles: {
//			options.recycle_log_file_num = 2;
//			break;
//		}
//		case kLevelSubcompactions: {
//			options.max_subcompactions = 4;
//			break;
//		}
//		case kUniversalSubcompactions: {
//			options.compaction_style = kCompactionStyleUniversal;
//			options.num_levels = 8;
//			options.max_subcompactions = 4;
//			break;
//		}
//		case kConcurrentSkipList: {
//			options.allow_concurrent_memtable_write = true;
//			options.enable_write_thread_adaptive_yield = true;
//			break;
//		}
//		case kDirectIO: {
//			options.use_direct_reads = true;
//			options.use_direct_io_for_flush_and_compaction = true;
//			options.compaction_readahead_size = 2 * 1024 * 1024;
//#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_SOLARIS) && \
//!defined(OS_AIX)
//			rocksdb::SyncPoint::GetInstance()->SetCallBack(
//			"NewWritableFile:O_DIRECT", [&](void* arg) {
//				int* val = static_cast<int*>(arg);
//				*val &= ~O_DIRECT;
//			});
//			rocksdb::SyncPoint::GetInstance()->SetCallBack(
//			"NewRandomAccessFile:O_DIRECT", [&](void* arg) {
//				int* val = static_cast<int*>(arg);
//				*val &= ~O_DIRECT;
//			});
//			rocksdb::SyncPoint::GetInstance()->EnableProcessing();
//#endif
//			break;
//		}
//		case kPipelinedWrite: {
//			options.enable_pipelined_write = true;
//			break;
//		}
//
//		default:
//			break;
//		}
//
//		if (options_override.filter_policy) {
//			table_options.filter_policy = options_override.filter_policy;
//			table_options.partition_filters = options_override.partition_filters;
//			table_options.metadata_block_size = options_override.metadata_block_size;
//		}
//		if (set_block_based_table_factory) {
//			options.table_factory.reset(NewBlockBasedTableFactory(table_options));
//		}
//		options.env = env_;
//		options.create_if_missing = true;
//		options.fail_if_options_file_error = true;
//		return options;
//	}
//
//	Status TryReopen(const Options& options) {
//		Close();
//		last_options_.table_factory.reset();
//		// Note: operator= is an unsafe approach here since it destructs shared_ptr in
//		// the same order of their creation, in contrast to destructors which
//		// destructs them in the opposite order of creation. One particular problme is
//		// that the cache destructor might invoke callback functions that use Option
//		// members such as statistics. To work around this problem, we manually call
//		// destructor of table_facotry which eventually clears the block cache.
//		last_options_ = options;
//		return DB::Open(options, dbname_, &db_);
//	}
//
//	void Reopen(const Options& options) {
//		ASSERT_OK(TryReopen(options));
//	}
//	DBTestBase(const std::string path)
//		: env_(new SpecialEnv(Env::Default())), 
//		option_config_(kDefault) {
//		Options options;
//	    options.create_if_missing = true;		
//		// env_->SetBackgroundThreads(1, Env::LOW);
//		// env_->SetBackgroundThreads(1, Env::HIGH);
// 		//dbname_ = "/tmp";
//		//dbname_ += path; 
//		dbname_ = test::TmpDir(env_) + path;
//		alternative_wal_dir_ = dbname_ + "/wal";
//	     alternative_db_log_dir_ = dbname_ + "/db_log_dir";
//		//auto options = CurrentOptions();
//		options.env = env_;
//		auto delete_options = options;
//		delete_options.wal_dir = alternative_wal_dir_;  
//
//		db_ = nullptr;
//		Reopen(options);
//		//Random::GetTLSInstance()->Reset(0xdeadbeef);
//	}
//
//	~DBTestBase() {
//		/* 		rocksdb::SyncPoint::GetInstance()->DisableProcessing();
//				rocksdb::SyncPoint::GetInstance()->LoadDependency( {});
//			rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks(); */
//			
//		Close();
//		Options options;
///*  		options.db_paths.emplace_back(dbname_, 0);
//		options.db_paths.emplace_back(dbname_ + "_2", 0);
//		options.db_paths.emplace_back(dbname_ + "_3", 0);
//		options.db_paths.emplace_back(dbname_ + "_4", 0);
//		options.env = env_;  */
//     
// 		if (getenv("KEEP_DB")) {
//			printf("DB is still at %s\n", dbname_.c_str());
//		} else {
//			EXPECT_OK(DestroyDB(dbname_, options));
//		} 
//		//delete env_;
//	};
//
//	Status Flush(int cf = 0) {
//		if (cf == 0) {
//			return db_->Flush(FlushOptions());
//		} else {
//			return db_->Flush(FlushOptions(), handles_[cf]);
//		}
//	}
//
//	Status DBTestBase::Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions()) {
//		return db_->Put(wo, k, v);
//	}
//
//	Status DBTestBase::Put(int cf, const Slice& k, const Slice& v,
//	           WriteOptions wo = WriteOptions()) {
//	    cout << "the put cf is " <<cf<<endl;
//		return db_->Put(wo, handles_[cf], k, v);
//	}
//
//	Status Delete(const std::string& k) {
//		return db_->Delete(WriteOptions(), k);
//	}
//
//	Status Delete(int cf, const std::string& k) {
//		return db_->Delete(WriteOptions(), handles_[cf], k);
//	}
//
//	std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
//		ReadOptions options;
//		options.verify_checksums = true;
//		options.snapshot = snapshot;
//		std::string result;
//		Status s = db_->Get(options, k, &result);
//		if (s.IsNotFound()) {
//			result = "NOT_FOUND";
//		} else if (!s.ok()) {
//			result = s.ToString();
//		}
//		return result;
//	}
//
//	std::string Get(int cf, const std::string& k,
//	                const Snapshot* snapshot = nullptr) {
//		ReadOptions options;
//		options.verify_checksums = true;
//		options.snapshot = snapshot;
//		std::string result;
//		Status s = db_->Get(options, handles_[cf], k, &result);
//		if (s.IsNotFound()) {
//			result = "NOT_FOUND";
//		} else if (!s.ok()) {
//			result = s.ToString();
//		}
//		return result;
//	}
//	Status ReadOnlyReopen(const Options& options) {
//		return DB::OpenForReadOnly(options, dbname_, &db_);
//	}
//
//	std::string DummyString(size_t len, char c) {
//		return std::string(len, c);
//	}
//
//	int NumTableFilesAtLevel(int level, int cf) {
//		std::string property;
//		if (cf == 0) {
//			// default cfd
//			EXPECT_TRUE(db_->GetProperty(
//			                "rocksdb.num-files-at-level" + NumberToString(level), &property));
//		} else {
//			EXPECT_TRUE(db_->GetProperty(
//			                handles_[cf], "rocksdb.num-files-at-level" + NumberToString(level),
//			                &property));
//		}
//		return atoi(property.c_str());
//	}
//
//	DBImpl* dbfull() {
//		return reinterpret_cast<DBImpl*>(db_);
//	}
//
//	Status TryReopenWithColumnFamilies(
//	    const std::vector<std::string>& cfs, const std::vector<Options>& options) {
//		Close();
//		EXPECT_EQ(cfs.size(), options.size());
//		std::vector<ColumnFamilyDescriptor> column_families;
//		for (size_t i = 0; i < cfs.size(); ++i) {
//			column_families.push_back(ColumnFamilyDescriptor(cfs[i], options[i]));
//		}
//		DBOptions db_opts = DBOptions(options[0]);
//		return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
//	}
//
//	Status TryReopenWithColumnFamilies(
//	    const std::vector<std::string>& cfs, const Options& options) {
//		Close();
//		std::vector<Options> v_opts(cfs.size(), options);
//		return TryReopenWithColumnFamilies(cfs, v_opts);
//	}
//
//	void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
//	                              const std::vector<Options>& options) {
//		ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
//	}
//
//	void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
//	                              const Options& options) {
//		ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
//	}
//
//	void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
//	                           const Options& options) {
//		CreateColumnFamilies(cfs, options);
//		std::vector<std::string> cfs_plus_default = cfs;
//		cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
//		ReopenWithColumnFamilies(cfs_plus_default, options);
//	}
//
//	void CreateColumnFamilies(const std::vector<std::string>& cfs,
//	                          const Options& options) {
//		ColumnFamilyOptions cf_opts(options);
//		size_t cfi = handles_.size();
//		handles_.resize(cfi + cfs.size());
//        for (auto cf : cfs) {
//			cout << "all cf is  "	<<cf<<endl;	
//			ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
//		}
//	}
//	
//	Status DBTestBase::SingleDelete(const std::string& k) {
//  return db_->SingleDelete(WriteOptions(), k);
//}
//
//Status DBTestBase::SingleDelete(int cf, const std::string& k) {
//  return db_->SingleDelete(WriteOptions(), handles_[cf], k);
//}
//
//	bool ShouldSkipOptions(int option_config, int skip_mask) {
//#ifdef ROCKSDB_LITE
//		// These options are not supported in ROCKSDB_LITE
//		if (option_config == kHashSkipList ||
//		        option_config == kPlainTableFirstBytePrefix ||
//		        option_config == kPlainTableCappedPrefix ||
//		        option_config == kPlainTableCappedPrefixNonMmap ||
//		        option_config == kPlainTableAllBytesPrefix ||
//		        option_config == kVectorRep || option_config == kHashLinkList ||
//		        option_config == kHashCuckoo || option_config == kUniversalCompaction ||
//		        option_config == kUniversalCompactionMultiLevel ||
//		        option_config == kUniversalSubcompactions ||
//		        option_config == kFIFOCompaction ||
//		        option_config == kConcurrentSkipList) {
//			return true;
//		}
//#endif
//
//		if ((skip_mask & kSkipUniversalCompaction) &&
//		        (option_config == kUniversalCompaction ||
//		         option_config == kUniversalCompactionMultiLevel)) {
//			return true;
//		}
//		if ((skip_mask & kSkipMergePut) && option_config == kMergePut) {
//			return true;
//		}
//		if ((skip_mask & kSkipNoSeekToLast) &&
//		        (option_config == kHashLinkList || option_config == kHashSkipList)) {
//			return true;
//		}
//		if ((skip_mask & kSkipPlainTable) &&
//		        (option_config == kPlainTableAllBytesPrefix ||
//		         option_config == kPlainTableFirstBytePrefix ||
//		         option_config == kPlainTableCappedPrefix ||
//		         option_config == kPlainTableCappedPrefixNonMmap)) {
//			return true;
//		}
//		if ((skip_mask & kSkipHashIndex) &&
//		        (option_config == kBlockBasedTableWithPrefixHashIndex ||
//		         option_config == kBlockBasedTableWithWholeKeyHashIndex)) {
//			return true;
//		}
//		if ((skip_mask & kSkipHashCuckoo) && (option_config == kHashCuckoo)) {
//			return true;
//		}
//		if ((skip_mask & kSkipFIFOCompaction) && option_config == kFIFOCompaction) {
//			return true;
//		}
//		if ((skip_mask & kSkipMmapReads) && option_config == kWalDirAndMmapReads) {
//			return true;
//		}
//		return false;
//	}
//
//	std::unique_ptr<Env> base_env_;
//	
//	bool ChangeOptions() {
//		option_config_++;
//		if (option_config_ >= kEnd) {
//			return false;
//		} else {
//			if (option_config_ == kMultiLevels) {
//				base_env_.reset(Env::Default());
//				// base_env_.reset(new MockEnv(Env::Default()));
//			}
//			return true;
//		}
//	}
//// Switch to a fresh database with the next option configuration to
//// test.  Return false if there are no more configurations to test.
//	bool ChangeOptions(int skip_mask) {
//		cout <<"********************the changed op is : "<<skip_mask<<endl;
//		for (option_config_++; option_config_ < kEnd; option_config_++) {
//			if (ShouldSkipOptions(option_config_, skip_mask)) {
//				continue;
//			}
//			break;
//		}
//
//		if (option_config_ >= kEnd) {
//			Destroy(last_options_);
//			return false;
//		} else {
//			auto options = CurrentOptions();
//			options.create_if_missing = true;
//			DestroyAndReopen(options);
//			return true;
//		}
//	}
//
//	void Destroy(const Options& options) {
//		Close();
//		ASSERT_OK(DestroyDB(dbname_, options));
//	}
//
//	void DestroyAndReopen(const Options& options) {
//		// Destroy using last options
//		Destroy(last_options_);
//		ASSERT_OK(TryReopen(options));
//	}
//
//	std::string AllEntriesFor(const Slice& user_key, int cf) {
//		//Arena arena;
//		cout <<"we are here now 1 "<<endl;
//		auto options = CurrentOptions();
//		InternalKeyComparator icmp(options.comparator);
//		RangeDelAggregator range_del_agg(icmp, {} /* snapshots */);
//		cout <<"we are here now 2 "<<endl;
//		ScopedArenaIterator iter;
//		InternalKey target(user_key, kMaxSequenceNumber, kTypeValue);
//		cout <<"we are here now 3 "<<endl;
//		iter->Seek(target.Encode());
//		cout <<"we are here now 4 "<<endl;
//		std::string result;
//		if (!iter->status().ok()) {
//			result = iter->status().ToString();
//		} else {
//			result = "[ ";
//			bool first = true;
//			while (iter->Valid()) {
//				ParsedInternalKey ikey(Slice(), 0, kTypeValue);
//				if (!ParseInternalKey(iter->key(), &ikey)) {
//					result += "CORRUPTED";
//				} else {
//					if (!last_options_.comparator->Equal(ikey.user_key, user_key)) {
//						break;
//					}
//					if (!first) {
//						result += ", ";
//					}
//					first = false;
//					switch (ikey.type) {
//					case kTypeValue:
//						result += iter->value().ToString();
//						break;
//					case kTypeMerge:
//						// keep it the same as kTypeValue for testing kMergePut
//						result += iter->value().ToString();
//						break;
//					case kTypeDeletion:
//						result += "DEL";
//						break;
//					case kTypeSingleDeletion:
//						result += "SDEL";
//						break;
//					default:
//						assert(false);
//						break;
//					}
//				}
//				iter->Next();
//			}
//			if (!first) {
//				result += " ";
//			}
//			result += "]";
//		}
//		return result;
//	}
//
//	// Switch between different compaction styles.
//	bool ChangeCompactOptions() {
//		cout << "the OP config is : +++++++"<<option_config_<<endl;
//		if (option_config_ == kDefault) {
//			option_config_ = kUniversalCompaction;
//			Destroy(last_options_);
//			auto options = CurrentOptions();
//			options.create_if_missing = true;
//			TryReopen(options);
//			return true;
//		} else if (option_config_ == kUniversalCompaction) {
//			option_config_ = kUniversalCompactionMultiLevel;
//			Destroy(last_options_);
//			auto options = CurrentOptions();
//			options.create_if_missing = true;
//			TryReopen(options);
//			return true;
//		} else if (option_config_ == kUniversalCompactionMultiLevel) {
//			
//			option_config_ = kLevelSubcompactions;
//			Destroy(last_options_);
//			auto options = CurrentOptions();
//			assert(options.max_subcompactions >= 1);
//			TryReopen(options);
//			return true;
//		} else if (option_config_ == kLevelSubcompactions) {
//			option_config_ = kUniversalSubcompactions;
//			Destroy(last_options_);
//			auto options = CurrentOptions();
//			assert(options.max_subcompactions >= 1);
//			TryReopen(options);
//			return true;
//		} else {
//			return false;
//		}
//	}
//
//	uint64_t GetNumSnapshots() {
//		uint64_t int_num;
//		dbfull()->GetIntProperty("rocksdb.num-snapshots", &int_num);
//		cout << "the snap number is :   "<<int_num<<endl;
//		//EXPECT_TRUE(dbfull()->GetIntProperty("rocksdb.num-snapshots", &int_num));
//		return int_num;
//	}
//
//	uint64_t GetTimeOldestSnapshots() {
//		uint64_t int_num;
//		EXPECT_TRUE(
//		    dbfull()->GetIntProperty("rocksdb.oldest-snapshot-time", &int_num));
//		return int_num;
//	}
//
//	std::vector<std::uint64_t> ListTableFiles(Env* env,
//	        const std::string& path) {
//		std::vector<std::string> files;
//		std::vector<uint64_t> file_numbers;
//		env->GetChildren(path, &files);
//		uint64_t number;
//		FileType type;
//		cout <<"~~~~~~~~~~~~~~~~ListTableFiles~~~~~~"<<files.size()<<endl;
//		for (size_t i = 0; i < files.size(); ++i) {
//			if (ParseFileName(files[i], &number, &type)) {
//				if (type == kTableFile) {
//					file_numbers.push_back(number);
//				}
//			}
//		}
//		return file_numbers;
//	}
//
//}
//
//class DBBasicTest : public DBTestBase {
//public:
//	DBBasicTest() : DBTestBase("/ddtest") {}
//};
//

//hainan.liu 2019/03/11
//class ColumnFamily : public testing::Test {
//public:
//	 // Sequence of option configurations to try
//	 enum OptionConfig : int {
//	   kDefault = 0,
//	   kBlockBasedTableWithPrefixHashIndex = 1,
//	   kBlockBasedTableWithWholeKeyHashIndex = 2,
//	   kPlainTableFirstBytePrefix = 3,
//	   kPlainTableCappedPrefix = 4,
//	   kPlainTableCappedPrefixNonMmap = 5,
//	   kPlainTableAllBytesPrefix = 6,
//	   kVectorRep = 7,
//	   kHashLinkList = 8,
//	   kHashCuckoo = 9,
//	   kMergePut = 10,
//	   kFilter = 11,
//	   kFullFilterWithNewTableReaderForCompactions = 12,
//	   kUncompressed = 13,
//	   kNumLevel_3 = 14,
//	   kDBLogDir = 15,
//	   kWalDirAndMmapReads = 16,
//	   kManifestFileSize = 17,
//	   kPerfOptions = 18,
//	   kHashSkipList = 19,
//	   kUniversalCompaction = 20,
//	   kUniversalCompactionMultiLevel = 21,
//	   kCompressedBlockCache = 22,
//	   kInfiniteMaxOpenFiles = 23,
//	   kxxHashChecksum = 24,
//	   kFIFOCompaction = 25,
//	   kOptimizeFiltersForHits = 26,
//	   kRowCache = 27,
//	   kRecycleLogFiles = 28,
//	   kConcurrentSkipList = 29,
//	   kPipelinedWrite = 30,
//	   kEnd = 31,
//	   kDirectIO = 32,
//	   kLevelSubcompactions = 33,
//	   kUniversalSubcompactions = 34,
//	   kBlockBasedTableWithIndexRestartInterval = 35,
//	   kBlockBasedTableWithPartitionedIndex = 36,
//	   kPartitionedFilterWithNewTableReaderForCompactions = 37,
//	 };
//
//
//
//public:
//	std::string dbname_;
//	std::string alternative_wal_dir_;
//	std::string alternative_db_log_dir_;
////	MockEnv* mem_env_;
////	SpecialEnv* env_;
//	DB* db_;
//	std::vector<ColumnFamilyHandle*> handles_;
//        int option_config_;
//        Options last_options_;
//
//explicit ColumnFamily(const std::string path);
//
//~ColumnFamily();
//
//void CreateColumnFamilies(const std::vector<std::string>& cfs,
//						  const Options& options);
//
//void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
//						   const Options& options);
//
//void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
//							  const std::vector<Options>& options);
//
//void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
//							  const Options& options);
//
//Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
//								   const std::vector<Options>& options);
//
//Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
//								   const Options& options);
//
//Status Put(int cf, const Slice& k, const Slice& v,
//		   WriteOptions wo = WriteOptions());
//
//Status Merge(int cf, const Slice& k, const Slice& v,
//			 WriteOptions wo = WriteOptions());
//
//Status Delete(int cf, const std::string& k);
//
//
//Status SingleDelete(int cf, const std::string& k);
//
//std::string Get(int cf, const std::string& k,
//				const Snapshot* snapshot = nullptr);
//
//void Reopen(const Options& options);
//
//void Close();
//
//Status TryReopen(const Options& options);
//};
//
  // Return the current option configuration.
//Options CurrentOptions(const anon::OptionsOverride& options_override =
//                             anon::OptionsOverride()) const;
//
//Options CurrentOptions(const Options& default_options,
//                         const anon::OptionsOverride& options_override =
//                             anon::OptionsOverride()) const;
//


}




#endif //__COMMON__
