/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file kvdb_Stress.cc
  *  \brief Test cases code of the KBDB Stress().
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
#include <time.h>
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
#include "compaction_filter.h"
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

using namespace std;

namespace rocksdb {

/*
 * Test functions definitions
 * */

/**
 * @brief     Try to Repeat Unit stress with defined KV pairs and running time , and verify the actions of API.
 * @param     k --- key length. \n
 *            v --- value length. \n
 *            path --- DB name. \n
 *            flag --- create if missing. \n
 *            num --- the number of KV pairs. \n
 *            ti --- the stress running time in seconds.
 * @return    SUCCESS(0) --- The function execution successfully . \n
 *            other-value --- The function execute error.
 * @author    Lining Dou (Lining.dou@sumsung.com)
 * @version   v0.1
 * @remark    - 1.Caller must ensure the data are valid.
 */
static int32_t Stress(int k, int v, string path, bool flag, int num, int ti) {
	srand((unsigned)time(0));
	double  start, end, seconds;
	start = clock();
	end = clock();
	while(seconds <= ti) {
		DB* db = NULL;
		CreateOpenDB(&db, path, flag);
		UnitStress(db, k, v, num);
		end = clock();
		seconds  =(double )(end - start)/CLOCKS_PER_SEC;
		delete db;
	}
	return SUCCESS;

}

/**
 * @brief     Try to Repeat Unit stress with defined KV pairs and running time  in same DB , and verify the actions of API.
 * @param     k --- key length. \n
 *            v --- value length. \n
 *            path --- DB name. \n
 *            flag --- create if missing. \n
 *            num --- the number of KV pairs. \n
 *            ti --- the stress running time in seconds.
 * @return    SUCCESS(0) --- The function execution successfully . \n
 *            other-value --- The function execute error.
 * @author    Lining Dou (Lining.dou@sumsung.com)
 * @version   v0.1
 * @remark    - 1.  Caller must ensure the data are valid.
 */
static int32_t Stress_1DB(int k, int v, string path, bool flag, int num, int ti) {
	srand((unsigned)time(0));
	double  start, end, seconds;
	start = clock();
	end = clock();
	DB* db = NULL;
	CreateOpenDB(&db, path, flag);
	ClearEnv(db);
	while(seconds <= ti) {
		UnitStress(db, k, v, num); 		
		end = clock();
		seconds  =(double)(end - start)/CLOCKS_PER_SEC;
	}
	ClearEnv(db);
	delete db;
	return SUCCESS;
}

/**
 * @brief     Try to Repeat Unit stress with defined KV pairs and running time in 2 DB , and verify the actions of API.
 * @param     k --- key length. \n
 *            v --- value length. \n
 *            path --- DB name. \n
 *            flag --- create if missing. \n
 *            num --- the number of KV pairs. \n
 *            ti --- the stress running time in seconds.
 * @return    SUCCESS(0) --- The function execution successfully . \n
 *            other-value --- The function execute error.
 * @author    Lining Dou (Lining.dou@sumsung.com)
 * @version   v0.1
 * @remark    - 1. Caller must ensure the data are valid.
 */
static int32_t Stress_2DB(int k, int v, string path, bool flag, int num, int ti) {
	srand((unsigned)time(0));
	double  start, end, seconds;
	start = clock();
	end = clock();
	DB* db_1 = NULL;
	DB* db_2 = NULL;
	CreateOpenDB(&db_1, path, flag);
	CreateOpenDB(&db_2, path, flag);
	while(seconds <= ti) {
		UnitStress(db_1, k, v, num);
		UnitStress(db_2, k, v, num);
		end = clock();
		seconds  =(double)(end - start)/CLOCKS_PER_SEC;
	}
	delete db_1;
	delete db_2;
	return SUCCESS;
}

TEST(ts_kv_Stress, Stress_1000KV_10s) {
	EXPECT_EQ(SUCCESS, Stress(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, TENSEC)) << "kvdb-Stress-1000KV-10s-1" << endl;
}

TEST(ts_kv_Stress, Stress_1000KV_100s) {
	EXPECT_EQ(SUCCESS, Stress(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, HUNDREDSEC)) << "kvdb-Stress-1000KV-100s-2" << endl;
}


TEST(ts_kv_Stress, Stress_10000KV_10s) {
	EXPECT_EQ(SUCCESS, Stress(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, TENSEC)) << "kvdb-Stress_10000KV_10s" << endl;
}

TEST(ts_kv_Stress, Stress_10000KV_100s) {
	EXPECT_EQ(SUCCESS, Stress(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, HUNDREDSEC)) << "kvdb-Stress_10000KV_100s" << endl;
}

TEST(ts_kv_Stress, Stress_1DB_1000KV_10s) {
	EXPECT_EQ(SUCCESS, Stress_1DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, TENSEC)) << "kvdb-Stress_1DB_1000KV_10s" << endl;
}

TEST(ts_kv_Stress, Stress_1DB_1000KV_100s) {
	EXPECT_EQ(SUCCESS, Stress_1DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, HUNDREDSEC)) << "kvdb-Stress_1DB_1000KV_100s" << endl;
}

TEST(ts_kv_Stress, Stress_1DB_10000KV_10s) {
	EXPECT_EQ(SUCCESS, Stress_1DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, TENSEC)) << "kvdb-Stress_1DB_10000KV_10s" << endl;
}

TEST(ts_kv_Stress, Stress_1DB_10000KV_100s) {
	EXPECT_EQ(SUCCESS, Stress_1DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, HUNDREDSEC)) << "kvdb-Stress_1DB_10000KV_100s" << endl;
}

TEST(ts_kv_Stress, Stress_1000KV_2DB_10s) {
	EXPECT_EQ(SUCCESS, Stress_2DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, TENSEC)) << "kvdb-Stress_1000KV_2DB_10s" << endl;
}

TEST(ts_kv_Stress, Stress_1000KV_2DB_100s) {
	EXPECT_EQ(SUCCESS, Stress_2DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, HUNDREDSEC)) << "kvdb-Stress_1000KV_2DB_100s" << endl;
}

TEST(ts_kv_Stress, Stress_10000KV_2DB_10s) {
	EXPECT_EQ(SUCCESS, Stress_2DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, TENSEC)) << "kvdb-Stress_10000KV_2DB_10s" << endl;
}

TEST(ts_kv_Stress, Stress_10000KV_2DB_100s) {
	EXPECT_EQ(SUCCESS, Stress_2DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, HUNDREDSEC)) << "kvdb-Stress_10000KV_2DB_100s" << endl;
}


TEST(ts_kv_Stress, Stress_1000KV_2H) {
	EXPECT_EQ(SUCCESS, Stress(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TWOKV, TOWHOUR)) << "kvdb-Stress_1000KV_2H" << endl;
}

TEST(ts_kv_Stress, Stress_10000KV_2H) {
	EXPECT_EQ(SUCCESS, Stress(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, TOWHOUR)) << "kvdb-Stress_10000KV_2H" << endl;
}

TEST(ts_kv_Stress, Stress_1DB_1000KV_2H) {
	EXPECT_EQ(SUCCESS, Stress_1DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, TOWHOUR)) << "kvdb-Stress_1DB_1000KV_2H" << endl;
}

TEST(ts_kv_Stress, Stress_1DB_10000KV_2H) {
	EXPECT_EQ(SUCCESS, Stress_1DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, TOWHOUR)) << "kvdb-Stress_1DB_10000KV_2H" << endl;
}

TEST(ts_kv_Stress, Stress_1000KV_2DB_2H) {
	EXPECT_EQ(SUCCESS, Stress_2DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, TOWHOUR)) << "kvdb-Stress_1000KV_2DB_2H" << endl;
}

TEST(ts_kv_Stress, Stress_10000KV_2DB_2H) {
	EXPECT_EQ(SUCCESS, Stress_2DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, TOWHOUR)) << "kvdb-Stress_10000KV_2DB_2H" << endl;
}

TEST(ts_kv_Stress, Stress_1000KV_12H) {
	EXPECT_EQ(SUCCESS, Stress(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, TWELVEHOUR)) << "kvdb-Stress_1000KV_12H" << endl;
}

TEST(ts_kv_Stress, Stress_10000KV_12H) {
	EXPECT_EQ(SUCCESS, Stress(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, TWELVEHOUR)) << "kvdb-Stress_10000KV_12H" << endl;
}

TEST(ts_kv_Stress, Stress_1DB_1000KV_12H) {
	EXPECT_EQ(SUCCESS, Stress_1DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, TWELVEHOUR)) << "kvdb-Stress_1DB_1000KV_12H" << endl;
}

TEST(ts_kv_Stress, Stress_1DB_10000KV_12H) {
	EXPECT_EQ(SUCCESS, Stress_1DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, TWELVEHOUR)) << "kvdb-Stress_1DB_10000KV_12H" << endl;
}

TEST(ts_kv_Stress, Stress_1000KV_2DB_12H) {
	EXPECT_EQ(SUCCESS, Stress_2DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, THOUSANDKV, TWELVEHOUR)) << "kvdb-Stress_1000KV_2DB_12H" << endl;
}

TEST(ts_kv_Stress, Stress_10000KV_2DB_12H) {
	EXPECT_EQ(SUCCESS, Stress_2DB(NORMALKEY, NORMALVALUE, DB_Path, CREATEIFMISSING, TENTHOUSDKV, TWELVEHOUR)) << "kvdb-Stress_10000KV_2DB_12H" << endl;
}

}

