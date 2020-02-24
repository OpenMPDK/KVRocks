/**
 * @ Aaron Chen (si.yu218@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file store_stress.cc
  *  \brief Stress test for kv_store API.
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
#include <string>
#include <sys/types.h>
#include <fcntl.h>

#include "gtest/gtest.h"
#include "kv_types.h"
#include "kv_apis.h"
#include "Common.h"
#include "CommonError.h"
#include "CommonType.h"
#include "Main.h"

using namespace std;

/*
 *  Global variables
 * */
static UINT64 _g_devHandle;                         // default device handle

/*
 *  Local constant definitions
 *  These constants is to define the default size of key, value length, which can simplify the testing.
 * */
const UINT16 TEST_KEY_LEN = 192;
const UINT32 TEST_VALUE_LEN = 192;
const UINT32 TEST_OFFSET = 64;

/** @class   ts_store_stress
 * @brief    - 1. Define Test Suite Set Up and Tear Down. \n
 *            - 2. Intialize the sharing resources between testcases in the same test suite.
 */
class ts_store_stress : public testing::Test
{
    protected:
    // set up test cases resources, called before the first test case in this test suite
    static void SetUpTestCase()
    {
        INT32 Res = KV_SUCCESS;
        _g_devHandle = 0;
        INT32 nr_device;
        UINT64 kvHandles[2];
        PUINT64 arr_handle = kvHandles;

        // init the KV SSD and KV cache
        char *json_path = "./kv_sdk_configuration.json";
        Res = kv_sdk_init(KV_SDK_INIT_FROM_JSON, json_path);
        if(Res != KV_ERR_SDK_OPEN)
        {
            return;
        }

        // get core id
        INT32 _coreID = kv_get_core_id();
        if(_coreID == -1)
        {
            return;
        }

        // get kv ssd handles
        Res = kv_get_devices_on_cpu(_coreID, &nr_device, arr_handle);
        if((Res != KV_SUCCESS) || (nr_device == 0))
        {
            _g_devHandle = 0;
            return;
        }
        _g_devHandle = arr_handle[0];
        return;
    }

    // Delete the sharing resources between test cases, called after the last test case in the test suite.
    static void TearDownTestCase()
    {
        INT32 Res = KV_SUCCESS;

        Res = kv_sdk_finalize();
        _g_devHandle = 0;
        return;
    }
};

/**
 * @brief      Verify the combination work flow: repeat update the same key N times
 *                  and verify the data of the key for every updating.
 * @param    void
 * @return    SUCCESS(0) --- The function execute successfully.
 *                  otherwise error code--- The function execution error.
 * @author    Aaron Chen (si.yu218@samsung.com)
 * @version   v0.1
 * @remark   - 1. test case no. : TestCase-strore_stress--001. \n
 *            - 2. we implement another function 'RepeatUpdateKV()' to handle the details steps.
 */
static INT32 RepeatStoreKV(void)
{
    INT32 Res = KV_SUCCESS;
    UINT32 nRepeatTimes = Key_Store_Times;
    UINT32 nCnt = 0;
    // Step1: Generate a kv pair and store it to KV SSD
    kv_pair pair = GenKVPair(TEST_KEY_LEN, TEST_VALUE_LEN, TEST_OFFSET, KV_STORE_DEFAULT);
    Res = kv_store(_g_devHandle, &pair);
    if(Res != KV_SUCCESS)
    {
        g_nAdiRes = Res;
        return Res;
    }

    // Step2: Repeat update the same key and verify
    g_nAdiRes = KV_SUCCESS;
    kv_pair new_pair;
    for (nCnt = 0; nCnt < nRepeatTimes; nCnt++)
    {
        // construct the update value for the same key
        new_pair = GenExistKVPair(pair.key, TEST_VALUE_LEN, TEST_OFFSET, KV_STORE_DEFAULT);

        // Store(Update) the key for new value
        Res = kv_store(_g_devHandle, &pair);
        if(Res != KV_SUCCESS)
        {
            g_nAdiRes = Res;
            Res = kv_delete(_g_devHandle, &new_pair);
            return g_nAdiRes;
        }
    }

    Res = kv_delete(_g_devHandle, &new_pair);
    if (Res != KV_SUCCESS)
    {
        return Res;
    }
    return SUCCESS;
}

/**
 * @brief      Verify the combination work flow: repeat delete and store the same key N times.
 * @param      void
 * @return     SUCCESS(0) --- The function execute successfully.
 *                  otherwise error code--- The function execution error.
 * @author    Dandan Wang (dandan.wang@samsung.com)
 * @version   v0.1
 * @remark   - 1. test case no. : TestCase-strore_stress--002. \n
 *            - 2. we implement another function 'RepeatUpdateKV()' to handle the details steps.
 */
static INT32 RepeatDelStoreKV(void)
{
    INT32 Res = KV_SUCCESS;
    UINT32 nRepeatTimes = Key_Store_Times;
    UINT32 nCnt = 0;
    
    // Step1: Generate a kv pair and store it to KV SSD
    kv_pair pair = GenKVPair(TEST_KEY_LEN, TEST_VALUE_LEN, TEST_OFFSET, KV_STORE_DEFAULT);
    kv_key pkey = pair.key;
    Res = kv_store(_g_devHandle, &pair);
    if(Res != KV_SUCCESS)
    {
        g_nAdiRes = Res;
        return Res;
    }

    // Step2: Repeat delete and store the same key and verify
    g_nAdiRes = KV_SUCCESS;
    for (nCnt = 0; nCnt < nRepeatTimes; nCnt++)
    {
        // delete the key 
        Res = kv_delete(_g_devHandle, &pair);
        if(Res != KV_SUCCESS)
        {
            g_nAdiRes = Res;
            return Res;
        }

        //Again store the same key
        pair = GenExistKVPair(pkey, TEST_VALUE_LEN, TEST_OFFSET, KV_STORE_DEFAULT);
        Res = kv_store(_g_devHandle, &pair);
        if(Res != KV_SUCCESS)
        {
            g_nAdiRes = Res;
            return Res;
        }
    }

    //Check the key is exist or not
    kv_key_list keyList = {pair.key, 1, TEST_KEY_LEN, KV_EXIST_DEFAULT};
    UINT8 pVal[TEST_VALUE_LEN];
    kv_value result = {(void *)pVal, TEST_VALUE_LEN, 0};

    Res = kv_exist(_g_devHandle, &keyList, &result);
    if (Res != KV_SUCCESS)
    {
        return Res;
    }
    
    //If the key not exist, it will return error
    if (result.value == 0)
    {
        return FAILURE;
    }
    return SUCCESS;
}

/**
 * @brief      Verify the combination work flow: repeat store and delete the same key N times.
 * @param      void
 * @return     SUCCESS(0) --- The function execute successfully.
 *                  otherwise error code--- The function execution error.
 * @author    Dandan Wang (dandan.wang@samsung.com)
 * @version   v0.1
 * @remark   - 1. test case no. : TestCase-strore_stress--003. \n
 *            - 2. we implement another function 'RepeatUpdateKV()' to handle the details steps.
 */
static INT32 RepeatStoreDelKV(void)
{
    INT32 Res = KV_SUCCESS;
    UINT32 nRepeatTimes = Key_Store_Times;
    UINT32 nCnt = 0;
    
    // Step1: Generate a kv pair and store it to KV SSD
    kv_pair pair = GenKVPair(TEST_KEY_LEN, TEST_VALUE_LEN, TEST_OFFSET, KV_STORE_DEFAULT);
    kv_key pkey = pair.key;
    Res = kv_store(_g_devHandle, &pair);
    if(Res != KV_SUCCESS)
    {
        g_nAdiRes = Res;
        return Res;
    }

    // Step2: Repeat delete and store the same key and verify
    g_nAdiRes = KV_SUCCESS;
    for (nCnt = 0; nCnt < nRepeatTimes; nCnt++)
    {
        // delete the key 
        Res = kv_delete(_g_devHandle, &pair);
        if(Res != KV_SUCCESS)
        {
            g_nAdiRes = Res;
            return Res;
        }

        //Again store the same key
        pair = GenExistKVPair(pkey, TEST_VALUE_LEN, TEST_OFFSET, KV_STORE_DEFAULT);
        Res = kv_store(_g_devHandle, &pair);
        if(Res != KV_SUCCESS)
        {
            g_nAdiRes = Res;
            return Res;
        }
    }

    // delete the key 
    Res = kv_delete(_g_devHandle, &pair);
    if(Res != KV_SUCCESS)
    {
        g_nAdiRes = Res;
        return Res;
    }

    //Check the key is exist or not
    kv_key_list keyList = {pair.key, 1, TEST_KEY_LEN, KV_EXIST_DEFAULT};
    UINT8 pVal[TEST_VALUE_LEN];
    kv_value result = {(void *)pVal, TEST_VALUE_LEN, 0};

    Res = kv_exist(_g_devHandle, &keyList, &result);
    if (Res != KV_SUCCESS)
    {
        return Res;
    }
    
    //If the key exist, it will return error
    if (result.value == 1)
    {
        return FAILURE;
    }
    return SUCCESS;
}


TEST_F(ts_store_stress, StoreStressInput)
{
    EXPECT_EQ(SUCCESS, RepeatStoreKV()) << "TestCase-strore_stress--001" << endl << g_nAdiRes;    
    EXPECT_EQ(SUCCESS, RepeatDelStoreKV()) << "TestCase-strore_stress--002" << endl << g_nAdiRes; 
    EXPECT_EQ(SUCCESS, RepeatStoreDelKV()) << "TestCase-strore_stress--003" << endl << g_nAdiRes;
}


