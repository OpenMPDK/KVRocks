/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file append_stress.cc
  *  \brief Stress test for kv_append API.
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

/** @class   ts_append_stress
 * @brief    - 1. Define Test Suite Set Up and Tear Down. \n
 *            - 2. Intialize the sharing resources between testcases in the same test suite.
 */
class ts_append_stress : public testing::Test
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
 * @brief      Verify the combination work flow: repeat append the value of same key N times until
 *             the value length reach the max_val_len, and then delete the key.
 * @param      void
 * @return     SUCCESS(0) --- The function execute successfully.
 *             otherwise error code--- The function execution error.
 * @author     Dandan Wang (dandan.wang@samsung.com)
 * @version    v0.1
 * @remark    - 1. test case no. : TestCase-append_stress--001. \n
 */
static INT32 RepeatAppendKV(void)
{
    INT32 Res = KV_SUCCESS;
    UINT32 nRepeatTimes = 11184810;  //The max_value_len / TEST_VALUE_LEN
    UINT32 nCnt = 0;

    // Step1: Generate a kv pair and store it to KV SSD
    kv_pair pair = GenKVPair(TEST_KEY_LEN, TEST_VALUE_LEN, TEST_OFFSET, KV_STORE_DEFAULT);
    Res = kv_store(_g_devHandle, &pair);
    if(Res != KV_SUCCESS)
    {
        g_nAdiRes = Res;
        return Res;
    }

    // Step2: Repeat append the value to the max_value_len  
    for (nCnt = 0; nCnt < nRepeatTimes; nCnt++)
    {
        Res = kv_append(_g_devHandle, &pair);
        if(Res != KV_SUCCESS)
        {
            g_nAdiRes = Res;
            return Res;
        }
    }
    
    //Delete the key
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
        g_nAdiRes = Res;
        return Res;
    }
    
    //If the key exist, it will return error
    if (result.value == 1)
    {
        return FAILURE;
    }
    return SUCCESS;
}

