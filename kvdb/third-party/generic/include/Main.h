/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file Main.h
  *  \brief Declare the global variables and define miscellaneous global constants.
*/
#ifndef __MAIN__
#define __MAIN__

///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
#include <string>
#include "CommonType.h"
//#include "kv_types.h"

using namespace std;

/*
 * Constans
 */
/*
 * The fixed parameters value of KV store
 */
/*#define KV_MIN_KEY_LEN 1                                            // minimum key size (The Samsung PM983 minimum key size (kv_device.min_key_len) is 16 bytes.)
#define KV_MIN_VALUE_LEN 1                                          // minimum value size (The Samsung PM983 minimum value size (kv_device.min_value_len) is 64 bytes.)
#define KV_MAX_KEY_LEN 255                                          // maximum key size (The maximum Samsung PM983 key size (kv_device.max_key_len) is 255 bytes.)
#define KV_MAX_VALUE_LEN (2*1024*1024)                              // maximum value size (2MB) (The maximum Samsung PM983 value size (kv_device.max_value_len) is 2MB.)
#define KV_NAMESPACE_ALL (uint32_t)(-1)                             // all attached namespaces
#define KV_NAMESPACE_DEFAULT (kv_namespace_handle)(1)               // default namespace
*/
/*
 * External variable declarations
 * */
extern string g_DevPath;
extern string g_DevNonDev;
extern string g_DevNonExist;
extern string g_LogDir;
extern string g_ConfigDir;
extern INT32 g_LogLevel;
extern INT32 g_VLogLevel;
extern string g_StoreName;

extern UINT32 g_MinKeyLen;
extern UINT32 g_MaxKeyLen;
extern UINT32 g_MinValueLen;
extern UINT32 g_MaxValueLen;
extern UINT32 g_MaxStoreValueLen;
extern UINT32 g_MaxTotalValueLen;

extern const string DEFAULT_CONFIG_DIR;
extern const string DEFAULT_LOG_DIR;

/*
 * Function declarations
 * */


#endif //__MAIN__
