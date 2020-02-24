/**
* @ Dandan Wang (dandan.wang@samsung.com)
*/

/**
* \addtogroup Doxygen Group
*
*/

/*! \file CommonType.h
*  \brief To typedef the primitive data types and keywords.
*/
#ifndef __COMMON_TYPE__
#define __COMMON_TYPE__

///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
#include <stdint.h>

/*
 * To define the DB path
 */
#define DB_Path   "/tmp/ddtest"  

/*
 * To define the legal values of variables of BOOL32 type
 */
#define TRUE32      (1)
#define FALSE32     (0)

/*
 * To define the return value
 */
#define SUCCESS                   0
#define FAILURE                     -1

/*
 * To define the option when create DB
 */
#define CREATEIFMISSING           1 
#define NOCREATEIFMISSING         0 

/*
 * To define the number of kv pair
 */
#define ZERO                      0
#define ONEKV                     1
#define TWOKV                     2 
#define THREEKV                   3 
#define HUNDREDKV                 100
#define THOUSANDKV                1000
#define THREETHOUSDKV             3000
#define TENTHOUSDKV               10000
#define HUNTHOUSDKV               100000

/*
 * To define the stress running time
 */
#define TENSEC                    10
#define HUNDREDSEC                100 
#define TOWHOUR                   2*3600
#define TWELVEHOUR                12*3600  
 
/*
 * To define the length of key and value
 */
#define NULLKEY                   0
#define NULLVALUE                 0
#define LESSKEY                   15
#define MINKEY                    16
#define NORMALKEY                 17
#define MAXKEY                    255
#define LONGKEY                   256
#define LESSVALUE                 63
#define MINVALUE                  64
#define NORMALVALUE               65
#define MAXVALUE                  2*1024*1024
#define LESSMAXVALUE              2*1024*1024-1
#define LONGVLAUE                 2*1024*1024+1

/*
 * Type defines
 */
/*
 * Character type definitions
 */
typedef char                      CHAR;
typedef char *                    PCHAR;
typedef const char *              CPCHAR;
typedef unsigned char             UCHAR;
typedef unsigned char *           PUCHAR;
typedef const unsigned char *     CPUCHAR;

/*
 * signed integer type definitions
 */
typedef int8_t                    INT8;
typedef int8_t *                  PINT8;
typedef int16_t                   INT16;
typedef int16_t *                 PINT16;
typedef int32_t                   INT32;
typedef int32_t *                 PINT32;
typedef int64_t                   INT64;
typedef int64_t *                 PINT64;

/*
 * unsigned integer type definitions
 */
typedef uint8_t                   UINT8;
typedef uint8_t *                 PUINT8;
typedef uint16_t                  UINT16;
typedef uint16_t *                PUINT16;
typedef uint32_t                  UINT32;
typedef uint32_t *                PUINT32;
typedef uint64_t                  UINT64;
typedef uint64_t *                PUINT64;

typedef struct {
    UINT64 low;         //low part
    UINT64 high;        //high part
}uint128_t;
/*
 * Bool type definitions
 */
typedef INT32                     BOOL32;

/*
 * address variables
 */
typedef void *                    PADDR;

#endif //end of #ifndef __COMMON_TYPE__
