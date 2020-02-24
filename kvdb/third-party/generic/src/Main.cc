//
// Created by Aaron on 2018/1/12.
//
#include <string>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <glog/logging.h>
#include "gtest/gtest.h"
#include "CommonError.h"
#include "CommonType.h"
#include "Main.h"

using namespace std;


/*
 * Global variables
 * */
string g_DevPath;
string g_DevNonDev;
string g_DevNonExist;
string g_LogDir;
string g_ConfigDir;
INT32 g_LogLevel = google::ERROR;
INT32 g_VLogLevel = 1;
string g_StoreName;

UINT32 g_MinKeyLen;
UINT32 g_MaxKeyLen;
UINT32 g_MinValueLen;
UINT32 g_MaxValueLen;
UINT32 g_MaxStoreValueLen;
UINT32 g_MaxTotalValueLen;

/*
 * Extern functions declarations
 * */
extern INT32 XMLParser(PINT32 argc, CHAR **argv);	//< parse XML file via libxml2
extern INT32 SetLogFile(void);  ///< configure log path and properties

/*
 * Default configuration
 * */
extern const string DEFAULT_CONFIG_DIR = "/usr/local/kv_config.xml";
extern const string DEFAULT_LOG_DIR = "/tmp/kvlog"; ///< defaul kv store configure file
/*
 * main functions to start google test process
 * */
INT32 main(INT32 argc, CHAR **argv)
{
    INT32 Res = 0;
    //hainan.liu
    testing::GTEST_FLAG(output) = "xml:";
    google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    google::InstallFailureSignalHandler();

   // Res = XMLParser(&argc, argv);
    if (Res == -1)
    {
        return 1;
    }

    Res = SetLogFile();
    if (Res == -1)
    {
        return 1;
    }

    Res = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();

    return Res;
}
