/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file logFile.cc
  *  \brief This file is to make log for test suite.
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////
#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include "Main.h"

using namespace std;

 /**
 * @brief     Set log file.
 * @param     void
 * @return    0 --- The function execute successfully. \n
 *            -1 --- The function execution error.
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 * @remark    - 1. This is only used for set log dir. \n
 */
int SetLogFile(void)
{
    //google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = false; // NOT print on the screen in normal status
    FLAGS_stderrthreshold = g_LogLevel; // print on the screen if severify is over [g_VLogLevel]
    FLAGS_colorlogtostderr=true; // print in color
    FLAGS_alsologtostderr = false; // NOT print in log file and screen in the same time
    FLAGS_v = g_VLogLevel; // if customized printing, only print log level is over [g_VLogLevel]

    if (g_LogDir.length() != 0)
    {
        FLAGS_log_dir = g_LogDir;
    }
    else
    {
        FLAGS_log_dir = DEFAULT_LOG_DIR;
    }

    if (access(FLAGS_log_dir.c_str(), R_OK) != 0)
    {
        cout << "ERROR: There is no directory " << FLAGS_log_dir << endl;
        return -1;
    }
    return 0;
    //google::ShutdownGoogleLogging();
}

 /**
 * @brief     Set stderr threshold. (gdb) call SetLevel(1) ==> means print on screen above WARNING level
 * @param     severity -- The threshold of error level.
 * @return    void
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 * @remark    - 1. This is only used for debugging \n
 */
void SetLevel(int severity)
{
    FLAGS_stderrthreshold = severity;
}

 /**
 * @brief     Set customized threshold. (gdb) call SetVLevel(1) ==> means print on screen above WARNING level
 * @param     v_severity -- The threshold of customized level.
 * @return    void
 * @author    Dandan Wang (dandan.wang@sumsung.com)
 * @version   v0.1
 * @remark    - 1. This is only used for debugging \n
 */
void SetVLevel(int v_severity)
{
    FLAGS_v = v_severity;
}