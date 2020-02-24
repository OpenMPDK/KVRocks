/**
 * @ Dandan Wang (dandan.wang@samsung.com)
 */

/**
  * \addtogroup Doxygen Group
  *
  */

/*! \file XMLParser.cc
  *  \brief This file is to parse XML configure file and setup the configure parameters for KV SSD.
  */
///////////////////////////////////////////////////////////////////////////////////////
// Include files
///////////////////////////////////////////////////////////////////////////////////////

#include <stdlib.h>
#include <string.h>
#include <string>
#include <iostream>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include "Main.h"

#define XML_EQUAL(NODE, TARGET)     \
            xmlStrcmp((NODE)->name, (const xmlChar *)(TARGET))

using namespace std;

/**
* @brief     Try to parse XML configure file.
* @param     argc -- The configure file number. \n
             argv -- The configure information.
* @return    0 --- The function execute successfully. \n
*            -1 --- The function execution error.
* @author    Dandan Wang (dandan.wang@sumsung.com)
* @version   v0.1
* @remark    - 1. If there is no specific XML file or values, use default ones instead. \n
*/
/*INT32 XMLParser(PINT32 argc, CHAR **argv)
{
    g_ConfigDir = DEFAULT_CONFIG_DIR;
    g_MinKeyLen = KV_MIN_KEY_LEN;
    g_MaxKeyLen = KV_MAX_KEY_LEN;
    g_MinValueLen = KV_MIN_VALUE_LEN;
    g_MaxValueLen = KV_MAX_VALUE_LEN;
    g_MaxStoreValueLen = KV_MAX_STORE_VALUE_LEN;
    g_MaxTotalValueLen = KV_MAX_TOTAL_VALUE_LEN;

    xmlDocPtr     pdoc = NULL;
    xmlNodePtr    proot = NULL;
    xmlNodePtr    curNode = NULL;
    xmlNodePtr    subNode = NULL;

    for (INT32 n = 1; n < *argc; n++)
    {
        string tmpstr(argv[n]);
        if (!tmpstr.substr(0, 9).compare("--config="))
        {
            g_ConfigDir = tmpstr.substr(9, tmpstr.length());
            break;
        }
    }

    // check if cofigure file exists
    if (g_ConfigDir.length() == 0)
    {
        cout << "ERROR: There is no configure file. Please re-check." << endl;
    }
    /*if (access(g_ConfigDir.c_str(), R_OK) != 0)
    {
        cout << "ERROR: File " << g_ConfigDir << " does not exist" << endl;
        return -1;
    }*/

    // read xml file
    /*pdoc = xmlReadFile(g_ConfigDir.c_str(), NULL, XML_PARSE_NOBLANKS);
    if (pdoc == NULL)
    {
        return -1;
    }
    proot = xmlDocGetRootElement(pdoc);
    if (proot == NULL)
    {
        return -1;
    }

    // get root node
    if (xmlStrcmp(proot->name, BAD_CAST "root") != 0)
    {
        return -1;
    }

    // retrieve version number
    if (xmlHasProp(proot, BAD_CAST "version"))
    {
        xmlChar *szAttr = xmlGetProp(proot, BAD_CAST "version");
        cout << "version number: " << szAttr << endl;
    }

    curNode = proot->xmlChildrenNode;

    while (curNode != NULL)
    {
        if (!XML_EQUAL(curNode, "kv_log"))
        {
            subNode = curNode->xmlChildrenNode;
            while (subNode != NULL)
            {
                if (!XML_EQUAL(subNode, "log_dir"))
                {
                    g_LogDir = (char *)xmlNodeGetContent(subNode);
                }
                else if(!XML_EQUAL(subNode, "log_threshold"))
                {
                    g_LogLevel = atoi((char *)xmlNodeGetContent(subNode));
                }
                else if(!XML_EQUAL(subNode, "log_v_threshold"))
                {
                    g_VLogLevel = atoi((char *)xmlNodeGetContent(subNode));
                }
                subNode = subNode->next;
            }
        }
        else if (!XML_EQUAL(curNode, "kv_env"))
        {
            subNode = curNode->xmlChildrenNode;
            while (subNode != NULL)
            {
                if (!XML_EQUAL(subNode, "device_path"))
                {
                    g_DevPath = (char *)xmlNodeGetContent(subNode);
                }
                else if (!XML_EQUAL(subNode, "invalid_dev_path"))
                {
                    g_DevNonDev = (char *)xmlNodeGetContent(subNode);
                }
                else if (!XML_EQUAL(subNode, "nonexistent_dev_path"))
                {
                    g_DevNonExist = (char *)xmlNodeGetContent(subNode);
                }
                else if (!XML_EQUAL(subNode, "store_name"))
                {
                    g_StoreName = (char *)xmlNodeGetContent(subNode);
                }
                subNode = subNode->next;
            }
        }
        else if (!XML_EQUAL(curNode, "kv_config"))
        {
            subNode = curNode->xmlChildrenNode;
            while (subNode != NULL)
            {
                if (!XML_EQUAL(subNode, "min_key_size"))
                {
                    g_MinKeyLen =
                            (unsigned int)strtoul((char *)xmlNodeGetContent(subNode), NULL, 10);
                }
                else if (!XML_EQUAL(subNode, "max_key_size"))
                {
                    g_MaxKeyLen =
                            (unsigned int)strtoul((char *)xmlNodeGetContent(subNode), NULL, 10);
                }
                else if (!XML_EQUAL(subNode, "min_value_size"))
                {
                    g_MinValueLen =
                            (unsigned int)strtoul((char *)xmlNodeGetContent(subNode), NULL, 10);
                }
                else if (!XML_EQUAL(subNode, "max_value_size"))
                {
                    g_MaxValueLen =
                            (unsigned int)strtoul((char *)xmlNodeGetContent(subNode), NULL, 10);
                }
                else if (!XML_EQUAL(subNode, "max_store_value_size"))
                {
                    g_MaxStoreValueLen =
                            (unsigned int)strtoul((char *)xmlNodeGetContent(subNode), NULL, 10);
                }
                else if (!XML_EQUAL(subNode, "max_total_value_size"))
                {
                    g_MaxTotalValueLen =
                            (unsigned int)strtoul((char *)xmlNodeGetContent(subNode), NULL, 10);
                }
                subNode = subNode->next;
            }
        }
        curNode = curNode->next;
    }

    // print all configures to check
    cout << "=== Configure List ===" << endl;
    cout << "[log file directory]: " << g_LogDir << endl;
    cout << "[device path]: " << g_DevPath << endl;
    cout << "[store name]: " << g_StoreName << endl;
    cout << "[max key len]: " << g_MaxKeyLen << endl;
    cout << "[max value len]: " << g_MaxValueLen << endl;
    cout << "=== Starting Test ===" << endl;
    return 0;

}
*/
