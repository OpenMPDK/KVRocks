/*
 * Original Code Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of the original source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 *
 * Modifications made 2019
 * Modifications Copyright (c) 2019, Samsung Electronics.
 *
 * Architect    : Heekwon Park(heekwon.p@samsung.com), Yangseok Ki(yangseok.ki@samsung.com)
 * Authors      : Heekwon Park, Ilgu Hong, Hobin Lee
 *
 * This modified version is distributed under a BSD-style license that can be
 * found in the LICENSE.insdb file  
 *                    
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  
 */


#ifndef STORAGE_INSDB_DB_KEYNAME_H_
#define STORAGE_INSDB_DB_KEYNAME_H_

#include <iostream>
#include <iostream>
#include <stdint.h>
#include <string>
#include "util/hash.h"
#include "insdb/env.h"
namespace insdb {
#define DBNAME_SHIFT 32
#define METATYPE_SHIFT 16
#define INVALID_SKT_ID 0xffffffff
    typedef uint16_t metatype_t;
    enum InSDBMetaType{
        /* use 64bit seq num*/
        kKBlock = 0,
        kKBMeta = 1,
        /* use 32bit meta id */
        kLogRecord = 2,
        kSKTUpdateList = 3, 
        kSKTIterUpdateList = 4,
        kManifest = 5,
        //sktable
        kSuperSKTable = 6,
        kSKTable = 7,
        kHashMap = 8,
        kColLog = 9,
        kRecoverLog = 10,
        kMaxMetaType = 10,
    };
    /**
      return Metadata key.
      ManiFest      		key = dbname | kManifest     		| INVALID_SKT_ID
      Super SKTable 		key = dbname | kSuperSKTable 		| SKTable ID
      SKTable       		key = dbname | kSKTable      		| SKTable ID
      kHashMap      		key = dbname | kSKTable      		| SKTable ID
      SKTUpdateList    		key = dbname | kSKTUpdateList		| SKTable ID
      kSKTIterUpdateList 	key = dbname | kSKTIterUpdateList	| SKTable ID
      LogRecord     		key = dbname | kLogRecord    		| INVALID_SKT_ID
      */
    inline InSDBKey GetMetaKey(const uint32_t dbname, enum InSDBMetaType name_type, const uint32_t skt_id = INVALID_SKT_ID) {
        InSDBKey key = {0,0};
        key.db_id = dbname;
        key.type = name_type;
        key.split_seq = 0;
        key.meta_id = skt_id;
        key.meta_info = 0;
        return key;
    }

    inline InSDBKey GetColInfoKey(const uint32_t dbname, const uint32_t skt_id, const uint32_t colinfo_id) {
        InSDBKey key = {0,0};
        key.db_id = dbname;
        key.type = kColLog;
        key.split_seq = 0;
        key.meta_id = skt_id;
        key.meta_info = colinfo_id;
        return key;
    }


    /**
      return KB or its Metadata key.
      kKBlock   key = dbname | kKBlock | seq
      kKBMeta   key = dbname | kKBMeta | seq
      */
    inline InSDBKey GetKBKey(const uint32_t dbname, enum InSDBMetaType name_type, const uint64_t seq) {
        InSDBKey key = {0,0};
        assert(name_type == kKBlock || name_type == kKBMeta);
        key.db_id = dbname;
        key.type = name_type;
        key.split_seq = 0;
        key.seq = seq;
        return key;
    }

    inline uint32_t GetSKTableID(const InSDBKey skt_key) {
        if (skt_key.type != kSKTable) {
            std::cout << "This key is not a sktable key(key type & value: " << skt_key.type << " & " << skt_key.meta_id << ")\n";
            return INVALID_SKT_ID;/* This line may not necessary because other types of manadata already have INVALID_SKT_ID */
        }
        return skt_key.meta_id;
    }
}  // namespace insdb

#endif  // STORAGE_INSDB_DB_KEYNAME_H_
