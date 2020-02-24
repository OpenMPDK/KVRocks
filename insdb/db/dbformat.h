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


#ifndef STORAGE_INSDB_DB_DBFORMAT_H_
#define STORAGE_INSDB_DB_DBFORMAT_H_

#include <stdio.h>
#include <limits.h>
#include "insdb/db.h"
#include "insdb/filter_policy.h"
#include "insdb/slice.h"
#include "util/coding.h"
#include "util/logging.h"
namespace insdb {

    // Grouping of constants.  We may want to make some of these
    // parameters set via options.
    namespace config {
        static const int kNumLevels = 7;

        // Level-0 compaction is started when we hit this many files.
        static const int kL0_CompactionTrigger = 4;

        // Soft limit on number of level-0 files.  We slow down writes at this point.
        static const int kL0_SlowdownWritesTrigger = 8;

        // Maximum number of level-0 files.  We stop writes at this point.
        static const int kL0_StopWritesTrigger = 12;

        // Maximum level to which a new compacted memtable is pushed if it
        // does not create overlap.  We try to push to level 2 to avoid the
        // relatively expensive level 0=>1 compactions and to avoid some
        // expensive manifest file operations.  We do not push all the way to
        // the largest level since that can generate a lot of wasted disk
        // space if the same key space is being repeatedly overwritten.
        static const int kMaxMemCompactLevel = 2;

        // Approximate gap in bytes between samples of data read during iteration.
        static const int kReadBytesPeriod = 1048576;

    }  // namespace config

    static const int kDefaultIKeySize = 16 ;      /** 16 B */

    // Value types encoded as the last component of internal keys.
    // DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
    // data structures.
    enum ValueType {
        kTypeDeletion = 0x0,
        kTypeValue = 0x1,
        kTypeMerge = 0x2,
    };
    // kValueTypeForSeek defines the ValueType that should be passed when
    // constructing a ParsedInternalKey object for seeking to a particular
    // sequence number (since we sort sequence numbers in decreasing order
    // and the value type is embedded as the low 8 bits in the sequence
    // number in internal keys, we need to use the highest-numbered
    // ValueType, not the lowest).
    static const ValueType kValueTypeForSeek = kTypeValue;

    typedef uint64_t SequenceNumber;

    static const SequenceNumber kMaxSequenceNumber = ULONG_MAX;
    // Max request size
    static const int kDeviceRquestMaxSize= 0x200000; // 2MB
    // Max User Thread Count
    static const int kMaxUserCount= 1024;
    // Max  Worker Count
    static const int kMaxWriteWorkerCount= 64;
    // Max Column Family count
    static const int kUnusedNextInternalKey = 1088; //kMaxUserCount + kMaxWriteWorkerCount
    // Minimun SKTable Size 
    static const int kMinSKTableSize= 4*1024;
    // Max SKTable Size 
    extern int kMaxSKTableSize;
    // The max number of request per Key Block
    // The value must be 64 or less.
    // Ues less count for large Value(ex: "4" for 4KB value (4*4KB = kMaxInternalValueSize)
    static const uint32_t kMaxRequestPerKeyBlock = 64;
    // Max Internal Value(Key Block) Size
    static const uint32_t kMaxInternalValueSize = 0x4000; // 16KB
    // Max Column Family count
    extern int kMaxColumnCount;
    // Write Worker count
    extern int kWriteWorkerCount;
    // Max request size
    extern uint32_t kMaxRequestSize;
    extern int kRequestAlignSize;

    extern const uint16_t kMaxAllocatorPool;
    extern int64_t kMaxCacheSize;
    extern int64_t kCacheSizeLowWatermark;

    extern uint32_t kMinUpdateCntForTableFlush;
    extern uint32_t kSKTableFlushWatermark;
    extern uint32_t  kFlushCheckKeyCount;
    // Max sktables cached in device format
    // KeyPrefixSize
    extern int kPrefixIdentifierSize;
    // Share IterPad
    extern bool kIterPadShare;
    // Keep Written Block
    extern bool kKeepWrittenKeyBlock;
}  // namespace insdb

#endif  // STORAGE_INSDB_DB_DBFORMAT_H_
