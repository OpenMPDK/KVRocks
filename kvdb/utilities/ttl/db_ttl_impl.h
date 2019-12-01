// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/utility_db.h"
#include "rocksdb/utilities/db_ttl.h"
#include "db/db_impl.h"

#ifdef _WIN32
// Windows API macro interference
#undef GetCurrentTime
#endif


namespace rocksdb {

class DBWithTTLImpl : public DBWithTTL {
 public:
  explicit DBWithTTLImpl(DB* db);

  virtual ~DBWithTTLImpl();

  Status CreateColumnFamilyWithTtl(const ColumnFamilyOptions& options,
                                   const std::string& column_family_name,
                                   ColumnFamilyHandle** handle,
                                   int ttl) override;

  Status CreateColumnFamily(const ColumnFamilyOptions& options,
                            const std::string& column_family_name,
                            ColumnFamilyHandle** handle) override;


  DB* GetBaseDB() override { return db_; }

  Status SetTtl(int32_t ttl) { return SetTtl(DefaultColumnFamily(), ttl); }

  Status SetTtl(ColumnFamilyHandle *h, int32_t ttl) override;

  static const int32_t kMinTimestamp = 1368146402;  // 05/09/2013:5:40PM GMT-8

  static const int32_t kMaxTimestamp = 2147483647;  // 01/18/2038:7:14PM GMT-8
};
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
