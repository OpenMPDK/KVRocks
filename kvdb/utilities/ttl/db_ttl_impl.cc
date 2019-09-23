// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "utilities/ttl/db_ttl_impl.h"

#include "db/write_batch_internal.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/coding.h"
#include "util/filename.h"

namespace rocksdb {

// Open the db inside DBWithTTLImpl because options needs pointer to its ttl
DBWithTTLImpl::DBWithTTLImpl(DB* db) : DBWithTTL(db) {}

DBWithTTLImpl::~DBWithTTLImpl() {
  // Need to stop background compaction before getting rid of the filter
  CancelAllBackgroundWork(db_, /* wait = */ true);
  delete GetOptions().compaction_filter;
}

Status UtilityDB::OpenTtlDB(const Options& options, const std::string& dbname,
                            StackableDB** dbptr, int32_t ttl, bool read_only) {
  DBWithTTL* db;
  Status s = DBWithTTL::Open(options, dbname, &db, ttl, read_only);
  if (s.ok()) {
    *dbptr = db;
  } else {
    *dbptr = nullptr;
  }
  return s;
}

Status DBWithTTL::Open(const Options& options, const std::string& dbname,
                       DBWithTTL** dbptr, int32_t ttl, bool read_only) {

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DBWithTTL::Open(db_options, dbname, column_families, &handles,
                             dbptr, {ttl}, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
    handles[0] = NULL;
  }
  return s;
}

Status DBWithTTL::Open(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DBWithTTL** dbptr,
    std::vector<int32_t> ttls, bool read_only) {

  if (ttls.size() != column_families.size()) {
    return Status::InvalidArgument(
        "ttls size has to be the same as number of column families");
  }

  std::vector<ColumnFamilyDescriptor> column_families_sanitized =
      column_families;
  for (size_t i = 0; i < column_families_sanitized.size(); ++i) {
    column_families_sanitized[i].ttl_enabled = true;
    column_families_sanitized[i].ttl = (ttls[i] < 0 ? 0 : ttls[i]);
  }
  DB* db;

  Status s;
  if (read_only) {
//    st = DB::OpenForReadOnly(db_options, dbname, column_families_sanitized,
//                             handles, &db);
    return Status::NotSupported("Not supported API OpenForReadOnly.");
  } else {
    s = DB::Open(db_options, dbname, column_families_sanitized, handles, &db);
  }
  if (s.ok()) {
    *dbptr = new DBWithTTLImpl(db);
  } else {
    *dbptr = nullptr;
  }
  return s;
}

Status DBWithTTLImpl::CreateColumnFamilyWithTtl(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    ColumnFamilyHandle** handle, int ttl) {
  ColumnFamilyOptions sanitized_options = options;
//  DBWithTTLImpl::SanitizeOptions(ttl, &sanitized_options, GetEnv());

  Status s = DBWithTTL::CreateColumnFamily(sanitized_options, column_family_name, handle);
  if (!s.ok())
    return s;

  return SetTtl(*handle, ttl);
}

Status DBWithTTLImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                         const std::string& column_family_name,
                                         ColumnFamilyHandle** handle) {
  return CreateColumnFamilyWithTtl(options, column_family_name, handle, 0);
}

Status DBWithTTLImpl::SetTtl(ColumnFamilyHandle *h, int32_t ttl) {
  return DBWithTTL::SetColumnFamilyTtl(h, ttl < 0 ? 0 : ttl);
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
