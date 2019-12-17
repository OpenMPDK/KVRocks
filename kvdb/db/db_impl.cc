//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdint.h>

#ifdef ROCKSDB_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

#include <algorithm>
#include <climits>
#include <cstdio>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <stdexcept>

#include "insdb/comparator.h"
#include "insdb/merge_operator.h"
#include "insdb/env.h"
#include "insdb/write_batch.h"

#include "rocksdb/comparator.h"
#include "rocksdb/merge_operator.h"

#include "db/db_impl.h"
#include "db/db_iter.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "table/block_based_table_factory.h"
#include "util/coding.h"
#include "util/sync_point.h"
#include "util/filename.h"

// Implementations of the DB interface
namespace rocksdb {


const std::string kDefaultColumnFamilyName("default");
const int kInSDB_MaxNrColumnFamilies = 2;
//Fix bug：max number of column familiy can not be configured
//Add max supported number
const int kInSDB_MaxSupportedColumnFamilies = 256;

// DB class default

// destructor
DB::~DB() {
}

Status DB::SetColumnFamilyTtl(ColumnFamilyHandle* column_family, uint32_t ttl){
  return Status::NotSupported("DB::SetColumnFamilyTtl");
}

Status DB::CreateColumnFamily(const ColumnFamilyOptions& options,
                                  const std::string& column_family_name,
                                  ColumnFamilyHandle** handle) {
    return Status::NotSupported("DB::CreateColumnFamily");
}

Status DB::DropColumnFamily(ColumnFamilyHandle* column_family) {
    return Status::NotSupported("DB::DropColumnFamily");
}

Status DB::CreateColumnFamilies(
    const ColumnFamilyOptions& /*cf_options*/,
    const std::vector<std::string>& /*column_family_names*/,
    std::vector<ColumnFamilyHandle*>* /*handles*/) {
  return Status::NotSupported("DB::CreateColumnFamilies ColumnFamilyOptions");
}

// Original RocksDB does not support neither.
Status DB::CreateColumnFamilies(
    const std::vector<ColumnFamilyDescriptor>& /*column_families*/,
    std::vector<ColumnFamilyHandle*>* /*handles*/) {
  return Status::NotSupported("DB::CreateColumnFamilies ColumnFamilyDescriptor");
}

Status DB::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& column_families) {
    return Status::NotSupported("DB::DropColumnFamilies");
}

Status DB::DestroyColumnFamilyHandle(ColumnFamilyHandle* column_family) {
    // Nothing to clean up.
    // The handle will be freed when it's removed from the handle map.
    return Status::OK();
}

namespace {
void DeleteOptionsFilesHelper(const std::map<uint64_t, std::string>& filenames,
                              const size_t num_files_to_keep,
                              const std::shared_ptr<Logger>& info_log,
                              Env* env) {
  if (filenames.size() <= num_files_to_keep) {
    return;
  }
  for (auto iter = std::next(filenames.begin(), num_files_to_keep);
       iter != filenames.end(); ++iter) {
    if (!env->DeleteFile(iter->second).ok()) {
      ROCKS_LOG_WARN(info_log, "Unable to delete options file %s",
                     iter->second.c_str());
    }
  }
}
}  // namespace

Status DBImpl::DeleteObsoleteOptionsFiles() {
  std::vector<std::string> filenames;
  // use ordered map to store keep the filenames sorted from the newest
  // to the oldest.
  std::map<uint64_t, std::string> options_filenames;
  Status s;
  s = GetEnv()->GetChildren(GetName(), &filenames);
  if (!s.ok()) {
    return s;
  }
  for (auto& filename : filenames) {
    uint64_t file_number;
    FileType type;
    if (ParseFileName(filename, &file_number, &type) && type == kOptionsFile) {
      options_filenames.insert(
          {std::numeric_limits<uint64_t>::max() - file_number,
           GetName() + "/" + filename});
    }
  }

  // Keeps the latest 2 Options file
  const size_t kNumOptionsFilesKept = 2;
  DeleteOptionsFilesHelper(options_filenames, kNumOptionsFilesKept,
                           immutable_db_options_.info_log, GetEnv());
  return Status::OK();
}

Status DBImpl::RenameTempFileToOptionsFile(const std::string& file_name) {
  Status s;

  options_file_number_ = NewFileNumber();
  std::string options_file_name =
      OptionsFileName(GetName(), options_file_number_);
  // Retry if the file name happen to conflict with an existing one.
  s = GetEnv()->RenameFile(file_name, options_file_name);

  DeleteObsoleteOptionsFiles();
  return s;
}

Status DBImpl::WriteOptionsFile(bool need_mutex_lock) {
  if (need_mutex_lock) {
    mutex_.Lock();
  } else {
    mutex_.AssertHeld();
  }

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;

  // This part requires mutex to protect the column family options
  for (auto cfpair : column_families_) {
      if(kDefaultColumnFamilyName.compare(cfpair.second->GetName()) == 0)
      {
          cf_names.insert(cf_names.begin(), cfpair.second->GetName());
          cf_opts.insert(cf_opts.begin(), cfpair.second->GetLatestCFOptions());
      }
      else
      {
          cf_names.push_back(cfpair.second->GetName());
          cf_opts.push_back(cfpair.second->GetLatestCFOptions());
      }
  }

  // Unlock during expensive operations.  New writes cannot get here
  // because the single write thread ensures all new writes get queued.
  DBOptions db_options =
      BuildDBOptions(immutable_db_options_, mutable_db_options_);
  mutex_.Unlock();

  TEST_SYNC_POINT("DBImpl::WriteOptionsFile:1");
  TEST_SYNC_POINT("DBImpl::WriteOptionsFile:2");

  std::string file_name =
      TempOptionsFileName(GetName(), NewFileNumber());
  Status s =
      PersistRocksDBOptions(db_options, cf_names, cf_opts, file_name, GetEnv());

  if (s.ok()) {
    s = RenameTempFileToOptionsFile(file_name);
  }
  // restore lock
  if (!need_mutex_lock) {
    mutex_.Lock();
  }

  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Unnable to persist options -- %s", s.ToString().c_str());
    if (immutable_db_options_.fail_if_options_file_error) {
      return Status::IOError("Unable to persist options.",
                             s.ToString().c_str());
    }
  }

  return Status::OK();
}

// KVDB metadata names
#define KvdbMetaKeyPrefix "kvdbmeta_"
const char DBImpl::kKvdbMeta_Manifest[] = KvdbMetaKeyPrefix "manifest";
const char DBImpl::kKvdbMeta_Statistics[] = KvdbMetaKeyPrefix "statistics";

// KVDB manifest signature
const char DBImpl::kKvdbMeta_Manifest_Signature[]  = {'K','V','D','B','M','A','N','I'};


Status WriteVariableString(WritableFile *file, char * buf, std::string& str)
{
    Slice readData;

    // string length: 4bytes
    EncodeFixed32(buf, str.size());
    Status status = file->Append(Slice(buf, 4));
    if (!status.ok())
    {
        return status;
    }

    // string: variable
    status = file->Append(Slice(str));
    if (!status.ok())
    {
        return status;
    }

    return Status::OK();
}

Status DBImpl::WriteManifest(Manifest& manifest)
{

    if (manifest.cf_count != manifest.column_families.size())
        return Status::InvalidArgument("mismatching column family count");

    std::unique_ptr<char[]> buf(new char[1024]);

    unique_ptr<WritableFile> CFMetaFilePtr;
    Logger *logger = immutable_db_options_.info_log.get();

    Status status = env_->CreateDirIfMissing(dbname_);
    if(!status.ok())
    {
        Log(logger, "failed to create DB directory %s", dbname_.c_str());
        return status;
    }

    EnvOptions env_options;
    status = env_->NewWritableFile(metadata_manifest_path, &CFMetaFilePtr, env_options);
    if (!status.ok())
    {
        Log(logger, "failed to create new meta %s. %s", metadata_manifest_path.c_str(), status.ToString().c_str());
        return status;
    }

    WritableFile *CFMetaFile = CFMetaFilePtr.get();

    // KVDB meta signature : 8bytes
    status = CFMetaFile->Append(Slice(kKvdbMeta_Manifest_Signature, sizeof(kKvdbMeta_Manifest_Signature)));
    if (!status.ok())
    {
        Log(logger, "could not write signature %s", metadata_manifest_path.c_str());
        return status;
    }
    // manifest version : 1 byte + 3 byte padding
    buf.get()[0] = manifest.manifest_version;
    buf.get()[1] = buf.get()[2] = buf.get()[3] = 0;

    status = CFMetaFile->Append(Slice(buf.get(), 4));
    if (!status.ok())
    {
        Log(logger, "could not write manifest version %u", manifest.manifest_version);
        return status;
    }

    Log(logger, "Write next file number %lu in %s", manifest.next_file_number, metadata_manifest_path.c_str());
    EncodeFixed64(buf.get(), manifest.next_file_number);

    // next file number: 8 bytes
    status = CFMetaFile->Append(Slice(buf.get(), 8));
    if (!status.ok())
    {
        Log(logger, "could not write next file number %s", metadata_manifest_path.c_str());
        return status;
    }

    Log(logger, "Write CF count %u in %s", manifest.cf_count, metadata_manifest_path.c_str());
    EncodeFixed32(buf.get(), manifest.cf_count);

    // Number of column family: 4bytes
    status = CFMetaFile->Append(Slice(buf.get(), 4));
    if (!status.ok())
    {
        Log(logger, "could not write CF count %s", metadata_manifest_path.c_str());
        return status;
    }

    mutex_.Lock();
    for (auto CFMetaPair : manifest.column_families)
    {
        // column family ID: 4bytes
        Log(logger, "CF ID %u in %s", CFMetaPair.second.ID, metadata_manifest_path.c_str());
        EncodeFixed32(buf.get(), CFMetaPair.second.ID);
        status = CFMetaFile->Append(Slice(buf.get(), 4));
        if (!status.ok())
        {
            mutex_.Unlock();
            Log(logger, "could not write CFID %s", metadata_manifest_path.c_str());
            return status;
        }

        //column family ttl: 4bytes
        Log(logger, "CF ttl %u in %s", CFMetaPair.second.ttl, metadata_manifest_path.c_str());
        EncodeFixed32(buf.get(), CFMetaPair.second.ttl);
        status = CFMetaFile->Append(Slice(buf.get(), 4));
        if (!status.ok())
        {
          mutex_.Unlock();
          Log(logger, "could not write CF ttl %s", metadata_manifest_path.c_str());
          return status;
        }

        // column family name
        Log(logger, "CF name %.*s (%lu) in %s", static_cast<int>(CFMetaPair.second.name.size()), CFMetaPair.second.name.c_str(), CFMetaPair.second.name.size(), metadata_manifest_path.c_str());
        status = WriteVariableString(CFMetaFile, buf.get(), CFMetaPair.second.name);
        if (!status.ok())
        {
            mutex_.Unlock();
            Log(logger, "could not write CF name %s", metadata_manifest_path.c_str());
            return status;
        }

        // column comparator name
        Log(logger, "CF comparator name %.*s in %s", static_cast<int>(CFMetaPair.second.comparator.size()), CFMetaPair.second.comparator.c_str(), metadata_manifest_path.c_str());
        status = WriteVariableString(CFMetaFile, buf.get(), CFMetaPair.second.comparator);
        if (!status.ok())
        {
            mutex_.Unlock();
            Log(logger, "could not write CF comparator %s", metadata_manifest_path.c_str());
            return status;
        }
    }
    mutex_.Unlock();

    CFMetaFile->Flush();
    CFMetaFile->Fsync();
    CFMetaFile->Close();
    return status;
}

Status ReadVariableString(SequentialFile *file, char * buf, std::string& str)
{
    Slice readData;

    // string length: 4bytes
    Status status = file->Read(4, &readData, buf);
    if (!status.ok())
    {
        return status;
    }
    uint32_t strLength = 0;
    bool success = GetFixed32(&readData, &strLength);
    assert(success);

    // string: variable
    char *strbuf = new char[strLength];
    Slice slicebuf;
    status = file->Read(strLength, &slicebuf, strbuf);
    if (!status.ok())
    {
        delete strbuf;
        return status;
    }

    str.assign(slicebuf.data(), slicebuf.size());
    delete [] strbuf;
    return Status::OK();
}


Status DBImpl::ReadManifest(Manifest& manifest)
{
    std::unique_ptr<char[]> buf(new char[1024]);

    unique_ptr<SequentialFile> CFMetaFilePtr;
    Logger *logger = immutable_db_options_.info_log.get();
    EnvOptions env_options;
    Status status = env_->NewSequentialFile(metadata_manifest_path, &CFMetaFilePtr, env_options);
    if (!status.ok())
    {
        Log(logger, "No kvdb meta file found %s errno %s(%d)", metadata_manifest_path.c_str(), strerror(errno), errno);
        return status;
    }

    SequentialFile *CFMetaFile = CFMetaFilePtr.get();
    Slice readData;

    // KVDB manifest signature : 8bytes
    status = CFMetaFile->Read(8, &readData, buf.get());
    if (!status.ok())
    {
        Log(logger, "could not read signature %s", metadata_manifest_path.c_str());
        return status;
    }
    if (readData.compare(Slice(kKvdbMeta_Manifest_Signature, 8)) != 0)
    {
        Log(logger, "Kvdb meta signature mismatch %s", metadata_manifest_path.c_str());
        return Status::Incomplete();
    }

    // KVDB manifest version : 1bytes
    status = CFMetaFile->Read(1, &readData, buf.get());
    if (!status.ok())
    {
        Log(logger, "could not read version %s", metadata_manifest_path.c_str());
        return status;
    }
    manifest.manifest_version = readData.data()[0];
    Log(logger, "manifest_version %u in %s", static_cast<unsigned>(manifest.manifest_version), metadata_manifest_path.c_str());
    if(manifest.manifest_version != kKvdbMeta_ManifestVersion)
    {
        return Status::InvalidArgument("mismatching manifest version");
    }

    status = CFMetaFile->Skip(3);
    if (!status.ok())
    {
        Log(logger, "could not skip %s", metadata_manifest_path.c_str());
        return status;
    }

    // KVDB next file number: 8 bytes
    status = CFMetaFile->Read(8, &readData, buf.get());
    if (!status.ok())
    {
        Log(logger, "could not read next file number %s", metadata_manifest_path.c_str());
        return status;
    }
#ifndef NDEBUG
    bool success =
#endif
         GetFixed64(&readData, &manifest.next_file_number);
    assert(success);
    Log(logger, "next file number %lu in %s", manifest.next_file_number, metadata_manifest_path.c_str());

    // Number of column family: 4bytes
    status = CFMetaFile->Read(4, &readData, buf.get());
    if (!status.ok())
    {
        Log(logger, "could not read CF count %s", metadata_manifest_path.c_str());
        return status;
    }
#ifndef NDEBUG
    success =
#endif
        GetFixed32(&readData, &manifest.cf_count);
    assert(success);
    Log(logger, "Read CF count %u in %s", manifest.cf_count, metadata_manifest_path.c_str());

    for (uint32_t i = 0; i < manifest.cf_count; i++)
    {
        Manifest_ColumnFamily CFMeta;
        uint32_t ttl = 0;

        // column family ID: 4bytes
        status = CFMetaFile->Read(4, &readData, buf.get());
        if (!status.ok())
        {
            Log(logger, "could not read CFID %s", metadata_manifest_path.c_str());
            return status;
        }
#ifndef NDEBUG
        success =
#endif
            GetFixed32(&readData, &CFMeta.ID);
        assert(success);
        Log(logger, "CF ID %u in %s", CFMeta.ID, metadata_manifest_path.c_str());

        // column family ttl: 4bytes
        status = CFMetaFile->Read(4, &readData, buf.get());
        if (!status.ok())
        {
          Log(logger, "could not read CFID %s", metadata_manifest_path.c_str());
          return status;
        }
#ifndef NDEBUG
        success =
#endif
        GetFixed32(&readData, &ttl);
        assert(success);
        CFMeta.ttl = ttl;
        Log(logger, "CF ttl %d in %s", CFMeta.ttl, metadata_manifest_path.c_str());

        // column family name
        status = ReadVariableString(CFMetaFile, buf.get(), CFMeta.name);
        if (!status.ok())
        {
            Log(logger, "could not read CF name %s", metadata_manifest_path.c_str());
            return status;
        }
        Log(logger, "CF name %.*s in %s", static_cast<int>(CFMeta.name.size()), CFMeta.name.data(), metadata_manifest_path.c_str());

        // column comparator name
        status = ReadVariableString(CFMetaFile, buf.get(), CFMeta.comparator);
        if (!status.ok())
        {
            Log(logger, "could not read CF comparator %s", metadata_manifest_path.c_str());
            return status;
        }
        Log(logger, "CF comparator name %.*s in %s", static_cast<int>(CFMeta.comparator.size()), CFMeta.comparator.data(), metadata_manifest_path.c_str());

        // push new column family metadata
        manifest.column_families.insert({std::string(CFMeta.name.data(), CFMeta.name.size()), CFMeta});
    }

    return status;
}


class InsDB_Logger : public insdb::Logger {
public:
    InsDB_Logger(const std::shared_ptr<rocksdb::Logger> &logger) : logger_(logger) { }
    virtual void Logv(const char* format, va_list ap){
        logger_->Logv(format, ap);
    }
    rocksdb::Logger *GetLogger() { return logger_.get(); }

private:
    std::shared_ptr<rocksdb::Logger> logger_;
};

Status DB::ListColumnFamilies(const DBOptions& db_options,
                                   const std::string& name,
                                   std::vector<std::string>* column_families)
{
    DBImpl::Manifest manifest;
    DBImpl *impl = new DBImpl(db_options, name);
    Status s = impl->ReadManifest(manifest);
    delete impl;
    if (!s.ok())
    {
        if (s.IsIOError() && errno == ENOENT) {
            // the error indicates metadata files are deleted.
            // take this opportunity to destroy from InSDB.
            // this is workaround for MyRocks
            int errno_saved = errno;

            insdb::Options insdb_options;
            insdb_options.kv_ssd.resize(0);
            for (auto devname : db_options.kv_ssd)
                insdb_options.kv_ssd.push_back(devname.c_str());
            insdb_options.num_column_count = kInSDB_MaxNrColumnFamilies;
            if(db_options.info_log)
                insdb_options.info_log = new InsDB_Logger(db_options.info_log);
            (void)insdb::DestroyDB(name, insdb_options);
            // ignore errno from DestroyDB() to perserve the one from ReadManifest().
            // MyRocks relys on errno to see whether to create new DB
            errno = errno_saved;
        }
        return s;
    }
    for (auto column_pair : manifest.column_families)
        column_families->push_back(column_pair.second.name);

    return Status::OK();
}

Status DB::DeleteRange(const WriteOptions& options,
                             ColumnFamilyHandle* column_family,
                             const Slice& begin_key, const Slice& end_key) {

    throw std::runtime_error("Not supported API");
}

// DBImpl

// constructor
DBImpl::DBImpl(const DBOptions& db_options, const std::string& dbname) :
    mutable_db_options_(db_options),
    immutable_db_options_(db_options),
    env_options_(BuildDBOptions(immutable_db_options_, mutable_db_options_)),
    env_(db_options.env),
    stats_(db_options.statistics.get()),
    dbname_(dbname),
    default_cf_handle_(nullptr),
    next_file_number_(2),
    options_file_number_(0),
    mutex_(stats_, env_, DB_MUTEX_WAIT_MICROS,
            db_options.use_adaptive_mutex),
    insdb_(nullptr),
    max_nr_column_family_(db_options.num_cols) //Fix bug：max number of column familiy can not be configured
{
    metadata_manifest_path.append(dbname_);
    metadata_manifest_path.append("/");
    metadata_manifest_path.append(DBImpl::kKvdbMeta_Manifest);

#ifdef KVDB_ENABLE_IOTRACE
    init_iotrace();
#endif

}

DBImpl::~DBImpl() {

#ifdef KVDB_ENABLE_IOTRACE
    flush_iotrace();
#endif

    // Clean up residual snapshots
    while (!snapshots_.empty()) {
        Snapshot* snapshot = snapshots_.newest();
        ReleaseSnapshot(snapshot);
    }

	if (insdb_)
		delete insdb_;

    if (insdb_options_.comparator && insdb_options_.comparator != insdb::BytewiseComparator())
        delete insdb_options_.comparator;

    if (insdb_options_.merge_operator)
        delete insdb_options_.merge_operator;

    delete default_cf_handle_;

    // Conditionally delete all ColumnFamilyData elements in this collection and not in column_families_.
    // column_families_ will be automatically cleared when its destructor is called
    for (const auto& i : column_family_ids_) {
        if (i.second->Unref())
            delete i.second;
    }
}

Status DBImpl::AllocateColumnFamilyID(uint32_t& cf_id)
{
    for (uint32_t next_cf_id = 1; next_cf_id < max_nr_column_family_; next_cf_id ++)
    {
        std::unordered_map<uint32_t,ColumnFamilyData*>::const_iterator cf_ids_itr = column_family_ids_.find(next_cf_id);
        if(cf_ids_itr != column_family_ids_.end())
        {
            continue;
        }
        cf_id = next_cf_id;
        return Status::OK();
    }
    return Status::NoSpace("Reached maximum of column families");
}

ColumnFamilyHandleImpl* DBImpl::FindColumnFamilyID(uint32_t cf_id)
{
    std::unordered_map<uint32_t,ColumnFamilyData*>::const_iterator cf_ids_itr = column_family_ids_.find(cf_id);
    if(cf_ids_itr != column_family_ids_.end())
    {
        return new ColumnFamilyHandleImpl(cf_ids_itr->second, this, &mutex_);
    }

    return nullptr;
}

Status DBImpl::SetColumnFamilyTtl(ColumnFamilyHandle* column_family, uint32_t ttl) {
  return SetColumnFamilyTtlImpl(column_family, ttl);
}

Status DBImpl::SetColumnFamilyTtlImpl(ColumnFamilyHandle* column_family, uint32_t ttl) {
  //set insdb ttl
  insdb::Status s = insdb_->SetTtlInTtlList(column_family->GetID(), ttl);
  if(!s.ok()) {
    return convert_insdb_status(s);
  }

  // Store DB options
  Logger *logger = immutable_db_options_.info_log.get();

  // Read manifest if available
  Manifest manifest;

  Status status = env_->FileExists(metadata_manifest_path);
  if (status.ok()) {
    status = ReadManifest(manifest);
    if (!status.ok())
      return status;
    // Load next file number
    next_file_number_.store(manifest.next_file_number);
  }

  std::unordered_map<std::string,Manifest_ColumnFamily>::iterator got = manifest.column_families.find(column_family->GetName());
  if (got == manifest.column_families.end()) {
    return Status::InvalidArgument("missing column family");
  }
  else {
    got->second.ttl = ttl;
  }

  manifest.manifest_version = kKvdbMeta_ManifestVersion;
  manifest.next_file_number = next_file_number_.load();
  status = WriteManifest(manifest);
  if(!status.ok()) {
    return status;
  }
  Log(logger, "DB '%s' manifest is written", column_family->GetName().c_str());

  return Status::OK();
}

Status DBImpl::CreateColumnFamilyImpl(Manifest& manifest, const ColumnFamilyOptions& options,
                                  const std::string& column_family_name,
                                  ColumnFamilyHandle** handle) {
    // Read manifest if available
    Status status = env_->FileExists(metadata_manifest_path);
    if (status.ok())
    {
        status = ReadManifest(manifest);
        if (!status.ok())
            return status;
    }
    else
    {
        return status;
    }

    uint32_t cf_id = 0;
    status = AllocateColumnFamilyID(cf_id);
    if(!status.ok())
        return status;

    Manifest_ColumnFamily new_cfmeta;

    // Create column family data and handle
    // Default column family is created here too.
    auto *CFD = new ColumnFamilyData(
            cf_id,
            column_family_name,
            nullptr,
            nullptr,
            nullptr,
            options,
            immutable_db_options_,
            env_options_,
            nullptr);
    auto cf_handle = new ColumnFamilyHandleImpl(CFD, this, &mutex_);

    mutex_.Lock();
    // insert into name-CFD tables
    bool inserted = column_families_.insert({CFD->GetName(), CFD}).second;
    if (!inserted)
    {
        mutex_.Unlock();
        status = Status::InvalidArgument("duplicate column name");
        goto error_out;
    }
    inserted = column_family_ids_.insert({CFD->GetID(), CFD}).second;
    if (!inserted)
    {
        mutex_.Unlock();
        status = Status::InvalidArgument("duplicate column ID");
        goto error_out;
    }
    mutex_.Unlock();

    // Write Manifest
    new_cfmeta.ID = cf_id;
    new_cfmeta.ttl = 0;
    new_cfmeta.name = column_family_name;
    new_cfmeta.comparator = options.comparator->Name();

    manifest.cf_count ++;
    mutex_.Lock();
    inserted = manifest.column_families.insert({column_family_name, new_cfmeta}).second;
    if (!inserted)
    {
        column_families_.erase(column_family_name);
        mutex_.Unlock();
        status = Status::InvalidArgument("duplicate column name in metadata");
        goto error_out;
    }
    mutex_.Unlock();
    status = WriteManifest(manifest);
    if(!status.ok())
        goto error_out;

    if (kDefaultColumnFamilyName.compare(column_family_name) == 0)
    {
        assert(!default_cf_handle_);
        default_cf_handle_ = new ColumnFamilyHandleImpl(CFD, this, &mutex_);
    }

    // return CF handle
    *handle = cf_handle;
    return Status::OK();

error_out:
    mutex_.Lock();
    column_families_.erase(column_family_name);
    if(cf_id)
        column_family_ids_.erase(cf_id);
    mutex_.Unlock();

    return status;

}

Status DBImpl::CreateColumnFamily(const ColumnFamilyOptions& options,
                                  const std::string& column_family_name,
                                  ColumnFamilyHandle** handle) {
    assert(handle != nullptr);
    Manifest manifest;
    Status s = CreateColumnFamilyImpl(manifest, options, column_family_name, handle);
    if (s.ok()) {
      s = WriteOptionsFile(true);
    }
    if (s.ok())
    {
        // persist the latest manifest with new next file number increased by WriteOptionsFile()
        manifest.manifest_version = kKvdbMeta_ManifestVersion;
        manifest.next_file_number = next_file_number_.load();
        s = WriteManifest(manifest);
        if(!s.ok())
            return s;
    }
    return s;
}

Status DBImpl::CreateColumnFamilies(
    const ColumnFamilyOptions& cf_options,
    const std::vector<std::string>& column_family_names,
    std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  handles->clear();
  size_t num_cf = column_family_names.size();
  Status s;
  bool success_once = false;
  Manifest manifest;

  for (size_t i = 0; i < num_cf; i++) {
    ColumnFamilyHandle* handle;
    manifest.column_families.clear();
    s = CreateColumnFamilyImpl(manifest, cf_options, column_family_names[i], &handle);
    if (!s.ok()) {
      break;
    }
    handles->push_back(handle);
    success_once = true;
  }
  if (success_once) {
    Status persist_options_status = WriteOptionsFile(true);
    if (s.ok() && !persist_options_status.ok()) {
      s = persist_options_status;
    }
    if (s.ok()) {
        // persist the latest manifest with new next file number increased by WriteOptionsFile()
        manifest.next_file_number = next_file_number_.load();
        s = WriteManifest(manifest);
        if(!s.ok())
            return s;
    }
  }
  return s;
}

Status DBImpl::CreateColumnFamilies(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  handles->clear();
  size_t num_cf = column_families.size();
  Status s;
  bool success_once = false;
  Manifest manifest;

  for (size_t i = 0; i < num_cf; i++) {
    ColumnFamilyHandle* handle;
    manifest.column_families.clear();
    s = CreateColumnFamilyImpl(manifest, column_families[i].options,
                               column_families[i].name, &handle);
    if (!s.ok()) {
      break;
    }
    handles->push_back(handle);
    success_once = true;
  }
  if (success_once) {
    Status persist_options_status = WriteOptionsFile(
        true);
    if (s.ok() && !persist_options_status.ok()) {
      s = persist_options_status;
    }
    if (s.ok()) {
        // persist the latest manifest with new next file number increased by WriteOptionsFile()
        manifest.next_file_number = next_file_number_.load();
        s = WriteManifest(manifest);
        if(!s.ok())
            return s;
    }
  }
  return s;
}

Status DBImpl::DropColumnFamilyImpl(Manifest& manifest, ColumnFamilyHandle* column_family) {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    auto cfd = cfh->cfd();

    // Read manifest if available
    Status status = env_->FileExists(metadata_manifest_path);
    if (status.ok())
    {
        status = ReadManifest(manifest);
        if (!status.ok())
            return status;
    }
    else
    {
        return status;
    }
    // remove from manifest
    mutex_.Lock();
    int nr_erased = manifest.column_families.erase(cfd->GetName());
    if (nr_erased == 0)
    {
        mutex_.Unlock();
        return Status::NotFound("Column family is not found in metadata");
    }
    mutex_.Unlock();

    manifest.cf_count--;
    status = WriteManifest(manifest);
    if(!status.ok())
        return status;

    // Remove from the maps
    mutex_.Lock();
    nr_erased = column_families_.erase(cfd->GetName());
    if(nr_erased == 0)
    {
        mutex_.Unlock();
        return Status::NotFound("key not found in CF name map");
    }
    nr_erased = column_family_ids_.erase(cfd->GetID());
    if(nr_erased == 0)
    {
        mutex_.Unlock();
        return Status::NotFound("key not found in CF ID map");
    }
    mutex_.Unlock();

    bool todelete = cfd->Unref();
    assert(todelete);
    if(todelete)
        delete cfd;

    return Status::OK();
}

Status DBImpl::DropColumnFamily(ColumnFamilyHandle* column_family) {
    assert(column_family != nullptr);
    Manifest manifest;
    Status status = DropColumnFamilyImpl(manifest, column_family);
    if(status.ok())
    {
        status = WriteOptionsFile(true);
        if (status.ok()) {
            // persist the latest manifest with new next file number increased by WriteOptionsFile()
            manifest.next_file_number = next_file_number_.load();
            status = WriteManifest(manifest);
            if(!status.ok())
                return status;
        }
    }
    return status;
}

Status DBImpl::DropColumnFamilies(
    const std::vector<ColumnFamilyHandle*>& column_families) {
  Status s;
  bool success_once = false;
  Manifest manifest;
  for (auto* handle : column_families) {
      manifest.column_families.clear();
    s = DropColumnFamilyImpl(manifest, handle);
    if (!s.ok()) {
      break;
    }
    success_once = true;
  }
  if (success_once) {
    Status persist_options_status = WriteOptionsFile(true);
    if (s.ok() && !persist_options_status.ok()) {
      s = persist_options_status;
    }
    if (s.ok()) {
        // persist the latest manifest with new next file number increased by WriteOptionsFile()
        manifest.next_file_number = next_file_number_.load();
        s = WriteManifest(manifest);
        if(!s.ok())
            return s;
    }
  }
  return s;
}

ColumnFamilyHandle* DBImpl::DefaultColumnFamily() const  {
	  return default_cf_handle_;
}


class InsDB_Comparator : public insdb::Comparator {
public:
	InsDB_Comparator(const rocksdb::Comparator *comparator) : comparator_(comparator) { }

  int Compare(const insdb::Slice &a, const insdb::Slice &b) const override {
    return comparator_->Compare(Slice(a.data(), a.size()), Slice(b.data(), b.size()));
  }
  const char *Name() const override { return comparator_->Name(); }
  void FindShortestSeparator(std::string *start,
                             const insdb::Slice &limit) const override {
	  Slice slice_limit(limit.data(), limit.size());
	  comparator_->FindShortestSeparator(start, slice_limit);
  }
  void FindShortSuccessor(std::string *key) const override {
	  comparator_->FindShortSuccessor(key);
  }
private:
  const rocksdb::Comparator *comparator_;
};

class InsDB_MergeOperator : public insdb::AssociativeMergeOperator {
 public:
  InsDB_MergeOperator(const std::shared_ptr<MergeOperator> merge_operator) : merge_operator_(merge_operator) {}

  virtual bool Merge(const insdb::Slice& key,
                     const insdb::Slice* existing_value,
                     const insdb::Slice& value,
                     std::string* new_value,
                     insdb::Logger* logger) const {

	  bool success;
	  Slice slice_key(key.data(), key.size());
	  Slice slice_value(value.data(), value.size());
	  std::vector<Slice> operand_list;
	  operand_list.push_back(slice_value);
	  auto *insdb_logger = (InsDB_Logger *)logger;
	  Slice existing_operand;
	  MergeOperator::MergeOperationOutput merge_out(*new_value, existing_operand);
	  if (existing_value)
	  {
		  Slice slice_existing_value(existing_value->data(), existing_value->size());
	  	  MergeOperator::MergeOperationInput merge_in(slice_key, &slice_existing_value, operand_list, insdb_logger->GetLogger());
		  success = merge_operator_->FullMergeV2(merge_in, &merge_out);
	  }
	  else
	  {
	  	  MergeOperator::MergeOperationInput merge_in(slice_key, nullptr, operand_list, insdb_logger->GetLogger());
		  success = merge_operator_->FullMergeV2(merge_in, &merge_out);
	  }
      if(success)
          // FIX: user defined merge operator type issue
          // Update the correct new_value
          *new_value = ( merge_out.new_value == "" ? merge_out.existing_operand.ToString() : merge_out.new_value );
          // FIX: end
      return success;
  }

  virtual const char* Name() const {
	  return merge_operator_->Name();
  }

 private:
  const std::shared_ptr<MergeOperator> merge_operator_;
};

static void convert_to_insdb_options(const DBOptions& db_options,
        const std::unordered_map<std::uint32_t, ColumnFamilyData*>& column_families,
        insdb::Options &insdb_options)
{

    insdb_options.create_if_missing = db_options.create_if_missing;
    insdb_options.error_if_exists = db_options.error_if_exists;
    insdb_options.paranoid_checks = db_options.paranoid_checks;
    if(db_options.info_log)
        insdb_options.info_log = new InsDB_Logger(db_options.info_log);
    insdb_options.max_open_files = db_options.max_open_files;

    auto default_cf_itr = column_families.find(0);
    assert(default_cf_itr != column_families.end());
    ColumnFamilyOptions default_cfoptions = default_cf_itr->second->GetLatestCFOptions();

    // InSDB specific
    // TODO: improve InSDB to support per-column options
    insdb_options.comparator = new InsDB_Comparator(default_cfoptions.comparator);
	insdb_options.merge_operator = new InsDB_MergeOperator(default_cfoptions.merge_operator);
	insdb_options.kv_ssd.resize(0);
    for (auto devname : db_options.kv_ssd)
        insdb_options.kv_ssd.push_back(devname);
    insdb_options.sktable_low_key_count = db_options.tablecache_low;
    insdb_options.sktable_high_key_count = db_options.tablecache_high;
    insdb_options.max_cache_size = db_options.max_cache_size;
    insdb_options.max_key_size = db_options.max_key_size;
    insdb_options.max_table_size = db_options.max_table_size;
    if (strcmp("BlockBasedTable", default_cfoptions.table_factory->Name()) == 0)
    {
        auto bbt = reinterpret_cast<BlockBasedTableFactory*>(default_cfoptions.table_factory.get());
        auto bbt_opt = bbt->table_options();
        insdb_options.max_uval_cache_size = bbt_opt.block_cache->GetCapacity();
    }

    // insdb prefix detection
    insdb_options.prefix_detection = db_options.prefix_detection;
    // insdb disable cache
    insdb_options.disable_cache = db_options.disable_cache;
    // insdb disable IO size check
    insdb_options.disable_io_size_check = db_options.disable_io_size_check;
    // share iterator pad
    insdb_options.iterpad_share = db_options.iterpad_share;
    // keep written key blocks in memory
    insdb_options.keep_written_keyblock = db_options.keep_written_keyblock;
    // allow table eviction
    insdb_options.table_eviction = db_options.table_eviction;
    // insdb write worker count
    insdb_options.num_write_worker = db_options.num_write_worker;
    // insdb max request size
    insdb_options.max_request_size = db_options.max_request_size;
    // insdb max flush threshold
    insdb_options.flush_threshold = db_options.flush_threshold;
    // insdb min flush update count
    insdb_options.min_update_cnt_table_flush = db_options.min_update_cnt_table_flush;
    // insdb write slowdown trigger
    insdb_options.slowdown_trigger = db_options.slowdown_trigger;
    // insdb align size
    insdb_options.align_size = db_options.align_size;
    // insdb prefetch cache size
    insdb_options.max_uval_prefetched_size = db_options.max_uval_prefetched_size;
    // insdb iterator cache size
    insdb_options.max_uval_iter_buffered_size = db_options.max_uval_iter_buffered_size;
    // insdb apporximate key size
    insdb_options.approx_key_size = db_options.approx_key_size;
    // insdb apporximate value size
    insdb_options.approx_val_size = db_options.approx_val_size;
    // insdb iterator prefetch hint
    insdb_options.iter_prefetch_hint = db_options.iter_prefetch_hint;
    // insdb random prefetch hint
    insdb_options.random_prefetch_hint = db_options.random_prefetch_hint;
    // insdb split prefix bits
    insdb_options.split_prefix_bits = db_options.split_prefix_bits;
    switch(default_cfoptions.compression) {
    case kNoCompression:
        insdb_options.compression = insdb::kNoCompression; break;
    default:
        insdb_options.compression = insdb::kSnappyCompression; break;
    }

    //insdb_options.env = options.env;
	//insdb_options.block_cache = options.memtable_factory;
#if 0
    //insdb_options.max_request_size = 4*1024;
    //insdb_options.write_buffer_size = default_cfoptions.write_buffer_size;
	insdb_options.block_size =;
	insdb_options.block_restart_interval =;
	insdb_options.max_file_size =;
	insdb_options.reuse_logs = ;
	insdb_options.filter_policy = ;
	insdb_options.max_cache_size =;
	insdb_options.cache_threshold = ;
	insdb_options.num_write_worker =;
	insdb_options.num_column_count =;
	insdb_options.max_request_size =;
	//insdb_options.max_key_size =;
	//insdb_options.max_cached_sktable_count =;
	//insdb_options.max_uval_prefetched_count;
	//insdb_options.max_uval_iter_buffered_size;
	//insdb_options.max_uval_cache_size;
	insdb_options.flush_threshold =;
#endif


#if 0
	// option list for review

	// ColumnFamilyOptions
	options.comparator;
	options.merge_operator;
	options.compaction_filter;
	options.compaction_filter_factory;
	options.write_buffer_size;
	options.max_write_buffer_number;
	options.min_write_buffer_number_to_merge;
	options.max_write_buffer_number_to_maintain;
	options.compression;
	options.compression_per_level;
	options.bottommost_compression;
	options.compression_opts;
	options.prefix_extractor;
	options.num_levels;
	options.level0_file_num_compaction_trigger;
	options.level0_slowdown_writes_trigger;
	options.level0_stop_writes_trigger;
	options.max_mem_compaction_level;
	options.target_file_size_base;
	options.target_file_size_multiplier;
	options.max_bytes_for_level_base;
	options.level_compaction_dynamic_level_bytes;
	options.max_bytes_for_level_multiplier;
	options.max_bytes_for_level_multiplier_additional;
	options.max_compaction_bytes;
	options.soft_rate_limit;
	options.hard_rate_limit;
	options.soft_pending_compaction_bytes_limit;
	options.hard_pending_compaction_bytes_limit;
	options.rate_limit_delay_max_milliseconds;
	options.arena_block_size;
	options.disable_auto_compactions;
	options.purge_redundant_kvs_while_flush;
	options.compaction_style;
	options.compaction_pri;
	options.verify_checksums_in_compaction;
	options.compaction_options_universal;
	options.compaction_options_fifo;
	options.max_sequential_skip_in_iterations;
	options.memtable_factory;
	options.table_factory;
	options.table_properties_collector_factories;
	options.inplace_update_support;
	options.inplace_update_num_locks;
	options.inplace_callback;
	options.memtable_prefix_bloom_size_ratio;
	options.memtable_huge_page_size;
	options.memtable_insert_with_hint_prefix_extractor;
	options.bloom_locality;
	options.max_successive_merges;
	options.min_partial_merge_operands;
	options.optimize_filters_for_hits;
	options.paranoid_file_checks;
	options.force_consistency_checks;
	options.report_bg_io_stats;

	// DBOptions
	options.create_if_missing;
	options.create_missing_column_families;
	options.error_if_exists;
	options.paranoid_checks;
	options.env;
	options.rate_limiter;
	options.sst_file_manager;
	options.info_log;
	options.info_log_level;
	options.max_open_files;
	options.max_file_opening_threads;
	options.max_total_wal_size;
	options.statistics;
	options.disableDataSync;
	options.use_fsync;
	options.db_paths;
	options.db_log_dir;
	options.wal_dir;
	options.delete_obsolete_files_period_micros;
	options.base_background_compactions;
	options.max_background_compactions;
	options.max_subcompactions;
	options.max_background_flushes;
	options.max_log_file_size;
	options.log_file_time_to_roll;
	options.keep_log_file_num;
	options.recycle_log_file_num;
	options.max_manifest_file_size;
	options.table_cache_numshardbits;
	options.WAL_ttl_seconds;
	options.WAL_size_limit_MB;
	options.manifest_preallocation_size;
	options.allow_mmap_reads;
	options.allow_mmap_writes;
	options.use_direct_reads;
	options.use_direct_writes;
	options.allow_fallocate;
	options.is_fd_close_on_exec;
	options.skip_log_error_on_recovery;
	options.stats_dump_period_sec;
	options.advise_random_on_open;
	options.db_write_buffer_size;
	options.write_buffer_manager;
	options.access_hint_on_compaction_start;
	options.new_table_reader_for_compaction_inputs;
	options.compaction_readahead_size;
	options.random_access_max_buffer_size;
	options.writable_file_max_buffer_size;
	options.use_adaptive_mutex;
	options.bytes_per_sync;
	options.wal_bytes_per_sync;
	options.listeners;
	options.enable_thread_tracking;
	options.delayed_write_rate;
	options.allow_concurrent_memtable_write;
	options.enable_write_thread_adaptive_yield;
	options.write_thread_max_yield_usec;
	options.write_thread_slow_yield_usec;
	options.skip_stats_update_on_db_open;
	options.wal_recovery_mode;
	options.allow_2pc;
	options.row_cache;
	options.wal_filter;
	options.fail_if_options_file_error;
	options.dump_malloc_stats;
	options.avoid_flush_during_recovery;
	options.avoid_flush_during_shutdown;
#endif
}

void ColumFamilyHandleToColumnFamilyMeta(ColumnFamilyHandleImpl* handle, DBImpl::Manifest_ColumnFamily *CFMeta)
{
    CFMeta->ID = handle->GetID();
    CFMeta->name = handle->GetName().data();
    CFMeta->comparator = handle->GetComparator()->Name();
}

#ifdef KVDB_TABLE_PROPERTY_COLLECTOR

insdb::Status Insdb_AddUserKeyCallback(
    const insdb::Slice &key,
    insdb::Slice &value,
    insdb::EntryType type,
     uint64_t seq,
     uint64_t file_size,
     uint16_t col_id,
     void *context)
{
    auto db = static_cast<DBImpl *>(context);
    auto cf = db->FindColumnFamilyID(col_id);
    if(cf==nullptr)
        return insdb::Status::NotFound("Column family not found");

    const Slice rocksdb_key(key.data(), key.size());
    const Slice rocksdb_value(value.data(), value.size());
    EntryType rocksdb_type;
    switch(type)
    {
    case insdb::kEntryPut: rocksdb_type = kEntryPut; break;
    case insdb::kEntryDelete: rocksdb_type = kEntryDelete; break;
    case insdb::kEntrySingleDelete: rocksdb_type = kEntrySingleDelete; break;
    case insdb::kEntryMerge: rocksdb_type = kEntryMerge; break;
    case insdb::kEntryOther:
    default:
        rocksdb_type = kEntryOther; break;
    }

    auto logger = db->GetLogger();
    //Log(logger, "cf %p", cf);
    Status final_status;
    TablePropertiesCollector *table_prop_collector;
    std::vector<std::unique_ptr<TablePropertiesCollector>> *table_prop_collectors = cf->GetTablePropCollectors();
    for(size_t i = 0; i < table_prop_collectors->size() ; i++)
    {
        table_prop_collector = (*table_prop_collectors)[i].get();
        Status status = table_prop_collector->AddUserKey(rocksdb_key, rocksdb_value,
                rocksdb_type, seq,file_size);
        if(!status.ok() && final_status.ok())
            final_status = status;
    }

    return convert_to_insdb_status(final_status);
}

insdb::Status Insdb_FlushCallback(
     uint16_t col_id,
     void *context)
{
    auto db = static_cast<DBImpl *>(context);
    auto cf = db->FindColumnFamilyID(col_id);
    if(cf==nullptr)
        return insdb::Status::NotFound("Column family not found");

    auto logger = db->GetLogger();
    Log(logger, "Insdb_FlushCallback Col%u", col_id);
    std::vector<std::unique_ptr<TablePropertiesCollector>> *table_prop_collectors = cf->GetTablePropCollectors();
    // collect custom properties and delete existing collectors.
    UserCollectedProperties properties;
    size_t i;
    for(i = 0; i < table_prop_collectors->size() ; i++)
    {
        (*table_prop_collectors)[i].get()->Finish(&properties);
        (*table_prop_collectors)[i].release();
    }
    table_prop_collectors->clear();

    // Call event listener if any exists.
    FlushJobInfo flush_job_info;
    flush_job_info.table_properties.user_collected_properties = properties;
    std::vector<std::shared_ptr<EventListener>> listeners = db->GetDBOptions().listeners;
    for(i = 0; i < listeners.size() ; i++)
    {
        listeners[i].get()->OnFlushCompleted(db, flush_job_info);
    }

    // create new collectors.
    TablePropertiesCollectorFactory::Context collector_factory_context;
    collector_factory_context.column_family_id = col_id;
    shared_ptr<TablePropertiesCollectorFactory> collector_factory;
    AdvancedColumnFamilyOptions::TablePropertiesCollectorFactories table_properties_collector_factories = cf->cfd()->GetLatestCFOptions().table_properties_collector_factories;
    for (i = 0; i<table_properties_collector_factories.size(); i++) {
      collector_factory = table_properties_collector_factories[i];
      Log(logger, "CreateTablePropertiesCollector %p", collector_factory);
      cf->GetTablePropCollectors()->emplace_back(
              collector_factory->CreateTablePropertiesCollector(collector_factory_context));
    }

    return insdb::Status::OK();
}
#endif

Status DBImpl::OpenImpl(const std::vector<ColumnFamilyDescriptor>& column_families,
        std::vector<ColumnFamilyHandle*>* handles)
{
    handles->clear();

    // Store DB options
    Logger *logger = immutable_db_options_.info_log.get();

    // Read manifest if available
    Manifest manifest;

    Status status = env_->FileExists(metadata_manifest_path);
    if (status.ok())
    {
        status = ReadManifest(manifest);
        if (!status.ok())
            return status;
        // Load next file number
        next_file_number_.store(manifest.next_file_number);
    }

    //Fix bug: not sepecify "default" leading core dump(NULL pointer)
    //Check "default" exist or not, give user prompt
    bool defaultExist = false;
    for(auto each_cf : column_families){
        if(kDefaultColumnFamilyName.compare(each_cf.name) == 0){
            defaultExist = true;
            break;
        }
    }
    if(!defaultExist)
        return Status::InvalidArgument("Default column family not specified");

    //Fix bug: output not same as RocksDB's
    //Check which column families not exist, give user prompt
    std::string notExistCol;
    std::unordered_set<std::string> set;
    bool allColExist = true;
    for(auto each_cf : column_families){
        set.insert(each_cf.name);
    }
    for(auto pair_cfmeta : manifest.column_families){
        if(set.find(pair_cfmeta.second.name) == set.end()){
            allColExist = false;
            if(notExistCol.size() < 1){
                notExistCol += pair_cfmeta.second.name;
            }
            else {
                notExistCol +=", " + pair_cfmeta.second.name;
            }
        }
    }
    if(!allColExist)
        return Status::InvalidArgument("You have to open all column families. Column families not opened:" +  notExistCol);

    //Fix bug：max number of column familiy can not be configured
    //set max_nr_column_family_ default value
    if(max_nr_column_family_ == 0)
        max_nr_column_family_ = kInSDB_MaxNrColumnFamilies;

    //Fix bug：max number of column familiy can not be configured
    //check the max_nr_column_family_ is valid
    if(max_nr_column_family_ > kInSDB_MaxSupportedColumnFamilies)
        return Status::InvalidArgument("The max column families supported is 256");

    // create ID map from manifest
    std::unordered_map<uint32_t,Manifest_ColumnFamily> idmap;
    for(auto pair_cfmeta : manifest.column_families)
    {
        if(idmap.insert({pair_cfmeta.second.ID, pair_cfmeta.second}).second == false)
            return Status::InvalidArgument("duplicate CF ID in manifest");
    }

    // Create or open column families
    uint32_t next_cf_id = 1;
    bool ttl_enabled_list[max_nr_column_family_] = {0};
    uint64_t ttl_list[max_nr_column_family_] = {0};
    bool ttl_flag = 0;
#ifdef KVDB_TABLE_PROPERTY_COLLECTOR
    bool table_prop_collector_exists = false;
#endif
    for(auto each_cf : column_families)
    {
        bool is_default_cf = kDefaultColumnFamilyName.compare(each_cf.name) == 0;
retry_id:
        std::unordered_map<uint32_t,Manifest_ColumnFamily>::const_iterator idmap_itr = idmap.find(next_cf_id);
        if(idmap_itr != idmap.end())
        {
            next_cf_id++;
            goto retry_id;
        }
        uint32_t cf_id = next_cf_id;
        uint32_t cf_ttl = each_cf.ttl;
        std::unordered_map<std::string,Manifest_ColumnFamily>::iterator got = manifest.column_families.find(each_cf.name);
        if (got == manifest.column_families.end())
        {
            // not found in metadata
            if (!is_default_cf && !immutable_db_options_.create_missing_column_families)
            {
                //Fix bug: output not same as RocksDB's
                //Modify prompt
                //return Status::InvalidArgument("missing column family");
                return Status::InvalidArgument("Column family not found:" + each_cf.name);
            }
            // allocate CF ID
            if (is_default_cf)
                cf_id = 0;
            else
                next_cf_id++;

            // Add to manifest
            Manifest_ColumnFamily new_cfmeta;
            new_cfmeta.ID = cf_id;
            new_cfmeta.ttl = cf_ttl;
            new_cfmeta.name.assign(each_cf.name);
            new_cfmeta.comparator.assign(each_cf.options.comparator->Name());

            manifest.cf_count++;
            bool inserted = manifest.column_families.insert({each_cf.name, new_cfmeta}).second;
            if (!inserted)
            {
                return Status::InvalidArgument("duplicate column name");
            }
        }
        else
        {
            Manifest_ColumnFamily cfmeta = got->second;
            // preserve column family ID
            cf_id = cfmeta.ID;
            //preserve column family ttl if the ttl is invalid
            if(each_cf.ttl_enabled)
              got->second.ttl = cf_ttl;
        #if 0
            else if (ttl_invalid?)
              cf_ttl = cfmeta.ttl;
        #endif
            // verify comparator
            if (cfmeta.comparator.compare(each_cf.options.comparator->Name()) != 0)
            {
                return Status::InvalidArgument("unmatched comparator");
            }
        }

        // Create column family handle
        auto *CFD = new ColumnFamilyData(
                cf_id,
                each_cf.name,
                nullptr,
                nullptr,
                nullptr,
                each_cf.options,
                immutable_db_options_,
                env_options_,
                nullptr);
        auto cf_handle = new ColumnFamilyHandleImpl(CFD, this, &mutex_);

#ifdef KVDB_TABLE_PROPERTY_COLLECTOR
        // Create initial table property collectors
        TablePropertiesCollectorFactory::Context collector_factory_context;
        collector_factory_context.column_family_id = cf_handle->GetID();
        shared_ptr<TablePropertiesCollectorFactory> collector_factory;
        AdvancedColumnFamilyOptions::TablePropertiesCollectorFactories table_properties_collector_factories = cf_handle->cfd()->GetLatestCFOptions().table_properties_collector_factories;
        for (size_t i = 0; i<table_properties_collector_factories.size(); i++) {
          collector_factory = table_properties_collector_factories[i];
          Log(logger, "CreateTablePropertiesCollector %p", collector_factory);
          cf_handle->GetTablePropCollectors()->emplace_back(
                  collector_factory->CreateTablePropertiesCollector(collector_factory_context));
          table_prop_collector_exists = true;
        }
#endif
        // insert into name-handle tables
        column_families_.insert({cf_handle->GetName(), CFD});
        column_family_ids_.insert({cf_handle->GetID(), CFD});
        if(each_cf.ttl_enabled) {
          ttl_list[cf_handle->GetID()] = cf_ttl;
          ttl_enabled_list[cf_handle->GetID()] = each_cf.ttl_enabled;
          ttl_flag = 1;
        }

        // push to the returning vector
        handles->push_back(cf_handle);

        // Save default column family
        if (is_default_cf)
        {
            assert(!default_cf_handle_);
            default_cf_handle_ = new ColumnFamilyHandleImpl(CFD, this, &mutex_);
        }
    }

    // Set up InSDB
    convert_to_insdb_options(BuildDBOptions(immutable_db_options_, mutable_db_options_), column_family_ids_, insdb_options_);
    insdb_options_.num_column_count = max_nr_column_family_;
#ifdef KVDB_TABLE_PROPERTY_COLLECTOR
    if(table_prop_collector_exists)
    {
        insdb_options.AddUserKey = Insdb_AddUserKeyCallback;
        insdb_options.AddUserKeyContext = this;
        insdb_options.Flush = Insdb_FlushCallback;
        insdb_options.FlushContext = this;
    }
#endif
    insdb::DB *insdb = nullptr;
    Log(logger, "DBOpen max_uval_cache_size %lu", insdb_options_.max_uval_cache_size);
    insdb::Status s;
    if(ttl_flag)
      s = insdb::DB::OpenWithTtl(insdb_options_, dbname_, &insdb, (uint64_t*)&ttl_list, (bool*)&ttl_enabled_list);
    else
      s = insdb::DB::Open(insdb_options_, dbname_, &insdb);
    if(!s.ok())
    {
        return convert_insdb_status(s);
    }

    // Create DB directory
    // TODO: store metadata into InSDB when it's supported
    status = env_->CreateDirIfMissing(dbname_);
    if(!status.ok())
    {
        delete insdb;
        Log(logger, "failed to create DB directory %s", dbname_.c_str());
        return status;
    }

    // Check for the IDENTITY file and create it if not there
    status = env_->FileExists(IdentityFileName(dbname_));
    if (status.IsNotFound()) {
        status = SetIdentityFile(env_, dbname_);
      if (!status.ok()) {
        delete insdb;
        return status;
      }
      Log(logger, "DB %s identity file is written", dbname_.c_str());
    } else if (!status.ok()) {
      assert(status.IsIOError());
      delete insdb;
      return status;
    }

    // Persist RocksDB Options.
    // The WriteOptionsFile() will release and lock the mutex internally.
    status = WriteOptionsFile(true);
    if(!status.ok())
    {
        delete insdb;
        return status;
    }
    Log(logger, "DB %s options file is written", dbname_.c_str());

    // persist manifest after updating next file number
    manifest.manifest_version = kKvdbMeta_ManifestVersion;
    manifest.next_file_number = next_file_number_.load();
    status = WriteManifest(manifest);
    if(!status.ok())
    {
        delete insdb;
        return status;
    }
    Log(logger, "DB '%s' manifest is written", dbname_.c_str());

    SetInSDB(insdb);
   return Status::OK();
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
    DBOptions db_options(options);
    ColumnFamilyOptions cf_options(options);
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.push_back(
        ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
    std::vector<ColumnFamilyHandle*> handles;
    Status s = DB::Open(db_options, dbname, column_families, &handles, dbptr);
    if (s.ok()) {
      assert(handles.size() == 1);
      delete handles[0];
    }
    return s;
}

Status DB::Open(const DBOptions& db_options, const std::string& name,
                   const std::vector<ColumnFamilyDescriptor>& column_families,
                   std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {

    if(column_families.size()==0)
        abort();
    // Create DB object
    DBImpl* impl = new DBImpl(db_options, name);

    Status status = impl->OpenImpl(column_families, handles);
    if(!status.ok())
        delete impl;
    else
        *dbptr = impl;
    return status;
}

extern insdb::WriteBatch *WrapperToInsdbWriteBatch(void *wrapper_context);

Status DBImpl::Write(const WriteOptions& options,
                     WriteBatch* updates) {
	insdb::WriteOptions insdb_options;
	insdb_options.sync = options.sync;
	insdb::WriteBatch *insdb_wb = WrapperToInsdbWriteBatch(updates->WrapperContext());
	insdb::Status s = insdb_->Write(insdb_options, insdb_wb);

#ifdef KVDB_ENABLE_IOTRACE
    if (s.ok())
        log_iotrace("WRITE",-1,"",0,updates->Count());
    else
        log_iotrace("WRITE FAIL",-1,"",0,updates->Count());
#endif

    return convert_insdb_status(s);
}


Status DBImpl::WriteImpl(const WriteOptions& options, WriteBatch* updates,
                   WriteCallback* callback,
                   uint64_t* log_used, uint64_t log_ref,
                   bool disable_memtable, uint64_t* seq_used,
                   size_t batch_cnt)
{
    if(callback || batch_cnt || log_ref)
    {
        throw std::runtime_error("Not supported parameter");
    }
    if(disable_memtable)
    {
        // TODO: find out what InSDB can do.
    }
    return Write(options, updates);
}

Status DBImpl::Get(const ReadOptions& options, 
                   const Slice& key, 
                   std::string* value) {
	insdb::Slice k(key.data(), key.size());
	insdb::Status s = insdb_->Get(convert_read_options_to_insdb(options), k, value);
#ifdef KVDB_ENABLE_IOTRACE
	if (s.ok())
	    log_iotrace("GET",0,key.data(),key.size(),value->size());
	else
        log_iotrace("GET FAIL",0,key.data(),key.size(),value->size());
#endif
    return convert_insdb_status(s);
}

// Cleanup function for InSDB PinnableSlice
void InsdbPinnableSlice_CleanupFunction(void* arg1, void* arg2)
{
    delete reinterpret_cast<insdb::PinnableSlice *>(arg1);
}

Status DBImpl::Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value)
{
    insdb::Slice k(key.data(), key.size());
    insdb::PinnableSlice *insdb_value = new insdb::PinnableSlice();
    insdb::Status s = insdb_->Get(convert_read_options_to_insdb(options), k, insdb_value, column_family->GetID());
    if(s.ok()) {
        value->PinSlice(Slice(insdb_value->data(), insdb_value->size()), InsdbPinnableSlice_CleanupFunction, insdb_value, nullptr);
    }
    else
    {
        delete insdb_value;
    }
#ifdef KVDB_ENABLE_IOTRACE
    if (s.ok())
        log_iotrace("GET PINNABLE",column_family->GetID(),key.data(),key.size(),value->size());
    else
        log_iotrace("GET PINNABLE FAIL",column_family->GetID(),key.data(),key.size(),value->size());
#endif
    return convert_insdb_status(s);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {

	insdb::WriteOptions insdb_options;
	insdb_options.sync = options.sync;
	insdb::Slice k(key.data(), key.size());
	insdb::Status s = insdb_->Delete(insdb_options, k);

#ifdef KVDB_ENABLE_IOTRACE
    if (s.ok())
        log_iotrace("DELETE",0,key.data(),key.size(),0);
    else
        log_iotrace("DELETE FAIL",0,key.data(),key.size(),0);
#endif

    return convert_insdb_status(s);
}

Iterator* DBImpl::NewIterator(const ReadOptions& options,
                              ColumnFamilyHandle* column_family)  {
    if (options.managed || options.tailing)
        return NewErrorIterator(Status::InvalidArgument(
            "Managed Iterators not supported in KVDB."));
    //Fix : NewIterator is not support kPersistedData option
    if (options.read_tier == kPersistedTier)
        return NewErrorIterator(Status::NotSupported(
            "ReadTier::kPersistedData is not yet supported in iterators."));
    //Fix end
#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("NEW ITERATOR",column_family->GetID(),"",0,0);
#endif

    //Feat:Iterator prefix seek
    return new IteratorImpl(insdb_->NewIterator(convert_read_options_to_insdb(options), column_family->GetID()),
                            options, column_family);
    //end
}

const std::string& DBImpl::GetName() const {
    return dbname_;
}

Env* DBImpl::GetEnv() const  {
    return env_;
}

//////////////////////////////////////
///// not implemented APIs listed below
//////////////////////////////////////

Status DBImpl::Put(const WriteOptions& options,
           ColumnFamilyHandle* column_family, const Slice& key,
           const Slice& value)  {

	insdb::WriteBatch wb;
	wb.Put(insdb::Slice(key.data(), key.size()), insdb::Slice(value.data(), value.size()), static_cast<unsigned short int>(column_family->GetID()));

	insdb::Status s = insdb_->Write(convert_write_options_to_insdb(options), &wb);

#ifdef KVDB_ENABLE_IOTRACE
    if (s.ok())
        log_iotrace("PUT",column_family->GetID(),key.data(),key.size(),value.size());
    else
        log_iotrace("PUT FAIL",column_family->GetID(),key.data(),key.size(),value.size());
#endif

	return convert_insdb_status(s);
}

Status DBImpl::Delete(const WriteOptions& options,
              ColumnFamilyHandle* column_family,
              const Slice& key)  {

    insdb::WriteBatch wb;
    wb.Delete(insdb::Slice(key.data(), key.size()), static_cast<unsigned short int>(column_family->GetID()));

    insdb::Status s = insdb_->Write(convert_write_options_to_insdb(options), &wb);

#ifdef KVDB_ENABLE_IOTRACE
    if (s.ok())
        log_iotrace("DELETE",column_family->GetID(),key.data(),key.size(),0);
    else
        log_iotrace("DELETE FAIL",column_family->GetID(),key.data(),key.size(),0);
#endif

    return convert_insdb_status(s);
}

Status DBImpl::SingleDelete(const WriteOptions& options,
                    ColumnFamilyHandle* column_family,
                    const Slice& key)  {

    insdb::WriteBatch wb;
    wb.Delete(insdb::Slice(key.data(), key.size()), static_cast<unsigned short int>(column_family->GetID()));

    // Delete and SingleDelete are identical to InSDB.
    insdb::Status s = insdb_->Write(convert_write_options_to_insdb(options), &wb);

#ifdef KVDB_ENABLE_IOTRACE
    if (s.ok())
        log_iotrace("SINGLE DELETE",column_family->GetID(),key.data(),key.size(),0);
    else
        log_iotrace("SINGLE DELETE FAIL",column_family->GetID(),key.data(),key.size(),0);
#endif

    return convert_insdb_status(s);
}

Status DBImpl::DeleteRange(const WriteOptions& options,
                             ColumnFamilyHandle* column_family,
                             const Slice& begin_key, const Slice& end_key) {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::Merge(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value)  {

  ColumnFamilyDescriptor cfd;
  Status status = column_family->GetDescriptor(&cfd);
  if(!status.ok())
      return status;
  if (!cfd.options.merge_operator) {
    // FIX: merge_operator not support prompt
    // modify same with RocksDB
    return Status::NotSupported();
    // FIX: end
    }

  insdb::WriteBatch wb;
  // FIX: merge core dump issue
  // Add slice value
  wb.Merge(insdb::Slice(key.data(), key.size()), insdb::Slice(value.data(), value.size()), static_cast<unsigned short int>(column_family->GetID()));
  // FIX: end
  insdb::Status s = insdb_->Write(convert_write_options_to_insdb(options), &wb);

#ifdef KVDB_ENABLE_IOTRACE
    if (s.ok())
        log_iotrace("MERGE",column_family->GetID(),key.data(),key.size(),value.size());
    else
        log_iotrace("MERGE FAIL",column_family->GetID(),key.data(),key.size(),value.size());
#endif

  return convert_insdb_status(s);
}
Status DBImpl::Get(const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   std::string* value)  {

    insdb::Status s = insdb_->Get(convert_read_options_to_insdb(options), insdb::Slice(key.data(), key.size()), value, static_cast<unsigned short int>(column_family->GetID()));

#ifdef KVDB_ENABLE_IOTRACE
    if (s.ok())
        log_iotrace("GET",column_family->GetID(),key.data(),key.size(),value->size());
    else
        log_iotrace("GET FAIL",column_family->GetID(),key.data(),key.size(),value->size());
#endif

    return convert_insdb_status(s);
}
std::vector<Status> DBImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values)  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators)  {
    //Fix :  NewIterators is not support kPersistedTier option
    if (options.read_tier == kPersistedTier)
        return Status::NotSupported(
            "ReadTier::kPersistedData is not yet supported in iterators.");
    //Fix end
    //feat NewIterators function
    iterators->clear();
    iterators->reserve(column_families.size());
    for (size_t i = 0; i < column_families.size(); ++i) {
      Iterator* db_iter = NewIterator(options, column_families[i]);
      iterators->push_back(db_iter);
    }

    return Status::OK();
}

const Snapshot* DBImpl::GetSnapshotImpl(bool is_write_conflict_boundary) {
  int64_t unix_time = 0;
  env_->GetCurrentTime(&unix_time);  // Ignore error
  SnapshotImpl* s = new SnapshotImpl;
  const insdb::Snapshot *insdb_snapshot = insdb_->GetSnapshot();
  assert(insdb_snapshot);

  InstrumentedMutexLock l(&mutex_);
  return snapshots_.New(s, insdb_snapshot, unix_time,
                        is_write_conflict_boundary);
}

const Snapshot* DBImpl::GetSnapshot()  {
    return GetSnapshotImpl(false);
}

const Snapshot* DBImpl::GetSnapshotForWriteConflictBoundary() {
  return GetSnapshotImpl(true);
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  const SnapshotImpl* casted_s = reinterpret_cast<const SnapshotImpl*>(s);
  {
    InstrumentedMutexLock l(&mutex_);
    snapshots_.Delete(casted_s);
  }
  insdb_->ReleaseSnapshot(casted_s->insdb_snapshot_);
  delete casted_s;
}

bool DBImpl::GetProperty(ColumnFamilyHandle* column_family,
                 const Slice& property, std::string* value)  {

    throw std::runtime_error("Not supported API");
    // return false;
}
bool DBImpl::GetMapProperty(ColumnFamilyHandle* column_family,
                    const Slice& property,
                    std::map<std::string, std::string>* value)  {
    if (property.compare(Slice(DB::Properties::kCFStats)) == 0)
    {
        // TODO: return real statistics
    }
    else
    {
        throw std::runtime_error("Not supported API");
    }
    return true;
}
bool DBImpl::GetIntProperty(ColumnFamilyHandle* column_family,
                    const Slice& property, uint64_t* value)  {

    throw std::runtime_error("Not supported API");
}
bool DBImpl::GetAggregatedIntProperty(const Slice& property,
                              uint64_t* value)  {

    // TODO: return real usage info
    if (property.compare(Slice(DB::Properties::kSizeAllMemTables)) == 0)
    {
        *value = 0;
    } else if (property.compare(Slice(DB::Properties::kCurSizeAllMemTables)) == 0)
    {
        *value = 0;
    } else if (property.compare(Slice(DB::Properties::kEstimateTableReadersMem)) == 0)
    {
        *value = 0;
    } else {
        throw std::runtime_error("Not supported API");
    }
    return true;
}

void DBImpl::GetApproximateSizes(ColumnFamilyHandle* column_family,
                         const Range* range, int n, uint64_t* sizes,
                         uint8_t include_flags)  {

    assert(include_flags & DB::SizeApproximationFlags::INCLUDE_FILES ||
           include_flags & DB::SizeApproximationFlags::INCLUDE_MEMTABLES);

    // Convert to InSDB ranges
    auto insdb_range = new insdb::Range[n];
    for(int i = 0; i < n; i++)
    {
        insdb_range[i].start.data_ = range[i].start.data();
        insdb_range[i].start.size_ = range[i].start.size();
        insdb_range[i].limit.data_ = range[i].limit.data();
        insdb_range[i].limit.size_ = range[i].limit.size();
    }

    insdb_->GetApproximateSizes(insdb_range,n,sizes,column_family->GetID());

    delete [] insdb_range;
}

void DBImpl::GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                         const Range& range,
                                         uint64_t* const count,
                                         uint64_t* const size)
{
    // TODO: return real sizes
    *count = 0;
    *size = 0;
}


Status DBImpl::CompactRange(const CompactRangeOptions& options,
                            ColumnFamilyHandle* column_family,
                            const Slice* begin, const Slice* end)  {
    insdb::Status s;
    insdb::Slice *insdb_begin_ptr;
    insdb::Slice *insdb_end_ptr;
    if(begin)
        insdb_begin_ptr = new insdb::Slice(begin->data(), begin->size());
    else
        insdb_begin_ptr = nullptr;
    if(end)
        insdb_end_ptr = new insdb::Slice(end->data(), end->size());
    else
        insdb_end_ptr = nullptr;
    s = insdb_->CompactRange(insdb_begin_ptr, insdb_end_ptr, column_family->GetID());
    if (insdb_begin_ptr)
        delete insdb_begin_ptr;
    if (insdb_end_ptr)
        delete insdb_end_ptr;
    return convert_insdb_status(s);
}

Status DBImpl::SetDBOptions(
    const std::unordered_map<std::string, std::string>& new_options)  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::CompactFiles(
    const CompactionOptions& compact_options,
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& input_file_names,
    const int output_level, const int output_path_id)  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::PauseBackgroundWork()  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::ContinueBackgroundWork()  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles)  {

    Logger *logger = immutable_db_options_.info_log.get();

    ROCKS_LOG_WARN(logger, "KVDB does not have compaction process. Ignore auto compaction setting.");
    return Status::OK();
}
int DBImpl::NumberLevels(ColumnFamilyHandle* column_family)  {

    throw std::runtime_error("Not supported API");
}
int DBImpl::MaxMemCompactionLevel(ColumnFamilyHandle* column_family)  {

    throw std::runtime_error("Not supported API");
}
int DBImpl::Level0StopWriteTrigger(ColumnFamilyHandle* column_family)  {

    throw std::runtime_error("Not supported API");
}

Options DBImpl::GetOptions(ColumnFamilyHandle* column_family) const  {

    ColumnFamilyDescriptor cfd;
    Status status = column_family->GetDescriptor(&cfd);
    if(!status.ok())
        throw std::runtime_error(status.ToString());
    return Options(BuildDBOptions(immutable_db_options_, mutable_db_options_), cfd.options);
}
DBOptions DBImpl::GetDBOptions() const  {

    return BuildDBOptions(immutable_db_options_, mutable_db_options_);
}
Status DBImpl::Flush(const FlushOptions& options,
             ColumnFamilyHandle* column_family)  {

    insdb::FlushOptions flushOptions;
    flushOptions.wait = options.wait;
    insdb::Status insdb_status = insdb_->Flush(flushOptions);
    return convert_insdb_status(insdb_status);
}
Status DBImpl::SyncWAL()  {

    throw std::runtime_error("Not supported API");
}
SequenceNumber DBImpl::GetLatestSequenceNumber() const  {

    return insdb_->GetLatestSequenceNumber();
}

Status DBImpl::GetLatestSequenceForKey(ColumnFamilyHandle* cfh, const Slice& key,
                               bool cache_only, SequenceNumber* seq,
                               bool* found_record_for_key,
                               bool* is_blob_index)
{
    insdb::Slice insdb_key(key.data(), key.size());
    insdb::Status insdb_status = insdb_->GetLatestSequenceForKey(insdb_key, seq, found_record_for_key, cfh->GetID());
    return convert_insdb_status(insdb_status);
}

#ifndef ROCKSDB_LITE
Status DBImpl::DisableFileDeletions()  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::EnableFileDeletions(bool force)  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::GetLiveFiles(std::vector<std::string>&,
                    uint64_t* manifest_file_size,
                    bool flush_memtable)  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::GetSortedWalFiles(VectorLogPtr& files)  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::GetUpdatesSince(
    SequenceNumber seq_number, unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options)  {

    throw std::runtime_error("Not supported API");
}
// Windows API macro interference
#undef DeleteFile
Status DBImpl::DeleteFile(std::string name)  {

    throw std::runtime_error("Not supported API");
}

Status DBImpl::DeleteFilesInRange(ColumnFamilyHandle* column_family,
                                  const Slice* begin, const Slice* end) {
    //Feat: delete the keys 
     CompactRangeOptions options;

    return CompactRange(options, column_family, begin, end);
    //Feat end
}

Status DBImpl::IngestExternalFile(
    ColumnFamilyHandle* column_family,
    const std::vector<std::string>& external_files,
    const IngestExternalFileOptions& options)  {

    throw std::runtime_error("Not supported API");
}

Status DBImpl::VerifyChecksum() {
    throw std::runtime_error("Not supported API");
}

Status DBImpl::GetDbIdentity(std::string& identity) const  {
    std::string idfilename = IdentityFileName(dbname_);
    const EnvOptions soptions;
    unique_ptr<SequentialFileReader> id_file_reader;
    Status s;
    {
      unique_ptr<SequentialFile> idfile;
      s = env_->NewSequentialFile(idfilename, &idfile, soptions);
      if (!s.ok()) {
        return s;
      }
      id_file_reader.reset(new SequentialFileReader(std::move(idfile)));
    }

    uint64_t file_size;
    s = env_->GetFileSize(idfilename, &file_size);
    if (!s.ok()) {
      return s;
    }
    char* buffer = reinterpret_cast<char*>(alloca(file_size));
    Slice id;
    s = id_file_reader->Read(static_cast<size_t>(file_size), &id, buffer);
    if (!s.ok()) {
      return s;
    }
    identity.assign(id.ToString());
    // If last character is '\n' remove it from identity
    if (identity.size() > 0 && identity.back() == '\n') {
      identity.pop_back();
    }
    return s;
}

Status DBImpl::GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                TablePropertiesCollection* props)  {

    throw std::runtime_error("Not supported API");
}
Status DBImpl::GetPropertiesOfTablesInRange(
    ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
    TablePropertiesCollection* props)  {
    Logger *logger = immutable_db_options_.info_log.get();

    ROCKS_LOG_WARN(logger, "GetPropertiesOfTablesInRange NIMP");
    return Status::OK();
}


#endif  // ROCKSDB_LITE

Status DBImpl::FlushWAL(bool sync) {
    insdb::FlushOptions flushOptions;
    flushOptions.wait = sync;
    insdb::Status insdb_status = insdb_->Flush(flushOptions);
    return convert_insdb_status(insdb_status);
}


Snapshot::~Snapshot() {
}

void DBImpl::CancelAllBackgroundWork(bool wait)
{
    insdb_->CancelAllBackgroundWork(wait);
}

Status DestroyDB(const std::string& dbname, const Options& options) {

    // Destroy InSDB object
	insdb::Options insdb_options;
	insdb_options.kv_ssd.resize(0);
    for (auto devname : options.kv_ssd)
        insdb_options.kv_ssd.push_back(devname.c_str());

    insdb_options.num_column_count = kInSDB_MaxNrColumnFamilies;
    insdb_options.disable_io_size_check = options.disable_io_size_check;

    if (options.info_log) {
        insdb_options.info_log = new InsDB_Logger(options.info_log);
    }

	insdb::Status insdb_status = insdb::DestroyDB(dbname, insdb_options);

	if(!insdb_status.ok())
	{
	    return convert_insdb_status(insdb_status);
	}

    Env *env = options.env;

    // Delete all files
    std::vector<std::string> filenames;
    Status status = env->GetChildren(dbname, &filenames);
    if (!status.ok()) {
      return status;
    }

    for (auto& filename : filenames) {
        std::string full_path(dbname);
        full_path.append("/");
        full_path.append(filename);
        (void)env->DeleteFile(full_path);
    }

    // Delete DB director
    status = env->DeleteDir(dbname);

    return status;

}

}  // namespace rocksdb
