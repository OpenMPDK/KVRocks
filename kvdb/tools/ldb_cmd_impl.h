//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/utilities/ldb_cmd.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace rocksdb {

class DBDumperCommand : public LDBCommand {
 public:
  static std::string Name() { return "dump"; }

  DBDumperCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  /**
   * Extract file name from the full path. We handle both the forward slash (/)
   * and backslash (\) to make sure that different OS-s are supported.
  */
  static std::string GetFileNameFromPath(const std::string& s) {
    std::size_t n = s.find_last_of("/\\");

    if (std::string::npos == n) {
      return s;
    } else {
      return s.substr(n + 1);
    }
  }

  void DoDumpCommand();

  bool null_from_;
  std::string from_;
  bool null_to_;
  std::string to_;
  int max_keys_;
  std::string delim_;
  bool count_only_;
  bool count_delim_;
  bool print_stats_;
  std::string path_;

  static const std::string ARG_COUNT_ONLY;
  static const std::string ARG_COUNT_DELIM;
  static const std::string ARG_STATS;
  static const std::string ARG_TTL_BUCKET;
};

class DBLoaderCommand : public LDBCommand {
 public:
  static std::string Name() { return "load"; }

  DBLoaderCommand(std::string& db_name, std::vector<std::string>& args);

  DBLoaderCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual Options PrepareOptionsForOpenDB() override;

 private:
  bool disable_wal_;
  bool bulk_load_;
  bool compact_;

  static const std::string ARG_DISABLE_WAL;
  static const std::string ARG_BULK_LOAD;
  static const std::string ARG_COMPACT;
};

class ListColumnFamiliesCommand : public LDBCommand {
 public:
  static std::string Name() { return "list_column_families"; }

  ListColumnFamiliesCommand(const std::vector<std::string>& params,
                            const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

 private:
  std::string dbname_;
};

class CreateColumnFamilyCommand : public LDBCommand {
 public:
  static std::string Name() { return "create_column_family"; }

  CreateColumnFamilyCommand(const std::vector<std::string>& params,
                            const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return false; }

 private:
  std::string new_cf_name_;
};

class GetCommand : public LDBCommand {
 public:
  static std::string Name() { return "get"; }

  GetCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class BatchPutCommand : public LDBCommand {
 public:
  static std::string Name() { return "batchput"; }

  BatchPutCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

  virtual Options PrepareOptionsForOpenDB() override;

 private:
  /**
   * The key-values to be inserted.
   */
  std::vector<std::pair<std::string, std::string>> key_values_;
};

class ScanCommand : public LDBCommand {
 public:
  static std::string Name() { return "scan"; }

  ScanCommand(const std::vector<std::string>& params,
              const std::map<std::string, std::string>& options,
              const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string start_key_;
  std::string end_key_;
  bool start_key_specified_;
  bool end_key_specified_;
  int max_keys_scanned_;
  bool no_value_;
};

class DeleteCommand : public LDBCommand {
 public:
  static std::string Name() { return "delete"; }

  DeleteCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class PutCommand : public LDBCommand {
 public:
  static std::string Name() { return "put"; }

  PutCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

  virtual Options PrepareOptionsForOpenDB() override;

 private:
  std::string key_;
  std::string value_;
};

/**
 * Command that starts up a REPL shell that allows
 * get/put/delete.
 */
class DBQuerierCommand : public LDBCommand {
 public:
  static std::string Name() { return "query"; }

  DBQuerierCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  static const char* HELP_CMD;
  static const char* GET_CMD;
  static const char* PUT_CMD;
  static const char* DELETE_CMD;
};

class CheckConsistencyCommand : public LDBCommand {
 public:
  static std::string Name() { return "checkconsistency"; }

  CheckConsistencyCommand(const std::vector<std::string>& params,
                          const std::map<std::string, std::string>& options,
                          const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

  static void Help(std::string& ret);
};

}  // namespace rocksdb

