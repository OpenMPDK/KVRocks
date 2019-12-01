//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE
#include "rocksdb/utilities/ldb_cmd.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include "port/port_dirent.h"
#include "db/version_set.h"
#include "rocksdb/cache.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/scoped_arena_iterator.h"
#include "tools/ldb_cmd_impl.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/filename.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "utilities/ttl/db_ttl_impl.h"

namespace rocksdb {

const std::string LDBCommand::ARG_DB = "db";
const std::string LDBCommand::ARG_PATH = "path";
const std::string LDBCommand::ARG_HEX = "hex";
const std::string LDBCommand::ARG_KEY_HEX = "key_hex";
const std::string LDBCommand::ARG_VALUE_HEX = "value_hex";
const std::string LDBCommand::ARG_CF_NAME = "column_family";
const std::string LDBCommand::ARG_TTL = "ttl";
const std::string LDBCommand::ARG_TTL_START = "start_time";
const std::string LDBCommand::ARG_TTL_END = "end_time";
const std::string LDBCommand::ARG_TIMESTAMP = "timestamp";
const std::string LDBCommand::ARG_TRY_LOAD_OPTIONS = "try_load_options";
const std::string LDBCommand::ARG_IGNORE_UNKNOWN_OPTIONS =
    "ignore_unknown_options";
const std::string LDBCommand::ARG_FROM = "from";
const std::string LDBCommand::ARG_TO = "to";
const std::string LDBCommand::ARG_MAX_KEYS = "max_keys";
const std::string LDBCommand::ARG_BLOOM_BITS = "bloom_bits";
const std::string LDBCommand::ARG_FIX_PREFIX_LEN = "fix_prefix_len";
const std::string LDBCommand::ARG_COMPRESSION_TYPE = "compression_type";
const std::string LDBCommand::ARG_COMPRESSION_MAX_DICT_BYTES =
    "compression_max_dict_bytes";
const std::string LDBCommand::ARG_BLOCK_SIZE = "block_size";
const std::string LDBCommand::ARG_AUTO_COMPACTION = "auto_compaction";
const std::string LDBCommand::ARG_DB_WRITE_BUFFER_SIZE = "db_write_buffer_size";
const std::string LDBCommand::ARG_WRITE_BUFFER_SIZE = "write_buffer_size";
const std::string LDBCommand::ARG_FILE_SIZE = "file_size";
const std::string LDBCommand::ARG_CREATE_IF_MISSING = "create_if_missing";
const std::string LDBCommand::ARG_NO_VALUE = "no_value";

const char* LDBCommand::DELIM = " ==> ";

LDBCommand* LDBCommand::InitFromCmdLineArgs(
    int argc, char** argv, const Options& options,
    const LDBOptions& ldb_options,
    const std::vector<ColumnFamilyDescriptor>* column_families) {
  std::vector<std::string> args;
  for (int i = 1; i < argc; i++) {
    args.push_back(argv[i]);
  }
  return InitFromCmdLineArgs(args, options, ldb_options, column_families,
                             SelectCommand);
}

/**
 * Parse the command-line arguments and create the appropriate LDBCommand2
 * instance.
 * The command line arguments must be in the following format:
 * ./ldb --db=PATH_TO_DB [--commonOpt1=commonOpt1Val] ..
 *        COMMAND <PARAM1> <PARAM2> ... [-cmdSpecificOpt1=cmdSpecificOpt1Val] ..
 * This is similar to the command line format used by HBaseClientTool.
 * Command name is not included in args.
 * Returns nullptr if the command-line cannot be parsed.
 */
LDBCommand* LDBCommand::InitFromCmdLineArgs(
    const std::vector<std::string>& args, const Options& options,
    const LDBOptions& ldb_options,
    const std::vector<ColumnFamilyDescriptor>* /*column_families*/,
    const std::function<LDBCommand*(const ParsedParams&)>& selector) {
  // --x=y command line arguments are added as x->y map entries in
  // parsed_params.option_map.
  //
  // Command-line arguments of the form --hex end up in this array as hex to
  // parsed_params.flags
  ParsedParams parsed_params;

  // Everything other than option_map and flags. Represents commands
  // and their parameters.  For eg: put key1 value1 go into this vector.
  std::vector<std::string> cmdTokens;

  const std::string OPTION_PREFIX = "--";

  for (const auto& arg : args) {
    if (arg[0] == '-' && arg[1] == '-') {
      std::vector<std::string> splits = StringSplit(arg, '=');
      // --option_name=option_value
      if (splits.size() == 2) {
        std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
        parsed_params.option_map[optionKey] = splits[1];
      } else if (splits.size() == 1) {
        // --flag_name
        std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
        parsed_params.flags.push_back(optionKey);
      } else {
        // --option_name=option_value, option_value contains '='
        std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
        parsed_params.option_map[optionKey] =
            arg.substr(splits[0].length() + 1);
      }
    } else {
      cmdTokens.push_back(arg);
    }
  }

  if (cmdTokens.size() < 1) {
    fprintf(stderr, "Command not specified!");
    return nullptr;
  }

  parsed_params.cmd = cmdTokens[0];
  parsed_params.cmd_params.assign(cmdTokens.begin() + 1, cmdTokens.end());

  LDBCommand* command = selector(parsed_params);

  if (command) {
    command->SetDBOptions(options);
    command->SetLDBOptions(ldb_options);
  }
  return command;
}

LDBCommand* LDBCommand::SelectCommand(const ParsedParams& parsed_params) {
  if (parsed_params.cmd == GetCommand::Name()) {
    return new GetCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  } else if (parsed_params.cmd == PutCommand::Name()) {
    return new PutCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  } else if (parsed_params.cmd == BatchPutCommand::Name()) {
    return new BatchPutCommand(parsed_params.cmd_params,
                               parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == ScanCommand::Name()) {
    return new ScanCommand(parsed_params.cmd_params, parsed_params.option_map,
                           parsed_params.flags);
  } else if (parsed_params.cmd == DeleteCommand::Name()) {
    return new DeleteCommand(parsed_params.cmd_params, parsed_params.option_map,
                             parsed_params.flags);
  } else if (parsed_params.cmd == DBQuerierCommand::Name()) {
    return new DBQuerierCommand(parsed_params.cmd_params,
                                parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == DBDumperCommand::Name()) {
    return new DBDumperCommand(parsed_params.cmd_params,
                               parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == DBLoaderCommand::Name()) {
    return new DBLoaderCommand(parsed_params.cmd_params,
                               parsed_params.option_map, parsed_params.flags);
  } else if (parsed_params.cmd == ListColumnFamiliesCommand::Name()) {
    return new ListColumnFamiliesCommand(parsed_params.cmd_params,
                                         parsed_params.option_map,
                                         parsed_params.flags);
  } else if (parsed_params.cmd == CreateColumnFamilyCommand::Name()) {
    return new CreateColumnFamilyCommand(parsed_params.cmd_params,
                                         parsed_params.option_map,
                                         parsed_params.flags);
  } else if (parsed_params.cmd == CheckConsistencyCommand::Name()) {
    return new CheckConsistencyCommand(parsed_params.cmd_params,
                                       parsed_params.option_map,
                                       parsed_params.flags);
  }
  return nullptr;
}

/* Run the command, and return the execute result. */
void LDBCommand::Run() {
  if (!exec_state_.IsNotStarted()) {
    return;
  }

  if (db_ == nullptr && !NoDBOpen()) {
    OpenDB();
    if (exec_state_.IsFailed() && try_load_options_) {
      // We don't always return if there is a failure because a WAL file or
      // manifest file can be given to "dump" command so we should continue.
      // --try_load_options is not valid in those cases.
      return;
    }
  }

  // We'll intentionally proceed even if the DB can't be opened because users
  // can also specify a filename, not just a directory.
  DoCommand();

  if (exec_state_.IsNotStarted()) {
    exec_state_ = LDBCommandExecuteResult::Succeed("");
  }

  if (db_ != nullptr) {
    CloseDB();
  }
}

LDBCommand::LDBCommand(const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags, bool is_read_only,
                       const std::vector<std::string>& valid_cmd_line_options)
    : db_(nullptr),
      db_ttl_(nullptr),
      is_read_only_(is_read_only),
      is_key_hex_(false),
      is_value_hex_(false),
      is_db_ttl_(false),
      timestamp_(false),
      try_load_options_(false),
      ignore_unknown_options_(false),
      create_if_missing_(false),
      option_map_(options),
      flags_(flags),
      valid_cmd_line_options_(valid_cmd_line_options) {
  std::map<std::string, std::string>::const_iterator itr = options.find(ARG_DB);
  if (itr != options.end()) {
    db_path_ = itr->second;
  }

  itr = options.find(ARG_CF_NAME);
  if (itr != options.end()) {
    column_family_name_ = itr->second;
  } else {
    column_family_name_ = kDefaultColumnFamilyName;
  }

  is_key_hex_ = IsKeyHex(options, flags);
  is_value_hex_ = IsValueHex(options, flags);
  is_db_ttl_ = IsFlagPresent(flags, ARG_TTL);
  timestamp_ = IsFlagPresent(flags, ARG_TIMESTAMP);
  try_load_options_ = IsFlagPresent(flags, ARG_TRY_LOAD_OPTIONS);
  ignore_unknown_options_ = IsFlagPresent(flags, ARG_IGNORE_UNKNOWN_OPTIONS);
}

void LDBCommand::OpenDB() {
  if (!create_if_missing_ && try_load_options_) {
    Status s = LoadLatestOptions(db_path_, Env::Default(), &options_,
                                 &column_families_, ignore_unknown_options_);
    if (!s.ok() && !s.IsNotFound()) {
      // Option file exists but load option file error.
      std::string msg = s.ToString();
      exec_state_ = LDBCommandExecuteResult::Failed(msg);
      db_ = nullptr;
      return;
    }
    if (options_.env->FileExists(options_.wal_dir).IsNotFound()) {
      options_.wal_dir = db_path_;
      fprintf(
          stderr,
          "wal_dir loaded from the option file doesn't exist. Ignore it.\n");
    }
  }
  options_ = PrepareOptionsForOpenDB();
  if (!exec_state_.IsNotStarted()) {
    return;
  }
  // Open the DB.
  Status st;
  std::vector<ColumnFamilyHandle*> handles_opened;
  if (is_db_ttl_) {
    // ldb doesn't yet support TTL DB with multiple column families
    if (!column_family_name_.empty() || !column_families_.empty()) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "ldb doesn't support TTL DB with multiple column families");
    }
    if (is_read_only_) {
      st = DBWithTTL::Open(options_, db_path_, &db_ttl_, 0, true);
    } else {
      st = DBWithTTL::Open(options_, db_path_, &db_ttl_);
    }
    db_ = db_ttl_;
  } else {
    if (column_families_.empty()) {
      // Try to figure out column family lists
      std::vector<std::string> cf_list;
      st = DB::ListColumnFamilies(DBOptions(), db_path_, &cf_list);
      // There is possible the DB doesn't exist yet, for "create if not
      // "existing case". The failure is ignored here. We rely on DB::Open()
      // to give us the correct error message for problem with opening
      // existing DB.
      if (st.ok() && cf_list.size() > 1) {
        // Ignore single column family DB.
        for (auto cf_name : cf_list) {
          column_families_.emplace_back(cf_name, options_);
        }
      }
    }
    if (is_read_only_) {
      if (column_families_.empty()) {
        st = DB::Open(options_, db_path_, &db_);
      } else {
        st = DB::Open(options_, db_path_, column_families_, &handles_opened,
                      &db_);
      }
    } else {
      if (column_families_.empty()) {
        st = DB::Open(options_, db_path_, &db_);
      } else {
        st = DB::Open(options_, db_path_, column_families_, &handles_opened,
                      &db_);
      }
    }
  }
  if (!st.ok()) {
    std::string msg = st.ToString();
    exec_state_ = LDBCommandExecuteResult::Failed(msg);
  } else if (!handles_opened.empty()) {
    assert(handles_opened.size() == column_families_.size());
    bool found_cf_name = false;
    for (size_t i = 0; i < handles_opened.size(); i++) {
      cf_handles_[column_families_[i].name] = handles_opened[i];
      if (column_family_name_ == column_families_[i].name) {
        found_cf_name = true;
      }
    }
    if (!found_cf_name) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Non-existing column family " + column_family_name_);
      CloseDB();
    }
  } else {
    // We successfully opened DB in single column family mode.
    assert(column_families_.empty());
    if (column_family_name_ != kDefaultColumnFamilyName) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Non-existing column family " + column_family_name_);
      CloseDB();
    }
  }
}

void LDBCommand::CloseDB() {
  if (db_ != nullptr) {
    for (auto& pair : cf_handles_) {
      delete pair.second;
    }
    delete db_;
    db_ = nullptr;
  }
}

ColumnFamilyHandle* LDBCommand::GetCfHandle() {
  if (!cf_handles_.empty()) {
    auto it = cf_handles_.find(column_family_name_);
    if (it == cf_handles_.end()) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          "Cannot find column family " + column_family_name_);
    } else {
      return it->second;
    }
  }
  return db_->DefaultColumnFamily();
}

std::vector<std::string> LDBCommand::BuildCmdLineOptions(
    std::vector<std::string> options) {
  std::vector<std::string> ret = {ARG_DB,
                                  ARG_BLOOM_BITS,
                                  ARG_BLOCK_SIZE,
                                  ARG_AUTO_COMPACTION,
                                  ARG_COMPRESSION_TYPE,
                                  ARG_COMPRESSION_MAX_DICT_BYTES,
                                  ARG_WRITE_BUFFER_SIZE,
                                  ARG_FILE_SIZE,
                                  ARG_FIX_PREFIX_LEN,
                                  ARG_TRY_LOAD_OPTIONS,
                                  ARG_IGNORE_UNKNOWN_OPTIONS,
                                  ARG_CF_NAME};
  ret.insert(ret.end(), options.begin(), options.end());
  return ret;
}

/**
 * Parses the specific integer option and fills in the value.
 * Returns true if the option is found.
 * Returns false if the option is not found or if there is an error parsing the
 * value.  If there is an error, the specified exec_state is also
 * updated.
 */
bool LDBCommand::ParseIntOption(
    const std::map<std::string, std::string>& /*options*/,
    const std::string& option, int& value,
    LDBCommandExecuteResult& exec_state) {
  std::map<std::string, std::string>::const_iterator itr =
      option_map_.find(option);
  if (itr != option_map_.end()) {
    try {
#if defined(CYGWIN)
      value = strtol(itr->second.c_str(), 0, 10);
#else
      value = std::stoi(itr->second);
#endif
      return true;
    } catch (const std::invalid_argument&) {
      exec_state =
          LDBCommandExecuteResult::Failed(option + " has an invalid value.");
    } catch (const std::out_of_range&) {
      exec_state = LDBCommandExecuteResult::Failed(
          option + " has a value out-of-range.");
    }
  }
  return false;
}

/**
 * Parses the specified option and fills in the value.
 * Returns true if the option is found.
 * Returns false otherwise.
 */
bool LDBCommand::ParseStringOption(
    const std::map<std::string, std::string>& /*options*/,
    const std::string& option, std::string* value) {
  auto itr = option_map_.find(option);
  if (itr != option_map_.end()) {
    *value = itr->second;
    return true;
  }
  return false;
}

Options LDBCommand::PrepareOptionsForOpenDB() {
  ColumnFamilyOptions* cf_opts;
  auto column_families_iter =
      std::find_if(column_families_.begin(), column_families_.end(),
                   [this](const ColumnFamilyDescriptor& cf_desc) {
                     return cf_desc.name == column_family_name_;
                   });
  if (column_families_iter != column_families_.end()) {
    cf_opts = &column_families_iter->options;
  } else {
    cf_opts = static_cast<ColumnFamilyOptions*>(&options_);
  }
  DBOptions* db_opts = static_cast<DBOptions*>(&options_);
  db_opts->create_if_missing = false;

  std::map<std::string, std::string>::const_iterator itr;

  itr = option_map_.find(ARG_AUTO_COMPACTION);
  if (itr != option_map_.end()) {
    cf_opts->disable_auto_compactions = !StringToBool(itr->second);
  }

  itr = option_map_.find(ARG_COMPRESSION_TYPE);
  if (itr != option_map_.end()) {
    std::string comp = itr->second;
    if (comp == "no") {
      cf_opts->compression = kNoCompression;
    } else if (comp == "snappy") {
      cf_opts->compression = kSnappyCompression;
    } else if (comp == "zlib") {
      cf_opts->compression = kZlibCompression;
    } else if (comp == "bzip2") {
      cf_opts->compression = kBZip2Compression;
    } else if (comp == "lz4") {
      cf_opts->compression = kLZ4Compression;
    } else if (comp == "lz4hc") {
      cf_opts->compression = kLZ4HCCompression;
    } else if (comp == "xpress") {
      cf_opts->compression = kXpressCompression;
    } else if (comp == "zstd") {
      cf_opts->compression = kZSTD;
    } else {
      // Unknown compression.
      exec_state_ =
          LDBCommandExecuteResult::Failed("Unknown compression level: " + comp);
    }
  }

  int compression_max_dict_bytes;
  if (ParseIntOption(option_map_, ARG_COMPRESSION_MAX_DICT_BYTES,
                     compression_max_dict_bytes, exec_state_)) {
    if (compression_max_dict_bytes >= 0) {
      cf_opts->compression_opts.max_dict_bytes = compression_max_dict_bytes;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_COMPRESSION_MAX_DICT_BYTES + " must be >= 0.");
    }
  }

  int db_write_buffer_size;
  if (ParseIntOption(option_map_, ARG_DB_WRITE_BUFFER_SIZE,
        db_write_buffer_size, exec_state_)) {
    if (db_write_buffer_size >= 0) {
      db_opts->db_write_buffer_size = db_write_buffer_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_DB_WRITE_BUFFER_SIZE +
                                                    " must be >= 0.");
    }
  }

  int write_buffer_size;
  if (ParseIntOption(option_map_, ARG_WRITE_BUFFER_SIZE, write_buffer_size,
        exec_state_)) {
    if (write_buffer_size > 0) {
      cf_opts->write_buffer_size = write_buffer_size;
    } else {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_WRITE_BUFFER_SIZE +
                                                    " must be > 0.");
    }
  }

  int file_size;
  if (ParseIntOption(option_map_, ARG_FILE_SIZE, file_size, exec_state_)) {
    if (file_size > 0) {
      cf_opts->target_file_size_base = file_size;
    } else {
      exec_state_ =
          LDBCommandExecuteResult::Failed(ARG_FILE_SIZE + " must be > 0.");
    }
  }

  if (db_opts->db_paths.size() == 0) {
    db_opts->db_paths.emplace_back(db_path_,
                                   std::numeric_limits<uint64_t>::max());
  }

  int fix_prefix_len;
  if (ParseIntOption(option_map_, ARG_FIX_PREFIX_LEN, fix_prefix_len,
                     exec_state_)) {
    if (fix_prefix_len > 0) {
      cf_opts->prefix_extractor.reset(
          NewFixedPrefixTransform(static_cast<size_t>(fix_prefix_len)));
    } else {
      exec_state_ =
          LDBCommandExecuteResult::Failed(ARG_FIX_PREFIX_LEN + " must be > 0.");
    }
  }
  // TODO(ajkr): this return value doesn't reflect the CF options changed, so
  // subcommands that rely on this won't see the effect of CF-related CLI args.
  // Such subcommands need to be changed to properly support CFs.
  return options_;
}

bool LDBCommand::ParseKeyValue(const std::string& line, std::string* key,
                               std::string* value, bool is_key_hex,
                               bool is_value_hex) {
  size_t pos = line.find(DELIM);
  if (pos != std::string::npos) {
    *key = line.substr(0, pos);
    *value = line.substr(pos + strlen(DELIM));
    if (is_key_hex) {
      *key = HexToString(*key);
    }
    if (is_value_hex) {
      *value = HexToString(*value);
    }
    return true;
  } else {
    return false;
  }
}

/**
 * Make sure that ONLY the command-line options and flags expected by this
 * command are specified on the command-line.  Extraneous options are usually
 * the result of user error.
 * Returns true if all checks pass.  Else returns false, and prints an
 * appropriate error msg to stderr.
 */
bool LDBCommand::ValidateCmdLineOptions() {
  for (std::map<std::string, std::string>::const_iterator itr =
           option_map_.begin();
       itr != option_map_.end(); ++itr) {
    if (std::find(valid_cmd_line_options_.begin(),
                  valid_cmd_line_options_.end(),
                  itr->first) == valid_cmd_line_options_.end()) {
      fprintf(stderr, "Invalid command-line option %s\n", itr->first.c_str());
      return false;
    }
  }

  for (std::vector<std::string>::const_iterator itr = flags_.begin();
       itr != flags_.end(); ++itr) {
    if (std::find(valid_cmd_line_options_.begin(),
                  valid_cmd_line_options_.end(),
                  *itr) == valid_cmd_line_options_.end()) {
      fprintf(stderr, "Invalid command-line flag %s\n", itr->c_str());
      return false;
    }
  }

  if (!NoDBOpen() && option_map_.find(ARG_DB) == option_map_.end() &&
      option_map_.find(ARG_PATH) == option_map_.end()) {
    fprintf(stderr, "Either %s or %s must be specified.\n", ARG_DB.c_str(),
            ARG_PATH.c_str());
    return false;
  }

  return true;
}

std::string LDBCommand::HexToString(const std::string& str) {
  std::string result;
  std::string::size_type len = str.length();
  if (len < 2 || str[0] != '0' || str[1] != 'x') {
    fprintf(stderr, "Invalid hex input %s.  Must start with 0x\n", str.c_str());
    throw "Invalid hex input";
  }
  if (!Slice(str.data() + 2, len - 2).DecodeHex(&result)) {
    throw "Invalid hex input";
  }
  return result;
}

std::string LDBCommand::StringToHex(const std::string& str) {
  std::string result("0x");
  result.append(Slice(str).ToString(true));
  return result;
}

std::string LDBCommand::PrintKeyValue(const std::string& key,
                                      const std::string& value, bool is_key_hex,
                                      bool is_value_hex) {
  std::string result;
  result.append(is_key_hex ? StringToHex(key) : key);
  result.append(DELIM);
  result.append(is_value_hex ? StringToHex(value) : value);
  return result;
}

std::string LDBCommand::PrintKeyValue(const std::string& key,
                                      const std::string& value, bool is_hex) {
  return PrintKeyValue(key, value, is_hex, is_hex);
}

std::string LDBCommand::HelpRangeCmdArgs() {
  std::ostringstream str_stream;
  str_stream << " ";
  str_stream << "[--" << ARG_FROM << "] ";
  str_stream << "[--" << ARG_TO << "] ";
  return str_stream.str();
}

bool LDBCommand::IsKeyHex(const std::map<std::string, std::string>& options,
                          const std::vector<std::string>& flags) {
  return (IsFlagPresent(flags, ARG_HEX) || IsFlagPresent(flags, ARG_KEY_HEX) ||
          ParseBooleanOption(options, ARG_HEX, false) ||
          ParseBooleanOption(options, ARG_KEY_HEX, false));
}

bool LDBCommand::IsValueHex(const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags) {
  return (IsFlagPresent(flags, ARG_HEX) ||
          IsFlagPresent(flags, ARG_VALUE_HEX) ||
          ParseBooleanOption(options, ARG_HEX, false) ||
          ParseBooleanOption(options, ARG_VALUE_HEX, false));
}

bool LDBCommand::ParseBooleanOption(
    const std::map<std::string, std::string>& options,
    const std::string& option, bool default_val) {
  std::map<std::string, std::string>::const_iterator itr = options.find(option);
  if (itr != options.end()) {
    std::string option_val = itr->second;
    return StringToBool(itr->second);
  }
  return default_val;
}

bool LDBCommand::StringToBool(std::string val) {
  std::transform(val.begin(), val.end(), val.begin(),
                 [](char ch) -> char { return (char)::tolower(ch); });

  if (val == "true") {
    return true;
  } else if (val == "false") {
    return false;
  } else {
    throw "Invalid value for boolean argument";
  }
}

// ----------------------------------------------------------------------------

const std::string DBLoaderCommand::ARG_DISABLE_WAL = "disable_wal";
const std::string DBLoaderCommand::ARG_BULK_LOAD = "bulk_load";
const std::string DBLoaderCommand::ARG_COMPACT = "compact";

DBLoaderCommand::DBLoaderCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, false,
          BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX, ARG_FROM,
                               ARG_TO, ARG_CREATE_IF_MISSING, ARG_DISABLE_WAL,
                               ARG_BULK_LOAD, ARG_COMPACT})),
      disable_wal_(false),
      bulk_load_(false),
      compact_(false) {
  create_if_missing_ = IsFlagPresent(flags, ARG_CREATE_IF_MISSING);
  disable_wal_ = IsFlagPresent(flags, ARG_DISABLE_WAL);
  bulk_load_ = IsFlagPresent(flags, ARG_BULK_LOAD);
  compact_ = IsFlagPresent(flags, ARG_COMPACT);
}

void DBLoaderCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DBLoaderCommand::Name());
  ret.append(" [--" + ARG_CREATE_IF_MISSING + "]");
  ret.append(" [--" + ARG_DISABLE_WAL + "]");
  ret.append(" [--" + ARG_BULK_LOAD + "]");
  ret.append(" [--" + ARG_COMPACT + "]");
  ret.append("\n");
}

Options DBLoaderCommand::PrepareOptionsForOpenDB() {
  Options opt = LDBCommand::PrepareOptionsForOpenDB();
  opt.create_if_missing = create_if_missing_;
  if (bulk_load_) {
    opt.PrepareForBulkLoad();
  }
  return opt;
}

void DBLoaderCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  WriteOptions write_options;
  if (disable_wal_) {
    write_options.disableWAL = true;
  }

  int bad_lines = 0;
  std::string line;
  // prefer ifstream getline performance vs that from std::cin istream
  std::ifstream ifs_stdin("/dev/stdin");
  std::istream* istream_p = ifs_stdin.is_open() ? &ifs_stdin : &std::cin;
  while (getline(*istream_p, line, '\n')) {
    std::string key;
    std::string value;
    if (ParseKeyValue(line, &key, &value, is_key_hex_, is_value_hex_)) {
      db_->Put(write_options, GetCfHandle(), Slice(key), Slice(value));
    } else if (0 == line.find("Keys in range:")) {
      // ignore this line
    } else if (0 == line.find("Created bg thread 0x")) {
      // ignore this line
    } else {
      bad_lines++;
    }
  }

  if (bad_lines > 0) {
    std::cout << "Warning: " << bad_lines << " bad lines ignored." << std::endl;
  }
  if (compact_) {
    db_->CompactRange(CompactRangeOptions(), GetCfHandle(), nullptr, nullptr);
  }
}

// ----------------------------------------------------------------------------

void ListColumnFamiliesCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ListColumnFamiliesCommand::Name());
  ret.append(" full_path_to_db_directory ");
  ret.append("\n");
}

ListColumnFamiliesCommand::ListColumnFamiliesCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false, {}) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "dbname must be specified for the list_column_families command");
  } else {
    dbname_ = params[0];
  }
}

void ListColumnFamiliesCommand::DoCommand() {
  std::vector<std::string> column_families;
  Status s = DB::ListColumnFamilies(DBOptions(), dbname_, &column_families);
  if (!s.ok()) {
    printf("Error in processing db %s %s\n", dbname_.c_str(),
           s.ToString().c_str());
  } else {
    printf("Column families in %s: \n{", dbname_.c_str());
    bool first = true;
    for (auto cf : column_families) {
      if (!first) {
        printf(", ");
      }
      first = false;
      printf("%s", cf.c_str());
    }
    printf("}\n");
  }
}

void CreateColumnFamilyCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(CreateColumnFamilyCommand::Name());
  ret.append(" --db=<db_path> <new_column_family_name>");
  ret.append("\n");
}

CreateColumnFamilyCommand::CreateColumnFamilyCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true, {ARG_DB}) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "new column family name must be specified");
  } else {
    new_cf_name_ = params[0];
  }
}

void CreateColumnFamilyCommand::DoCommand() {
  ColumnFamilyHandle* new_cf_handle = nullptr;
  Status st = db_->CreateColumnFamily(options_, new_cf_name_, &new_cf_handle);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Fail to create new column family: " + st.ToString());
  }
  delete new_cf_handle;
  CloseDB();
}

// ----------------------------------------------------------------------------

const std::string DBDumperCommand::ARG_COUNT_ONLY = "count_only";
const std::string DBDumperCommand::ARG_COUNT_DELIM = "count_delim";
const std::string DBDumperCommand::ARG_STATS = "stats";
const std::string DBDumperCommand::ARG_TTL_BUCKET = "bucket";

DBDumperCommand::DBDumperCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, true,
                 BuildCmdLineOptions(
                     {ARG_TTL, ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX, ARG_FROM,
                      ARG_TO, ARG_MAX_KEYS, ARG_COUNT_ONLY, ARG_COUNT_DELIM,
                      ARG_STATS, ARG_TTL_START, ARG_TTL_END, ARG_TTL_BUCKET,
                      ARG_TIMESTAMP, ARG_PATH})),
      null_from_(true),
      null_to_(true),
      max_keys_(-1),
      count_only_(false),
      count_delim_(false),
      print_stats_(false) {
  std::map<std::string, std::string>::const_iterator itr =
      options.find(ARG_FROM);
  if (itr != options.end()) {
    null_from_ = false;
    from_ = itr->second;
  }

  itr = options.find(ARG_TO);
  if (itr != options.end()) {
    null_to_ = false;
    to_ = itr->second;
  }

  itr = options.find(ARG_MAX_KEYS);
  if (itr != options.end()) {
    try {
#if defined(CYGWIN)
      max_keys_ = strtol(itr->second.c_str(), 0, 10);
#else
      max_keys_ = std::stoi(itr->second);
#endif
    } catch (const std::invalid_argument&) {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_MAX_KEYS +
                                                    " has an invalid value");
    } catch (const std::out_of_range&) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_MAX_KEYS + " has a value out-of-range");
    }
  }
  itr = options.find(ARG_COUNT_DELIM);
  if (itr != options.end()) {
    delim_ = itr->second;
    count_delim_ = true;
  } else {
    count_delim_ = IsFlagPresent(flags, ARG_COUNT_DELIM);
    delim_ = ".";
  }

  print_stats_ = IsFlagPresent(flags, ARG_STATS);
  count_only_ = IsFlagPresent(flags, ARG_COUNT_ONLY);

  if (is_key_hex_) {
    if (!null_from_) {
      from_ = HexToString(from_);
    }
    if (!null_to_) {
      to_ = HexToString(to_);
    }
  }

  itr = options.find(ARG_PATH);
  if (itr != options.end()) {
    path_ = itr->second;
    if (db_path_.empty()) {
      db_path_ = path_;
    }
  }
}

void DBDumperCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DBDumperCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append(" [--" + ARG_TTL + "]");
  ret.append(" [--" + ARG_MAX_KEYS + "=<N>]");
  ret.append(" [--" + ARG_TIMESTAMP + "]");
  ret.append(" [--" + ARG_COUNT_ONLY + "]");
  ret.append(" [--" + ARG_COUNT_DELIM + "=<char>]");
  ret.append(" [--" + ARG_STATS + "]");
  ret.append(" [--" + ARG_TTL_BUCKET + "=<N>]");
  ret.append(" [--" + ARG_TTL_START + "=<N>:- is inclusive]");
  ret.append(" [--" + ARG_TTL_END + "=<N>:- is exclusive]");
  ret.append(" [--" + ARG_PATH + "=<path_to_a_file>]");
  ret.append("\n");
}

/**
 * Handles two separate cases:
 *
 * 1) --db is specified - just dump the database.
 *
 * 2) --path is specified - determine based on file extension what dumping
 *    function to call. Please note that we intentionally use the extension
 *    and avoid probing the file contents under the assumption that renaming
 *    the files is not a supported scenario.
 *
 */
void DBDumperCommand::DoCommand() {
  if (!db_) {
    assert(!path_.empty());
    std::string fileName = GetFileNameFromPath(path_);
    uint64_t number;
    FileType type;

    exec_state_ = LDBCommandExecuteResult::Succeed("");

    if (!ParseFileName(fileName, &number, &type)) {
      exec_state_ =
          LDBCommandExecuteResult::Failed("Can't parse file type: " + path_);
      return;
    }

    switch (type) {
      case kLogFile:
        // TODO(myabandeh): allow configuring is_write_commited
          printf("dumpWalFile");
        // DumpWalFile(options_, path_, /* print_header_ */ true,
        //             /* print_values_ */ true, true /* is_write_commited */,
        //             &exec_state_);
        break;
      case kTableFile:
        printf("dumpSstFile");
        // DumpSstFile(options_, path_, is_key_hex_, /* show_properties */ true);
        break;
      // case kDescriptorFile:
      //   DumpManifestFile(options_, path_, /* verbose_ */ false, is_key_hex_,
      //                    /*  json_ */ false);
      //   break;
      default:
        exec_state_ = LDBCommandExecuteResult::Failed(
            "File type not supported: " + path_);
        break;
    }

  } else {
    DoDumpCommand();
  }
}

void DBDumperCommand::DoDumpCommand() {
  assert(nullptr != db_);
  assert(path_.empty());

  // Parse command line args
  uint64_t count = 0;
  if (print_stats_) {
    std::string stats;
    if (db_->GetProperty("rocksdb.stats", &stats)) {
      fprintf(stdout, "%s\n", stats.c_str());
    }
  }

  // Setup key iterator
  ReadOptions scan_read_opts;
  scan_read_opts.total_order_seek = true;
  Iterator* iter = db_->NewIterator(scan_read_opts, GetCfHandle());
  Status st = iter->status();
  if (!st.ok()) {
    exec_state_ =
        LDBCommandExecuteResult::Failed("Iterator error." + st.ToString());
  }

  if (!null_from_) {
    iter->Seek(from_);
  } else {
    iter->SeekToFirst();
  }

  int max_keys = max_keys_;
  int ttl_start;
  if (!ParseIntOption(option_map_, ARG_TTL_START, ttl_start, exec_state_)) {
    ttl_start = DBWithTTLImpl::kMinTimestamp;  // TTL introduction time
  }
  int ttl_end;
  if (!ParseIntOption(option_map_, ARG_TTL_END, ttl_end, exec_state_)) {
    ttl_end = DBWithTTLImpl::kMaxTimestamp;  // Max time allowed by TTL feature
  }
  if (ttl_end < ttl_start) {
    fprintf(stderr, "Error: End time can't be less than start time\n");
    delete iter;
    return;
  }
  int time_range = ttl_end - ttl_start;
  int bucket_size;
  if (!ParseIntOption(option_map_, ARG_TTL_BUCKET, bucket_size, exec_state_) ||
      bucket_size <= 0) {
    bucket_size = time_range; // Will have just 1 bucket by default
  }
  // cretaing variables for row count of each type
  std::string rtype1, rtype2, row, val;
  rtype2 = "";
  uint64_t c = 0;
  uint64_t s1 = 0, s2 = 0;

  // At this point, bucket_size=0 => time_range=0
  int num_buckets = (bucket_size >= time_range)
                        ? 1
                        : ((time_range + bucket_size - 1) / bucket_size);
  std::vector<uint64_t> bucket_counts(num_buckets, 0);
  HistogramImpl vsize_hist;

  for (; iter->Valid(); iter->Next()) {
    int rawtime = 0;
    // If end marker was specified, we stop before it
    if (!null_to_ && (iter->key().ToString() >= to_))
      break;
    // Terminate if maximum number of keys have been dumped
    if (max_keys == 0)
      break;
    if (max_keys > 0) {
      --max_keys;
    }
    ++count;
    if (count_delim_) {
      rtype1 = "";
      row = iter->key().ToString();
      val = iter->value().ToString();
      s1 = row.size()+val.size();
      for (int j = 0; row[j] != delim_[0] && row[j] != '\0'; j++)
        rtype1 += row[j];
      if (rtype2.compare("") && rtype2.compare(rtype1) != 0) {
        fprintf(stdout, "%s => count:%" PRIu64 "\tsize:%" PRIu64 "\n",
                rtype2.c_str(), c, s2);
        c = 1;
        s2 = s1;
        rtype2 = rtype1;
      } else {
          c++;
          s2 += s1;
          rtype2 = rtype1;
      }
    }

    if (count_only_) {
      vsize_hist.Add(iter->value().size());
    }

    if (!count_only_ && !count_delim_) {
      // if (is_db_ttl_ && timestamp_) {
      //   fprintf(stdout, "%s ", ReadableTime(rawtime).c_str());
      // }
      std::string str =
          PrintKeyValue(iter->key().ToString(), iter->value().ToString(),
                        is_key_hex_, is_value_hex_);
      fprintf(stdout, "%s\n", str.c_str());
    }
  }

  if (num_buckets > 1 && is_db_ttl_) {
    // PrintBucketCounts(bucket_counts, ttl_start, ttl_end, bucket_size,
    //                   num_buckets);
  } else if (count_delim_) {
    fprintf(stdout, "%s => count:%" PRIu64 "\tsize:%" PRIu64 "\n",
            rtype2.c_str(), c, s2);
  } else {
    fprintf(stdout, "Keys in range: %" PRIu64 "\n", count);
  }

  if (count_only_) {
    fprintf(stdout, "Value size distribution: \n");
    fprintf(stdout, "%s\n", vsize_hist.ToString().c_str());
  }
  // Clean up
  delete iter;
}

// ----------------------------------------------------------------------------

GetCommand::GetCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, true,
          BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "<key> must be specified for the get command");
  } else {
    key_ = params.at(0);
  }

  if (is_key_hex_) {
    key_ = HexToString(key_);
  }
}

void GetCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(GetCommand::Name());
  ret.append(" <key>");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void GetCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  std::string value;
  Status st = db_->Get(ReadOptions(), GetCfHandle(), key_, &value);
  if (st.ok()) {
    fprintf(stdout, "%s\n",
              (is_value_hex_ ? StringToHex(value) : value).c_str());
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

// ----------------------------------------------------------------------------

BatchPutCommand::BatchPutCommand(
    const std::vector<std::string>& params,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_CREATE_IF_MISSING})) {
  if (params.size() < 2) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "At least one <key> <value> pair must be specified batchput.");
  } else if (params.size() % 2 != 0) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "Equal number of <key>s and <value>s must be specified for batchput.");
  } else {
    for (size_t i = 0; i < params.size(); i += 2) {
      std::string key = params.at(i);
      std::string value = params.at(i + 1);
      key_values_.push_back(std::pair<std::string, std::string>(
          is_key_hex_ ? HexToString(key) : key,
          is_value_hex_ ? HexToString(value) : value));
    }
  }
  create_if_missing_ = IsFlagPresent(flags_, ARG_CREATE_IF_MISSING);
}

void BatchPutCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(BatchPutCommand::Name());
  ret.append(" <key> <value> [<key> <value>] [..]");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void BatchPutCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  WriteBatch batch;

  for (std::vector<std::pair<std::string, std::string>>::const_iterator itr =
           key_values_.begin();
       itr != key_values_.end(); ++itr) {
    batch.Put(GetCfHandle(), itr->first, itr->second);
  }
  Status st = db_->Write(WriteOptions(), &batch);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

Options BatchPutCommand::PrepareOptionsForOpenDB() {
  Options opt = LDBCommand::PrepareOptionsForOpenDB();
  opt.create_if_missing = create_if_missing_;
  return opt;
}

// ----------------------------------------------------------------------------

ScanCommand::ScanCommand(const std::vector<std::string>& /*params*/,
                         const std::map<std::string, std::string>& options,
                         const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, true,
          BuildCmdLineOptions({ARG_TTL, ARG_NO_VALUE, ARG_HEX, ARG_KEY_HEX,
                               ARG_TO, ARG_VALUE_HEX, ARG_FROM, ARG_TIMESTAMP,
                               ARG_MAX_KEYS, ARG_TTL_START, ARG_TTL_END})),
      start_key_specified_(false),
      end_key_specified_(false),
      max_keys_scanned_(-1),
      no_value_(false) {
  std::map<std::string, std::string>::const_iterator itr =
      options.find(ARG_FROM);
  if (itr != options.end()) {
    start_key_ = itr->second;
    if (is_key_hex_) {
      start_key_ = HexToString(start_key_);
    }
    start_key_specified_ = true;
  }
  itr = options.find(ARG_TO);
  if (itr != options.end()) {
    end_key_ = itr->second;
    if (is_key_hex_) {
      end_key_ = HexToString(end_key_);
    }
    end_key_specified_ = true;
  }

  std::vector<std::string>::const_iterator vitr =
      std::find(flags.begin(), flags.end(), ARG_NO_VALUE);
  if (vitr != flags.end()) {
    no_value_ = true;
  }

  itr = options.find(ARG_MAX_KEYS);
  if (itr != options.end()) {
    try {
#if defined(CYGWIN)
      max_keys_scanned_ = strtol(itr->second.c_str(), 0, 10);
#else
      max_keys_scanned_ = std::stoi(itr->second);
#endif
    } catch (const std::invalid_argument&) {
      exec_state_ = LDBCommandExecuteResult::Failed(ARG_MAX_KEYS +
                                                    " has an invalid value");
    } catch (const std::out_of_range&) {
      exec_state_ = LDBCommandExecuteResult::Failed(
          ARG_MAX_KEYS + " has a value out-of-range");
    }
  }
}

void ScanCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(ScanCommand::Name());
  ret.append(HelpRangeCmdArgs());
  ret.append(" [--" + ARG_TTL + "]");
  ret.append(" [--" + ARG_TIMESTAMP + "]");
  ret.append(" [--" + ARG_MAX_KEYS + "=<N>q] ");
  ret.append(" [--" + ARG_TTL_START + "=<N>:- is inclusive]");
  ret.append(" [--" + ARG_TTL_END + "=<N>:- is exclusive]");
  ret.append(" [--" + ARG_NO_VALUE + "]");
  ret.append("\n");
}

void ScanCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  int num_keys_scanned = 0;
  ReadOptions scan_read_opts;
  scan_read_opts.total_order_seek = true;
  Iterator* it = db_->NewIterator(scan_read_opts, GetCfHandle());
  if (start_key_specified_) {
    it->Seek(start_key_);
  } else {
    it->SeekToFirst();
  }
  int ttl_start;
  if (!ParseIntOption(option_map_, ARG_TTL_START, ttl_start, exec_state_)) {
    ttl_start = DBWithTTLImpl::kMinTimestamp;  // TTL introduction time
  }
  int ttl_end;
  if (!ParseIntOption(option_map_, ARG_TTL_END, ttl_end, exec_state_)) {
    ttl_end = DBWithTTLImpl::kMaxTimestamp;  // Max time allowed by TTL feature
  }
  if (ttl_end < ttl_start) {
    fprintf(stderr, "Error: End time can't be less than start time\n");
    delete it;
    return;
  }
  for ( ;
        it->Valid() && (!end_key_specified_ || it->key().ToString() < end_key_);
        it->Next()) {
    Slice key_slice = it->key();

    std::string formatted_key;
    if (is_key_hex_) {
      formatted_key = "0x" + key_slice.ToString(true /* hex */);
      key_slice = formatted_key;
    } else if (ldb_options_.key_formatter) {
      formatted_key = ldb_options_.key_formatter->Format(key_slice);
      key_slice = formatted_key;
    }

    if (no_value_) {
      fprintf(stdout, "%.*s\n", static_cast<int>(key_slice.size()),
              key_slice.data());
    } else {
      Slice val_slice = it->value();
      std::string formatted_value;
      if (is_value_hex_) {
        formatted_value = "0x" + val_slice.ToString(true /* hex */);
        val_slice = formatted_value;
      }
      fprintf(stdout, "%.*s : %.*s\n", static_cast<int>(key_slice.size()),
              key_slice.data(), static_cast<int>(val_slice.size()),
              val_slice.data());
    }

    num_keys_scanned++;
    if (max_keys_scanned_ >= 0 && num_keys_scanned >= max_keys_scanned_) {
      break;
    }
  }
  if (!it->status().ok()) {  // Check for any errors found during the scan
    exec_state_ = LDBCommandExecuteResult::Failed(it->status().ToString());
  }
  delete it;
}

// ----------------------------------------------------------------------------

DeleteCommand::DeleteCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false,
                 BuildCmdLineOptions({ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {
  if (params.size() != 1) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "KEY must be specified for the delete command");
  } else {
    key_ = params.at(0);
    if (is_key_hex_) {
      key_ = HexToString(key_);
    }
  }
}

void DeleteCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DeleteCommand::Name() + " <key>");
  ret.append("\n");
}

void DeleteCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Status st = db_->Delete(WriteOptions(), GetCfHandle(), key_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

PutCommand::PutCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false,
                 BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX,
                                      ARG_VALUE_HEX, ARG_CREATE_IF_MISSING})) {
  if (params.size() != 2) {
    exec_state_ = LDBCommandExecuteResult::Failed(
        "<key> and <value> must be specified for the put command");
  } else {
    key_ = params.at(0);
    value_ = params.at(1);
  }

  if (is_key_hex_) {
    key_ = HexToString(key_);
  }

  if (is_value_hex_) {
    value_ = HexToString(value_);
  }
  create_if_missing_ = IsFlagPresent(flags_, ARG_CREATE_IF_MISSING);
}

void PutCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(PutCommand::Name());
  ret.append(" <key> <value> ");
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
}

void PutCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }
  Status st = db_->Put(WriteOptions(), GetCfHandle(), key_, value_);
  if (st.ok()) {
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}

Options PutCommand::PrepareOptionsForOpenDB() {
  Options opt = LDBCommand::PrepareOptionsForOpenDB();
  opt.create_if_missing = create_if_missing_;
  return opt;
}

// ----------------------------------------------------------------------------

const char* DBQuerierCommand::HELP_CMD = "help";
const char* DBQuerierCommand::GET_CMD = "get";
const char* DBQuerierCommand::PUT_CMD = "put";
const char* DBQuerierCommand::DELETE_CMD = "delete";

DBQuerierCommand::DBQuerierCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(
          options, flags, false,
          BuildCmdLineOptions({ARG_TTL, ARG_HEX, ARG_KEY_HEX, ARG_VALUE_HEX})) {
}

void DBQuerierCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(DBQuerierCommand::Name());
  ret.append(" [--" + ARG_TTL + "]");
  ret.append("\n");
  ret.append("    Starts a REPL shell.  Type help for list of available "
             "commands.");
  ret.append("\n");
}

void DBQuerierCommand::DoCommand() {
  if (!db_) {
    assert(GetExecuteState().IsFailed());
    return;
  }

  ReadOptions read_options;
  WriteOptions write_options;

  std::string line;
  std::string key;
  std::string value;
  while (getline(std::cin, line, '\n')) {
    // Parse line into std::vector<std::string>
    std::vector<std::string> tokens;
    size_t pos = 0;
    while (true) {
      size_t pos2 = line.find(' ', pos);
      if (pos2 == std::string::npos) {
        break;
      }
      tokens.push_back(line.substr(pos, pos2-pos));
      pos = pos2 + 1;
    }
    tokens.push_back(line.substr(pos));

    const std::string& cmd = tokens[0];

    if (cmd == HELP_CMD) {
      fprintf(stdout,
              "get <key>\n"
              "put <key> <value>\n"
              "delete <key>\n");
    } else if (cmd == DELETE_CMD && tokens.size() == 2) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      db_->Delete(write_options, GetCfHandle(), Slice(key));
      fprintf(stdout, "Successfully deleted %s\n", tokens[1].c_str());
    } else if (cmd == PUT_CMD && tokens.size() == 3) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      value = (is_value_hex_ ? HexToString(tokens[2]) : tokens[2]);
      db_->Put(write_options, GetCfHandle(), Slice(key), Slice(value));
      fprintf(stdout, "Successfully put %s %s\n",
              tokens[1].c_str(), tokens[2].c_str());
    } else if (cmd == GET_CMD && tokens.size() == 2) {
      key = (is_key_hex_ ? HexToString(tokens[1]) : tokens[1]);
      if (db_->Get(read_options, GetCfHandle(), Slice(key), &value).ok()) {
        fprintf(stdout, "%s\n", PrintKeyValue(key, value,
              is_key_hex_, is_value_hex_).c_str());
      } else {
        fprintf(stdout, "Not found %s\n", tokens[1].c_str());
      }
    } else {
      fprintf(stdout, "Unknown command %s\n", line.c_str());
    }
  }
}

// ----------------------------------------------------------------------------

CheckConsistencyCommand::CheckConsistencyCommand(
    const std::vector<std::string>& /*params*/,
    const std::map<std::string, std::string>& options,
    const std::vector<std::string>& flags)
    : LDBCommand(options, flags, false, BuildCmdLineOptions({})) {}

void CheckConsistencyCommand::Help(std::string& ret) {
  ret.append("  ");
  ret.append(CheckConsistencyCommand::Name());
  ret.append("\n");
}

void CheckConsistencyCommand::DoCommand() {
  Options opt = PrepareOptionsForOpenDB();
  opt.paranoid_checks = true;
  if (!exec_state_.IsNotStarted()) {
    return;
  }
  DB* db;
  // Status st = DB::OpenForReadOnly(opt, db_path_, &db, false);
  Status st = DB::Open(opt, db_path_, &db);
  if (st.ok()) {
    delete db;
    fprintf(stdout, "OK\n");
  } else {
    exec_state_ = LDBCommandExecuteResult::Failed(st.ToString());
  }
}
}   // namespace rocksdb
#endif  // ROCKSDB_LITE

