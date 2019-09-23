//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE


#include <regex>
#include <utility>
#include <vector>

#include "rocksdb/persistent_cache.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {

Status NewPersistentCache(Env* const env, const std::string& path,
                          const uint64_t size,
                          const std::shared_ptr<Logger>& log,
                          const bool optimized_for_nvm,
                          std::shared_ptr<PersistentCache>* cache) {
    throw std::runtime_error("Not supported API");
}  // namespace rocksdb
}
#endif  // ifndef ROCKSDB_LITE
