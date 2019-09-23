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


#include "insdb/options.h"

#include "insdb/comparator.h"
#include "insdb/env.h"

namespace insdb {

    uint64_t cache_size_change = 1024;
    Options::Options()
    : comparator(BytewiseComparator()),
      merge_operator(nullptr),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(4<<20),
      max_open_files(1000),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      max_file_size(2<<20),
      compression(kSnappyCompression),
      reuse_logs(false),
      prefix_detection(false),
      filter_policy(NULL),
      max_cache_size(2*1024*1024*1024UL),
      num_write_worker(3),
      num_column_count(1),
      max_request_size(32*1024),
      align_size(4),
      //align_size(4096),
      slowdown_trigger(64*1024),
      max_key_size(128),
      max_table_size(512*1024),
      max_uval_prefetched_size(200000000UL),
      max_uval_iter_buffered_size(2*1024*1024*1024UL),
      flush_threshold(1024*1024),
      /*Do not increase min_update_cnt_table_flush too high, it may cause out of memory*/
      min_update_cnt_table_flush(64*1024),
      kv_ssd("/dev/nvme0n1"),
      approx_key_size(16),
      approx_val_size(4096),
      iter_prefetch_hint(1),
      random_prefetch_hint(1),
      split_prefix_bits(0),
      AddUserKey(nullptr),
      AddUserKeyContext(nullptr),
      Flush(nullptr),
      FlushContext(nullptr)
      {
}

}  // namespace insdb
