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

#include "cache/lru_cache.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>

namespace rocksdb {


LRUCache::LRUCache(size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio) {
	capacity_ = capacity;
	num_shard_bits_ = num_shard_bits;
	strict_capacity_limit_ = strict_capacity_limit;
	high_pri_pool_ratio_ = high_pri_pool_ratio;

	insdb_lru_cache = insdb::NewLRUCache(capacity);
	if (insdb_lru_cache == nullptr)
	{
		throw std::runtime_error("Not supported API");
	}
}

LRUCache::~LRUCache() {
	delete insdb_lru_cache;
}

Status LRUCache::Insert(const Slice& key, void* value, size_t charge,
                      void (*deleter)(const Slice& key, void* value),
					  Cache::Handle** handle,
                      Priority priority)
{
	// deleter must be expanded to support caller context.
	return Status::NotSupported();
}
Cache::Handle* LRUCache::Lookup(const Slice& key, Statistics* stats) { return nullptr; }
bool LRUCache::Ref(Cache::Handle* handle) { return false; }
bool LRUCache::Release(Cache::Handle* handle, bool force_erase) { return false; }
void* LRUCache::Value(Cache::Handle* handle) { return nullptr; }
void LRUCache::Erase(const Slice& key) { }
uint64_t LRUCache::NewId() { return 0; }
void LRUCache::SetCapacity(size_t capacity) { capacity_ = capacity; }
void LRUCache::SetStrictCapacityLimit(bool strict_capacity_limit) { }
bool LRUCache::HasStrictCapacityLimit() const { return false; }
size_t LRUCache::GetCapacity() const { return capacity_; }
// TODO: returns real usage
size_t LRUCache::GetUsage() const { return 0; }
size_t LRUCache::GetUsage(Cache::Handle* handle) const { return 0; };
size_t LRUCache::GetPinnedUsage() const { return 0; }
void LRUCache::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                    bool thread_safe) { }
void LRUCache::EraseUnRefEntries() { }


// copied from sharded_cache.cc
int GetDefaultCacheShardBits(size_t capacity) {
  int num_shard_bits = 0;
  size_t min_shard_size = 512L * 1024L;  // Every shard is at least 512KB.
  size_t num_shards = capacity / min_shard_size;
  while (num_shards >>= 1) {
    if (++num_shard_bits >= 6) {
      // No more than 6.
      return num_shard_bits;
    }
  }
  return num_shard_bits;
}

std::shared_ptr<Cache> NewLRUCache(size_t capacity, int num_shard_bits,
                                   bool strict_capacity_limit,
                                   double high_pri_pool_ratio) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // invalid high_pri_pool_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<LRUCache>(capacity, num_shard_bits,
                                    strict_capacity_limit, high_pri_pool_ratio);
}

}  // namespace rocksdb
