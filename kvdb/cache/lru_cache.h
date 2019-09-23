//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <string>
#include "insdb/cache.h"
#include "rocksdb/cache.h"
#include "port/port.h"

namespace rocksdb {

class LRUCache : public Cache {
 public:
  LRUCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
           double high_pri_pool_ratio);
  virtual ~LRUCache();
  virtual const char* Name() const override { return "LRUCache"; }
  virtual Status Insert(const Slice& key, void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value),
                        Handle** handle = nullptr,
                        Priority priority = Priority::LOW) override;
  virtual Handle* Lookup(const Slice& key, Statistics* stats = nullptr);
  virtual bool Ref(Handle* handle) override;
  virtual bool Release(Handle* handle, bool force_erase = false) override;
  virtual void* Value(Handle* handle) override;
  virtual void Erase(const Slice& key) override;
  virtual uint64_t NewId() override;
  virtual void SetCapacity(size_t capacity);
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit);
  virtual bool HasStrictCapacityLimit() const override;
  virtual size_t GetCapacity() const override;
  virtual size_t GetUsage() const override;
  virtual size_t GetUsage(Handle* handle) const override;
  virtual size_t GetPinnedUsage() const override;
  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;
  virtual void EraseUnRefEntries() override;

 private:
  size_t capacity_;
  int num_shard_bits_;
  bool strict_capacity_limit_;
  double high_pri_pool_ratio_;
  insdb::Cache *insdb_lru_cache;
};

}  // namespace rocksdb
