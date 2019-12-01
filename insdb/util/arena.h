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


#ifndef STORAGE_INSDB_UTIL_ARENA_H_
#define STORAGE_INSDB_UTIL_ARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/mman.h>
#include "port/port.h"
#include "insdb/slice.h"
#include "db/global_stats.h"
#define ALLOC_FROM_FREED
#define ALLOC_FROM_FREED_DEBUG
namespace insdb {

class Arena {
    const static uint32_t arena_default_size = 1024*1024*2;
    const static uint32_t arena_resrv = 44;
    struct Arena_hdr {
       uint8_t resrv_[arena_resrv];
        /*keymap info */
        uint32_t max_height_;
        uint32_t begin_key_offset_;
        uint32_t head_keynode_offset_;
        uint64_t count_;
        /* arena manage ment */
        uint32_t allocated_bytes_;
        uint32_t freed_bytes_; 
        /* initial version with single free list */
        uint32_t freed_list_offset_;
    };
 public:
  Arena() : arena_size_(arena_default_size), hdr_(nullptr) {
      char* base = (char*) mmap(NULL, arena_default_size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
      if (base == MAP_FAILED) abort();
      hdr_ = (Arena_hdr *)base;
      hdr_->allocated_bytes_ = sizeof(Arena_hdr);
#ifdef ALLOC_FROM_FREED_DEBUG
        alloc_from_freed_ = 0;
        alloc_from_freed_fragment_ = 0;
#endif
#ifdef INSDB_GLOBAL_STATS
        g_new_arena_cnt++;
#endif
  }
  Arena(uint32_t size) : arena_size_(size), hdr_(nullptr) {
      char* base = (char*) mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
      if (base == MAP_FAILED) abort();
      hdr_ = (Arena_hdr *)base;
      hdr_->allocated_bytes_ = sizeof(Arena_hdr);
#ifdef ALLOC_FROM_FREED_DEBUG
        alloc_from_freed_ = 0;
        alloc_from_freed_fragment_ = 0;
#endif
#ifdef INSDB_GLOBAL_STATS
        g_new_arena_cnt++;
#endif
  }

  ~Arena() {
      if (hdr_) munmap((void*)hdr_, arena_size_);
#ifdef INSDB_GLOBAL_STATS
        g_del_arena_cnt++;
#endif
  }

  Slice GetReserved() {
      return Slice((char*)hdr_->resrv_, arena_resrv); 
  }

  void DumpState () {
      printf("-------------------------------------------------\n");
      printf("Memory Size %d\n", arena_size_);
      printf("Memory addr %p\n", (char*)hdr_);
      printf("Table Max Height %d\n", hdr_->max_height_);
      printf("Table Node count %ld\n", hdr_->count_);
      printf("Table Head node offset 0x%x\n", hdr_->head_keynode_offset_);
      printf("Table Begin Key  offset 0x%x\n", hdr_->begin_key_offset_);
      printf("Table allocated bytes %d\n", hdr_->allocated_bytes_);
      printf("Table freed bytes %d\n", hdr_->freed_bytes_);
#ifdef ALLOC_FROM_FREED_DEBUG
      printf("Table alloc from freed bytes %d\n", alloc_from_freed_);
      printf("Table alllc from freed fragment bytes %d\n", alloc_from_freed_fragment_);
#endif
      printf("-------------------------------------------------\n");
  }

  void SetMaxHeight(uint32_t val) {
      hdr_->max_height_ = val;
  }
  uint32_t GetMaxHeight() {
      return hdr_->max_height_;
  }

  void SetBeginKeyOffset(uint32_t val) {
      hdr_->begin_key_offset_ = val;
  }
  uint32_t GetBeginKeyOffset() {
      return hdr_->begin_key_offset_;
  }

  void SetHeadNodeOffset(uint32_t val) {
      hdr_->head_keynode_offset_ = val;
  }
  uint32_t GetHeadNodeOffset() {
      return hdr_->head_keynode_offset_;
  }

  void IncNodeCount() {
      hdr_->count_++;
  }
  uint64_t DecNodeCount() {
      hdr_->count_--;
  }
  uint64_t GetNodeCount() {
      return hdr_->count_;
  }
  void SetNodeCount(uint64_t count) {
      hdr_->count_ = count;
  }
  uint32_t PreAllocate(uint32_t &bytes);
  uint32_t Allocate(uint32_t &bytes);
  void  DeAllocate(uint32_t addr_offset);
  void DumpFreeList();

  uint32_t AllocatedMemorySize() const {
      return (hdr_->allocated_bytes_);
  }

#ifdef ALLOC_FROM_FREED_DEBUG
  uint32_t AllocatedFromFreed() const {
      return alloc_from_freed_;
  }
#endif
  uint32_t FreedMemorySize() const {
      return (hdr_->freed_bytes_);
  }

  uint32_t RemainMemorySize() const {
      return (arena_size_ - hdr_->allocated_bytes_);
  }

  float MemoryUtilization() const {
      /*
        (hdr_->allocated_bytes - hdr_->freed_bytes);
       */
      return (hdr_->allocated_bytes_ * 100)/arena_size_;
  }

  char* GetBaseAddr() {
      return (char*)hdr_;
  }

  uint32_t GetBufferSize() {
      return arena_size_;
  }
  void SetBufferSize(uint32_t val) {
      if (val < hdr_->allocated_bytes_) abort();
      arena_size_ = val;
  }
 private:
  // Allocation state
  uint32_t arena_size_;
  Arena_hdr* hdr_;
#ifdef ALLOC_FROM_FREED_DEBUG
  uint32_t alloc_from_freed_;
  uint32_t alloc_from_freed_fragment_;
#endif

  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

}  // namespace insdb

#endif  // STORAGE_INSDB_UTIL_ARENA_H_
