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


#include "util/arena.h"

#include "util/random.h"
#include "util/testharness.h"

namespace insdb {

class ArenaTest { };

TEST(ArenaTest, Empty) {
  Arena arena;
}

TEST(ArenaTest, Simple) {
  std::vector<std::pair<uint32_t, uint32_t> > allocated;
  Arena arena;
  const int N = 1000;
  size_t bytes = 0;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    uint32_t s;
    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = rnd.OneIn(4000) ? rnd.Uniform(6000) :
          (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    uint32_t ur;
    if (rnd.OneIn(10)) {
      ur = arena.Allocate(s);
    } else {
      ur = arena.Allocate(s);
    }
    if (ur) {
        r = arena.GetBaseAddr() + ur;
        for (size_t b = 8; b < s; b++) {
            // Fill the "i"th allocation with a known bit pattern
            r[b] = i % 256;
        }
        bytes += s;
        allocated.push_back(std::make_pair(s, ur));
        ASSERT_GE(arena.AllocatedMemorySize(), bytes);
    }
  }
  for (size_t i = 0; i < allocated.size(); i++) {
    uint32_t num_bytes = allocated[i].first;
    const char* p = arena.GetBaseAddr() +  allocated[i].second;
    for (size_t b = 8; b < num_bytes; b++) {
      // Check the "i"th allocation for the known bit pattern
      ASSERT_EQ(int(p[b]) & 0xff, i % 256);
    }
  }
}


TEST(ArenaTest, Default) {
  std::vector<std::pair<uint32_t, uint32_t> > allocated;
  std::vector<std::pair<uint32_t, uint32_t> > allocated2;
  Arena arena;
  const int N = 1000000;
  uint32_t bytes = 0;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    uint32_t s;
    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = rnd.OneIn(4000) ? rnd.Uniform(6000) :
          (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    uint32_t ur;
    if (rnd.OneIn(10)) {
      ur = arena.Allocate(s);
    } else {
      ur = arena.Allocate(s);
    }
    if (ur) {
        r = arena.GetBaseAddr() + ur;
        for (size_t b = 8; b < s; b++) {
            // Fill the "i"th allocation with a known bit pattern
            r[b] = i % 256;
        }
        bytes += s;
        allocated.push_back(std::make_pair(s, ur));
    } else abort();
  }
  printf("Step 1 Allocated Item (%ld) Allocated arena size %d\n", allocated.size(),  arena.AllocatedMemorySize());
  for (size_t i = 0; i < allocated.size()/2; i++) {
    uint32_t num_bytes = allocated[i].first;
    uint32_t p = allocated[i].second;
    arena.DeAllocate(p);
  }
  printf("Step 1 Free half of item %d\n", arena.FreedMemorySize());
  for (int i = 0; i < N; i++) {
    uint32_t s;
    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = rnd.OneIn(4000) ? rnd.Uniform(6000) :
          (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    uint32_t ur;
    if (rnd.OneIn(10)) {
      ur = arena.Allocate(s);
    } else {
      ur = arena.Allocate(s);
    }
    if (ur) {
        r = arena.GetBaseAddr() + ur;
        for (size_t b = 8; b < s; b++) {
            // Fill the "i"th allocation with a known bit pattern
            r[b] = i % 256;
        }
        bytes += s;
        allocated2.push_back(std::make_pair(s, ur));
    } else abort();
  }
#ifdef ALLOC_FROM_FREED_DEBUG  
  printf("Step 2 Allocated Item (%ld) Allocated arena size %d Alloc from freed memory %d\n", allocated2.size(), arena.AllocatedMemorySize(), arena.AllocatedFromFreed());
#else
  printf("Step 2 Allocated Item (%ld) Allocated arena size %d\n", allocated2.size(), arena.AllocatedMemorySize());
#endif
  printf("Step 2 Freeed arena size %d\n", arena.FreedMemorySize());
  for (size_t i = 0; i < allocated2.size(); i++) {
    uint32_t num_bytes = allocated2[i].first;
    uint32_t p = allocated2[i].second;
    arena.DeAllocate(p);
  }
  printf("Step 2 Freed all second step arean size %d\n", arena.FreedMemorySize());
  for (size_t i = allocated.size()/2; i < allocated.size(); i++) {
    uint32_t num_bytes = allocated[i].first;
    uint32_t p = allocated[i].second;
    arena.DeAllocate(p);
  }
  printf("Step 3 Freed first area size %d\n", arena.FreedMemorySize());

  //arena.DumpFreeList();
}




}  // namespace insdb

int main(int argc, char** argv) {
  return insdb::test::RunAllTests();
}
