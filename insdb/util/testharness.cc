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


#include "util/testharness.h"

#include <string>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace insdb {
extern void TestThreadBody(void *v) {
  TestThreadArg *arg = reinterpret_cast<TestThreadArg*>(v);
  TestSharedState *shared = arg->shared;
  {
    MutexLock l(&shared->mu);
    shared->num_initialized++;
    if (shared->num_initialized >= shared->total) {
      shared->cv.SignalAll();
    }
    while (!shared->start) {
      shared->cv.Wait();
    }
  }
  /** do action*/
  if (arg->func && arg->arg) {
    arg->func(arg->arg);
  }

  {
    MutexLock l(&shared->mu);
    shared->num_done++;
    if (shared->num_done >= shared->total) {
      shared->cv.SignalAll();
    }
  }
}

extern void TestLaunch(TestSharedState *shared, TestThreadArg *arg, int count) {
  int total_num = shared->total;
  for (int i = 0; i < count; i++) {
    Env::Default()->StartThread(TestThreadBody, &arg[i]);
  }

  shared->mu.Lock();
  while(shared->num_initialized < total_num) {
    shared->cv.Wait();
  }

  shared->start = true;
  shared->cv.SignalAll();
  while(shared->num_done < total_num) {
    shared->cv.Wait();
  }
  shared->mu.Unlock();
}
 
namespace test {

namespace {
struct Test {
  const char* base;
  const char* name;
  void (*func)();
};
std::vector<Test>* tests;
}

bool RegisterTest(const char* base, const char* name, void (*func)()) {
  if (tests == NULL) {
    tests = new std::vector<Test>;
  }
  Test t;
  t.base = base;
  t.name = name;
  t.func = func;
  tests->push_back(t);
  return true;
}

int RunAllTests() {
  const char* matcher = getenv("INSDB_TESTS");

  int num = 0;
  if (tests != NULL) {
    for (size_t i = 0; i < tests->size(); i++) {
      const Test& t = (*tests)[i];
      if (matcher != NULL) {
        std::string name = t.base;
        name.push_back('.');
        name.append(t.name);
        if (strstr(name.c_str(), matcher) == NULL) {
          continue;
        }
      }
      fprintf(stderr, "==== Test %s.%s\n", t.base, t.name);
      (*t.func)();
      ++num;
    }
  }
  fprintf(stderr, "==== PASSED %d tests\n", num);
  return 0;
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != NULL ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

}  // namespace test
}  // namespace insdb
