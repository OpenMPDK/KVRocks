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


#ifndef STORAGE_INSDB_UTIL_TESTHARNESS_H_
#define STORAGE_INSDB_UTIL_TESTHARNESS_H_

#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include "insdb/env.h"
#include "insdb/slice.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace insdb {
struct TestSharedState{
  port::Mutex mu;
  port::CondVar cv;
  int total;
  int num_initialized;
  int num_done;
  bool start;

  TestSharedState() : cv(&mu) {}
};

typedef void (*TestMethodFunc)(void *arg);

struct TestThreadArg {
  int thread_id;
  TestSharedState *shared;
  TestMethodFunc func;
  void *arg;
};

extern void TestThreadBody(void *v);
extern void TestLaunch(TestSharedState *shared,TestThreadArg *arg, int count);

namespace test {

// Run some of the tests registered by the TEST() macro.  If the
// environment variable "INSDB_TESTS" is not set, runs all tests.
// Otherwise, runs only the tests whose name contains the value of
// "INSDB_TESTS" as a substring.  E.g., suppose the tests are:
//    TEST(Foo, Hello) { ... }
//    TEST(Foo, World) { ... }
// INSDB_TESTS=Hello will run the first test
// INSDB_TESTS=o     will run both tests
// INSDB_TESTS=Junk  will run no tests
//
// Returns 0 if all tests pass.
// Dies or returns a non-zero value if some test fails.
extern int RunAllTests();


// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
extern int RandomSeed();

// An instance of Tester is allocated to hold temporary state during
// the execution of an assertion.
class Tester {
 private:
  bool ok_;
  const char* fname_;
  int line_;
  std::stringstream ss_;

 public:
  Tester(const char* f, int l)
      : ok_(true), fname_(f), line_(l) {
  }

  ~Tester() {
    if (!ok_) {
      fprintf(stderr, "%s:%d:%s\n", fname_, line_, ss_.str().c_str());
      exit(1);
    }
  }

  Tester& Is(bool b, const char* msg) {
    if (!b) {
      ss_ << " Assertion failure " << msg;
      ok_ = false;
    }
    return *this;
  }

  Tester& IsOk(const Status& s) {
    if (!s.ok()) {
      ss_ << " " << s.ToString();
      ok_ = false;
    }
    return *this;
  }

#define BINARY_OP(name,op)                              \
  template <class X, class Y>                           \
  Tester& name(const X& x, const Y& y) {                \
    if (! (x op y)) {                                   \
      ss_ << " failed: " << x << (" " #op " ") << y;    \
      ok_ = false;                                      \
    }                                                   \
    return *this;                                       \
  }

  BINARY_OP(IsEq, ==)
  BINARY_OP(IsNe, !=)
  BINARY_OP(IsGe, >=)
  BINARY_OP(IsGt, >)
  BINARY_OP(IsLe, <=)
  BINARY_OP(IsLt, <)
#undef BINARY_OP

  // Attach the specified value to the error message if an error has occurred
  template <class V>
  Tester& operator<<(const V& value) {
    if (!ok_) {
      ss_ << " " << value;
    }
    return *this;
  }
};

#define ASSERT_TRUE(c) ::insdb::test::Tester(__FILE__, __LINE__).Is((c), #c)
#define ASSERT_OK(s) ::insdb::test::Tester(__FILE__, __LINE__).IsOk((s))
#define ASSERT_EQ(a,b) ::insdb::test::Tester(__FILE__, __LINE__).IsEq((a),(b))
#define ASSERT_NE(a,b) ::insdb::test::Tester(__FILE__, __LINE__).IsNe((a),(b))
#define ASSERT_GE(a,b) ::insdb::test::Tester(__FILE__, __LINE__).IsGe((a),(b))
#define ASSERT_GT(a,b) ::insdb::test::Tester(__FILE__, __LINE__).IsGt((a),(b))
#define ASSERT_LE(a,b) ::insdb::test::Tester(__FILE__, __LINE__).IsLe((a),(b))
#define ASSERT_LT(a,b) ::insdb::test::Tester(__FILE__, __LINE__).IsLt((a),(b))

#define TCONCAT(a,b) TCONCAT1(a,b)
#define TCONCAT1(a,b) a##b

#define TEST(base,name)                                                 \
class TCONCAT(_Test_,name) : public base {                              \
 public:                                                                \
  void _Run();                                                          \
  static void _RunIt() {                                                \
    TCONCAT(_Test_,name) t;                                             \
    t._Run();                                                           \
  }                                                                     \
};                                                                      \
bool TCONCAT(_Test_ignored_,name) =                                     \
  ::insdb::test::RegisterTest(#base, #name, &TCONCAT(_Test_,name)::_RunIt); \
void TCONCAT(_Test_,name)::_Run()

// Register the specified test.  Typically not used directly, but
// invoked via the macro expansion of TEST.
extern bool RegisterTest(const char* base, const char* name, void (*func)());


}  // namespace test
}  // namespace insdb

#endif  // STORAGE_INSDB_UTIL_TESTHARNESS_H_
