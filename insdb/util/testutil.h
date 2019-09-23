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


#ifndef STORAGE_INSDB_UTIL_TESTUTIL_H_
#define STORAGE_INSDB_UTIL_TESTUTIL_H_

#include "insdb/env.h"
#include "insdb/slice.h"
#include "util/random.h"

namespace insdb {
namespace test {

// Store in *dst a random string of length "len" and return a Slice that
// references the generated data.
extern Slice RandomString(Random* rnd, int len, std::string* dst);

// Return a random key with the specified length that may contain interesting
// characters (e.g. \x00, \xff, etc.).
extern std::string RandomKey(Random* rnd, int len);

// Store in *dst a string of length "len" that will compress to
// "N*compressed_fraction" bytes and return a Slice that references
// the generated data.
extern Slice CompressibleString(Random* rnd, double compressed_fraction,
                                size_t len, std::string* dst);

// A wrapper that allows injection of errors.
class ErrorEnv : public EnvWrapper {
 public:
  bool writable_data_error_;
  int num_writable_data_errors_;

  ErrorEnv() : EnvWrapper(Env::Default()),
               writable_data_error_(false),
               num_writable_data_errors_(0) { }


  virtual Status Put(const InSDBKey key, const Slice& value, bool async = false) {
    if (writable_data_error_) {
      ++num_writable_data_errors_;
      return Status::IOError("fake Put error");
    }
    return target()->Put(key, value, async); 
  }
  virtual Status Flush(const InSDBKey key = {0,0}) {
    if (writable_data_error_) {
      ++num_writable_data_errors_;
      return Status::IOError("fake Flush error");
    }
    return target()->Flush(kWriteFlush, key);
  }
};

}  // namespace test
}  // namespace insdb

#endif  // STORAGE_INSDB_UTIL_TESTUTIL_H_
