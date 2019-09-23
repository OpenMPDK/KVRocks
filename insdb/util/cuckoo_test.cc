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


#include "insdb/filter_policy2.h"

#include "util/coding.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace insdb {

static const int kVerbose = 1;

static Slice Key(int i, char* buffer) {
  EncodeFixed32(buffer, i);
  return Slice(buffer, sizeof(uint32_t));
}

class CuckooTest {
 private:
  FilterPolicy2* policy_;
  std::vector<std::string> keys_;

 public:
  CuckooTest() : policy_(NewCuckooFilterPolicy(8, 100000)) {}

  ~CuckooTest() {
    delete policy_;
  }

  void Reset() {
    keys_.clear();
    delete policy_;
    policy_ = NewCuckooFilterPolicy(8, 100000);
  }

  void Add(const Slice& s) {
    keys_.push_back(s.ToString());
  }

  void Build() {
    /*
    std::vector<Slice > key_slices;
    for (size_t i = 0; i < keys_.size(); i++) {
      key_slices.push_back(Slice(keys_[i]));
    }
    */
    delete policy_;
    policy_ = NewCuckooFilterPolicy(8, 100000);
    for(int i = 0; i < keys_.size(); i++) {
      Slice a(keys_[i]);
      policy_->Add(a);
    }
    keys_.clear();
    if (kVerbose >= 2) DumpFilter();
  }

  size_t FilterSize() const {
    return policy_->Count();
  }

  void DumpFilter() {
    fprintf(stderr, "%s\n", policy_->Info().c_str());
  }

  bool Matches(const Slice& s) {
    if (!keys_.empty()) {
      Build();
    }
    return policy_->KeyMayMatch(s);
  }

  double FalsePositiveRate() {
    char buffer[sizeof(int)];
    int result = 0;
    for (int i = 0; i < 10000; i++) {
      if (Matches(Key(i + 1000000000, buffer))) {
        result++;
      }
    }
    return result / 10000.0;
  }
};

TEST(CuckooTest, EmptyFilter) {
  ASSERT_TRUE(! Matches("hello"));
  ASSERT_TRUE(! Matches("world"));
}

TEST(CuckooTest, Small) {
  Add("hello");
  Add("world");
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  ASSERT_TRUE(! Matches("x"));
  ASSERT_TRUE(! Matches("foo"));
}

static int NextLength(int length) {
  if (length < 10) {
    length += 1;
  } else if (length < 100) {
    length += 10;
  } else if (length < 1000) {
    length += 100;
  } else {
    length += 1000;
  }
  return length;
}

TEST(CuckooTest, VaryingLengths) {
  char buffer[sizeof(int)];

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;

  for (int length = 1; length <= 10000; length = NextLength(length)) {
    Reset();
    for (int i = 0; i < length; i++) {
      Add(Key(i, buffer));
    }
    Build();

    ASSERT_LE(FilterSize(), static_cast<size_t>((length * 10 / 8) + 40))
        << length;

    // All added keys must match
    for (int i = 0; i < length; i++) {
      ASSERT_TRUE(Matches(Key(i, buffer)))
          << "Length " << length << "; key " << i;
    }

    // Check false positive rate
    double rate = FalsePositiveRate();
    if (kVerbose >= 1) {
      fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
              rate*100.0, length, static_cast<int>(FilterSize()));
    }
    ASSERT_LE(rate, 0.02);   // Must not be over 2%
    if (rate > 0.0125) mediocre_filters++;  // Allowed, but not too often
    else good_filters++;
  }
  if (kVerbose >= 1) {
    fprintf(stderr, "Filters: %d good, %d mediocre\n",
            good_filters, mediocre_filters);
  }
  ASSERT_LE(mediocre_filters, good_filters/5);
}

// Different bits-per-byte

}  // namespace insdb

int main(int argc, char** argv) {
  return insdb::test::RunAllTests();
}
