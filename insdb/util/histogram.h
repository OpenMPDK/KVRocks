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


#ifndef STORAGE_INSDB_UTIL_HISTOGRAM_H_
#define STORAGE_INSDB_UTIL_HISTOGRAM_H_

#include <string>

namespace insdb {

class Histogram {
 public:
  Histogram() { }
  ~Histogram() { }

  void Clear();
  void Add(double value);
  void Merge(const Histogram& other);

  std::string ToString() const;

 private:
  double min_;
  double max_;
  double num_;
  double sum_;
  double sum_squares_;

  enum { kNumBuckets = 154 };
  static const double kBucketLimit[kNumBuckets];
  double buckets_[kNumBuckets];

  double Median() const;
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;
};

}  // namespace insdb

#endif  // STORAGE_INSDB_UTIL_HISTOGRAM_H_
