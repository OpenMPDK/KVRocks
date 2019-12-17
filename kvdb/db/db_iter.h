//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>
#include <string>

#include "insdb/iterator.h"

#ifdef _ORIG_ROCKSDB_
#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#endif
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "db/column_family.h"
#ifdef _ORIG_ROCKSDB_
#include "util/arena.h"
#include "util/autovector.h"
#include "util/cf_options.h"
#endif

namespace rocksdb {

class IteratorImpl : public Iterator {
 public:

  // Feat:Iterator prefix seek
  IteratorImpl(insdb::Iterator *iterator, const ReadOptions& read_options,
                     ColumnFamilyHandle* column_family)
    : insdb_iterator(iterator),
    valid_(true),
    prefix_same_as_start_(read_options.prefix_same_as_start),
    prefix_start_key_() {
      auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
      auto cfd = cfh->cfd();
      prefix_extractor_ = cfd->ioptions()->prefix_extractor;
	  start_key_buf_ = NULL;
	  last_key_buf_ = NULL;
    }
    // end

  ~IteratorImpl() {
    delete insdb_iterator;
    prefix_extractor_ = NULL;
	if (start_key_buf_)
		delete[] start_key_buf_;
	if (last_key_buf_)
		delete[] last_key_buf_;
  }

  virtual bool Valid() const override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Seek(const Slice& target) override;
  virtual void SeekForPrev(const Slice& target) override;
  virtual void Next() override;
  virtual void Prev() override;
  virtual Slice key() const override;
  virtual Slice value() const override;
  virtual Status status() const override;

 private:
  insdb::Iterator* insdb_iterator;
  // Feat:Iterator prefix seek
  bool valid_;
  const SliceTransform* prefix_extractor_;
  const bool prefix_same_as_start_;
  Slice prefix_start_key_;
  char *start_key_buf_;
  char *last_key_buf_;
  // end
};

}  // namespace rocksdb
