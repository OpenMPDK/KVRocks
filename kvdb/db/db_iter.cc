//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/insdb_wrapper.h"
#include "db/db_iter.h"
#include "rocksdb/iterator.h"

namespace rocksdb {

bool IteratorImpl::Valid() const {
    // Feat:Iterator prefix seek
    // if valid_ if false, then need not find key from insdb
    if (valid_) {
      return insdb_iterator->Valid();
    } else {
      return false;
    }
    //end
}

void IteratorImpl::SeekToFirst() {
    insdb_iterator->SeekToFirst();

    // Feat: Iterator prefix seek
    // Find the first key as prefix target key
    if (insdb_iterator->Valid() && prefix_extractor_ &&
        prefix_same_as_start_) {
      prefix_start_key_ = prefix_extractor_->Transform(key());
    }
    // end
}

void IteratorImpl::SeekToLast() {
    insdb_iterator->SeekToLast();

    // Feat: Iterator prefix seek
    // If find the last key is larger than prefix key, then return valid
    if (insdb_iterator->Valid() && prefix_extractor_ &&
        prefix_same_as_start_ &&
        prefix_extractor_->Transform(key()).compare(prefix_start_key_) != 0) {
        valid_ = false;
        return;
    }

    if (insdb_iterator->Valid() && prefix_extractor_ &&
        prefix_same_as_start_) {
          prefix_start_key_ = prefix_extractor_->Transform(key());
    }
    //end
}

void IteratorImpl::Seek(const Slice& target) {
#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("SEEK IN ITERATOR",0,target.data(),target.size(),0);
#endif
    insdb::Slice insdb_target(target.data(), target.size());
    insdb_iterator->Seek(insdb_target);

    // Feat: Iterator prefix seek
    if (insdb_iterator->Valid() && prefix_extractor_ && prefix_same_as_start_ ) {
      prefix_start_key_ = prefix_extractor_->Transform(target);
    }

    // iterator prefix function
    if (Valid() && prefix_extractor_ && prefix_same_as_start_ &&
        prefix_extractor_->Transform(key()).compare(prefix_start_key_) != 0) {
      valid_ = false;
      prefix_start_key_.clear();
      return;
    }
    //end
}

void IteratorImpl::SeekForPrev(const Slice& target) {
#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("SEEK FOR PREV IN ITERATOR",0,target.data(),target.size(),0);
#endif
    insdb::Slice insdb_target(target.data(), target.size());
    insdb_iterator->SeekForPrev(insdb_target);

    // Feat: iterator prefix function
    if (insdb_iterator->Valid() && prefix_extractor_ && prefix_same_as_start_ ) {
      prefix_start_key_ = prefix_extractor_->Transform(target);
    }

    if (insdb_iterator->Valid() && prefix_extractor_ && prefix_same_as_start_ &&
        prefix_extractor_->Transform(key()).compare(prefix_start_key_) != 0) {
      valid_ = false;
      prefix_start_key_.clear();
      return;
    }
    //end
}

void IteratorImpl::Next() {
    assert(valid_);
    insdb_iterator->Next();

    // Feat: iterator prefix function
    if (insdb_iterator->Valid() && prefix_extractor_ &&
        prefix_same_as_start_ &&
        prefix_extractor_->Transform(key()).compare(prefix_start_key_) != 0) {
      valid_ = false;
    }
    return;
}

void IteratorImpl::Prev() {
    assert(valid_);
    insdb_iterator->Prev();

    // Feat: iterator prefix function
    if (insdb_iterator->Valid() && prefix_extractor_ &&
        prefix_same_as_start_ &&
          prefix_extractor_->Transform(key()).compare(prefix_start_key_) != 0) {
      valid_ = false;
    }
    return;
}

Slice IteratorImpl::key() const {
#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("KEY IN ITERATOR",0,insdb_iterator->key().data(),insdb_iterator->key().size(),0);
#endif
    insdb::Slice key = insdb_iterator->key();
	return Slice(key.data(), key.size());
}

Slice IteratorImpl::value() const {
#ifdef KVDB_ENABLE_IOTRACE
  log_iotrace("VALUE IN ITERATOR",0,insdb_iterator->key().data(),insdb_iterator->key().size(),insdb_iterator->value().size());
#endif
    insdb::Slice value = insdb_iterator->value();
    return Slice(value.data(), value.size());
}

Status IteratorImpl::status() const {
	return convert_insdb_status(insdb_iterator->status());
}

}  // namespace rocksdb
