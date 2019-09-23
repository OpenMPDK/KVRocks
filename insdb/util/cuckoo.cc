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

#include <assert.h>
#include <sstream>
#include <xmmintrin.h>


#include "insdb/slice.h"
#include "util/coding.h"
#include "insdb/filter_policy2.h"

namespace insdb {

// inspired from
// http://www-graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
#define haszero4(x) (((x)-0x1111ULL) & (~(x)) & 0x8888ULL)
#define hasvalue4(x, n) (haszero4((x) ^ (0x1111ULL * (n))))

#define haszero8(x) (((x)-0x01010101ULL) & (~(x)) & 0x80808080ULL)
#define hasvalue8(x, n) (haszero8((x) ^ (0x01010101ULL * (n))))

#define haszero12(x) (((x)-0x001001001001ULL) & (~(x)) & 0x800800800800ULL)
#define hasvalue12(x, n) (haszero12((x) ^ (0x001001001001ULL * (n))))

#define haszero16(x) \
  (((x)-0x0001000100010001ULL) & (~(x)) & 0x8000800080008000ULL)
#define hasvalue16(x, n) (haszero16((x) ^ (0x0001000100010001ULL * (n))))

inline uint64_t upperpower2(uint64_t x) {
  x--;
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
  x |= x >> 32;
  x++;
  return x;
}

// the most naive table implementation: one huge bit array
class SingleTable {
  static const size_t kTagsPerBucket = 4;

  struct Bucket {
    char bits_[1];
  };

  // using a pointer adds one more indirection
  Bucket **buckets_;
  char *bits_data_;
  size_t bits_per_tag_;
  size_t num_buckets_;
  size_t kBytesPerBucket_;
  size_t kTagMask_;

 public:
  explicit SingleTable(const size_t bits_per_tag, const size_t num)
      : bits_per_tag_(bits_per_tag), num_buckets_(num) {

    kBytesPerBucket_ = (bits_per_tag_ * kTagsPerBucket + 7) >> 3;
    kTagMask_ = (1ULL << bits_per_tag_) -1;
    buckets_  = new Bucket*[num_buckets_];
    bits_data_ = new char[num_buckets_ * kBytesPerBucket_];
    for(size_t i = 0; i < num_buckets_; i++) {
      buckets_[i] = (Bucket *)&bits_data_[kBytesPerBucket_ * i];
    }
    memset(bits_data_, 0, kBytesPerBucket_ * num_buckets_);
  }

  explicit SingleTable(const size_t bits_per_tag, const size_t num, Slice &bitmap)
      :bits_per_tag_(bits_per_tag),  num_buckets_(num) {
    kBytesPerBucket_ = (bits_per_tag_ * kTagsPerBucket + 7) >> 3;
    kTagMask_ = (1ULL << bits_per_tag_) -1;
    assert(bitmap.size() == kBytesPerBucket_ * num_buckets_);
    buckets_  = new Bucket*[num_buckets_];
    bits_data_ = new char[num_buckets_ * kBytesPerBucket_];
    for(size_t i = 0; i < num_buckets_; i++) {
      buckets_[i] =(Bucket *)&bits_data_[kBytesPerBucket_ * i];
    }
    memcpy(bits_data_, bitmap.data(), bitmap.size());
  }

  ~SingleTable() { 
    delete[] buckets_;
    delete[] bits_data_;
  }

  Slice GetBitMap() {
    return Slice((const char *)bits_data_, (kBytesPerBucket_ * num_buckets_));
  }

  size_t NumBuckets() const {
    return num_buckets_;
  }

  size_t SizeInBytes() const {
    return kBytesPerBucket_ * num_buckets_; 
  }

  size_t SizeInTags() const {
    return kTagsPerBucket * num_buckets_; 
  }

  std::string Info() const {
    std::stringstream ss;
    ss << "SingleHashtable with tag size: " << bits_per_tag_ << " bits \n";
    ss << "\t\tAssociativity: " << kTagsPerBucket << "\n";
    ss << "\t\tTotal # of rows: " << num_buckets_ << "\n";
    ss << "\t\tTotal # slots: " << SizeInTags() << "\n";
    return ss.str();
  }

  // read tag from pos(i,j)
  inline uint32_t ReadTag(const size_t i, const size_t j) const {
    const char *p = buckets_[i]->bits_;
    uint32_t tag = 0;
    /* following code only works for little-endian */
    if (bits_per_tag_ == 2) {
      tag = *((uint8_t *)p) >> (j * 2);
    } else if (bits_per_tag_ == 4) {
      p += (j >> 1);
      tag = *((uint8_t *)p) >> ((j & 1) << 2);
    } else if (bits_per_tag_ == 8) {
      p += j;
      tag = *((uint8_t *)p);
    } else if (bits_per_tag_ == 12) {
      p += j + (j >> 1);
      tag = *((uint16_t *)p) >> ((j & 1) << 2);
    } else if (bits_per_tag_ == 16) {
      p += (j << 1);
      tag = *((uint16_t *)p);
    } else if (bits_per_tag_ == 32) {
      tag = ((uint32_t *)p)[j];
    }
    return tag & kTagMask_;
  }

  // write tag to pos(i,j)
  inline void WriteTag(const size_t i, const size_t j, const uint32_t t) {
    char *p = buckets_[i]->bits_;
    uint32_t tag = t & kTagMask_;
    /* following code only works for little-endian */
    if (bits_per_tag_ == 2) {
      *((uint8_t *)p) |= tag << (2 * j);
    } else if (bits_per_tag_ == 4) {
      p += (j >> 1);
      if ((j & 1) == 0) {
        *((uint8_t *)p) &= 0xf0;
        *((uint8_t *)p) |= tag;
      } else {
        *((uint8_t *)p) &= 0x0f;
        *((uint8_t *)p) |= (tag << 4);
      }
    } else if (bits_per_tag_ == 8) {
      ((uint8_t *)p)[j] = tag;
    } else if (bits_per_tag_ == 12) {
      p += (j + (j >> 1));
      if ((j & 1) == 0) {
        ((uint16_t *)p)[0] &= 0xf000;
        ((uint16_t *)p)[0] |= tag;
      } else {
        ((uint16_t *)p)[0] &= 0x000f;
        ((uint16_t *)p)[0] |= (tag << 4);
      }
    } else if (bits_per_tag_ == 16) {
      ((uint16_t *)p)[j] = tag;
    } else if (bits_per_tag_ == 32) {
      ((uint32_t *)p)[j] = tag;
    }
  }

  inline bool FindTagInBuckets(const size_t i1, const size_t i2,
                               const uint32_t tag) const {
    const char *p1 = buckets_[i1]->bits_;
    const char *p2 = buckets_[i2]->bits_;

    uint64_t v1 = *((uint64_t *)p1);
    uint64_t v2 = *((uint64_t *)p2);

    // caution: unaligned access & assuming little endian
    if (bits_per_tag_ == 4 && kTagsPerBucket == 4) {
      return hasvalue4(v1, tag) || hasvalue4(v2, tag);
    } else if (bits_per_tag_ == 8 && kTagsPerBucket == 4) {
      return hasvalue8(v1, tag) || hasvalue8(v2, tag);
    } else if (bits_per_tag_ == 12 && kTagsPerBucket == 4) {
      return hasvalue12(v1, tag) || hasvalue12(v2, tag);
    } else if (bits_per_tag_ == 16 && kTagsPerBucket == 4) {
      return hasvalue16(v1, tag) || hasvalue16(v2, tag);
    } else {
      for (size_t j = 0; j < kTagsPerBucket; j++) {
        if ((ReadTag(i1, j) == tag) || (ReadTag(i2, j) == tag)) {
          return true;
        }
      }
      return false;
    }
  }

  inline bool FindTagInBucket(const size_t i, const uint32_t tag) const {
    // caution: unaligned access & assuming little endian
    if (bits_per_tag_ == 4 && kTagsPerBucket == 4) {
      const char *p = buckets_[i]->bits_;
      uint64_t v = *(uint64_t *)p;  // uint16_t may suffice
      return hasvalue4(v, tag);
    } else if (bits_per_tag_ == 8 && kTagsPerBucket == 4) {
      const char *p = buckets_[i]->bits_;
      uint64_t v = *(uint64_t *)p;  // uint32_t may suffice
      return hasvalue8(v, tag);
    } else if (bits_per_tag_ == 12 && kTagsPerBucket == 4) {
      const char *p = buckets_[i]->bits_;
      uint64_t v = *(uint64_t *)p;
      return hasvalue12(v, tag);
    } else if (bits_per_tag_ == 16 && kTagsPerBucket == 4) {
      const char *p = buckets_[i]->bits_;
      uint64_t v = *(uint64_t *)p;
      return hasvalue16(v, tag);
    } else {
      for (size_t j = 0; j < kTagsPerBucket; j++) {
        if (ReadTag(i, j) == tag) {
          return true;
        }
      }
      return false;
    }
  }

  inline bool DeleteTagFromBucket(const size_t i, const uint32_t tag) {
    for (size_t j = 0; j < kTagsPerBucket; j++) {
      if (ReadTag(i, j) == tag) {
        assert(FindTagInBucket(i, tag) == true);
        WriteTag(i, j, 0);
        return true;
      }
    }
    return false;
  }

  inline bool InsertTagToBucket(const size_t i, const uint32_t tag,
                                const bool kickout, uint32_t &oldtag) {
    for (size_t j = 0; j < kTagsPerBucket; j++) {
      if (ReadTag(i, j) == 0) {
        WriteTag(i, j, tag);
        return true;
      }
    }
    if (kickout) {
      size_t r = rand() % kTagsPerBucket;
      oldtag = ReadTag(i, r);
      WriteTag(i, r, tag);
    }
    return false;
  }

  inline size_t NumTagsInBucket(const size_t i) const {
    size_t num = 0;
    for (size_t j = 0; j < kTagsPerBucket; j++) {
      if (ReadTag(i, j) != 0) {
        num++;
      }
    }
    return num;
  }
};

// generate 64bit hash with given Slice
static uint64_t CukooHash(const Slice& key) {
  /**
    Generate 64bit hash key from Slice - revise from murmurHash2.
TODO: maybe need to change more fast hash function if we find.
  */
  const uint64_t seed = 0x25f992d38a208b61;
  const uint64_t m = 0xc6a4a7935bd1e995;
  const uint32_t r = 47;

  const uint64_t *data = (const uint64_t *)key.data();
  size_t len = key.size();
  const uint64_t *end = data + (len/8);

  uint64_t h = seed ^ (len * m);

  while(data != end) {
    uint64_t k = DecodeFixed64((const char *)data);
    data++;
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
  }
  const unsigned char *data2 = (const unsigned char *)data;
  switch(len & 7) {
      case 7: h ^= uint64_t(data2[6]) << 48;
      case 6: h ^= uint64_t(data2[5]) << 40;
      case 5: h ^= uint64_t(data2[4]) << 32;
      case 4: h ^= uint64_t(data2[3]) << 24;
      case 3: h ^= uint64_t(data2[2]) << 16;
      case 2: h ^= uint64_t(data2[1]) << 8;
      case 1: h ^= uint64_t(data2[0]);
            h *= m;
  };
  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}


// A cuckoo filter class exposes a Bloomier filter interface,
// providing methods of Add, Delete, Contain. It takes three
// template parameters:
//   ItemType:  the type of item you want to insert
//   bits_per_item_: how many bits each item is hashed into
//   TableType: the storage of table, SingleTable by default, and
// PackedTable to enable semi-sorting
class CuckooFilter : public FilterPolicy2 {
  // maximum number of cuckoo kicks before claiming failure
  const size_t kMaxCuckooCount = 500;
  // Storage of items
  SingleTable *table_;
  // Number of items stored
  size_t num_items_;
  // Number of bits for item
  size_t bits_per_item_;
  // Max number of items stored
  size_t max_num_keys_;
  // Is Collision happen?
  bool hasCollision_;

  inline size_t IndexHash(uint32_t hv) const {
    // table_->num_buckets is always a power of two, so modulo can be replaced
    // with
    // bitwise-and:
    return hv & (table_->NumBuckets() - 1);
  }

  inline uint32_t TagHash(uint32_t hv) const {
    uint32_t tag;
    tag = hv & ((1ULL << bits_per_item_) - 1);
    tag += (tag == 0);
    return tag;
  }

  inline void GenerateIndexTagHash(const Slice &item, size_t* index,
                                   uint32_t* tag) const {
    const uint64_t hash = CukooHash(item);
    *index = IndexHash(hash >> 32);
    *tag = TagHash(hash);
  }

  inline size_t AltIndex(const size_t index, const uint32_t tag) const {
    // NOTE(binfan): originally we use:
    // index ^ HashUtil::BobHash((const void*) (&tag), 4)) & table_->INDEXMASK;
    // now doing a quick-n-dirty way:
    // 0x5bd1e995 is the hash constant from MurmurHash2
    return IndexHash((uint32_t)(index ^ (tag * 0x5bd1e995)));
  }

  bool AddImpl(const size_t i, const uint32_t tag) {
    size_t curindex = i;
    uint32_t curtag = tag;
    uint32_t oldtag;

    for (uint32_t count = 0; count < kMaxCuckooCount; count++) {
      bool kickout = count > 0;
      oldtag = 0;
      if (table_->InsertTagToBucket(curindex, curtag, kickout, oldtag)) {
        num_items_++;
        return true;
      }
      if (kickout) {
        curtag = oldtag;
      }
      curindex = AltIndex(curindex, curtag);
    }

    hasCollision_ = true;
    return false;
  }

  // load factor is the fraction of occupancy
  double LoadFactor() const { return 1.0 * Count() / table_->SizeInTags(); }

  double BitsPerItem() const { return 8.0 * table_->SizeInBytes() / Count(); }

 public:
  explicit CuckooFilter(size_t bits_per_item, size_t max_num_keys)
      : num_items_(0), bits_per_item_(bits_per_item),  max_num_keys_(max_num_keys),  hasCollision_(false) {
    size_t assoc = 4;
    size_t num_buckets = upperpower2(max_num_keys_ / assoc);
    double frac = (double)max_num_keys / num_buckets / assoc;
    if (frac > 0.96) {
      num_buckets <<= 1;
    }
    hasCollision_ = false;
    table_ = new SingleTable(bits_per_item_, num_buckets);
  }

  explicit CuckooFilter(size_t num_item, size_t bits_per_item,  size_t max_num_keys, Slice &bitmap)
      : num_items_(num_item), bits_per_item_(bits_per_item), max_num_keys_(max_num_keys), hasCollision_(false) {
    size_t assoc = 4;
    size_t num_buckets = upperpower2(max_num_keys_ / assoc);
    double frac = (double)max_num_keys / num_buckets / assoc;
    if (frac > 0.96) {
      num_buckets <<= 1;
    }
    hasCollision_ = false;
    table_ = new SingleTable(bits_per_item_, num_buckets, bitmap);
  }
 

  ~CuckooFilter() { delete table_; }


  virtual const char* Name() const {
    return "insdb.BuiltinCukooFilter";
  }

  // number of current inserted items;
  size_t Count() const { return num_items_; }

  // size of the filter in bytes.
  size_t MemUsage() const { return table_->SizeInBytes(); }


  bool Add(const Slice &item) {
    size_t i1, i2;
    uint32_t tag;

    if (hasCollision_) {
      return false;
    }
    // check key first
    GenerateIndexTagHash(item, &i1, &tag);
    i2 = AltIndex(i1, tag);
    assert(i1 == AltIndex(i2, tag));

    if (table_->FindTagInBuckets(i1, i2, tag)) {
      return true;
    }
    return AddImpl(i1, tag);
  } 
  

  bool AddForce(const Slice &item) {
    size_t i;
    uint32_t tag;

    if (hasCollision_) {
      return false;
    }
    return AddImpl(i, tag);
  }


  bool KeyMayMatch(const Slice &key) const {
    size_t i1, i2;
    uint32_t tag;

    GenerateIndexTagHash(key, &i1, &tag);
    i2 = AltIndex(i1, tag);

    assert(i1 == AltIndex(i2, tag));

    if (table_->FindTagInBuckets(i1, i2, tag)) {
      return true;
    } else {
      return false;
    }
  }

  void Delete(const Slice &key) {
    size_t i1, i2;
    uint32_t tag;

    GenerateIndexTagHash(key, &i1, &tag);
    i2 = AltIndex(i1, tag);

    if (table_->DeleteTagFromBucket(i1, tag)) {
      num_items_--;
    } else if (table_->DeleteTagFromBucket(i2, tag)) {
      num_items_--;
    }
    return;
  }

  std::string Info() const { 
    std::stringstream ss;
    ss << "CuckooFilter Status:\n"
      << "\t\t" << table_->Info() << "\n"
      << "\t\tKeys stored: " << Count() << "\n"
      << "\t\tLoad factor: " << LoadFactor() << "\n"
      << "\t\tHashtable size: " << (table_->SizeInBytes() >> 10) << " KB\n";
    if (Count() > 0) {
      ss << "\t\tbit/key:   " << BitsPerItem() << "\n";
    } else {
      ss << "\t\tbit/key:   N/A\n";
    }
    return ss.str();
  }

  /**
    EcodeTo build filter string for storage.
    <bits_per_item_><max_num_keys><bitmap size><bitmap data>
  */
  void EncodeTo(std::string *bitmap) const {
    Slice bits = table_->GetBitMap();
    PutVarint32(bitmap, (int32_t)num_items_);
    PutVarint32(bitmap, (int32_t)bits_per_item_); /// append bits_per_item_
    PutVarint32(bitmap, (int32_t)max_num_keys_); /// append max_num_keys
    PutLengthPrefixedSlice(bitmap, bits); /// append bitmap size + bitmap data
    return;
  }
};


FilterPolicy2* NewCuckooFilterPolicy(size_t bits_per_key, size_t max_num_keys) {
  return new CuckooFilter(bits_per_key, max_num_keys);
}

FilterPolicy2* BuildCuckooFilterPolicy(size_t num_items, size_t bits_per_key, size_t max_num_keys, Slice &bitmap) {
  return new CuckooFilter(num_items, bits_per_key, max_num_keys, bitmap);
}

const char *DecodeCuckooFilter(const char *p, const char *limit, uint32_t *num_items,
    uint32_t *bits_per_key, uint32_t *max_num_keys, Slice *bitmap) {
    if (limit -p < 3) return NULL;
    if ((p = GetVarint32Ptr(p, limit, num_items)) == NULL) return NULL;
    if ((p = GetVarint32Ptr(p, limit, bits_per_key)) == NULL) return NULL;
    if ((p = GetVarint32Ptr(p, limit, max_num_keys)) == NULL) return NULL;
    if ((p = GetLengthPrefixedSliceInplace(p, limit, bitmap)) == NULL) return NULL;
    return p;
}

}  // namespace insdb

