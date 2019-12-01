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


// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#ifndef STORAGE_INSDB_INCLUDE_SLICE_H_
#define STORAGE_INSDB_INCLUDE_SLICE_H_

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>
#include "insdb/export.h"
#include "insdb/cleanable.h"
#ifdef INSDB_GLOBAL_STATS
#include <atomic>
#endif
namespace insdb {

#ifdef INSDB_GLOBAL_STATS
    extern std::atomic<uint64_t> g_pinslice_uv_cnt;
#endif

    class INSDB_EXPORT Slice {
        public:
            // Create an empty slice.
            Slice() : data_(""), size_(0) { }

            // Create a slice that refers to d[0,n-1].
            Slice(const char* d, size_t n) : data_(d), size_(n) { }

            Slice(int n) : data_(""), size_(0) {}

            // Create a slice that refers to the contents of "s"
            Slice(const std::string& s) : data_(s.data()), size_(s.size()) { }

            // Create a slice that refers to s[0,strlen(s)-1]
            Slice(const char* s) : data_(s), size_(strlen(s)) { }

            // Return a pointer to the beginning of the referenced data
            const char* data() const { return data_; }

            // Return the length (in bytes) of the referenced data
            size_t size() const { return size_; }

            // Return true iff the length of the referenced data is zero
            bool empty() const { return size_ == 0; }

            // Return the ith byte in the referenced data.
            // REQUIRES: n < size()
            char operator[](size_t n) const {
                assert(n < size());
                return data_[n];
            }

            // Change this slice to refer to an empty array
            void clear() { data_ = ""; size_ = 0; }

            // Drop the first "n" bytes from this slice.
            void remove_prefix(size_t n) {
                assert(n <= size());
                data_ += n;
                size_ -= n;
            }

            // Return a string that contains the copy of the referenced data.
            std::string ToString() const { return std::string(data_, size_); }

            // Three-way comparison.  Returns value:
            //   <  0 iff "*this" <  "b",
            //   == 0 iff "*this" == "b",
            //   >  0 iff "*this" >  "b"
            int compare(const Slice& b) const;

            // Return true iff "x" is a prefix of "*this"
            bool starts_with(const Slice& x) const {
                return ((size_ >= x.size_) &&
                        (memcmp(data_, x.data_, x.size_) == 0));
            }

            // private: make these public for child class access

            const char* data_;
            size_t size_;

            // Intentionally copyable
    };


    #define FORCE_INLINE inline __attribute__((always_inline))

    inline uint32_t ROTL32 ( uint32_t x, int8_t r )
    {
      return (x << r) | (x >> (32 - r));
    }

    //-----------------------------------------------------------------------------
    // Finalization mix - force all bits of a hash block to avalanche

    FORCE_INLINE uint32_t fmix32 ( uint32_t h )
    {
      h ^= h >> 16;
      h *= 0x85ebca6b;
      h ^= h >> 13;
      h *= 0xc2b2ae35;
      h ^= h >> 16;

      return h;
    }

    const uint32_t INSDB_32_HASH_START = 2166136261UL;
    // MurmurHash3_x86_32
    inline uint32_t InSDBUKHash (const char *data, uint32_t len) {
        const int nblocks = len >> 2;

        uint32_t h1 = INSDB_32_HASH_START;

        const uint32_t c1 = 0xcc9e2d51;
        const uint32_t c2 = 0x1b873593;

        //----------
        // body

        const uint32_t * blocks = (const uint32_t *)(data + (nblocks << 2));

        for(int i = -nblocks; i; i++)
        {
          uint32_t k1 = blocks[i];

          k1 *= c1;
          k1 = ROTL32(k1,15);
          k1 *= c2;

          h1 ^= k1;
          h1 = ROTL32(h1,13);
          h1 = h1*5+0xe6546b64;
        }

        //----------
        // tail

        const uint8_t * tail = (const uint8_t*)(data + nblocks*4);

        uint32_t k1 = 0;

        switch(len & 3)
        {
        case 3: k1 ^= tail[2] << 16;
        case 2: k1 ^= tail[1] << 8;
        case 1: k1 ^= tail[0];
                k1 *= c1; k1 = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
        };

        //----------
        // finalization

        h1 ^= len;

        h1 = fmix32(h1);

        return h1;
    }



    class INSDB_EXPORT KeySlice : public Slice {
        public:
            // Create an empty slice.
            KeySlice() {
                data_ = NULL;
                size_ = 0;
                hash_ = 0;
                has_hash_ = false;
            }

            // Create a slice that refers to d[0,n-1].
            KeySlice(const char* d, size_t n, uint32_t hash) {
                data_ = d;
                size_ = n;
                hash_ = hash;
                has_hash_ = true;
            } 
            KeySlice(const char* d, uint32_t n) { 
                data_ = d;
                size_ = n;
                hash_ = Hash();
                has_hash_ = true;
            }

            KeySlice(int n) {
                data_ = NULL;
                size_ = 0;
                hash_ = Hash();
                has_hash_ = true;
            }

            // Create a slice that refers to the contents of "s"
#if 1
            KeySlice(const std::string& s) {
                data_ = s.data();
                size_ = s.size();
                hash_ = Hash();
                has_hash_ = true;
            }
            KeySlice(const std::string& s, uint32_t hash) {
                data_ = s.data();
                size_ = s.size();
                hash_ = hash;
                has_hash_ =true;
            }
#endif

            // Create a slice that refers to s[0,strlen(s)-1]
            KeySlice(const char* s) {
                data_ = s;
                size_ = strlen(s);
                hash_ = Hash();
                has_hash_ = true;
            }
#if 0
            KeySlice(const char* s, uint32_t hash) {
                data_ = s;
                size_ = strlen(s);
                hash_ = Hash();
                has_hash_ = true;
            }
#endif

            KeySlice(const Slice& key) {
                data_ = key.data();
                size_ = key.size();
                hash_ = Hash();
                has_hash_ = true;
            }
            Slice GetSlice(){
                return Slice(data_, size_); 
            }

            uint32_t GetHashValue() const { return hash_; }
            bool HasHash() const { return has_hash_; }
        private:
            /**
             To remove class cross reference problem, defind InSDBHash function and use it.
             */
            uint32_t Hash() {
                return InSDBUKHash (data_, size_);
            }
            uint32_t hash_;
            bool has_hash_;

            // Intentionally copyable
    };

    struct UKHash {
        inline uint32_t operator() (const KeySlice& a) const {
            if(a.HasHash()){
                return a.GetHashValue();
            }
            return InSDBUKHash (a.data(), a.size());
        }
    };

    struct UKEqual {
        bool operator() (const KeySlice& lhs, const KeySlice& rhs) const {
            return ((lhs.size() == rhs.size()) &&
                    (memcmp(lhs.data(), rhs.data(), lhs.size()) == 0));
        }
    };

    /**
     * A Slice that can be pinned with some cleanup tasks, which will be run upon
     * ::Reset() or object destruction, whichever is invoked first. This can be used
     * to avoid memcpy by having the PinnsableSlice object referring to the data
     * that is locked in the memory and release them after the data is consumed.
     */
    class PinnableSlice : public Slice, public Cleanable {
     public:
      PinnableSlice() { buf_ = &self_space_; }
      explicit PinnableSlice(std::string* buf) { buf_ = buf; }

      inline void PinSlice(const Slice& s, CleanupFunction f, void* arg1,
                           void* arg2) {
        assert(!pinned_);
        pinned_ = true;
        data_ = s.data();
        size_ = s.size();
        RegisterCleanup(f, arg1, arg2);
        assert(pinned_);
#ifdef INSDB_GLOBAL_STATS
        g_pinslice_uv_cnt++;
#endif
      }

      inline void PinSlice(const Slice& s, Cleanable* cleanable) {
        assert(!pinned_);
        pinned_ = true;
        data_ = s.data();
        size_ = s.size();
        cleanable->DelegateCleanupsTo(this);
        assert(pinned_);
#ifdef INSDB_GLOBAL_STATS
        g_pinslice_uv_cnt++;
#endif
      }

      inline void PinSelf(const Slice& slice) {
        assert(!pinned_);
        buf_->assign(slice.data(), slice.size());
        data_ = buf_->data();
        size_ = buf_->size();
        assert(!pinned_);
      }

      inline void PinSelf() {
        assert(!pinned_);
        data_ = buf_->data();
        size_ = buf_->size();
        assert(!pinned_);
      }

      void remove_suffix(size_t n) {
        assert(n <= size());
        if (pinned_) {
          size_ -= n;
        } else {
          buf_->erase(size() - n, n);
          PinSelf();
        }
      }

      void remove_prefix(size_t n) {
        assert(0);  // Not implemented
      }

      void Reset() {
        Cleanable::Reset();
        pinned_ = false;
      }

      inline std::string* GetSelf() { return buf_; }

      inline bool IsPinned() { return pinned_; }

     private:
      friend class PinnableSlice4Test;
      std::string self_space_;
      std::string* buf_;
      bool pinned_ = false;
    };

    inline bool operator==(const Slice& x, const Slice& y) {
        return ((x.size() == y.size()) &&
                (memcmp(x.data(), y.data(), x.size()) == 0));
    }

    inline bool operator!=(const Slice& x, const Slice& y) {
        return !(x == y);
    }

    inline int Slice::compare(const Slice& b) const {
        const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
        int r = memcmp(data_, b.data_, min_len);
        if (r == 0) {
            if (size_ < b.size_) r = -1;
            else if (size_ > b.size_) r = +1;
        }
        return r;
    }

}  // namespace insdb


#endif  // STORAGE_INSDB_INCLUDE_SLICE_H_
