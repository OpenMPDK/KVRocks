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


#ifndef STORAGE_INSDB_INCLUDE_COMPARATOR_H_
#define STORAGE_INSDB_INCLUDE_COMPARATOR_H_

#include <string>
#include "insdb/export.h"

namespace insdb {

    class Slice;

    // A Comparator object provides a total order across slices that are
    // used as keys in an sstable or a database.  A Comparator implementation
    // must be thread-safe since insdb may invoke its methods concurrently
    // from multiple threads.
    class INSDB_EXPORT Comparator {
        public:
            virtual ~Comparator();

            // Three-way comparison.  Returns value:
            //   < 0 iff "a" < "b",
            //   == 0 iff "a" == "b",
            //   > 0 iff "a" > "b"
            virtual int Compare(const Slice& a, const Slice& b) const = 0;

            // The name of the comparator.  Used to check for comparator
            // mismatches (i.e., a DB created with one comparator is
            // accessed using a different comparator.
            //
            // The client of this package should switch to a new name whenever
            // the comparator implementation changes in a way that will cause
            // the relative ordering of any two keys to change.
            //
            // Names starting with "insdb." are reserved and should not be used
            // by any clients of this package.
            virtual const char* Name() const = 0;

            // Advanced functions: these are used to reduce the space requirements
            // for internal data structures like index blocks.

            // If *start < limit, changes *start to a short string in [start,limit).
            // Simple comparator implementations may return with *start unchanged,
            // i.e., an implementation of this method that does nothing is correct.
            virtual void FindShortestSeparator(
                    std::string* start,
                    const Slice& limit) const = 0;

            // Changes *key to a short string >= *key.
            // Simple comparator implementations may return with *key unchanged,
            // i.e., an implementation of this method that does nothing is correct.
            virtual void FindShortSuccessor(std::string* key) const = 0;
    };

    // Return a builtin comparator that uses lexicographic byte-wise
    // ordering.  The result remains the property of this module and
    // must not be deleted.
    INSDB_EXPORT const Comparator* BytewiseComparator();

}  // namespace insdb

#endif  // STORAGE_INSDB_INCLUDE_COMPARATOR_H_
