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


#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <set>
#include "insdb/env.h"
#include "insdb/comparator.h"
#include "db/insdb_internal.h"
#include "db/keyname.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testharness.h"
#include "port/port.h"

namespace insdb {

    class ManifestTest{};

    TEST(ManifestTest, PrintMemoryFootprint) {
        printf("== struct size ==\n");
	printf("std::size_t %lu\n", sizeof(std::size_t));
        printf("SuperSKTable %lu\n", sizeof(SuperSKTable));
        printf("SKTableInfo %lu\n", sizeof(SKTableInfo));
        printf("Transaction %lu\n", sizeof(Transaction));
        printf("RequestType %lu\n", sizeof(enum RequestType));
        printf("Manifest %lu\n", sizeof(Manifest));
        printf("GCInfo %lu\n", sizeof(GCInfo));
        printf("SKTableMem %lu\n", sizeof(SKTableMem));
        printf("KeyBlock %lu\n", sizeof(KeyBlock));
        printf("KeyBlockPrimaryMeta %lu\n", sizeof(KeyBlockPrimaryMeta));
        printf("Spinlock %lu\n", sizeof(Spinlock));
        printf("port::Mutex %lu\n", sizeof(port::Mutex));
        printf("std::string %lu\n", sizeof(std::string));
        printf("Slice %lu\n", sizeof(Slice));
        printf("KeySlice %lu\n", sizeof(KeySlice));
        printf("PinnableSlice %lu\n", sizeof(PinnableSlice));
        printf("std::atomic<bool> %lu\n", sizeof(std::atomic<bool>));
        printf("UserKey %lu\n", sizeof(UserKey));
        printf("UserValue %lu\n", sizeof(UserValue));
        printf("ColumnData %lu\n", sizeof(UserKey::ColumnData));
        printf("ColumnNode %lu\n", sizeof(ColumnNode));
        printf("port::AtomicPointer %lu\n", sizeof(port::AtomicPointer));
        printf("SkipList<UserKey*, Slice, UserKeyComparator> %lu\n",sizeof(SkipList<UserKey*, Slice, UserKeyComparator>));
        printf("SimpleLinkedList %lu\n",sizeof(SimpleLinkedList));
    }
} // namespace insdb

int main(int argc, char **argv) {
    return insdb::test::RunAllTests();
}

