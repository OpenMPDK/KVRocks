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

//
// Simple hash function used for internal data structures

#ifndef STORAGE_INSDB_UTIL_HASH_H_
#define STORAGE_INSDB_UTIL_HASH_H_

#include <stddef.h>
#include <stdint.h>

namespace insdb {

extern uint32_t Hash(const char* data, size_t n, uint32_t seed);

}

#endif  // STORAGE_INSDB_UTIL_HASH_H_
