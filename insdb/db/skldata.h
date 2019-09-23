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


#ifndef STORAGE_INSDB_SKLDATA_H_
#define STORAGE_INSDB_SKLDATA_H_
#include <stdint.h>
#include "insdb/slice.h"
#include "insdb/comparator.h"
namespace insdb {

    template<typename Key, class Comparator>
        class SKLData {
            public:
                virtual ~SKLData(){}
                virtual void SetNode(void *InternalNode) = 0;
                virtual void *GetNode() = 0;
                virtual Key GetKeySlice() = 0;
                virtual bool CompareKey(Comparator cmp, const Key& key, int& delta) = 0;
        };

}
#endif //STORAGE_INSDB_DATA_H_

