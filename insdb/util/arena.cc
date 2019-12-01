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


#include "util/arena.h"
#include "util/coding.h"
#include <assert.h>

namespace insdb {
/* Delete entry managed by single linked list and each has 8B (4B: next offset: 4B: size) */
#define ARENA_DEL_SIZE_MASK     0x00000000FFFFFFFFUL
#define ARENA_DEL_OFFSET_MASK   0xFFFFFFFF00000000UL
uint32_t Arena::PreAllocate(uint32_t &bytes) {
    if (bytes == 0) return 0;
    if (bytes < 8) bytes = 8;
    uint32_t remains = arena_size_ - hdr_->allocated_bytes_;
    if (remains > bytes) {
        return hdr_->allocated_bytes_;
    }
#ifdef ALLOC_FROM_FREED
    else if (hdr_->freed_list_offset_) {
        char *del_entry =(char *)(GetBaseAddr() + hdr_->freed_list_offset_);
        uint32_t size = 0;
        uint32_t* del_offset_start = (uint32_t *)GetVarint32Ptr(del_entry, del_entry+8, &size);
        if (!del_offset_start) abort();
        if (size >= bytes) { /* get from header */
            return (del_entry - GetBaseAddr());
        } else {
            char *old_del_entry = del_entry;
            uint32_t *old_del_offset_start = del_offset_start;
            uint32_t offset = *del_offset_start;
            while(offset) {
                del_entry = (char *)(GetBaseAddr() + offset);
                del_offset_start = (uint32_t *)GetVarint32Ptr(del_entry, del_entry+8, &size);
                if (!del_offset_start) abort();
                if (size > bytes) {
                    return (del_entry - GetBaseAddr());
                }
                old_del_entry = del_entry;
                old_del_offset_start = del_offset_start;
                offset = *del_offset_start;
            }
        }
    }
#endif

    /* realloc will return hdr_->allocated_bytes_ */
    return hdr_->allocated_bytes_;
}

uint32_t Arena::Allocate(uint32_t &bytes) {
    if (bytes == 0) return 0;
    if (bytes < 8) bytes = 8;
retry:
    uint32_t remains = arena_size_ - hdr_->allocated_bytes_;
    if (remains > bytes) {
        char *addr = GetBaseAddr() + hdr_->allocated_bytes_;
        hdr_->allocated_bytes_ += bytes;
        EncodeVarint32(addr, bytes);
        return (addr - GetBaseAddr());
    }
#ifdef ALLOC_FROM_FREED
    else if (hdr_->freed_list_offset_) {
        char *del_entry =(char *)(GetBaseAddr() + hdr_->freed_list_offset_);
        uint32_t size = 0;
        uint32_t* del_offset_start = (uint32_t *)GetVarint32Ptr(del_entry, del_entry+8, &size);
        if (!del_offset_start) abort();
        if (size >= bytes) { /* get from header */
            /* update link */
            hdr_->freed_list_offset_ = *del_offset_start;
#ifdef ALLOC_FROM_FREED_DEBUG
            alloc_from_freed_fragment_ += (size - bytes);
            alloc_from_freed_ += size;
#endif
            bytes = size;
            hdr_->freed_bytes_ -= size;
            return (del_entry - GetBaseAddr());
        } else {
            char *old_del_entry = del_entry;
            uint32_t *old_del_offset_start = del_offset_start;
            uint32_t offset = *del_offset_start;
            while(offset) {
                del_entry = (char *)(GetBaseAddr() + offset);
                del_offset_start = (uint32_t *)GetVarint32Ptr(del_entry, del_entry+8, &size);
                if (!del_offset_start) abort();
                if (size > bytes) {
                    /* update link */
                    *old_del_offset_start = *del_offset_start;
#ifdef ALLOC_FROM_FREED_DEBUG
                    alloc_from_freed_fragment_ += (size - bytes);
                    alloc_from_freed_ += size;
#endif
                    bytes = size;
                    hdr_->freed_bytes_ -= bytes;
                    return (del_entry - GetBaseAddr());
                }
                old_del_entry = del_entry;
                old_del_offset_start = del_offset_start;
                offset = *del_offset_start;
            }
        }
    }
#endif

    /* realloc arena memory */
    uint32_t new_arena_size = arena_size_*2;
    char* mem = (char*)mmap(NULL, new_arena_size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (mem == MAP_FAILED) abort();
    memcpy(mem, GetBaseAddr(), hdr_->allocated_bytes_);
    char* old_mem = GetBaseAddr();
    uint32_t old_mem_size = arena_size_;
    hdr_ = (Arena_hdr*)mem;
    arena_size_ = new_arena_size;
    munmap((void*)old_mem, old_mem_size);
    goto retry;
}
void Arena::DeAllocate(uint32_t addr_offset) {
    uint32_t offset = 0;
    if (!addr_offset) return;
    assert(addr_offset > sizeof(Arena_hdr) && addr_offset < hdr_->allocated_bytes_);
    uint32_t size = 0;
    char* addr = GetBaseAddr()+ addr_offset;
    uint32_t* del_offset_start = (uint32_t *)GetVarint32Ptr(addr, addr+8, &size);
    if (!del_offset_start) abort();
    hdr_->freed_bytes_ += size;
    *del_offset_start =(uint32_t)hdr_->freed_list_offset_;
    hdr_->freed_list_offset_ = (uint32_t)(addr - GetBaseAddr());
}


void Arena::DumpFreeList() {
    uint32_t freed_size = 0;
    if (hdr_->freed_list_offset_) {
        printf("<Freed Memory info>\n");
        uint32_t i = 0;
        uint32_t size = 0;
        uint32_t offset = hdr_->freed_list_offset_;
        while(offset) {
            char *del_entry = (char*)(GetBaseAddr() + offset);
            uint32_t* del_offset_start = (uint32_t *)GetVarint32Ptr(del_entry, del_entry+8, &size);
            if (!del_offset_start) abort();
            printf("%d entry : addr(%p) - size(%d)\n", i, (GetBaseAddr() + offset), size);
            freed_size += size;
            offset = *del_offset_start;
            i++;
        }
    }
#ifdef ALLOC_FROM_FREED_DEBUG
    printf("allocaed mem(%d) - freed_mem(%d : %d) - alloc_from_freed(%d:%d)\n",
            hdr_->allocated_bytes_,
            hdr_->freed_bytes_,
            freed_size,
            alloc_from_freed_,
            alloc_from_freed_fragment_);
#endif
}

}  // namespace insdb
