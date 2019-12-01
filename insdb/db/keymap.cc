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
#include <stdint.h>
#include <cstring>
#include "db/keymap.h"
#include "db/dbformat.h"
#include "db/insdb_internal.h"
namespace insdb {
    /* KeyNode function */
    uint32_t Keymap::KeyNode::GetNodeSize() {
        uint32_t size;
#ifndef NDEBUG
        if(!GetVarint32Ptr(rep_, rep_ + 4, &size)) abort();
#else
        GetVarint32Ptr(rep_, rep_ + 4, &size);
#endif
        return size;
    }
    int Keymap::KeyNode::GetHeight() {
        uint32_t size;
        char* height_addr =const_cast<char*>(GetVarint32Ptr(rep_, rep_ + 4, &size));
        assert(height_addr);
        return (int) (*height_addr);
    }
    uint32_t* Keymap::KeyNode::NextAddrInfo(int *p_height, uint32_t *p_size) {
        uint32_t height = 0;
        uint32_t size = 0;
        char* height_addr = const_cast<char*>(GetVarint32Ptr(rep_, rep_ + 4, &size));
        assert(height_addr);
        if (p_height) *p_height = (*height_addr);
        height_addr++;
        if (p_size) *p_size = size;
        return (uint32_t *)height_addr;
    }
    void Keymap::KeyNode::InitNext() {
        int height = 0;
        uint32_t* next_addr = NextAddrInfo(&height);
        for (int i = 0; i < height; i++) next_addr[i] = 0;
    }

    Keymap::KeyNode* Keymap::KeyNode::Next(int n, char* base_addr, uint32_t *offset_ptr) {
        int height = 0;
        uint32_t* next_addr = NextAddrInfo(&height);
        assert(n >= 0 && n < height);
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        if (offset_ptr) *offset_ptr = next_addr[n];
        if (!next_addr[n]) return nullptr;
        return (KeyNode*)(base_addr + next_addr[n]);
    }
    uint32_t Keymap::KeyNode::NextOffset(int n) {
        int height = 0;
        uint32_t* next_addr = NextAddrInfo(&height);
        assert(n >= 0 && n < height);
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        return  next_addr[n];
    }
    void Keymap::KeyNode::SetNextOffset(int n, uint32_t next_offset) {
        int height = 0;
        uint32_t* next_addr = NextAddrInfo(&height);
        assert(n >= 0 && n < height);
        next_addr[n] = next_offset;
    }
    Slice Keymap::KeyNode::GetNodeKey(bool use_compact_key) {
        /*
         * return node internal key
         * [shared:vint32|non shared:vint32|key:non shared]
         */
        int height = 0;
        uint32_t size = 0;
        char* next_start = (char*) NextAddrInfo(&height, &size);
        next_start += height*sizeof(uint32_t); //move key start position
        size -= (next_start - rep_); //remain size
        Slice parse(next_start, size);
        if (use_compact_key) {
            uint32_t shared = 0, non_shared = 0;
#ifndef NDEBUG
            if (!GetVarint32(&parse, &shared)) abort();
            if (!GetVarint32(&parse, &non_shared)) abort();
#else
            GetVarint32(&parse, &shared);
            GetVarint32(&parse, &non_shared);
#endif
            size = (parse.data() - next_start) + non_shared; //internal key size
            return Slice(next_start, size);
        } 
        /*
         * return node internal key
         * [keysize:vint32|key]
         */
        uint32_t key_size = 0;
#ifndef NDEBUG
        if (!GetVarint32(&parse, &key_size)) abort();
#else
        GetVarint32(&parse, &key_size);
#endif
        return Slice(parse.data(), key_size);

    }
    Slice Keymap::KeyNode::GetKeyInfo(bool use_compact_key) {
        int height = 0;
        uint32_t size = 0, key_size = 0;
        char* next_start = (char*) NextAddrInfo(&height, &size);
        next_start += height*sizeof(uint32_t); //move key start position
        size -= (next_start - rep_); //remain size
        Slice parse(next_start, size);

        if (use_compact_key) {
#ifndef NDEBUG
            if (!GetVarint32(&parse, &key_size)) abort();
            if (!GetVarint32(&parse, &key_size)) abort();
#else
            GetVarint32(&parse, &key_size);
            GetVarint32(&parse, &key_size);
#endif
            parse.remove_prefix(key_size); // skip key string
        } else {
#ifndef NDEBUG
            if (!GetVarint32(&parse, &key_size)) abort();
#else
            GetVarint32(&parse, &key_size);
#endif
            parse.remove_prefix(key_size); // skip key string
        }
        uint16_t info_size = 0;
#ifndef NDEBUG
        if (!GetVarint16(&parse, &info_size)) abort(); // get key info size 
#else
        GetVarint16(&parse, &info_size); // get key info size 
#endif
        return Slice(parse.data(), info_size);
    }
    Slice Keymap::KeyNode::GetKeyInfoWithSize(bool use_compact_key) {
        int height = 0;
        uint32_t size = 0, key_size = 0;
        char* next_start = (char*) NextAddrInfo(&height, &size);
        next_start += height*sizeof(uint32_t); //move key start position
        size -= (next_start - rep_); //remain size
        Slice parse(next_start, size);
        if (use_compact_key) {
#ifndef NDEBUG
            if (!GetVarint32(&parse, &key_size)) abort();
            if (!GetVarint32(&parse, &key_size)) abort();
#else
            GetVarint32(&parse, &key_size);
            GetVarint32(&parse, &key_size);
#endif
            parse.remove_prefix(key_size); // skip key string
        } else {
#ifndef NDEBUG
            if (!GetVarint32(&parse, &key_size)) abort();
#else
            GetVarint32(&parse, &key_size);
#endif
            parse.remove_prefix(key_size); // skip key string
        }
        return parse;
    }
    KeyMeta* Keymap::KeyNode::GetKeyMeta(char* base_addr, bool use_compact_key, uint32_t* new_meta_offset, uint32_t *new_meta_size) {
        /* get key info */
        Slice keyinfo = GetKeyInfoWithSize(use_compact_key);
        if (new_meta_offset) *new_meta_offset = (keyinfo.data() - base_addr);
        if (new_meta_size) *new_meta_size = keyinfo.size();
        uint32_t alloc_size = 0;
        char* addr = const_cast<char*>(GetVarint32Ptr(keyinfo.data(), keyinfo.data() + 4, &alloc_size)); /* old keyinfo size */
        assert(addr);
        KeyMeta* key_meta = (KeyMeta*)addr;
        return key_meta->GetLatestKeyMeta(base_addr, new_meta_offset, new_meta_size);
    }
    void Keymap::KeyNode::Cleanup(char* base_addr, std::vector<uint32_t> &free_offset, bool use_compact_key) {
        /* get key info */
        Slice keyinfo = GetKeyInfoWithSize(use_compact_key);
        uint32_t alloc_size = 0;
        char* addr = const_cast<char*>(GetVarint32Ptr(keyinfo.data(), keyinfo.data() + 4, &alloc_size)); /* old keyinfo size */
        assert(addr);
        KeyMeta* key_meta = (KeyMeta*)addr;
        key_meta->CleanupKeyMeta(base_addr, free_offset);
        return;
    }

    Slice Keymap::KeyNode::InitNoCompact(int height, Slice key, uint32_t keyinfo_size, uint32_t all_size) {
        uint32_t len = 0;
        int size = 0;
        char* buf = nullptr; 
        char* addr =const_cast<char*>(GetVarint32Ptr(rep_, rep_ + 4, &len)); /* skip node size */
        assert(addr);
        assert(len == all_size);
        size = len - (addr - rep_);
        *addr = (uint8_t)height; /* add height */
        addr++;
        size--;
        assert(size >=  keyinfo_size); 
        uint32_t* next = (uint32_t*)addr; /* init next pointer */
        for (int i = 0; i < height; i++) {
            next[i] = 0;
            addr += sizeof(uint32_t);
            size -= sizeof(uint32_t);
        }
        assert(size >=  keyinfo_size); 
        buf = EncodeVarint32(addr, key.size()); /* key size */
        size -= (buf-addr);
        assert(size >=  keyinfo_size); 
        memcpy(buf, key.data(), key.size()); /* copy key */
        size -= key.size();
        buf += key.size();
        assert(size >=  keyinfo_size + 1); 
        addr = EncodeVarint32(buf, keyinfo_size);
        assert(size >= (addr - buf));
        size -= (addr - buf); 
        if (keyinfo_size == 0) return Slice(0);
        assert(size != 0);
        return Slice(addr, size);
    }



    Slice Keymap::KeyNode::Init(int height, Slice key, uint32_t shared, uint32_t non_shared, uint32_t keyinfo_size, uint32_t all_size) {
        uint32_t len = 0;
        int size = 0;
        char* buf = nullptr; 
        char* addr =const_cast<char*>(GetVarint32Ptr(rep_, rep_ + 4, &len)); /* skip node size */
        assert(addr);
        assert(len == all_size);
        size = len - (addr - rep_);
        *addr = (uint8_t)height; /* add height */
        addr++;
        size--;
        assert(size >=  keyinfo_size); 
        uint32_t* next = (uint32_t*)addr; /* init next pointer */
        for (int i = 0; i < height; i++) {
            next[i] = 0;
            addr += sizeof(uint32_t);
            size -= sizeof(uint32_t);
        }
        assert(size >=  keyinfo_size); 
        buf = EncodeVarint32(addr, shared); /* shared */
        size -= (buf-addr);
        assert(size >=  keyinfo_size); 
        addr = EncodeVarint32(buf, non_shared); /* non_shared */
        size -= (addr - buf);
        assert(size >=  keyinfo_size); 
        memcpy(addr, key.data() + shared, non_shared); /* copy key */
        size -= non_shared;
        addr += non_shared;
        assert(size >=  keyinfo_size+1); 
        buf = EncodeVarint32(addr, keyinfo_size);
        assert(size >= (buf - addr));
        size -= (buf - addr); 
        if (keyinfo_size == 0) return Slice(0);
        assert(size); 
        return Slice(buf, size);
    }


    /* ColMeta function */
    bool ColMeta::HasReference() {
        return (rep_[1] & first_flags_mask);
    }
    bool ColMeta::HasSecondChanceReference() {
        return ((rep_[1] & first_flags_mask) == first_flags_second_ref_bit);
    }
    void ColMeta::SetSecondChanceReference() {
        (rep_[1] = (rep_[1] & ~first_flags_mask) | first_flags_second_ref_bit);
    }
    void ColMeta::SetReference() {
        (rep_[1] |= first_flags_mask);
    }
    void ColMeta::ClearReference() {
        (rep_[1] &= ~first_flags_mask);
    }
    bool ColMeta::IsDeleted() {
        return (rep_[0] & flags_deleted);
    }
    uint8_t ColMeta::ColumnID() {
        return ((rep_[1] & ~first_flags_mask));
    }
    bool ColMeta::HasTTL() {
        return (rep_[2] & second_flags_ttl);
    }
    uint8_t ColMeta::KeyBlockOffset() {
        return (rep_[2] & ~second_flags_ttl);
    }
    uint8_t ColMeta::GetColSize() {
        return (rep_[0] & ~flags_deleted);
    } 

    uint64_t ColMeta::GetSequenceNumber() {
        uint64_t seq = 0;
        char* data_addr = rep_ + 3;
        data_addr = const_cast<char*>(GetVarint64Ptr(data_addr, data_addr + 8, &seq)); /* get iterator sequence number */
        assert(data_addr);
        return seq;
    }

    void ColMeta::ColInfo(ColumnData &col_data) {
        col_data.col_size = GetColSize();
        col_data.referenced = HasReference();
        col_data.is_deleted = IsDeleted();
        col_data.column_id = ColumnID();
        col_data.has_ttl = HasTTL();
        col_data.kb_offset = KeyBlockOffset();
        char* data_addr = rep_ + 3;
#if 0
        col_data.iter_sequence = *((uint64_t*)data_addr);
        data_addr += sizeof(uint64_t);
#else
        data_addr = const_cast<char*>(GetVarint64Ptr(data_addr, data_addr + 8, &col_data.iter_sequence)); /* get iterator sequence number */
        assert(data_addr);
#endif
        if (col_data.has_ttl) {
#if 0
            col_data.ttl = *((uint64_t*)data_addr);
            data_addr += sizeof(uint64_t);
#else
            data_addr = const_cast<char*>(GetVarint64Ptr(data_addr, data_addr + 8, &col_data.ttl)); /* get iterator sequence number */
            assert(data_addr);
#endif
        }
#if 0
        col_data.ikey_sequence = *((uint64_t*)data_addr);
        data_addr += sizeof(uint64_t);
#else
        data_addr = const_cast<char*>(GetVarint64Ptr(data_addr, data_addr + 8, &col_data.ikey_sequence)); /* get iterator sequence number */
        assert(data_addr);

        if (col_data.is_deleted) return;
#endif
#if 0
        col_data.ikey_size = *((uint32_t*)data_addr);
#else
        data_addr = const_cast<char*>(GetVarint32Ptr(data_addr, data_addr + 4, &col_data.ikey_size)); /* get iterator sequence number */
        assert(data_addr);
#endif
    }

    /* KeyMeta function */
    uint8_t KeyMeta::NRColumn() { /* can be recursive function */
        return  ((rep_[0] & ~flags_mask) + 1);
    }
    ColMeta* KeyMeta::ColMetaArray() { /* can be recursive function */
        return (ColMeta*)(rep_ + 1);
    }
    bool KeyMeta::IsRelocated() {
        return (rep_[0] & flags_all_column_relocated);
    }
    bool KeyMeta::HasUserKey() {
        return (rep_[0] & flags_userkey);
    }
    uint32_t KeyMeta::RelocatedOffset() {
        if (!(rep_[0] & flags_all_column_relocated)) return 0; /* not relocated */
        return (*((uint32_t*)(rep_ + 1)));
    }
    void* KeyMeta::UserKeyAddr() {
        if (!(rep_[0] & flags_userkey)) return nullptr;
        return (*((void**)(rep_ + 1)));
    }

    ColMeta* KeyMeta::GetColumn(uint16_t col_id) {
        uint8_t nr_col =  NRColumn();
        if (!nr_col) return nullptr; /* no columne */
        ColMeta* col_array = ColMetaArray();
        assert(col_array);
        char* col_addr = (char*)col_array;
        for (uint8_t i = 0; i < nr_col; i++) {
            if (col_array->ColumnID() == col_id) return col_array;
            col_array = (ColMeta*)(col_addr + col_array->GetColSize());
        }
        return nullptr; 
    }
    KeyMeta* KeyMeta::GetLatestKeyMeta(char* base_addr, uint32_t* new_meta_offset, uint32_t *new_meta_size) {
        if (!IsRelocated()) return this; /* not reloacated */
        uint32_t reloc_offset = RelocatedOffset();
        uint32_t len = 0;
        if (new_meta_offset) *new_meta_offset = reloc_offset;
        char* data_addr = base_addr + reloc_offset;
        char* keymeta_addr = const_cast<char*>(GetVarint32Ptr(data_addr, data_addr + 4, &len)); /* skip node size */
        assert(keymeta_addr);
        if (new_meta_size) *new_meta_size = len;
        len -= (keymeta_addr - data_addr); /* get data len */
        assert(len); 
        return (KeyMeta*)keymeta_addr;
    }
    void KeyMeta::CleanupKeyMeta(char* base_addr, std::vector<uint32_t> &free_offset) {
        if (IsRelocated()) {
            uint32_t len = 0;
            uint32_t reloc_offset = RelocatedOffset();
            free_offset.push_back(reloc_offset);
        }
        return; 
    }
    uint32_t KeyMeta::CurrentMetaSize(char* base_addr, std::vector<SnapInfo> &snapinfo_list) {
        uint32_t alloc_size = 0;
        KeyMeta* meta = GetLatestKeyMeta(base_addr);
        assert(meta);
        int nr_col =  meta->NRColumn();
        alloc_size++;
        if (!nr_col) return 0; /* no column - remove */
        ColMeta* col_array = meta->ColMetaArray();
        assert(col_array); 
        int remain_col = nr_col; 
        char* col_addr = nullptr;
        for (int i = 0; i < nr_col; i++) {
            uint8_t col_size = col_array->GetColSize();
            col_addr = (char*)col_array;
            uint64_t seq = col_array->GetSequenceNumber();
            if (snapinfo_list.size()) {
                int32_t snap_index = 0;
                int32_t nr_snap = snapinfo_list.size();
                bool iter = false;
                while(snap_index < nr_snap && seq <= snapinfo_list[snap_index].cseq ){
                    if(!col_array->IsDeleted()){
                        iter = true;
                        break; 
                    }
                    ++snap_index;
                }
                if (iter) alloc_size += col_size;
                else {
                    if (!(--remain_col)) return 0;
                }
            } else {
                if (!col_array->IsDeleted()) alloc_size += col_size;
                else {
                    if (!(--remain_col)) return 0;
                }
            }
            col_array = (ColMeta*)(col_addr + col_size);
        }
        return alloc_size; 
    }

    void KeyMeta::CopyCurrentMeta(char* dest_addr, uint32_t allocated_size, char* base_addr, std::vector<SnapInfo> &snapinfo_list) {
        uint32_t copied_size = 0;
        char* cur_addr = dest_addr;
        KeyMeta* meta = GetLatestKeyMeta(base_addr);
        assert(meta);
        int nr_col =  meta->NRColumn();
        assert(nr_col);
        cur_addr++;
        copied_size++;
        ColMeta* col_array = meta->ColMetaArray();
        assert(col_array);
        char* col_addr = nullptr;
        int remain_col = nr_col; 
        for (int i = 0; i < nr_col; i++) {
            uint8_t col_size = col_array->GetColSize();
            col_addr = (char*)col_array;
            uint64_t seq = col_array->GetSequenceNumber();
            if (snapinfo_list.size()) {
                int32_t snap_index = 0;
                int32_t nr_snap = snapinfo_list.size();
                bool iter = false;
                while(snap_index < nr_snap && seq <= snapinfo_list[snap_index].cseq ){
                    if(!col_array->IsDeleted()){
                        iter = true;
                        break; 
                    }
                    ++snap_index;
                }
                if (iter){
                    assert((copied_size + col_size) <= allocated_size);
                    memcpy(cur_addr, (char*)col_array, col_size);
                    cur_addr += col_size;
                } else {
#ifndef NDEBUG
                    if (!(--remain_col)) abort();
#else
                    --remain_col;
#endif
                }
            } else {
                if (!col_array->IsDeleted()) {
                    assert((copied_size + col_size) <= allocated_size);
                    memcpy(cur_addr, (char*)col_array, col_size);
                    cur_addr += col_size;
                } else { 
#ifndef NDEBUG
                    if (!(--remain_col)) abort();
#else
                    --remain_col;
#endif
                } 
            } 
            col_array = (ColMeta*)(col_addr + col_size);
        }
        assert(remain_col);
        *dest_addr = (remain_col -1); /* update remain column */
        return; 
    }


    void KeyMeta::GetiKeySeqNum(std::set<uint64_t> &KeysList) {
        int nr_col =  NRColumn();
        assert(nr_col);
        ColMeta* col_array = ColMetaArray();
        assert(col_array);
        char* col_addr = nullptr;
        ColumnData col_data;
        for (int i = 0; i < nr_col; i++) {
            col_data.init();
            uint8_t col_size = col_array->GetColSize();
            col_array->ColInfo(col_data);
            KeysList.insert(col_data.ikey_sequence);
            col_addr = (char*)col_array;
            col_array = (ColMeta*)(col_addr + col_size);
        }
        return;  
    }


 
    /* KeymapComparator function */
    int Keymap::KeymapComparator::operator()(Slice begin_key/*begin_key*/, Slice node_key/*node key*/, Slice user_key/* key*/) const {
        /*
         * key format:
         * begin_key : [key:key_size]
         * node key : [shared:vint32|non_shared:vint32|key:non_shared]
         * user key: [key:key size]
         */
        uint32_t shared = 0, non_shared = 0;
        Slice parse(node_key.data(), node_key.size());
#ifndef NDEBUG
        if (!GetVarint32(&parse, &shared)) abort();
        if (!GetVarint32(&parse, &non_shared)) abort();
#else
        GetVarint32(&parse, &shared);
        GetVarint32(&parse, &non_shared);
#endif
        std::string key_str;
        if (shared) key_str.append(begin_key.data(), shared);
        if (non_shared) key_str.append(parse.data(), non_shared);
        Slice key(key_str.data(), key_str.size());
        if (key.size() == 0 || user_key.size() == 0) {
            if (key.size() == 0 && user_key.size() == 0) return 0;
            if (key.size() == 0) return -1;
            return 1;
        }
        return keymap_comparator->Compare(key, user_key);
    }
    /* KeymapNoCompactComparator function */
    int Keymap::KeymapNoCompactComparator::operator()(Slice node_key/*node key*/, Slice user_key/* key*/) const {
        if (node_key.size() == 0 || user_key.size() == 0) {
            if (node_key.size() == 0 && user_key.size() == 0) return 0;
            if (node_key.size() == 0) return -1;
            return 1;
        }
        return keymap_comparator->Compare(node_key, user_key);
    }


    /* keymap function */
    Keymap::Keymap(KeySlice begin_key, const Comparator *cmp)
        : compare_nocompact_(cmp), compare_(cmp), rnd_(0xdeadbeef), hashmap_(new KeyMapHashMap(1024*32)), use_compact_key_(false), begin_key_offset_(0), begin_key_size_(0), ref_cnt_(1), op_count_(0), cmp_count_(0) {
            key_str_.resize(0);
            key_str_.append(begin_key.data(), begin_key.size());
            key_hash_ = begin_key.GetHashValue();
            arena_ = nullptr;
#ifdef INSDB_GLOBAL_STATS
        g_new_keymap_cnt++;
#endif
    }

    Keymap::Keymap(Arena* arena, const Comparator *cmp)
        : compare_nocompact_(cmp), compare_(cmp), rnd_(0xdeadbeef), hashmap_(new KeyMapHashMap(1024*32)), use_compact_key_(false),  begin_key_offset_(0), begin_key_size_(0), ref_cnt_(1), op_count_(0), cmp_count_(0) {
            arena_ = arena;
            KeySlice begin_key = GetBeginKey();
            key_str_.resize(0);
            key_str_.append(begin_key.data(), begin_key.size());
            key_hash_ = begin_key.GetHashValue();
            KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
            use_compact_key_ = (hdr->use_compact_key) ? true : false ;
#ifdef INSDB_GLOBAL_STATS
        g_new_keymap_cnt++;
#endif
    }

    Keymap::Keymap(Arena* arena, const Comparator *cmp, KeyMapHashMap* hashmap)
        : compare_nocompact_(cmp), compare_(cmp), rnd_(0xdeadbeef), hashmap_(hashmap), use_compact_key_(false),  begin_key_offset_(0), begin_key_size_(0), ref_cnt_(1), op_count_(0), cmp_count_(0) {
            arena_ = arena;
            KeySlice begin_key = GetBeginKey();
            key_str_.resize(0);
            key_str_.append(begin_key.data(), begin_key.size());
            key_hash_ = begin_key.GetHashValue();
            KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
            use_compact_key_ = (hdr->use_compact_key) ? true : false ;
#ifdef INSDB_GLOBAL_STATS
        g_new_keymap_cnt++;
#endif
    }



    Keymap::~Keymap() {
        if (arena_) delete arena_;
        CleanupHashMap();
#ifdef INSDB_GLOBAL_STATS
        g_del_keymap_cnt++;
#endif
    }

    void Keymap::CleanupHashMap() {
        if (!hashmap_) return;
        if (!hashmap_->empty()) {
            for(auto it = hashmap_->cbegin(); it != hashmap_->cend(); ++it) {
                std::string* offset_str = it->second;
                if (offset_str) delete offset_str;
            }
        }
        delete hashmap_;
        hashmap_ = nullptr;
    }

    void Keymap::InsertArena(Arena* arena, KeyMapHashMap *hashmap) {
            if (arena_) delete arena_;
            arena_ = arena;
            if (arena == nullptr && hashmap_) {
                CleanupHashMap();
                hashmap_ = hashmap;
                return;
            }
            if (hashmap) {
                if (hashmap_) CleanupHashMap();
                hashmap_ = hashmap;
            }
            if (arena_) {
                Slice saved_beginkey = GetBeginKey();
                Slice cur_beginkey = Slice(key_str_.data(), key_str_.size());
                assert(!saved_beginkey.compare(cur_beginkey));
                KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
                use_compact_key_ = (hdr->use_compact_key) ? true : false ;
            }
            return;
    }

    bool Keymap::BuildHashMapData(std::string &hash_data) {
        uint32_t total_count = 0;
        hash_data.resize(0);
        hash_data.append(8, 0x0); /* skip crc and nr count */
        for(auto it = hashmap_->cbegin(); it != hashmap_->cend();++it) {
            std::string* offset_str = it->second;
            if (offset_str) {
                uint32_t *pcount = (uint32_t*)(const_cast<char*>(offset_str->data()));
                uint32_t *offset_array = pcount + 1;
                uint32_t count = *pcount;
                for (uint32_t i = 0; i < count; i++) {
                    uint64_t entry = (uint64_t)(((uint64_t)it->first << 32) | (uint64_t)(offset_array[i]));
                    hash_data.append((char*)&entry, 8);
                    total_count++;
                }
            }
        }
        uint32_t* addr = (uint32_t*)(const_cast<char*>(hash_data.data()));
        addr[1] = total_count;
        uint32_t size = hash_data.size();
        uint32_t peding_size = (size % kRequestAlignSize) ? (kRequestAlignSize - (size % kRequestAlignSize)) : 0;
        hash_data.append(peding_size, 0xcf);
        return true;
    }

#define KEYMAP_DEV_OFFSET_MASK  0x00000000FFFFFFFFL
    KeyMapHashMap* BuildKeymapHashmapWithString(std::string &hash_data) {
        if (hash_data.size() < 8) return nullptr;
        KeyMapHashMap* hashmap = new KeyMapHashMap(1024*32);
        uint32_t* addr = (uint32_t*)(const_cast<char*>(hash_data.data()));
        uint32_t count = addr[1];
        uint64_t *entry = (uint64_t*)&addr[2];
        for (uint32_t i = 0; i < count; i++) {
            uint32_t hashval = (uint32_t)(entry[i] >> 32);
            uint32_t offset = (uint32_t)(entry[i] & KEYMAP_DEV_OFFSET_MASK);
            auto it = hashmap->find(hashval);
            if (it != hashmap->end()) {
                std::string *offset_str = it->second;
                assert(offset_str);
                offset_str->append((char*)&offset, 4);
                uint32_t *pcount = (uint32_t*)(const_cast<char*>(offset_str->data()));
                *pcount = *pcount + 1; 
            } else { 
                std::string *offset_str = new std::string();
                uint32_t nr_count = 1;
                offset_str->append((char*)&nr_count, 4);
                offset_str->append((char*)&offset, 4);
                auto ret = hashmap->insert(hashval, offset_str);
                if (!ret.second) delete offset_str;
            }
        }
        return hashmap;
    }


    /* used for initialize arean for key map */
    void Keymap::InitArena(Arena* arena, bool use_compact_key) {
        /*
         * copy begin key
         * [node size:vint32|key_size:vint32|key:key size]
         */
        uint32_t begin_key_size = key_str_.size() + VarintLength(key_str_.size()) + sizeof(uint32_t);
        uint32_t begin_key_offset = arena->Allocate(begin_key_size);
        assert(begin_key_offset);
        char* addr = arena->GetBaseAddr() + begin_key_offset;
        uint32_t len = 0;
        char* buf = const_cast<char*>(GetVarint32Ptr(addr, addr + 4, &len)); /* skip node size */
        int size = len;
        size -= (buf - addr);
        assert(size >= 0);
        char* key_addr = EncodeVarint32(buf, key_str_.size()); /* begin_key size */
        size -= (key_addr - buf);
        assert(size >= key_str_.size());
        memcpy(key_addr, key_str_.data(), key_str_.size()); /* begin_key */
        arena->SetBeginKeyOffset(begin_key_offset); /* set begin key offset */
        /*
         * Set Header
         */
        int height = kMaxHeight;
        uint32_t est_size = 0;
        uint32_t addr_offset = 0;
        KeyNode* node = nullptr;
        if (UseCompact()) {
            est_size = sizeof(uint8_t)/*height*/ + sizeof(uint32_t)*height/*nexts */
                + VarintLength(0)/*shared key size*/ + VarintLength(0) /*non shared key size*/ + 0/*encode key*/
                + 0 /* keyinfo_size */ + VarintLength(0);
            est_size += VarintLength(est_size); /* node size */
            addr_offset = arena->Allocate(est_size);
            assert(addr_offset);
            node = (KeyNode*)(arena->GetBaseAddr() + addr_offset);
            node->Init(height, 0, 0, 0, 0, est_size);
        } else {
            est_size = sizeof(uint8_t)/*height*/ + sizeof(uint32_t)*height/*nexts */
                + VarintLength(0)/*key size*/ + 0/*encode key*/
                + 0 /* keyinfo_size */ + VarintLength(0);
            est_size += VarintLength(est_size); /* node size */
            addr_offset = arena->Allocate(est_size);
            assert(addr_offset);
            node = (KeyNode*)(arena->GetBaseAddr() + addr_offset);
            node->InitNoCompact(height, 0, 0, est_size);
 
        }
        node->InitNext();
        arena->SetHeadNodeOffset(addr_offset);
        arena->SetMaxHeight(1);
        KEYMAP_HDR* hdr = (KEYMAP_HDR*)arena->GetBaseAddr();
        hdr->use_compact_key = (use_compact_key) ? 1 : 0;
    }

    void Keymap::SetSKTableID(uint32_t skt_id) {
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        hdr->skt_id = skt_id;
    } 
    void Keymap::SetPreAllocSKTableID(uint32_t skt_id) {
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        hdr->prealloc_skt_id = skt_id;
    }
    void Keymap::SetNextSKTableID(uint32_t skt_id){
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        hdr->next_skt_id = skt_id;
    }
    uint32_t Keymap::IncCurColInfoID(){
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        uint32_t old = hdr->last_colinfo++;
        return old;
    }
    void Keymap::SetPrevColInfoID(){
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        hdr->start_colinfo = hdr->last_colinfo;
    }
    uint32_t Keymap::GetSKTableID() {
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        return hdr->skt_id;
    }
    uint32_t Keymap::GetPreAllocSKTableID() {
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        return hdr->prealloc_skt_id;
    }
    uint32_t Keymap::GetNextSKTableID(){
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        return hdr->next_skt_id;
    }
    uint32_t Keymap::GetPrevColInfoID(){
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        return hdr->start_colinfo;
    }
    uint32_t Keymap::GetCurColInfoID(){
        assert(HasArena());
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(GetBaseAddr());
        return hdr->last_colinfo;
    }



    Keymap::KeyNode* Keymap::NewNode(
            const Slice key, uint32_t keyinfo_size, int height, uint32_t &addr_offset, Slice &keyinfo){
        uint32_t est_size = 0;
        uint32_t init_est_size = 0;
        uint32_t shared = 0;
        KeyNode* node = nullptr;
        if (UseCompact()) {
            Slice begin_key = GetBeginKey();
            const size_t min_length = std::min(begin_key.size(), key.size());
            while ((shared < min_length) && begin_key[shared] == key[shared]) shared++;
            uint32_t non_shared = key.size() - shared;
            init_est_size = sizeof(uint8_t)/*height*/ + sizeof(uint32_t)*height/*nexts */
                + VarintLength(shared)/*shared key size*/ + VarintLength(non_shared) /*non shared key size*/ + non_shared/*encode key*/
                + keyinfo_size + VarintLength(keyinfo_size); 
            est_size = init_est_size + VarintLength(init_est_size); /* node size */
            if ((init_est_size / 128) != (est_size / 128)) est_size++; /* total size overflow .. need to increase*/
            addr_offset = arena_->Allocate(est_size);
            assert(addr_offset);
            node = (KeyNode*)(GetBaseAddr() + addr_offset);
            keyinfo = node->Init(height, key, shared, non_shared, keyinfo_size, est_size);
        } else {
            init_est_size = sizeof(uint8_t)/*height*/ + sizeof(uint32_t)*height/*nexts */
                + VarintLength(key.size())/*key size*/ + key.size()/*encode key*/
                + keyinfo_size + VarintLength(keyinfo_size);
            est_size = init_est_size + VarintLength(init_est_size); /* node size */
            if ((init_est_size / 128) != (est_size / 128)) est_size++; /* total size overflow .. need to increase*/
            addr_offset = arena_->Allocate(est_size);
            assert(addr_offset);
            node = (KeyNode*)(GetBaseAddr() + addr_offset);
            keyinfo = node->InitNoCompact(height, key, keyinfo_size, est_size);
        }
        assert(keyinfo.size() == keyinfo_size);
        return node;
    }

    int Keymap::RandomHeight() {
        // Increase height with probability 1 in kBranching
        static const unsigned int kBranching = 4;
        int height = 1;
        while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
            height++;
        }
        assert(height <= kMaxHeight);
        return height;
    }

    bool Keymap::KeyIsAfterNode(const Slice& key, KeyNode* n) {
        cmp_count_++;
        // null n is considered infinite
        if (UseCompact()) {
            return (n != nullptr) && (compare_(GetBeginKey(), n->GetNodeKey(UseCompact()), key) < 0);
        }
        return (n != nullptr) && (compare_nocompact_(n->GetNodeKey(UseCompact()), key) < 0);
    }

    uint32_t Keymap::FindGreaterOrEqual(const Slice& key, uint32_t* prev) {
        KeyNode* x = GetHeadNode();
        uint32_t x_offset = GetHeadNodeOffset();
        int level = GetMaxHeight() - 1;
        uint32_t next_offset = 0;
        while (true) {
            KeyNode* next = x->Next(level, GetBaseAddr(), &next_offset);
            if (KeyIsAfterNode(key, next)) {
                // Keep searching in this list
                x = next;
                x_offset = next_offset;
            } else {
                if (prev != nullptr) prev[level] = x_offset;
                if (level == 0) {
                    return next_offset;
                } else {
                    // Switch to next list
                    level--;
                }
            }
        }
    }

    uint32_t Keymap::FindLessThan(const Slice& key) {
        KeyNode* x = GetHeadNode();
        uint32_t x_offset = GetHeadNodeOffset();
        int level = GetMaxHeight() - 1;
        uint32_t next_offset = 0;
        while (true) {
            assert(x == GetHeadNode() || Compare(x->GetNodeKey(UseCompact()), key) < 0);
            KeyNode* next = x->Next(level, GetBaseAddr(), &next_offset);
            if (next == nullptr || Compare(next->GetNodeKey(UseCompact()), key) >= 0) {
                if (level == 0) {
                    return x_offset;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
                x_offset = next_offset;
            }
        }
    }


    uint32_t Keymap::FindLessThanEqual(const Slice& key) {
        KeyNode* x = GetHeadNode();
        uint32_t x_offset = GetHeadNodeOffset();
        int level = GetMaxHeight() - 1;
        uint32_t next_offset = 0;
        while (true) {
            assert(x == GetHeadNode() || Compare(x->GetNodeKey(UseCompact()), key) <= 0);
            KeyNode* next = x->Next(level, GetBaseAddr(), &next_offset);
            if (next == nullptr || Compare(next->GetNodeKey(UseCompact()), key) > 0) {
                if (level == 0) {
                    return x_offset;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
                x_offset = next_offset;
            }
        }
    }


    uint32_t Keymap::FindLast() {
        KeyNode* x = GetHeadNode();
        uint32_t x_offset = GetHeadNodeOffset();
        int level = GetMaxHeight() - 1;
        uint32_t next_offset = 0;
        while (true) {
            KeyNode* next = x->Next(level, GetBaseAddr(), &next_offset);
            if (next == nullptr) {
                if (level == 0) {
                    return x_offset;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
                x_offset = next_offset;
            }
        }
    }

    Slice Keymap::_Insert(const KeySlice& key, uint32_t keyinfo_size, KeyMapHandle &node_offset) {
        op_count_++;
        node_offset = 0;
        uint32_t prev[kMaxHeight];
        uint32_t x_offset = FindGreaterOrEqual(key, prev);
        KeyNode* x = (KeyNode*)GetAddr(x_offset);
        // Our data structure does not allow duplicate insertion
        //assert(x == nullptr || !Equal(x->GetNodeKey(UseCompact()), key));
        if (x  && Equal(x->GetNodeKey(UseCompact()),key)) {
            node_offset = x_offset; 
            return Slice(0);
        }

        int height = RandomHeight();
        if (height > GetMaxHeight()) {
            for (int i = GetMaxHeight(); i < height; i++) {
                prev[i] = GetHeadNodeOffset();
            }
            SetMaxHeight(height);
        }
        Slice keyinfo(0);
        x = NewNode(key, keyinfo_size, height, x_offset, keyinfo);
        uint32_t* next_addr =  x->NextAddrInfo();
        for (int i = 0; i < height; i++) {
            next_addr[i] =((KeyNode*)GetAddr(prev[i]))->NextOffset(i); /* update next link */
            ((KeyNode*)GetAddr(prev[i]))->SetNextOffset(i, x_offset); /* update prev link */
   
        }
        node_offset = x_offset;
        IncCount();
        return keyinfo;
    }


    Slice Keymap::Insert(const KeySlice& key, uint32_t keyinfo_size, KeyMapHandle &node_offset) {
        Slice keyinfo = _Insert(key, keyinfo_size, node_offset);
#ifdef USE_HASHMAP
        if (!keyinfo.size()) return keyinfo;
        auto it = hashmap_->find(key.GetHashValue());
        if (it != hashmap_->end()) {
            std::string *offset_str = it->second;
            assert(offset_str);
            offset_str->append((char*)&node_offset, 4);
            uint32_t *pcount = (uint32_t*)(const_cast<char*>(offset_str->data()));
            *pcount = *pcount + 1; 
        } else { 
            std::string *offset_str = new std::string();
            uint32_t nr_count = 1;
            offset_str->append((char*)&nr_count, 4);
            offset_str->append((char*)&node_offset, 4);
            auto ret = hashmap_->insert(key.GetHashValue(), offset_str);
            if (!ret.second) delete offset_str;
        }
#endif
        return keyinfo;
    }

    Slice Keymap::_ReplaceOrInsert(const KeySlice& key, uint32_t keyinfo_size, KeyMapHandle &node_offset, bool& existing) {
        op_count_++;
        existing = false;
        uint32_t prev[kMaxHeight];
        uint32_t x_offset = FindGreaterOrEqual(key, prev);
        KeyNode* x = (KeyNode*)GetAddr(x_offset);
        if (x  && Equal(x->GetNodeKey(UseCompact()), key)) {
            Slice orig_keyinfo = x->GetKeyInfoWithSize(UseCompact());
            if (orig_keyinfo.size() > keyinfo_size) { /* check in-place update */
                node_offset = x_offset;
                existing = true;
                return orig_keyinfo;
            } else { /* replace internal node. */
                /* step 1. keep node offset, height and next info */
                uint32_t orig_next[kMaxHeight];
                uint32_t orig_node_offset = x_offset;
                int height;
                uint32_t* next_addr = x->NextAddrInfo(&height);
                for (int i = 0; i < height; i++) orig_next[i] = next_addr[i];
                /* step 2. create new node */
                Slice keyinfo(0);
                x = NewNode(key, keyinfo_size, height, x_offset, keyinfo);
                /* step 3. update next/prev link */
                next_addr =  x->NextAddrInfo();
                for (int i = 0; i < height; i++) {
                    next_addr[i] = orig_next[i]; /* copy next info */
                    ((KeyNode*)GetAddr(prev[i]))->SetNextOffset(i, x_offset); /* update prev link */
                }
                /* step 4. delete x */
                std::vector<uint32_t> free_offset;
                KeyNode* orig_node = (KeyNode*)GetAddr(orig_node_offset);
                orig_node->Cleanup(GetBaseAddr(), free_offset, UseCompact());
                for (int i = 0; i < free_offset.size(); i++) {
                    arena_->DeAllocate(free_offset[i]);
                }
                arena_->DeAllocate(orig_node_offset);
#ifdef USE_HASHMAP
                if (orig_node_offset) {
                    auto it = hashmap_->find(key.GetHashValue());
                    if (it != hashmap_->end()) {
                        std::string *offset_str = it->second;
                        uint32_t *pcount = (uint32_t*)(const_cast<char*>(offset_str->data()));
                        uint32_t *offset_array = pcount + 1;
                        uint32_t count = *pcount;
                        for (uint32_t i = 0; i < count; i++) {
                            if(offset_array[i] == orig_node_offset) {
                                memcpy((char*)(offset_array + i), (char*)(offset_array +i +1), (count -1 -i)* sizeof(uint32_t)); /* remove node_offset */
                                offset_str->resize(offset_str->size() - 4);
                                *pcount = count -1;
                                break;
                            }
                        }
                    }
                }
#endif
                node_offset = x_offset;
                assert(keyinfo.size() >= keyinfo_size);
                return keyinfo;
            }
        }
        int height = RandomHeight();
        if (height > GetMaxHeight()) {
            for (int i = GetMaxHeight(); i < height; i++) {
                prev[i] = GetHeadNodeOffset();
            }
            SetMaxHeight(height);
        }
        Slice keyinfo(0);
        x = NewNode(key, keyinfo_size, height, x_offset, keyinfo);
        uint32_t* next_addr =  x->NextAddrInfo();
        for (int i = 0; i < height; i++) {
            next_addr[i] =((KeyNode*)GetAddr(prev[i]))->NextOffset(i); /* update next link */
            ((KeyNode*)GetAddr(prev[i]))->SetNextOffset(i, x_offset); /* update prev link */
        }
        node_offset = x_offset;
        IncCount();
        return keyinfo;
    }


    Slice Keymap::ReplaceOrInsert(const KeySlice& key, uint32_t keyinfo_size, KeyMapHandle &node_offset) {
        bool existing = false;
        Slice keyinfo =_ReplaceOrInsert(key, keyinfo_size, node_offset, existing);
#ifdef USE_HASHMAP
        if (existing) return keyinfo;

        auto it = hashmap_->find(key.GetHashValue());
        if (it != hashmap_->end()) {
            std::string *offset_str = it->second;
            assert(offset_str);
            offset_str->append((char*)&node_offset, 4);
            uint32_t *pcount = (uint32_t*)(const_cast<char*>(offset_str->data()));
            *pcount = *pcount + 1; 
        } else {
            std::string *offset_str = new std::string();
            uint32_t nr_count = 1;
            offset_str->append((char*)&nr_count, 4);
            offset_str->append((char*)&node_offset, 4);
            auto ret = hashmap_->insert(key.GetHashValue(), offset_str);
            if (!ret.second) delete offset_str;
        }
#endif
        return keyinfo;
 
    }


    bool Keymap::_Remove(const KeySlice& key, KeyMapHandle* handle) {
        op_count_++;
        uint32_t prev[kMaxHeight];
        uint32_t x_offset = FindGreaterOrEqual(key, prev);
        KeyNode* x = (KeyNode*)GetAddr(x_offset);
        if (!x) return true;
        if (!Equal(x->GetNodeKey(UseCompact()), key)) return true;

        if (handle) *handle = x_offset;
        int height;
        uint32_t* next_addr = x->NextAddrInfo(&height);
        /* update prev link to x's next link */
        for (int i = 0; i < height; i++)
            ((KeyNode*)GetAddr(prev[i]))->SetNextOffset(i, next_addr[i]);
        std::vector<uint32_t> free_offset;
        x->Cleanup(GetBaseAddr(), free_offset, UseCompact());
        for (int i = 0; i < free_offset.size(); i++) {
            arena_->DeAllocate(free_offset[i]);
        }
        arena_->DeAllocate(x_offset);
        DecCount();
        return true;
    }

    bool Keymap::Remove(const KeySlice& key) {
        KeyMapHandle node_offset = 0;
        bool result = _Remove(key, &node_offset);
#ifdef USE_HASHMAP
        if (node_offset) {
            auto it = hashmap_->find(key.GetHashValue());
            if (it != hashmap_->end()) {
                std::string *offset_str = it->second;
                uint32_t *pcount = (uint32_t*)(const_cast<char*>(offset_str->data()));
                uint32_t *offset_array = pcount + 1;
                uint32_t count = *pcount;
                for (uint32_t i = 0; i < count; i++) {
                    if(offset_array[i] == node_offset) {
                        memcpy((char*)(offset_array + i), (char*)(offset_array + i + 1), (count -1 -i)* sizeof(uint32_t)); /* remove node_offset */
                        offset_str->resize(offset_str->size() - 4);
                        *pcount = count -1;
                        break;
                    }
                }
            }
        }
#endif
        return result;
    }


    KeyMapHandle Keymap::_Search(const KeySlice& key) {
        if (!HasArena()) return 0;
        uint32_t prev[kMaxHeight];
        uint32_t x_offset = FindGreaterOrEqual(key, nullptr);
        KeyNode* x = (KeyNode*)GetAddr(x_offset);
        if (x != nullptr && Equal(x->GetNodeKey(UseCompact()), key)) return x_offset;
        return 0;
    }

    
#define SEARCH_SKIPLIST
    KeyMapHandle Keymap::Search(const KeySlice& key) {
        op_count_++;
#ifdef USE_HASHMAP
        auto it = hashmap_->find(key.GetHashValue());
        if (it != hashmap_->end()) {
            std::string *offset_str = it->second;
            uint32_t *pcount = (uint32_t*)(const_cast<char*>(offset_str->data()));
            uint32_t *offset_array = pcount + 1;
            uint32_t count = *pcount;
            for (uint32_t i = 0; i < count; i++) {
                KeyNode* x = (KeyNode*)GetAddr(offset_array[i]);
                if (Equal(x->GetNodeKey(UseCompact()), key)) return offset_array[i];
            }
        }
#endif
        KeyMapHandle handle = 0;
#ifdef SEARCH_SKIPLIST
        handle = _Search(key);
#ifdef USE_HASHMAP
        if (handle) {
            std::string *offset_str = new std::string();
            uint32_t nr_count = 1;
            offset_str->append((char*)&nr_count, 4);
            offset_str->append((char*)&handle, 4);
            auto ret = hashmap_->insert(key.GetHashValue(), offset_str);
            if (!ret.second) delete offset_str; 
        }
#endif
#endif
        return handle;
    }


    KeyMapHandle Keymap::SearchLessThan(const KeySlice& key) {
        op_count_++;
        if (!HasArena()) return 0;
        uint32_t x_offset = FindLessThan(key);
        if (x_offset == GetHeadNodeOffset()) return 0;
        return x_offset;
    }

    KeyMapHandle Keymap::SearchLessThanEqual(const KeySlice& key) {
        op_count_++;
        if (!HasArena()) return 0;
        uint32_t x_offset = FindLessThanEqual(key);
        if (x_offset == GetHeadNodeOffset()) return 0;
        return x_offset;
    }

    KeyMapHandle Keymap::SearchGreaterOrEqual(const KeySlice& key) {
        op_count_++;
        if (!HasArena()) return 0;
        uint32_t prev[kMaxHeight];
        uint32_t x_offset = FindGreaterOrEqual(key, nullptr);
        if (x_offset != 0) return x_offset;
        return 0;
    }

    KeyMapHandle Keymap::SearchGreater(const KeySlice& key) {
        op_count_++;
        if (!HasArena()) return 0;
        uint32_t prev[kMaxHeight];
        uint32_t x_offset = FindGreaterOrEqual(key, nullptr);
        KeyNode* x = (KeyNode*)GetAddr(x_offset); 
        if (x != nullptr && Equal(x->GetNodeKey(UseCompact()), key))  {
            KeyNode* next = x->Next(0, GetBaseAddr(), &x_offset);
            return x_offset;
        }
        if (x_offset != 0) return x_offset;
        return 0;
    }


    KeyMapHandle Keymap::Next(KeyMapHandle handle) {
        if (!HasArena()) return 0;
        assert(handle);
        uint32_t next_offset = ((KeyNode*)GetAddr(handle))->NextOffset(0);
        if (!next_offset) return 0;
        return next_offset;
    }

    KeyMapHandle Keymap::Prev(KeyMapHandle handle) {
        op_count_++;
        if (!HasArena()) return 0;
        assert(handle);
        std::string key_str;
        GetKeyString(handle, key_str);
        Slice cur_key(key_str.data(), key_str.size());
        uint32_t x_offset = FindLessThan(cur_key);
        if (x_offset == GetHeadNodeOffset()) return 0;
        return x_offset;
    }

    KeyMapHandle Keymap::First() {
        if (!HasArena()) return 0;
        KeyNode* head = GetHeadNode();
        uint32_t x_offset = head->NextOffset(0);
        return x_offset;
    }

    KeyMapHandle Keymap::Last() {
        op_count_++;
        if (!HasArena()) return 0;
        uint32_t x_offset = FindLast();
        if (!x_offset || x_offset == GetHeadNodeOffset()) {
            return 0;
        }
        return x_offset;
    }

    bool Keymap::Contains(const Slice& key) {
        op_count_++;
        if (!HasArena()) return false;
        uint32_t x_offset = FindGreaterOrEqual(key, nullptr);
        KeyNode* x = (KeyNode*)GetAddr(x_offset);
        if (x != nullptr && Equal(x->GetNodeKey(UseCompact()), key)) {
            return true;
        } else {
            return false;
        }
    }


    void Keymap::GetiKeySeqNum(KeyMapHandle handle, std::set<uint64_t> &KeysList) {
        if (!HasArena()) return;
        KeyNode *x = (KeyNode*)GetAddr(handle);
        assert(x);
        KeyMeta *meta = x->GetKeyMeta(GetBaseAddr(), UseCompact()); /* get latest KeyMeta */
        assert(meta);
        meta->GetiKeySeqNum(KeysList);
        return; 
    } 

    KeyMeta* Keymap::GetKeyMeta(KeyMapHandle handle) {
        if (!HasArena()) return nullptr;
        KeyNode *x = (KeyNode*)GetAddr(handle);
        assert(x);
        KeyMeta *meta = x->GetKeyMeta(GetBaseAddr(), UseCompact()); /* get latest KeyMeta */
        assert(meta);
        return meta; 
    } 



    Slice Keymap::GetKeyInfoLatest(KeyMapHandle handle) {
        if (!HasArena()) return Slice(0);
        uint32_t new_meta_offset = 0;
        uint32_t new_meta_size = 0;
        KeyNode *x = (KeyNode*)GetAddr(handle);
        assert(x);
        KeyMeta *meta = x->GetKeyMeta(GetBaseAddr(), UseCompact(), &new_meta_offset, &new_meta_size);
        assert(meta);
        uint32_t alloc_size = 0;
        char* addr = GetAddr(new_meta_offset);
        char* addr2 = const_cast<char*>(GetVarint32Ptr(addr, addr + 4, &alloc_size));
        assert(addr2);
        return Slice(addr2, alloc_size);
    }
    Slice Keymap::GetKeyInfo(KeyMapHandle handle) {
        if (!HasArena()) return Slice(0);
        uint32_t new_meta_offset = 0;
        uint32_t new_meta_size = 0;
        KeyNode *x = (KeyNode*)GetAddr(handle);
        assert(x);
        return x->GetKeyInfo(UseCompact());
    }
    /*
     * Fill column info with Key.
     * - key is malloced and need to be freed by caller.
     */
    bool Keymap::GetKeyColumnData(KeyMapHandle handle, uint8_t col_id, ColumnData &col_data) {
        if (!HasArena()) return false;
        col_data.init();
        KeyNode *x = (KeyNode*)GetAddr(handle);
        assert(x);
        Slice node_key = x->GetNodeKey(UseCompact());
        if (UseCompact()) {
            Slice begin_key = GetBeginKey(); 
            uint32_t shared = 0, non_shared=0; 
            Slice parse(node_key.data(), node_key.size());
#ifndef NDEBUG
            if (!GetVarint32(&parse, &shared)) abort();
            if (!GetVarint32(&parse, &non_shared)) abort();
#else
            GetVarint32(&parse, &shared);
            GetVarint32(&parse, &non_shared);
#endif
            uint32_t key_size = shared + non_shared;
            if (key_size) {
                /* caller must free key slice's buffer */
                char* key_str = (char*)malloc(key_size);
                assert(key_str);
                memcpy(key_str, begin_key.data(), shared);
                memcpy(key_str + shared, parse.data(), non_shared);
                col_data.key = Slice(key_str, key_size);
            }
        } else {
            if (node_key.size()) {
                /* caller must free key slice's buffer */
                char* key_str = (char*)malloc(node_key.size());
                memcpy(key_str, node_key.data(), node_key.size());
                col_data.key = Slice(key_str, node_key.size());
            }
        }
        KeyMeta *meta = x->GetKeyMeta(GetBaseAddr(), UseCompact()); /* get latest KeyMeta */
        assert(meta);
        col_data.has_userkey = meta->HasUserKey();
        if (col_data.has_userkey) { 
            col_data.userkey = meta->UserKeyAddr();
            return true;
        }
        ColMeta* col = meta->GetColumn(col_id); /* get latest ColMeta */
        if (!col) return false;
        col->ColInfo(col_data);
        return true;
    }

    void Keymap::SetKeyReference(KeyMapHandle handle, uint8_t col_id) {
        if (!HasArena()) return;
        KeyNode *x = (KeyNode*)GetAddr(handle);
        assert(x);
        KeyMeta *meta = x->GetKeyMeta(GetBaseAddr(), UseCompact()); /* get latest KeyMeta */
        assert(meta);
        if (meta->HasUserKey()) return;
        ColMeta* col = meta->GetColumn(col_id); /* get latest ColMeta */
        if (col) col->SetReference();
        return;
    }


    /*
     * Fill column info.
     */
    Slice Keymap::GetColumnData(KeyMapHandle handle, uint8_t col_id, void* &uk) {
        if (!HasArena()) return Slice(0);
        uk = nullptr;
        KeyNode *x = (KeyNode*)GetAddr(handle);
        assert(x);
        KeyMeta *meta = x->GetKeyMeta(GetBaseAddr(), UseCompact()); /* get latest KeyMeta */
        assert(meta);
        if (meta->HasUserKey()) {
            uk = (void*)meta->UserKeyAddr();
            return Slice(0);
        }
        ColMeta* col = meta->GetColumn(col_id); /* get latest ColMeta */
        if (!col) return Slice(0);
        return Slice((char*)col, col->GetColSize());
    }
    Slice Keymap::GetBeginKey() {
        /*
         * begin key
         * [key:key size]
         */ 
        if (begin_key_offset_) {
            return Slice(GetBaseAddr() + begin_key_offset_, begin_key_size_);
        }
        uint32_t size = 0, key_size = 0, offset = 0, alloc_size = 0;
        char* buf = nullptr;
        char* addr = GetBaseAddr() + arena_->GetBeginKeyOffset();
        buf = const_cast<char*>(GetVarint32Ptr(addr, addr + 4, &alloc_size));
        assert(buf); 
        size = alloc_size;
        size -= (buf - addr);
        offset += (buf - addr);
        assert(size >=  0); 
        addr = const_cast<char*>(GetVarint32Ptr(buf, buf + size, &key_size));
        size -= (addr - buf);
        offset += (addr - buf);
        assert(size >=  key_size);
        begin_key_offset_ = arena_->GetBeginKeyOffset() + offset;
        begin_key_size_ = key_size;
        return Slice(addr, key_size);
    }

    void Keymap::GetKeyString(uint32_t handle, std::string &key_str) {
        assert(HasArena());
        assert(handle);
        Slice node_key = ((KeyNode*)GetAddr(handle))->GetNodeKey(UseCompact());
        if (UseCompact()) {
            Slice begin_key = GetBeginKey(); 
            uint32_t shared = 0, non_shared=0; 
            Slice parse(node_key.data(), node_key.size());
#ifndef NDEBUG
            if (!GetVarint32(&parse, &shared)) abort();
            if (!GetVarint32(&parse, &non_shared)) abort();
#else
            GetVarint32(&parse, &shared);
            GetVarint32(&parse, &non_shared);
#endif
            if (shared) key_str.append(begin_key.data(), shared);
            if (non_shared) key_str.append(parse.data(), non_shared);
        } else {
            key_str.append(node_key.data(), node_key.size());
        }
        return;
    }

    Slice Keymap::GetKeySlice(uint32_t handle) {
        assert(HasArena());
        assert(handle);
        Slice node_key = ((KeyNode*)GetAddr(handle))->GetNodeKey(UseCompact());
        if (UseCompact()) {
            Slice begin_key = GetBeginKey(); 
            uint32_t shared = 0, non_shared=0; 
            Slice parse(node_key.data(), node_key.size());
#ifndef NDEBUG
            if (!GetVarint32(&parse, &shared)) abort();
            if (!GetVarint32(&parse, &non_shared)) abort();
#else
            GetVarint32(&parse, &shared);
            GetVarint32(&parse, &non_shared);
#endif
            uint32_t key_size = shared + non_shared;
            if (key_size) {
                /* caller must free key slice's buffer */
                char* key_str = (char*)malloc(key_size);
                assert(key_str);
                memcpy(key_str, begin_key.data(), shared);
                memcpy(key_str + shared, parse.data(), non_shared);
                return Slice(key_str, key_size);
            }
        }
        if (node_key.size()) {
            /* caller must free key slice's buffer */
            char* key_str = (char*)malloc(node_key.size());
            memcpy(key_str, node_key.data(), node_key.size());
            return Slice(key_str, node_key.size());
        }
        return Slice(0);
    }

    Arena* Keymap::MemoryCleanup(Manifest* mf, KeyMapHashMap* hashmap) {
        assert(HasArena());
        assert(hashmap);
        std::vector<SnapInfo> snapinfo_list;
        if (mf) mf->GetSnapshotInfo(&snapinfo_list);
        /* step 1. create new arean */
        Arena * new_arena = new Arena(GetBufferSize());
        memcpy(new_arena->GetBaseAddr(), GetBaseAddr(), sizeof(KEYMAP_HDR));
        /* step 2. copy begin key */
        uint32_t alloc_size = 0;
        uint32_t orig_offset = GetBeginKeyOffset(); /* begin key offset */
        assert(orig_offset);
        char* addr = GetBaseAddr() + orig_offset; /* get beginkey addr */
        char* addr2 = const_cast<char*>(GetVarint32Ptr(addr, addr + 4, &alloc_size));
        assert(addr2);
        uint32_t new_offset = new_arena->Allocate(alloc_size); /* alloc begin key */
        assert(new_offset);
        memcpy(new_arena->GetBaseAddr() + new_offset, addr, alloc_size); /* copy begin key */
        new_arena->SetBeginKeyOffset(new_offset); /* set new begin key offset */
        /* step 2. copy header node */
        orig_offset = GetHeadNodeOffset(); /* header offset */
        assert(orig_offset);
        addr = GetBaseAddr() + orig_offset; /* get header addr */
        addr2 = const_cast<char*>(GetVarint32Ptr(addr, addr + 4, &alloc_size));
        assert(addr2);
        new_offset = new_arena->Allocate(alloc_size); /* alloc header */
        assert(new_offset);
        memcpy(new_arena->GetBaseAddr() + new_offset, addr, alloc_size); /* copy header */
        KeyNode* head = (KeyNode*)(new_arena->GetBaseAddr() + new_offset);
        uint32_t* next_addr = head->NextAddrInfo();
        for (int i = 0; i < kMaxHeight; i++) next_addr[i] = 0; /* init new header next as 0 */

        new_arena->SetHeadNodeOffset(new_offset); /* set Header offset */
        uint32_t head_offset = new_offset;
        new_arena->SetMaxHeight(GetMaxHeight()); /* set max height */
        //new_arena->SetNodeCount(GetCount()); /* set node count */
        /* step 3, copy nodes */
        uint32_t prev[kMaxHeight];
        for (int i = 0; i < kMaxHeight; i++) prev[i] = head_offset; /* initialize new header offset */
        uint64_t copy_count = 0;
        uint32_t x_offset = GetHeadNodeOffset();
        KeyNode* x = (KeyNode*)GetAddr(x_offset);
        x_offset = x->NextOffset(0);
        Slice begin_key = GetBeginKey();
        while (x_offset) {
            uint32_t old_node_size = 0;
            char *src_addr = GetAddr(x_offset); /* get Node start address */
            char *src_data_start = const_cast<char*>(GetVarint32Ptr(src_addr, src_addr + 4, &old_node_size));
            assert(src_data_start);
            x = (KeyNode*)src_addr;
            /* 
             * get orginal key for update hashmap.
             * key format: node key : [shared:vint32|non_shared:vint32|key:non_shared]
             */
            Slice node_key = x->GetNodeKey(UseCompact());
            std::string key_str;
            KeySlice key(0);            
            if (UseCompact()) {
                uint32_t shared = 0, non_shared = 0;
                Slice parse(node_key.data(), node_key.size());
#ifndef NDEBUG
                if (!GetVarint32(&parse, &shared)) abort();
                if (!GetVarint32(&parse, &non_shared)) abort();
#else
                GetVarint32(&parse, &shared);
                GetVarint32(&parse, &non_shared);
#endif
                if (shared) key_str.append(begin_key.data(), shared);
                if (non_shared) key_str.append(parse.data(), non_shared);
                key = KeySlice(key_str.data(), key_str.size());
            } else {
                key = KeySlice(node_key.data(), node_key.size());
            }

           /*
             * get original keyifo size
             */
            Slice keyinfo = x->GetKeyInfoWithSize(UseCompact());
            uint32_t est_copy_size = keyinfo.data() - src_data_start; /* get est copy size, which inclue height, next array and key */

            addr = const_cast<char*>(GetVarint32Ptr(keyinfo.data(), keyinfo.data() + 4, &alloc_size)); /* old keyinfo size */
            assert(addr);
            KeyMeta* key_meta = (KeyMeta*)addr;
            uint32_t new_key_info_size = key_meta->CurrentMetaSize(GetBaseAddr(), snapinfo_list); /* keyinfo size */
            if (!new_key_info_size) { /* skip this node */
                x_offset = x->NextOffset(0);
                continue;
            }
            /*
             * get new node size
             */
            uint32_t est_keyinfo_size = (VarintLength(new_key_info_size) + new_key_info_size); /* get new keyinfo size */
            uint32_t init_est_size = est_copy_size + est_keyinfo_size;
            uint32_t est_size = init_est_size + VarintLength(init_est_size); /* node size */
            if ((init_est_size / 128) != (est_size / 128)) est_size++; /* total size overflow .. need to increase*/
            /*
             * allocate new node
             */
            uint32_t new_offset = new_arena->Allocate(est_size);
            assert(new_offset);
            uint32_t new_node_size = 0;

            /*
             * copy height, nest array and key.
             */
            char *dest_addr = new_arena->GetBaseAddr() + new_offset;
            char *dest_data_start = const_cast<char*>(GetVarint32Ptr(dest_addr, dest_addr + 4, &new_node_size));
            assert(dest_data_start);
            assert(new_node_size == est_size);
            memcpy(dest_data_start, src_data_start, est_copy_size);

            /*
             * copy key info
             */
            addr2 = dest_data_start + est_copy_size;
            addr = EncodeVarint32(addr2, new_key_info_size); /* set new key info size */
            assert(addr);
            /* fill key info */
            key_meta->CopyCurrentMeta(addr, new_key_info_size, GetBaseAddr(), snapinfo_list); /* fill new keyinfo */
            /* update prev info */
            KeyNode* node = (KeyNode*)dest_addr;
            int height = 0;
            uint32_t* next_addr = node->NextAddrInfo(&height);
            for (int i = 0; i < height; i++) {
                next_addr[i] = 0; /* init new node next as 0 */
                ((KeyNode*)(new_arena->GetBaseAddr() + prev[i]))->SetNextOffset(i, new_offset);
                prev[i] = new_offset;
            }
            new_arena->IncNodeCount();
#ifdef USE_HASHMAP
           /* update hashmap */
            auto it = hashmap->find(key.GetHashValue());
            if (it != hashmap->end()) {
                std::string *offset_str = it->second;
                assert(offset_str);
                offset_str->append((char*)&new_offset, 4);
                uint32_t *pcount = (uint32_t*)(const_cast<char*>(offset_str->data()));
                *pcount = *pcount + 1; 
            } else {
                std::string *offset_str = new std::string();
                uint32_t nr_count = 1;
                offset_str->append((char*)&nr_count, 4);
                offset_str->append((char*)&new_offset, 4);
                auto ret = hashmap->insert(key.GetHashValue(), offset_str);
            }
#endif
           copy_count++;
            x_offset = x->NextOffset(0);
        } 
        /* need to confirm with ilgu */
        //if (copy_count != GetCount()) abort();
        return new_arena;;
    }
    
    bool Keymap::MemoryMigration(Manifest* mf, Arena *new_arena, struct KeyMigrationCtx& ctx) {
        assert(HasArena());
        std::vector<SnapInfo> snapinfo_list;
        if (mf) mf->GetSnapshotInfo(&snapinfo_list);
        uint32_t alloc_size = 0;
        uint32_t orig_offset = 0, new_offset = 0, x_offset = 0;
        char *addr = nullptr, *addr2 = nullptr;
        KeyNode* x = nullptr;
        int max_height = 1;
        if (ctx.first) {
            /* create begin_key*/
            alloc_size = ctx.begin_key.size() + VarintLength(ctx.begin_key.size()) + sizeof(uint32_t);
            new_offset = new_arena->Allocate(alloc_size); /* begin key offset */
            addr = new_arena->GetBaseAddr() + new_offset; /* get header addr */
            addr2 = const_cast<char*>(GetVarint32Ptr(addr, addr + 4, &alloc_size));
            assert(addr2);
            alloc_size -= (addr2 - addr);
            assert(alloc_size >= 0);
            addr = EncodeVarint32(addr2, ctx.begin_key.size()); /* begin_key size */
            assert(addr);
            alloc_size -= (addr - addr2);
            assert(alloc_size >= ctx.begin_key.size());
            memcpy(addr, ctx.begin_key.data(), ctx.begin_key.size()); /* copy key string */
            new_arena->SetBeginKeyOffset(new_offset); /* set new begin key offset */

            /* create header */
            orig_offset = GetHeadNodeOffset(); /* header offset */
            assert(orig_offset);
            addr = GetBaseAddr() + orig_offset; /* get header addr */
            addr2 = const_cast<char*>(GetVarint32Ptr(addr, addr + 4, &alloc_size));
            assert(addr2);
            new_offset = new_arena->Allocate(alloc_size); /* alloc header */
            assert(new_offset);
            memcpy(new_arena->GetBaseAddr() + new_offset, addr, alloc_size); /* copy header */
            KeyNode* head = (KeyNode*)(new_arena->GetBaseAddr() + new_offset);
            uint32_t* next_addr = head->NextAddrInfo();
            for (int i = 0; i < kMaxHeight; i++) next_addr[i] = 0; /* init new header next as 0 */
            new_arena->SetHeadNodeOffset(new_offset); /* set Header offset */
            for (int i = 0; i < kMaxHeight; i++) ctx.prev[i] = new_offset; /* initialize new header offset */
            ctx.first = false;
            new_arena->SetMaxHeight(max_height);
            /* get first keynode offset */
            if (ctx.next_start_offset) x_offset = ctx.next_start_offset;
            else x_offset = GetHeadNode()->NextOffset(0);
        } else {
            if (ctx.next_start_offset) x_offset = ctx.next_start_offset;
            else x_offset = GetHeadNode()->NextOffset(0);
            max_height = new_arena->GetMaxHeight();
        }

        if (!x_offset) return true; /* nothing to do */
        Slice begin_key = GetBeginKey();
        while (x_offset) {
            x = (KeyNode*)GetAddr(x_offset); /* get Node addr */
            int height = x->GetHeight();
            assert(height <= kMaxHeight);
            if (height > max_height) max_height = height;
            Slice node_key = x->GetNodeKey(UseCompact());
            uint32_t new_node_size = 0;
            uint32_t shared = 0, non_shared = 0;
            std::string key_str;
            KeySlice key(0);            
            if (UseCompact()) { 
                /* 
                 * get orginal key
                 * key format: node key : [shared:vint32|non_shared:vint32|key:non_shared]
                 */
                Slice parse(node_key.data(), node_key.size());
#ifndef NDEBUG
                if (!GetVarint32(&parse, &shared)) abort();
                if (!GetVarint32(&parse, &non_shared)) abort();
#else
                GetVarint32(&parse, &shared);
                GetVarint32(&parse, &non_shared);
#endif
                if (shared) key_str.append(begin_key.data(), shared);
                if (non_shared) key_str.append(parse.data(), non_shared);
                key = Slice(key_str.data(), key_str.size());         
                /*
                 * Calc new share and non shared key size
                 */
                shared = non_shared= 0;
                const size_t min_length = std::min(ctx.begin_key.size(), key_str.size());
                while ((shared < min_length) && ctx.begin_key[shared] == key[shared]) shared++;
                non_shared = key_str.size() - shared;
                new_node_size = sizeof(uint8_t)/*height*/ + sizeof(uint32_t)*height/*nexts */
                    + VarintLength(shared)/*shared key size*/ + VarintLength(non_shared) /*non shared key size*/ + non_shared/*encode key*/;
            } else {
                new_node_size = sizeof(uint8_t)/*height*/ + sizeof(uint32_t)*height/*nexts */
                    + VarintLength(node_key.size())/*key size*/ + node_key.size()/*encode key*/;
                key = Slice(node_key.data(), node_key.size());        
            }
                /* get key info */
            Slice keyinfo = x->GetKeyInfoWithSize(UseCompact()); 
            addr = const_cast<char*>(GetVarint32Ptr(keyinfo.data(), keyinfo.data() + 4, &alloc_size)); /* old keyinfo size */
            assert(addr);
            KeyMeta* key_meta = (KeyMeta*)addr;
            uint32_t new_key_info_size = key_meta->CurrentMetaSize(GetBaseAddr(), snapinfo_list); /* keyinfo size */
            if (!new_key_info_size) { /* skip this node */
                x_offset = x->NextOffset(0);
                continue;
            }
            uint32_t est_keyinfo_size = (VarintLength(new_key_info_size) + new_key_info_size); /* get new keyinfo size */
            uint32_t init_est_size = new_node_size /* height + next[] + key */ + est_keyinfo_size;
            uint32_t est_size =  init_est_size + VarintLength(init_est_size); /* node size */
            if ((init_est_size / 128) != (est_size / 128)) est_size++; /* total size overflow .. need to increase*/

            if (ctx.max_arena_size) {
                if (new_arena->AllocatedMemorySize() + est_size > ctx.max_arena_size) {
                    new_arena->SetMaxHeight(max_height);
                    ctx.cur_arena_size = new_arena->AllocatedMemorySize();
                    ctx.next_start_offset = x_offset; 
                    return true;
                }
            }

            /* allocate new node */
            new_offset = new_arena->Allocate(est_size);
            assert(new_offset);
            KeyNode* node = (KeyNode*)(new_arena->GetBaseAddr() + new_offset);
            Slice new_keyinfo(0);
            if (UseCompact()) {
                new_keyinfo = node->Init(height, key, shared, non_shared, new_key_info_size, est_size);
            } else {
                new_keyinfo = node->InitNoCompact(height, key, new_key_info_size, est_size);
            }
            assert(new_keyinfo.size() >=new_key_info_size);
            addr = const_cast<char*>(new_keyinfo.data());
            /* copy key info */
            key_meta->CopyCurrentMeta(addr, new_key_info_size, GetBaseAddr(), snapinfo_list); /* fill new keyinfo */
            /* update ctx.prev info */
            uint32_t* next_addr = node->NextAddrInfo();
            for (int i = 0; i < height; i++) {
                next_addr[i] = 0; /* init new node next as 0 */
                ((KeyNode*)(new_arena->GetBaseAddr() + ctx.prev[i]))->SetNextOffset(i, new_offset);
                ctx.prev[i] = new_offset;
            }
            new_arena->IncNodeCount();
#ifdef USE_HASHMAP
            /* update hashmap */
            auto it = ctx.hashmap->find(key.GetHashValue());
            if (it != ctx.hashmap->end()) {
                std::string *offset_str = it->second;
                assert(offset_str);
                offset_str->append((char*)&new_offset, 4);
                uint32_t *pcount = (uint32_t*)(const_cast<char*>(offset_str->data()));
                *pcount = *pcount + 1; 
            } else {
                std::string *offset_str = new std::string();
                uint32_t nr_count = 1;
                offset_str->append((char*)&nr_count, 4);
                offset_str->append((char*)&new_offset, 4);
                auto ret = ctx.hashmap->insert(key.GetHashValue(), offset_str);
            }
#endif
            x_offset = x->NextOffset(0); 
        }
        new_arena->SetMaxHeight(max_height);
        ctx.next_start_offset = 0;
        ctx.cur_arena_size = new_arena->AllocatedMemorySize();
        return true;
    }
    /*
     * Existing keymap will not impact during split.
     * - new keymaps are inserted into keysmpas.
     */
    bool Keymap::SplitKeymap(Manifest* mf, std::vector<Keymap*>& keymaps, uint32_t check_size) {
        assert(HasArena());
        /*
         * Split Keymap, based on kMaxSKTableSize if not given chunk_size
         */
        /* estimate split size and count */
        uint32_t memory_size = 0;
        uint32_t cur_memory_size = 0;
        if (check_size) memory_size = check_size;
        else memory_size = kMaxSKTableSize;
        if (memory_size < kMaxSKTableSize/2) memory_size = kMaxSKTableSize/2;
        assert(GetAllocatedSize() >= GetFreedMemorySize()); 
        cur_memory_size = GetAllocatedSize() - GetFreedMemorySize();
        if (cur_memory_size < memory_size) {
            KeyMapHashMap* new_hashmap = new KeyMapHashMap(1024*32);
            Arena* new_arena = MemoryCleanup(mf, new_hashmap); /* cleanup existing arena */
            Keymap *new_keymap = new Keymap(new_arena, compare_.keymap_comparator, new_hashmap);
            assert(new_keymap);
            keymaps.push_back(new_keymap);
            return true;
        }
        uint32_t num_keymaps = (cur_memory_size + memory_size -1) / memory_size;
        uint32_t cum_data_size = 0;
        assert(num_keymaps);
        /* build first arena for itself */
        KeyMigrationCtx ctx;
        memset(&ctx, 0, sizeof(ctx));
        Arena* first_arena = new Arena();
        memcpy(first_arena->GetBaseAddr(), GetBaseAddr(), sizeof(KEYMAP_HDR));
        ctx.first = true;
        ctx.begin_key = GetBeginKey();
        ctx.next_start_offset = 0;
        if (num_keymaps < 3) {
            ctx.max_arena_size = 0; /* don't want to split until 2*times bigger than memory size */
        } else {
            ctx.max_arena_size = memory_size;
        }
        ctx.hashmap = new KeyMapHashMap(1024*32);
#ifndef NDEBUG
        if (!MemoryMigration(mf, first_arena, ctx)) abort();
#else
        MemoryMigration(mf, first_arena, ctx);
#endif
        Keymap *new_keymap = new Keymap(first_arena, compare_.keymap_comparator, ctx.hashmap);
        assert(new_keymap);
        keymaps.push_back(new_keymap); 
        num_keymaps--;
        cum_data_size = ctx.cur_arena_size;
        std::string node_key_str;
        /* build sub keymap */
        while(ctx.next_start_offset) {
            /* create new arena with splited data */
            node_key_str.resize(0);
            GetKeyString(ctx.next_start_offset, node_key_str);
            ctx.first = true;
            ctx.begin_key = Slice(node_key_str.data(), node_key_str.size());
            ctx.hashmap = new KeyMapHashMap(1024*32);
            if (cur_memory_size <= (cum_data_size + memory_size*2) || num_keymaps == 1) ctx.max_arena_size = 0;
            Arena* new_arena = new Arena();
            KEYMAP_HDR * hdr = (KEYMAP_HDR*)new_arena->GetBaseAddr();
            hdr->use_compact_key = (UseCompact()) ? 1 : 0; 
#ifndef NDEBUG
            if (!MemoryMigration(mf, new_arena, ctx)) abort();
#else
            MemoryMigration(mf, new_arena, ctx);
#endif
            cum_data_size +=  (ctx.cur_arena_size - 128); /* estimated header size */;
            /* create new key map */
            Keymap *new_keymap = new Keymap(new_arena, compare_.keymap_comparator, ctx.hashmap);
            assert(new_keymap);
            keymaps.push_back(new_keymap);
            num_keymaps--;
        }
        /* replace existing arena with new one */
        return true;
    }

    Keymap* Keymap::MergeKeymap(Manifest* mf, std::vector<Keymap*>& keymaps) {
        HasArena();
        /*
         * Assume sub keymap does not have overwapped key and is sorted.
         */
        KeyMigrationCtx ctx;
        Arena* new_arena = new Arena();
        KEYMAP_HDR * hdr = (KEYMAP_HDR*)new_arena->GetBaseAddr();
        memcpy(new_arena->GetBaseAddr(), GetBaseAddr(), sizeof(KEYMAP_HDR));
        ctx.first = true;
        ctx.begin_key = GetBeginKey();
        ctx.max_arena_size = 0;
        ctx.next_start_offset = 0;
        ctx.hashmap = new KeyMapHashMap(1024*32);
#ifndef NDEBUG
        if (!MemoryMigration(mf, new_arena, ctx)) abort();
#else
        MemoryMigration(mf, new_arena, ctx);
#endif
        for (auto i = 0; i < keymaps.size(); i++) {
            Keymap* sub = keymaps[i];
            ctx.max_arena_size = 0;
            ctx.next_start_offset = 0;
#ifndef NDEBUG
            if (!sub->MemoryMigration(mf, new_arena, ctx)) abort();
#else
            sub->MemoryMigration(mf, new_arena, ctx);
#endif
        }

        Keymap *new_keymap = new Keymap(new_arena, compare_.keymap_comparator, ctx.hashmap);
        assert(new_keymap);
        return new_keymap;
    }
} //namespace insdb
