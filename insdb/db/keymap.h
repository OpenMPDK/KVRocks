// Copyright (c) 2011 The insdb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_INSDB_DB_KEYMAP_H_
#define STORAGE_INSDB_DB_KEYMAP_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the Keymap will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the Keymap is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a KeyNode* except for the next/prev pointers are
// immutable after the KeyNode* has been linked into the Keymap.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <set>
#include "insdb/comparator.h"
#include "util/random.h"
#include "util/arena.h"
#include "util/coding.h"
#include <folly/concurrency/ConcurrentHashMap.h>

//#define USE_HASHMAP

namespace insdb {
    typedef uint32_t KeyMapHandle;

    class Manifest;
    struct SnapInfo;

    struct KeymapHash {
        inline uint32_t operator() (const uint32_t key_hash) noexcept {
            return key_hash;
        }
    };
    typedef folly::ConcurrentHashMap<uint32_t, std::string *, KeymapHash> KeyMapHashMap;

    struct KEYMAP_HDR {
        uint32_t crc;
        uint32_t keymap_size; /*compress start pointer */
        uint32_t skt_id;
        uint32_t prealloc_skt_id;
        uint32_t next_skt_id;
        uint32_t start_colinfo;
        uint32_t last_colinfo;
        uint32_t hashmap_data_size;
        uint32_t use_compact_key;
    } __attribute__((packed));
#define KEYMAP_COMPRESS         0x80000000
#define KEYMAP_SIZE_MASK        0x7FFFFFFF

    typedef struct {
        void init() {
            col_size = 0;
            referenced = false;
            is_deleted = false;
            has_ttl = false;
            has_userkey = false;
            column_id = 0;
            kb_offset = 0;
            userkey = nullptr;
            iter_sequence = 0;
            ttl = 0;
            ikey_sequence = 0;
            ikey_size = 0;
            key = Slice(0);
        }
        uint8_t col_size;
        bool referenced;
        bool is_deleted;
        bool has_ttl;
        bool has_userkey;
        uint8_t column_id;
        
        uint8_t kb_offset;
        void*   userkey;
        uint64_t iter_sequence;
        uint64_t ttl;
        uint64_t ikey_sequence;
        uint32_t ikey_size;
        Slice key;
    } ColumnData;

    struct ColMeta {
        public:
            const static uint8_t flags_deleted                      = 0x80;
            const static uint8_t first_flags_fisrt_ref_bit          = 0x80; 
            const static uint8_t first_flags_second_ref_bit         = 0x40; 
            const static uint8_t first_flags_mask                   = 0xC0;

            const static uint8_t second_flags_ttl                   = 0x80;

            bool HasReference();
            bool HasSecondChanceReference();
            void SetSecondChanceReference();
            void SetReference();
            void ClearReference();
            bool IsDeleted();
            uint8_t ColumnID();
            bool HasTTL();
            uint8_t KeyBlockOffset();
            uint8_t GetColSize();
            uint64_t GetSequenceNumber();
            void ColInfo(ColumnData &col_data);
        private:
            char rep_[1];
    };

    struct KeyMeta {
        public:
            const static uint8_t flags_all_column_relocated     = 0x80;
            const static uint8_t flags_userkey                  = 0x40;
            const static uint8_t flags_mask                     = 0xC0;

            uint8_t NRColumn(); /* can be recursive function */
            ColMeta* ColMetaArray(); /* can be recursive function */
            bool IsRelocated();
            bool HasUserKey();
            uint32_t RelocatedOffset();
            void* UserKeyAddr();
            ColMeta* GetColumn(uint16_t col_id);
            KeyMeta* GetLatestKeyMeta(char* base_addr, uint32_t* new_meta_offset = nullptr, uint32_t *new_meta_size = nullptr);
            void CleanupKeyMeta(char* base_addr, std::vector<uint32_t> &free_offset);
            uint32_t CurrentMetaSize(char* base_addr, std::vector<SnapInfo> &snapinfo_list);
            void CopyCurrentMeta(char* dest_addr, uint32_t allocated_size, char* base_addr, std::vector<SnapInfo> &snapinfo_list);
            void GetiKeySeqNum(std::set<uint64_t> &KeysList);
        private:
            char rep_[1];
    };

    KeyMapHashMap* BuildKeymapHashmapWithString(std::string &hash_data);

    class Keymap {
        public:
            enum { kMaxHeight = 12 };
            /* internal key node */
            struct KeyNode {
                public:
                    uint32_t GetNodeSize();
                    int GetHeight();
                    uint32_t* NextAddrInfo(int *p_height = 0, uint32_t *p_size = 0);
                    void InitNext();
                    KeyNode* Next(int n, char* base_addr, uint32_t *offset_ptr = nullptr);
                    uint32_t NextOffset(int n);
                    void SetNextOffset(int n, uint32_t next_offset);
                    Slice GetNodeKey(bool use_compact_key);
                    Slice GetKeyInfo(bool use_compact_key);
                    Slice GetKeyInfoWithSize(bool use_compact_key);
                    void Cleanup(char* base_addr, std::vector<uint32_t> &free_offset, bool use_compact_key);
                    Slice Init(int height, Slice key, uint32_t shared, uint32_t non_shared, uint32_t keyinfo_size, uint32_t size);
                    Slice InitNoCompact(int height, Slice key, uint32_t keyinfo_size, uint32_t size);
                    KeyMeta* GetKeyMeta(char* base_addr,  bool use_compact_key, uint32_t* new_meta_offset = nullptr, uint32_t* new_meat_size = nullptr);

                private:
                    char rep_[1];
            };
            struct KeyMigrationCtx {
                bool first; /* set only first migration with begin_key slice */
                Slice begin_key; /* only valid when first is set */
                uint32_t next_start_offset; /* keep next start offset */
                uint32_t prev[kMaxHeight]; /* keep prev offset */
                uint32_t max_arena_size; /* allowed max arean size. 0 = no limit */
                uint32_t cur_arena_size; /* set when finish migration */
                std::string cur_begin_key; /* keep begin key */
                KeyMapHashMap* hashmap;
            };
            /* comparator */
            struct KeymapComparator {
                const Comparator *keymap_comparator;
                explicit KeymapComparator(const Comparator *c) : keymap_comparator(c) {}
                int operator()(Slice begin_key/*begin_key*/, Slice node_key/*node key*/, Slice user_key/* key*/) const ;
            };
            /* comparator */
            struct KeymapNoCompactComparator {
                const Comparator *keymap_comparator;
                explicit KeymapNoCompactComparator(const Comparator *c) : keymap_comparator(c) {}
                int operator()(Slice node_key/*node key*/, Slice user_key/* key*/) const ;
            };

            // Create a new Keymap object that will use "cmp" for comparing keys
            explicit Keymap(Arena *arena, const Comparator *cmp);
            explicit Keymap(Arena *arena, const Comparator *cmp, KeyMapHashMap* hashmap);
            explicit Keymap(KeySlice beginkey, const Comparator *cmp);
            void InsertArena(Arena *arena, KeyMapHashMap* hashmap = nullptr);
            void InitArena(Arena *arena, bool use_compact_key = false);
            bool HasArena() {
                if (arena_){
                    if (__sync_add_and_fetch((uint32_t*)arena_,0)) 
                        return true;
                }
                return false;
            }

            Keymap(const Keymap&) = delete;
            Keymap& operator=(const Keymap&) = delete;
            ~Keymap();
            // Insert key into the list.
            // REQUIRES: nothing that compares equal to key is currently in the list.
            Slice Insert(const KeySlice& key, uint32_t keyinfo_size, KeyMapHandle &handle);

            Slice ReplaceOrInsert(const KeySlice& key, uint32_t keyinfo_size, KeyMapHandle &handle);

            bool Remove(const KeySlice& key);

            KeyMapHandle Search(const KeySlice& key);

            KeyMapHandle SearchLessThan(const KeySlice& key);

            KeyMapHandle SearchLessThanEqual(const KeySlice& key);

            KeyMapHandle SearchGreaterOrEqual(const KeySlice& key);
            
            KeyMapHandle SearchGreater(const KeySlice& key);

            KeyMapHandle Next(KeyMapHandle handle);

            KeyMapHandle Prev(KeyMapHandle handle);

            KeyMapHandle First();

            KeyMapHandle Last();
           
            bool BuildHashMapData(std::string &hash_data);

            void GetiKeySeqNum(KeyMapHandle handle, std::set<uint64_t> &KeysList); 
            KeyMeta* GetKeyMeta(KeyMapHandle handle);
            Slice GetKeyInfoLatest(KeyMapHandle handle);
            Slice GetKeyInfo(KeyMapHandle handle);
            bool GetKeyColumnData(KeyMapHandle handle, uint8_t col_id, ColumnData &col_data);
            void SetKeyReference(KeyMapHandle handle, uint8_t col_id);
            Slice GetColumnData(KeyMapHandle handle, uint8_t col_id, void*& uk);
            void GetKeyString(uint32_t handle, std::string &key_str);
            Slice GetKeySlice(uint32_t handle);
            KeySlice GetKeymapBeginKey() {
                return KeySlice(key_str_.data(), key_str_.size(), key_hash_);
            }
            void SetSKTableID(uint32_t skt_id);
            void SetPreAllocSKTableID(uint32_t skt_id);
            void SetNextSKTableID(uint32_t skt_id);
            uint32_t IncCurColInfoID();
            void SetPrevColInfoID();
            uint32_t GetSKTableID();
            uint32_t GetPreAllocSKTableID();
            uint32_t GetNextSKTableID();
            uint32_t GetPrevColInfoID();
            uint32_t GetCurColInfoID();
            size_t GetCount() {
                return arena_->GetNodeCount();
            }
            Slice GetReserved() {
                if (!HasArena()) return Slice(0);
                return arena_->GetReserved();
            }
            void DumpState() { arena_->DumpState(); }
            bool Contains(const Slice& key);
            uint32_t GetBufferSize() { return arena_->GetBufferSize(); }
            uint32_t GetAllocatedSize() { return arena_->AllocatedMemorySize(); }
            uint32_t GetFreedMemorySize() { return arena_->FreedMemorySize(); }

            Arena* MemoryCleanup(Manifest* mf, KeyMapHashMap* hashmap);
            bool MemoryMigration(Manifest* mf, Arena *new_arena, struct KeyMigrationCtx& ctx);
            bool SplitKeymap(Manifest* mf, std::vector<Keymap*>& keymaps, uint32_t chunk_sise = 0);
            Keymap* MergeKeymap(Manifest* mf, std::vector<Keymap*>& keymaps);
            void CopyMemory(char *addr, uint32_t size) { memcpy(addr, GetBaseAddr(), size);}
            void CopyToString(std::string &dest_str, uint32_t size) {dest_str.append(GetBaseAddr(), size);}
            KeyMapHandle GetNextAllocOffset(uint32_t bytes) {
                uint32_t new_bytes = bytes + VarintLength(bytes);
                if ((bytes/128) != (new_bytes/128)) new_bytes++; /* length field overflow */
                return arena_->PreAllocate(new_bytes);
            }
            char* AllocateMemory(uint32_t bytes, KeyMapHandle handle) {
                uint32_t new_bytes = bytes + VarintLength(bytes);
                if ((bytes/128) != (new_bytes/128)) new_bytes++; /* length field overflow */
                KeyMapHandle new_handle =  arena_->Allocate(new_bytes);
                if (handle != new_handle) abort();
                char* addr = arena_->GetBaseAddr() + handle;
                char* addr2 = const_cast<char*>(GetVarint32Ptr(addr, addr + 4, &new_bytes));
                if (!addr2) abort();
                return addr2;
            }
            void FreeAllocatedMemory(KeyMapHandle handle) {
                if (handle > arena_->AllocatedMemorySize()) abort();
                arena_->DeAllocate(handle);
            } 
            Slice GetRelocatedKeyInfo(KeyMapHandle handle) {
                if (handle > arena_->AllocatedMemorySize()) abort();
                uint32_t alloc_size = 0;
                char* addr = arena_->GetBaseAddr() + handle;
                char* addr2 = const_cast<char*>(GetVarint32Ptr(addr, addr + 4, &alloc_size));
                if (!addr2) abort();
                return Slice(addr2, alloc_size);
            }
            uint32_t IncRefCnt() {
                return ref_cnt_.fetch_add(1, std::memory_order_relaxed);
            }
            uint32_t DecRefCnt() {
                return ref_cnt_.fetch_sub(1, std::memory_order_seq_cst);
            }
            bool DecRefCntCheckForDelete() {
                if (1 == ref_cnt_.fetch_sub(1, std::memory_order_seq_cst)) return true;
                return false;
            }
            uint32_t GetRefCnt() {
                return ref_cnt_.load(std::memory_order_seq_cst);
            }
            uint64_t GetOpCnt() { return op_count_; }
            uint64_t GetCmpCnt() { return cmp_count_; }
            Arena* GetArena() { return arena_; }
        private:

            KeyNode* NewNode(const Slice key, uint32_t keyinfo_size, int height, uint32_t &addr_offset, Slice &keyinfo);
            Slice GetBeginKey();
            int RandomHeight();
            bool Equal(const Slice& a, const Slice& b) {
                cmp_count_++;
                if (UseCompact()) return (compare_(GetBeginKey(), a, b) == 0);
                return (compare_nocompact_(a, b) == 0);
            }
            int Compare(const Slice a, Slice b) {
                cmp_count_++;
                if (UseCompact()) return compare_(GetBeginKey(), a, b);
                return compare_nocompact_(a, b);
            }
            bool KeyIsAfterNode(const Slice& key, KeyNode* n);

            uint32_t FindGreaterOrEqual(const Slice& key, uint32_t* prev);

            uint32_t FindLessThan(const Slice& key);

            uint32_t FindLessThanEqual(const Slice& key);

            uint32_t FindLast();

            Slice _Insert(const KeySlice& key, uint32_t keyinfo_size, KeyMapHandle &handle);

            Slice _ReplaceOrInsert(const KeySlice& key, uint32_t keyinfo_size, KeyMapHandle &handle, bool& existing);

            bool _Remove(const KeySlice& key, KeyMapHandle *handle);

            KeyMapHandle _Search(const KeySlice& key);

            void CleanupHashMap();

            int GetMaxHeight() {
                return (int)arena_->GetMaxHeight();
            }
            char* GetBaseAddr() { return arena_->GetBaseAddr(); }
            uint32_t GetOffset(char* addr) { return (addr - GetBaseAddr()); }
            char* GetAddr(uint32_t offset) {
                if (!offset) return nullptr;
                return (GetBaseAddr() + offset);
            }

            void SetMaxHeight(int val) { arena_->SetMaxHeight(val);}
            void IncCount() { arena_->IncNodeCount(); }
            void DecCount() { arena_->DecNodeCount(); }
            void SetHeadNodeOffset(uint32_t offset) { arena_->SetHeadNodeOffset(offset); }
            uint32_t GetHeadNodeOffset() { return arena_->GetHeadNodeOffset(); }
            KeyNode* GetHeadNode() {return (KeyNode*)(GetBaseAddr() + arena_->GetHeadNodeOffset());}
            void SetBeginKeyOffset(uint32_t offset) { arena_->SetBeginKeyOffset(offset); }
            uint32_t GetBeginKeyOffset() { return arena_->GetBeginKeyOffset(); }
            bool UseCompact() { return use_compact_key_; }


            ///////////////// Member variable //////////////////
            KeymapNoCompactComparator const compare_nocompact_;
            KeymapComparator  const compare_;
            Random rnd_;
            KeyMapHashMap* hashmap_;
            bool use_compact_key_;
            /* fast key begin access remove node size prefix */
            uint32_t begin_key_offset_;
            uint32_t begin_key_size_;
            std::string key_str_;
            uint32_t key_hash_;
            std::atomic<uint32_t> ref_cnt_;
            Arena *arena_;
            uint64_t op_count_;
            uint64_t cmp_count_;
    };

    void static inline IncKeymapRefCnt(Keymap *keymap) {
        if (keymap) keymap->IncRefCnt();
    }

    void static inline DecKeymapRefCnt(Keymap* keymap) {
        if (keymap) {
            if (keymap->DecRefCntCheckForDelete()) delete keymap;
        }
    }
    uint16_t static inline GetKeymapRefCnt(Keymap* keymap) {
        if (keymap) return keymap->GetRefCnt();
        else return 0;
    }
}  // namespace insdb

#endif  //STORAGE_INSDB_DB_KEYMAP_H_
