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
#include <set>
#include <deque>
#include "insdb/db.h"
#include "insdb/status.h"
#include "insdb/comparator.h"
#include "insdb/kv_trace.h"
#include "db/insdb_internal.h"
#include "db/skiplist.h"
#include "db/skiplist2.h"
#include "db/keyname.h"
#include "db/dbformat.h"
#include "db/keymap.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/crc32c.h"

namespace insdb {

    /* without Arena */
#ifdef USE_HASHMAP_RANDOMREAD
    SKTableMem::SKTableMem(Manifest* mf, KeySlice& key, uint32_t skt_id, uint32_t keymap_size, uint32_t keymap_hash_size) : 
#else
    SKTableMem::SKTableMem(Manifest* mf, KeySlice& key, uint32_t skt_id, uint32_t keymap_size) : 
#endif
        keyqueue_lock_(),
        active_key_queue_(0),
        /*Two Queue : Active & Inactive key queue*/
        key_queue_(NULL),
        key_queue_smallest_seq_{kMaxSequenceNumber, kMaxSequenceNumber},
        keymap_mu_(),
        keymap_random_read_mu_(),
        random_read_wait_cnt_(0),
        keymap_(NULL),
        sktdf_load_mu_(),
        //sktdf_update_mu_(),
        //build_iter_mu_(),
        //iter_mu_(),
        flush_candidate_key_deque_(),
        internal_node_(0),
        //sktdf_fetch_spinlock_(),
        flags_(0),
        skt_sorted_node_(),
        skt_id_(skt_id),
        ref_cnt_(0)
        {
            keymap_ = new Keymap(key, mf->GetComparator());
            keymap_node_size_ = keymap_size; /* size of keymap on device */
#ifdef USE_HASHMAP_RANDOMREAD
            keymap_hash_size_ = keymap_hash_size; /* size of keymap on device */
#endif
            key_queue_ = new SimpleLinkedList[2];

            mf->InsertSKTableMem(this);

            SKTableMem *prev = mf->Prev(this);
            if(!prev) 
                //mf_->InsertSKTableSortedList(&sorted_list_);
                mf->InsertFrontToSKTableSortedList(&skt_sorted_node_);
            else
            {
                //prev->InsertSKTableSortedList(&sorted_list_);
                mf->InsertNextToSKTableSortedList(&skt_sorted_node_, prev->GetSKTableSortedListNode());
            }
            mf->IncreaseMemoryUsage(sizeof(SKTableMem));
#ifdef MEM_TRACE
            mf->IncSKT_MEM_Usage(sizeof(SKTableMem));
#endif
        }


    /**
     * This constructor is for sub-SKTable creation.
     */
    SKTableMem::SKTableMem(Manifest *mf, Keymap *keymap) : 
        keyqueue_lock_(),
        active_key_queue_(0),
        /*Two Queue : Active & Inactive key queue*/
        key_queue_(NULL),
        key_queue_smallest_seq_{kMaxSequenceNumber,kMaxSequenceNumber},
        keymap_mu_(),
        keymap_random_read_mu_(),
        random_read_wait_cnt_(0),
        keymap_(keymap),
        keymap_node_size_(0),
#ifdef USE_HASHMAP_RANDOMREAD
        keymap_hash_size_(0),
#endif
        sktdf_load_mu_(),
        //sktdf_update_mu_(),
        //build_iter_mu_(),
        //iter_mu_(),
        flush_candidate_key_deque_(),
        internal_node_(0),
        //sktdf_fetch_spinlock_(),
        flags_(flags_in_clean_cache),
        skt_sorted_node_(),
        skt_id_(0),
        ref_cnt_(0)
        {
            key_queue_ = new SimpleLinkedList[2];
            keymap_node_size_ = 0; /* not set */

            mf->IncreaseMemoryUsage(sizeof(SKTableMem));
#ifdef MEM_TRACE
            mf->IncSKT_MEM_Usage(sizeof(SKTableMem));
#endif
        }

    /**
     * Implement one more Constructor for split
     * SKTableMem::SKTableMem()
     * */

    /**
     * Destructor is called if the SKTableMem contains no key!
     */
    SKTableMem::~SKTableMem() {
        // Remove from SKTable cache and check fetched count.
        //check = sorted_list_.Delete();
        delete [] key_queue_;
        if (keymap_->DecRefCntCheckForDelete())
            delete keymap_; // must be cleared before desturtor, see DeleteSKTDeviceFormatCache().
    }

    void SKTableMem::SetSKTableID(uint32_t skt_id) {
        skt_id_ = skt_id;
        if (!keymap_->HasArena()) abort();
        keymap_->SetSKTableID(skt_id);
    } 
    void SKTableMem::SetPreAllocSKTableID(uint32_t skt_id) {
        if (!keymap_->HasArena()) abort();
        keymap_->SetPreAllocSKTableID(skt_id);
    }
    void SKTableMem::SetNextSKTableID(uint32_t skt_id){
        if (!keymap_->HasArena()) abort();
        keymap_->SetNextSKTableID(skt_id);
    }
    uint32_t SKTableMem::IncCurColInfoID(){
        if (!keymap_->HasArena()) abort();
        return keymap_->IncCurColInfoID();
    }
    void SKTableMem::ResetCurColInfoID(uint32_t prev_col_id, uint32_t next_col_id) {
        if (!keymap_->HasArena()) abort();
        return keymap_->ResetCurColInfoID(prev_col_id, next_col_id);
    }
    void SKTableMem::SetPrevColInfoID(){
        if (!keymap_->HasArena()) abort();
        keymap_->SetPrevColInfoID();
    }
    uint32_t SKTableMem::GetPreAllocSKTableID() {
        if (!keymap_->HasArena()) abort();
        return keymap_->GetPreAllocSKTableID();
    }
    uint32_t SKTableMem::GetNextSKTableID(){
        if (!keymap_->HasArena()) abort();
        return keymap_->GetNextSKTableID();
    }
    uint32_t SKTableMem::GetPrevColInfoID(){
        if (!keymap_->HasArena()) abort();
        return keymap_->GetPrevColInfoID();
    }
    uint32_t SKTableMem::GetCurColInfoID(){
        if (!keymap_->HasArena()) abort();
        return keymap_->GetCurColInfoID();
    }

    void SKTableMem::UpdateKeyInfoUsingUserKeyFromKeyQueue(Manifest *mf, SequenceNumber seq, bool acquirelock, std::string &col_info_table, SequenceNumber &largest_ikey, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue) {
        UserKey* uk = NULL;
        if (acquirelock) KeyMapGuardLock();

        for(int i = 0 ; i < 2 ; i++){//For Inactive & Active Queue
            if(!i ? !IsInactiveKeyQueueEmpty() : !IsActiveKeyQueueEmpty()){
                // do not need to insert userkeys if snapshot does not see them anyway.
                if (seq && GetSmallestSequenceInKeyQueue(!i ? InactiveKeyQueueIdx():InactiveKeyQueueIdx()^1) > seq)  
                    continue;
                /* Read Preemption Locking */
                KeyMapRandomReadGuardLock();
                uint8_t idx = !i ? InactiveKeyQueueIdx() : ExchangeActiveAndInActiveKeyQueue();
                while((uk = PopUserKeyFromKeyQueue(idx))){
                    /* 
                     * KeyInfo Format
                     *  : || 1B NR column(+"reloc" + "uk addr" flag) | 1st ColInfo | ... | Last ColInfo ||
                     * ColInfo
                     *  : || 1B Col info size | 1B ColID(+"ref cnt" & "del cmd" flag) | column information ||
                     */
                    uint32_t offset = 0;
                    bool submitted = true;//if true, the uk has been submitted and trxn has been committed.
                    uk->ColumnLock();
                    uint16_t size = uk->GetKeyInfoSize(mf, submitted);
                    if(submitted) if(!mf->RemoveUserKeyFromHashMap(uk->GetKeySlice())) abort();/*given key must exist*/
                    uk->ColumnUnlock();
                    bool key_info_exist = false;
                    Slice key_info = keymap_->Insert(uk->GetKeySlice(), size,  offset, key_info_exist);
                    assert(key_info.size());
                    char* key_info_data = const_cast<char*>(key_info.data());
                    if(key_info_exist){
                        bool resize = false;
                        Slice old_key_info = key_info;
                        /* KeyNode exist */
                        uint8_t nr_col = (uint8_t)(*(key_info.data()));
                        old_key_info.remove_prefix(1);//remove nr column

                        assert(!(nr_col & USE_USERKEY));
                        if(nr_col & RELOC_KEY_INFO){
                            /* Get the slice of Relocated Key Info. then insert the col info to UK */
                            uint32_t reloc_ofs = (*((uint32_t*)old_key_info.data()));
                            Slice old_key_info = keymap_->GetRelocatedKeyInfo(reloc_ofs);
                            nr_col = (uint8_t)(*(old_key_info.data()));
                            assert(!(nr_col & USE_USERKEY || nr_col & RELOC_KEY_INFO));
                            old_key_info.remove_prefix(1);//remove nr column
                            resize = uk->InsertColumnNodes(mf, nr_col + 1, old_key_info);
                            keymap_->FreeAllocatedMemory(reloc_ofs);
                        }else{
                            resize = uk->InsertColumnNodes(mf, (nr_col & NR_COL_MASK) + 1, old_key_info);
                        }
                        if(resize && submitted){
                            /* 
                             * Key info will be updated in this function.
                             * But, the size is changed.
                             */
                            if(!(size = uk->GetKeyInfoSize(mf))){
                                abort();
                            }
                            assert(size);

                        }
                    }
                    if(!submitted){
                        flush_candidate_key_deque_.push_back(offset);
                        *key_info_data = USE_USERKEY;//Set "use userkey" & clean nr_col
                        key_info_data++;
                        *((uint64_t*)key_info_data) = (uint64_t)uk;
                    }else{
                        /* Update Column Info */
                        if(size > key_info.size()){
                            //TODO : Atomic??
                            KeyMapHandle allocate_mem_offset  = keymap_->GetNextAllocOffset(size);
                            assert(allocate_mem_offset != 0);
                            char* key_info_data = const_cast<char*>(key_info.data());
                            *key_info_data = RELOC_KEY_INFO;//Set "use userkey" & clean nr_col
                            key_info_data++;
                            *((uint32_t*)key_info_data) = allocate_mem_offset;
                            key_info = Slice(keymap_->AllocateMemory(size, allocate_mem_offset), size);

                            assert(key_info.size()); 
                        }
                        bool has_del = false;
                        bool valid = uk->BuildColumnInfo(mf, size, key_info, largest_ikey, in_log_KBM_queue, has_del);
                        assert(size);
                        col_info_table.append(key_info.data(), size);
                        if(has_del){
                            KeySlice key = uk->GetKeySlice();
                            PutVarint16(&col_info_table, key.size());
                            col_info_table.append(key.data(), key.size());
                        }
                        assert(key_info.size()); 
                        if(!valid){
                            /*No invalid columns exists(col list is empty)*/
                            assert(!uk->GetReferenceCount());
                            mf->FreeUserKey(uk);
                        }else{
                            mf->FlushedKeyHashMapFindAndMergeUserKey(uk);
                        }
                    }
                    /* We don't need to check ref count here because the uk will not be freed for now. */
                    uint8_t wait_cnt = 0;
                    if(wait_cnt = GetRandomReadWaitCount()){
                        KeyMapRandomReadGuardUnLock();
                        while(wait_cnt == GetRandomReadWaitCount());
                        KeyMapRandomReadGuardLock();
                    }
                }
                KeyMapRandomReadGuardUnLock();

                // all user keys are consumed. reset smallest sequence number
                ResetSmallestSequenceInKeyQueue(idx);
            }
        }
        if (acquirelock) KeyMapGuardUnLock();
    }



    bool SKTableMem::InsertUserKeyToKeyMapFromInactiveKeyQueue(Manifest *mf, bool inactive, SequenceNumber seq, bool acquirelock) {
        bool flush_queue = false;
        UserKey* uk = NULL;
        if (acquirelock) KeyMapGuardLock();

        if(inactive ? !IsInactiveKeyQueueEmpty() : !IsActiveKeyQueueEmpty()){
            // do not need to insert userkeys if snapshot does not see them anyway.
            if (seq) {
                uint8_t idx = inactive?InactiveKeyQueueIdx():InactiveKeyQueueIdx()^1;
                if (GetSmallestSequenceInKeyQueue(idx) > seq) {
#ifdef CODE_TRACE
                    printf ("smallest %lu snapshot %lu skipped\n", GetSmallestSequenceInKeyQueue(idx), seq);
#endif
                    goto exit;
                }
            }
            flush_queue = true;
            /* Read Preemption Locking */
            KeyMapRandomReadGuardLock();
            uint8_t idx = inactive ? InactiveKeyQueueIdx() : ExchangeActiveAndInActiveKeyQueue();
            while((uk = PopUserKeyFromKeyQueue(idx))){
#ifdef CODE_TRACE
                std::string str("BEFORE ADD COL INFO SKTID ");
                char buffer[100];
                sprintf(buffer, "%d (flush_candi is %s)", skt_id_, flush_candidate_key_deque_.empty()?"empty":"not empty");
                str.append(buffer);
                uk->ScanColumnInfo(str);
#endif
                /* 
                 * KeyInfo Format
                 *  : || 1B NR column(+"reloc" + "uk addr" flag) | 1st ColInfo | ... | Last ColInfo ||
                 * ColInfo
                 *  : || 1B Col info size | 1B ColID(+"ref cnt" & "del cmd" flag) | column information ||
                 */
                uint32_t offset = 0;
                uint16_t size = uk->GetKeyInfoSizeForNew();
                bool key_info_exist = false;
                Slice key_info = keymap_->Insert(uk->GetKeySlice(), size,  offset, key_info_exist);
                char* key_info_data = NULL;
                if(key_info_exist){
                    /* KeyNode exist */
                    uint8_t nr_col = (uint8_t)(*(key_info.data()));
                    key_info_data = const_cast<char*>(key_info.data());
                    key_info.remove_prefix(1);//remove nr column

#if 0
                    if(nr_col & USE_USERKEY){
                        /* UserKey Merge */
                        UserKey *old_uk = (UserKey*)(*((uint64_t*)key_info.data()))/*Old UserKey Address*/;
                        uk->UserKeyMerge(old_uk);
                        mf->FreeUserKey(old_uk);
#ifdef CODE_TRACE
                        std::string str("AFTER MERGE SKTID ");
                        char buffer[100];
                        sprintf(buffer, "%d (flush_candi is %s)", skt_id_, flush_candidate_key_deque_.empty()?"empty":"not empty");
                        str.append(buffer);
                        uk->ScanColumnInfo(str);
#endif

                        /* the offest has already been added to flush_candidata_queue. */
                    }else {
#else
                        assert(!(nr_col & USE_USERKEY));
#endif
                        if(nr_col & RELOC_KEY_INFO){
                            /* Get the slice of Relocated Key Info. then insert the col info to UK */
                            uint32_t reloc_ofs = (*((uint32_t*)key_info.data()));
                            key_info = keymap_->GetRelocatedKeyInfo(reloc_ofs);
                            nr_col = (uint8_t)(*(key_info.data()));
                            assert(!(nr_col & USE_USERKEY || nr_col & RELOC_KEY_INFO));
                            key_info.remove_prefix(1);//remove nr column
                            uk->InsertColumnNodes(mf, nr_col + 1, key_info);
                            keymap_->FreeAllocatedMemory(reloc_ofs);
                        }else{
                            uk->InsertColumnNodes(mf, (nr_col & NR_COL_MASK) + 1, key_info);
                        }
                        flush_candidate_key_deque_.push_back(offset);
#ifdef CODE_TRACE
                        std::string str("AFTER INSERT COLINFO TO UK SKTID ");
                        char buffer[100];
                        sprintf(buffer, "%d (flush_candi is %s)", skt_id_, flush_candidate_key_deque_.empty()?"empty":"not empty");
                        str.append(buffer);
                        uk->ScanColumnInfo(str);
#endif
//                    }
                }else{
                    /* KeyNode does not exist */
                    assert(key_info.size());
                    key_info_data = const_cast<char*>(key_info.data());
                    flush_candidate_key_deque_.push_back(offset);
                }
                *key_info_data = USE_USERKEY;//Set "use userkey" & clean nr_col
#ifdef CODE_TRACE
                printf("[%d :: %s]key_info_data addr : %p(%d) Keymap(%p) arrena(%p) offset(%d) size(%d)  uk(%p) key(%s)\n", __LINE__, __func__,
                        key_info_data, *key_info_data, keymap_, keymap_->GetArena(), offset, size, uk, EscapeString(uk->GetKeySlice()).c_str());
#endif
                key_info_data++;
                *((uint64_t*)key_info_data) = (uint64_t)uk;
                /* We don't need to check ref count here because the uk will not be freed for now. */
#if 0
#ifdef USE_HOTSCOTCH
                if(!mf->RemoveUserKeyFromHopscotchHashMap(uk->GetKeySlice())){
                    printf("[%d :: %s]Fail to remove uk from hopscotch\n", __LINE__, __func__);
                    abort();
                }
#else
                if(!mf->RemoveUserKeyFromHashMap(uk->GetKeySlice())) abort();/*given key must exist*/
#endif
#endif
                uint8_t wait_cnt = 0;
                if(wait_cnt = GetRandomReadWaitCount()){
                    KeyMapRandomReadGuardUnLock();
                    /* 
                     * Write Lock has higher priority.  So, It can acquire lock again 
                     * To avoid this situation, wait for the read thread to aquires the lock.
                     * The read thread will decrease the count immediately after acquiring the lock.
                     * So, Spinlock(while) does not consume CPU a lot.
                     */
                    while(wait_cnt == GetRandomReadWaitCount());
                    KeyMapRandomReadGuardLock();
                }
            }
            KeyMapRandomReadGuardUnLock();

            // all user keys are consumed. reset smallest sequence number
            ResetSmallestSequenceInKeyQueue(idx);
        }
exit:
        if (acquirelock) KeyMapGuardUnLock();
        return flush_queue;

    }

    bool SKTableMem::PrefetchSKTable(Manifest* mf, bool dirtycache, bool no_cachecheck, bool nio) {

        bool prefetch = false;
        /*It can be sub-SKTable having no sktdf yet*/
        if(((no_cachecheck && !IsFetchInProgress()) || !IsCachedOrPrefetched()) && !IsKeymapLoaded() && !SKTDFLoadTryLock()){
            if(((no_cachecheck && !IsFetchInProgress()) || !IsCachedOrPrefetched()) && !IsKeymapLoaded()){
#ifndef NDEBUG
                uint16_t check = 0; 
                while(IsKeymapLoaded()){
                    if(!(check++ % 512))
                        printf("[%d :: %s]\n", __LINE__, __func__);
                }
#endif
                if (!nio) {
                    SetFetchInProgress();
                    if (!no_cachecheck) {
                        if (dirtycache){
                            mf->InsertSKTableMemCache(this, kDirtySKTCache, SKTableMem::flags_in_dirty_cache);
                        }else{
                            mf->InsertSKTableMemCache(this, kCleanSKTCache, SKTableMem::flags_in_clean_cache);
                        }
                    }

                    // Issue prefetch
                    int value_size = keymap_node_size_;
                    InSDBKey skt_key = GetMetaKey(mf->GetDBHash(), kSKTable, skt_id_);
                    Status s = Env::Default()->Get(skt_key, NULL, &value_size, kReadahead);
                    if (!s.ok()){
                        s.NotFound("SKTable meta does not exist");
                        abort();
                    }
#ifdef USE_HASHMAP_RANDOMREAD
                    value_size = keymap_hash_size_;
                    skt_key = GetMetaKey(mf->GetDBHash(), kHashMap, skt_id_);
                    s = Env::Default()->Get(skt_key, NULL, &value_size, kReadahead);
                    if (!s.ok()){
                        s.NotFound("SKTable hash meta does not exist");
                        abort();
                    }
#endif
#ifdef TRACE_READ_IO
                    g_sktable_async++;
#endif
                    prefetch = true;
                }
            }
            SKTDFLoadUnlock();
        }
        return prefetch;
    }

    void SKTableMem::DiscardSKTablePrefetch(Manifest* mf) {
        /*It can be sub-SKTable having no sktdf yet*/
        if(!IsCached() && !SKTDFLoadTryLock()){
            if(!IsKeymapLoaded()){ 
                if (ClearFetchInProgress()){
                    /* If it has not been prefetched or fetched*/
                    int value_size = keymap_node_size_;
                    InSDBKey skt_key = GetMetaKey(mf->GetDBHash(), kSKTable, skt_id_);
                    (Env::Default())->DiscardPrefetch(skt_key);

#ifdef USE_HASHMAP_RANDOMREAD
                    value_size = keymap_hash_size_;
                    skt_key = GetMetaKey(mf->GetDBHash(), kHashMap, skt_id_);
                    (Env::Default())->DiscardPrefetch(skt_key);
#endif
 

                }
            }
            SKTDFLoadUnlock();
        }
    }

#ifdef CODE_TRACE
    uint32_t skt_load_cnt = 0;
#endif
    /* Return value has two meaning 
     * 1. type == kSyncGet
     *    - The SKTable is in clean or dirty cache
     * 2. type == kNonblockingRead
     *    - Read success or not
     */

    bool SKTableMem::LoadSKTableDeviceFormat(Manifest *mf, GetType type) {
        // Get cached device format if available.
        bool incache = true;
#if 0
        /* Sub-SKTable can not be found before it is inserted to Skiplist. 
         * In previous version, UserKey has SKTable pointer. So that, this situation can happen.
         * But a UserKey does not have a SKTable pointer anymore in new version.
         */
        if (!ignore_waiting) {
            /*
             * The SKTable can be sub-SKT which is not inserted to skiplist & sorted list.
             * In this case the skt_df_node_ may not written to device yet.
             */
            while(IsSKTFlushing() && !skt_df_node_);
        }
#endif
        SKTDFLoadLock();
        if(!IsKeymapLoaded()){
#ifdef USE_HASHMAP_RANDOMREAD
            KeyMapHashMap* hashmap = nullptr;
            Arena* arena = mf->LoadKeymapFromDevice(GetSKTableID(), keymap_node_size_, keymap_hash_size_, hashmap, type);
#else
            Arena* arena = mf->LoadKeymapFromDevice(GetSKTableID(), keymap_node_size_, type);
#endif
            if (arena) {
#ifdef CODE_TRACE
                printf("[%d :: %s]Load Arena for SKTID %d\n", __LINE__, __func__, skt_id_);
#endif
#ifdef USE_HASHMAP_RANDOMREAD
                keymap_->InsertArena(arena, hashmap);
#else
                keymap_->InsertArena(arena);
#endif
                // size should be allocated size, not value size.
                incache = ClearFetchInProgress();
                SKTDFLoadUnlock();
#ifdef MEM_TRACE
                mf->IncSKTDF_MEM_Usage(GetBufferSize());
#endif                
                if (mf->CheckKeyEvictionThreshold()) mf->SignalSKTEvictionThread(); /* send signal when memory need to release */
                //mf->IncreaseSKTDFNodeSize(GetAllocatedSize());
                mf->IncreaseSKTDFNodeSize(GetBufferSize());
                /* In PrefetchSKTable(), this SKTableMem is already inserted to dirty list */
            }else{ 
                SKTDFLoadUnlock();
                /* Fail to read SKTable */
                if(type != kNonblockingRead)
                    abort();
                /* kNonblockingRead is only called after prefetch which conducted cache insertion */
                return false;
            }
        }else{
            SKTDFLoadUnlock();
        }
        if(type == kNonblockingRead && !incache)
            abort();
        return incache;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////

    SequenceNumber SKTableMem::GetLatestSequenceNumberFromSKTableMem(Manifest* mf, const KeySlice& key, uint8_t col_id, bool &deleted) {

        SequenceNumber seq_num = 0;
        if(!IsKeymapLoaded()){
            if(!LoadSKTableDeviceFormat(mf)){
                assert(!IsCachedOrPrefetched());
                mf->InsertSKTableMemCache(this, kCleanSKTCache, SKTableMem::flags_in_clean_cache);
            }
        }
        /* Read Preemption Locking */
        IncRandomReadWaitCount();
#if 1
        KeyMapRandomReadSharedLock();
#else
        KeyMapSharedLock(); 
#endif
        DecRandomReadWaitCount();

        assert(IsKeymapLoaded());
        KeyMapHandle handle = keymap_->Search(key);
        if (handle) {
            void * col_uk = nullptr;
            Slice col_info_slice = keymap_->GetColumnData(handle, col_id, col_uk);
            if (col_uk) {
                UserKey *uk = (UserKey*)col_uk;
                uk->ColumnLock(col_id);
                seq_num = uk->GetLatestColumnNodeSequenceNumber(col_id, deleted);
                uk->ColumnUnlock(col_id);
            }else if (col_info_slice.size()) {
                ColumnData col_data;
                col_data.init();
                ColMeta* colmeta = (ColMeta*)(const_cast<char*>(col_info_slice.data()));
                colmeta->ColInfo(col_data);
                seq_num = col_data.iter_sequence;
                deleted = col_data.is_deleted;
            }
        }
#if 1
        KeyMapRandomReadSharedUnLock();
#else
        KeyMapSharedUnLock(); 
#endif
        return seq_num;
    }

    bool SKTableMem::GetColumnInfo(Manifest* mf, const Slice& key, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, ColumnScratchPad& column, NonBlkState &non_blk_flag_) {


        SKTableMem *next = mf->Next(this);
        if(next &&
                (mf->GetComparator()->Compare(next->GetBeginKeySlice(), key) <= 0)){
            /*The key belongs to next(or beyond) SKTable. */
            return false;
        }

        if(!IsKeymapLoaded()){
            if (non_blk_flag_ == kNonblockingBefore) {
                non_blk_flag_ = kNonblockingAfter;
                return true;
            }
            if(!LoadSKTableDeviceFormat(mf)){
                if(!IsCached()){
                    mf->InsertSKTableMemCache(this, kCleanSKTCache, SKTableMem::flags_in_clean_cache);
                }
            }
            assert(IsKeymapLoaded());
        }
        /* Read Preemption Locking */
        IncRandomReadWaitCount();
#if 1
        KeyMapRandomReadSharedLock();
#else
        KeyMapSharedLock(); 
#endif
        DecRandomReadWaitCount();

        KeyMapHandle handle = keymap_->Search(key);
        if (handle) {
            void * col_uk = nullptr;
            Slice col_info_slice = keymap_->GetColumnData(handle, col_id, col_uk);
            if (col_uk) {
                /* If Col info in Keymap has UserKey Pointer */
                UserKey *uk = (UserKey*)col_uk;
#ifdef CODE_TRACE
                std::string str("Get Column from UK ");
                char buffer[5];
                sprintf(buffer, "%d", skt_id_);
                str.append(buffer);
                uk->ScanColumnInfo(str);
#endif
                bool deleted = false;
                uk->ColumnLock(col_id);
                ColumnNode* col_node = uk->GetColumnNode(col_id, deleted, cur_time, seq_num, mf->GetTtlFromTtlList(col_id));
                if (col_node) {
                    /* it must not has UserValue */
                    if (col_node->HasUserValue()) {
                        column.uv = col_node->GetUserValue();
                    } else {
                        uint64_t ttl = col_node->GetTS();
                        uint64_t seq = col_node->GetBeginSequenceNumber();
                        if (seq_num >= seq && (!ttl || mf->GetTtlFromTtlList(col_id) == 0 || (ttl + mf->GetTtlFromTtlList(col_id)) > cur_time)){
                            column.ikey_seq_num = col_node->GetInSDBKeySeqNum();
                            column.ivalue_size = col_node->GetiValueSize();
                            column.offset = col_node->GetKeyBlockOffset();
                        }
                    }
                }
                uk->ColumnUnlock(col_id);
            } else if (col_info_slice.size()) {
                /* If type is del?? */
                ColumnData col_data;
                col_data.init();
                ColMeta* colmeta = (ColMeta*)(const_cast<char*>(col_info_slice.data()));
                colmeta->ColInfo(col_data);
                if (!col_data.is_deleted) {
                    if (seq_num >= col_data.iter_sequence && (!col_data.has_ttl || mf->GetTtlFromTtlList(col_id) == 0 || (col_data.ttl + mf->GetTtlFromTtlList(col_id)) > cur_time)){
                        column.ikey_seq_num = col_data.ikey_sequence;
                        column.ivalue_size = col_data.ikey_size;
                        column.offset = col_data.kb_offset;
                    }
                }
#ifdef CODE_TRACE
                std::string str(key.data(), key.size());
                printf("[%d : %s];Get column from col_info SKTID %d;|;key;%s;|;key size;%d;|;uk;%p;|;%s;|;Seq;%ld;~;%ld;|;iKey;%ld;|;KB size;%u;|;offset;%u;|\n", __LINE__, __func__, skt_id_, EscapeString(key).c_str(), key.size(), NULL, !col_data.is_deleted?"Put" : "Del", col_data.iter_sequence, -1, column.ikey_seq_num, column.ivalue_size,  column.offset);
#endif
            }
        }
#if 1 
        KeyMapRandomReadSharedUnLock();
#else
        KeyMapSharedUnLock(); 
#endif
        return true;
    }
#if 0
    /////////////////////////////////////////////////////////////////////////////////////////

    UserKey* SKTableMem::MoveToNextKeyInKeyMap(Manifest* mf, UserKey* ukey)
    {
        UserKey* next_ukey = NULL;

        while(true) {
            keymap_mu_.lock_shared();
            next_ukey = keymap_.Next(ukey);
            if(next_ukey){
                next_ukey = mf->IncreaseUserKeyReference(next_ukey);
                if(next_ukey) next_ukey = next_ukey->IsSKTableEvictionInProgress();
                if(next_ukey) break;
            } else {
                break;
            }
            keymap_mu_.unlock_shared();
        }

        keymap_mu_.unlock_shared();
        ukey->DecreaseReferenceCount();
        return next_ukey;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////

#if 0
    void SKTableMem::EvictKBPM(Manifest *mf){
        KeyMapHandle handle = keymap_->First();
        while(handle) {
            if(GetSKTRefCnt()){
                //assert(!force_evict);
                break;
            }
            /*
             * Clear reference bit
             */
            KeyMeta* meta = keymap_->GetKeyMeta(handle);
            if (!meta) abort();
            if (meta->HasUserKey()) {
                /* userkey skip */
                handle = keymap_->Next(handle);
                continue;
            }
            uint8_t nr_col = meta->NRColumn();
            ColMeta* col_array = meta->ColMetaArray();
            char* col_addr = (char*)col_array;
            ColumnData col_info;
            for (uint8_t i = 0; i < nr_col; i++) {
                if (col_array->HasReference()) {
                    if (col_array->HasSecondChanceReference()) {
                        col_info.init();
                        col_array->ColInfo(col_info);
                        KeyBlockPrimaryMeta* kbpm = mf->FindKBPMFromHashMap(col_info.ikey_sequence);
                        if(kbpm){
                            if(kbpm->ClearRefBitAndTryEviction(mf, col_info.kb_offset&COLUMN_NODE_OFFSET_MASK)){
#ifdef CODE_TRACE
                                printf("[%d :: %s]Free (kbpm : 0x%p), ikey = %ld, offset : %d, flag : %x\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber(), col_info.kb_offset, kbpm->GetFlag());
#endif
                                mf->FreeKBPM(kbpm);
                            }

                        }
                        //#ifndef NDEBUG
#ifdef CODE_TRACE
                        else{
                            printf("[%d :: %s]KBPM must exists\n", __LINE__, __func__);
                            abort();
                        }
#endif
                        col_array->ClearReference();
                    } else {
                        col_array->SetSecondChanceReference();
                    }
                }
                col_array = (ColMeta*)(col_addr + col_array->GetColSize());
            }
            handle = keymap_->Next(handle);
        }
        return;
    }
#endif

    bool SKTableMem::SKTDFReclaim(Manifest *mf){
#if 1
        if(GetSKTRefCnt()){
            //assert(!force_evict);
            return true;
        }
        return false;
#else
        KeyMapHandle handle = keymap_->First();
        while(handle) {
            if(GetSKTRefCnt()){
                //assert(!force_evict);
                return true;
                break;
            }
            /*
             * Clear reference bit
             */
            KeyMeta* meta = keymap_->GetKeyMeta(handle);
            assert(meta);
            assert(!meta->HasUserKey());
            uint8_t nr_col = meta->NRColumn();
            ColMeta* col_array = meta->ColMetaArray();
            char* col_addr = (char*)col_array;
            ColumnData col_info;
            for (uint8_t i = 0; i < nr_col; i++) {
                if (col_array->HasReference()) {
                    col_info.init();
                    col_array->ColInfo(col_info);
                    KeyBlockPrimaryMeta* kbpm = mf->FindKBPMFromHashMap(col_info.ikey_sequence);
                    if(kbpm){
                        if(kbpm->ClearRefBitAndTryEviction(mf, col_info.kb_offset&COLUMN_NODE_OFFSET_MASK)){
#ifdef CODE_TRACE
                            printf("[%d :: %s]Free (kbpm : 0x%p), ikey = %ld, offset : %d, flag : %x\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber(), col_info.kb_offset, kbpm->GetFlag());
#endif
                            mf->FreeKBPM(kbpm);
                        }

                    }
                    col_array->ClearReference();
                }
                col_array = (ColMeta*)(col_addr + col_array->GetColSize());
            }
            handle = keymap_->Next(handle);
        }
        return false;
#endif
    }

    bool SKTableMem::SKTDFReclaim(Manifest *mf, int64_t &reclaim_size, bool &active, bool force_evict, uint32_t &put_col, uint32_t &del_col){
#ifdef MEM_TRACE
        mf->IncEvictReclaim();
#endif
        bool referenced = false;
        active = false;
        KeyMapHandle handle = keymap_->First();
        while(handle) {
            if(GetSKTRefCnt()){
                //assert(!force_evict);
                referenced = true;
                active = true;
#ifdef MEM_TRACE
                mf->IncEvictStopSKT();
#endif
                break;
            }
            /*
             * Clear reference bit
             */
            KeyMeta* meta = keymap_->GetKeyMeta(handle);
            if (!meta) abort();
#if 0
            if (meta->IsRelocated()) {
                abort();
            }
#endif
            if (meta->HasUserKey()) {
                /* userkey skip */
            //    abort();
                referenced = true;
                active = true;
                handle = keymap_->Next(handle);
                continue;
            }
            uint8_t nr_col = meta->NRColumn();
            ColMeta* col_array = meta->ColMetaArray();
            char* col_addr = (char*)col_array;
            ColumnData col_info;
            for (uint8_t i = 0; i < nr_col; i++) {
                if (col_array->IsDeleted()) del_col++;
                else put_col++;
#if 0
                if (col_array->HasReference()) {
                    if (col_array->HasSecondChanceReference() || force_evict) {
                        col_info.init();
                        col_array->ColInfo(col_info);
                        KeyBlockPrimaryMeta* kbpm = mf->FindKBPMFromHashMap(col_info.ikey_sequence);
                        if(kbpm){
                            if(kbpm->ClearRefBitAndTryEviction(mf, col_info.kb_offset&COLUMN_NODE_OFFSET_MASK, reclaim_size)){
#ifdef CODE_TRACE
                                printf("[%d :: %s]Free (kbpm : 0x%p), ikey = %ld, offset : %d, flag : %x\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber(), col_info.kb_offset, kbpm->GetFlag());
#endif
                                mf->FreeKBPM(kbpm);
                            }

                        }
//#ifndef NDEBUG
#ifdef CODE_TRACE
                        else{
                            printf("[%d :: %s]KBPM must exists\n", __LINE__, __func__);
                            abort();
                        }
#endif
                        col_array->ClearReference();
                    } else {
                        referenced = true;
                        col_array->SetSecondChanceReference();
                    }
                }
#endif
                col_array = (ColMeta*)(col_addr + col_array->GetColSize());
            }
            handle = keymap_->Next(handle);
        }
        return referenced;

    }

    bool SKTableMem::CompareKey(UserKeyComparator comp, const KeySlice& key, int& delta){
        delta = comp(GetBeginKeySlice(), key);
#ifdef CODE_TRACE
        printf("[%d :: %s][SKT ID : %d]delta : %d(skt begin key : %s | given key : %s)\n", __LINE__, __func__, skt_id_, delta, GetBeginKeySlice().data(),  key.data());
#endif
        return true;
    }

    KeySlice SKTableMem::GetKeySlice(){ return GetBeginKeySlice();}

#define SEQ_NUM_SHIFT 7
#define TTL_SHIFT 6

#ifdef KV_TIME_MEASURE 
    volatile cycles_t updatekeyinfo_cycle = 0 ;
    volatile cycles_t split_cycle = 0 ;
    volatile uint32_t updatekeyinfo_cnt = 0 ;
    volatile uint32_t split_cnt = 0 ;
    volatile cycles_t insertuk_cycle = 0 ;
    volatile uint32_t insertuk_cnt = 0 ;
    volatile cycles_t insertuk_in_split_cycle = 0 ;
    volatile uint32_t insertuk_in_split_cnt = 0 ;
#endif




    void SKTableMem::UpdateKeyInfo(Manifest* mf, std::string &col_info_table, SequenceNumber &largest_ikey, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue){
#ifdef KV_TIME_MEASURE 
        cycles_t t1, t2;
        t1 = get_cycles();
#endif
        std::lock_guard<folly::SharedMutex> l(keymap_mu_);
        //KeyMapGuardLock();
        KeyMapRandomReadGuardLock();
        for (auto it = flush_candidate_key_deque_.begin(); it!=flush_candidate_key_deque_.end();){
            uint32_t offset= *it;
            assert(offset);
            Slice keyinfo = keymap_->GetKeyInfo(offset);
#ifdef CODE_TRACE
            printf("[%d :: %s]key_info_data addr : %p(%d) offset(%d) size(%ld)\n", __LINE__, __func__, keyinfo.data(), *keyinfo.data(), offset, keyinfo.size());
#endif
            if (keyinfo.size() == 0) abort();
            if (!((uint8_t)(*(keyinfo.data())) & USE_USERKEY)) abort();
            UserKey *uk = ((UserKey*)(*((uint64_t*)(keyinfo.data()+1))));
            uint16_t new_key_info_size = 0;

            uk->ColumnLock();
            if(new_key_info_size = uk->GetKeyInfoSize(mf)){//If trxn is not completed yet, do not apply the col info the keymap_
                if(!mf->RemoveUserKeyFromHashMap(uk->GetKeySlice())) abort();/*given key must exist*/
                uk->ColumnUnlock();
                if(new_key_info_size > keyinfo.size()){
                    //TODO : Atomic??
                    KeyMapHandle allocate_mem_offset  = keymap_->GetNextAllocOffset(new_key_info_size);
                    assert(allocate_mem_offset != 0);
                    char* key_info_data = const_cast<char*>(keyinfo.data());
                    *key_info_data = RELOC_KEY_INFO;//Set "use userkey" & clean nr_col
                    key_info_data++;
                    *((uint32_t*)key_info_data) = allocate_mem_offset;
                    keyinfo = Slice(keymap_->AllocateMemory(new_key_info_size, allocate_mem_offset), new_key_info_size);

                    assert(keyinfo.size()); 
                }
                bool has_del = false;
                bool valid = uk->BuildColumnInfo(mf, new_key_info_size, keyinfo, largest_ikey, in_log_KBM_queue, has_del);
                it = flush_candidate_key_deque_.erase(it);
                assert(new_key_info_size);
                col_info_table.append(keyinfo.data(), new_key_info_size);
                if(has_del){
                    KeySlice key = uk->GetKeySlice();
                    PutVarint16(&col_info_table, key.size());
                    col_info_table.append(key.data(), key.size());
                }
                assert(keyinfo.size()); 
#ifdef CODE_TRACE
                std::string str("Store col info the keymap SKTID ");
                char buffer[200];
                sprintf(buffer, "%d (flush_candi is %s)(col_info_table size : %d)(keyinfo size : %d)", skt_id_, flush_candidate_key_deque_.empty()?"empty":"not empty", col_info_table.size(), keyinfo.size());
                str.append(buffer);
                uk->ScanColumnInfo(str);
#endif
                if(!valid){
                    /*No invalid columns exists(col list is empty)*/
                    assert(!uk->GetReferenceCount());
                    mf->FreeUserKey(uk);
                }else{
                    mf->FlushedKeyHashMapFindAndMergeUserKey(uk);
                }
            }else{
                uk->ColumnUnlock();
                it++;
            }
out:
#if 0
            if(col_info_table.size() >= kDeviceRquestMaxSize/2)
                break;
#endif
            uint8_t wait_cnt = 0;
            if(wait_cnt = GetRandomReadWaitCount()){
#if 1
                KeyMapRandomReadGuardUnLock();
#else
                KeyMapGuardUnLock();
#endif
                /* 
                 * Write Lock has higher priority.  So, It can acquire lock again 
                 * To avoid this situation, wait for the read thread to aquires the lock.
                 * The read thread will decrease the count immediately after acquiring the lock.
                 * So, Spinlock(while) does not consume CPU a lot.
                 */
                while(wait_cnt == GetRandomReadWaitCount());
#if 1
                KeyMapRandomReadGuardLock();
#else
                KeyMapGuardLock();
#endif

            }
        }
#if 1
        KeyMapRandomReadGuardUnLock();
#endif
#ifdef KV_TIME_MEASURE 
        t2 = get_cycles();
        updatekeyinfo_cycle += t2-t1;
        updatekeyinfo_cnt++;
#endif
    } 

    // acquire keymap write lock berfore entering
    void SKTableMem::SplitInactiveKeyQueue(Manifest* mf, std::vector<std::pair<SKTableMem*, Slice>> &sub_skt){
        UserKey* uk = NULL;
        int nr_sub_skt = sub_skt.size();

#ifdef KV_TIME_MEASURE 
        cycles_t t1, t2;
        t1 = get_cycles();
#endif
        if(!IsActiveKeyQueueEmpty()){
            //std::lock_guard<folly::SharedMutex> l(keymap_mu_);

            /* 
             * Read Preemption Locking 
             * Random read lock is not necessary because we don't touch keymap
             */
            //KeyMapRandomReadGuardLock();
            uint8_t idx = ExchangeActiveAndInActiveKeyQueue();

            SimpleLinkedNode *cur, *next;
            // reset smallest sequence number
            ResetSmallestSequenceInKeyQueue(idx);
            for (cur = key_queue_[idx].newest(); cur; cur = next){
                next = key_queue_[idx].next(cur);
                UserKey *uk= class_container_of(cur, UserKey, link_);
                Slice key_slice = uk->GetKeySlice();
                int j = 0;
                for(j = nr_sub_skt - 1 ; j >= 0 ; --j){
                    // store smallest sequence number in sub sktable
                    if(mf->GetComparator()->Compare(sub_skt[j].second, key_slice) <= 0){
                        /*migration*/
                        key_queue_[idx].Delete(cur);
                        sub_skt[j].first->PushUserKeyToInactiveKeyQueue(uk);
                        // store smallest sequence number in sub sktable
                        sub_skt[j].first->SetSmallestSequenceInKeyQueue(idx, uk->GetSmallestSequence());
                        break;
                    }
                }
                if (j<0) {
                    // store smallest sequence number in main sktable
                    SequenceNumber seq = uk->GetSmallestSequence();
                    if (key_queue_smallest_seq_[idx] > seq) {
                        key_queue_smallest_seq_[idx] = seq;
                    }
                }
            }
        }
#ifdef KV_TIME_MEASURE 
        t2 = get_cycles();
        insertuk_in_split_cycle += t2-t1;
        insertuk_in_split_cnt++;
#endif
#if 1
        if(!IsInactiveKeyQueueEmpty()){
            InsertUserKeyToKeyMapFromInactiveKeyQueue(mf, true, 0, false);
        }
        for(int i = 0 ; i < nr_sub_skt ; ++i){
            if(!sub_skt[i].first->IsInactiveKeyQueueEmpty()){
                sub_skt[i].first->InsertUserKeyToKeyMapFromInactiveKeyQueue(mf, true, 0, false);
            }
        }
#endif
    }


    InSDBKey SKTableMem::StoreColInfoTable(Manifest *mf, std::string &col_info_table, std::string &comp_col_info_table) {
        Status s;
        uint32_t col_info_table_size = col_info_table.size();
        char* data = const_cast<char*>(col_info_table.data());
        EncodeFixed32(data, col_info_table_size);
        uint32_t crc = crc32c::Value(data + COL_INFO_TABLE_META, col_info_table_size -COL_INFO_TABLE_META);
        EncodeFixed32(data+COL_INFO_TABLE_CRC_LOC, crc32c::Mask(crc));

        size_t outlen{};

#ifdef HAVE_SNAPPY
        uint32_t size = SizeAlignment(snappy::MaxCompressedLength(col_info_table_size) + COL_INFO_TABLE_META/*comp size & CRC*/);
        if (comp_col_info_table.size() < size){
            comp_col_info_table.reserve(size);
        }
        char* comp_buffer = const_cast<char*>(comp_col_info_table.data());

        size_t inlen{ col_info_table.size() };
        snappy::RawCompress(data, inlen, comp_buffer + COMP_COL_INFO_TABLE_SIZE, &outlen); /* compress data except KEYMAP_HDR */
        if (outlen < (inlen - (inlen / 8u))) {
            data = comp_buffer;
            EncodeFixed32(data, outlen | KEYMAP_COMPRESS);
            outlen = SizeAlignment(outlen + COMP_COL_INFO_TABLE_SIZE);
        } else
#endif 
        {
            outlen = SizeAlignment(col_info_table_size);
        }
#if 0
        Slice value(data, outlen);
        InSDBKey colinfo_key = GetColInfoKey(mf->GetDBHash(), keymap_->GetSKTableID(), keymap_->IncCurColInfoID());
        s = mf->GetEnv()->Put(colinfo_key, value, true); 
#endif

        uint32_t num_col_table = (outlen + kDeviceRquestMaxSize - 1) / kDeviceRquestMaxSize;
        uint32_t remain_size = outlen;

        InSDBKey colinfo_key;
        for (uint32_t i = 0; i < num_col_table ; i++) { // split and save request.
            colinfo_key = GetColInfoKey(mf->GetDBHash(), keymap_->GetSKTableID(), keymap_->IncCurColInfoID());
            size_t req_size = (remain_size < kDeviceRquestMaxSize) ? remain_size : kDeviceRquestMaxSize;
            Slice value(data, req_size);
            Status s = mf->GetEnv()->Put(colinfo_key, value, true);
            if (!s.ok()){
                printf("[%d :: %s]Fail to write Col info Table\n", __LINE__, __func__);
                abort();
            }
            remain_size -= req_size;
            data += req_size;
        }
        return colinfo_key;
    }

#ifdef USE_HASHMAP_RANDOMREAD
    Status SKTableMem::StoreKeymap(Manifest *mf, Keymap* sub, uint32_t &io_size, uint32_t &hash_data_size) 
#else
    Status SKTableMem::StoreKeymap(Manifest *mf, Keymap* sub, uint32_t &io_size)
#endif
    {
        Status s;
        std::string hash_data;
#ifdef USE_HASHMAP_RANDOMREAD
        sub->BuildHashMapData(hash_data);
        hash_data_size = hash_data.size();
        char* data = const_cast<char*>(hash_data.data());
        uint32_t crc = crc32c::Value(data + 4 /*crc 4B*/, hash_data_size -4/*crc 4B */);
        *((uint32_t*)data) = crc32c::Mask(crc);
        InSDBKey hashkey = GetMetaKey(mf->GetDBHash(), kHashMap, sub->GetSKTableID());
        Slice hash_value(data, hash_data_size);
        s = mf->GetEnv()->Put(hashkey, hash_value, true);
        if (!s.ok()) return s;
#endif

        KEYMAP_HDR *hdr = nullptr;
        io_size = 0;
        uint32_t keymap_size = sub->GetAllocatedSize();
#ifdef USE_HASHMAP_RANDOMREAD
        data = (sub->GetArena())->GetBaseAddr();
#else
        char* data = (sub->GetArena())->GetBaseAddr();
#endif
#ifdef HAVE_SNAPPY
        uint32_t compressed_data_size = SizeAlignment(sizeof(KEYMAP_HDR) + snappy::MaxCompressedLength(keymap_size - sizeof(KEYMAP_HDR)));
        Slice comp_slice = mf->AllocSKTableBuffer(compressed_data_size);
        char* comp_buffer = const_cast<char*>(comp_slice.data());
        size_t outlen = 0;
        size_t inlen = keymap_size - sizeof(KEYMAP_HDR);
        snappy::RawCompress(data + sizeof(KEYMAP_HDR), inlen, comp_buffer + sizeof(KEYMAP_HDR), &outlen); /* compress data except KEYMAP_HDR */
        if (outlen < (inlen - (inlen/ 8u))) {
            memcpy(comp_buffer, data, sizeof(KEYMAP_HDR)); /* copy keymap hdr */
            KEYMAP_HDR* hdr = (KEYMAP_HDR*)comp_buffer;
            hdr->keymap_size = outlen | KEYMAP_COMPRESS;

            //printf("[%s:%d] hdr skt_id(%d) prealloc_skt_id(%d) next_skt_id(%d)\n", __func__, __LINE__,  hdr->skt_id, hdr->prealloc_skt_id, hdr->next_skt_id);

#ifdef USE_HASHMAP_RANDOMREAD
            hdr->hashmap_data_size = hash_data_size;
#endif
            uint32_t crc = crc32c::Value(comp_buffer + 4 /*crc 4B*/, outlen + sizeof(KEYMAP_HDR) -4/*crc 4B */);
            hdr->crc = crc32c::Mask(crc);
            io_size = outlen + sizeof(KEYMAP_HDR);
            uint32_t peding_size = (io_size % kRequestAlignSize) ? (kRequestAlignSize - (io_size % kRequestAlignSize)) : 0;
            if ((io_size + peding_size) > compressed_data_size) io_size = compressed_data_size;
            else io_size += peding_size;
            InSDBKey subkey = GetMetaKey(mf->GetDBHash(), kSKTable, sub->GetSKTableID());
            Slice value(comp_buffer, io_size);
#ifdef CODE_TRACE
            printf("[%d :: %s](prev col_table_id : new id = %d : %d)Store SKTID %d sub %d (flush_candidate_key_deque_ is %s)\n", __LINE__, __func__, GetPrevColInfoID(), GetCurColInfoID(), GetSKTableID(), sub->GetSKTableID(), flush_candidate_key_deque_.empty()?"empty":"not empty");
#endif
            s = mf->GetEnv()->Put(subkey, value, true);
            mf->FreeSKTableBuffer(comp_slice);
        } else
#endif 
        {
#ifdef HAVE_SNAPPY
            mf->FreeSKTableBuffer(comp_slice);
#endif

            hdr = (KEYMAP_HDR*)data;
            hdr->keymap_size = keymap_size - sizeof(KEYMAP_HDR);
#ifdef USE_HASHMAP_RANDOMREAD
            hdr->hashmap_data_size = hash_data_size;
#endif
            uint32_t crc = crc32c::Value(data + 4 /*crc 4B*/, keymap_size -4/*crc 4B */);
            hdr->crc = crc32c::Mask(crc);
            uint32_t peding_size = ((keymap_size % kRequestAlignSize) ? (kRequestAlignSize - (keymap_size % kRequestAlignSize)) : 0);
            if (keymap_size + peding_size < sub->GetBufferSize()) io_size = keymap_size + peding_size;
            else io_size = sub->GetBufferSize();
            InSDBKey subkey = GetMetaKey(mf->GetDBHash(), kSKTable, sub->GetSKTableID());
            Slice value(data, io_size);
#ifndef CODE_TRACE
            printf("[%d :: %s](prev col_table_id : new id = %d : %d)Store SKTID %d(flush_candidate_key_deque_ is %s)\n", __LINE__, __func__, GetPrevColInfoID(), GetCurColInfoID(), GetSKTableID(), flush_candidate_key_deque_.empty()?"empty":"not empty");
#endif
            s = mf->GetEnv()->Put(subkey, value, true); 
        }
        return s;
    }

    void SKTableMem::DeleteSKTDeviceFormatCache(Manifest* mf, bool skip) {
        uint32_t io_size = 0;
#ifdef USE_HASHMAP_RANDOMREAD
        uint32_t hash_data_size = 0;
#endif
        if (!keymap_->HasArena()) return;
#ifdef MEM_TRACE
        mf->DecSKTDF_MEM_Usage(GetBufferSize());
#endif                
        mf->DecreaseSKTDFNodeSize(GetBufferSize());


        uint32_t prev_colinfo_id = GetPrevColInfoID();
        uint32_t cur_col_info_id = GetCurColInfoID();
        if (!skip && prev_colinfo_id != cur_col_info_id) {
#ifdef CODE_TRACE
            printf("[%d :: %s](prev col_table_id : new id = %d : %d)Store SKTID %d(flush_candidate_key_deque_ is %s)\n", __LINE__, __func__, GetPrevColInfoID(), GetCurColInfoID(), GetSKTableID(), flush_candidate_key_deque_.empty()?"empty":"not empty");
#endif

            assert (GetAllocatedSize() <= 0x200000);
            SetPrevColInfoID();
#ifdef USE_HASHMAP_RANDOMREAD
            Status s =  StoreKeymap(mf, keymap_ , io_size, hash_data_size);
#else
            Status s =  StoreKeymap(mf, keymap_ , io_size);
#endif
            if (!s.ok()) {
                printf("[%d :: %s] fail to flush sub key %d(size : 0x%x)\n", __LINE__, __func__, GetSKTableID(), io_size);
                abort();
            }
#ifdef USE_HASHMAP_RANDOMREAD
            SetKeymapNodeSize(io_size, hash_data_size);
#else
            SetKeymapNodeSize(io_size);
#endif
            for (uint32_t i = prev_colinfo_id; i < cur_col_info_id; i++) {
                InSDBKey colinfo_key = GetColInfoKey(mf->GetDBHash(), skt_id_, i);
                mf->GetEnv()->Delete(colinfo_key);
            }

        }
#ifdef CODE_TRACE
        else
            printf("[%d :: %s](prev col_table_id : new id = %d : %d)SKIP Store SKTID %d(flush_candidate_key_deque_ is %s)\n", __LINE__, __func__, GetPrevColInfoID(), GetCurColInfoID(), GetSKTableID(), flush_candidate_key_deque_.empty()?"empty":"not empty");
#endif
        keymap_->InsertArena(nullptr);
    }

    Status SKTableMem::FlushSKTableMem(Manifest *mf, std::string &col_info_table, std::string &comp_col_info_table, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue)
    {/*force == true for DB termination*/
        Status s;
        std::vector<std::pair<SKTableMem*, Slice>> sub_skt_;
        std::vector<Keymap*> sub_skt_keymap;
        SequenceNumber largest_ikey = 0;
        std::deque<uint32_t> del_skt_list;
        Keymap *split_main = nullptr;
        uint32_t io_size = 0; 
#ifdef USE_HASHMAP_RANDOMREAD
        uint32_t hash_data_size = 0; 
#endif
        del_skt_list.clear();

        col_info_table.resize(8);/*4 byte is for size and another 4 byte is for CRC*/

        if(!IsKeymapLoaded()) abort();

        /* update_key_info is for skt keymap memory reclaim 
         * If keymap has UserKey pointer which is inserted in InsertUserKeyToKeyMapFromInactiveKeyQueue() , the keymap can not be freed.
         */
        bool update_key_info = false;
        bool only_col_table_update = false;
        //int32_t old_size = GetAllocatedSize();
        int32_t old_size = GetBufferSize();
        if(flush_candidate_key_deque_.size()){
            UpdateKeyInfo(mf, col_info_table, largest_ikey, in_log_KBM_queue);
        }
        /* Move keys in unsorted key queue to FlushCandidateQueue 
         * Unsorted Key Queue : The keys has not been inserted to keymap yet
         * FlushCandidateQueue : The keys has been inserted to keymap. But the col_info has UserKey Addr 
         */
        if(flush_candidate_key_deque_.empty()){
            /* 
             * Check split here 
             * all key info in keymap must be completed(No UserKey point exists in keymap
             * So, we need to 
             */
            if(!GetSKTRefCnt() && ((keymap_->GetAllocatedSize() - keymap_->GetFreedMemorySize() > kMaxSKTableSize * 2) // If size of keymap is bigger than 2 times of kMaxSKTableSize, split the SKTable
                        || (keymap_->GetAllocatedSize() > kDeviceRquestMaxSize ))) { // If size of keymap is bigger than kMaxRequestSize 2M.
                /*Create SKTable with keymap & Insert the SKTable to skiplist from last one. */
#ifdef KV_TIME_MEASURE 
                cycles_t t1, t2;
                t1 = get_cycles();
#endif

                Keymap* sub = nullptr;
                uint32_t next_skt_id = 0;
                uint32_t last_next_skt_id = 0; // Used for last sub SKTable ID
                uint32_t prealloc_skt_id = 0;

                std::lock_guard<folly::SharedMutex> l(keymap_mu_);
                if(!flush_candidate_key_deque_.empty() || GetSKTRefCnt()) {
                    goto split_exit;
                }
                keymap_->SplitKeymap(mf, sub_skt_keymap);
                assert(sub_skt_keymap.size() > 0);
                split_main = sub_skt_keymap[0];
                sub_skt_keymap.erase(sub_skt_keymap.cbegin()); /* extract split main ... main already has keymap_hdr */
                if (sub_skt_keymap.size() == 0) goto process_main;
                /* processing sub sktable */
                next_skt_id = GetPreAllocSKTableID();
                last_next_skt_id = GetNextSKTableID(); // Used for last sub SKTable ID
                prealloc_skt_id = mf->GenerateNextSKTableID();
                split_main->SetNextSKTableID(next_skt_id);
                split_main->SetPreAllocSKTableID(prealloc_skt_id);
                for(int i = 0; i < sub_skt_keymap.size()-1; i++) {
                    sub = sub_skt_keymap[i];
                    /////////////////////////////////////////////////
                    /* Create Sub SKTable's SKT IDs */
                    sub->SetSKTableID(next_skt_id);
                    next_skt_id = mf->GenerateNextSKTableID();
                    prealloc_skt_id = mf->GenerateNextSKTableID();
                    sub->SetNextSKTableID(next_skt_id);
                    sub->SetPreAllocSKTableID(prealloc_skt_id);
                }
                /* For last Sub SKTable */
                sub = sub_skt_keymap[sub_skt_keymap.size() -1];
                sub->SetSKTableID(next_skt_id);
                sub->SetNextSKTableID(last_next_skt_id);
                prealloc_skt_id = mf->GenerateNextSKTableID();
                sub->SetPreAllocSKTableID(prealloc_skt_id);
                io_size = 0;
#ifdef USE_HASHMAP_RANDOMREAD
                hash_data_size = 0; 
#endif
                /* write sub sktable */
                for(int i = 0; i < sub_skt_keymap.size(); i++) {
                    sub = sub_skt_keymap[i];
#ifdef USE_HASHMAP_RANDOMREAD
                    s = StoreKeymap(mf, sub, io_size, hash_data_size);
#else
                    s = StoreKeymap(mf, sub, io_size);
#endif
                    if (!s.ok()) {
                        printf("[%d :: %s] fail to flush sub key %d(size : 0x%x)\n", __LINE__, __func__, sub->GetSKTableID(), io_size);
                        abort();
                    }
                    /* create new skt */
                    SKTableMem* sub_skt = new SKTableMem(mf, sub);
#ifdef INSDB_GLOBAL_STATS
                    g_new_skt_cnt++;
#endif
                    if (!sub_skt) abort();
                    sub_skt->SetSKTableID(sub->GetSKTableID());
#ifdef USE_HASHMAP_RANDOMREAD
                    sub_skt->SetKeymapNodeSize(io_size, hash_data_size);
#else
                    sub_skt->SetKeymapNodeSize(io_size);
#endif
                    sub_skt_.push_back(std::make_pair(sub_skt, sub->GetKeymapBeginKey()));  
                }
                /* write split main sktable */
process_main:
                uint32_t prev_colinfo_id = split_main->GetPrevColInfoID();
                uint32_t cur_col_info_id = split_main->GetCurColInfoID();
                split_main->SetPrevColInfoID();
#ifdef USE_HASHMAP_RANDOMREAD
                s = StoreKeymap(mf, split_main, io_size, hash_data_size);
                if (!s.ok()) {
                    printf("[%d :: %s] fail to flush split main key %d(size : 0x%x)\n", __LINE__, __func__, split_main->GetSKTableID(), io_size);
                    abort();
                }
                SetKeymapNodeSize(io_size, hash_data_size);
#else
                s = StoreKeymap(mf, split_main, io_size);
                if (!s.ok()) {
                    printf("[%d :: %s] fail to flush split main key %d(size : 0x%x)\n", __LINE__, __func__, split_main->GetSKTableID(), io_size);
                    abort();
                }
                SetKeymapNodeSize(io_size);

#endif

                /* delete Column Info table */
                for (uint32_t i = prev_colinfo_id; i < cur_col_info_id; i++) {
                    InSDBKey colinfo_key = GetColInfoKey(mf->GetDBHash(), skt_id_, i);
                    mf->GetEnv()->Delete(colinfo_key);
                }

                // acquire sub skt keymap lock
                for(int i = sub_skt_.size() - 1 ; i >= 0 ; --i)
                    sub_skt_[i].first->KeyMapGuardLock();

                /*
                 * Insert to skiplist.
                 */
                for(int i = sub_skt_.size()-1; i >= 0; i-- ){
                    SKTableMem* sub_skt = sub_skt_[i].first;
                    if (!sub_skt) abort();
                    mf->InsertNextToSKTableSortedList(sub_skt->GetSKTableSortedListNode(), GetSKTableSortedListNode());
                    if(!mf->InsertSKTableMem(sub_skt)){
                        printf("[%d :: %s]fail to sub skt %u insert\n", __LINE__, __func__, sub_skt->GetSKTableID());
                        abort();
                    }
                }

                /*
                 * Update split main 
                 */
                KeyMapRandomReadGuardLock();
                Keymap *del_keymap = keymap_;
                keymap_ = split_main;
                if(del_keymap->DecRefCntCheckForDelete()) delete del_keymap;
                KeyMapRandomReadGuardUnLock();
                SplitInactiveKeyQueue(mf, sub_skt_);

                // release sub skt keymap lock
                for(int i = 0; i < sub_skt_.size() ; i++) {
                    SKTableMem* sub_skt = sub_skt_[i].first;
#ifdef MEM_TRACE
                    mf->IncSKTDF_MEM_Usage(sub_skt->GetBufferSize());
#endif                

                    //mf->IncreaseSKTDFNodeSize(sub_skt->GetAllocatedSize());
                    mf->IncreaseSKTDFNodeSize(sub_skt->GetBufferSize());
                    sub_skt->KeyMapGuardUnLock();
                }
#ifdef KV_TIME_MEASURE 
                t2 = get_cycles();
                split_cycle += t2-t1;
                split_cnt++;
#endif
            }else {
                only_col_table_update = true;
                SKTableMem *del_skt = mf->NextSKTableInSortedList(GetSKTableSortedListNode()); 
                if(!GetSKTRefCnt() && del_skt && del_skt->IsDeletedSKT()){
                    bool first_deleted_skt = false;
                    while(true){
                        /**
                         * More than 2 emtpy SKTables can exist,
                         * In this case, the current SKTable should merge all contiguous empty SKTables 
                         * If not, empty SKTables may not be merged during BuildManifest.  
                         */
                        if(del_skt && del_skt->IsDeletedSKT() && !del_skt->IsCached()){
                            /** Merge SKTable */
                            if(!del_skt->IsKeyQueueEmpty() || !del_skt->FlushCandidateDequeEmpty()) {
                                abort();
#if 0
                                for( int qidx = 0; qidx < 2; qidx++) {
                                    UserKey *del_skt_uk;
                                    while (del_skt_uk = del_skt->PopUserKeyFromKeyQueue(qidx)) {
                                        PushUserKeyToActiveKeyQueue(del_skt_uk);
#ifdef CODE_TRACE
                                        printf("[%d :: %s] Before delete SKTableMem(id : %d), merged uk %p\n", __LINE__, __func__, del_skt->GetSKTableID(), del_skt_uk);
#endif
                                    }
                                }
#endif
                            }

                            if (!del_skt->IsKeymapLoaded()) {
                                del_skt->LoadSKTableDeviceFormat(mf);
                                assert(del_skt->IsKeymapLoaded());
                            } 

                            mf->RemoveFromSKTableSortedList(del_skt->GetSKTableSortedListNode());
                            if(!first_deleted_skt){
                                SetPreAllocSKTableID(del_skt->GetSKTableID());
                                //EncodeFixed32(const_cast<char*>(dfb_info->new_skt_df_cache.data())+SKTDF_PREALLOC_ID_OFFSET, del_skt->GetSKTableID()); 
                                first_deleted_skt = true;
                            }
                            SetNextSKTableID(del_skt->GetNextSKTableID());
                            //EncodeFixed32(const_cast<char*>(dfb_info->new_skt_df_cache.data())+SKTDF_NEXT_ID_OFFSET, del_skt->GetNextSKTableID()); 
                            del_skt_list.push_front(del_skt->GetSKTableID());
                            (void)del_skt->DeleteSKTDeviceFormatCache(mf, true);
                            /**
                             * the del_skt was removed from SKTable skiplist on last flush time
                             */

                            //printf("[%d :: %s] Before delete SKTableMem(id : %d)\n", __LINE__, __func__, del_skt->GetSKTableID());
                            delete del_skt;
                        } else { 
                            break;/*No Merge, No Split*/
                        } 

                        uint32_t prev_colinfo_id = GetPrevColInfoID();
                        uint32_t cur_col_info_id = GetCurColInfoID();
                        SetPrevColInfoID();
#ifdef USE_HASHMAP_RANDOMREAD
                        s = StoreKeymap(mf, keymap_, io_size, hash_data_size);
                        if (!s.ok()) {
                            printf("[%d :: %s] fail to flush split main key %d(size : 0x%x)\n", __LINE__, __func__, split_main->GetSKTableID(), io_size);
                            abort();
                        }
                        SetKeymapNodeSize(io_size, hash_data_size);
#else
                        s = StoreKeymap(mf, keymap_, io_size);
                        if (!s.ok()) {
                            printf("[%d :: %s] fail to flush split main key %d(size : 0x%x)\n", __LINE__, __func__, split_main->GetSKTableID(), io_size);
                            abort();
                        }
                        SetKeymapNodeSize(io_size);
#endif

                        /* delete Column Info table */
                        for (uint32_t i = prev_colinfo_id; i < cur_col_info_id; i++) {
                            InSDBKey colinfo_key = GetColInfoKey(mf->GetDBHash(), skt_id_, i);
                            mf->GetEnv()->Delete(colinfo_key);
                        }

                        /////10. Delete Invalid SKTables //////////////////////////////////////////////////////////////////////////
                        for(std::deque<uint32_t>::iterator it = del_skt_list.begin() ; it != del_skt_list.end() ; it++){
                            InSDBKey key = GetMetaKey(mf->GetDBHash(), kSKTable, *it);
                            s = mf->GetEnv()->Delete(key, true);
                        }
                        del_skt = mf->NextSKTableInSortedList(GetSKTableSortedListNode()); 
                    }
                }else if(mf->SKTMemoryUsage() < kMaxCacheSize){
#if 1
                    if(InsertUserKeyToKeyMapFromInactiveKeyQueue(mf, false, 0)) //key_queue_ => flush_candidate_key_deque_  // true of false ?
                        /* Don't do this after split in order to prevent insert sub's key to main's keymap  */
                        UpdateKeyInfo(mf, col_info_table, largest_ikey, in_log_KBM_queue); // flush_candidate_key_deque_ => flushed_key_hash_
#else
#ifdef KV_TIME_MEASURE 
                    cycles_t t1, t2;
                    t1 = get_cycles();
#endif
                    UpdateKeyInfoUsingUserKeyFromKeyQueue(mf, 0, true, col_info_table, largest_ikey, in_log_KBM_queue);
#ifdef KV_TIME_MEASURE 
                    t2 = get_cycles();
                    insertuk_cycle += t2-t1;
                    insertuk_cnt++;
#endif
#endif
                }
            }
        }
split_exit:
        /*
         * Flush SKTable & last KB with largest iKey
         * Write col_info_table
         * if(sub-skt or cleanup){
         *   write keymap
         *   delete col_info_tables
         * }
         */
        std::list<SKTableMem*> dirty_list;
        std::list<SKTableMem*> clean_list;
        /* flush sktable */
        //Env::Default()->Flush(kWriteFlush, -1, GetMetaKey(mf->GetDBHash(), kSKTable, GetSKTableID()));
        if(largest_ikey)
            Env::Default()->Flush(kWriteFlush, -1, GetKBKey(mf->GetDBHash(), kKBlock, largest_ikey));
        
        if( only_col_table_update && col_info_table.size() > 8){ /* only update when it is not splited ... */
            InSDBKey last_cit_key = StoreColInfoTable(mf, col_info_table, comp_col_info_table);//Write column info table to device
            mf->SetLastColInfoTableKey(last_cit_key);
        }

        for(int i = 0 ; i < sub_skt_.size();i++){
            SKTableMem* skt = sub_skt_[i].first;
            if(skt->IsSKTDirty(mf)){
                skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
                dirty_list.push_back(skt);
            }else{
                clean_list.push_back(skt);
            }
            //skt->KeyMapGuardUnLock();

        }
        if(!clean_list.empty()){
            mf->SpliceCacheList(clean_list, kCleanSKTCache);
            mf->SignalSKTEvictionThread();
        }
        if(!dirty_list.empty()){
            mf->SpliceCacheList(dirty_list, kDirtySKTCache);
        }

        ///////////////// 5. Change in-log KBM Status to out-log //////////////////
      

        //int32_t new_size = GetAllocatedSize();
        int32_t new_size = GetBufferSize();

#ifdef MEM_TRACE
        if(new_size > old_size)
            mf->IncSKTDF_MEM_Usage(new_size - old_size);
        else if(old_size - new_size)
            mf->DecSKTDF_MEM_Usage(old_size - new_size);
#endif
        mf->IncreaseSKTDFNodeSize(new_size - old_size);
#if 0

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
#ifdef KV_TIME_MEASURE 
        e1 = get_cycles();
#endif

#if 0
        dfb_info->target_skt = new SKTableMem(mf, &dfb_info->begin_key);
        dfb_info->target_skt->SetInCleanCache();
#endif
        /*
         * snapinfo_list has been clear()
         * dfb_info->snapinfo_list.clear();
         */

#ifdef KV_TIME_MEASURE 
        e2 = get_cycles();
#endif
#ifdef KV_TIME_MEASURE 
        e3 = get_cycles();
#endif

#ifdef KV_TIME_MEASURE 
        e5 = get_cycles();
#endif
#ifdef KV_TIME_MEASURE 
        e6 = get_cycles();
#endif


        ///////////////////// 4. Device Flush with largest iKey Sequence Number /////////////////////
        //std::queue<KeyBlockMetaLog*> in_log_KBM_queue;
        //s = Env::Default()->Flush(kWriteFlush, -1, GetKBKey(mf->GetDBHash(), kKBlock, dfb_info->flush_ikey));
        //key_vector.push_back(GetMetaKey(mf->GetDBHash(), kSKTable, skt->GetSKTableID()));
        //size_vector.push_back(data.size());
        //data_vector.push_back(const_cast<char*>(data.data()));
        //Env::Default()->MultiPut(key_vector, data_vector, size_vector, null_insdbkey, true);
        //key_vector.resize(0);
        //data_vector.resize(0);
        //size_vector.resize(0);
#ifdef KV_TIME_MEASURE 
        e7 = get_cycles();
#endif

#ifdef KV_TIME_MEASURE 
        e8 = get_cycles();
#endif


#ifdef KV_TIME_MEASURE 
        e9 = get_cycles();
#endif

#ifdef KV_TIME_MEASURE 
        e10 = get_cycles();
#endif

#if 0
        /* This code need to move to eviction code*/
        if(IsDeletedSKT()){
            SKTableMem *prev = this;
            while(1){
                prev = mf->PrevSKTableInSortedList(prev->GetSKTableSortedListNode());
                if(!prev->IsDeletedSKT() ){
                    /*Fetched status will be checked in PrefetchSKTable()*/
                    if(!prev->IsKeymapLoaded())
                        prev->PrefetchSKTable(mf);
                    assert(prev->IsCached());
                    break;
                }
#ifdef CODE_TRACE
                else if(prev->GetSKTableID() == 1) abort();
#endif
            }
        }
#endif
#ifdef KV_TIME_MEASURE 
        end= get_cycles();
#endif
#ifdef KV_TIME_MEASURE 
        uint64_t key_cnt_tmp = keymap_->GetCount();
        if(!SetCounted()){
            if(key_cnt_tmp > 10000){
                l_skt.fetch_add(1, std::memory_order_relaxed);
            }else{
                s_skt.fetch_add(1, std::memory_order_relaxed);
            }

        }
        uint64_t exe_time = time_nsec_measure(start, end);
        uint64_t key_cnt_tmp_avg;
        uint64_t exe_time_avg;
        if(key_cnt_tmp > 10000){
            if(1||exe_time <14014713){

                dfb_info->flush_cnt_l++;
                dfb_info->prev_large_key_cnt += key_cnt_tmp;
                dfb_info->prev_large_key_lat += exe_time;
                key_cnt_tmp_avg = dfb_info->prev_large_key_cnt/dfb_info->flush_cnt_l;
                exe_time_avg = dfb_info->prev_large_key_lat/dfb_info->flush_cnt_l;

                dfb_info->existing_key_cnt_l+=existing_cnt;
                dfb_info->update_key_cnt_l+=new_cnt;

                if(!(dfb_info->flush_cnt_s%100)){
                    printf("[%d :: %s] [***LARGE***][uk in hash : %7u][ID : %5u (key count : %5lu(avg : %6lu))][total time : %6lu(avg : %8lu)][Per key avr : %6.4f]\n", __LINE__, __func__, mf->GetHashSize(), GetSKTableID(), key_cnt_tmp, key_cnt_tmp_avg, exe_time, exe_time_avg, (double)exe_time_avg/(double)key_cnt_tmp_avg);
                    printf("[%d :: %s] [***LARGE***]existing_cnt : %lu || new_cnt : %lu(%f%)\n", __LINE__, __func__, dfb_info->existing_key_cnt_l/dfb_info->flush_cnt_l, dfb_info->update_key_cnt_l/dfb_info->flush_cnt_l, (double)dfb_info->update_key_cnt_l/(double)dfb_info->existing_key_cnt_l*100.0);
                    uint64_t s = s_skt.load(std::memory_order_relaxed);
                    uint64_t l = l_skt.load(std::memory_order_relaxed);
                    printf("[%d :: %s] Small SKT count : Large SKT count = %lu : %lu(%f%)\n", __LINE__, __func__, s, l, (double)l/(double)s*100.0);
                }
            }else
                key_cnt_tmp = 0;
        }else if(key_cnt_tmp > 0){
            if( 1|| exe_time <730000) {
                dfb_info->flush_cnt_s++;
                dfb_info->prev_small_key_cnt+=key_cnt_tmp;
                dfb_info->prev_small_key_lat+=exe_time;
                key_cnt_tmp_avg = dfb_info->prev_small_key_cnt/dfb_info->flush_cnt_s;
                exe_time_avg = dfb_info->prev_small_key_lat/dfb_info->flush_cnt_s;
                dfb_info->existing_key_cnt_s+=existing_cnt;
                dfb_info->update_key_cnt_s+=new_cnt;
                if(!(dfb_info->flush_cnt_s%1000)){
                    printf("[%d :: %s] [---SMALL---][uk in hash : %7u][ID : %5u (key count : %5lu(avg : %6lu))][total time : %6lu(avg : %8lu)][Per key avr : %6.4f]\n", __LINE__, __func__, mf->GetHashSize(), GetSKTableID(), key_cnt_tmp, key_cnt_tmp_avg, exe_time, exe_time_avg, (double)exe_time_avg/(double)key_cnt_tmp_avg);
                    printf("[%d :: %s] [---SMALL---]existing_cnt : %lu || new_cnt : %lu(%f%)\n", __LINE__, __func__, dfb_info->existing_key_cnt_s/dfb_info->flush_cnt_s, dfb_info->update_key_cnt_s/dfb_info->flush_cnt_s, ((double)dfb_info->update_key_cnt_s)/((double)dfb_info->existing_key_cnt_s)*((double)(100)));
                    uint64_t s = s_skt.load(std::memory_order_relaxed);
                    uint64_t l = l_skt.load(std::memory_order_relaxed);
                    printf("[%d :: %s] Small SKT count : Large SKT count = %lu : %lu(%f%)\n", __LINE__, __func__, s, l, (double)l/(double)s*100.0);
                }
            }else
                key_cnt_tmp = 0;
        }
        //printf("[%d :: %s] [ID : %d (key count : %ld)][uk in hash : %ld][total time : %ld]1: %lld || 2: %lld || 3: %lld || 4: %lld || 5: %lld || 6: %lld || 7: %lld || 8: %lld || 9: %lld || 10:%lld\n", __LINE__, __func__, GetSKTableID(), keymap_->GetCount(), mf->GetHashSize(), time_usec_measure(start, end), time_usec_measure(s1,e1), time_usec_measure(e1,e2), time_usec_measure(e2,e3), time_usec_measure(e3,e4), time_usec_measure(e4,e5), time_usec_measure(e5,e6), time_usec_measure(e6,e7), time_usec_measure(e7,e8), time_usec_measure(e8,e9), time_usec_measure(e9,e10));
#endif

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
#endif
        return s;
    }

    SKTableMem* SKTableMem::Next(Manifest* mf) {
        return mf->Next(this);
    }

    SKTableMem* SKTableMem::Prev(Manifest* mf) {
        return mf->Prev(this);
    }

#if 0
    char* SKTableMem::EncodeFirstKey(const KeySlice &key, char *buffer, uint32_t &buffer_size){
        char* buf = EncodeVarint16(buffer, 0/*shared*/);
        buffer_size+= (buf-buffer);
        buffer = EncodeVarint16(buf, key.size());
        buffer_size+= (buffer-buf);
        memcpy(buffer, key.data(), key.size());
        buffer+=key.size();
        buffer_size+=key.size();
        return buffer;
    }

    char* SKTableMem::EncodeKey(const std::string& begin_key_of_target, const KeySlice &key, char *buffer, uint32_t &buffer_size){
        const size_t min_length = std::min(begin_key_of_target.size(), key.size());
        uint16_t shared = 0;
        while ((shared < min_length) && (begin_key_of_target[shared] == key[shared])) {
            shared++;
        }
        uint16_t non_shared = key.size() - shared;
        char* buf = EncodeVarint16(buffer, shared);
        buffer_size+= (buf-buffer);
        buffer = EncodeVarint16(buf, non_shared);
        buffer_size+= (buffer-buf);
        memcpy(buffer, key.data()+shared, non_shared);
        buffer+=non_shared;
        buffer_size += non_shared;
        return buffer;
    }

    /* Encode the zero size Key */
    char* SKTableMem::EncodeKey(char *buffer, uint32_t &buffer_size){
        uint16_t shared = 0;
        uint16_t non_shared =0;
        char* buf = EncodeVarint16(buffer, shared);
        buffer_size+= (buf-buffer);
        buffer = EncodeVarint16(buf, non_shared);
        buffer_size+= (buffer-buf);
        return buffer;
    }
#endif

    bool SKTableMem::SplitByPrefix(const Slice& a, const Slice& b){
        if (!kPrefixIdentifierSize || a.size() < kPrefixIdentifierSize ||  b.size() < kPrefixIdentifierSize || a.size() != b.size()) {
            return false;
        }
        if(kPrefixIdentifierSize && (memcmp(a.data(), b.data(), kPrefixIdentifierSize) != 0));
        // return (kPrefixIdentifierSize && (memcmp(a.data(), b.data(), kPrefixIdentifierSize) != 0));
        return true;
    }
} //name space insdb
