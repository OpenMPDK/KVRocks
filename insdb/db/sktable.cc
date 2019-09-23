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
#include "db/insdb_internal.h"
#include "db/skiplist.h"
#include "db/skiplist2.h"
#include "db/keyname.h"
#include "db/dbformat.h"
#include "db/keyname.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/crc32c.h"

namespace insdb {
    SKTDFCacheNode::SKTDFCacheNode(Manifest* mf, SKTableMem *skt, Slice device_format/* comp/decomp DF */, bool from_mem) : 
        skt_(skt), device_format_(0), decoding_base_key_(0), key_offset_(NULL), next_skt_id_(0), preallocated_skt_id_(0), key_(){
            char* raw_data = const_cast<char*>(device_format.data());

            uint32_t skt_info_2_offset = *((uint32_t*)(raw_data + SKTDF_INFO_2_OFFSET));
            uint32_t comp_data_start = *((uint32_t *)(raw_data + SKTDF_COMP_START_OFFSET));
            uint32_t skt_size = *((uint32_t*)(raw_data + SKTDF_SKT_SIZE_OFFSET));
            /* crc size = 44 byte meta + sizeof(2ed key ~ skt info 2) + sizeof(begin key) */
            /*uint32_t crc_size = (SKTDF_BEGIN_KEY_OFFSET - SKTDF_CRC_2_OFFSET) + skt_size + (comp_data_start - SKTDF_BEGIN_KEY_OFFSET);*/
            if (!from_mem) {
                uint32_t crc_size = 0;
                assert(comp_data_start);
                crc_size = (comp_data_start - SKTDF_CRC_2_OFFSET + skt_size);

                /* Check CRC */
                if(crc32c::Unmask(DecodeFixed32(raw_data)) != crc32c::Value(raw_data + SKTDF_CRC_2_OFFSET, crc_size )){
                    printf("[%d :: %s]sktable(%p), comp_data_start : %u || crc :%u(real : %u) || device_format size : %lu || comp_size : %d\n", __LINE__, __func__,  skt_, comp_data_start, crc32c::Unmask(DecodeFixed32(raw_data)), crc32c::Value(raw_data + SKTDF_CRC_2_OFFSET, crc_size ), device_format.size(), skt_size);
                    abort();
                }
                next_skt_id_ = *((uint32_t*)(raw_data + SKTDF_NEXT_ID_OFFSET));
                preallocated_skt_id_ = *((uint32_t*)(raw_data + SKTDF_PREALLOC_ID_OFFSET));
            }

            if(skt_info_2_offset & flag_compression){

                size_t ulength = 0;
                if (!port::Snappy_GetUncompressedLength(raw_data+comp_data_start, skt_size, &ulength)) {
                    fprintf(stderr, "corrupted compressed data\n");
                    abort();
                } else {
                    /* Copy SKT Info 1 */
                    device_format_ = mf->AllocSKTableBuffer(ulength + comp_data_start);
                    char* uncomp_data = const_cast<char*>(device_format_.data());
                    memcpy(uncomp_data, raw_data, comp_data_start);
                    if (!port::Snappy_Uncompress(raw_data+comp_data_start, skt_size, uncomp_data+comp_data_start)) {
                        fprintf(stderr, "corrupted compressed data\n");
                        abort();
                    }
                    mf->FreeSKTableBuffer(device_format);//free compressed device format
                    raw_data = uncomp_data;
                    skt_info_2_offset &= ~flag_compression;
                    *((uint32_t*)(uncomp_data + SKTDF_INFO_2_OFFSET)) = skt_info_2_offset;
                    *((uint32_t*)(uncomp_data + SKTDF_SKT_SIZE_OFFSET)) = ulength;
                }
                /* Store decompressed size 
                 * But, it can cause (re)allocation overhead.
                 * So, for now, we are going to use kMaxSKTableSize for every uncompressed SKTable DF.
                 * Note that, compressed SKTable DF is only temporary used. So, it is okay to use kMaxSKTableSize for compressed one.
                 */
            }else
                device_format_ = device_format;
            /* 
             * For first SKTable, we need to use decode_key 
             * decode key in first SKTable = second key
             * other SKTable = begin key
             */
            uint16_t decoding_base_key_size = DecodeFixed16(raw_data+skt_info_2_offset);
            decoding_base_key_ = Slice(raw_data+skt_info_2_offset+2/*sizeof(uint16_t)*/, decoding_base_key_size);
            key_offset_ = (uint32_t *)(decoding_base_key_.data()+decoding_base_key_size);
#ifdef CODE_TRACE
            printf("[ %d :: %s ] key_offset : 0x%lx \n", __LINE__, __func__, key_offset_[0]);
#endif
            key_.reserve(kMaxKeySize);
        }

    void SKTDFCacheNode::CleanUp(Manifest* mf) {
        skt_ = NULL;                                               // 8 Byte
        if (device_format_.size())  
            mf->FreeSKTableBuffer(device_format_);
        device_format_ = Slice(0);
        decoding_base_key_ = Slice(0);                                       // 16Byte
        key_offset_ = NULL;                                     // 8 Byte
        next_skt_id_ = 0;                                          // 4 Byte
        preallocated_skt_id_ = 0;                                  // 4 Byte
    }
    void SKTDFCacheNode::UpdateDeviceFormat(Manifest* mf, Slice device_format/*uncomp DF*/){
        mf->FreeSKTableBuffer(device_format_);
        char* raw_data = const_cast<char*>(device_format.data());
        /* Do not Check CRC because the data is just created by host(not from device) */
        uint32_t skt_info_2_offset = *((uint32_t*)(raw_data + SKTDF_INFO_2_OFFSET));

        device_format_ = device_format;
        skt_->IncSKTDFVersion();

        uint16_t decoding_base_key_size = DecodeFixed16(device_format_.data()+skt_info_2_offset);
        decoding_base_key_ = Slice(device_format_.data()+skt_info_2_offset+2/*sizeof(uint16_t)*/, decoding_base_key_size);
        key_offset_ = (uint32_t *)(decoding_base_key_.data()+decoding_base_key_size);
#ifdef CODE_TRACE
        printf("[%d :: %s]Update DF 1st hash_and_offset  : 0x%lx\n", __LINE__, __func__, *key_offset_);
#endif
    }

    KeyBlockPrimaryMeta* SKTDFCacheNode::GetKBPMAndiKeyWithOffset(Manifest *mf, uint8_t& kb_offset, SequenceNumber& ikey, KeyBlockPrimaryMeta* kbpm, uint32_t& ivalue_size, uint32_t offset, uint16_t remove_size/*, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time*/){

        assert(GetKeyCount());
        Slice col_info_slice = GetColumnInfoWithOffset(offset);
        col_info_slice.remove_prefix(remove_size);
        uint64_t ikey_seq_num = 0, seq = 0, ttl = 0;
#if 0
        /* Debugging code */
        if(col_id != (col_info_slice[0] & COLUMN_NODE_ID_MASK)){
            abort();
        }
#endif
        // column ID
        col_info_slice.remove_prefix(1);
        // flags and key block offset
        kb_offset = col_info_slice[0] & COLUMN_NODE_OFFSET_MASK;
        col_info_slice.remove_prefix(1);
        // ikey seq
        GetVarint64(&col_info_slice, &ikey_seq_num);

        assert(ikey_seq_num >= kUnusedNextInternalKey) ;
        if(ikey_seq_num == ikey){
            /* KBPM is same to previous key' KBPM */
            /* Only kb offset is different */
            assert(kbpm);
            return kbpm;
        }
        ikey = ikey_seq_num;

        GetVarint32(&col_info_slice, &ivalue_size);

        return mf->GetKBPM(ikey_seq_num, ivalue_size, false);

#if 0
        /* Debugging code */
        if(kb_offset & COLUMN_NODE_HAS_SEQ_NUM_BIT){
            GetVarint64(&col_info_slice, &seq);
            if(!HasBeenFlushed()) seq = 0;
        }
        if(kb_offset & COLUMN_NODE_HAS_TTL_BIT) {
            GetVarint64(&col_info_slice, &ttl);
        }

        if(!(seq_num >= seq && (!ttl || (ttl + mf->GetTtlFromTtlList(col_id)) > cur_time))){
            /* It should be true because it was true during creating IterScratchPad */
            abort();
        }
#endif

    }
    Slice  __attribute__((optimize("O0"))) SKTDFCacheNode::GetKeyColumnInfobyHash(Manifest *mf, KeySlice given_key){

        //////////////////////// Binary Search /////////////////////////
        uint32_t offset = 0;
        uint32_t left = 0;
        uint32_t right = GetKeyCount();
        uint32_t mid = 0;
        uint32_t sktdf_hash = 0;
        uint16_t key_info_size = 0 ;
        Slice key_info;
        int32_t comp = 0;
        std::string key;
        key.reserve(kMaxKeySize);
        while (left < right) {
            //key_.resize(0);

            mid = (left + right) / 2;
            offset = key_offset_[mid];
            key_info_size = DecodeFixed16((device_format_.data() + (offset+2/*skip prev key size*/)));
            key_info = Slice((device_format_.data() + (offset+4)), (key_info_size-4/*Prev & Current key size info*/));


            /*
             * If key is found from hash,  we don't know the next offset.
             * But the key-column is encoded. so that, the caller can get the column info.
             */

                //assert(offset);
            /* Shared Key Size */
            if(!GetVarint16(&key_info, &key_info_size)) port::Crash(__FILE__,__LINE__);
            assert(key_info_size <= kMaxKeySize);
            key.assign(decoding_base_key_.data(), key_info_size);

            /* Non-Shared Key Size */
            if(!GetVarint16(&key_info, &key_info_size)) port::Crash(__FILE__,__LINE__);
            key.append(key_info.data(), key_info_size);
            comp = mf->GetComparator()->Compare(Slice(key), given_key);

            /*if( sktdf_hash < hash )*/ 
            if( comp < 0 )
                {
                // sktdf_hash at "sktdf_hash[mid]" is < "hash".  
                // Therefore all hash values at or before "mid" are uninteresting.
                left = mid + 1;
            }else if (comp > 0) {
                // sktdf_hash at "sktdf_hash[mid]" is < "hash".  
                // Therefore all hash values after "mid" are uninteresting.
                right = mid;
            }else/*(comp == 0)*/{
                /*
                 * This column has been deleted while Flush SKTableMem.
                 * this SKTDF will be updated soon.
                 */
                key_info.remove_prefix(key_info_size);
                if(*key_info.data() == 0) return Slice(0);
                return key_info;
            }
        }
        /*There is no same key exists.*/
        return Slice(0);
    }

    bool IterScratchPad::SetIterSKTDF(Manifest* mf, SKTableMem* iter_skt){
        if(!iter_skt->IsFlushing()){
            sktdf_ = iter_skt->GetSKTableDeviceFormatNode();
            return true;/* Use SKTableMem's sktdf */
        }
        Slice org_device_format = iter_skt->GetSKTableDeviceFormatNode()->GetDeviceFormat();
        Slice iter_device_format = mf->AllocSKTableBuffer(org_device_format.size());
        memcpy(const_cast<char*>(iter_device_format.data()), const_cast<char *>(org_device_format.data()), org_device_format.size());
        sktdf_ = new SKTDFCacheNode(mf, NULL, iter_device_format, true);
        return false;/* Use copy of SKTableMem's sktdf */
    }

    SKTableMem::SKTableMem(Manifest* mf, KeySlice& key, uint32_t skt_id, uint32_t sktdf_size) : 
        keyqueue_lock_(),
        active_key_queue_(0),
        /*Two Queue : Active & Inactive key queue*/
        key_queue_(NULL),
        keymap_mu_(),
#if 1
        keymap_(mf->GetComparator()),
#else
         gcinfo_(16), keymap_(mf->GetComparator(), &gcinfo_, mf),
#endif
        sktdf_load_mu_(),
        sktdf_update_mu_(),
        build_iter_mu_(),
        iter_mu_(),
        latest_iter_pad_(NULL),
        begin_key_buffer_{NULL,NULL},
        begin_key_buffer_size_{0,0},
        begin_key_size_{0,0},
        begin_key_hash_{0,0},
        active_begin_key_buffer_(0),
        internal_node_(0),
        sktdf_fetch_spinlock_(),
        flags_(0),
        skt_sorted_node_(),
        cache_pos_(mf->GetSKTCleanCache()->end()),
        skt_df_node_(nullptr),
        skt_df_node_size_(sktdf_size),
        skt_df_version_(0),
        skt_id_(skt_id),
        approximate_dirty_col_cnt_(0),
        ref_cnt_(0),
        prev_dirty_col_cnt_(0),
        coldness_(mf->GetColdnessSeed())
    {
        key_queue_ = new std::queue<UserKey*>[2];
        //if(key.size()){
            begin_key_buffer_[0/*active_begin_key_buffer_*/] = (char*)malloc(key.size());
            begin_key_buffer_size_[0] = key.size();
            begin_key_size_[0] = begin_key_buffer_size_[0];
            memcpy(begin_key_buffer_[0], key.data(), begin_key_buffer_size_[0]);
            begin_key_hash_[0] = key.GetHashValue();
        //}

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
        latest_iter_pad_ = (IterScratchPad **)calloc(sizeof(IterScratchPad*), kMaxColumnCount);
    }


    /**
     * This constructor is for sub-SKTable creation.
     */
    SKTableMem::SKTableMem(Manifest *mf, KeySlice *begin_key) : 
        keyqueue_lock_(),
        active_key_queue_(0),
        /*Two Queue : Active & Inactive key queue*/
        key_queue_(NULL),
        keymap_mu_(),
#if 1
        keymap_(mf->GetComparator()),
#else
         gcinfo_(16), keymap_(mf->GetComparator(), &gcinfo_, mf),
#endif
        sktdf_load_mu_(),
        sktdf_update_mu_(),
        build_iter_mu_(),
        iter_mu_(),
        latest_iter_pad_(NULL),
        begin_key_buffer_{NULL,NULL},
        begin_key_buffer_size_{0,0},
        begin_key_size_{0,0},
        begin_key_hash_{0,0},
        active_begin_key_buffer_(0),
        internal_node_(NULL),
        sktdf_fetch_spinlock_(),
        flags_(0),
        skt_sorted_node_(),
        cache_pos_(mf->GetSKTCleanCache()->end()),
        skt_df_node_(nullptr),
        skt_df_node_size_(0),
        skt_df_version_(0),
        skt_id_(0),
        approximate_dirty_col_cnt_(0),
        ref_cnt_(0),
        prev_dirty_col_cnt_(0),
        coldness_(mf->GetColdnessSeed())

        {
            key_queue_ = new std::queue<UserKey*>[2];

            /* A zero-sized Key belongs to the 1st SKTable, and the SKTable is never deleted */
            if(!begin_key->size()){
                abort();
            }
            begin_key_buffer_[0] = (char*)malloc(begin_key->size());
            begin_key_buffer_size_[0] = begin_key->size();
            begin_key_size_[0] = begin_key_buffer_size_[0];
            memcpy(begin_key_buffer_[0], begin_key->data(), begin_key_buffer_size_[0]);
            begin_key_hash_[0] = begin_key->GetHashValue();
            latest_iter_pad_ = (IterScratchPad **)calloc(sizeof(IterScratchPad*), kMaxColumnCount);
            //cache_pos_ = mf->GetSKTCleanCache()->end();
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
        if(begin_key_buffer_[0])
            free(begin_key_buffer_[0]);
        if(begin_key_buffer_[1])
            free(begin_key_buffer_[1]);
        delete [] key_queue_;
        assert(!skt_df_node_); // must be cleared before desturtor, see DeleteSKTDeviceFormatCache().
        free(latest_iter_pad_);
    }

    bool SKTableMem::InsertUserKeyToKeyMapFromInactiveKeyQueue(Manifest *mf) {
        bool flush_queue = false;
        UserKey* uk = NULL;
        if(!IsActiveKeyQueueEmpty()){
            ReturnIterScratchPad(mf);
            flush_queue = true;
            std::lock_guard<folly::SharedMutex> l(keymap_mu_);
            uint8_t idx = ExchangeActiveAndInActiveKeyQueue();
            while((uk = PopUserKeyFromKeyQueue(idx))){
                if(!keymap_.Insert(uk->GetKeySlice(), uk))
                    abort(); /* Insertion must be success because it held the keymap lock */
            }
        }
        return flush_queue;
    }

    bool SKTableMem::EvictCleanUserKeyFromKeymap(Manifest *mf, bool eviction_only, bool &evicted, bool cache_evict) {
        UserKey* uk = NULL;
        bool referenced = false;

        {
            std::lock_guard<folly::SharedMutex> l(keymap_mu_);
            SLSplitCtx m_ctx;
            void *t_node = NULL;
            uk = keymap_.First(&m_ctx);
            while(uk){
                // Mark prepare in the current key
                uk->SetEvictionPrepare();
                if(uk->IsDirty() || uk->GetReferenceCount()){
                    referenced = true;
                    uk->ClearEvictionPrepare();
                    // If the caller is not the evictor, try next time because user keys are busy.
                    if (!cache_evict) break;
                }else{
                    evicted = true;
                    mf->RemoveUserKeyFromHashMapWrapper(uk);
                    if (keymap_.Remove(&m_ctx) == nullptr) abort();
                    /* clear internal node */
                    t_node = m_ctx.src_cur_del_node;
                    m_ctx.src_cur_del_node = nullptr;
                    assert(t_node);
                    free(t_node);
                    uk->SetNode(nullptr);
                    uk->UserKeyCleanup(mf, cache_evict);
                    mf->FreeUserKey(uk);
                }
                uk = keymap_.Next(nullptr, &m_ctx);
            }
        }
        if(!eviction_only){
            IterLock();
            InsertUserKeyToKeyMapFromInactiveKeyQueue(mf);
            IterUnlock();
        }
        return referenced;
    }

    bool SKTableMem::PrefetchSKTable(Manifest* mf, bool dirtycache) {

        bool prefetch = false;
        /*It can be sub-SKTable having no sktdf yet*/
        if(!IsFlushOrEvicting() && !SKTDFLoadTryLock()){
            if(!GetSKTableDeviceFormatNode()){ 
                if (!SetFetchInProgress()){
                    /* If it has not been prefetched or fetched*/
                    assert(!IsInSKTableCache(mf));
#ifdef CODE_TRACE    
                    printf("[%d :: %s] Insert SKT(%p) to dirty cache\n", __LINE__, __func__, this);
#endif
                    if (dirtycache)
                        mf->InsertSKTableMemCache(this, kDirtySKTCache);
                    else
                        mf->InsertSKTableMemCache(this, kCleanSKTCache);

                    // Issue prefetch
                    int value_size = skt_df_node_size_;
                    InSDBKey skt_key = GetMetaKey(mf->GetDBHash(), kSKTable, skt_id_);
                    Status s = Env::Default()->Get(skt_key, NULL, &value_size, kReadahead);
                    if (!s.ok()){
                        s.NotFound("SKTable meta does not exist");
                        abort();
                    }
                    prefetch = true;
                }
            }
            SKTDFLoadUnlock();
        }
        return prefetch;
    }

    void SKTableMem::DiscardSKTablePrefetch(Manifest* mf) {
        /*It can be sub-SKTable having no sktdf yet*/
        if(!IsFlushOrEvicting() && !SKTDFLoadTryLock()){
            if(!GetSKTableDeviceFormatNode()){ 
                if (ClearFetchInProgress()){
                    /* If it has not been prefetched or fetched*/
                    int value_size = skt_df_node_size_;
                    InSDBKey skt_key = GetMetaKey(mf->GetDBHash(), kSKTable, skt_id_);
                    (Env::Default())->DiscardPrefetch(skt_key);
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
     * 2. type == kNonblokcingRead
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
            while(IsFlushing() && !skt_df_node_);
        }
#endif
        SKTDFLoadLock();
        if(!skt_df_node_){
            Slice skt_buffer = mf->AllocSKTableBuffer(skt_df_node_size_);  // It will be freed after submitting SKTable.
            if(skt_buffer.size() < skt_df_node_size_) abort();
#ifdef CODE_TRACE
            printf("[%d :: %s]Loading size : %d\n", __LINE__, __func__, skt_df_node_size_);
#endif
            int size = mf->ReadSKTableMeta(const_cast<char*>(skt_buffer.data()), kSKTable, GetSKTableID(), type, skt_df_node_size_);
            if(size){
#ifdef CODE_TRACE
                printf("[%d :: %s](SKTID : %d)Loaded size : %d(loading cnt : %d/%ld)\n", __LINE__, __func__, skt_id_, size, ++skt_load_cnt, mf->GetSKTableCountInSortedList());
#endif
                // size should be allocated size, not value size.
                SKTDFCacheNode* sktdf = new SKTDFCacheNode(mf, this, skt_buffer);
                skt_df_node_ = sktdf;
                incache = ClearFetchInProgress();
                SKTDFLoadUnlock();
                mf->IncreaseMemoryUsage(sktdf->GetSize());
                if(skt_df_node_size_ != size){
                    printf("[%d :: %s] The SKTDF size is different to recorded size in SKTableMem(actual size : %d || given size : %d)\n", __LINE__, __func__, size, skt_df_node_size_);
                    abort();
                }
                /* In PrefetchSKTable(), this SKTableMem is already inserted to dirty list */
            }else{ 
                SKTDFLoadUnlock();
                mf->FreeSKTableBuffer(skt_buffer);  // It will be freed after submitting SKTable.
                /* Fail to read SKTable */
                if(type != kNonblokcingRead)
                    abort();
                /* kNonblokcingRead is only called after prefetch which conducted cache insertion */
                return false;
            }
        }else{
            SKTDFLoadUnlock();
        }
        if(type == kNonblokcingRead && !incache)
            abort();
        return incache;
    }

    UserKey* SKTableMem::InsertDeviceFormatUserKey(Manifest* mf, KeySlice& target_key, Slice col_info_slice, bool inc_ref_cnt){
        UserKey *ukey = NULL;
retry:
        ukey = GetKeyMap()->Search(target_key);
        if(ukey){
            ukey = mf->IncreaseUserKeyReference(ukey);
            if(ukey) ukey = ukey->IsSKTableEvictionInProgress();
            if(!ukey)
                goto retry;
        }
        if (ukey){
            if(ukey->IsFetched()) {
                if(!inc_ref_cnt)
                    ukey->DecreaseReferenceCount();
                return ukey;
            }
        }else{
            ukey = mf->AllocUserKey(target_key.size());
            ukey->InitUserKey(target_key);
            if (!mf->InsertUserKey(ukey)) {
                //assert(false);
                ukey->UserKeyCleanup(mf);
                mf->FreeUserKey(ukey);
                ukey = GetKeyMap()->Search(target_key);
                if(ukey){
                    ukey = mf->IncreaseUserKeyReference(ukey);
                    if(ukey) ukey = ukey->IsSKTableEvictionInProgress();
                    if(!ukey)
                        goto retry;
                }
                assert(ukey);
            }
        }
#ifndef NDEBUG
        if (target_key.size() && col_info_slice.size() == 1) assert(0);
#endif
        ukey->InsertColumnNodes(col_info_slice, HasBeenFlushed()/* If this skt has not been flushed after DB open, discard seq_num*/);
        ukey->SetFetched();
        if(!inc_ref_cnt)
            ukey->DecreaseReferenceCount();
        return ukey;
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////


    void SKTableMem::LoadSKTable(Manifest* mf) {
        if(!LoadSKTableDeviceFormat(mf)){
            assert(!IsInSKTableCache(mf) && !IsFlushOrEvicting());
            mf->InsertSKTableMemCache(this, kCleanSKTCache);
        }
        uint32_t sktdf_next_key_offset = 0, sktdf_cur_key_offset = SKTDF_BEGIN_KEY_OFFSET;
        std::shared_lock<folly::SharedMutex> l(sktdf_update_mu_);

#ifdef KV_TIME_MEASURE
        cycles_t st, e;
        uint64_t delta;
        st = get_cycles();
#endif

        SKTDFFetchSpinLock();
        SKTDFCacheNode* skt_df_cache = skt_df_node_;

#ifdef KV_TIME_MEASURE
        e = get_cycles();
        delta = time_cycle_measure(st,e);
        printf("skt %u flag checking %llu\n", this->GetSKTableID(), cycle_to_usec(delta));
        st = get_cycles();
#endif

        Slice col_info_slice;
        std::string key_from_skt_df;
        if(!skt_df_cache->GetKeyCount())
            sktdf_cur_key_offset = 0;
        else
            col_info_slice = skt_df_cache->GetKeyWithOffset(sktdf_cur_key_offset, &key_from_skt_df, sktdf_next_key_offset);
        while(sktdf_cur_key_offset){
            if(col_info_slice[0]){
                KeySlice tkey(key_from_skt_df);
                InsertDeviceFormatUserKey(mf, tkey, col_info_slice, false);
            }
            /////////////////////////// Move to Next Key /////////////////////////// 
            if(sktdf_next_key_offset){
                sktdf_cur_key_offset = sktdf_next_key_offset;
                col_info_slice = skt_df_cache->GetKeyWithOffset(sktdf_cur_key_offset, &key_from_skt_df, sktdf_next_key_offset);
            }else
                sktdf_cur_key_offset = 0;
        }
#ifdef KV_TIME_MEASURE
        e = get_cycles();
        delta = time_cycle_measure(st,e);
        printf("skt %u Insert DeviceFormat UserKey %llu\n", this->GetSKTableID(), cycle_to_usec(delta));
#endif

        SKTDFFetchSpinUnlock();


#ifdef TRACE_MEM
        if (!((g_miss_df_skt + g_hit_df_skt) % 100000)) {
            mf->PrintParam();
            printf("IO [%ld]\n\n", g_io_cnt.load());
        }
#endif

    }

    bool SKTableMem::GetLatestSequenceNumber(Manifest* mf, const KeySlice& key, uint8_t col_id, SequenceNumber* seq_num, uint64_t cur_time) {
#ifdef CODE_TRACE
                printf("[%d :: %s]\n", __LINE__, __func__);
#endif

        IncSKTRefCnt();

        if(mf->Next(this) && 
                (mf->GetComparator()->Compare(mf->Next(this)->GetBeginKeySlice(), key) <= 0)){
            /*The key belongs to next(or beyond) SKTable. */
            return false;
        }

        if(!skt_df_node_){
            if(!LoadSKTableDeviceFormat(mf)){
                assert(!IsInSKTableCache(mf) && !IsFlushOrEvicting());
                mf->InsertSKTableMemCache(this, kCleanSKTCache);
            }
        }
        std::shared_lock<folly::SharedMutex> l(sktdf_update_mu_);

        assert(skt_df_node_);
        Slice col_info_slice = skt_df_node_.load()->GetKeyColumnInfobyHash(mf, key);

        if(col_info_slice.size()){
            uint8_t nr_column = col_info_slice[0];
            col_info_slice.remove_prefix(1);
            uint8_t offset;
            uint32_t ivalue_size = 0;
            uint64_t ikey_seq_num = 0;
            uint64_t seq = 0;
            uint64_t ttl = 0;

            for( int i = 0 ; i < nr_column ; i++){ 
                if(col_id == (col_info_slice[0] & COLUMN_NODE_ID_MASK)){
                    col_info_slice.remove_prefix(1);
                    offset = col_info_slice[0];
                    col_info_slice.remove_prefix(1);
                    GetVarint64(&col_info_slice, &ikey_seq_num);
                    GetVarint32(&col_info_slice, &ivalue_size);
                    if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT){
                        GetVarint64(&col_info_slice, &seq);
                        if(!HasBeenFlushed()) seq= 0;
                    }else seq = 0;
                    if(offset & COLUMN_NODE_HAS_TTL_BIT) GetVarint64(&col_info_slice, &ttl);
                    else ttl = 0;
                    if(*seq_num >= seq && (!ttl || mf->GetTtlFromTtlList(col_id) == 0 ||(ttl + mf->GetTtlFromTtlList(col_id)) > cur_time)){
                        *seq_num = seq;
                    }
                    break;
                }else{
                    col_info_slice.remove_prefix(1);
                    offset = col_info_slice[0];
                    col_info_slice.remove_prefix(1);
                    GetVarint64(&col_info_slice, &ikey_seq_num);
                    GetVarint32(&col_info_slice, &ivalue_size);
                    if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT)
                        GetVarint64(&col_info_slice, &seq);
                    if(offset & COLUMN_NODE_HAS_TTL_BIT) 
                        GetVarint64(&col_info_slice, &ttl);
                }
            }
        }
        return true;
    }

    bool SKTableMem::GetColumnInfo(Manifest* mf, const KeySlice& key, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, ColumnScratchPad& column) {

        IncSKTRefCnt();

        SKTableMem *next = mf->Next(this);
        if(next &&
                (mf->GetComparator()->Compare(next->GetBeginKeySlice(), key) <= 0)){
            DecSKTRefCnt();
            /*The key belongs to next(or beyond) SKTable. */
            return false;
        }

        if(!skt_df_node_){
            if(!LoadSKTableDeviceFormat(mf)){
                assert(!IsInSKTableCache(mf) && !IsFlushOrEvicting());
                mf->InsertSKTableMemCache(this, kCleanSKTCache);
            }
        }
        std::shared_lock<folly::SharedMutex> l(sktdf_update_mu_);

        assert(skt_df_node_);
        Slice col_info_slice = skt_df_node_.load()->GetKeyColumnInfobyHash(mf, key);
#if 1
        for(int i=0;i<7;i++){
            mf->GetComparator()->Compare(GetBeginKeySlice(), key); 
        }
#endif

        if(col_info_slice.size()){
            uint8_t nr_column = col_info_slice[0];
            col_info_slice.remove_prefix(1);
            uint8_t offset;
            uint32_t ivalue_size = 0;
            uint64_t ikey_seq_num = 0;
            uint64_t seq = 0;
            uint64_t ttl = 0;

            for( int i = 0 ; i < nr_column ; i++){ 
                if(col_id == (col_info_slice[0] & COLUMN_NODE_ID_MASK)){
                    char *ref_bit = const_cast<char*>(col_info_slice.data());
                    col_info_slice.remove_prefix(1);
                    offset = col_info_slice[0];
                    col_info_slice.remove_prefix(1);
                    GetVarint64(&col_info_slice, &ikey_seq_num);
                    GetVarint32(&col_info_slice, &ivalue_size);
                    if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT){
                        GetVarint64(&col_info_slice, &seq);
                        if(!HasBeenFlushed()) seq= 0;
                    }else seq = 0;
                    if(offset & COLUMN_NODE_HAS_TTL_BIT) GetVarint64(&col_info_slice, &ttl);
                    else ttl = 0;
                    if(seq_num >= seq && (!ttl || mf->GetTtlFromTtlList(col_id) == 0 || (ttl + mf->GetTtlFromTtlList(col_id) ) > cur_time)){
                        column.ikey_seq_num = ikey_seq_num;
                        column.ivalue_size  = ivalue_size; 
                        column.offset = offset & COLUMN_NODE_OFFSET_MASK;
                        (*ref_bit) |= COLUMN_NODE_REF_BIT;
                    }
                    break;
                }else{
                    col_info_slice.remove_prefix(1);
                    offset = col_info_slice[0];
                    col_info_slice.remove_prefix(1);
                    GetVarint64(&col_info_slice, &ikey_seq_num);
                    GetVarint32(&col_info_slice, &ivalue_size);
                    if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT)
                        GetVarint64(&col_info_slice, &seq);
                    if(offset & COLUMN_NODE_HAS_TTL_BIT) 
                        GetVarint64(&col_info_slice, &ttl);
                }
            }
        }
        DecSKTRefCnt();
        return true;
    }

    void SKTableMem::FillIterScaratchPadWithColumnInfo(Manifest *mf, IterScratchPad* iter_pad, char*& key_buffer_loc, Slice& col_info_slice, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, SequenceNumber &biggest_begin_seq, bool prefetch) {
        uint8_t nr_column = col_info_slice[0];
        uint16_t remove_size = 0;
        const char* begin_data = col_info_slice.data();
        col_info_slice.remove_prefix(1);
        uint8_t offset;
        uint32_t ivalue_size = 0;
        uint64_t ikey_seq_num = 0, seq = 0, ttl = 0;
        KeyBlockPrimaryMeta* kbpm = NULL;

        for( int i = 0 ; i < nr_column ; i++){ 
            if(col_id == (col_info_slice[0] & COLUMN_NODE_ID_MASK)){

                /* remove_size is useful when iterator try to get KBPM
                 * It can save column searching time.
                 */
                remove_size = (uint16_t)(col_info_slice.data() - begin_data);
                if(remove_size > 0xFFF){
                    printf("[%d :: %s]remove_size is too big(%d) It must be less than 0xF000\n", __LINE__, __func__, remove_size);
                    printf("[%d :: %s]But, we can increase offset size to uin64_t (currently uint32_t)\n", __LINE__, __func__);
                    abort();
                }

                char *ref_bit = const_cast<char*>(col_info_slice.data());
                col_info_slice.remove_prefix(1);
                offset = col_info_slice[0];
                col_info_slice.remove_prefix(1);
                GetVarint64(&col_info_slice, &ikey_seq_num);
                GetVarint32(&col_info_slice, &ivalue_size);
                if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT){
                    GetVarint64(&col_info_slice, &seq);
                    if(!HasBeenFlushed()) seq= 0;
                }
                if(offset & COLUMN_NODE_HAS_TTL_BIT) {
                    GetVarint64(&col_info_slice, &ttl);
                }
                offset &= COLUMN_NODE_OFFSET_MASK;
                if(seq_num >= seq && (!ttl || (ttl + mf->GetTtlFromTtlList(col_id)) > cur_time)){
                    if (seq > biggest_begin_seq )
                        biggest_begin_seq = seq;
                    /* check and extend key buffer */
                    uint32_t key_buffer_offset = (uint32_t)(key_buffer_loc - iter_pad->key_buffer_);
                    if (iter_pad->key_buffer_size_ <= (key_buffer_offset + kKeyEntrySize /* 8B */)) {
                        iter_pad->key_buffer_size_ = mf->IncMaxIterKeyBufferSize();
                        iter_pad->key_buffer_ = (char*)realloc(iter_pad->key_buffer_, iter_pad->key_buffer_size_);
                        key_buffer_loc = iter_pad->key_buffer_ + key_buffer_offset;
                    }

                    ////////////////////// Prefetch KB //////////////////////
                    kbpm = mf->GetKBPM(ikey_seq_num, ivalue_size);
                    auto cur_key_entry = (KeyBufferEntry*)key_buffer_loc;
                    cur_key_entry->uv_kb = (uint64_t)((((uint64_t)offset) << kKeySizeOffsetShift) | (uint64_t)kbpm);
                    cur_key_entry->key = 0;
#ifdef CODE_TRACE
                    printf("sktdf ...kbpm entry 0x%lx\n", *(uint64_t*)key_buffer_loc);
#endif
                    key_buffer_loc += kUserValueAddrOrKBPMSize;
                    iter_pad->key_entry_count_++; 
                    if(prefetch){
                        KeyBlock *kb = NULL;
                        kbpm->SetRefBitmap(offset & COLUMN_NODE_OFFSET_MASK);
                        if (!(kb = kbpm->GetKeyBlockAddr()) && !kbpm->IsKBPrefetched()) {
                            kbpm->Lock();
                            if (!(kb = kbpm->GetKeyBlockAddr()) && !kbpm->IsKBPrefetched()) {
                                InSDBKey kbkey = GetKBKey(mf->GetDBHash(), kKBlock, ikey_seq_num);
                                int buffer_size = SizeAlignment(ivalue_size);
                                Env::Default()->Get(kbkey, NULL, &buffer_size, kReadahead);
                                kbpm->SetKBPrefetched();
                            }
                            kbpm->Unlock();
                        }
                    }
                    iter_pad->kbpm_queue_.push(kbpm);
                    /////////////////////////////////////////////////////////

                    (*ref_bit) |= COLUMN_NODE_REF_BIT;
                    return;
                }
                return;
            }else{
                col_info_slice.remove_prefix(1);
                offset = col_info_slice[0];
                col_info_slice.remove_prefix(1);
                GetVarint64(&col_info_slice, &ikey_seq_num);
                GetVarint32(&col_info_slice, &ivalue_size);
                if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT)
                    GetVarint64(&col_info_slice, &seq);
                if(offset & COLUMN_NODE_HAS_TTL_BIT) 
                    GetVarint64(&col_info_slice, &ttl);
            }
        }
        return;

    }




    /* If Column exists, prefetch the KB */
    /* Return target column offset in order to reduce search time in Iterator when the iterator accesses KBPM */
    uint16_t SKTableMem::LookupColumnInfo(Manifest *mf, Slice& col_info_slice, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, SequenceNumber &biggest_begin_seq, bool prefetch){
        uint8_t nr_column = col_info_slice[0];
        uint16_t remove_size = 0;
        const char* begin_data = col_info_slice.data();
        col_info_slice.remove_prefix(1);
        uint8_t offset;
        uint32_t ivalue_size = 0;
        uint64_t ikey_seq_num = 0, seq = 0, ttl = 0;
        KeyBlockPrimaryMeta* kbpm = NULL;

        for( int i = 0 ; i < nr_column ; i++){ 
            if(col_id == (col_info_slice[0] & COLUMN_NODE_ID_MASK)){

                /* remove_size is useful when iterator try to get KBPM
                 * It can save column searching time.
                 */
                remove_size = (uint16_t)(col_info_slice.data() - begin_data);
                if(remove_size > 0xFFF){
                    printf("[%d :: %s]remove_size is too big(%d) It must be less than 0xF000\n", __LINE__, __func__, remove_size);
                    printf("[%d :: %s]But, we can increase offset size to uin64_t (currently uint32_t)\n", __LINE__, __func__);
                    abort();
                }

                char *ref_bit = const_cast<char*>(col_info_slice.data());
                col_info_slice.remove_prefix(1);
                offset = col_info_slice[0];
                col_info_slice.remove_prefix(1);
                GetVarint64(&col_info_slice, &ikey_seq_num);
                GetVarint32(&col_info_slice, &ivalue_size);
                if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT){
                    GetVarint64(&col_info_slice, &seq);
                    if(!HasBeenFlushed()) seq= 0;
                }
                if(offset & COLUMN_NODE_HAS_TTL_BIT) {
                    GetVarint64(&col_info_slice, &ttl);
                }

                if(seq_num >= seq && (!ttl || mf->GetTtlFromTtlList(col_id) == 0 || (ttl + mf->GetTtlFromTtlList(col_id)) > cur_time)){
                    if (seq > biggest_begin_seq )
                        biggest_begin_seq = seq;
                    ////////////////////// Prefetch KB //////////////////////
                    KeyBlock *kb = NULL;
                    if(prefetch){
                        kbpm = mf->GetKBPM(ikey_seq_num, ivalue_size);
                        kbpm->SetRefBitmap(offset & COLUMN_NODE_OFFSET_MASK);
                        kbpm->DecRefCnt();
                        if (!(kb = kbpm->GetKeyBlockAddr()) && !kbpm->IsKBPrefetched()) {
                            kbpm->Lock();
                            if (!(kb = kbpm->GetKeyBlockAddr()) && !kbpm->IsKBPrefetched()) {
                                InSDBKey kbkey = GetKBKey(mf->GetDBHash(), kKBlock, ikey_seq_num);
                                int buffer_size = SizeAlignment(ivalue_size);
                                Env::Default()->Get(kbkey, NULL, &buffer_size, kReadahead);
                                kbpm->SetKBPrefetched();
                            }
                            kbpm->Unlock();
                        }
                    }
                    /////////////////////////////////////////////////////////

                    (*ref_bit) |= COLUMN_NODE_REF_BIT;
                    return remove_size;
                }
                return 0;
            }else{
                col_info_slice.remove_prefix(1);
                offset = col_info_slice[0];
                col_info_slice.remove_prefix(1);
                GetVarint64(&col_info_slice, &ikey_seq_num);
                GetVarint32(&col_info_slice, &ivalue_size);
                if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT)
                    GetVarint64(&col_info_slice, &seq);
                if(offset & COLUMN_NODE_HAS_TTL_BIT) 
                    GetVarint64(&col_info_slice, &ttl);
            }
        }
        return 0;
    }

    /* If this SKTable is in flush, do not call PrefetchKeyBlocks*/
    bool SKTableMem::PrefetchKeyBlocks(Manifest *mf, uint8_t col_id, uint64_t cur_time, SequenceNumber seq_num, PrefetchDirection dir){
        bool incache = true;

        if(dir == kNext){
            SKTableMem* skt = mf->Next(this);
            /* TODO Discard SKT Prefetch*/
            /* Prefetch next SKTs */
            if(skt && !skt->GetSKTableDeviceFormatNode() && !skt->IsFetchInProgress()) 
                skt->PrefetchSKTable(mf, false/*To clean cache*/);
        }else{
            assert(dir == kPrev);
            /* Prefetch prev SKTs */
            SKTableMem* skt = mf->Prev(this);
            if(skt && !skt->GetSKTableDeviceFormatNode() && !skt->IsFetchInProgress()) 
                skt->PrefetchSKTable(mf, false/*To clean cache*/);
        }

        if(!skt_df_node_){
            incache = LoadSKTableDeviceFormat(mf, kNonblokcingRead);
            if(skt_df_node_ && !incache){
                assert(!IsInSKTableCache(mf) && !IsFlushOrEvicting());
                mf->InsertSKTableMemCache(this, kCleanSKTCache);
            }
        }
        SKTDFCacheNode* skt_df_cache = GetSKTableDeviceFormatNode();
        if(skt_df_cache && !SetPrefetchKeyBlock()){
#ifdef CODE_TRACE
            printf("prefetch keyblocks for skt %d\n", this->GetSKTableID());
#endif
            std::shared_lock<folly::SharedMutex> dfl(sktdf_update_mu_);
            KeyBlock *kb;
            SequenceNumber biggest_begin_key = 0;
            //uint32_t sktdf_prev_key_offset = 0;
            uint32_t sktdf_next_key_offset = SKTDF_BEGIN_KEY_OFFSET;
            Slice col_info_slice;
            /* Prefetch all KBs with col_id in the SKTableDF */
            while(sktdf_next_key_offset){
                assert(skt_df_cache->GetKeyCount());
                col_info_slice = skt_df_cache->GetColumnInfoWithOffset(sktdf_next_key_offset,/* sktdf_prev_key_offset,*/ sktdf_next_key_offset);
                assert(col_info_slice.size());
                LookupColumnInfo(mf, col_info_slice, col_id, seq_num, cur_time, biggest_begin_key, true);
            }
        }

        return incache;
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    void SKTableMem::SetIterCurrentOffset(Manifest* mf, KeyBufferEntry*& key_buffer_loc, /*KeyBlockPrimaryMeta* kbpm,*/ IterCurrentKeyInfo* cur_info, const KeySlice* key, std::string& key_from_skt_df, bool search_less_than){
        if(!cur_info->offset){
            if(!key){
#if 0
                /* Set cur_info even if this is not the current key 
                 * It will be used for search_less_than case(prev key)
                 */
                cur_info->key.resize(0);
                cur_info->key.append(key_from_skt_df);
                cur_info->kbpm = kbpm;
                cur_info->ikey= kbpm->GetiKeySequenceNumber();
#endif
                if(!search_less_than){
                    cur_info->offset = key_buffer_loc;
                }
            }else{
                int cur_comp = mf->GetComparator()->Compare(KeySlice(key_from_skt_df), *key); 
                if(cur_comp > 0 && search_less_than){/* search_less_than => set to previous offset */
                    /*It does not need to set current key because the current_key has already been set(prev key) */
                    cur_info->offset = (key_buffer_loc - 1);
                }else{ 
#if 0
                    /* Set cur_info even if this is not the current key 
                     * It will be used for search_less_than case(prev key)
                     */
                    cur_info->key.resize(0);
                    cur_info->key.append(key_from_skt_df);
                    cur_info->kbpm = kbpm;
                    cur_info->ikey= kbpm->GetiKeySequenceNumber();
#endif
                    if(cur_comp == 0 || (cur_comp > 0 )){
                        cur_info->offset = key_buffer_loc;
                    }
                }
            }
        }

    }

    UserKey* SKTableMem::MoveToNextKeyInKeyMap(Manifest* mf, UserKey* ukey)
    {
        UserKey* next_ukey = NULL;
        ukey->DecreaseReferenceCount();

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
        return next_ukey;
    }

    bool SKTableMem::BuildIteratorScratchPad(Manifest *mf, IterScratchPad* iter_pad, const KeySlice* key, uint8_t col_id, uint64_t cur_time, SequenceNumber seq_num){
        SequenceNumber biggest_begin_seq = 0;

        IncSKTRefCnt();

        SKTableMem* skt = mf->Next(this);
        if(key && skt && (mf->GetComparator()->Compare(skt->GetKeySlice(), *key) <= 0)){
            /*The key belongs to next(or beyond) SKTable. */
            DecSKTRefCnt();
            return false;/* IterScratchPad is not created */
        }
        iter_pad->skt_ = this;
        /* TODO Discard SKT Prefetch*/
        if(!skt_df_node_){
            if(!LoadSKTableDeviceFormat(mf)){
                assert(!IsInSKTableCache(mf) && !IsFlushOrEvicting());
                mf->InsertSKTableMemCache(this, kCleanSKTCache);
            }
        }
        int comp;
        uint32_t /*sktdf_prev_key_offset = 0,*/ sktdf_next_key_offset = 0, sktdf_cur_key_offset = SKTDF_BEGIN_KEY_OFFSET;
        Slice sktdf_col_info_slice;
        std::string key_from_skt_df;
        /*Keep prev key string for search_less_than*/
        UserKey *ukey = NULL;
        ColumnNode *col_node = NULL;
        bool unlock_needed = true;

        SKTDFUpdateSharedLock();
        iter_pad->next_skt_ = mf->Next(this);
        if(!iter_pad->SetIterSKTDF(mf, this)){
            unlock_needed = false;
            iter_pad->use_org_sktdf_ = false;
            SKTDFUpdateSharedUnLock();
        }else
            iter_pad->use_org_sktdf_ = true;
        iter_pad->sktdf_version_ = GetSKTDFVersion();
        SKTDFCacheNode* skt_df_cache = iter_pad->sktdf_;
        char* key_buffer_loc = iter_pad->key_buffer_;
        uint32_t key_buffer_size = 0;
        bool kb_prefetch = !SetPrefetchKeyBlock();//If it was not prefetched, do prefetch

#ifdef CODE_TRACE
        printf("build iter prefetch keyblocks for skt %d\n", this->GetSKTableID());
#endif
        /*Set begin key*/
        {
            while(true) {
                keymap_mu_.lock_shared();
                ukey = keymap_.First();
                if (ukey) {
                    ukey = mf->IncreaseUserKeyReference(ukey);
                    if(ukey) ukey = ukey->IsSKTableEvictionInProgress();
                    if(ukey) break;
                } else
                    break;
                keymap_mu_.unlock_shared();
            }
            keymap_mu_.unlock_shared();
        }
        if(!skt_df_cache->GetKeyCount())
            sktdf_cur_key_offset = 0;
        else
            sktdf_col_info_slice = skt_df_cache->GetKeyWithOffset(sktdf_cur_key_offset, &key_from_skt_df, /*sktdf_prev_key_offset,*/ sktdf_next_key_offset);
        while(ukey || sktdf_cur_key_offset){            
            /*Compare SKTDF's key & UserKey*/
            if(sktdf_cur_key_offset){
                if(ukey) comp = ukey->Compare(KeySlice(key_from_skt_df), mf);
                else comp = 1; //remained keys are not fetched. 
            }else comp = -1; 

            if( comp > 0 ){
                assert(sktdf_col_info_slice.size());
                FillIterScaratchPadWithColumnInfo(mf, iter_pad, key_buffer_loc, sktdf_col_info_slice, col_id, seq_num, cur_time, biggest_begin_seq, kb_prefetch);
#ifdef CODE_TRACE
                printf("[%d :: %s]From First DF : %s\n", __LINE__, __func__, EscapeString(key_from_skt_df.data()).c_str());
#endif
 
                /////////////////////////// Move to Next Key /////////////////////////// 
                if(sktdf_next_key_offset){
                    sktdf_cur_key_offset = sktdf_next_key_offset;
                    sktdf_col_info_slice = skt_df_cache->GetKeyWithOffset(sktdf_cur_key_offset, &key_from_skt_df, /*sktdf_prev_key_offset,*/ sktdf_next_key_offset);
                }else
                    sktdf_cur_key_offset = 0;
                //////////////////////////////////////////////////////////////////////// 
            }else if(comp == 0){
                /*Check both, UserKey & SKTDF */
                bool deleted = false;
                if((col_node = ukey->GetColumnNode(col_id, deleted, cur_time, seq_num))){ 
                    if(col_node->GetBeginSequenceNumber() > biggest_begin_seq)
                        biggest_begin_seq = col_node->GetBeginSequenceNumber();
                    ukey->FillIterScratchPad(mf, iter_pad, key_buffer_loc, key, col_node, col_id);
                    
#ifdef CODE_TRACE
                    printf("[%d :: %s]From First UK : %s\n", __LINE__, __func__, EscapeString(ukey->GetKeySlice().data()).c_str());
#endif
                }else if(!deleted && !ukey->IsFetched()){
                    assert(sktdf_col_info_slice.size());
                    FillIterScaratchPadWithColumnInfo(mf, iter_pad, key_buffer_loc, sktdf_col_info_slice, col_id, seq_num, cur_time, biggest_begin_seq, kb_prefetch);
#ifdef CODE_TRACE
                    printf("[%d :: %s]From second DF : %s\n", __LINE__, __func__, EscapeString(key_from_skt_df.data()).c_str());
#endif
                }
                /////////////////////////// Move to Next Key /////////////////////////// 
                ukey = MoveToNextKeyInKeyMap(mf, ukey);
                if(sktdf_next_key_offset){
                    sktdf_cur_key_offset = sktdf_next_key_offset;
                    sktdf_col_info_slice = skt_df_cache->GetKeyWithOffset(sktdf_cur_key_offset, &key_from_skt_df, /*sktdf_prev_key_offset,*/ sktdf_next_key_offset);
                }else
                    sktdf_cur_key_offset = 0;
                //////////////////////////////////////////////////////////////////////// 

            }else{ /*if ( comp < 1 )*/
                /*Need to check UserKey */
                bool deleted = false;
                if((col_node = ukey->GetColumnNode(col_id, deleted, cur_time, seq_num))){ 
                    if(col_node->GetBeginSequenceNumber() > biggest_begin_seq)
                        biggest_begin_seq = col_node->GetBeginSequenceNumber();
                    ukey->FillIterScratchPad(mf, iter_pad, key_buffer_loc, key, col_node, col_id);
#ifdef CODE_TRACE
                    printf("[%d :: %s]From Second UK : %s\n", __LINE__, __func__, EscapeString(ukey->GetKeySlice().data()).c_str());
#endif
                }

                /////////////////////////// Move to Next Key /////////////////////////// 
                ukey = MoveToNextKeyInKeyMap(mf, ukey);
                //////////////////////////////////////////////////////////////////////// 
            }
        }

        uint32_t key_buffer_offset = (uint32_t)(key_buffer_loc - iter_pad->key_buffer_);
        if (iter_pad->key_buffer_size_ <= (key_buffer_offset + kKeyEntrySize)) {
            iter_pad->key_buffer_size_ = mf->IncMaxIterKeyBufferSize();
            iter_pad->key_buffer_ = (char*)realloc(iter_pad->key_buffer_, iter_pad->key_buffer_size_);
            key_buffer_loc = iter_pad->key_buffer_ + key_buffer_offset;
        }

        /* last entry mark */
        auto cur_key_entry = (KeyBufferEntry *)key_buffer_loc;
        cur_key_entry->uv_kb = cur_key_entry->key = 0;

        iter_pad->skt_ = this;
        iter_pad->begin_seq_ = biggest_begin_seq;
        iter_pad->last_seq_ = seq_num;

        if(unlock_needed){
            SKTDFUpdateSharedUnLock();
            IterLock();
            if(iter_pad->key_entry_count_)
                SetLatestIterPad(mf, iter_pad, col_id);
            IterUnlock();
        }
        DecSKTRefCnt();
        return true;/* IterScratchPad has been created */
    }




/////////////////////////////////////////////////////////////////////////////////////////

    bool SKTableMem::SKTDFReclaim(Manifest *mf, int64_t &reclaim_size, bool &evicted, bool force_evict){
        bool referenced = false;
        SKTDFCacheNode* skt_df_cache = GetSKTableDeviceFormatNode();
        if(!skt_df_cache) abort();
        /* write lock for ref bit modification*/
        std::lock_guard<folly::SharedMutex> l(sktdf_update_mu_);
        char* device_format = skt_df_cache->GetDeviceFormatData();
        uint16_t key_size;
        char* key_info_start;
        Slice col_info;
        uint32_t offset;

        uint8_t kb_offset;
        uint32_t ivalue_size = 0;
        uint64_t ikey_seq_num = 0, seq = 0, ttl = 0;
        uint16_t next_ofs_diff = 0;

        uint32_t comp_data_start = DecodeFixed32(device_format+SKTDF_COMP_START_OFFSET);
        uint32_t skt_data_size = DecodeFixed32(device_format + SKTDF_SKT_SIZE_OFFSET);
        uint32_t max_size = skt_data_size + comp_data_start - offset;

        //uint32_t sktdf_prev_key_offset = 0;
        uint32_t sktdf_next_key_offset = SKTDF_BEGIN_KEY_OFFSET;
        bool pf_is_cleared = false;

        /* Prefetch all KBs with col_id in the SKTableDF */
        if(!skt_df_cache->GetKeyCount())
            sktdf_next_key_offset= 0;
        while(sktdf_next_key_offset){
            if(GetSKTRefCnt()){
                referenced = true;
                break;
            }
            col_info = skt_df_cache->GetColumnInfoWithOffset(sktdf_next_key_offset, /*sktdf_prev_key_offset,*/ sktdf_next_key_offset);
            assert(col_info.size());

            uint8_t nr_column = col_info[0];
            col_info.remove_prefix(1);

            for( int j = 0 ; j < nr_column ; j++){ 
                /* 
                 * | ref.bit | Col ID | flags | KB offset | ikey seq   | ikey size  | (seq num)  | (TTL)      |
                 * |  2bit   | 6bit   | 2bit  | 6bit      | varint(8B) | varint(4B) | varint(8B) | varint(8B) |  
                 * ---------------------------------------|            |            |            |            |
                 * |       1Byte      |     1Byte         |            |            |            |            |
                 */
                /* col id is not needed */

                bool clear_kbpm = false;
#if 0
                bool check_kbpm = false;
                if(col_info[0] & COLUMN_NODE_REF_BIT){
                    char *ref_bit = const_cast<char*>(col_info.data());
                    if(col_info[0] & 0x80){
                        *ref_bit |= 0x40;/*Give Second Chance*/
                        *ref_bit &= 0x7F;/*Give Second Chance*/

                    }else{
                        *ref_bit &= 0x3F;/*Clear KBPM->ClearRefBitmap()*/
                        clear_kbpm = true;
                    }
                    referenced = true;
                }
                else
                    check_kbpm = true;
#else
                if(col_info[0] & COLUMN_NODE_REF_BIT){
                    char *ref_bit = const_cast<char*>(col_info.data());
                    if (force_evict) {
                        *ref_bit &= 0x3F;/*Clear KBPM->ClearRefBitmap()*/
                        clear_kbpm = true;
                    } else { 
                        if(col_info[0] & 0x40){
                            *ref_bit &= 0xBF;/*Give Second Chance*/
                        }else{
                            *ref_bit &= 0x3F;/*Clear KBPM->ClearRefBitmap()*/
                            clear_kbpm = true;
                        }
                    }
                    referenced = true;
                }
#endif
                col_info.remove_prefix(1);
                kb_offset = col_info[0];
                col_info.remove_prefix(1);
                GetVarint64(&col_info, &ikey_seq_num);

                GetVarint32(&col_info, &ivalue_size);
#ifdef CODE_TRACE
                printf("[%d :: %s](ID : %d)nr_col : %d || ikey : %ld || size : %d || offset : %d\n", __LINE__, __func__, skt_id_, nr_column, ikey_seq_num, ivalue_size, kb_offset & COLUMN_NODE_OFFSET_MASK);
#endif
                if(kb_offset & COLUMN_NODE_HAS_SEQ_NUM_BIT)
                    GetVarint64(&col_info, &seq);
                if(kb_offset & COLUMN_NODE_HAS_TTL_BIT) 
                    GetVarint64(&col_info, &ttl);
                if(clear_kbpm){
                    evicted = true;
                    KeyBlockPrimaryMeta* kbpm = mf->FindKBPMFromHashMap(ikey_seq_num);
                    if(kbpm){
                        if(kbpm->ClearRefBitAndTryEviction(mf, kb_offset&COLUMN_NODE_OFFSET_MASK, reclaim_size)){
                            if(!pf_is_cleared){
#ifdef CODE_TRACE
                                printf("sktdf reclaim skt %d\n", this->GetSKTableID());
#endif
                                ClearPrefetchKeyBlock();
                                pf_is_cleared = true;
                            }
                            mf->FreeKBPM(kbpm);
                        }
                    }
                }
#ifdef CODE_TRACE
                else if(check_kbpm){
                    KeyBlockPrimaryMeta* kbpm = mf->FindKBPMFromHashMap(ikey_seq_num);
                    if(kbpm && kbpm->IsRefBitmapSet(kb_offset&COLUMN_NODE_OFFSET_MASK)){
                        printf("[%d :: %s]Ref bitmap has been set!!\n", __LINE__, __func__);
                    }
                }
#endif
            }
        }
        return referenced;
    }

    bool SKTableMem::CompareKey(UserKeyComparator comp, const KeySlice& key, int& delta){
        delta = comp(GetBeginKeySlice(), key);
        return true;
    }

    KeySlice SKTableMem::GetKeySlice(){ return GetBeginKeySlice();}

#define SEQ_NUM_SHIFT 7
#define TTL_SHIFT 6

    void SKTableMem::BuildUpdateList(std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>>& del, std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>>& update, std::string* buffer, uint32_t& size){
        PutFixed32(buffer, 0); /* total size */
        PutFixed32(buffer, (uint32_t)del.size());
        PutFixed32(buffer, (uint32_t)update.size());
#ifdef CODE_TRACE
        printf("[%d :: %s] skt id : %d || del size : %ld || update size : %ld\n", __LINE__, __func__, skt_id_, del.size(), update.size());
#endif
        for (std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>>::iterator it=del.begin(); it != del.end(); ++it){
                PutVarint64(buffer, (std::get<0>(*it))->GetiKeySequenceNumber());
                buffer->append(1, (uint8_t)((std::get<1>(*it))/*offset*/ | (std::get<2>(*it) << 7)/*request type*/));
        }
        for (std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>>::iterator it=update.begin(); it != update.end(); ++it){
                PutVarint64(buffer, (std::get<0>(*it))->GetiKeySequenceNumber());
                buffer->append(1, (uint8_t)((std::get<1>(*it))/*offset*/ | (std::get<2>(*it) << 7)/*request type*/));
        }
        size = buffer->size();
        if (size % kRequestAlignSize) {
            uint32_t padding_size = kRequestAlignSize - (size % kRequestAlignSize);
            buffer->append(padding_size, 0xff);
        }
        size = buffer->size();
        EncodeFixed32(const_cast<char*>(buffer->data()), (uint32_t)size); /* total size */
    }

    bool SKTableMem::DecodeUpdateList(std::list<std::tuple<uint64_t, uint8_t/*offset*/, RequestType>>& del, std::list<std::tuple<uint64_t, uint8_t/*offset*/, RequestType>>& update, char* buffer, uint32_t& size){
        Slice buf(buffer, size);
        uint32_t req_size = DecodeFixed32(buf.data());
        buf.remove_prefix(sizeof(uint32_t));
        // deleted key count
        uint32_t del_cnt = DecodeFixed32(buf.data());
        buf.remove_prefix(sizeof(uint32_t));
        // updated key count
        uint32_t update_cnt = DecodeFixed32(buf.data());
        buf.remove_prefix(sizeof(uint32_t));
        // parse deleted keys
        for (int i=0; i<del_cnt;i++){
            uint64_t ikey_seq;
            if(!GetVarint64(&buf, &ikey_seq))
                return false;
            uint8_t offset = buf.data()[0]&0x7f;
            RequestType req_type = (RequestType)(buf.data()[0]>>7);
            buf.remove_prefix(1);
            del.push_back(std::make_tuple(ikey_seq, offset, req_type));
        }
        // parse updated keys
        for (int i=0; i<update_cnt;i++){
            uint64_t ikey_seq;
            if(!GetVarint64(&buf, &ikey_seq))
                return false;
            uint8_t offset = buf.data()[0]&0x7f;
            RequestType req_type = (RequestType)(buf.data()[0]>>7);
            buf.remove_prefix(1);
            update.push_back(std::make_tuple(ikey_seq, offset, req_type));
        }
    }

    void SKTableMem::GetUpdatedKBMList(Manifest *mf, std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>> *kbm_list, std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>> *kbm_pending_list, std::list<std::tuple<KeyBlockMeta*, bool>> *device_submit_queue, uint8_t worker_id){

        for (auto it=kbm_list->begin(); it != kbm_list->end(); ){
            KeyBlockMeta* kbm = std::get<0>(*it);
            /* NRS can be decreased before calling ClearWriteProtection() if the log thread remove the kbml right after decreasing the nrs*/
            KeyBlockPrimaryMeta* kbpm = kbm->GetKBPM();

            if(!kbm->SetSubmitInProgress()){
                while(kbpm->SetWriteProtection());
                if(kbm->TestWorkerBit(worker_id)){
                    kbm->ClearWorkerBit();
                    device_submit_queue->push_back(std::make_tuple(kbm, std::get<3>(*it)));
                }else {
                    kbm->ClearSubmitInProgress();
#if 0
                    if (std::get<3>(*it))  {
                        if(!kbm->DecNRSCount()){
                            if(!(mf->IncCleanableKBM() &0xFF))
                                mf->LogCleanupSignal();
                            /* 
                             * Increase Cleanupable count 
                             * If the count reaches a treshold, wake LogCleanupThread up 
                             */
                        }
                    }
#endif
                }
                kbpm->ClearWriteProtection();

            }else{
                while(kbpm->SetWriteProtection());
                if(kbm->TestWorkerBit(worker_id)){
                    kbm_pending_list->push_back(*it);
                }
#if 0
                else{
                    if (std::get<3>(*it))  {
                        if(!kbm->DecNRSCount()){
                            if(!(mf->IncCleanableKBM() &0xFF))
                                mf->LogCleanupSignal();
                            /* 
                             * Increase Cleanupable count 
                             * If the count reaches a treshold, wake LogCleanupThread up 
                             */
                        }
                    }
                }
#endif
                kbpm->ClearWriteProtection();

            }
            it = kbm_list->erase(it);   
        }
    }

    Status SKTableMem::FlushSKTableMem(Manifest *mf, uint8_t worker_id, SKTableDFBuilderInfo *dfb_info)
    {/*force == true for DB termination*/

        if(!GetSKTableDeviceFormatNode())
            abort();
        Status s;
#if 0
        SKTableDFBuilderInfo *dfb_info = mf->GetSKTableDFBuilderInfoBuffer();
#endif
        assert(dfb_info->mf == mf);
        assert(dfb_info->skt_list.empty());
        assert(dfb_info->skt_comp_slice.empty());
        /* dfb_info->ofs_map_pos will be updated */
        assert(dfb_info->snapinfo_list.empty());
        assert(!dfb_info->smallest_snap_seq);
        assert(!dfb_info->nr_snap);
        assert(dfb_info->updated_kbm_list.empty());
        assert(dfb_info->iter_delayed_del.empty()); 
        assert(dfb_info->iter_update.empty()); 
        assert(dfb_info->update_list.empty()); 
        assert(dfb_info->delete_list.empty()); 
        assert(!dfb_info->skt_df_cache);
        assert(!dfb_info->new_skt_df_cache.size());
        /* Flush all pending requests at here */

        /*
         * Only deleted SKTable has no begin key.
         * And a deleted SKTable should not be here.
         */
        assert(GetBeginKeySlice().size() || skt_id_ == 1);


        /*
         * SKTable Split Condition
         * 1. Remainning Key Count > (The # of Recorded Keys  / 3)
         *    After filling 3/4 of SKTable, check the remainning key count.
         *    If the remainning key count is more than 1/4 of SKTable, Split the SKTable.
         * 2. kSplitSKTablePrefixSize 
         *    If the prefix of current key is differnt from the that of begin key.
         * 3. Cold/Hot based split
         *    If continious cold(not updated) key is more than 4KB(minimun SKTable Size) and has a hot key after cold keys,
         *    split the SKTable and make sub-SKTable with the hot key as begin key.
         * 4. Hash value conflict
         *    The hash value must be unique in a SKTable.
         */
        /* For Split Condition 2 : 
         * (sktm.key_count--) after scan a key
         * if(("Target SKTable Size" > "3/4 of max size") && (sktm.key_count > (target SKTable Keycoun / 3)))
         *   Split SKTable
         */
        dfb_info->cur_time = Env::Default()->NowSecond(); //For TTL check
        dfb_info->worker_id = worker_id;
        dfb_info->flush_ikey = 0;
#ifdef CODE_TRACE
        printf("[%d :: %s]SKTID : %d || worker_id : %d\n", __LINE__, __func__,skt_id_, worker_id);
#endif

        /*Get End(close:not included) key first*/
        uint32_t key_hash = 0;
        uint16_t key_size = 0;
        uint16_t key_buffer_size = kMaxKeySize;
        /*Get begin key & Starting UserKey which may not be the Begin Key */
        key_size = GetBeginKey(dfb_info->begin_key_buffer, key_buffer_size, key_hash);
        dfb_info->begin_key = KeySlice(dfb_info->begin_key_buffer, key_size, key_hash);

        IterLock();
        if(!InsertUserKeyToKeyMapFromInactiveKeyQueue(mf) && !keymap_.GetCount()){
            ReturnIterScratchPad(mf);
        }
        IterUnlock();


        dfb_info->ukey = keymap_.First();
        if (!dfb_info->ukey && !mf->IsNextSKTableDeleted(GetSKTableSortedListNode())) {
            ClearApproximateDirtyColumnCount();
            return s;
        }
        /* 
         * This is to reduce the size of InSDBKey stored in the SKTable. 
         * EncodeVarint64(cur_ikey_seq_num - col_node->ikey_seq);
         * But I'm not going to use it for small col_node->ikey_seq while huge cur_ikey_seq_num.
         * In this case, difference is much bigger than original ikey seq.
         */
        dfb_info->target_skt = this;
#if 0 /* check this */
        dfb_info->max_ivalue_size = kMaxInternalValueSzie; 
#endif
        /* 
         * Do not recored beyond the cur_ikey_seq_num for snapshot. 
         * snapshot can be changed & new key can be inserted.
         * If you want to apply latest snapshot & new key which is inserted during flush
         * you have to get snapshot list for every column scan.
         */


        /*
         * snapinfo_list has been clear()
         * dfb_info->snapinfo_list.clear();
         */
        mf->GetSnapshotInfo(&dfb_info->snapinfo_list);
        dfb_info->nr_snap = dfb_info->snapinfo_list.size();
#ifdef CODE_TRACE
        printf("[%d :: %s](SKTDI : %d)nr snapshot : %d\n", __LINE__, __func__, skt_id_, dfb_info->nr_snap);
#endif

        if(dfb_info->nr_snap){
            dfb_info->smallest_snap_seq = dfb_info->snapinfo_list[dfb_info->nr_snap - 1].cseq;
        }else/*Don't need to store the col_node->begin_sequence_number_ */
            dfb_info->smallest_snap_seq  = ULONG_MAX;//every seq # is smaller than this.

        dfb_info->skt_df_cache = GetSKTableDeviceFormatNode();
        dfb_info->sktdf_cur_key_offset  = SKTDF_BEGIN_KEY_OFFSET;
        dfb_info->sktdf_next_key_offset = 0;

        /*
         * Main SKTable
         *  - Insert key into the Main SKTable until the size reaches kMaxSKTableSize.
         * Sub SKTable
         *  - Make a 1/16 room for new key in Sub-SKTable
         *  - Insert key into the Sub SKTable until the size reaches 15/16 kMaxSKTableSize.
         *  - The Sub SKTable becomes main SKTable next flush time.
         */
        bool sub_skt = false;
        SKTableMem *target = NULL;
#ifdef CODE_TRACE
        char print_begin_key[dfb_info->begin_key.size()+1];
        memcpy(print_begin_key, dfb_info->begin_key.data(), dfb_info->begin_key.size());
        print_begin_key[dfb_info->begin_key.size()] = '\0';
        printf("[%d :: %s](main : %d)Main Begin key: %s\n", __LINE__, __func__, skt_id_, print_begin_key);
#endif
        ClearApproximateDirtyColumnCount();
        while(true){
            /* Create device format SKTables in memory  : Main -> 1st Sub -> ... -> Last Sub */
            dfb_info->key_offset.resize(0);/* Max # of keys per SKTable */
            DeviceFormatBuilder(mf, dfb_info, sub_skt);

            assert(dfb_info->target_skt->GetSKTableDeviceFormatNode());

            /* zero-sized begin key belongs to First SKTable which can not be sub-SKTable */
            if(dfb_info->begin_key.size()){/* The SKTable is split */
                dfb_info->target_skt = new SKTableMem(mf, &dfb_info->begin_key);
                assert(!dfb_info->target_skt->GetKeyMap()->GetCount());
                assert(!dfb_info->target_skt->IsInSKTableCache(mf));
#ifdef CODE_TRACE
                char print_begin_key[dfb_info->begin_key.size()+1];
                memcpy(print_begin_key, dfb_info->begin_key.data(), dfb_info->begin_key.size());
                print_begin_key[dfb_info->begin_key.size()] = '\0';
                printf("[%d :: %s]main : %d(skt : %p)Set Sub Begin key: %p\n", __LINE__, __func__, skt_id_, dfb_info->target_skt, print_begin_key);
#endif
                //dfb_info->target_skt->Lock();
                dfb_info->target_skt->SetFlushing();

            }else/* No more keys are remained in the SKTable */
                break;
            sub_skt = true;
        }/* End while */

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        ///////////////// Packing is not split yet /////////////////////////////////////////////////////////////////
        //////1. SKT ID allocation(Consider deleted next skt merge) ///////////////////////////////////////////////
        //////2. Calculate 2 CRCs: Whole CRC & Begin Key CRC //////////////////////////////////////////////////////
        //////3. Build Updated/Deleted KBM List Device Format /////////////////////////////////////////////////////
        //////4. Device Flush with largest iKey Sequence Number ///////////////////////////////////////////////////
        //////5. Update Completed TRXN Group ID ///////////////////////////////////////////////////////////////////
        //////6. device write /////////////////////////////////////////////////////////////////////////////////////
        //////// a. Updated/Deleted KBM List //////////////////////////////////////////////////////////////////////
        //////// b. SKTable Write(sub+main) ///////////////////////////////////////////////////////////////////////
        //////// c. Delete removeable KBMs ////////////////////////////////////////////////////////////////////////
        //////7. Delete kSKTUpdateList ////////////////////////////////////////////////////////////////////////////
        //////8. Update KBM Reference Count for non-iterator //////////////////////////////////////////////////////
        //////9. Insert Sub-SKTables into the Skiplist & Sorted List //////////////////////////////////////////////
        /////10. Delete Invalid SKTables //////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////



        //////1. SKT ID allocation(Consider deleted next skt merge) ///////////////////////////////////////////////
        std::deque<uint32_t> del_skt_list;
        del_skt_list.clear();
        if(dfb_info->skt_list.size() == 1){
            /*
             * No split
             * In this case, the SKTable can merge deleted SKTables
             */
            assert(dfb_info->target_skt == this);
            std::list<std::tuple<SKTableMem*, Slice, bool>>::iterator it;
            it = dfb_info->skt_list.begin();
            assert(std::get<0>(*it) == this);
            /* Main SKTable already has the SKT IDs. */
#if 0
            sktm.next_skt_id = GetNextSKTableID();
            sktm.prealloc_skt_id = GetPreallocSKTableID();
#endif
            Slice data = std::get<1>(*it);
            //EncodeFixed32(const_cast<char*>(dfb_info->new_skt_df_cache.data())+SKTDF_NEXT_ID_OFFSET, GetNextSKTableID()); 
            //EncodeFixed32(const_cast<char*>(dfb_info->new_skt_df_cache.data())+SKTDF_PREALLOC_ID_OFFSET, GetPreallocSKTableID()); 
            bool first_deleted_skt = false;
            while(true){
                /**
                 * More than 2 emtpy SKTables can exist,
                 * In this case, the current SKTable should merge all contiguous empty SKTables 
                 * If not, empty SKTables may not be merged during BuildManifest.  
                 */
                SKTableMem *del_skt = mf->NextSKTableInSortedList(GetSKTableSortedListNode()); 
                if(del_skt && del_skt->IsDeletedSKT() && !del_skt->IsFlushing()){
                    assert(!del_skt->IsEvictingSKT());
                    /** Merge SKTable */
                    mf->RemoveFromSKTableSortedList(del_skt->GetSKTableSortedListNode());
                    if(!first_deleted_skt){
                        SetPreallocSKTableID(del_skt->GetSKTableID());
                        //EncodeFixed32(const_cast<char*>(dfb_info->new_skt_df_cache.data())+SKTDF_PREALLOC_ID_OFFSET, del_skt->GetSKTableID()); 
                        first_deleted_skt = true;
                    }
                    SetNextSKTableID(del_skt->GetNextSKTableID());
                    //EncodeFixed32(const_cast<char*>(dfb_info->new_skt_df_cache.data())+SKTDF_NEXT_ID_OFFSET, del_skt->GetNextSKTableID()); 
                    del_skt_list.push_front(del_skt->GetSKTableID());
                    (void)del_skt->DeleteSKTDeviceFormatCache(mf);
                    /**
                     * the del_skt was removed from SKTable skiplist on last flush time
                     */

                    //printf("[%d :: %s] Before delete SKTableMem(id : %d)\n", __LINE__, __func__, del_skt->GetSKTableID());
                    delete del_skt;
                }else{ 
                    break;/*No Merge, No Split*/
                } 
            }
            /* Begin Key CRC */
            EncodeFixed32(const_cast<char*>(data.data())+SKTDF_NEXT_ID_OFFSET, GetNextSKTableID()); 
            EncodeFixed32(const_cast<char*>(data.data())+SKTDF_PREALLOC_ID_OFFSET, GetPreallocSKTableID()); 

        }else{
            assert(dfb_info->skt_list.size() > 1);
            /* The SKTable has been split */

#ifdef CODE_TRACE
            printf("[%d :: %s]SKTID(%d) || dfb_info->skt_list.size() : %d\n", __LINE__, __func__, skt_id_, dfb_info->skt_list.size());
#endif
            SKTableMem *sub_skt;
            std::list<std::tuple<SKTableMem*, Slice, bool>>::iterator it = dfb_info->skt_list.begin();
            std::tuple<SKTableMem*, Slice, bool> main = *it;
            assert(std::get<0>(main) == this);
            /* 
             * Move to last for device write
             * Write sequence : sub 1 => sub 2 => ... => last sub => main 
             */

            uint32_t last_next_skt_id = GetNextSKTableID(); // Used for last sub SKTable ID
            uint32_t next_skt_id = GetPreallocSKTableID(); // Used for very next sub SKTable ID
            uint32_t prealloc_skt_id = mf->GenerateNextSKTableID();
            SetNextSKTableID(next_skt_id);
            SetPreallocSKTableID(prealloc_skt_id );
            //EncodeFixed32(const_cast<char*>(dfb_info->new_skt_df_cache.data())+SKTDF_NEXT_ID_OFFSET, next_skt_id); 
            //EncodeFixed32(const_cast<char*>(dfb_info->new_skt_df_cache.data())+SKTDF_PREALLOC_ID_OFFSET, prealloc_skt_id); 
            EncodeFixed32(const_cast<char*>(std::get<1>(main).data())+SKTDF_NEXT_ID_OFFSET, next_skt_id); 
            EncodeFixed32(const_cast<char*>(std::get<1>(main).data())+SKTDF_PREALLOC_ID_OFFSET, prealloc_skt_id); 
            if(dfb_info->target_skt->IsDeletedSKT()){
                /* Deleted */ 
                delete dfb_info->target_skt;/* The last Sub SKTable has no key */
            }
            /* Create Sub SKTable's SKT IDs */
            ++it;
            dfb_info->skt_list.pop_front();
            for (; (it) != dfb_info->skt_list.end() && std::next(it) != dfb_info->skt_list.end(); ++it){
                sub_skt = std::get<0>(*it);
                Slice data = std::get<1>(*it);
                sub_skt->SetSKTableID(next_skt_id);
                next_skt_id = mf->GenerateNextSKTableID();
                sub_skt->SetNextSKTableID(next_skt_id);
                EncodeFixed32(const_cast<char*>(data.data())+SKTDF_NEXT_ID_OFFSET, next_skt_id); 
                prealloc_skt_id = mf->GenerateNextSKTableID();
                sub_skt->SetPreallocSKTableID(prealloc_skt_id);
                EncodeFixed32(const_cast<char*>(data.data())+SKTDF_PREALLOC_ID_OFFSET, prealloc_skt_id); 
            }
            /* For last Sub SKTable */
            sub_skt = std::get<0>(*it);
            Slice data = std::get<1>(*it);
            sub_skt->SetSKTableID(next_skt_id);
            sub_skt->SetNextSKTableID(last_next_skt_id);
            EncodeFixed32(const_cast<char*>(data.data())+SKTDF_NEXT_ID_OFFSET, last_next_skt_id); 
            prealloc_skt_id = mf->GenerateNextSKTableID();
            sub_skt->SetPreallocSKTableID(prealloc_skt_id);
            EncodeFixed32(const_cast<char*>(data.data())+SKTDF_PREALLOC_ID_OFFSET, prealloc_skt_id); 
            dfb_info->skt_list.push_back(main);
        }
        //////2. Calculate 2 CRCs: Whole CRC & Begin Key CRC //////////////////////////////////////////////////////
        for (std::list<std::tuple<SKTableMem*, Slice, bool>>::iterator it=dfb_info->skt_list.begin(); (it) != dfb_info->skt_list.end() ; ++it){
            /* Begin Key CRC */
            Slice data = std::get<1>(*it);
            uint32_t second_key_start = *((uint32_t*)(data.data()+SKTDF_COMP_START_OFFSET));
            if(second_key_start){
                uint32_t crc = crc32c::Value(data.data()+SKTDF_NR_KEY_OFFSET, second_key_start-SKTDF_NR_KEY_OFFSET);
                EncodeFixed32(const_cast<char*>((data.data())+SKTDF_CRC_2_OFFSET), crc32c::Mask(crc));
                /* Whole SKT CRC */
                uint32_t skt_size = *((uint32_t*)(data.data()+SKTDF_SKT_SIZE_OFFSET));
                crc = crc32c::Value(const_cast<char*>(data.data())+SKTDF_CRC_2_OFFSET, second_key_start + skt_size -SKTDF_CRC_2_OFFSET);
                EncodeFixed32(const_cast<char*>(data.data())+SKTDF_CRC_1_OFFSET, crc32c::Mask(crc));
#ifdef CODE_TRACE
                printf("[%d :: %s]sktable(%p:%d), comp_data_start : %u || crc :%u || device_format size : %lu || comp_size : %d\n", __LINE__, __func__,  std::get<0>(*it), (std::get<0>(*it))->GetSKTableID(), second_key_start, (crc), data.size(), *((uint32_t*)(data.data() + SKTDF_SKT_SIZE_OFFSET)));
#endif
            }
        }
        //////3. Build Updated/Deleted KBM List Device Format /////////////////////////////////////////////////////
        InSDBKey key;
        std::string* update_list = NULL;
        uint32_t update_list_size = 0;
        if(!dfb_info->delete_list.empty() || !dfb_info->update_list.empty()){
            update_list = mf->GetKBMListBuffer(false);
            update_list->clear();
            BuildUpdateList(dfb_info->delete_list, dfb_info->update_list, update_list, update_list_size);
        }

        std::string* iter_update_list = NULL;
        uint32_t iter_update_list_size = 0;
        if(!dfb_info->iter_delayed_del.empty() || !dfb_info->iter_update.empty()){
            iter_update_list = mf->GetKBMListBuffer(true);
            iter_update_list->clear();
            BuildUpdateList(dfb_info->iter_delayed_del, dfb_info->iter_update, iter_update_list, iter_update_list_size);
            SetIterUpdateList();
        }else if(HasIterUpdateList()){
            /* TODO delete kSKTIterUpdateList which was created on previous flush */
            key = GetMetaKey(mf->GetDBHash(), kSKTIterUpdateList, skt_id_);
            for(uint16_t i = 0; i < 10; i++) {
                key.split_seq = i;
                s = Env::Default()->Delete(key, true);//async
            }
            ClearIterUpdateList();
        }

        //////4. Device Flush with largest iKey Sequence Number ///////////////////////////////////////////////////
        s = Env::Default()->Flush(kWriteFlush, GetKBKey(mf->GetDBHash(), kKBlock, dfb_info->flush_ikey));

        //////6. device write /////////////////////////////////////////////////////////////////////////////////////
        //////// a. Updated/Deleted KBM List //////////////////////////////////////////////////////////////////////
        /* TODO Device Write : if not NULL, [iter_update_list, iter_update_list_size] & [update_list, update_list_size] */
        if(update_list_size){
            key = GetMetaKey(mf->GetDBHash(), kSKTUpdateList, skt_id_);
            char *data = const_cast<char*>(update_list->data());
            uint32_t remain_size = update_list_size;
            uint32_t req_size = 0;
            uint16_t num_mf = (remain_size + kDeviceRquestMaxSize - 1)/kDeviceRquestMaxSize;
            for(uint16_t i = 0; i < num_mf; i++) {
                key.split_seq = i;
                req_size = (remain_size < kDeviceRquestMaxSize) ? remain_size : kDeviceRquestMaxSize;
                s = Env::Default()->Put(key, Slice(data, req_size), false);//sync
                if (!s.ok()) printf("Fail to write SKT update list\n");
                remain_size -= req_size;
                data += req_size;
            }
        }
        if(iter_update_list_size){
            key = GetMetaKey(mf->GetDBHash(), kSKTIterUpdateList, skt_id_);
            char *data = const_cast<char*>(iter_update_list->data());
            uint32_t remain_size = iter_update_list_size;
            uint32_t req_size = 0;
            uint16_t num_mf = (remain_size + kDeviceRquestMaxSize - 1)/kDeviceRquestMaxSize;
            for(uint16_t i = 0; i < num_mf; i++) {
                key.split_seq = i;
                req_size = (remain_size < kDeviceRquestMaxSize) ? remain_size : kDeviceRquestMaxSize;
                s = Env::Default()->Put(key, Slice(data, req_size), false);//sync
                if (!s.ok()) printf("Fail to write SKT update list\n");
                remain_size -= req_size;
                data += req_size;
            }
        }
        //////// b. SKTable Write(sub+main) ///////////////////////////////////////////////////////////////////////
        for (auto it=dfb_info->skt_list.begin(); (it) != dfb_info->skt_list.end(); ++it){
            SKTableMem* skt = std::get<0>(*it);
            Slice data = std::get<1>(*it);
#ifdef CODE_TRACE
            printf("[%d :: %s][Main : worker %u %d (%p) Sub : %d  (%p)](SKTDF size : %lu)\n", __LINE__, __func__, dfb_info->worker_id, skt_id_, this, skt->GetSKTableID(), skt, data.size());
#endif
            key = GetMetaKey(mf->GetDBHash(), kSKTable, skt->GetSKTableID());
            s = Env::Default()->Put(key, data, false);//sync
            /* TODO Write SKTable [sktid = skt->SetSKTableID(), data.data(), data.size()]*/
            // Set HasBeenFlushed flag.
            skt->SetHasBeenFlushed();
        }


        //////// c. Delete removeable KBMs ////////////////////////////////////////////////////////////////////////
        std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>> queue[2];
        std::list<std::tuple<KeyBlockMeta*, bool>>      device_submit_queue;
        uint8_t pend_q_idx = 0;
        queue[pend_q_idx] = dfb_info->delete_list;
        std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>>      *kbm_list;
        std::list<std::tuple<KeyBlockMeta*, uint8_t/*offset*/, RequestType, bool>>      *kbm_pending_list;
        std::vector<InSDBKey> updated_keys;
        InSDBKey null_insdbkey;
        null_insdbkey.first = 0;
        null_insdbkey.second = 0;

remove:
        kbm_list = &queue[pend_q_idx];
        kbm_pending_list = &queue[pend_q_idx^1];
        assert(device_submit_queue.empty());
        assert(kbm_pending_list->empty());

        GetUpdatedKBMList(mf, kbm_list, kbm_pending_list, &device_submit_queue, worker_id);
        assert(kbm_list->empty());
        assert(updated_keys.empty());

        /* Get deleted key list from KBM */
        for (std::list<std::tuple<KeyBlockMeta*, bool>>::iterator it=device_submit_queue.begin(); it != device_submit_queue.end(); it++){
            /*
             * prepare batch delete
             * if(kbm->GetOriginalKeyCount()) { Delete only KBM }
             * else { Delete both, KBM & KB }
             */
#ifdef CODE_TRACE    
            printf("[%d :: %s]Delete Meta ikey : %ld\n", __LINE__, __func__, (std::get<0>(*it))->GetiKeySequenceNumber());
#endif
            updated_keys.push_back(GetKBKey(mf->GetDBHash(), kKBMeta, (std::get<0>(*it))->GetiKeySequenceNumber()));
            if((std::get<0>(*it))->GetOriginalKeyCount())
                updated_keys.push_back(GetKBKey(mf->GetDBHash(), kKBlock, (std::get<0>(*it))->GetiKeySequenceNumber()));
        }
        /* Batch Delete*/
        if(!updated_keys.empty()) {
            std::vector<Env::DevStatus> status;
#ifdef CODE_TRACE
            printf("[%d :: %s] SKTID(%d) ||  MultiDel cnt : %d\n", __LINE__, __func__, GetSKTableID(), updated_keys.size());
#endif
            Env::Default()->MultiDel(updated_keys, null_insdbkey, false, status);
        }

        while(!device_submit_queue.empty()){
            std::tuple<KeyBlockMeta*, bool> item = device_submit_queue.front();
            device_submit_queue.pop_front();
            (std::get<0>(item))->ClearSubmitInProgress();
#if 0
            if (std::get<1>(item)) {
                if(!(std::get<0>(item))->DecNRSCount()){
                    if(!(mf->IncCleanableKBM() &0xFF))
                        mf->LogCleanupSignal();
                    /* 
                     * Increase Cleanupable count 
                     * If the count reaches a treshold, wake LogCleanupThread up 
                     */
                }
            }
#endif
        }
        assert(device_submit_queue.empty());
        updated_keys.clear();
        if(!kbm_pending_list->empty()){
            /* TODO exchange queue[x] <-> queue[x^1] */
            pend_q_idx ^= 1;
            goto remove;
        }
        assert(queue[0].empty());
        assert(queue[1].empty());
        pend_q_idx  = 0;


        //////// d. Write updated KBMs ////////////////////////////////////////////////////////////////////////////
#ifdef CODE_TRACE
        printf("[%d :: %s]dfb_info->iter_delayed_del : %d || dfb_info->iter_update : %d || dfb_info->update_list : %d\n", __LINE__, __func__, dfb_info->iter_delayed_del.size(), dfb_info->iter_update.size(), dfb_info->update_list.size());
#endif
        queue[0] = dfb_info->update_list;
        queue[0].insert(queue[0].end(), dfb_info->iter_delayed_del.begin(), dfb_info->iter_delayed_del.end());
        queue[0].insert(queue[0].end(), dfb_info->iter_update.begin(), dfb_info->iter_update.end());

        std::vector<char*> kbm_buf;
        std::vector<int> kbm_size;
update:
        kbm_list = &(queue[pend_q_idx]);
        kbm_pending_list = &(queue[pend_q_idx^1]);
        assert(device_submit_queue.empty());
        assert(kbm_pending_list->empty());

        GetUpdatedKBMList(mf, kbm_list, kbm_pending_list, &device_submit_queue, worker_id);
        assert(kbm_list->empty());
        assert(updated_keys.empty());

        std::vector<Slice> io_buffer;/* For free the allocated memory*/
        /* Get updated key list from KBM */
        for (auto it=device_submit_queue.begin(); it != device_submit_queue.end(); it++){
            uint32_t size = (std::get<0>(*it))->GetEstimatedBufferSize();
            uint32_t aligned_size = SizeAlignment(size);
            Slice buf = mf->PerThreadAllocRawMemoryNodeWrapper(aligned_size);

            /*NRS has not been decreased yet! so, we don't need to get the kbpm*/
            KeyBlockMeta* kbm = std::get<0>(*it);
            //KeyBlockPrimaryMeta* kbpm = kbm->GetKBPM();
            while(kbm->SetWriteProtection());
            size = kbm->BuildDeviceFormatKeyBlockMeta(const_cast<char*>(buf.data()));
            kbm->ClearWriteProtection();
            assert(size <= aligned_size);
            io_buffer.push_back(buf);

            updated_keys.push_back(GetKBKey(mf->GetDBHash(), kKBMeta, (std::get<0>(*it))->GetiKeySequenceNumber()));
            kbm_buf.push_back(const_cast<char*>(buf.data()));
            kbm_size.push_back(SizeAlignment(size));
        }
        if(!updated_keys.empty()){
#ifdef CODE_TRACE
            printf("[%d :: %s] SKTID(%d) ||  MultiPut cnt : %d\n", __LINE__, __func__,  GetSKTableID(), updated_keys.size());
#endif
            Env::Default()->MultiPut(updated_keys, kbm_buf, kbm_size, null_insdbkey, false);
        }

        while(!device_submit_queue.empty()){
            std::tuple<KeyBlockMeta*, bool> item = device_submit_queue.front();
            device_submit_queue.pop_front();
            (std::get<0>(item))->ClearSubmitInProgress();
        }
        assert(device_submit_queue.empty());
        updated_keys.clear();
        kbm_buf.clear();
        kbm_size.clear();
        for (auto it=io_buffer.begin(); it != io_buffer.end(); it++){
            mf->PerThreadFreeRawMemoryNodeWrapper(*it);
        }
        if(!kbm_pending_list->empty()){
            /* Swap queue[x] <-> queue[x^1] */
            pend_q_idx ^= 1;
            goto update;
        }

        //////7. Delete kSKTUpdateList ////////////////////////////////////////////////////////////////////////////
        /* TODO delete kSKTUpdateList*/
        if(update_list_size){
            key = GetMetaKey(mf->GetDBHash(), kSKTUpdateList, skt_id_);
            uint32_t remain_size = update_list_size;
            uint32_t req_size = 0;
            uint16_t num_mf = (remain_size + kDeviceRquestMaxSize - 1)/kDeviceRquestMaxSize;
            for(uint16_t i = 0; i < num_mf; i++) {
                key.split_seq = i;
                req_size = (remain_size < kDeviceRquestMaxSize) ? remain_size : kDeviceRquestMaxSize;
                s = Env::Default()->Delete(key, true);//async
                if (!s.ok()) printf("Fail to Delete SKT update list\n");
                remain_size -= req_size;
            }
        }
        bool wakeup_log_cleanup_thread = false;
        //////8-1. Update KBM Reference Count for iterator //////////////////////////////////////////////////////
        assert(queue[0].empty());
        assert(queue[1].empty());
        queue[0] = dfb_info->iter_delayed_del;
        queue[0].insert(queue[0].end(), dfb_info->iter_update.begin(), dfb_info->iter_update.end());
        kbm_list = &(queue[0]);
        for (auto it = kbm_list->begin(); it != kbm_list->end(); ){
            KeyBlockMeta* kbm = std::get<0>(*it);
            KeyBlockPrimaryMeta* kbpm = kbm->GetKBPM();
            if (std::get<3>(*it))  {
                if(!kbm->DecNRSCount() && !(mf->IncCleanableKBM() &0xFF)) wakeup_log_cleanup_thread = true;
            }
            it = kbm_list->erase(it);   
        }

        //////8-2. Update KBM Reference Count for non-iterator //////////////////////////////////////////////////////

        assert(queue[0].empty());
        assert(queue[1].empty());
        queue[0] = dfb_info->update_list;
        queue[0].insert(queue[0].end(), dfb_info->delete_list.begin(), dfb_info->delete_list.end());
        kbm_list = &(queue[0]);
        for (auto it = kbm_list->begin(); it != kbm_list->end(); ){
            KeyBlockMeta* kbm = std::get<0>(*it);
            KeyBlockPrimaryMeta* kbpm = kbm->GetKBPM();
            if (std::get<3>(*it))  {
                if(!kbm->DecNRSCount() && !(mf->IncCleanableKBM() &0xFF)) wakeup_log_cleanup_thread = true;
            }

            /*kbpm->GetOriginalKeyCount()*//* Filter out the delete only Key Block:delete only must be in-log state &&*/

            if(kbpm->DecRefCntAndTryEviction(mf))
                mf->FreeKBPM(kbpm);
            it = kbm_list->erase(it);   
        }
        if(wakeup_log_cleanup_thread)
            mf->LogCleanupSignal();

        dfb_info->delete_list.clear();
        dfb_info->iter_delayed_del.clear();
        dfb_info->iter_update.clear();
        dfb_info->update_list.clear();
        //////9. Insert Sub-SKTables into the Skiplist & Sorted List //////////////////////////////////////////////
        dfb_info->skt_list.pop_back(); //remove main SKTable
        {
            /* write lock for IterScratchPad 
             * every columns in IterScratchPad should not be evicted.
             * So, copy main SKT has ref cnt to sub skt to prevent sub skt eviction.
             */
            std::lock_guard<folly::SharedMutex> l(sktdf_update_mu_);

#if 1
            /* Lock for update sub-SKTable's keymap_*/
            for (auto sub_data : dfb_info->skt_list ) {
                target = std::get<0>(sub_data);
                target->SKTDFUpdateGuardLock();
                target->KeyMapGuardLock();
            }
#endif

            uint32_t skt_ref_cnt = GetSKTRefCnt();
#ifdef CODE_TRACE
                printf("[%d :: %s]Start\n", __LINE__, __func__);
#endif
            for (auto sub_data = dfb_info->skt_list.rbegin() ; sub_data != dfb_info->skt_list.rend() ; sub_data++) {
                target = std::get<0>(*sub_data);
                assert(!target->GetKeyMap()->GetCount());
                mf->InsertNextToSKTableSortedList(target->GetSKTableSortedListNode(), GetSKTableSortedListNode());
#ifdef CODE_TRACE
                printf("[%d :: %s] worker %u inserting sub skt %u beginkey %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\n", __LINE__, __func__, dfb_info->worker_id, target->GetSKTableID(),
                        (unsigned char)target->begin_key_buffer_[0][0], (unsigned char)target->begin_key_buffer_[0][1], (unsigned char)target->begin_key_buffer_[0][2], (unsigned char)target->begin_key_buffer_[0][3], (unsigned char)target->begin_key_buffer_[0][4], (unsigned char)target->begin_key_buffer_[0][5], (unsigned char)target->begin_key_buffer_[0][6],(unsigned char)target->begin_key_buffer_[0][7],
                        (unsigned char)target->begin_key_buffer_[0][8], (unsigned char)target->begin_key_buffer_[0][9], (unsigned char)target->begin_key_buffer_[0][10], (unsigned char)target->begin_key_buffer_[0][11], (unsigned char)target->begin_key_buffer_[0][12], (unsigned char)target->begin_key_buffer_[0][13], (unsigned char)target->begin_key_buffer_[0][14],(unsigned char)target->begin_key_buffer_[0][15]
                        );
#endif
                if(!mf->InsertSKTableMem(target)){
                    printf("[%d :: %s]fail to sub skt %u insert\n", __LINE__, __func__, target->GetSKTableID());
                    abort();
                }
#ifdef CODE_TRACE
                printf("[%d :: %s]SKTID : %d\n", __LINE__, __func__, target->GetSKTableID());
#endif
            }
#ifdef CODE_TRACE
                printf("[%d :: %s]End\n", __LINE__, __func__);
#endif
#ifndef NDEBUG //For debug
            for (auto sub_data = dfb_info->skt_list.rbegin() ; sub_data != dfb_info->skt_list.rend() ; sub_data++) {
                target = std::get<0>(*sub_data);
                assert(!target->GetKeyMap()->GetCount());
            }
#endif



            /*The main SKTable's Device Format must be updated after Sub-SKTable insertion*/
            assert(dfb_info->new_skt_df_cache.size());
            SKTDFCacheNode* main_sktdf = skt_df_node_;
            UpdateSKTDFNodeSize(mf, dfb_info->new_skt_df_cache.size(), main_sktdf->GetSize());
            main_sktdf->UpdateDeviceFormat(mf, dfb_info->new_skt_df_cache);

            bool evicted = false;
            EvictCleanUserKeyFromKeymap(mf, false, evicted);
            KeyMapGuardLock();
            if(!dfb_info->skt_list.empty()){
                auto it=dfb_info->skt_list.begin();//First sub-SKTableMem
                SKTableMem* skt = std::get<0>(*it);
                KeySlice begin_key = skt->GetBeginKeySlice();

                UserKey* uk = NULL, *next_start = NULL;
                SLSplitCtx m_ctx;
                assert(!skt->GetKeyMap()->GetCount());
                skt->GetKeyMap()->InitSplitTarget(&m_ctx);
                {
                    uk = keymap_.SearchGreaterOrEqual(begin_key, &m_ctx); /* For Main-SKTableMem's key, just skip*/
                    for (++it; uk && (it) != dfb_info->skt_list.end(); ++it)
                    {
                        begin_key = std::get<0>(*it)->GetBeginKeySlice();
                        next_start = keymap_.SearchGreaterOrEqual(begin_key); /* search next beginkey without split context */
                        while(uk){
                            /* Insert UserKey to current Sub-SKTableMem if the UserKey is smaller than Next Sub-SKTable's begin key*/
                            if(uk != next_start ){
                                keymap_.Remove(&m_ctx);
                                skt->GetKeyMap()->Append(&m_ctx);
                                uk = keymap_.Next(nullptr, &m_ctx);
                            }else
                                break;
                        }
                        skt = std::get<0>(*it);
                        assert(!skt->GetKeyMap()->GetCount());
                        skt->GetKeyMap()->InitSplitTarget(&m_ctx);
                    }
                    /*Remained Keys belong to last Sub-SKTableMem*/
                    while(uk){
                        keymap_.Remove(&m_ctx);
                        skt->GetKeyMap()->Append(&m_ctx);
                        uk = keymap_.Next(nullptr, &m_ctx);
                    }
                }
            }
#if 1
            bool first_sub_skt = true;
            for (auto sub_data : dfb_info->skt_list ) {
                target = std::get<0>(sub_data);
                target->KeyMapGuardUnLock();
                target->SKTDFUpdateGuardUnLock();
                if(target->GetApproximateDirtyColumnCount() || (first_sub_skt && mf->IsNextSKTableDeleted(target->GetSKTableSortedListNode()))){
                    mf->InsertSKTableMemCache(target, kDirtySKTCache, true/*Clear Flush Flag*/ );//Insert it to dirty cache list

                }else{
                    mf->InsertSKTableMemCache(target, kCleanSKTCache, true/*Clear Flush Flag*/, true/*Wake Cache Evictor*/);//Insert it to clean cache list
                }
                first_sub_skt = false;
            }
            KeyMapGuardUnLock();
#endif

            dfb_info->skt_list.clear();
        }

        /////10. Delete Invalid SKTables //////////////////////////////////////////////////////////////////////////
        for(std::deque<uint32_t>::iterator it = del_skt_list.begin() ; it != del_skt_list.end() ; it++){
            InSDBKey key = GetMetaKey(mf->GetDBHash(), kSKTable, *it);
            s = Env::Default()->Delete(key, true);
        }


        /*Free compressed SKTable Device Formats */
        while(!(dfb_info->skt_comp_slice.empty())){
            Slice item = dfb_info->skt_comp_slice.front();
            dfb_info->skt_comp_slice.pop_front();
            mf->FreeSKTableBuffer(item);
        }
        assert(dfb_info->skt_comp_slice.empty());

        if(IsDeletedSKT()){
            DecreaseSKTDFNodeSize(mf);
            SKTableMem *prev = this;
            while(1){
                prev = mf->PrevSKTableInSortedList(prev->GetSKTableSortedListNode());
                if(!prev->IsDeletedSKT() ){
                    /*Fetched status will be checked in PrefetchSKTable()*/
                    if(!prev->GetSKTableDeviceFormatNode())
                        prev->PrefetchSKTable(mf);
#ifndef NDEBUG
                    mf->SKTableCacheMemLock();
                    assert(prev->IsInSKTableCache(mf) || prev->IsFlushOrEvicting());
                    mf->SKTableCacheMemUnlock();
#endif
                    mf->MigrateSKTableMemCache(prev);
                    break;
                }
#ifdef CODE_TRACE
                else if(prev->GetSKTableID() == 1) abort();
#endif
            }
        }
#ifdef CODE_TRACE
        printf("[%d :: %s]Finish \n\n", __LINE__, __func__);
#endif

        dfb_info->smallest_snap_seq = 0;
        dfb_info->skt_df_cache = NULL;
        dfb_info->new_skt_df_cache = Slice(0);
        dfb_info->snapinfo_list.clear();
        dfb_info->nr_snap = 0;

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

        return s;
    }

    SKTableMem* SKTableMem::Next(Manifest* mf) {
        return mf->Next(this);
    }

    SKTableMem* SKTableMem::Prev(Manifest* mf) {
        return mf->Prev(this);
    }

    char* SKTableMem::EncodeFirstKey(const KeySlice &key, char *buffer, uint32_t &buffer_size){
#ifdef CODE_TRACE
        printf("[%d :: %s]BeGin(%d) : key size : %d\n", __LINE__, __func__, skt_id_,key.size());
#endif
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
#ifdef CODE_TRACE
        if(buffer_size == 40)
            printf("[%d :: %s]BEGIN KEY BUT CALL EncodeKey()\n", __LINE__, __func__);
#endif
        const size_t min_length = std::min(begin_key_of_target.size(), key.size());
        uint16_t shared = 0;
        while ((shared < min_length) && (begin_key_of_target[shared] == key[shared])) {
            shared++;
        }
        uint16_t non_shared = key.size() - shared;
        char* buf = EncodeVarint16(buffer, shared);
        buffer_size+= (buf-buffer);
#ifdef CODE_TRACE
        if(!non_shared)
            printf("[%d :: %s]Non shared %d:%d\n", __LINE__, __func__,non_shared,shared);
#endif
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

    /* if the col_node is valid, return it. * */
    ColumnStatus SKTableMem::CheckColumnStatus(Manifest* mf, SKTableDFBuilderInfo *dfb_info, UserKey* uk, ColumnNode* col_node, ColumnNode* next_col_node, uint8_t col_id,
            KeyBlockMeta* kbm, bool iter, bool &drop_col_node, bool& recent_col_node_deleted, bool bDecNrs){
        assert(col_node);

        if(!drop_col_node && (col_node->GetInSDBKeySeqNum() != ULONG_MAX)){
            drop_col_node = true;
            if( col_node->GetRequestType() == kPutType  && !col_node->GetRecentColNodeyDeleted() &&
                (mf->GetTtlFromTtlList(col_id) ? (col_node->GetTS() + mf->GetTtlFromTtlList(col_id) >= dfb_info->cur_time) : true)) {
                /* 
                 * it is valid column node.
                 * In this case, we don't need to update KBM/KB.
                 * Just add the col_node to SKTable. 
                 */
#ifdef CODE_TRACE
                printf("[%d :: %s]insert new key\n", __LINE__, __func__);
#endif
                return kValidColumn;
            }
        }
#ifdef CODE_TRACE
        printf("[%d :: %s]invalidte key\n", __LINE__, __func__);
#endif
        /*
         * The col_node has been updated(including vertical merge(== ULONG_MAX) or delete(kDelType) command(including expired)
         * So, all col_node at this point will update their KBM.
         */
        /* Update KeyBlockMeta and insert to update Q unless it should be deleted */
        /*At this point, kbm can be either KeyBlockPrimaryMeta or KeyBlockMetaLog */
        if(iter){
            /*It must be kPutType */
            /* Do not remove column because an iterator has this column */
            if(col_node->GetInSDBKeySeqNum() != ULONG_MAX){
                /////////////////////// Begin Spinlock for Write Protection /////////////////////// 
                /* 
                 * spinlock: SetWriteProtection() return true if Write Protection has already been set by another thread(either worker or log thread).
                 * Log thread needs to aquires a lock to change InLog state. 
                 */
                while(kbm->SetWriteProtection());
                kbm->SetWorkerBit(dfb_info->worker_id);
                if(!col_node->IsHoldingIter()){
                    col_node->SetHoldingIter();
                    /* Clear bit only when first time access */
                    kbm->ClearKeyBlockBitmap(col_node->GetKeyBlockOffset(), col_node->GetRequestType());
                }
                /*else{ HoldingIter count was increased during previous flush. } */

                /* 
                 * Clear the col_node bit in the KBPM->bitmap.
                 * And then increase "Iterator holding count" in KBMP
                 * if KBM has "Iterator Holding Count", Log Thread should not delete the KBM.
                 *
                 * If do not clear the col_node bit in the bitmap to provent deletion, it may considered as valid in recovery in following situation.
                 *
                 * when iter is found & other bit is set(KBM update needed)
                 * But, even in this case we need to keep the pre-record-log in device.
                 * If we don't keep the pre-record-log ,
                 * the KBM may not be deleted(garbage becuase no one have the KBM) or very hard to find its status.
                 * 
                 */
                /* */
                /* 
                 * Following list keeps both, deleted and updated KBM which are held by Iterator
                 * 1. update KBM, other column in the KBM are still valid.
                 *    Even in this case we need to keep the log in device for the case of crash.
                 * 2. The KBM has no valid columns. But we need to keep it because of iterator
                 *    So, we'ar going to update KBM in Device with "0" bitmap.
                 *    It will be deleted when it become non-iter column or recovery
                 */
                if(kbm->TestKeyBlockBitmap())
                    dfb_info->iter_update.push_back(std::make_tuple(kbm, col_node->GetKeyBlockOffset(), col_node->GetRequestType(), bDecNrs));/* Do not delete this pre-record-log after flush */
                else
                    dfb_info->iter_delayed_del.push_back(std::make_tuple(kbm, col_node->GetKeyBlockOffset(), col_node->GetRequestType(), bDecNrs));/* Do not delete this pre-record-log after flush */
                kbm->ClearWriteProtection(); 
#ifdef CODE_TRACE
                printf("[%d :: %s]invalidte key Set iter dirty col_node(%p)\n", __LINE__, __func__, col_node);
#endif
            }
            /* Can not evict the UserKey until iterator releases the col_node */
            return kIterColumn;

        }
        /* Need to check type because delete has no bitmap( only put has bitmap in KBPM ) */
        /* ColumnNode can be deleted because either it is invalid and does not belongs to an iterator. */
        if(col_node->GetInSDBKeySeqNum() != ULONG_MAX){
            /////////////////////// Begin Spinlock for Write Protection /////////////////////// 
            /* 
             * spinlock: SetWriteProtection() return true if Write Protection has already been set by another thread(either worker or log thread).
             * Log thread needs to aquires a lock to change InLog state. 
             */
            while(kbm->SetWriteProtection());
            kbm->SetWorkerBit(dfb_info->worker_id);
            if(!col_node->IsHoldingIter()){
                /* update bitmap */
                kbm->ClearKeyBlockBitmap(col_node->GetKeyBlockOffset(), col_node->GetRequestType());
            }else
                col_node->ClearHoldingIter();
            if( kbm->TestKeyBlockBitmap() || kbm->IsInlog() || kbm->GetRefCnt() > 1/*For iter holding bit(1 for  this column)*/){
                /* KeyBlock has valid Keys *//*<------ Key Block has NO valid keys ------>*/
#ifdef CODE_TRACE    
                char key[17];
                strncpy(key, uk->GetKeySlice().data() , 16);
                key[16] = '\0';
                printf("[%d :: %s] for Update list || col_node ikey : %ld || kbm ikey : %ld || kbm->orgkeycnt : %d || uk : %s || uk addr : %p || col addr : %p\n", __LINE__, __func__ , col_node->GetInSDBKeySeqNum(), kbm->GetiKeySequenceNumber(), kbm->GetOriginalKeyCount(), key, uk, col_node);
#endif
                /*decrease the ref cnt after submit, if the ref cnt becomes 0 free the kbm */
                dfb_info->update_list.push_back(std::make_tuple(kbm, col_node->GetKeyBlockOffset(), col_node->GetRequestType(), bDecNrs));

            }else{
                assert(!kbm->IsDummy());
                /* Key Block has NO valid keys */
                /*
                 * Out Log : the KBM/KB should be deleted.
                 */
#ifdef CODE_TRACE    
                char key[17];
                strncpy(key, uk->GetKeySlice().data() , 16);
                key[16] = '\0';
                printf("[%d :: %s] for Delete list || col_node ikey : %ld || kbm ikey : %ld || kbm->orgkeycnt : %d || uk : %s || uk addr : %p || col addr : %p\n", __LINE__, __func__ , col_node->GetInSDBKeySeqNum(), kbm->GetiKeySequenceNumber(), kbm->GetOriginalKeyCount(), key, uk, col_node);
#endif
                /*decrease the ref cnt after submit, if the ref cnt becomes 0 free the kbm */
                dfb_info->delete_list.push_back(std::make_tuple(kbm, col_node->GetKeyBlockOffset(), col_node->GetRequestType(), bDecNrs));
            }

            kbm->ClearWriteProtection();
            /////////////////////// End Spinlock for Write Protection /////////////////////// 

            SimpleLinkedList* col_list= uk->GetColumnList(col_id);
            col_list->Delete(col_node->GetColumnNodeNode());
            col_node->InvalidateUserValue(mf);
            /*
             * There is possiblity that previous sktdf node has set reference for those.
             * To remove dangling reference, clear reference bit here.
             */
            KeyBlockPrimaryMeta *kbpm = kbm->GetKBPM();
            if (kbpm) kbpm->ClearRefBitmap(col_node->GetKeyBlockOffset());
            /*
             * The key will be deleted during building the del_queue ... .
             * delete del_ik;
             * But in packing version, it is okay to delete the col_node here.. maybe..
             */
#ifdef CODE_TRACE
            printf("[%d :: %s]invalidte key Set delete col_node(%p)\n", __LINE__, __func__, col_node);
#endif

        }else{
            /*
             * if ikey is ULONG_MAX, 
             * it means this column node has been vertical merged 
             * In this case, its' value was not inserted to Value Cache.
             * And it does not have releated KBM or KB.
             * It just have User Value.
             */
            SimpleLinkedList* col_list= uk->GetColumnList(col_id);
            col_list->Delete(col_node->GetColumnNodeNode());
            UserValue *uv = col_node->GetUserValue();
            assert(uv);
            col_node->RemoveUserValue(); /* clear uservalue pointer */
            mf->FreeUserValue(uv);
#ifdef INSDB_GLOBAL_STATS
            g_del_col_node_cnt++;
#endif

#ifdef CODE_TRACE
            printf("[%d :: %s]invalidte key Set delete vertical merged col_node(%p)\n", __LINE__, __func__, col_node);
#endif
        }
        recent_col_node_deleted = true;
        /* If a col_node is deleted, it means the key is updated 
         * TODO : a case of drop should increase update count.
         */
        if(uk->GetLatestColumnNode(col_id) == col_node)
            uk->SetLatestColumnNode(col_id, next_col_node);
        delete col_node;
        return kInvalidColumn;
    }

    bool SKTableMem::SplitByPrefix(const Slice& a, const Slice& b){
        if (!kPrefixIdentifierSize || a.size() < kPrefixIdentifierSize ||  b.size() < kPrefixIdentifierSize || a.size() != b.size()) {
            return false;
        }
       if(kPrefixIdentifierSize && (memcmp(a.data(), b.data(), kPrefixIdentifierSize) != 0));
       // return (kPrefixIdentifierSize && (memcmp(a.data(), b.data(), kPrefixIdentifierSize) != 0));
        return true;
    }
    ///////////////////// Split Condition Check /////////////////////////// 
    
#if 0
    bool SKTableMem::CheckSplitCondition(Manifest* mf, bool sktdf_data, UserKey* next_uk, std::string& key_from_skt_df, KeySlice cur_key_slice, uint32_t buffer_size, SKTableDFBuilderInfo *dfb_info, uint32_t skt_info_2_size, uint32_t max_buffer_size){
        bool split = false;
        Slice tkey(key_from_skt_df.data(), key_from_skt_df.size());
        /* 
         * Check following conditions 
         * 1. It is last key
         * 1. The size of the SKTable exceeds kMaxSKTableSize.
         * 2. Prefix is different or not in pre-determined prefix size.
         * 3. Conflict hash value within the SKTable
         * Future works
         * 1. Hot/Cold based Split
         *   : if next_uk is hot & prev keys(size must be "4K <" ) are cold, split the SKTable
         */
#ifdef CODE_TRACE
        if((buffer_size + skt_info_2_size) >= (max_buffer_size - (kMinSKTableSize>>2)) )
            printf("[%d :: %s]size problem : buffer_size : %d || skt_info_2_size : %d || max_buffer_size : %d\n", __LINE__, __func__, buffer_size, skt_info_2_size, max_buffer_size);
#endif
        if(sktdf_data && (!next_uk || next_uk->Compare(tkey, mf) > 0)){//All the nodes in the sktdf must be scaned.
            /* Next UserKey exists in SKTDF */
            if((buffer_size + skt_info_2_size) >= (max_buffer_size - (kMinSKTableSize>>2)) || 
                    (buffer_size + skt_info_2_size) >= (kMaxSKTableSize - (kMinSKTableSize>>2)) || 
                    SplitByPrefix(tkey, cur_key_slice)){
                /* Need to load next df_ukey */
                if(key_from_skt_df.size()>kMaxKeySize) abort();
                memcpy(dfb_info->begin_key_buffer, key_from_skt_df.data(), key_from_skt_df.size());
                dfb_info->begin_key = KeySlice(dfb_info->begin_key_buffer, key_from_skt_df.size());
#ifdef CODE_TRACE
                unsigned char print_begin_key[dfb_info->begin_key.size()+1];
                memcpy(print_begin_key, dfb_info->begin_key.data(), dfb_info->begin_key.size());
                print_begin_key[dfb_info->begin_key.size()] = '\0';
                printf("[%d :: %s] worker %u (main SKTID : %d)next skt begin key From 1st : %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\n", __LINE__, __func__, dfb_info->worker_id, GetSKTableID(),
                        print_begin_key[0], print_begin_key[1], print_begin_key[2], print_begin_key[3], print_begin_key[4], print_begin_key[5], print_begin_key[6], print_begin_key[7],
                        print_begin_key[8], print_begin_key[9], print_begin_key[10], print_begin_key[11], print_begin_key[12], print_begin_key[13], print_begin_key[14], print_begin_key[15]);
#endif
                split = true;
            }
        }else if(next_uk){
            /* next_uk is the Next UserKey */
            if(IsWritable(mf, next_uk, dfb_info->cur_time) && 
                    ((buffer_size + skt_info_2_size) >= (max_buffer_size - (kMinSKTableSize>>2)) || 
                     (buffer_size + skt_info_2_size) >= (kMaxSKTableSize - (kMinSKTableSize>>2)) || 
                     (SplitByPrefix(next_uk->GetKeySlice(), cur_key_slice)))){
                KeySlice key = next_uk->GetKeySlice();
                if(key.size()>kMaxKeySize) abort();
                memcpy(dfb_info->begin_key_buffer, key.data(), key.size());
                dfb_info->begin_key = KeySlice(dfb_info->begin_key_buffer, key.size());
#ifdef CODE_TRACE
                unsigned char print_begin_key[dfb_info->begin_key.size()+1];
                memcpy(print_begin_key, dfb_info->begin_key.data(), dfb_info->begin_key.size());
                print_begin_key[dfb_info->begin_key.size()] = '\0';
                printf("[%d :: %s] worker %u (main SKTID : %d)next skt begin key From 2nd : %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\n", __LINE__, __func__, dfb_info->worker_id, GetSKTableID(),
                        print_begin_key[0], print_begin_key[1], print_begin_key[2], print_begin_key[3], print_begin_key[4], print_begin_key[5], print_begin_key[6], print_begin_key[7],
                        print_begin_key[8], print_begin_key[9], print_begin_key[10], print_begin_key[11], print_begin_key[12], print_begin_key[13], print_begin_key[14], print_begin_key[15]);
#endif
                split = true;
            }
            else {
                if((buffer_size + skt_info_2_size) > kMaxSKTableSize) abort();
                if ((buffer_size + skt_info_2_size) >= (kMaxSKTableSize - (kMinSKTableSize>>2))) {
                    if (sktdf_data && next_uk->Compare(tkey, mf) == 0) {
                    /*
                     * In this case, we can split with key_from_skt_df, at least we have old valid key.
                     */
                        if(key_from_skt_df.size()>kMaxKeySize) abort();
                        memcpy(dfb_info->begin_key_buffer, key_from_skt_df.data(), key_from_skt_df.size());
                        dfb_info->begin_key = KeySlice(dfb_info->begin_key_buffer, key_from_skt_df.size());
                    } else {
                        /*
                         * It will be better stop flushing here and keep remains as next FlushSKTableMem.
                         * For the safty .... increase approximate count 
                         */
#if 0
                        UserKey* check_uk = next_uk;
                        while(check_uk) {
                            dfb_info->target_skt->IncApproximateDirtyColumnCount();
                            check_uk = keymap_.Next(check_uk);
                        }
#else
                        dfb_info->target_skt->SetPrevDirtyColumnCount(dfb_info->target_skt->GetKeyMap()->GetCount());
#endif
                        dfb_info->begin_key = KeySlice(0);
                    }
                    split = true;
                }
            }
        }else{/* !sktdf_data && !next_uk */
#ifdef CODE_TRACE
            printf("[%d :: %s]No more key\n", __LINE__, __func__);
#endif
            dfb_info->begin_key = KeySlice(0);
            split = true;
        }
#ifdef CODE_TRACE
        if(dfb_info->begin_key.size()){
            char print_begin_key[dfb_info->begin_key.size()+1];
            memcpy(print_begin_key, dfb_info->begin_key.data(), dfb_info->begin_key.size());
            print_begin_key[dfb_info->begin_key.size()] = '\0';
            printf("[%d :: %s](main SKTID : %d)next skt begin key : %p\n", __LINE__, __func__, GetSKTableID(), print_begin_key);
        }
#endif
        return split;
    }
#endif
    /////////////////////////////////////////////////////////////////////// 
    void  SKTableMem::DeviceFormatBuilder(Manifest* mf, SKTableDFBuilderInfo *dfb_info, bool sub_skt)
    {
        UserKey *next_uk = dfb_info->ukey;
        SKTableMem *target = dfb_info->target_skt;
        dfb_info->begin_key = KeySlice(0);
        KeySlice beginkey_for_prefix = target->GetBeginKeySlice();
        /* 
         * For shared/non-shared key 
         * If it had the 0 size begin key(First SKTable), use next key for encoding.
         * The next key(actual begin key) should not be evicted.
         */
        std::string begin_key_of_target;
        begin_key_of_target.resize(0);

        /**
         * stored_key_count : Key count saved in SKTable(device format)
         */
        uint16_t stored_key_count = 0; 
        int32_t snap_index = 0;

        SimpleLinkedList* col_list = NULL;
        ColumnNode* col_node = NULL;
        bool iter = false;
        /* inactive_key is the key having no col_node. So, it can be deleted*/
        bool recent_col_node_deleted = false;
        bool drop_col_node = false;
        bool active_key = false;


        Slice uncomp_slice = mf->AllocSKTableBuffer(kMinSKTableSize);// It will be inserted to DFSKTable cache.
        char* uncomp_buffer = const_cast<char*>(uncomp_slice.data());// It will be inserted to DFSKTable cache.
        if (!uncomp_buffer) abort();
        uint32_t uncomp_buffer_size = uncomp_slice.size();
        char* buffer = uncomp_buffer+SKTDF_BEGIN_KEY_OFFSET; 
        uint32_t buffer_size = SKTDF_BEGIN_KEY_OFFSET;
        uint16_t prev_diff = 0;
        uint32_t current_offset = 0;
        char *key_start = NULL;


        SKTDFCacheNode *skt_df_cache = dfb_info->skt_df_cache; 
        std::string key_from_skt_df;
        Slice col_info_slice;
        std::string key_offset;
        std::queue<std::pair<ColumnNode*, uint8_t>> valid_col_nodes;

        int32_t comp;/* Key Comparison Result */
        //bool split = false; // check split using current key, not next key
        SequenceNumber largest_seq_num = 0;
        uint32_t compression_start_ofs = SKTDF_BEGIN_KEY_OFFSET;

        uint32_t cur_key_info_size = 0;
        uint32_t skt_info_2_size = 0;

        if(!skt_df_cache->GetKeyCount())
            dfb_info->sktdf_cur_key_offset = 0;
        else if(dfb_info->sktdf_cur_key_offset)
            col_info_slice = skt_df_cache->GetKeyWithOffset(dfb_info->sktdf_cur_key_offset, &key_from_skt_df, dfb_info->sktdf_next_key_offset);
        while(dfb_info->ukey || dfb_info->sktdf_cur_key_offset){
            active_key = false;
            /* 
             * comp  > 0 : The key in sktdf has not been fetched and has not been updated.
             * comp == 0 : The key in sktdf has been fetched. In this case, if the ukey is dirty, 
             *                    record new info. Otherwise, copy the col_info from sktdf
             * comp  < 0 : The key in sktdf is bigger than the ukey. it means the ukey is new key. 
             */
            assert(valid_col_nodes.empty());
            if(dfb_info->sktdf_cur_key_offset){
                if(dfb_info->ukey) {
                    KeySlice tkey(key_from_skt_df);
                    comp = dfb_info->ukey->Compare(tkey, mf);
                } else {
                    comp  = 1; //remained keys are not fetched. 
                }
            }else comp = -1;//the new key has been added to the end of the SKTable.



            if( comp > 0 || (comp == 0 && !dfb_info->ukey->IsDirty())){
                /***************************************** Check split condition with current key *****************************************/
                /* prev_diff(2) & cur key size(next_diff(2)) + shared/non-shared key size info(2/2) + max key size(no encoding case) + col info size*/
                cur_key_info_size = 8 + key_from_skt_df.size() + col_info_slice.size();
                skt_info_2_size = (begin_key_of_target.size() + sizeof(uint16_t)/*size info of begin_key_of_target*/) + dfb_info->key_offset.size() + sizeof(uint32_t)/*For current key*/;
                if(((buffer_size + cur_key_info_size + skt_info_2_size) > uncomp_buffer_size) && uncomp_buffer_size < kMaxSKTableSize ){
                    uncomp_buffer_size += kMinSKTableSize;
                    if (uncomp_buffer_size > kMaxSKTableSize) uncomp_buffer_size = kMaxSKTableSize;
                    uncomp_buffer = (char*)realloc(uncomp_buffer, uncomp_buffer_size);
                    if(!uncomp_buffer){
                        printf("[%d :: %s] Memory allocation Failure(size : %d)\n", __LINE__, __func__, uncomp_buffer_size);
                        abort();
                    }
                    uncomp_slice  = Slice(uncomp_buffer, uncomp_buffer_size);
                    buffer = uncomp_buffer + buffer_size;
                }
                if(((buffer_size + cur_key_info_size + skt_info_2_size) > uncomp_buffer_size)){
                    /* 
                     * TODO : Check SplitByPrefix(tkey, cur_key_slice); for first SKTable.
                     * check prefix for only for first SKTable 
                     * if the main skt is not the first sktable, the prefix must be same for this case(use key info in SKTDF)
                     */
                    memcpy(dfb_info->begin_key_buffer, key_from_skt_df.data(), key_from_skt_df.size());
                    dfb_info->begin_key = KeySlice(dfb_info->begin_key_buffer, key_from_skt_df.size());
                    goto out;
                }
                /***************************************** Check split condition done *****************************************/
                KeySlice tkey(key_from_skt_df);
                dfb_info->key_offset.append((char*)&buffer_size, sizeof(uint32_t));
                /* Store previous key info size in order to get prev key info */
                current_offset = buffer_size;
                EncodeFixed16(buffer, prev_diff);
                buffer+=2;
                buffer_size+=2;
                /* Reserve the space for the current key info size in order to get next key info. */
                key_start = buffer;
                buffer+=2;/*sizeof(uint16_t)*/
                buffer_size+=2;
                /*if comp is 0(same key) and ukey is not dirty, the ukey may be fetched for Get cmd. */
                if(!begin_key_of_target.size()){
                    begin_key_of_target.assign(key_from_skt_df);
                    /////////////////// Key Encoding(shared/non-shared) /////////////////// 
                    if(key_from_skt_df.size())
                        buffer =  EncodeFirstKey(tkey, buffer, buffer_size);
                    else
                        buffer =  EncodeKey(buffer, buffer_size);
                    /*
                     * If the original begin key was deleted and the next is in the SKTDF only,
                     * The new next might be fetched in key deletion logic.
                     */
                } else {
                    buffer =  EncodeKey(begin_key_of_target, tkey, buffer, buffer_size);
                }

#ifdef CODE_TRACE
                printf("[%d :: %s](sktid :%d)Copy form DF(ukey : %p( %s ))(comp : %d || %s)\n", __LINE__, __func__, target->GetSKTableID(),  dfb_info->ukey, EscapeString(key_from_skt_df.data()).c_str(), comp, dfb_info->ukey? (dfb_info->ukey->IsDirty() ? "dirty" : "clean") : "NONE");
#endif

                /*
                 * if ukey is bigger than the key in SKTDF,
                 * Store the col_info_slice.
                 */

                ///////////// Copy Column Info from SKTable Device Format /////////////
                memcpy(buffer, col_info_slice.data(), col_info_slice.size());
                buffer += col_info_slice.size();
                buffer_size += col_info_slice.size();
                stored_key_count++;

                /* Store the size of this key info in the begining of the key low */
                prev_diff = (uint16_t)(buffer_size - current_offset);
                EncodeFixed16(key_start, prev_diff);
                /////////////////////////////////////////////////////////////////////// 
#ifdef CODE_TRACE
                {
                    if(col_info_slice.size() > 3){
                        Slice col_info = col_info_slice;
                        uint8_t nr_column = col_info[0];
                        col_info.remove_prefix(1);
                        uint8_t col_id = (col_info[0]);
                        col_info.remove_prefix(1);
                        uint8_t offset = col_info[0];
                        col_info.remove_prefix(1);
                        uint64_t ikey_seq_num;
                        GetVarint64(&col_info, &ikey_seq_num);
                        uint32_t ivalue_size;
                        GetVarint32(&col_info, &ivalue_size);
                        uint64_t seq;
                        if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT)
                            GetVarint64(&col_info, &seq);
                        uint64_t ttl;
                        if(offset & COLUMN_NODE_HAS_TTL_BIT) 
                            GetVarint64(&col_info, &ttl);
                        printf("[%d :: %s]COPY (ID : %d)nr_col : %d || col_id : %d || ikey : %ld || size : %d || offset : %d\n", __LINE__, __func__, skt_id_, nr_column, col_id, ikey_seq_num, ivalue_size, offset & COLUMN_NODE_OFFSET_MASK);
                    }
                }

#endif

                ///////////// Get Next Column info from SKT Device Format /////////////
                std::string cur_key_str(key_from_skt_df);
                KeySlice cur_key_slice = KeySlice(cur_key_str, tkey.GetHashValue());
                /////////////////////////// Move to Next Key /////////////////////////// 
                assert(dfb_info->sktdf_cur_key_offset);
                if(dfb_info->sktdf_next_key_offset){
                    dfb_info->sktdf_cur_key_offset = dfb_info->sktdf_next_key_offset;
                    col_info_slice = skt_df_cache->GetKeyWithOffset(dfb_info->sktdf_cur_key_offset, &key_from_skt_df, dfb_info->sktdf_next_key_offset);
                }else
                    dfb_info->sktdf_cur_key_offset = 0;
                ///////////////////////////////////////////////////////////////////////

                if(comp == 0){ 
                    std::shared_lock<folly::SharedMutex> l(keymap_mu_);
                    next_uk = keymap_.Next(dfb_info->ukey);
                }

#ifdef CODE_TRACE
                printf("[%d :: %s]this :%d | target : %d | ukey : %p\n", __LINE__, __func__, GetSKTableID(), target->GetSKTableID(), dfb_info->ukey);
#endif
                if(compression_start_ofs ==  SKTDF_BEGIN_KEY_OFFSET)
                    compression_start_ofs = buffer_size;

            }else{/* comp < 0 || (comp == 0 && ukey->IsDirty())*/

                /***************************************** Check split condition with current key *****************************************/
                bool need_to_split = false;
                /* prev_diff(2) & cur key size(next_diff(2)) + shared/non-shared key size info(2/2) + nr valid col(1) + max key size(no encoding case) + col info size*/
                /*col info size  = (nr valid col) *34((col_id(1) + iter seq(9) + keyblock offset(1) + ikey(9) + ivalue size(5) + ttl(9))*/
                cur_key_info_size = 9 + dfb_info->ukey->GetKeySize() + (kMaxColumnCount*34);
                skt_info_2_size = (begin_key_of_target.size() + sizeof(uint16_t)/*size info of begin_key_of_target*/) + dfb_info->key_offset.size() + sizeof(uint32_t)/*For current key*/;
                if(((buffer_size + cur_key_info_size + skt_info_2_size) > uncomp_buffer_size) && uncomp_buffer_size < kMaxSKTableSize ){
                    uncomp_buffer_size += kMinSKTableSize;
                    if (uncomp_buffer_size > kMaxSKTableSize) uncomp_buffer_size = kMaxSKTableSize;
                    uncomp_buffer = (char*)realloc(uncomp_buffer, uncomp_buffer_size);
                    if(!uncomp_buffer){
                        printf("[%d :: %s] Memory allocation Failure(size : %d)\n", __LINE__, __func__, uncomp_buffer_size);
                        abort();
                    }
                    uncomp_slice  = Slice(uncomp_buffer, uncomp_buffer_size);
                    buffer = uncomp_buffer + buffer_size;
                }
                bool dirty = false;
                const KeySlice key = dfb_info->ukey->GetKeySlice();
                if(((buffer_size + cur_key_info_size + skt_info_2_size) > uncomp_buffer_size)/*|| SplitByPrefix(beginkey_for_prefix, key)*/){
                    /* 
                     * TODO : Check SplitByPrefix(tkey, cur_key_slice); for first SKTable.
                     * check prefix for only for first SKTable 
                     * if the main skt is not the first sktable, the prefix must be same for this case(use key info in SKTDF)
                     */
                    need_to_split = true;
                //    memcpy(dfb_info->begin_key_buffer, key_from_skt_df.data(), key_from_skt_df.size());
                //    dfb_info->begin_key = KeySlice(dfb_info->begin_key_buffer, key_from_skt_df.size());
                }
                /***************************************** Check split condition done *****************************************/


                /* To adjust update count */
                /*
                 * if Dirty was set in this function because of iterator, the SKT DF node may same 
                 * But, the SKT DF may not reuse for simple implementation
                 */
                if(!need_to_split){

#if 0
                    while(!dfb_info->ukey->ClearDirty());
#else
                    dfb_info->ukey->ClearDirty();
#endif
                }

                if(comp == 0){ 
                    /* 
                     * Same key exists in SKT DF. 
                     * It means the key has been updated.
                     */
                    /* Fetch columns first before create col info */
                    if(!dfb_info->ukey->IsFetched()){
                        dfb_info->ukey->InsertColumnNodes(col_info_slice, HasBeenFlushed());
                        dfb_info->ukey->SetFetched();
                    }
                    std::lock_guard<folly::SharedMutex> l(sktdf_update_mu_);
                    /* UserKey exists, so this row will not be accessed */
                    dfb_info->skt_df_cache->ClearValidColumnCount(dfb_info->sktdf_cur_key_offset);
                }else{
                    /* 
                     * Same key does not exist in SKT DF.
                     * It means the key is newly inserted to the DB. 
                     */
                    dfb_info->ukey->SetFetched();
                }
                /* all columns in the Key has not been updated */
                std::queue<ColumnNode*> split_col_nodes;
                for( uint8_t col_id = 0 ; col_id < kMaxColumnCount ; col_id++){
                    snap_index = 0;
                    /*
                     * ColumnLock may not be needed 
                     * dfb_info->ukey->ColumnLock(col_id);
                     */
                    dfb_info->ukey->ColumnLock(col_id);
                    col_list = dfb_info->ukey->GetColumnList(col_id);
                    dfb_info->ukey->ColumnListLock(col_id);

                    col_node = NULL;
                    recent_col_node_deleted = false;
                    drop_col_node= false;
                    for(SimpleLinkedNode* col_list_node = col_list->newest(); col_list_node ; ) {
                        ColumnNode* valid_col_node = NULL;
                        assert(dfb_info->ukey->GetLatestColumnNode(col_id));

                        col_node = dfb_info->ukey->GetColumnNodeContainerOf(col_list_node);
                        /*
                                assert(valid_col_nodes.empty());
                         * col_node can be deleted.(col_list_node exists in col_node ).
                         * Therefore, keep next col_list_node first.
                         */
                        col_list_node = col_list->next(col_list_node);

                        /////////// Check whether the col_node must be deleted or not regardless of cmd type ///////////  
                        if(recent_col_node_deleted){
                            col_node->SetRecentColNodeyDeleted();
                            drop_col_node = true;
                        }

                        if(col_node->IsDropped()){
                            snap_index = dfb_info->nr_snap;
                            drop_col_node = true;
                        }
                        ////////////////////////////////////////////////////////////////////////////////////////////////  

                        /////////////////////////// Check Iterator /////////////////////////// 
                        iter = false;
                        while(snap_index < dfb_info->nr_snap && col_node->GetBeginSequenceNumber() <= dfb_info->snapinfo_list[snap_index].cseq ){
                            if(col_node->GetEndSequenceNumber() > dfb_info->snapinfo_list[snap_index].cseq && col_node->GetRequestType() == kPutType){
                                iter  = true;
                            }
                            ++snap_index;
                        }
                        ////////////////////////////////////////////////////////////////////// 

                        ///////////////// Check whether col_node has been submitted or not ///////////////// 
                        /* it can be droped before submission */
                        SequenceNumber ikey = col_node->GetInSDBKeySeqNum();
                        /*
                         * If ikey is 0, the col_node is in the request pending Q.
                         * Otherwise, the col_node submission is in progress by a WorkerThread.
                         */
                        if(!ikey){ 
                            /*In this case, approximate_dirty_col_cnt_ will be increased by other thread */
                            dirty = true;
#if 0
                            target->IncApproximateDirtyColumnCount();
#endif
                            continue; 
                        }
                        if( ikey < kUnusedNextInternalKey){
                            dirty = true;
                            /*In this case, approximate_dirty_col_cnt_ may/may not be increased by other thread */
                            /* 
                             * If another thread increases approximate_dirty_col_cnt_ for this column, count can be doubled
                             * But, it is expacted situation. The approximate_dirty_col_cnt_ is always set to 0 before flush.
                             */
                            target->IncApproximateDirtyColumnCount();
#ifdef CODE_TRACE
                            printf("[%d :: %s](sktid :%d)(skip for non-submitted column)ukey : %p(%s) || col_node addr %p || type : %s || nr_node in col : %d\n", __LINE__, __func__, target->GetSKTableID(), dfb_info->ukey, dfb_info->ukey->IsDirty() ? "dirty" : "clean", col_node, col_node->GetRequestType() == kPutType ? "Put" : "Del", col_list->Size());
#endif
                            continue;
                        }
                        //////////////////////////////////////////////////////////////////////////////////// 
                        //assert(!col_node->HasUserValue());
                        assert(!col_node->HasUserValue() || ikey == ULONG_MAX);
                        KeyBlockMeta* kbm = col_node->GetKeyBlockMeta();

                        if((!kbm && ikey != ULONG_MAX) || (kbm && kbm->IsDummy())) {
                            if (!kbm){
#ifdef CODE_TRACE    
                                printf("[%d :: %s]KBM is empty in FlushSKTableMem()->DeviceFormatBuilder()\n", __LINE__, __func__);
#endif
                                kbm =(KeyBlockMeta*) mf->GetKBPM(col_node);
                                kbm->DecRefCnt(); /* decrease additional reference */
                            }
                            assert(kbm);
                            while(kbm->SetWriteProtection());
                            if(kbm->IsDummy()) {
                                assert(kbm->GetKeyBlockMetaType() == kKeyBlockPrimary);
                                auto kbpm = (KeyBlockPrimaryMeta*)kbm;
                                assert(!kbpm->GetKBML());
                                KeyBlockMetaLog* kbml;
                                char *kbm_buf;
                                int kbm_buf_size;
                                kbpm->SetInSDBKeySeqNum(ikey);
#ifdef CODE_TRACE    
                                printf("[%d :: %s]Before LoadDeviceFormat in FlushSKTableMem()->DeviceFormatBuilder()\n", __LINE__, __func__);
#endif
                                Status s = kbpm->LoadDeviceFormat(mf, kbm_buf, kbm_buf_size);
                                if(!s.ok()) abort();
#ifndef NDEBUG
                                assert(kbpm->InitWithDeviceFormat(mf, kbm_buf, (uint32_t)kbm_buf_size, kbml));
#else
                                kbpm->InitWithDeviceFormat(mf, kbm_buf, (uint32_t)kbm_buf_size, kbml);
#endif
                                // KBML should not be created for normal dummy state KBM.
                                if(kbml) abort();
                                kbpm->FreeDeviceFormatBuffer(kbm_buf);
                                kbpm->ClearDummy();
                            }
                            kbm->ClearWriteProtection();
                        }

                        if(col_node->IsTRXNCommitted()){
                            ColumnStatus col_stat = CheckColumnStatus(mf, dfb_info, dfb_info->ukey, col_node, col_list_node ? dfb_info->ukey->GetColumnNodeContainerOf(col_list_node) : NULL/*next_col_node*/, col_id, kbm, iter, drop_col_node, recent_col_node_deleted, false);
                            /* Vertical Merged TRXNs are precessed at here*/
                            /* NRS has been decreased */
                            ///////////////////////////////////////////////////////////////////////////////////////////////////
                            if(col_stat == kValidColumn){
                                if(need_to_split){
                                    if(key.size()>kMaxKeySize) abort();

                                    // Clear flushed state from the begin key of next sktable
                                    ColumnNode* reset_col_node;
                                    while(!split_col_nodes.empty()) {
                                        reset_col_node = split_col_nodes.front();
                                        split_col_nodes.pop();
                                    }

                                    memcpy(dfb_info->begin_key_buffer, key.data(), key.size());
                                    dfb_info->begin_key = KeySlice(dfb_info->begin_key_buffer, key.size());
                                    dfb_info->ukey->ColumnListUnlock(col_id);
                                    dfb_info->ukey->ColumnUnlock(col_id);
                                    assert(valid_col_nodes.empty());
                                    goto out;
                                }
                                valid_col_node = col_node;
                            }else if(col_stat == kIterColumn){
                                dirty = true;
                                target->IncApproximateDirtyColumnCount();
                            }
                            ///////////////////////////////////////////////////////////////////////////////////////////////////
#ifndef NDEBUG
                            if (ikey != ULONG_MAX) assert(kbm == kbm->GetKBPM()); /* skip if ikey == ULONG_MAX */
#endif
#ifdef CODE_TRACE
                            printf("[%d :: %s](sktid :%d)(has been committed(previously nrs dec))ukey : %p(%s) ||col_node addr %p || type : %s || valid_col_node : %p\n", __LINE__, __func__, target->GetSKTableID(), dfb_info->ukey, dfb_info->ukey->IsDirty() ? "dirty" : "clean", col_node, col_node->GetRequestType() == kPutType ? "Put" : "Del", valid_col_node);
#endif

                        }else if(!col_node->IsTransaction() || mf->IsTrxnGroupCompleted(kbm->GetTrxnGroupID(col_node->GetRequestType(), col_node->GetKeyBlockOffset()))){
                            ColumnStatus col_stat;
                            assert(!kbm->IsDummy());
#ifdef CODE_TRACE
                            printf("[%d :: %s](sktid :%d)(decrease NRS)ukey : %p(%s)(ref : %d) ||col_node addr %p || type : %s\n", __LINE__, __func__, target->GetSKTableID(), dfb_info->ukey, dfb_info->ukey->IsDirty() ? "dirty" : "clean", dfb_info->ukey->GetReferenceCount(), col_node, col_node->GetRequestType() == kPutType ? "Put" : "Del");
#endif
                            /* Check or update KBM first(before DecNRSCount())*/
                            ///////////////////////////////////////////////////////////////////////////////////////////////////
                            col_stat = CheckColumnStatus(mf, dfb_info, dfb_info->ukey, col_node,  col_list_node ? dfb_info->ukey->GetColumnNodeContainerOf(col_list_node) : NULL/*next_col_node*/, col_id, kbm, iter, drop_col_node, recent_col_node_deleted, true);
                            ///////////////////////////////////////////////////////////////////////////////////////////////////
                            if(col_stat == kValidColumn && need_to_split){
                                if(key.size()>kMaxKeySize) abort();
                                memcpy(dfb_info->begin_key_buffer, key.data(), key.size());
                                dfb_info->begin_key = KeySlice(dfb_info->begin_key_buffer, key.size());
                                dfb_info->ukey->ColumnListUnlock(col_id);
                                dfb_info->ukey->ColumnUnlock(col_id);
                                assert(valid_col_nodes.empty());
                                goto out;
                            }
                            if(col_stat != kInvalidColumn){
                                col_node->SetTRXNCommitted();
                                KeyBlockPrimaryMeta* kbpm = kbm->GetKBPM();
                                assert(kbm != kbpm);
                                col_node->RemoveKeyBlockMeta();//Replace the KBML with KBPM
                                col_node->SetKeyBlockMeta(kbpm);
                            }
                            if(col_stat == kValidColumn){
                                valid_col_node = col_node;
                                while(kbm->SetWriteProtection());
                                ((KeyBlockMetaLog*)kbm)->UpdateTrxnInfo(col_node->GetKeyBlockOffset(), col_node->GetRequestType());
                                kbm->ClearWriteProtection(); 
                                if(!kbm->DecNRSCount()){
                                    if(!(mf->IncCleanableKBM() &0xFF))
                                        mf->LogCleanupSignal();
                                    /* 
                                     * Increase Cleanupable count 
                                     * If the count reaches a treshold, wake LogCleanupThread up 
                                     */
                                }
                            }else if(col_stat == kIterColumn){
                                dirty = true;
                                target->IncApproximateDirtyColumnCount();
                            }
                        }else{
                            /*
                             * KeyBlock has been flushed but the TRXN Group is not completed yet!
                             *  - In this case, other column nodes in the same KBM has not been flushed.
                             *   once all they are flushed it will be recorded to SKTable during flush.
                             *   The log thread can NOT change the KBM status to "outLog" 
                             *   before the column is written to the SKTable because the NRS still have positive value.
                             *   The column is written to the SKTable only if all trxns in Trxn Group has been flushed.
                             *   And the NRS count will be decreased during this time.
                             *   The NRS will be "0" after all column in the KBM are written to a SKTable.
                             */
                            dirty = true;
                            target->IncApproximateDirtyColumnCount();
#ifdef CODE_TRACE
                            printf("[%d :: %s](sktid :%d)(flushed but not committed)ukey : %p(%s) ||col_node addr %p || type : %s\n", __LINE__, __func__, target->GetSKTableID(), dfb_info->ukey, dfb_info->ukey->IsDirty() ? "dirty" : "clean", col_node, col_node->GetRequestType() == kPutType ? "Put" : "Del");
#endif
                        }
                        if(valid_col_node){
                            assert(!need_to_split);
                            valid_col_nodes.push(std::make_pair(valid_col_node, col_id));
                        }
                    }

                    if(!col_list->empty()) active_key = true;
                    /**
                     * The invalid list in a Column is not empty
                     */
                    dfb_info->ukey->ColumnListUnlock(col_id);
                    dfb_info->ukey->ColumnUnlock(col_id);
                }
                if(dirty) dfb_info->ukey->SetDirty();
                else if(need_to_split) dfb_info->ukey->ClearDirty();

                if(!valid_col_nodes.empty() /*|| !dfb_info->ukey->GetKeySize()*/){
                    assert(!need_to_split);
#ifdef CODE_TRACE
                    printf("[%d :: %s][Store Key(SKT ID : -%d-)Key] Key size : %ld(ukey : %p)\n", __LINE__, __func__, target->GetSKTableID(), dfb_info->ukey->GetKeySize(), dfb_info->ukey);
#endif
                    ColumnNode* valid_col_node = NULL;
                    SequenceNumber smallest_snap_seq = 0;
                    active_key  = true; //For zero size user key

                    dfb_info->key_offset.append((char*)&buffer_size, sizeof(uint32_t));
                    /* Store previous key info size in order to get prev key info */
                    current_offset = buffer_size;
                    EncodeFixed16(buffer, prev_diff);
                    buffer+=2;
                    buffer_size+=2;
                    key_start = buffer;
                    /* Reserve the space for the current key info size in order to get next key info. */
                    buffer+=2;/*sizeof(uint16_t)*/
                    buffer_size+=2;
                    if(!begin_key_of_target.size()){
                        begin_key_of_target.assign(key.data(), key.size()); 
                        if(dfb_info->ukey->GetKeySize()){
                            buffer =  EncodeFirstKey(key, buffer, buffer_size);
                        }else
                            buffer =  EncodeKey(buffer, buffer_size);
                        /////////////////// Key Encoding(shared/non-shared) /////////////////// 
                        /////////////////////////////////////////////////////////////////////// 
                    }else{
                        buffer =  EncodeKey(begin_key_of_target, key, buffer, buffer_size);
                        /* This is zero size key(begin key of the first SKTable) */
                    }

                    ////////////////// The number of Valid Columns //////////////////////// 
                    assert(valid_col_nodes.size() <= kMaxColumnCount);
                    *buffer = (uint8_t)valid_col_nodes.size();
                    buffer++;
                    buffer_size++;
                    while(!valid_col_nodes.empty()){
                        std::pair<ColumnNode*, uint8_t> valid = valid_col_nodes.front();
                        valid_col_nodes.pop();
                        valid_col_node = valid.first;

                        *buffer = valid.second;//Store column ID
                        buffer++;
                        buffer_size++;

                        uint8_t flag_and_offset = valid_col_node->GetKeyBlockOffset();
                        assert(!(flag_and_offset & 0xC0));//upper 2 bit are used for flags(seq_num & ttl)
                        uint64_t seq_num = valid_col_node->GetBeginSequenceNumber(); 
                        if( seq_num < smallest_snap_seq){
                            /* If col_node's seq # is less than smallest_snap_seq, set the seq # to "0" in order to reduce SKTable Device Format Size*/
                            valid_col_node->SetBeginSequenceNumber(0);
                            seq_num = 0;
                        }else{
                            flag_and_offset |= 1 << SEQ_NUM_SHIFT;
                        }
                        uint64_t ts = valid_col_node->GetTS();
                        if(ts) flag_and_offset |= 1 << TTL_SHIFT;
                        *buffer = flag_and_offset;
                        buffer++;
                        buffer_size++;
                        char* buf = NULL;
                        uint64_t ikey = valid_col_node->GetInSDBKeySeqNum();
                        /* Find largest ikey and send Device flush command with the ikey */
                        if(ikey > dfb_info->flush_ikey)
                            dfb_info->flush_ikey = ikey; 


                        buf = EncodeVarint64(buffer, ikey);
                        buffer_size+= (buf-buffer);
#ifdef CODE_TRACE
                        printf("[%d :: %s](SKTID : %d)ikey : %d || size : %d || offset : %d\n", __LINE__, __func__, skt_id_, ikey, valid_col_node->GetiValueSize(), valid_col_node->GetKeyBlockOffset());
#endif
                        buffer = EncodeVarint32(buf, valid_col_node->GetiValueSize());
                        buffer_size+= (buffer-buf);
                        if(seq_num){
                            buf = buffer;
                            buffer = EncodeVarint64(buf, seq_num);
                            buffer_size+= (buffer-buf);
                            if(largest_seq_num < seq_num)
                                largest_seq_num = seq_num;
                        } 
                        if(ts){
                          buf = buffer;
                          buffer = EncodeVarint64(buf, ts);
                          buffer_size+= (buffer-buf);
                        }
                    }
                    /* Store the size of this key info in the begining of the key low */
                    prev_diff = (uint16_t)(buffer_size - current_offset);
                    EncodeFixed16(key_start, prev_diff);
                    stored_key_count++;
                    if(compression_start_ofs == SKTDF_BEGIN_KEY_OFFSET)
                        compression_start_ofs = buffer_size;
                }
                ///////////// Get Next Column info from SKT Device Format /////////////
                if(comp == 0){
                    /* 
                     * The key has been updated. So, we're not going to use the key from skt device format. 
                     * But, if the column is not in col_list, we need to get KBPM(can not be KBML) and update the KBM.
                     * if iterator exists, the col_info_slice->ikey may not be same to last col in col_list.
                     * if it is not the same, it means the key has not been inserted into
                     */
                    /////////////////////////// Move to Next Key /////////////////////////// 
                    assert(dfb_info->sktdf_cur_key_offset);
                    if(dfb_info->sktdf_next_key_offset){
                        dfb_info->sktdf_cur_key_offset = dfb_info->sktdf_next_key_offset;
                        col_info_slice = skt_df_cache->GetKeyWithOffset(dfb_info->sktdf_cur_key_offset, &key_from_skt_df, dfb_info->sktdf_next_key_offset);
                    }else
                        dfb_info->sktdf_cur_key_offset = 0;
                    ///////////////////////////////////////////////////////////////////////
#ifdef CODE_TRACE
                    printf("[%d :: %s](sktid :%d){Get Next key in DF} ukey : %p \n", __LINE__, __func__, target->GetSKTableID(), dfb_info->ukey);
#endif
                }
                /////////////////////////////////////////////////////////////////////// 
                assert(dfb_info->ukey);
                {
                    std::shared_lock<folly::SharedMutex> l(keymap_mu_);
                    next_uk = keymap_.Next(dfb_info->ukey);
                } 

                /**
                 * Fill User Key Information after checking columns.
                 * All valid columns can be created after the flush is started.
                 * or all columns have only invalid ColumnNode.
                 * In both cases, do not store the user key
                 */
#if 1
                /* Evict/Delete keys in FlushSKTableMem() after create/update SKTDFs */
                if(!active_key){
                    /**
                     * In this UserKey, no valid internalkey exists and all invalid list is empty excep zero key. 
                     */
                    /* try_delete_ has not been set or some functions use this UserKey */
#ifdef CODE_TRACE
                    printf("[%d :: %s][Try to Delete Key(SKT ID : -%d-)Key] Key : %s || Key size : %ld\n", __LINE__, __func__, target->GetSKTableID(), EscapeString(dfb_info->ukey->GetKeySlice().data()).c_str(),dfb_info->ukey->GetKeySize());
#endif
                    assert(!dfb_info->ukey->IsDirty());
                    dfb_info->ukey->SetEvictionPrepare();
remove:
                    if( !dfb_info->ukey->GetReferenceCount()){
                        //////////////////// Fetch Next Key if the key is begin key //////////////////// 
                        /* 
                         * If begin key has not been set & the next key is not fetched
                         * fetch the next key & set it to begin key.
                         * the next key has not been updated.
                         * That means next key is valid.
                         */
                        if(!(dfb_info->ukey->Compare(target->GetBeginKeySlice(),mf)) && target->GetSKTableID() != 1/*First SKTableMem*/ ){
                            if(dfb_info->sktdf_cur_key_offset/* col_info_slice is next key within the range */){
                                KeySlice tkey(key_from_skt_df);
                                if(!next_uk || next_uk->Compare(tkey, mf) > 0){
                                    target->SetBeginKey(tkey);
                                }else
                                    target->SetBeginKey(next_uk->GetKeySlice());
                            }else if(next_uk){
                                target->SetBeginKey(next_uk->GetKeySlice());
                            }else if(!sub_skt){
                                /*For main*/
                                SetDeletedSKT();
                                mf->RemoveSKTableMemFromCacheAndSkiplist(this);
                            }else{
                                abort();/* Sub SKTable should have valid keys */
                            }

                        }

#ifdef CODE_TRACE
                        printf("[%d :: %s](SKTID : %d(%p))Removing UK(%p)\n", __LINE__, __func__, target->GetSKTableID(), target, dfb_info->ukey);
#endif
                        /* Delay to remove ukey from keymap_ in order to reduce getting write lock(lock_guard) */
#ifdef CODE_TRACE
                        printf("[%d :: %s]\nRemove From Hash : %p(data : %p : %ld)\n", __LINE__, __func__, dfb_info->ukey, dfb_info->ukey->GetKeySlice().data(), dfb_info->ukey->GetKeySlice().size());
                        UserKey *uk= keymap_.Search(dfb_info->ukey->GetKeySlice());
                        printf("[%d :: %s]Search From Keymap before remove : %p\n", __LINE__, __func__, uk/*, ukey->GetKeySlice().data(), dfb_info->ukey->GetKeySlice().size()*/);
                        uk= keymap_.First();
                        printf("[%d :: %s]First in Keymap before remove : %p\n", __LINE__, __func__, uk/*, ukey->GetKeySlice().data(), dfb_info->ukey->GetKeySlice().size()*/);
                        uk= keymap_.Search(uk->GetKeySlice());
                        printf("[%d :: %s]Search Using First Key Keymap before remove: %p\n", __LINE__, __func__, uk/*, ukey->GetKeySlice().data(), dfb_info->ukey->GetKeySlice().size()*/);
#endif
                        mf->RemoveUserKeyFromHashMapWrapper(dfb_info->ukey);
                        std::lock_guard<folly::SharedMutex> l(keymap_mu_);
                        if (keymap_.Remove(dfb_info->ukey->GetKeySlice()) == nullptr) abort();
#ifdef CODE_TRACE
                        uk= keymap_.Search(dfb_info->ukey->GetKeySlice());
                        printf("[%d :: %s]Search From Keymap after remove: %p\n", __LINE__, __func__, uk/*, ukey->GetKeySlice().data(), dfb_info->ukey->GetKeySlice().size()*/);
                        uk= keymap_.First();
                        printf("[%d :: %s]First in Keymap after remove : %p\n", __LINE__, __func__, uk/*, ukey->GetKeySlice().data(), dfb_info->ukey->GetKeySlice().size()*/);
                        uk= keymap_.Search(uk->GetKeySlice());
                        printf("[%d :: %s]Search Using First Key Keymap after remove: %p\n", __LINE__, __func__, uk/*, ukey->GetKeySlice().data(), dfb_info->ukey->GetKeySlice().size()*/);
#endif
                        dfb_info->ukey->UserKeyCleanup(mf, true);
                        mf->FreeUserKey(dfb_info->ukey);
                    }else{ 
#ifdef CODE_TRACE
                        printf("[%d :: %s] Check status before remove skipping for referenced ukey : %p(%s) colum empty : %s\n", __LINE__, __func__,
                                dfb_info->ukey, dfb_info->ukey->IsDirty() ? "dirty" : "clean", dfb_info->ukey->IsColumnEmpty(0) ? "Empty" : "Has column");
#endif
                        bool can_remove = true;
                        while(dfb_info->ukey->GetReferenceCount() && can_remove){

                            for( int col_id = 0 ; col_id < kMaxColumnCount  ; ++col_id){
                                //dfb_info->ukey->ColumnLock(col_id);
                                if(!dfb_info->ukey->IsColumnEmpty(col_id)){
                                    can_remove = false;
                                    target->IncApproximateDirtyColumnCount();
                                    dfb_info->ukey->SetDirty(); 
                                    break;

                                }
                                //dfb_info->ukey->ColumnUnlock(col_id);
                            }
                        }
                        if(can_remove) goto remove;
                        dfb_info->ukey->ClearEvictionPrepare(); 
#ifdef CODE_TRACE
                        printf("[%d :: %s] Remove skipped for referenced dfb_info->ukey : %p(%s) colum empty : %s\n", __LINE__, __func__,
                                dfb_info->ukey, dfb_info->ukey->IsDirty() ? "dirty" : "clean", dfb_info->ukey->IsColumnEmpty(0) ? "Empty" : "Has column");
#endif
                    }
                }
#if 0
                else{
                    /* The UserKey can not be evicted at here because the SKTDF has not been updated yet */
                    dfb_info->ukey->SetEvictionPrepare();
                    if(dfb_info->ukey->GetReferenceCount() || dfb_info->ukey->IsDirty()){
                        dfb_info->ukey->ClearEvictionPrepare();
                    }else{
                        /*begin key can not be evicted*/
                        RemoveUserKeyByName(dfb_info->ukey);
                        std::lock_guard<folly::SharedMutex> l(keymap_mu_);
                        if (keymap_.Remove(*(ukey->GetKeySlice())) == nullptr) abort();
                    }
                }
#endif
#endif
            }
            dfb_info->ukey = next_uk;
        }
out:

#ifdef CODE_TRACE
        char print_begin_key[dfb_info->begin_key.size()+1];
        memcpy(print_begin_key, dfb_info->begin_key.data(), dfb_info->begin_key.size());
        print_begin_key[dfb_info->begin_key.size()] = '\0';
        printf("[%d :: %s]split is %s(next begin : %p)\n", __LINE__, __func__, split?"true":"false", print_begin_key);
#endif
        if(stored_key_count || target->GetSKTableID() == 1){

            uint32_t skt_info_2_offset = buffer_size;
            assert(begin_key_of_target.size() < USHRT_MAX);
            if(uncomp_buffer_size  < buffer_size + begin_key_of_target.size() + stored_key_count*sizeof(uint32_t)) abort();
            EncodeFixed16(buffer, (uint16_t)begin_key_of_target.size());
            buffer += 2;
            buffer_size += 2;
            memcpy(buffer, begin_key_of_target.data(), begin_key_of_target.size());

            buffer += begin_key_of_target.size();
            buffer_size += begin_key_of_target.size();

            //printf("[%d :: %s]========================== Offset print START ========================\n", __LINE__, __func__);
            assert(dfb_info->key_offset.size() == stored_key_count*sizeof(uint32_t));

            memcpy(buffer, dfb_info->key_offset.data(), dfb_info->key_offset.size());
            buffer_size += dfb_info->key_offset.size();
            //printf("[%d :: %s]========================== Offset print End ========================\n", __LINE__, __func__);


            size_t inlen = buffer_size-compression_start_ofs;
            /*
               to keep uncompressed image
               - update SKT info on uncomp buffer by force.
               */
#ifdef CODE_TRACE
                printf("[%d :: %s](SKTID : %d)stored_key_count : %d || skt_info_2_offset : %d || compression_start_ofs : %d || inlen : %d || largest_seq_num : %ld\n", __LINE__, __func__,  target->GetSKTableID(), stored_key_count, skt_info_2_offset, compression_start_ofs, inlen, largest_seq_num);
#endif
            *((uint32_t *)(uncomp_buffer+SKTDF_NR_KEY_OFFSET)) = stored_key_count;
            *((uint32_t *)(uncomp_buffer+SKTDF_INFO_2_OFFSET)) = skt_info_2_offset;
            *((uint32_t *)(uncomp_buffer+SKTDF_COMP_START_OFFSET)) = compression_start_ofs;
            *((uint32_t *)(uncomp_buffer+SKTDF_SKT_SIZE_OFFSET)) = inlen;
            *((uint64_t *)(uncomp_buffer+SKTDF_LARGEST_SEQ_NUM)) = largest_seq_num;

            Slice comp_slice = mf->AllocSKTableBuffer(SizeAlignment(snappy::MaxCompressedLength(buffer_size)));  // It will be freed after submitting SKTable.
            char* comp_buffer = const_cast<char*>(comp_slice.data());  // It will be freed after submitting SKTable.
#ifdef HAVE_SNAPPY
            size_t outlen = 0;
            snappy::RawCompress(uncomp_buffer+compression_start_ofs, inlen, comp_buffer+compression_start_ofs, &outlen);

            /*
             * compressed less than 12.5%, just store uncompressed form 
             * This ratio is the same as that of LevelDB/RocksDB
             *
             * Regardless of the compression efficiency, We may not have any penalty even if we store compressed data.
             * But read have to decompress the data.
             */



            if(outlen < (inlen-(inlen / 8u))){
                assert(skt_info_2_offset < SKTDFCacheNode::flag_compression);
                skt_info_2_offset |= SKTDFCacheNode::flag_compression;
#ifdef CODE_TRACE
                printf("[%d :: %s](SKTID : %d)comp|| inlen : %ld || outlen : %ld || compression_start_ofs : %d || align : %ld\n", __LINE__, __func__,  target->GetSKTableID(), inlen, outlen, compression_start_ofs, SizeAlignment(outlen+compression_start_ofs));
#endif
                *((uint32_t *)(comp_buffer+SKTDF_NR_KEY_OFFSET)) = stored_key_count;
                *((uint32_t *)(comp_buffer+SKTDF_INFO_2_OFFSET)) = skt_info_2_offset;
                *((uint32_t *)(comp_buffer+SKTDF_COMP_START_OFFSET)) = compression_start_ofs;
                *((uint32_t *)(comp_buffer+SKTDF_SKT_SIZE_OFFSET)) = outlen;
                *((uint64_t *)(comp_buffer+SKTDF_LARGEST_SEQ_NUM)) = largest_seq_num;
                memcpy(comp_buffer + SKTDF_BEGIN_KEY_OFFSET, uncomp_buffer + SKTDF_BEGIN_KEY_OFFSET, compression_start_ofs - SKTDF_BEGIN_KEY_OFFSET);
                uint32_t comp_data_size = SizeAlignment(outlen+compression_start_ofs);
                if(comp_slice.size() < comp_data_size){
                    abort();
                }else if(comp_slice.size() > comp_data_size){
                    char* data = (char*)realloc(comp_buffer, comp_data_size);
                    if(!data){
                        printf("[%d :: %s] Memory allocation Failure(size : %d)\n", __LINE__, __func__, comp_data_size);
                        abort();
                    }
                    comp_slice  = Slice(data, comp_data_size);
                }
                dfb_info->skt_list.push_back(std::make_tuple(target, comp_slice, true));
                target->SetSKTDFNodeSize(comp_slice.size());
                dfb_info->skt_comp_slice.push_back(comp_slice);
            }else
#endif 
            {

#ifdef CODE_TRACE
                printf("[%d :: %s](SKTID : %d)uncomp|| inlen : %d || compression_start_ofs : %d || align : %ld\n", __LINE__, __func__, target->GetSKTableID(), inlen, compression_start_ofs, SizeAlignment(outlen+compression_start_ofs));
#endif
                uint32_t uncomp_data_size = SizeAlignment(inlen+compression_start_ofs);
                if(uncomp_slice.size() < uncomp_data_size){
                    abort();
                }else if(uncomp_slice.size() > uncomp_data_size){
                    char* data = (char*)realloc(uncomp_buffer, uncomp_data_size);
                    if(!data){
                        printf("[%d :: %s] Memory allocation Failure(size : %d)\n", __LINE__, __func__, uncomp_data_size);
                        abort();
                    }
                    uncomp_slice  = Slice(data, uncomp_data_size);
                }
                dfb_info->skt_list.push_back(std::make_tuple(target, uncomp_slice, false));
                target->SetSKTDFNodeSize(uncomp_slice.size());
                mf->FreeSKTableBuffer(comp_slice);/*Don't use compression buffer */
            }


            if(sub_skt){
                target->SetSKTableDeviceFormatNode(mf, new SKTDFCacheNode(dfb_info->mf, target, uncomp_slice, true));

            }else{
                /* 
                 * For main SKTable, sktdf should be updated after inserting all sub-sktables 
                 * user may try to read th device format during flush.
                 */
#ifdef CODE_TRACE
                printf("[%d :: %s]New df created sktable skt_id(%d)\n", __LINE__, __func__, target->GetSKTableID());
#endif
                dfb_info->new_skt_df_cache = uncomp_slice;

            }
        }else{
            /* The SKTable should be deleted because it has no keys.*/
            if(!sub_skt){
                assert(buffer_size == SKTDF_BEGIN_KEY_OFFSET);
                *((uint32_t *)(uncomp_buffer+SKTDF_NR_KEY_OFFSET)) = stored_key_count;
                *((uint32_t *)(uncomp_buffer+SKTDF_INFO_2_OFFSET)) = 0;
                *((uint32_t *)(uncomp_buffer+SKTDF_COMP_START_OFFSET)) = 0;
                *((uint32_t *)(uncomp_buffer+SKTDF_SKT_SIZE_OFFSET)) = 0;
                *((uint64_t *)(uncomp_buffer+SKTDF_LARGEST_SEQ_NUM)) = 0;

                uint32_t uncomp_data_size = SizeAlignment(SKTDF_BEGIN_KEY_OFFSET);
                if(uncomp_slice.size() < uncomp_data_size){
                    abort();
                }else if(uncomp_slice.size() > uncomp_data_size){
                    char* data = (char*)realloc(uncomp_buffer, uncomp_data_size);
                    if(!data){
                        printf("[%d :: %s] Memory allocation Failure(size : %d)\n", __LINE__, __func__, uncomp_data_size);
                        abort();
                    }
                    uncomp_slice  = Slice(data, uncomp_data_size);
                }

                dfb_info->skt_list.push_back(std::make_tuple(target, uncomp_slice, false));
#ifdef CODE_TRACE
                printf("[%d :: %s]New df created for deleted sktable skt_id(%d)\n", __LINE__, __func__, target->GetSKTableID());
#endif
                dfb_info->new_skt_df_cache = uncomp_slice;
            } else {
                mf->FreeSKTableBuffer(uncomp_slice);/*Don't use compression buffer */
            }
        }
    }

} //name space insdb
