/*
 *
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
#include <vector>
#include <functional>
#include <algorithm>
#include <set>
#include <thread>
#include "insdb/db.h"
#include "insdb/status.h"
#include "insdb/comparator.h"
#include "db/insdb_internal.h"
#include "db/skiplist.h"
#include "db/keyname.h"
#include "db/dbformat.h"
#include "db/keyname.h"
#include "port/port.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"
namespace insdb {
#if 0 /* move to dbformat.h */
    // Max request size
    const int kDeviceRquestMaxSize= 0x200000; // 2MB
    // Max User Thread Count
    const int kMaxUserCount= 1024;
    // Max  Worker Count
    const int kMaxWriteWorkerCount= 64;
    // Max Column Family count
    const int kUnusedNextInternalKey = 1088; //kMaxUserCount + kMaxWriteWorkerCount
#endif

    // Free buffer size
    //  - free_request_node_
    //  - free_uservalue_
    //  - free_skt_buffer_
    uint64_t kFreeBufferSize = 128UL;

    int kMaxSKTableSize= 512*1024;

    int kMaxColumnCount = 1;
    // Write Worker count
    int kWriteWorkerCount = 8;
    // Max request size
    uint32_t kMaxRequestSize = 4096;
    //  Max Key Size 
    int kMaxKeySize = 40;
    const uint16_t kMaxAllocatorPool = 8;
    //  Max preferred User value size
    int64_t kMaxCacheSize = 16*1024*1024*1024UL;
    int64_t kCacheSizeLowWatermark = 0;/*Set it to 80% of kMaxCacheSize in Manifest*/
    // Max threshold size to flush
    //Minimun update count for triggering SKTableMem Flush
    uint32_t  kSlowdownTrigger = 64*1024;
    uint32_t  kSlowdownLowWaterMark = 4*1024;
    uint32_t  kMinUpdateCntForTableFlush = (kMaxSKTableSize*17UL)/512;
    uint32_t  kSKTableFlushWatermark = kMinUpdateCntForTableFlush << 1;
    /* the kRequestAlignSize must be power of two*/
#ifdef NEW_GENERIC_FIRMWARE
    int kRequestAlignSize = 4; /**  */
#else
    int kRequestAlignSize = 4096; /**  */
#endif

    // KeyPrefixSize
    int kPrefixIdentifierSize = 0;
    uint64_t kCongestionControl = 0x800000; // 256*1024;

    // Share IterPad
    bool kIterPadShare = false;
    // Keep Written Block
    bool kKeepWrittenKeyBlock = true;


#ifdef INSDB_GLOBAL_STATS
    std::atomic<uint64_t> g_new_ikey_cnt;
    std::atomic<uint64_t> g_del_ikey_cnt;
    std::atomic<uint64_t> g_new_uservalue_io_cnt;
    std::atomic<uint64_t> g_del_uservalue_io_cnt;
    std::atomic<uint64_t> g_new_uservalue_cache_cnt;
    std::atomic<uint64_t> g_del_uservalue_cache_cnt;
    std::atomic<uint64_t> g_new_ukey_cnt;
    std::atomic<uint64_t> g_del_ukey_cnt;
    std::atomic<uint64_t> g_new_col_cnt;
    std::atomic<uint64_t> g_del_col_cnt;
    std::atomic<uint64_t> g_new_skt_cnt;
    std::atomic<uint64_t> g_del_skt_cnt;
    std::atomic<uint64_t> g_new_keymap_cnt;
    std::atomic<uint64_t> g_del_keymap_cnt;
    std::atomic<uint64_t> g_new_arena_cnt;
    std::atomic<uint64_t> g_del_arena_cnt;
    std::atomic<uint64_t> g_new_snap_cnt;
    std::atomic<uint64_t> g_del_snap_cnt;
    std::atomic<uint64_t> g_new_itr_cnt;
    std::atomic<uint64_t> g_del_itr_cnt;
    std::atomic<uint64_t> g_pin_uv_cnt;
    std::atomic<uint64_t> g_unpin_uv_cnt;
    std::atomic<uint64_t> g_pinslice_uv_cnt;
    std::atomic<uint64_t> g_unpinslice_uv_cnt;
    std::atomic<uint64_t> g_new_slnode_cnt;
    std::atomic<uint64_t> g_del_slnode_cnt;
    std::atomic<uint64_t> g_api_merge_cnt;
    std::atomic<uint64_t> g_api_put_cnt;
    std::atomic<uint64_t> g_api_delete_cnt;
    std::atomic<uint64_t> g_api_get_cnt;
    std::atomic<uint64_t> g_api_itr_seek_cnt;
    std::atomic<uint64_t> g_api_itr_next_cnt;
    std::atomic<uint64_t> g_api_itr_key_cnt;
    std::atomic<uint64_t> g_api_itr_value_cnt;
    std::atomic<uint64_t> g_api_itr_value_ikey_cnt;
    std::atomic<uint64_t> g_api_itr_value_dup_cnt;
    std::atomic<uint64_t> g_findikey_in_invalidlist_cnt;
    std::atomic<uint64_t> g_findikey_loop_in_invalidlist_cnt;
    std::atomic<uint64_t> g_api_itr_move_nullikey_cnt;
    std::atomic<uint64_t> g_api_write_ttl_cnt;
    std::atomic<uint64_t> g_skiplist_newnode_retry_cnt;
    std::atomic<uint64_t> g_skiplist_nextwithnode_retry_cnt;
    std::atomic<uint64_t> g_skiplist_prevwithnode_retry_cnt;
    std::atomic<uint64_t> g_skiplist_estimate_retry_cnt;
    std::atomic<uint64_t> g_skiplist_find_prevandsucess_retry_cnt;
    std::atomic<uint64_t> g_skiplist_insert_retry_cnt;
    std::atomic<uint64_t> g_skiplist_search_retry_cnt;
    std::atomic<uint64_t> g_skiplist_searchlessthanequal_retry_cnt;
    std::atomic<uint64_t> g_skiplist_searchgreaterequal_retry_cnt;
#endif

#ifdef TRACE_READ_IO
    std::atomic<uint64_t> g_sktable_sync;
    std::atomic<uint64_t> g_sktable_sync_hit;
    std::atomic<uint64_t> g_sktable_async;
    std::atomic<uint64_t> g_kb_sync;
    std::atomic<uint64_t> g_kb_sync_hit;
    std::atomic<uint64_t> g_kb_async;
    std::atomic<uint64_t> g_itr_kb;
    std::atomic<uint64_t> g_itr_kb_prefetched;
    std::atomic<uint64_t> g_kbm_sync;
    std::atomic<uint64_t> g_kbm_sync_hit;
    std::atomic<uint64_t> g_kbm_async;
    std::atomic<uint64_t> g_loadvalue_column;
    std::atomic<uint64_t> g_loadvalue_sktdf;
#endif

#ifdef MEASURE_WAF
    std::atomic<uint64_t> g_acc_appwrite_keyvalue_size;
    std::atomic<uint64_t> g_acc_devwrite_keyvalue_size;
#endif

#ifdef TRACE_MEM
    std::atomic<uint64_t> g_unloaded_skt_cnt;
    std::atomic<uint64_t> g_io_cnt;
    std::atomic<uint64_t> g_evicted_key;
    std::atomic<uint64_t> g_evicted_skt;
    std::atomic<uint64_t> g_evict_dirty_canceled;
    std::atomic<uint64_t> g_evict_skt_busy_canceled;
    std::atomic<uint64_t> g_evict_skt_next_deleted_canceled;
    std::atomic<uint64_t> g_prefetch_after_evict;
    std::atomic<uint64_t> g_evict_call_cnt;
    std::atomic<uint64_t> g_skt_loading;
    std::atomic<uint64_t> g_skt_flushing;
    std::atomic<uint64_t> g_hit_df_skt;
    std::atomic<uint64_t> g_miss_df_skt;
    std::atomic<uint64_t> g_insert_df_skt;
    std::atomic<uint64_t> g_evict_df_skt;
#ifdef KV_TIME_MEASURE
    std::atomic<uint64_t> g_flush_cum_time;
    std::atomic<uint64_t> g_flush_start_count;
    std::atomic<uint64_t> g_flush_end_count;
    std::atomic<uint64_t> g_flush_max_time;
    std::atomic<uint64_t> g_evict_cum_time;
    std::atomic<uint64_t> g_evict_count;
    std::atomic<uint64_t> g_evict_max_time;
    std::atomic<uint64_t> g_write_io_flush_cum_time;
    std::atomic<uint64_t> g_write_io_flush_count;
    std::atomic<uint64_t> g_write_io_flush_max_time;
    std::atomic<uint64_t> g_skt_flush_cum_time;
    std::atomic<uint64_t> g_skt_flush_count;
    std::atomic<uint64_t> g_skt_flush_max_time;
#endif
#endif

#ifdef INSDB_GLOBAL_STATS
    void printGlobalStats(const char *header)
    {
        printf("\n%s\n",header);
        printf("g_new_ikey_cnt %lu\n", g_new_ikey_cnt.load());
        printf("g_del_ikey_cnt %lu\n", g_del_ikey_cnt.load());
        printf("g_new_uservalue_io_cnt %lu\n", g_new_uservalue_io_cnt.load());
        printf("g_del_uservalue_io_cnt %lu\n", g_del_uservalue_io_cnt.load());
        printf("g_new_uservalue_cache_cnt %lu\n", g_new_uservalue_cache_cnt.load());
        printf("g_del_uservalue_cache_cnt %lu\n", g_del_uservalue_cache_cnt.load());
        printf("g_new_ukey_cnt %lu\n", g_new_ukey_cnt.load());
        printf("g_del_ukey_cnt %lu\n", g_del_ukey_cnt.load());
        printf("g_new_col_cnt %lu\n", g_new_col_cnt.load());
        printf("g_del_col_cnt %lu\n", g_del_col_cnt.load());
        printf("g_new_skt_cnt %lu\n", g_new_skt_cnt.load());
        printf("g_del_skt_cnt %lu\n", g_del_skt_cnt.load());
        printf("g_new_keymap_cnt %lu\n", g_new_keymap_cnt.load());
        printf("g_del_keymap_cnt %lu\n", g_del_keymap_cnt.load());
        printf("g_new_arena_cnt %lu\n", g_new_arena_cnt.load());
        printf("g_del_arena_cnt %lu\n", g_del_arena_cnt.load());
        printf("g_new_snap_cnt %lu\n", g_new_snap_cnt.load());
        printf("g_del_snap_cnt %lu\n", g_del_snap_cnt.load());
        printf("g_new_itr_cnt %lu\n", g_new_itr_cnt.load());
        printf("g_del_itr_cnt %lu\n", g_del_itr_cnt.load());
        printf("g_pin_uv_cnt %lu\n", g_pin_uv_cnt.load());
        printf("g_unpin_uv_cnt %lu\n", g_unpin_uv_cnt.load());
        printf("g_pinslice_uv_cnt %lu\n", g_pinslice_uv_cnt.load());
        printf("g_unpinslice_uv_cnt %lu\n", g_unpinslice_uv_cnt.load());
        printf("g_new_slnode_cnt %lu\n", g_new_slnode_cnt.load());
        printf("g_del_slnode_cnt %lu\n", g_del_slnode_cnt.load());
        printf("g_api_merge_cnt %lu\n", g_api_merge_cnt.load());
        printf("g_api_put_cnt %lu\n", g_api_put_cnt.load());
        printf("g_api_delete_cnt %lu\n", g_api_delete_cnt.load());
        printf("g_api_get_cnt %lu\n", g_api_get_cnt.load());
        printf("g_api_itr_seek_cnt %lu\n", g_api_itr_seek_cnt.load());
        printf("g_api_itr_next_cnt %lu\n", g_api_itr_next_cnt.load());
        printf("g_api_itr_key_cnt %lu\n", g_api_itr_key_cnt.load());
        printf("g_api_itr_value_cnt %lu\n", g_api_itr_value_cnt.load());
        printf("g_api_itr_value_ikey_cnt %lu\n", g_api_itr_value_ikey_cnt.load());
        printf("g_api_itr_value_dup_cnt %lu\n", g_api_itr_value_dup_cnt.load());
        printf("g_findikey_in_invalidlist_cnt %lu\n", g_findikey_in_invalidlist_cnt.load());
        printf("g_findikey_loop_in_invalidlist_cnt %lu\n", g_findikey_loop_in_invalidlist_cnt.load());
        printf("g_api_itr_move_nullikey_cnt %lu\n", g_findikey_loop_in_invalidlist_cnt.load());
    }
#endif

    void RequestNode::UpdateLatestColumnNode(Manifest *mf, uint32_t kb_size, uint64_t ikey_seq, KeyBlockPrimaryMeta *kbpm){
        UserKey *uk = NULL;
        if(latest_col_node_->GetTGID()) mf->DecTrxnGroup(latest_col_node_->GetTGID(), 1);/* AddCompletedTrxnGroupRange() is called in DecTrxnGroup() */
        do{
            /*uk can be changed by UserKeyMerge()*/
            if(uk) uk->ColumnUnlock(col_id_);
            uk = latest_col_node_->GetUserKey();
            uk->ColumnLock(col_id_);
        }while(uk != latest_col_node_->GetUserKey());

        /*Free UserValue in the UpdateColumnNode()*/
        latest_col_node_->UpdateColumnNode(mf, kb_size, ikey_seq, kbpm);
        latest_col_node_->SetSubmitted();
        uk->SetLatestiKey(ikey_seq);
        uk->DecreaseReferenceCount();

        uk->ColumnUnlock(col_id_);

    }

    int UserKeyComparator::operator()(const Slice& a, const Slice& b) const
    {
        //TODO TODO change length to -1 
        if (a.size() == 0 || b.size() == 0) {
            if (a.size() == 0 && b.size() == 0) return 0;
            if (a.size() == 0) return -1;
            if (b.size() == 0) return 1;
        }
        return user_comparator->Compare(a, b);
    }

    void DBRecoveryThread::ThreadFn() {
        uint32_t new_uk = 0;
        uint32_t removed_uk = 0;
        uint32_t replace_uk = 0;
        Env *env_ = recovery_->GetEnv();
        Manifest *mf_ = recovery_->GetManifest();
        std::list<TrxnGroupRange> &completed_trxn_groups_ = recovery_->GetCompletedTrxnGroup();

        // get the beginning ikey sequence of the worker
        // note that the beginning ikey must be the beginning of transaction group.
        SequenceNumber ikey_seq = recovery_->GetWorkerNextIKeySeq(worker_id_);

        while(true) {
            /*
             * Search KBPM from Hashmap and device.
             */
            KeyBlockPrimaryMeta *kbpm = mf_->FindKBPMFromHashMap(ikey_seq);
            if (!kbpm) {
                kbpm = mf_->AllocKBPM();
                kbpm->DummyKBPMInit(ikey_seq);
                kbpm->SetDummy();
                char kbm_buf[kDeviceRquestMaxSize];
                int kbm_buf_size = kDeviceRquestMaxSize;
                Status s = env_->Get(GetKBKey(mf_->GetDBHash(), kKBMeta, ikey_seq), kbm_buf, &kbm_buf_size);
                if(s.IsNotFound()) {
                    break;
                }
                else if(!s.ok()) {
                    printf("recovery(%u): cannot read key block meta. %s\n", worker_id_, s.ToString().data());
                    status_ = s;
                    return;
                }
                if (kbm_buf_size <= kOutLogKBMSize) {
                    kbpm->InitWithDeviceFormat(mf_, kbm_buf, (uint16_t)kbm_buf_size);
                } else {
                    char *inLogBuffer = (char*)malloc(kbm_buf_size); /* will be freed whenc FreeKBPM() */
                    assert(inLogBuffer);
                    memcpy(inLogBuffer, kbm_buf, kbm_buf_size);
                    kbpm->InitWithDeviceFormatForInLog(mf_, inLogBuffer, kbm_buf_size);
                }
                mf_->InsertKBPMToHashMap(kbpm); 
            } else if (kbpm->IsDummy()) {
                while(kbpm->SetWriteProtection());
                if (kbpm->IsDummy()) {
                    char kbm_buf[kDeviceRquestMaxSize];
                    int kbm_buf_size = kDeviceRquestMaxSize;
                    Status s = env_->Get(GetKBKey(mf_->GetDBHash(), kKBMeta, ikey_seq), kbm_buf, &kbm_buf_size);
                    if(s.IsNotFound()) {
                        kbpm->ClearWriteProtection();
                        break;
                    }
                    else if(!s.ok()) {
                        kbpm->ClearWriteProtection();
                        printf("recovery(%u): cannot read key block meta. %s\n", worker_id_, s.ToString().data());
                        status_ = s;
                        return;
                    }
                    if (kbm_buf_size <= kOutLogKBMSize) {
                        kbpm->InitWithDeviceFormat(mf_, kbm_buf, (uint16_t)kbm_buf_size);
                    } else {
                        char *inLogBuffer = (char*)malloc(kbm_buf_size); /* will be freed whenc FreeKBPM() */
                        assert(inLogBuffer);
                        memcpy(inLogBuffer, kbm_buf, kbm_buf_size);
                        kbpm->InitWithDeviceFormatForInLog(mf_, inLogBuffer, kbm_buf_size);
                    }
                }
                kbpm->ClearWriteProtection();
            }
            if (!kbpm->IsInlog()) {
                /* It may happen retry recover. and we may have already recovered this .. ignore */
                break;
            }

            bool rh_hit = false;
            // Read key block from the device
            std::list<ColumnInfo> orphan_col_list;
            if(kbpm->GetOriginalKeyCount()) {
                KeyBlock * kb = mf_->FindKBCache(ikey_seq);
                assert(kb);
                if (kb->IsDummy()) {
                    bool b_load = false;
                    kb->KBLock();
                    if (kb->IsDummy()) {
                        b_load = mf_->LoadValue(ikey_seq, kDeviceRquestMaxSize, &rh_hit, kb, true);
                        if (b_load) {
                            mf_->DummyKBToActive(ikey_seq, kb);
                        } else {
                            mf_->RemoveDummyKB(kb);
                        }
                    }
                    kb->KBUnlock();
                    if (!b_load) {
                        mf_->FreeKB(kb);
                        kb = nullptr;
                    }
                }
                if (!kb) {
                    kbpm->ClearAllKeyBlockBitmap();
                    /* may be power down befor writing kb */
                    printf("[%s:%d] not found skip for kb (%ld)!\n", __func__, __LINE__,  ikey_seq);
                    break;
                }
                // Decode key block device format
                // userkey_list receives user keys including valid and deleted user keys
                if (!kb->DecodeAllKeyMeta(kbpm, orphan_col_list/*, largest_seq_*/)) {
                    printf("recovery(%u): cannot decode key block\n", worker_id_);
                    status_ = Status::Corruption("cannot decode key block");
                    return;
                }
                mf_->FreeKB(kb);
            } else {
                if (kbpm->DecodeAllDeleteKeyMeta(orphan_col_list/*, largest_seq_ */)) {
                    printf("recovery(%u): cannot decode key block\n", worker_id_);
                    status_ = Status::Corruption("cannot decode key block");
                    return;
                }
            }

            if (ikey_seq > largest_ikey_) largest_ikey_ = ikey_seq; /* keep track largest ikey */
            // Recover keys in completed transaction groups
            for(auto colinfo : orphan_col_list){
                if (colinfo.iter_seq > largest_seq_) largest_seq_ = colinfo.iter_seq;
                TrxnInfo *trxninfo = colinfo.trxninfo;
                if (!trxninfo) {
                    /* non transaction columne */
                    recovery_->InsertRecoveredKeyToKeymap(mf_, colinfo, new_uk, removed_uk, replace_uk);
                    continue;
                }
                TrxnGroupID tgid = trxninfo->GetTrxnGroupID();
                assert(trxninfo->GetColId() < kMaxColumnCount);

                // If in completed transaction group, add the user key to keymap
                if(mf_->IsCompletedTrxnGroupRange(completed_trxn_groups_, tgid)) {
                    recovery_->InsertRecoveredKeyToKeymap(mf_, colinfo, new_uk, removed_uk, replace_uk);
                    delete trxninfo;
                } else {
                    // Collect column nodes and transactions to further inspect in the next step
                    // if the transaction is completed, free them.
                    bool trxn_completed = recovery_->AddCompletedColumn(UncompletedTRXNList_, trxninfo->GetTrxn().id, trxninfo->GetTrxn().count);
                    if(trxn_completed) {
                        // complete vertical-merged columns
                        uint16_t nr_merged_trxn = trxninfo->GetNrMergedTrxnList();
                        Transaction *merged_trxn = trxninfo->GetMergedTrxnList();
                        for(int i = 0; i < nr_merged_trxn; i++)
                            recovery_->AddCompletedColumn(UncompletedTRXNList_, merged_trxn[i].id, merged_trxn[i].count);
                        // free completed column and its transaction information
                        delete trxninfo;
                    } else {
                        // collect uncompleted column nodes.
                        UncompletedKeyList_.push_back(colinfo);
                    }
                }
            }
            // Get ikeu seq for next KBM
            ikey_seq = kbpm->GetNextIKeySeqNumFromDeviceFormat();
            if (ikey_seq > largest_ikey_) largest_ikey_ = ikey_seq; /* keep track largest ikey */
        } //while(true)
        new_uk_ = new_uk;
        removed_uk_ = removed_uk;
        replace_uk_ = replace_uk;
        status_ = Status::OK();
    }

    DBRecovery::DBRecovery(Manifest *mf) : mf_(mf), env_(mf_->GetEnv()),  rdb_mu_(), logkey_(mf->GetDBHash()) { }
    Status DBRecovery::LoadLogRecordKey() { return logkey_.Load(mf_->GetEnv()); }
    void DBRecovery::PrepareLoadForSKTable(std::vector<KEYMAP_HDR*> &keymap_hdrs, uint32_t sktid)
    {
        int size = 2048;
        char buffer[size];
        bool first = true;
        uint32_t max_used_skt_id = 0;
        uint32_t next_skt_id = sktid;
        while(next_skt_id) {
            //
            // read partial sktable to insert begin key
            //
            size = 2048;
            Status s = env_->Get(GetMetaKey(mf_->GetDBHash(), kSKTable, next_skt_id), buffer, &size);
            if(!s.ok())
                break;
            if(size < sizeof(KEYMAP_HDR)) {
                printf ("[%s:%d] sktable %d is invalid", __func__, __LINE__, next_skt_id);
                break;
            }

            //
            // parse necessary information ... ignore CRC checking.
            //
            KEYMAP_HDR* hdr = new KEYMAP_HDR();
            memcpy((void *)hdr, buffer, sizeof(KEYMAP_HDR));

            if (max_used_skt_id < hdr->prealloc_skt_id) max_used_skt_id = hdr->prealloc_skt_id;

            uint32_t keymap_size = SizeAlignment((hdr->keymap_size & KEYMAP_SIZE_MASK) + sizeof(KEYMAP_HDR));
            // send prefetch for mext processing.
            size = 2048;
            InSDBKey prekey = GetMetaKey(GetManifest()->GetDBHash(), kSKTable, hdr->prealloc_skt_id);  //prealloc
            GetEnv()->Get(prekey, nullptr, &size, kReadahead);

            size = 2048;
            prekey = GetMetaKey(GetManifest()->GetDBHash(), kSKTable, hdr->next_skt_id); //next_skt_id
            GetEnv()->Get(prekey, nullptr, &size, kReadahead);

            size = keymap_size;
            prekey = GetMetaKey(GetManifest()->GetDBHash(), kSKTable, hdr->skt_id); // current_skt_id
            GetEnv()->Get(prekey, nullptr, &size, kReadahead);

#ifdef USE_HASHMAP_RANDOMREAD
            size = hdr->hashmap_data_size;
            prekey = GetMetaKey(GetManifest()->GetDBHash(), kHashMap, hdr->skt_id); // current_skt_id
            GetEnv()->Get(prekey, nullptr, &size, kReadahead);
#endif
            if (hdr->start_colinfo != hdr->last_colinfo) {
                for(uint32_t colid = hdr->start_colinfo; colid < hdr->last_colinfo; colid++) {
                    prekey = GetColInfoKey(GetManifest()->GetDBHash(), hdr->skt_id, colid);
                    GetEnv()->Delete(prekey);
                }
            }
            
            // blindless prefetch first skt
            if (first) {
                first = false;
                for(uint32_t colid = hdr->last_colinfo; colid < (hdr->last_colinfo + 20); colid++) {
                    size = kDeviceRquestMaxSize;
                    prekey = GetColInfoKey(GetManifest()->GetDBHash(), hdr->skt_id, colid);
                    GetEnv()->Get(prekey, nullptr, &size, kReadahead);
                }
            }
            
            mf_->CleanupPreallocSKTable(hdr->prealloc_skt_id, hdr->next_skt_id); //remove prealloc and not finished splited sktable.
            keymap_hdrs.push_back(hdr);
            next_skt_id = hdr->next_skt_id;
        }
        /* update manifest's next sktable id */
        if (mf_->GetCurNextSKTableID() < max_used_skt_id) mf_->SetNextSKTableID(max_used_skt_id);
        printf("[%s:%d] -- Update skt id --\n\t new next skt id (%d)\n", __func__, __LINE__, max_used_skt_id);

        return;
    }

    Status Manifest::LoadColInfoFromDevice(uint32_t skt_id, uint32_t &cur_col_id, std::string &col_info) {
        Status s;
        char* dest = nullptr;
        col_info.resize(0);
        std::string io_buf;
        char buffer[kDeviceRquestMaxSize];
        char* cmp_buffer = nullptr;
        char* src = nullptr;
        int size = kDeviceRquestMaxSize;
        InSDBKey colkey = GetColInfoKey(dbhash_, skt_id, cur_col_id);
        s = env_->Get(colkey, buffer, &size, kSyncGet);
        if (!s.ok()) return s;
        uint32_t col_info_size = DecodeFixed32(buffer);
        if (col_info_size & KEYMAP_COMPRESS) {
            col_info_size = (col_info_size & KEYMAP_SIZE_MASK);
            if (col_info_size > kDeviceRquestMaxSize) {
                io_buf.append(buffer, size);
                uint16_t nr_cnt = (col_info_size + kDeviceRquestMaxSize -1) / kDeviceRquestMaxSize;
                for (uint16_t i = 1; i < nr_cnt; i++) {
                    int size = kDeviceRquestMaxSize;
                    cur_col_id++;
                    InSDBKey colkey = GetColInfoKey(dbhash_, skt_id, cur_col_id);
                    s = env_->Get(colkey, buffer, &size, kSyncGet);
                    if (!s.ok()) {
                        return s;
                    }
                    io_buf.append(buffer, size);
                }
                cmp_buffer = const_cast<char*>(io_buf.data());
            } else {
                cmp_buffer = buffer;
            }

            size_t ulength = 0;
            if (!port::Snappy_GetUncompressedLength(cmp_buffer + COMP_COL_INFO_TABLE_SIZE, col_info_size, &ulength)) {
                printf("[%d :: %s] corrupted compressed data\n", __LINE__, __func__);
                abort();
            } else {
                col_info.append(ulength, 0x0);
                dest = const_cast<char*>(col_info.data());
                if (!port::Snappy_Uncompress(cmp_buffer + COMP_COL_INFO_TABLE_SIZE, col_info_size, dest)) {
                    printf("[%d :: %s] corrupted compressed data\n", __LINE__, __func__);
                    abort();
                }
            }
        } else {
            if (col_info_size > kDeviceRquestMaxSize) {
                io_buf.append(buffer, size);
                uint16_t nr_cnt = (col_info_size + kDeviceRquestMaxSize -1) / kDeviceRquestMaxSize;
                for (uint16_t i = 1; i < nr_cnt; i++) {
                    int size = kDeviceRquestMaxSize;
                    cur_col_id++;
                    InSDBKey colkey = GetColInfoKey(dbhash_, skt_id, cur_col_id);
                    s = env_->Get(colkey, buffer, &size, kSyncGet);
                    if (!s.ok()) {
                        return s;
                    }
                    io_buf.append(buffer, size);
                }
                src = const_cast<char*>(io_buf.data());
            } else {
                src = buffer;
            }

            col_info.append(col_info_size, 0x0);
            dest = const_cast<char*>(col_info.data());
            memcpy(dest, src, col_info_size); 
        }
        uint32_t total_size = DecodeFixed32(dest);
        assert(total_size == col_info.size());
        uint32_t orig_crc = crc32c::Unmask(DecodeFixed32(dest + COL_INFO_TABLE_CRC_LOC));
        uint32_t actual_crc = crc32c::Value(dest + COL_INFO_TABLE_META, total_size - COL_INFO_TABLE_META);
        if (orig_crc != actual_crc) {
            printf("[%d :: %s] crc mismatch chek this orig(%d) and actual(%d)\n", __LINE__, __func__, orig_crc, actual_crc);
            abort();
        }
        return s;
    }


#ifdef INSDB_USE_HASHMAP
    void DBRecovery::UpdateColInfoToUk(std::string &col_info, KeyHashMap *key_hashmap, uint64_t &largest_ikey, uint64_t &largest_seq) {
        char* data = const_cast<char*>(col_info.data());
        KeyBlock * kb = nullptr;
        uint32_t size = col_info.size();
        data += COL_INFO_TABLE_META;
        size -= COL_INFO_TABLE_META;
        while(size) {
            kb = nullptr;
            KeySlice user_key(0);
            bool has_key = false;
            bool found_key = false;
           /* parse col info */ 
            KeyMeta* meta = (KeyMeta*)data;
            uint8_t nr_col = meta->NRColumn();
            data++;
            size--;
            assert(size >0);
            ColMeta* col = nullptr;
            /* parse colum_info_table */
            ColumnData colinfo;
            for(uint8_t index = 0; index < nr_col; index++) {
                col = (ColMeta*)data;
                colinfo.init();
                col->ColInfo(colinfo);
                if (colinfo.col_size == 0) {
                    printf("[%s: %d] skip invalid column info \n", __func__, __LINE__);
                    break;
                }
                if (colinfo.iter_sequence > largest_seq) largest_seq = colinfo.iter_sequence;
                if (colinfo.ikey_sequence > largest_ikey) largest_ikey = colinfo.ikey_sequence;
                if (colinfo.is_deleted) {
                    has_key = true;
                } else if (!has_key && !found_key) {
                   bool rh_hit = false;
                   kb = mf_->FindKBCache(colinfo.ikey_sequence);
                   assert(kb);
                   if (kb->IsDummy()) {
                       bool b_load = false;
                       kb->KBLock();
                       if (kb->IsDummy()) {
                           b_load = mf_->LoadValue(colinfo.ikey_sequence, colinfo.ikey_size, &rh_hit, kb, true);
                           if (b_load) {
                               mf_->DummyKBToActive(colinfo.ikey_sequence, kb);
                           } else {
                               mf_->RemoveDummyKB(kb);
                           }
                       }
                       kb->KBUnlock();
                       if (!b_load) {
                           mf_->FreeKB(kb);
                           kb = nullptr;
                       }
                   }
                   if (kb) { 
                       user_key = kb->GetKeyFromKeyBlock(mf_, colinfo.kb_offset);
                       found_key = true;
                   }
                }
                data += col->GetColSize();
                size -= col->GetColSize();
                assert(size >= 0);
            }
            if (has_key) {
                uint16_t key_size = 0;
                /* loop find key information */
                char* addr = const_cast<char*>(GetVarint16Ptr(data, data + 2, &key_size));
                assert(addr);
                uint16_t offset = addr - data;
                data += offset;
                size -= offset;
                assert(size > key_size);
                if (!found_key) {
                    user_key = KeySlice(data, key_size);
                    found_key = true;
                }
                data += key_size;
                size -= key_size;
                assert(size > 0);
            } 
            if (found_key) { /* not found means ... this key already deleted */
                uint32_t meta_size = data - (char*)meta;
                assert(meta_size > 1);
                Slice column_info = Slice((char*)meta, meta_size);
                column_info.remove_prefix(1); /* skip nr column */
                /* search uk or create uk */
                UserKey* uk = nullptr;
                auto it = key_hashmap->find(user_key);
                if (it != key_hashmap->end()) {
                    uk = it->second;
                } else {
                    uk = mf_->AllocUserKey(user_key.size());
                    uk->InitUserKey(user_key);
                    uk->DecreaseReferenceCount();
                    auto new_it = key_hashmap->insert(uk->GetKeySlice(), uk);
                    assert(new_it.second);
                }
                assert(uk);
                uk->InsertColumnNodes(mf_, nr_col, column_info, true);
                assert(size >= 0);
                if (kb) mf_->FreeKB(kb);
            }
        }
    }
#endif
    void DBRecovery::FlushUserKey(UserKey* uk, uint64_t &largest_ikey, uint64_t &largest_seq){
        SimpleLinkedList* col_list = nullptr;
        ColumnNode* col_node = NULL;
        uint64_t ikey = 0;
        uint64_t begin_seq = 0;
        assert(uk); 
        for( uint8_t col_id = 0 ; col_id < kMaxColumnCount ; col_id++){
            /*
             * ColumnLock may not be needed 
             * dfb_info->ukey->ColumnLock(col_id);
             */
            col_list = uk->GetColumnList(col_id);

            col_node = NULL;
            for(SimpleLinkedNode* col_list_node = col_list->newest(); col_list_node ; ) {
                ColumnNode* valid_col_node = NULL;

                col_node = uk->GetColumnNodeContainerOf(col_list_node);
                /*
                   assert(valid_col_nodes.empty());
                 * col_node can be deleted.(col_list_node exists in col_node ).
                 * Therefore, keep next col_list_node first.
                 */
                col_list_node = col_list->next(col_list_node);

                ///////////////// Check whether col_node has been submitted or not ///////////////// 
                /* it can be droped before submission */
                ikey = col_node->GetInSDBKeySeqNum();
                begin_seq = col_node->GetBeginSequenceNumber();
                if (ikey > largest_ikey) largest_ikey = ikey;
                if (begin_seq != ULONG_MAX && begin_seq > largest_seq) largest_seq = begin_seq;
                assert(ikey);
                KeyBlockPrimaryMeta* kbpm = col_node->GetKeyBlockMeta();
                assert(kbpm); // kbpm must exist becuase it was prefetched in InsertUserKeyToKeyMapFromInactiveKeyQueue()->InsertColumnNodes()

                if(kbpm->IsDummy()){
                    while(kbpm->SetWriteProtection());
                    if (kbpm->IsDummy()) {
                        char kbm_buf[kOutLogKBMSize] = {0,};
                        int kbm_buf_size = kOutLogKBMSize;
                        kbpm->SetInSDBKeySeqNum(ikey);
                        if(kbpm->IsKBMPrefetched()) kbpm->ClearKBMPrefetched();
                        if (!kbpm->LoadKBPMDeviceFormat(mf_, kbm_buf, kbm_buf_size, false)) { /* try sync */
                            /*
                             * key may already deleted.. 
                             */
                            kbpm->ClearKeyBlockBitmap(col_node->GetKeyBlockOffset());
                            col_list->Delete(col_node->GetColumnNodeNode());
                            col_node->InvalidateUserValue(mf_);

                            if(uk->GetLatestColumnNode(col_id) == col_node)
                                uk->SetLatestColumnNode(col_id, col_list_node ? uk->GetColumnNodeContainerOf(col_list_node): NULL);
#ifndef NDEBUG
                            if(col_node->GetRequestNode()) {
                                assert(col_node->GetRequestNode()->GetRequestNodeLatestColumnNode() != col_node);
                            }
#endif
                            col_node->SetRequestNode(nullptr);
                            mf_->FreeColumnNode(col_node);
                            kbpm->ClearWriteProtection();
                            continue;
                        }
                        if (kbm_buf_size <= kOutLogKBMSize) {
                            kbpm->InitWithDeviceFormat(mf_, kbm_buf, (uint16_t)kbm_buf_size);
                        } else {
                            char *inLogBuffer = (char*)malloc(kbm_buf_size); /* will be freed whenc FreeKBPM() */
                            assert(inLogBuffer);
                            kbpm->LoadKBPMDeviceFormat(mf_, inLogBuffer, kbm_buf_size, false);
                            kbpm->InitWithDeviceFormatForInLog(mf_, inLogBuffer, kbm_buf_size);
                        }
                    }
                    kbpm->ClearWriteProtection();
                }

                if(col_node->IsTRXNCommitted()){

                    assert(col_node->GetRequestType() == kPutType);
                    /* update bitmap */
                    assert(kbpm);
                    while(kbpm->SetWriteProtection());
                    kbpm->ClearKeyBlockBitmap(col_node->GetKeyBlockOffset());
                    kbpm->ClearWriteProtection();
                    /*
                     * There is possiblity that previous sktdf node has set reference for those.
                     * To remove dangling reference, clear reference bit here.
                     */
#ifdef CODE_TRACE
                    printf("[%d :: %s]push KBM : 0x%p [iKey : %ld] (offset : %u)\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber(), col_node->GetKeyBlockOffset());
#endif
                    //kbpm_list.push_back(kbpm);

                    /////////////////////// End Spinlock for Write Protection /////////////////////// 
#ifdef CODE_TRACE
                    printf("[%d :: %s];Not inserted to Inlog;|;KBPM;%p;|;iKey;%ld;|;offset;%d;|\n", __LINE__, __func__,col_node->GetKeyBlockMeta(), col_node->GetInSDBKeySeqNum(), col_node->GetKeyBlockOffset());
#endif

                    col_list->Delete(col_node->GetColumnNodeNode());
                    col_node->InvalidateUserValue(mf_);

                    if(uk->GetLatestColumnNode(col_id) == col_node)
                        uk->SetLatestColumnNode(col_id, col_list_node ? uk->GetColumnNodeContainerOf(col_list_node): NULL);
#ifndef NDEBUG
                    if(col_node->GetRequestNode()) {
                        assert(col_node->GetRequestNode()->GetRequestNodeLatestColumnNode() != col_node);
                    }
#endif
                    col_node->SetRequestNode(nullptr);
                    mf_->FreeColumnNode(col_node);

                } else {
                    abort();
                }
            }
        }
        return;
    }




    void DBRecovery::LoadKeymapAndProcessColTable(std::vector<KEYMAP_HDR*> &keymap_hdrs, SequenceNumber &largest_ikey, SequenceNumber &largest_seq, uint32_t &new_uk, uint32_t &removed_uk, uint32_t &replace_uk, uint32_t start_index, uint32_t count){
        /* create sktable */
        bool has_updated_col_info_table = false;
        uint32_t end = start_index + count;
        for(uint32_t i = start_index; i < end; i++) {
            if ((i+1) < end) {
                KEYMAP_HDR* next_hdr = keymap_hdrs[i + 1];
                // blindless prefetch for next skt
                for(uint32_t colid = next_hdr->last_colinfo; colid < (next_hdr->last_colinfo + 20); colid++) {
                    int size = kDeviceRquestMaxSize;
                    InSDBKey prekey = GetColInfoKey(GetManifest()->GetDBHash(), next_hdr->skt_id, colid);
                    GetEnv()->Get(prekey, nullptr, &size, kReadahead);
                }
            }
            /* load keymap */
            KEYMAP_HDR* hdr = keymap_hdrs[i];
            uint32_t keymap_size = SizeAlignment((hdr->keymap_size & KEYMAP_SIZE_MASK) + sizeof(KEYMAP_HDR));
#ifdef USE_HASHMAP_RANDOMREAD
            KeyHashMap* hashmap = nullptr;
            Arena* arena = mf_->LoadKeymapFromDevice(hdr->skt_id, keymap_size, hdr->hashmap_data_size, hashmap, kSyncGet);
            assert(arena);
            Keymap* keymap = new Keymap(arena, mf_->GetComparator(), hashmap);
            assert(keymap);
#else
            Arena* arena = mf_->LoadKeymapFromDevice(hdr->skt_id, keymap_size, kSyncGet);
            assert(arena);
            Keymap* keymap = new Keymap(arena, mf_->GetComparator());
            assert(keymap);
#endif
            SKTableMem *skt = new SKTableMem(mf_, keymap);
            assert(skt);
#ifdef USE_HASHMAP_RANDOMREAD
            skt->SetKeymapNodeSize(keymap_size, hdr->hashmap_data_size);
#else
            skt->SetKeymapNodeSize(keymap_size);
#endif
            skt->SetSKTableID(keymap->GetSKTableID());

            mf_->InsertSKTableMem(skt);
            rdb_mu_.Lock();
            SKTableMem* prev = mf_->Prev(skt);
            if (!prev) {
                mf_->InsertFrontToSKTableSortedList(skt->GetSKTableSortedListNode());
            } else {
                mf_->InsertNextToSKTableSortedList(skt->GetSKTableSortedListNode(), prev->GetSKTableSortedListNode());
            }
            rdb_mu_.Unlock();
            std::queue<KeyBlockPrimaryMeta*> in_log_KBPM_queue;
#ifdef INSDB_USE_HASHMAP
            KeyHashMap *key_hashmap = new KeyHashMap(64*1024); 
#endif
            /* process column info table */
            uint32_t cur_col_id = hdr->last_colinfo;
            uint32_t prev_col_id = hdr->last_colinfo;
            std::string col_info;
            while(1) {
                Status s = mf_->LoadColInfoFromDevice(hdr->skt_id, cur_col_id, col_info);
                if (!s.ok()) break;
#ifdef INSDB_USE_HASHMAP
                UpdateColInfoToUk(col_info, key_hashmap, largest_ikey, largest_seq);
#endif
                cur_col_id++; /* keep tracking cur_col_id */
                has_updated_col_info_table = true;
            }
#ifdef INSDB_USE_HASHMAP
            /* user key has updated column information */
            if (!key_hashmap->empty()) {
                for (auto it = key_hashmap->cbegin(); it != key_hashmap->cend(); ++it) {
                    UserKey* uk = it->second;
                    assert(uk);
                    /* update keymap */
                    uint32_t handle = keymap->Search(uk->GetKeySlice());
                    if (handle) {
                        Slice key_info = keymap->GetKeyInfoLatest(handle);
                        assert(key_info.size());
                        uint8_t nr_col = (uint8_t)(*(key_info.data()));
                        key_info.remove_prefix(1); /* skip nr_col */
                        uk->InsertColumnNodes(mf_, (nr_col & NR_COL_MASK) + 1, key_info);
                        keymap->Remove(uk->GetKeySlice());
                        replace_uk++;
                    } else {
                        new_uk++;
                    }

                    uint16_t new_key_info_size = uk->GetKeyInfoSizeForNew();
                    assert(new_key_info_size);
                    bool is_old = false;
                    Slice keyinfo = keymap->Insert(uk->GetKeySlice(), new_key_info_size, handle, is_old);
                    assert(!is_old/*keyinfo.size()*/ );
                    bool has_del = false;
                    uk->BuildColumnInfo(mf_, new_key_info_size, keyinfo, largest_ikey, in_log_KBPM_queue, has_del);
                    /*
                     * in_log_KBPM_queue maybe clean at this time.
                     * Need to check manually with uk.
                     */
                    assert(in_log_KBPM_queue.empty());
                    FlushUserKey(uk, largest_ikey, largest_seq);
                    mf_->FreeUserKey(uk);
                }
            }
            delete key_hashmap;
#endif

            /* CleanupAndStore keymap */
#ifdef USE_HASHMAP_RANDOMREAD
            KeyHashMap* hashmap = new KeyMapHashMap(1024*32);
            Arena* new_arena = skt->MemoryCleanupSKTDeviceFormat(mf_, hashmap);
#else
            Arena* new_arena = skt->MemoryCleanupSKTDeviceFormat(mf_);
#endif
            if (new_arena) {
#ifdef USE_HASHMAP_RANDOMREAD
                skt->InsertArena(new_arena, hashmap);
#else
                skt->InsertArena(new_arena);
#endif
            }

            if (skt->GetSKTableID() != 1 && !skt->GetCount()) {
                skt->SetDeletedSKT(mf_);
                continue;
            }
           /*
             * Check whether it is possible to flush ..
             * if possible, flush keymap and remove memory.
             * else insert to dirty cache by expecting after recover will be flushed by flush thread.
             */
            if (!skt->NeedToSplit()) {
                uint32_t io_size = 0;
                if (has_updated_col_info_table && cur_col_id) {
                    skt->ResetCurColInfoID(cur_col_id, cur_col_id); /* reset col info table range */
                }
#ifdef USE_HASHMAP_RANDOMREAD
                uint32_t hash_data_size = 0;
                Status s = skt->StoreKeymap(mf_, keymap, io_size, hash_data_size);
#else
                Status s = skt->StoreKeymap(mf_, keymap, io_size);
#endif
                if (!s.ok()) {
                    printf("[%d :: %s] fail to flush sub key %d\n", __LINE__, __func__, skt->GetSKTableID());
                    abort();
                }
#ifdef USE_HASHMAP_RANDOMREAD
                skt->SetKeymapNodeSize(io_size, hash_data_size);
#else
                skt->SetKeymapNodeSize(io_size);
#endif
                skt->InsertArena(nullptr); /* remove memory */

                if (has_updated_col_info_table && cur_col_id) {
                    for(uint32_t colid = prev_col_id; colid < cur_col_id; colid++) {
                        InSDBKey prekey = GetColInfoKey(GetManifest()->GetDBHash(), skt->GetSKTableID(), colid);
                        GetEnv()->Delete(prekey);
                    }
                }
                mf_->InsertSKTableMemCache(skt, kDirtySKTCache, SKTableMem::flags_in_dirty_cache);
            } else {
#ifdef MEM_TRACE
                mf_->IncSKTDF_MEM_Usage(skt->GetBufferSize());
#endif                
                mf_->IncreaseSKTDFNodeSize(skt->GetBufferSize());
 
                /* split sktable */
                if (has_updated_col_info_table && cur_col_id) {
                    skt->ResetCurColInfoID(prev_col_id, cur_col_id); /* reset col info table range, flush will remove col info tables */
                }
                mf_->InsertSKTableMemCache(skt, kDirtySKTCache, SKTableMem::flags_in_dirty_cache);
                std::string comp_col_info_table;
                comp_col_info_table.reserve(kDeviceRquestMaxSize);
                std::string col_info_table;
                std::queue<KeyBlockPrimaryMeta*> in_log_KBM_queue;
                skt->FlushSKTableMem(mf_, col_info_table, comp_col_info_table, in_log_KBM_queue);
            }
        }
        return;
    }

    bool DBRecovery::AddCompletedColumn(std::unordered_map<TransactionID, TransactionColCount> &TRXNList, TransactionID col_trxn_id, uint32_t count)
    {
        assert(count > 1);

        bool trxn_completed = false;
        // find a head transaction the column belongs to.
        auto trxn = TRXNList.find(col_trxn_id);
        if(trxn == TRXNList.end()) {
            // insert
            TransactionColCount colcount = { .count = count, .completed = 1};
            TRXNList.insert({col_trxn_id, colcount});
        } else {
            assert(trxn->second.count == count);
            // increase the completed column count
            trxn->second.completed ++;
            // remove transaction if transaction is completed.
            if(trxn->second.count == trxn->second.completed) {
                printf("Transaction %lu %u is completed.\n", col_trxn_id, trxn->second.count);
                TRXNList.erase(trxn);
                trxn_completed = true;
            }
        }

        return trxn_completed;
    }

    void DBRecovery::AddUncompletedColumn(std::unordered_map<TransactionID, TransactionColCount> &TRXNList, TransactionID col_trxn_id, uint32_t count)
    {
        assert(count > 1);

        // find a head transaction the column belongs to.
        auto trxn = TRXNList.find(col_trxn_id);
        if(trxn == TRXNList.end()) {
            // insert
            TransactionColCount colcount = { .count = count, .completed = 0};
            TRXNList.insert({col_trxn_id, colcount});
        }
    }


    void DBRecovery::InsertRecoveredKeyToKeymap(Manifest *mf, ColumnInfo &colinfo, uint32_t &new_uk, uint32_t &removed_uk, uint32_t &replace_uk) {
        SKTableMem* skt = mf_->FindSKTableMem(colinfo.key);
        assert(skt);
        if (!skt->IsKeymapLoaded()) {
            if (!skt->LoadSKTableDeviceFormat(mf)) {
                if (!skt->IsCached()) { 
                    mf->InsertSKTableMemCache(skt, kDirtySKTCache, SKTableMem::flags_in_dirty_cache);
                }
            }
        }
        Keymap* keymap = skt->GetKeyMap();
        int32_t old_size = skt->GetBufferSize();
        skt->KeyMapGuardLock();
        KeyMapHandle handle = keymap->Search(colinfo.key);
        if (handle) {
            void *col_uk = nullptr;
            Slice col_info_slice = keymap->GetColumnData(handle, colinfo.col_id, col_uk);
            assert(!col_uk);
            if (col_info_slice.size()) {
                ColumnData col_data;
                col_data.init();
                ColMeta* colmeta = (ColMeta*)(const_cast<char*>(col_info_slice.data()));
                colmeta->ColInfo(col_data);
                /* if colinfo is older information  .... nothing to do */
                if (col_data.iter_sequence >= colinfo.iter_seq) {
                    skt->KeyMapGuardUnLock();
                    return;
                }
            }
            /* rebuild key info */
            ColumnData col_data[kMaxColumnCount];
            memset((char*)col_data, 0, sizeof(ColumnData)*kMaxColumnCount);
            Slice keyinfo = keymap->GetKeyInfoLatest(handle); /* get key info */
            assert(keyinfo.size());
            /* parse orig key info */
            char* data = const_cast<char*>(keyinfo.data());
            KeyMeta* meta = (KeyMeta*)data;
            assert(!meta->HasUserKey());
            uint8_t nr_col = meta->NRColumn();
            if (nr_col) {
                ColMeta *col_meta = meta->ColMetaArray();
                char* col_addr = (char*) col_meta;
                for (uint8_t i = 0; i < nr_col; i++) {
                    uint8_t col_id = col_meta->ColumnID();
                    col_data[col_id].init();
                    col_meta->ColInfo(col_data[col_id]);
                    col_addr += col_meta->GetColSize();
                    col_meta = (ColMeta *)col_addr;
                }
            }
            /* update col info */
            assert(colinfo.col_id <= kMaxColumnCount);
            /* clear kbpm bitmap for old column node */
            if (col_data[colinfo.col_id].ikey_sequence && !col_data[colinfo.col_id].is_deleted) {
                KeyBlockPrimaryMeta *kbpm =  mf_->GetKBPMWithiKey(col_data[colinfo.col_id].ikey_sequence, col_data[colinfo.col_id].kb_offset);
                assert(kbpm); 
                if(kbpm->IsDummy()){
                    while(kbpm->SetWriteProtection());
                    if (kbpm->IsDummy()) {
                        char kbm_buf[kOutLogKBMSize] = {0,};
                        int kbm_buf_size = kOutLogKBMSize;
                        kbpm->SetInSDBKeySeqNum(col_data[colinfo.col_id].ikey_sequence);
                        if(kbpm->IsKBMPrefetched()) kbpm->ClearKBMPrefetched();
                        if (!kbpm->LoadKBPMDeviceFormat(mf_, kbm_buf, kbm_buf_size, false)) { /* try sync */
                            kbpm->ClearWriteProtection();
                            goto update_col_info;
                        }
                        if (kbm_buf_size <= kOutLogKBMSize) {
                            kbpm->InitWithDeviceFormat(mf_, kbm_buf, (uint16_t)kbm_buf_size);
                        } else {
                            char *inLogBuffer = (char*)malloc(kbm_buf_size); /* will be freed whenc FreeKBPM() */
                            assert(inLogBuffer);
                            kbpm->LoadKBPMDeviceFormat(mf_, inLogBuffer, kbm_buf_size, false);
                            kbpm->InitWithDeviceFormatForInLog(mf_, inLogBuffer, kbm_buf_size);
                        }
                    }

                    kbpm->ClearWriteProtection();
                }
                assert(!kbpm->IsDummy());
                kbpm->ClearKeyBlockBitmap(col_data[colinfo.col_id].kb_offset);
            }
update_col_info: 
            col_data[colinfo.col_id].init();
            if (colinfo.type == kDelType) col_data[colinfo.col_id].is_deleted = true;
            else col_data[colinfo.col_id].kb_offset = colinfo.kb_index;

            if (colinfo.ttl) {
                col_data[colinfo.col_id].has_ttl = true;
                col_data[colinfo.col_id].ttl = colinfo.ttl;
            }
            col_data[colinfo.col_id].iter_sequence = colinfo.iter_seq;
            col_data[colinfo.col_id].ikey_sequence = colinfo.ikey_seq;
            col_data[colinfo.col_id].ikey_size = colinfo.kb_size;
            col_data[colinfo.col_id].col_size = 12; /* dummy stuff to recognize valid col */

            /* rebuild column info */
            std::string new_col_info;
            new_col_info.resize(0);
            new_col_info.append(1, 0x0); /* reserve for nr column */
            nr_col = 0;
            for (int i = 0; i < kMaxColumnCount; i++) {
                if (!col_data[i].is_deleted && col_data[i].col_size) {
                    uint32_t start_offset = new_col_info.size();
                    new_col_info.append(1, 0x0); /* reserve for column size */
                    uint8_t colid = col_data[i].column_id;
                    if (col_data[i].has_ttl) colid |= 0x80;
                    new_col_info.append(1, colid);
                    uint8_t kb_offset = col_data[i].kb_offset;
                    new_col_info.append(1, kb_offset);
                    PutVarint64(&new_col_info, col_data[i].iter_sequence);
                    if (col_data[i].has_ttl) PutVarint64(&new_col_info, col_data[i].ttl);
                    PutVarint64(&new_col_info, col_data[i].ikey_sequence);
                    PutVarint32(&new_col_info, col_data[i].ikey_size);
                    uint32_t last_offset = new_col_info.size();
                    uint8_t col_size = last_offset - start_offset;
                    new_col_info[start_offset] = col_size;
                    nr_col++;
                }
            }
            if (nr_col ==0) {
                keymap->Remove(colinfo.key);
                removed_uk++;
            } else {
                replace_uk++;
                new_col_info[0] = nr_col -1;
                /* update keymap */
                Slice new_col_slice(new_col_info);
                keymap->Remove(colinfo.key);
                bool key_info_exist = false;
                Slice new_keyinfo = keymap->Insert(colinfo.key, new_col_slice.size(), handle, key_info_exist);
                assert(!key_info_exist/*new_keyinfo.size() >= new_col_slice.size() && handle*/);
                memcpy(const_cast<char*>(new_keyinfo.data()), const_cast<char*>(new_col_slice.data()), new_col_slice.size());
            }
        } else if (colinfo.type != kDelType) {
            new_uk++; 
            /* rebuild column info */
            std::string new_col_info;
            new_col_info.resize(0);
            new_col_info.append(1, 0x0); /* reserve for nr column */
            uint32_t start_offset = new_col_info.size();
            new_col_info.append(1, 0x0); /* reserve for column size */
            uint8_t colid = colinfo.col_id;
            if (colinfo.ttl) colid |= 0x80;
            new_col_info.append(1, colid);
            uint8_t kb_offset = colinfo.kb_index;
            new_col_info.append(1, kb_offset);
            PutVarint64(&new_col_info, colinfo.iter_seq);
            if (colinfo.ttl) PutVarint64(&new_col_info, colinfo.ttl);
            PutVarint64(&new_col_info, colinfo.ikey_seq);
            PutVarint32(&new_col_info, colinfo.kb_size);
            uint32_t last_offset = new_col_info.size();
            uint8_t col_size = last_offset - start_offset;
            new_col_info[start_offset] = col_size;
            /* insert keymap */
            Slice new_col_slice(new_col_info);
            bool is_old = false;
            Slice new_keyinfo = keymap->Insert(colinfo.key, new_col_slice.size(), handle, is_old);
            assert(!is_old);
            assert(new_keyinfo.size() >= new_col_slice.size() && handle);
            memcpy(const_cast<char*>(new_keyinfo.data()), const_cast<char*>(new_col_slice.data()), new_col_slice.size());
        }
        int32_t new_size = skt->GetBufferSize();
        mf_->IncreaseSKTDFNodeSize(new_size - old_size);
        skt->KeyMapGuardUnLock();
   }

    /**
     *
     */
    bool DBRecovery::UpdateAllKBMsInOrder() {
        std::map<SequenceNumber, KeyBlockPrimaryMeta*> kbpm_order_list;
        mf_->GetCurrentOrderKBPMList(kbpm_order_list);
        if (!kbpm_order_list.empty()) {
            for (auto it = kbpm_order_list.crbegin(); it != kbpm_order_list.crend(); ++it) {
                KeyBlockPrimaryMeta* kbpm = it->second;
                if (kbpm) {
                    if (kbpm->IsInlog()) {
                        kbpm->CleanupDeviceFormat();
                    }
                    if (!kbpm->IsDummy()) {
                        /* update devcie with updated kbpm */
                        if (!kbpm->GetKeyBlockBitmap()) { /* Deleted kb/kbpm */
                            mf_->GetEnv()->Delete(GetKBKey(mf_->GetDBHash(), kKBMeta, kbpm->GetiKeySequenceNumber()));
                            mf_->GetEnv()->Delete(GetKBKey(mf_->GetDBHash(), kKBlock, kbpm->GetiKeySequenceNumber()));
                        } else {
                            /* update kbpm */
                            char kbpm_buffer[128];
                            uint32_t size = SizeAlignment((kbpm->BuildDeviceFormatKeyBlockMeta(kbpm_buffer)).size());
                            Slice kbm_info(kbpm_buffer, size);
                            mf_->GetEnv()->Put(GetKBKey(mf_->GetDBHash(), kKBMeta, kbpm->GetiKeySequenceNumber()), kbm_info, true);
                        }
                    }
                }
            }
        }
        mf_->ResetKBPMHashMap();
        return true;
    }

    /**
    */
    Status DBRecovery::RunRecovery() {
        Status s;
        SuperSKTable super_skt;
        uint32_t skt_id = 0;
        uint64_t largest_seq = 0;
        uint64_t largest_ikey = 0;
        uint32_t new_uk = 0;
        uint32_t removed_uk = 0;
        uint32_t replace_uk = 0;
        // decode the log record
        // note that it's already loaded when checking log record existence.
        bool success = logkey_.DecodeBuffer(workers_next_ikey_seqs_, completed_trxn_groups_);
        if(!success) {
            printf("cannot decode the log record\n");
            return Status::Corruption("could not decode log record");
        }
        for (uint32_t i = 0; i < workers_next_ikey_seqs_.size(); i++) {
            if (workers_next_ikey_seqs_[i] > largest_ikey) largest_ikey = workers_next_ikey_seqs_[i];
        }
        std::string recover_log;
        recover_log.append(RECOVER_LOG_SIZE, 0x0);
        char* recover_log_buf = const_cast<char*>(recover_log.data());
        int size = RECOVER_LOG_SIZE;
        s = mf_->GetEnv()->Get(GetMetaKey(mf_->GetDBHash(), kRecoverLog, 0), recover_log_buf, &size);
        if (s.ok() && size == RECOVER_LOG_SIZE) {
            uint32_t orig_crc = crc32c::Unmask(DecodeFixed32(recover_log_buf)); //get crc
            uint32_t size = DecodeFixed32(recover_log_buf + RECOVER_LOG_SIZE_OFFSET);
            uint32_t actual_crc = crc32c::Value(recover_log_buf + RECOVER_LOG_SIZE_OFFSET, size - RECOVER_LOG_SIZE_OFFSET);
            if (orig_crc == actual_crc) {
               uint64_t recovered_largest_ikey = DecodeFixed64(recover_log_buf + RECOVER_LOG_IKEY_OFFSET);
               uint64_t recovered_largest_seq = DecodeFixed64(recover_log_buf + RECOVER_LOG_SEQ_OFFSET);
               if (recovered_largest_ikey > largest_ikey) largest_ikey = recovered_largest_ikey;
               if (recovered_largest_seq > largest_seq) largest_seq = recovered_largest_seq;
            }
        }

        /*
         * prefetch sktable 2K
         */
        size = 0;
        for (uint32_t i = 1; i < 1000; i++) {
            size = 2048;
            InSDBKey prekey = GetMetaKey(GetManifest()->GetDBHash(), kSKTable, i);
            GetEnv()->Get(prekey, nullptr, &size, kReadahead);
        }

        // clean up skt update record
        // get the first SKT ID
        s = mf_->ReadSuperSKTable(&super_skt);
        if (!s.ok()) {
            printf("cannot read super sktable\n");
            return s;
        }
        skt_id = super_skt.first_skt_id;

        // load sktable meta and its begin keys
        // and, clean up updated key blocks
        // TODO utilize prefetch.
        //      utilize multithreads.
        PrepareLoadForSKTable(keymap_hdrs_, skt_id);
        if (!keymap_hdrs_.empty()) {
            /*
             * process with multi-thread ....
             */
            int sleep_microsecond = 1000000; //1sec
            uint32_t max_thread = kmax_recover_skt_threads;
            //uint32_t max_thread = 1;
            uint32_t num_skt = keymap_hdrs_.size();
            uint32_t thread_count = (num_skt > max_thread) ? max_thread : num_skt;
            uint32_t skt_count = num_skt / thread_count;
            uint32_t  mod_remains =  num_skt % thread_count;
            InSDBThreadsState state;
            state.num_running = thread_count;
            RecoverSKTableCtx * recover_ctxs = new RecoverSKTableCtx[thread_count];
            uint32_t remains = num_skt;
            uint32_t req = 0;
            uint32_t index = 0;
            for (uint32_t i = 0; i < thread_count; i++) {
                req = (remains > skt_count) ? skt_count : remains;
                if (mod_remains && i < mod_remains) req++;
                recover_ctxs[i].recovery = this;
                recover_ctxs[i].state = &state;
                recover_ctxs[i].count = req;
                recover_ctxs[i].start_index = index;
                recover_ctxs[i].largest_ikey = 0;
                recover_ctxs[i].largest_seq = 0;
                recover_ctxs[i].new_uk = 0;
                recover_ctxs[i].removed_uk = 0;
                recover_ctxs[i].replace_uk = 0;
                GetEnv()->StartThread(DBRecovery::RecoverSKTableWrapper, (void*)&recover_ctxs[i]);
                index += req;
                remains -= req;
            }
            while(1) {
                state.mu.Lock();
                int num = state.num_running;
                state.mu.Unlock();
                if (num == 0) break;
                GetEnv()->SleepForMicroseconds(sleep_microsecond);
            }
            for (uint32_t i = 0; i < thread_count; i++) {
                if (recover_ctxs[i].largest_ikey > largest_ikey) largest_ikey = recover_ctxs[i].largest_ikey;
                if (recover_ctxs[i].largest_seq > largest_seq) largest_seq = recover_ctxs[i].largest_seq;
                new_uk += recover_ctxs[i].new_uk;
                removed_uk += recover_ctxs[i].removed_uk;
                replace_uk += recover_ctxs[i].replace_uk;
            }

            printf("[%s:%d] --Recover Keymap--\n\t detected largest ikey (%ld) : largest seq (%ld) found new uk(%d) removed uk(%d) replace uk(%d)\n",
                    __func__, __LINE__,  largest_ikey, largest_seq, new_uk, removed_uk, replace_uk);

            for (uint32_t i = 0; i < keymap_hdrs_.size(); i++) {
                KEYMAP_HDR* hdr = keymap_hdrs_[i];
                delete hdr;
            }
            keymap_hdrs_.clear();
            delete [] recover_ctxs;
        }
        recover_log.resize(RECOVER_LOG_SIZE); //skip crc
        recover_log_buf = const_cast<char*>(recover_log.data());
        EncodeFixed32(recover_log_buf + RECOVER_LOG_SIZE_OFFSET, RECOVER_LOG_SIZE);
        EncodeFixed64(recover_log_buf + RECOVER_LOG_IKEY_OFFSET, largest_ikey);
        EncodeFixed64(recover_log_buf + RECOVER_LOG_SEQ_OFFSET, largest_seq);
        uint32_t crc = crc32c::Value(recover_log_buf + RECOVER_LOG_SIZE_OFFSET, RECOVER_LOG_SIZE - RECOVER_LOG_SIZE_OFFSET);
        EncodeFixed32(recover_log_buf, crc32c::Mask(crc));
        Slice recover_log_slice(recover_log);
        mf_->GetEnv()->Put(GetMetaKey(mf_->GetDBHash(), kRecoverLog, 0), recover_log_slice, true);



        //
        // Redo logs and delete incomplete transactions
        //
        std::unordered_map<TransactionID, TransactionColCount> UncompletedTRXNList;
        std::list<ColumnInfo> UncompletedKeyList;
#if 1
        // start worker log recovery threads
        DBRecoveryThread *recovery_threads[kWriteWorkerCount];
        for(int worker_idx = 0; worker_idx < kWriteWorkerCount; worker_idx++) {
            recovery_threads[worker_idx] = new DBRecoveryThread(this, worker_idx);
            recovery_threads[worker_idx]->StartThread();
        }

        // wait for worker log recovery threads, and merge the results
        Status thread_status;
        for(int worker_idx = 0; worker_idx < kWriteWorkerCount; worker_idx++) {
            recovery_threads[worker_idx]->JointThread();
            if(recovery_threads[worker_idx]->GetStatus().ok())
            {
                // merge uncompleted transaction logs
                auto worker_trxn_list = recovery_threads[worker_idx]->GetUncompletedTrxnList();
                for ( auto trxn : worker_trxn_list )
                    AddCompletedColumn(UncompletedTRXNList, trxn.first, trxn.second.count);
                // concatenate uncompleted keys
                auto worker_key_list = recovery_threads[worker_idx]->GetUncompletedKeyList();
                UncompletedKeyList.splice(UncompletedKeyList.end(), worker_key_list);
                if (recovery_threads[worker_idx]->GetLargestIKey() > largest_ikey) largest_ikey = recovery_threads[worker_idx]->GetLargestIKey();
                if (recovery_threads[worker_idx]->GetLargestSeq() > largest_seq) largest_seq = recovery_threads[worker_idx]->GetLargestSeq();
                new_uk += recovery_threads[worker_idx]->GetNewUK();
                removed_uk += recovery_threads[worker_idx]->GetRemovedUK();
                replace_uk += recovery_threads[worker_idx]->GetReplaceUK();
            }
            else
            {
                thread_status = recovery_threads[worker_idx]->GetStatus();
            }
            delete recovery_threads[worker_idx];
        }
#else
        // start worker log recovery threads
        DBRecoveryThread *recovery_threads[kWriteWorkerCount];
        for(int worker_idx = 0; worker_idx < kWriteWorkerCount; worker_idx++) {
            recovery_threads[worker_idx] = new DBRecoveryThread(this, worker_idx);
        }
        Status thread_status;
        for(int worker_idx = 0; worker_idx < kWriteWorkerCount; worker_idx++) {
            recovery_threads[worker_idx]->StartThread();
            recovery_threads[worker_idx]->JointThread();
            if(recovery_threads[worker_idx]->GetStatus().ok())
            {
                // merge uncompleted transaction logs
                auto worker_trxn_list = recovery_threads[worker_idx]->GetUncompletedTrxnList();
                for ( auto trxn : worker_trxn_list )
                    AddCompletedColumn(UncompletedTRXNList, trxn.first, trxn.second.count);
                // concatenate uncompleted keys
                auto worker_key_list = recovery_threads[worker_idx]->GetUncompletedKeyList();
                UncompletedKeyList.splice(UncompletedKeyList.end(), worker_key_list);
                if (recovery_threads[worker_idx]->GetLargestIKey() > largest_ikey) largest_ikey = recovery_threads[worker_idx]->GetLargestIKey();
                if (recovery_threads[worker_idx]->GetLargestSeq() > largest_seq) largest_seq = recovery_threads[worker_idx]->GetLargestSeq();
                new_uk += recovery_threads[worker_idx]->GetNewUK();
                removed_uk += recovery_threads[worker_idx]->GetRemovedUK();
                replace_uk += recovery_threads[worker_idx]->GetReplaceUK();
            }
            else
            {
                thread_status = recovery_threads[worker_idx]->GetStatus();
            } 
            delete recovery_threads[worker_idx];
        }

#endif
        // Check whether any thread fails
        if(!thread_status.ok()) {
            return thread_status;
        }

restart_all_workers:
        // resolve uncompleted transaction list and uncompleted key list collected from all workers
        for(auto colinfo = UncompletedKeyList.begin() ; colinfo != UncompletedKeyList.end() ; ) {

            // get the pointers
            TrxnInfo *trxninfo = colinfo->trxninfo;

            auto trxn = UncompletedTRXNList.find(trxninfo->GetTrxnID());
            if(trxn != UncompletedTRXNList.end()){
                //UncompletedTRXNList.delete(trxn->id, trxn->count);

                // Add new uncompleted transactions from merged transactions
                //if there is no mgd_trxn->id, force insert(make it uncompleted TRXN).
                uint16_t nr_merged_trxn = trxninfo->GetNrMergedTrxnList();
                Transaction *merged_trxn = trxninfo->GetMergedTrxnList();
                for(int i = 0; i < nr_merged_trxn; i++)
                    AddUncompletedColumn(UncompletedTRXNList, merged_trxn[i].id, merged_trxn[i].count);

                // mark KB as deleted in memory
                // ikey->UpdateKBM(clear bit), or UpdateKBDL(erase it from delete list or trxn head info);
                if (colinfo->type == kPutType) colinfo->kbpm->ClearKeyBlockBitmap(colinfo->kb_index);

                // remove from the list and move to next key
                colinfo = UncompletedKeyList.erase(colinfo);
                // free completed column and its transaction information
                delete trxninfo;
                // restart with updated uncompleted transactions.
                goto restart_all_workers;
            }
            else
                colinfo++;
        }

        // the lefover is completed trasactions
        // Add them to keymap
        for (auto colinfo : UncompletedKeyList)
            InsertRecoveredKeyToKeymap(mf_, colinfo, new_uk, removed_uk, replace_uk);

        // Flush all key block meta updates
        //Do not delete KBM even if it has no valid bit in the bitmap
        // from each workers begin log to last
        success = UpdateAllKBMsInOrder();
        if(!success)
            return Status::IOError("could not update KBMs");

        /* update manifest sequence & ikeys */

        printf("[%s:%d] --Recover worker's next id--\n\t\t detected largest ikey (%ld) : largest seq (%ld) found new uk(%d) removed uk(%d) replace uk(%d)\n",
                __func__, __LINE__,  largest_ikey, largest_seq, new_uk, removed_uk, replace_uk);

        recover_log.resize(RECOVER_LOG_SIZE); //skip crc
        recover_log_buf = const_cast<char*>(recover_log.data());
        EncodeFixed32(recover_log_buf + RECOVER_LOG_SIZE_OFFSET, RECOVER_LOG_SIZE);
        EncodeFixed64(recover_log_buf + RECOVER_LOG_IKEY_OFFSET, largest_ikey);
        EncodeFixed64(recover_log_buf + RECOVER_LOG_SEQ_OFFSET, largest_seq);
        crc = crc32c::Value(recover_log_buf + RECOVER_LOG_SIZE_OFFSET, RECOVER_LOG_SIZE - RECOVER_LOG_SIZE_OFFSET);
        EncodeFixed32(recover_log_buf, crc32c::Mask(crc));
        recover_log_slice = Slice(recover_log);
        mf_->GetEnv()->Put(GetMetaKey(mf_->GetDBHash(), kRecoverLog, 0), recover_log_slice, true);


        uint64_t next_ikeys[kWriteWorkerCount];
        for (uint32_t i = 0; i < kWriteWorkerCount; i++) next_ikeys[i] = ++largest_ikey;
        mf_->InitNextInSDBKeySeqNum(kWriteWorkerCount, next_ikeys);
        mf_->SetNextInSDBKeySeqNum(largest_ikey);
        mf_->SetLastSequenceNumber(largest_seq); 


        SKTableMem* skt = mf_->GetFirstSKTable();
        while(skt) { // set sktable infomation.
            if (skt->IsDeletedSKT()) {
                SKTableMem* next_skt = skt->Next(mf_);
                SKTableMem* prev = mf_->Prev(skt);
                assert(prev);

                if (!prev->IsKeymapLoaded()) {
                    if (!prev->LoadSKTableDeviceFormat(mf_)) {
                        if (!prev->IsCached()) { 
                            mf_->InsertSKTableMemCache(prev, kDirtySKTCache, SKTableMem::flags_in_dirty_cache);
                        }
                    }
                }

                mf_->RemoveFromSKTableSortedList(skt->GetSKTableSortedListNode());
                prev->SetPreAllocSKTableID(skt->GetSKTableID());
                prev->SetNextSKTableID(skt->GetNextSKTableID());
                skt->DeleteSKTDeviceFormatCache(mf_);
                mf_->DeleteSKTableMeta(kSKTable, skt->GetSKTableID()); // remove sktale
                delete skt;
                uint32_t io_size = 0;
#ifdef USE_HASHMAP_RANDOMREAD
                uint32_t hash_data_size = 0;
                Status s = prev->StoreKeymap(mf_, prev->GetKeyMap(), io_size, hash_data_size);
#else
                Status s = prev->StoreKeymap(mf_, prev->GetKeyMap(), io_size);
#endif
                if (!s.ok()) {
                    printf("[%d :: %s] fail to flush sub key %d\n", __LINE__, __func__, skt->GetSKTableID());
                    abort();
                }
#ifdef USE_HASHMAP_RANDOMREAD
                prev->SetKeymapNodeSize(io_size, hash_data_size);
#else
                prev->SetKeymapNodeSize(io_size);
#endif
                prev->DeleteSKTDeviceFormatCache(mf_);
                skt = next_skt;
                continue;
            }
            if (skt->IsKeymapLoaded()) {
                if (skt->NeedToSplit()) {
                    /* split sktable */
                    std::string comp_col_info_table;
                    comp_col_info_table.reserve(kDeviceRquestMaxSize);
                    std::string col_info_table;
                    std::queue<KeyBlockPrimaryMeta*> in_log_KBM_queue;
                    skt->FlushSKTableMem(mf_, col_info_table, comp_col_info_table, in_log_KBM_queue);
                } 
            }
            skt = skt->Next(mf_);
        }

        // write manifest
        mf_->BuildManifest();
        mf_->ResetTrxn();
        // Delete the log record
        // If it exists when InDB is restarted, DB is dirty and needs a recovery.
        s = mf_->GetEnv()->Delete(GetMetaKey(mf_->GetDBHash(), kLogRecord), false);
        mf_->GetEnv()->Delete(GetMetaKey(mf_->GetDBHash(), kRecoverLog, 0), false);
        /* remove log key */
        return Status::OK();
    }

    /////////////////////////////////////////// START: Manifest Functions ///////////////////////////////////////////
    Manifest::Manifest(const Options& option, std::string& dbname)
        : env_(option.env), options_(option), dbname_(dbname),
        worker_mu_(),
        worker_cv_(&worker_mu_),
        worker_cnt_(0), 
        term_worker_cnt_(0),
        worker_terminated_(false),
        worker_termination_mu_(),
        worker_termination_cv_(&worker_termination_mu_),
        nr_worker_in_running_(0),
        /* "0 ~ kUnusedNextInternalKey(kMaxUserCount + kMaxWriteWorkerCount)" is used for ColumnNodes being submitted*/
        next_InSDBKey_seq_num_(kUnusedNextInternalKey),
        sequence_number_(1),/* Must be stored in ~Manifest and reuse the number */
        next_trxn_id_(1 << TGID_SHIFT), // Trxn ID 0 is researved for non-TRXN request. and only this has 0 TGID.
        next_sktable_id_(0),
        mem_use_(0),
        mem_use_cached_(0),
        skt_memory_usage_(0),
#ifdef MEM_TRACE
        skt_usage_(0),
        uk_usage_(0),
        col_usage_(0),
        sktdf_usage_(0),
        kbpm_usage_(0),
        iter_usage_(0),
        flush_miss_(0),
        flush_hit_(0),
        update_to_clean_(0),
        evicted_skt_(0),
        flush_to_clean_(0),
        flush_to_dirty_(0),
        flush_split_inc_(0),
        pr_iterbuild_cnt_(0),
        pr_next_cnt_(0),
        clear_pr_cnt_(0),
        skip_pr_cnt_(0),
        evict_with_pr_(0),
        evict_iter_(0),
        evict_skt_iter_(0),
        evict_stop_skt_ref_(0),
        evict_recalim_(0),
#endif
        update_cnt_(0),
        nr_iterator_(0),
        ukey_comparator_(option.comparator), gcinfo_(16), sktlist_(ukey_comparator_, &gcinfo_),/* keymap_(option.comparator, &gcinfo_, this),*/
#ifdef USE_HOTSCOTCH
        hopscotch_key_hasp_lock_(),
        hopscotch_key_hashmap_(new HotscotchKeyHashMap(1024*1024)),
#endif
#ifdef INSDB_USE_HASHMAP
        key_hashmap_(new KeyHashMap(256*1024)),
        uk_count_(0),
        congestion_control_(false),
        congestion_control_mu_(),
        congestion_control_cv_(&congestion_control_mu_),
        kbpm_hashmap_(new KBPMHashMap(1024*1024)),
        kbpm_pad_size_(0),
        kbpm_pad_(nullptr),
        kb_pad_(nullptr),
        flushed_key_hashmap_(new KeyHashMap(256*1024)),
        kbpm_cnt_(0), 
        flushed_key_cnt_(0),
        enable_blocking_read_(false),
#endif
        skt_sorted_simplelist_lock_(),
        snapshot_simplelist_lock_(),
        skt_sorted_simplelist_(),
        snapshot_simplelist_(),

        TtlList_(NULL),
        TtlEnabledList_(NULL),

        skt_dirty_cache_lock_(),
        skt_clean_cache_lock_(),
        skt_cache_{skt_dirty_cache_lock_, skt_clean_cache_lock_},
        skt_kbm_update_cache_lock_(),
        kbpm_update_list_(skt_kbm_update_cache_lock_),
        kb_pool_(NULL),
        kbpm_pool_(NULL),
        skt_flush_mu_(),
        skt_flush_cv_(&skt_flush_mu_),

        offload_lock_(),
        offload_cnt_(0),
        flush_offload_queue_(),

        force_kbm_update_(),
        kbpm_update_mu_(),
        kbpm_update_cv_(&kbpm_update_mu_),

        skt_eviction_mu_(),
        skt_eviction_cv_(&skt_eviction_mu_),

        //skt_eviction_shutdown_sem_(),
        flags_(0),
        cache_thread_shutdown_(false),
        skt_flush_thread_tid_(0),
        skt_eviction_thread_tid_(0),
        kbm_update_thread_tid_(0),

        dio_buffer_(),
        /* Transaction Management */
        uctrxn_group_lock_(),
        uncompleted_trxn_group_(),
        ctrxn_group_lock_(),
        completed_trxn_group_list_(),

        max_iter_key_buffer_size_(kMinSKTableSize),
        max_iter_key_offset_buffer_size_(kMinSKTableSize),

        uv_pool_(NULL),
        uk_pool_(NULL),
        req_pool_(NULL),
        cn_pool_(NULL),
        //free_request_node_spinlock_(NULL),
        //free_request_node_(NULL),

        logcleanup_data_(NULL),
        free_skt_buffer_spinlock_(),
        free_skt_buffer_(),
        //thread_memory_pool_(),
        normal_start_(false),
        dbhash_(Hash(dbname.c_str(), dbname.size(), MF_HASH_SEED))
        {
            if (option.max_request_size) {
                kMaxRequestSize = option.max_request_size;
            }
            if (option.num_column_count) kMaxColumnCount = option.num_column_count;
            if (option.num_write_worker) kWriteWorkerCount = option.num_write_worker;
            if (option.max_key_size) kMaxKeySize = option.max_key_size;
            if (option.max_table_size) kMaxSKTableSize = option.max_table_size;
#if 1 
            kMinUpdateCntForTableFlush = (kMaxSKTableSize*17UL)/512;
#else
            if (option.min_update_cnt_table_flush) kMinUpdateCntForTableFlush= option.min_update_cnt_table_flush;
#endif
            if (option.max_cache_size) kMaxCacheSize = option.max_cache_size;
            if (option.flush_threshold) 
                kSKTableFlushWatermark = option.flush_threshold;
            else
                kSKTableFlushWatermark = kMinUpdateCntForTableFlush << 5;
            if (option.slowdown_trigger) kSlowdownTrigger = option.slowdown_trigger;
            if (option.align_size) kRequestAlignSize = option.align_size;
            if (option.split_prefix_bits) kPrefixIdentifierSize = option.split_prefix_bits/8;
            kSlowdownLowWaterMark = kSlowdownTrigger - (kSlowdownTrigger/2);
            /* If total update count exceeds kSKTableFlushWatermark, flush all SKTables */
            kCacheSizeLowWatermark = (kMaxCacheSize-(kMaxCacheSize/ 8u));


            /* Iter Pad share */ 
            kIterPadShare = option.iterpad_share;
            /* Keep Written key block */
            kKeepWrittenKeyBlock = option.keep_written_keyblock;

            // KBPM pad
            // 10% of cache size will be cached in this pad
            kbpm_pad_size_ = kMaxCacheSize/(option.approx_key_size + option.approx_val_size + 30)    // max number of keys in memory
                    / 64                                                                            // max number of key blocks
                    * 10 / 100;                                                                     // 10%
#if 0
            printf ("kbpm_pad_size %u\n", kbpm_pad_size_);
#endif
            kbpm_pad_ = new std::atomic<KeyBlockPrimaryMeta*>[kbpm_pad_size_];
            kb_pad_ = new std::atomic<KeyBlock*>[kbpm_pad_size_];
            for (int i = 0; i< kbpm_pad_size_; ++i) {
                kbpm_pad_[i].store(nullptr, std::memory_order_relaxed);
                kb_pad_[i].store(nullptr, std::memory_order_relaxed);
            }

            TtlList_ = new uint64_t[kMaxColumnCount];
            memset(TtlList_, 0, kMaxColumnCount*8);

            TtlEnabledList_ = new bool[kMaxColumnCount];
            memset(TtlEnabledList_, 0, kMaxColumnCount);

            kb_cache_ = new KBCache((kMaxCacheSize - (kMaxCacheSize/4)));
            kbpm_cache_ = new KBPM_Cache(1024*1024);

            /*
             * For New design(packing)
             * TODO it must be initialized with worker's last ikey(read from MF or max ikey+worker_id(recovery) 
             */

            // print internal insdb settings for tracking
#if 0
            fprintf(stderr, "INSDB option: write_buffer_size %lu\n", option.write_buffer_size);
            fprintf(stderr, "INSDB option: max_open_files %d\n", option.max_open_files);
            fprintf(stderr, "INSDB option: block_size %lu\n", option.block_size);
            fprintf(stderr, "INSDB option: block_restart_interval %d\n", option.block_restart_interval);
            fprintf(stderr, "INSDB option: max_file_size %lu\n", option.max_file_size);
            fprintf(stderr, "INSDB option: max_cache_size %lu\n", option.max_cache_size);
            fprintf(stderr, "INSDB option: num_write_worker %d\n", option.num_write_worker);
            fprintf(stderr, "INSDB option: num_column_count %d\n", option.num_column_count);
            fprintf(stderr, "INSDB option: max_request_size %d\n", option.max_request_size);
            fprintf(stderr, "INSDB option: align_size %u\n", option.align_size);
            fprintf(stderr, "INSDB option: slowdown_trigger %u\n", option.slowdown_trigger);
            fprintf(stderr, "INSDB option: max_key_size %u\n", option.max_key_size);
            fprintf(stderr, "INSDB option: max_table_size %u\n", option.max_table_size);
            fprintf(stderr, "INSDB option: max_uval_prefetched_size %lu\n", option.max_uval_prefetched_size);
            fprintf(stderr, "INSDB option: max_uval_iter_buffered_size %lu\n", option.max_uval_iter_buffered_size);
            fprintf(stderr, "INSDB option: flush_threshold %lu\n", option.flush_threshold);
            fprintf(stderr, "INSDB option: min_update_cnt_table_flush %u kMinUpdateCntForTableFlush %u\n", option.min_update_cnt_table_flush, kMinUpdateCntForTableFlush);
            fprintf(stderr, "INSDB option: kv_ssd %s\n", option.kv_ssd);
            fprintf(stderr, "INSDB option: approx_key_size %lu\n", option.approx_key_size);
            fprintf(stderr, "INSDB option: approx_val_size %lu\n", option.approx_val_size);
            fprintf(stderr, "INSDB option: iter_prefetch_hint %u\n", option.iter_prefetch_hint);
#endif

            /**
             * 0 is default. if 0, it has no threshold.
             * skt_sorted_list.SetThreshold(0);
             * snapshot_list_.SetThreshold(0);
             */
            /* 140 is average key count per 4KB SKTable */
            uk_pool_    = new ObjectPool<UserKey>(kMaxAllocatorPool, def_uk_entry_cnt, -1, "uk");
            uv_pool_    = new ObjectPool<UserValue>(kMaxAllocatorPool, def_uv_entry_cnt, 1ULL << 35, "uv");
            cn_pool_    = new ObjectPool<ColumnNode>(kMaxAllocatorPool, def_cn_entry_cnt, -1, "cn");
            req_pool_   = new ObjectPool<RequestNode>(kMaxAllocatorPool, def_req_entry_cnt, -1, "req");
            kb_pool_    = new ObjectPool<KeyBlock>(kMaxAllocatorPool, 1024, /*1ULL << 35*/-1, "kb");
            kbpm_pool_  = new ObjectPool<KeyBlockPrimaryMeta>(kMaxAllocatorPool, 1024, /*1ULL << 32*/-1, "kbpm");
#if 0
            UserKey uk(16);
            uint32_t key_size = uk.GetSize();
            uint32_t value_size = 100+sizeof(UserValue);
            uint64_t nr_free_node = 0;//kFreeBufferSize/value_size;
            for(int id = 0 ; id < kMaxAllocatorPool ; id++){
                UserValue *uv;
#ifdef ALLOCATOR_CHECK
                uv_allocator_[id].SetID(0);
#endif
                for(uint64_t i = 0 ; i < nr_free_node ; i++){
                    uv = new UserValue(sizeof(UserValue), &uv_allocator_[id]);
#ifdef ALLOCATOR_CHECK
                    uv_allocator_[id].NoFree(uv);
#else
                    uv_allocator_[id].Free(uv);

#endif
                }
            }
            /*
             * UserValue life cycle : "Put(Store())" ~ "KeyBlock Submission in WorkerThread"
             * UserKey life cycle : "Put(Store())" ~ (KB Submission)(Flush) ~ Eviction"
             */
            nr_free_node *= 2;
            for(int id = 0 ; id < kMaxAllocatorPool ; id++){
                UserKey *uk;
#ifdef ALLOCATOR_CHECK
                uk_allocator_[id].SetID(1);
#endif
                for(uint64_t i = 0 ; i < nr_free_node ; i++){
                    uk = new UserKey(kMaxKeySize, &uk_allocator_[id]);
#ifdef ALLOCATOR_CHECK
                    uk_allocator_[id].NoFree(uk);
#else
                    uk_allocator_[id].Free(uk);
#endif
                }
            }
            /* A RequestNode can contains 32 key-value pairs*/
            //nr_free_node /= 16;
            for(int id = 0 ; id < kMaxAllocatorPool ; id++){
                RequestNode *rn;
#ifdef ALLOCATOR_CHECK
                request_node_allocator_[id].SetID(2);
#endif
                for(uint64_t i = 0 ; i < nr_free_node ; i++){
                    rn = new RequestNode(&request_node_allocator_[id]);
#ifdef ALLOCATOR_CHECK
                    request_node_allocator_[id].NoFree(rn);
#else
                    request_node_allocator_[id].Free(rn);
#endif
                }
            }
#endif
            //free_request_node_spinlock_ = new Spinlock[kWriteWorkerCount];
            //free_request_node_ = new std::deque<RequestNode*>[kWriteWorkerCount];
            logcleanup_data_ = new WorkerData[kWriteWorkerCount];

#ifdef TRACE_MEM
            g_unloaded_skt_cnt = 0;
            g_io_cnt = 0;
            g_evicted_key = 0;
            g_evicted_skt = 0;
            g_evict_dirty_canceled= 0;
            g_evict_skt_busy_canceled = 0;
            g_evict_skt_next_deleted_canceled = 0;
            g_prefetch_after_evict= 0;
            g_evict_call_cnt = 0;
            g_skt_loading = 0;
            g_skt_flushing = 0;
#ifdef KV_TIME_MEASURE
            g_flush_cum_time = 0;
            g_flush_start_count = 0;
            g_flush_end_count = 0;
            g_flush_max_time = 0;

            g_evict_cum_time = 0;
            g_evict_count = 0;
            g_evict_max_time = 0;

            g_write_io_flush_cum_time = 0;
            g_write_io_flush_count = 0;
            g_write_io_flush_max_time = 0;

            g_skt_flush_cum_time = 0;
            g_skt_flush_count = 0;
            g_skt_flush_max_time = 0;
#endif


#endif
#if 0
            printf("[%d :: %s]lockfree_skt_cache_ count & capacity = %ld : %ld \n", __LINE__, __func__, lockfree_skt_cache_->GetSize(), lockfree_skt_cache_->GetCapacity());
            printf("[%d :: %s]lockfree_skt_dirty_ count & capacity = %ld : %ld \n", __LINE__, __func__, lockfree_skt_dirty_->GetSize(), lockfree_skt_dirty_->GetCapacity());
            printf("[%d :: %s]lockfree_write_cache_ count & capacity = %ld : %ld \n", __LINE__, __func__, lockfree_write_cache_->GetSize(), lockfree_write_cache_->GetCapacity());
            printf("[%d :: %s]lockfree_read_cache_ count & capacity = %ld : %ld \n", __LINE__, __func__, lockfree_read_cache_->GetSize(), lockfree_read_cache_->GetCapacity());
            printf("[%d :: %s]lockfree_iter_buffer_ count& capacity = %ld : %ld \n", __LINE__, __func__, lockfree_iter_buffer_->GetSize(), lockfree_iter_buffer_->GetCapacity());
            printf("[%d :: %s]free_uservalue_ count& capacity = %ld : %ld \n", __LINE__, __func__, free_uservalue_->GetSize(), free_uservalue_->GetCapacity());
#endif

            /**
             * Key count calculation
             * User Key Meta Size : 16  + α Byte  = 4(Shared/Non-shared key size) + 8(Smallest ikey seq) + 4( smallest ivalue size) + α(User Key size)
             * Column Information Size : 22 × β Byte  = (1(property bit) + 1(Col ID Gap) + 4(iValue size) + 8(iKeu seq #) + 8(TTL))  × β(Column count)
             * Maximum Key count : 4K
             *
             */
            /////////////////////****ADD FOR PACKING****////////////////////
            //Support 128 Billions(1 << (32+5)) TRXN per DB Open(Intialized to 1 << TGID_SHIFT )
            completed_trxn_group_list_.push_front(TrxnGroupRange(0)); 
            completed_trxn_group_list_.push_back(TrxnGroupRange(0xFFFFFFFF)); 
            ////////////////////////////////////////////////////////////////

            // Start SKTable cache thread
            skt_flush_thread_tid_ = env_->StartThread(&SKTFlushThread_, this);
            skt_eviction_thread_tid_ = env_->StartThread(&SKTEvictionThread_, this);
            kbm_update_thread_tid_ = env_->StartThread(&KBMUpdateThread_, this);
        }

#ifdef CODE_TRACE
    uint64_t new_alloc = 0;
    uint64_t pop_alloc = 0;
#endif
    Manifest::~Manifest() 

    {
        SKTableMem* skt = NULL;

        if(normal_start_)
        {
            // write manifest
            BuildManifest();

            // Delete the log record
            // If it exists when InDB is restarted, DB is dirty and needs a recovery.
            Status s = env_->Delete(GetMetaKey(dbhash_, kLogRecord), false);
            if (!s.ok() && !s.IsNotFound()) printf("failed to delete kLogRecord\n");
        }

        sktlist_.SkipListCleanUp();
#if 0
        DoubleLinkedListWithLock<SKTableMem>* skt_link = NULL;
        while(!skt_sorted_list_.empty()) {
            skt_link = skt_sorted_list_.newest();
            assert(skt_link);
            skt = skt_link->GetOwner();
            assert(skt);
            delete skt;
#ifdef INSDB_GLOBAL_STATS
            g_del_skt_cnt++;
#endif
        }
#else
        SimpleLinkedNode* sorted_node = NULL;
        while((sorted_node = skt_sorted_simplelist_.newest())) {
            skt = GetSKTableMem(sorted_node);
            skt_sorted_simplelist_.Delete(sorted_node); 
            assert(skt);
            /* 
             * It does not need to aquire read lock(sktdf_update_mu_)
             * because no one flush SKTable(no SKTDF update)
             */
            if(skt->IsKeymapLoaded()){

                /* Delete Arena */
                skt->DeleteSKTDeviceFormatCache(this);
            }
            delete skt;
#ifdef INSDB_GLOBAL_STATS
            g_del_skt_cnt++;
#endif
        }
#endif
        uint32_t nr_node = 0;
        uint32_t nr_stale_node = 0;

        //CleanupRequestNodePool();
        gcinfo_.CleanupGCInfo();

        if (uk_count_) abort();

        nr_node = 0;

        if(nr_node || nr_stale_node)
            printf("[%d :: %s]%u nodes are deleted from lockfree_prefetch_buffer_ %u failed\n", __LINE__, __func__, nr_node, nr_stale_node);

        kb_cache_->Clear(this);
        kbpm_cache_->Clear(this);
        uint32_t total_kbpm_in_hash = 0;
        uint32_t total_kbpm_has_ref = 0;
        uint32_t total_kbpm_has_ref_bit = 0;
        uint32_t total_kbpm_has_prefetch = 0;
        uint32_t total_kbpm_has_dummy = 0;
        uint32_t total_kbpm_has_origin = 0;
        uint32_t total_kbpm_has_no_origin = 0;
        uint32_t total_kbpm_has_pending_update = 0;
        uint32_t total_kbpm_has_set_w_protection = 0;
        uint32_t total_kbpm_has_none = 0;
        if (!kbpm_hashmap_->empty()) {
            for (auto it = kbpm_hashmap_->cbegin(); it != kbpm_hashmap_->cend();) {
                KeyBlockPrimaryMeta *kbpm = it->second;
                if (kbpm) {
                    total_kbpm_in_hash++;
                    if (kbpm->CheckRefBitmap() !=0 ) total_kbpm_has_ref_bit++;
                    if (kbpm->IsKBMPrefetched()) total_kbpm_has_prefetch++;
                    if (kbpm->IsDummy()) {
                        total_kbpm_has_dummy++;
                    } else {
                        if (kbpm->GetOriginalKeyCount()) total_kbpm_has_origin++;
                        else total_kbpm_has_no_origin++;
                    }
                    if (kbpm->GetPendingUpdatedValueCnt()) total_kbpm_has_pending_update++;
                    if (kbpm->SetWriteProtection()) total_kbpm_has_set_w_protection++;
                    kbpm->ClearWriteProtection();
                    if (kbpm->CheckRefBitmap() == 0) total_kbpm_has_none++;
                    FreeKBPM(kbpm);
                    it = kbpm_hashmap_->erase(it);
                    kbpm_cnt_.fetch_sub(1, std::memory_order_relaxed);
                } else {
                    ++it;
                }
            }
        }
        if (total_kbpm_in_hash) {
#ifdef MEM_TRACE
            printf("total kbpm in hash (%d): ref (%d) bit(%d) prefetch(%d) none(%d) dummy(%d) has_orig(%d) has_no_orig(%d) has_pending_update(%d) has_w_protection(%d)\n",
                    total_kbpm_in_hash, total_kbpm_has_ref, total_kbpm_has_ref_bit, total_kbpm_has_prefetch, total_kbpm_has_none,
                    total_kbpm_has_dummy, total_kbpm_has_origin, total_kbpm_has_no_origin, total_kbpm_has_pending_update, total_kbpm_has_set_w_protection);
        std::cout << kb_cache_->to_string(); 
        std::cout << kbpm_cache_->to_string();
#endif
        }

        delete uv_pool_;
        delete cn_pool_;
        delete uk_pool_;
        delete req_pool_;
        delete kb_pool_;
        delete kbpm_pool_;
        delete [] logcleanup_data_;
        CleanupFreeSKTableBuffer();
        //delete [] free_request_node_;
        //delete [] free_request_node_spinlock_;
        Status s = Env::Default()->Flush(kWriteFlush);
        if(!s.ok()){
            printf("[%d :: %s]Flush Fail \n", __LINE__, __func__);
            abort();
        }

#ifdef CODE_TRACE
        printf("[%d :: %s]new alloc(%ld) pop alloc(%ld)\n", __LINE__, __func__,new_alloc, pop_alloc);
#endif

#ifdef USE_HOTSCOTCH
        if(!hopscotch_key_hashmap_->empty())
            abort();
        delete hopscotch_key_hashmap_;
#endif
        delete [] kbpm_pad_;
        delete [] kb_pad_;
        if(!key_hashmap_->empty())
            abort();
        delete key_hashmap_;
        if(!flushed_key_hashmap_->empty())
            abort();
        delete flushed_key_hashmap_;

#ifdef MEM_TRACE
        printf("total flush miss %ld, hit %ld\n", GetMissFlush(), GetHitFlush());
#endif
        delete kbpm_hashmap_;
        delete kb_cache_;
        delete kbpm_cache_;
        delete[] TtlList_;
        delete[] TtlEnabledList_;
    }

    /////////////////////////////////////////// START: SKTableMem Skiplist ///////////////////////////////////////////
    SKTableMem* Manifest::FindSKTableMem(const Slice& ukey) {
        return sktlist_.SearchLessThanEqual(ukey);
    }

    bool Manifest::InsertSKTableMem(SKTableMem* table) {
        return sktlist_.Insert(table->GetBeginKeySlice(), table);
    }

    SKTableMem* Manifest::RemoveSKTableMemFromSkiplist(SKTableMem* table) {

        /*It can not be deleted by SKTable evictor because the caller, FlushSKTableMem(), has lock */
        /* table->RemoveSKTableCacheNode(); */
        return sktlist_.Remove(table->GetBeginKeySlice());
    }

    SKTableMem* Manifest::Next(SKTableMem *cur) {
        return sktlist_.Next(cur);
    }

    SKTableMem* Manifest::Prev(SKTableMem *cur) {
        return sktlist_.Prev(cur);
    }

    SKTableMem* Manifest::GetFirstSKTable() {
        return sktlist_.First();
    }

    SKTableMem* Manifest::GetLastSKTable() {
        return sktlist_.Last();
    }

    SKTableMem* Manifest::GetFromNode(void *anon_node) {
        return (SKTableMem*)sktlist_.GetDataFromNode(anon_node);
    }
    uint64_t Manifest::GetEstimateNrSKTable(Slice key)
    {
        return sktlist_.EstimateCount(key);
    }
    /////////////////////////////////////////// END  : SKTableMem Skiplist ///////////////////////////////////////////

    /////////////////////////////////////////// START: Manifest/InSDB Creation/Recovery/Initializtion/Deletion ///////////////////////////////////////////
    Status Manifest::ReadManifest(std::string* contents) {
        // read manifest data
        Status s;
        char *buff = new char [kDeviceRquestMaxSize];
        int32_t value_size = kDeviceRquestMaxSize;
        uint16_t num_mf = 0;
        assert(buff);
        InSDBKey mf_key = GetMetaKey(dbhash_, kManifest, 0);
        s = env_->Get(mf_key, buff, &value_size);
        if (!s.ok()){
            delete [] buff;
            return s.NotFound("Manifest does not exist");
        }
        uint32_t mf_size = DecodeFixed32(buff);
        uint32_t padding_size = (mf_size % kRequestAlignSize) ? (kRequestAlignSize - (mf_size % kRequestAlignSize)) : 0;
        uint32_t remaining_value_size = 0;
        if (options_.disable_io_size_check) {
            remaining_value_size = mf_size + padding_size;
            if (remaining_value_size > kDeviceRquestMaxSize) {
                value_size = kDeviceRquestMaxSize;
                remaining_value_size -= kDeviceRquestMaxSize;
            } else {
                value_size = remaining_value_size;
                remaining_value_size = 0;
            }
        }
        uint32_t crc = crc32c::Unmask(DecodeFixed32(buff + MF_OFFSET_CRC));
        contents->resize(0);
        contents->append(buff, value_size);
        /* calc buffer size */
        num_mf = (mf_size + padding_size +  kDeviceRquestMaxSize- 1) / kDeviceRquestMaxSize;
        for (int i = 1; i < num_mf; i++) {
            mf_key.split_seq = i;
            value_size = kDeviceRquestMaxSize;
            s = env_->Get(mf_key, buff, &value_size);
            if (!s.ok()) {
                for (int j = 0; j < i; j++) {
                    mf_key.split_seq = j;
                    env_->Delete(mf_key);
                }
                delete [] buff;
                return s.NotFound("Manifest does not exist");
            }
            if (options_.disable_io_size_check) {
                if (remaining_value_size > kDeviceRquestMaxSize) {
                    value_size = kDeviceRquestMaxSize;
                    remaining_value_size -= kDeviceRquestMaxSize;
                } else {
                    value_size = remaining_value_size;
                    remaining_value_size = 0;
                }
            }
            contents->append(buff, value_size);
        }
        contents->resize(mf_size);

        /* calculate crc */
        Slice crc_data(*contents);
        crc_data.remove_prefix(8);
        uint32_t actual = crc32c::Value(const_cast<char*>(crc_data.data()), crc_data.size());
        if (crc != actual) {
            for (int i = 0; i < num_mf; i++) {
                mf_key.split_seq = i;
                env_->Delete(mf_key);
            }
            delete [] buff;
            return s.Corruption("manifest mismatches");
        }
        delete [] buff;
        return s.OK();
    }

    Status Manifest::DeleteManifest() {
        // read manifest data
        Status s;
        char * buff = new char [kDeviceRquestMaxSize];
        int32_t value_size = kDeviceRquestMaxSize;
        assert(buff);
        InSDBKey mf_key = GetMetaKey(dbhash_, kManifest, 0);
        s = env_->Get(mf_key, buff, &value_size);
        if (!s.ok()) {
            delete [] buff;
            return s.OK();
        }

        uint32_t mf_size = DecodeFixed32(buff);
        uint32_t padding_size = (mf_size % kRequestAlignSize) ? (kRequestAlignSize - (mf_size % kRequestAlignSize)) : 0;
        uint16_t num_mf = (mf_size + padding_size +  kDeviceRquestMaxSize- 1) / kDeviceRquestMaxSize;


        for (int i = 0; i < num_mf; i++) {
            mf_key.split_seq = i;
            env_->Delete(mf_key);
        }
        delete [] buff;
        return s.OK();
    }

    void Manifest::InitNextInSDBKeySeqNum(int old_worker_count, uint64_t* seq) {
        int nr_seq = (old_worker_count < kWriteWorkerCount) ? old_worker_count : kWriteWorkerCount;
        for (int i = 0; i < nr_seq; i++) {
            logcleanup_data_[i].InitWorkerNextIKeySeq(seq[i]);
        }
        if (nr_seq != kWriteWorkerCount) { /* it means old_worker_count is less than kWriteWorkerCount */
            for(int i = nr_seq; i < kWriteWorkerCount; i++) {
                logcleanup_data_[i].InitWorkerNextIKeySeq(GenerateNextInSDBKeySeqNum());
            }
        }
    }

    /* Load Manifest from device and then initialize the class Manifest*/
    Status Manifest::InitManifest() {
        Status s;
        std::string mf_data;
        s = ReadManifest(&mf_data);
        if (!s.ok()) return s;
        SKTableMem* skt = NULL;
        const char* data = mf_data.data();

        next_InSDBKey_seq_num_ = DecodeFixed64(data + MF_OFFSET_LAST_IKEY_SEQ);
        sequence_number_ = DecodeFixed64(data + MF_OFFSET_LAST_SEQ);
        next_sktable_id_ = DecodeFixed32(data + MF_OFFSET_LAST_SKT_SEQ);

        uint32_t sktinfo_count = DecodeFixed32(data + MF_OFFSET_NUM_SKT_INFO);
        uint32_t worker_count = DecodeFixed32(data + MF_OFFSET_NUM_WORKER);
        uint64_t* next_seq = new uint64_t[worker_count];
        uint32_t* key_count= new uint32_t[kMaxColumnCount];
        uint64_t skt_id = 0;
        uint32_t keymap_node_size = 0;
        uint32_t keymap_hash_size = 0;
        uint32_t offset = MF_OFFSET_NEXT_WORKER_INFO;
        uint32_t begin_key_size = 0;
        /* calc buffer size */
        InSDBThreadsState state;
        Slice parse(mf_data);
        parse.remove_prefix(offset);
        assert(worker_count);
        for (uint32_t i = 0; i < worker_count; i++) {
            next_seq[i] = DecodeFixed64(parse.data());
            parse.remove_prefix(8);
        }
        /* update logcleaup_data_[] next_ikey_seq_ */
        InitNextInSDBKeySeqNum(worker_count, next_seq);

        assert(sktinfo_count);
        kMaxSKTableSize * sktinfo_count;
        /* Randomly fill 60% of Cache with SKTableDF
         * Assumtion : The size of SKTableDF is kMaxCacheSize
         */
        Random rnd(MF_HASH_SEED);
        uint64_t cacheable_skt_cnt = (6*kMaxCacheSize) / (10*kMaxSKTableSize);

        for (uint32_t i = 0; i < sktinfo_count; i++) { //Parseing SKTable infomation.
            skt_id = DecodeFixed32(parse.data());
            parse.remove_prefix(4);
            keymap_node_size = DecodeFixed32(parse.data());
            parse.remove_prefix(4);
            //keymap_hash_size = DecodeFixed32(parse.data());
            //parse.remove_prefix(4);
            skt_id &= ~MF_SKT_ID_MASK;
            if(!(GetVarint32(&parse, &begin_key_size)))
                port::Crash(__FILE__,__LINE__);
            KeySlice begin_key(parse.data(), begin_key_size);
            parse.remove_prefix(begin_key_size);
            //uint16_t col_info_size = DecodeFixed16(parse.data());
            //parse.remove_prefix(2);
            //Slice col_slice(parse.data(), col_info_size);
#ifdef USE_HASHMAP_RANDOMREAD
            skt = new SKTableMem(this, begin_key, skt_id, keymap_node_size, keymap_hash_size); //parseing SKTable
#else
            skt = new SKTableMem(this, begin_key, skt_id, keymap_node_size); //parseing SKTable
#endif
            //parse.remove_prefix(col_info_size);
#ifdef INSDB_GLOBAL_STATS
            g_new_skt_cnt++;
#endif
            assert(skt);
            /* Randomly fill 60% of Cache with SKTableDF */
            if ( (rnd.Next()%sktinfo_count) < cacheable_skt_cnt )
                skt->PrefetchSKTable(this, false/*To clean cache*/);
        }
#ifdef CODE_TRACE
        printf("[%d :: %s]InitManifest current sequence number %ld\n", __LINE__, __func__, GetLastSequenceNumber());
#endif
        delete [] next_seq;
        delete [] key_count;
        return s.OK();
    }

    //#pragma GCC push_options
    //#pragma GCC optimize ("O0")
    //#pragma GCC pop_options
    //Status __attribute__((optimize("O0"))) Manifest::BuildManifest() 
    Status Manifest::BuildManifest() 
    {
        Status s;
        std::string contents;
        uint32_t num_sktable_count = 0;
        uint16_t num_mf = 0;
        uint32_t skt_id;
#ifdef TRACE_MEM
        uint32_t num_fetched = 0;
#endif
        uint32_t mf_size = 0, remain_size = 0, req_size = 0;
        contents.resize(0);
        contents.append(8, (char)0); /* size and crc */
        PutFixed64(&contents, 0 /* next_InSDBKey_seq_num_*/);
        PutFixed64(&contents, 0 /* sequence_number_ */);
        PutFixed32(&contents, 0 /* next_sktable_id_*/);
        PutFixed32(&contents, 0 /* num_sktable_count */);
        PutFixed32(&contents, kWriteWorkerCount);
        for (int i = 0; i < kWriteWorkerCount; i++) {
            PutFixed64(&contents, logcleanup_data_[i].GetWorkerNextIKeySeqUnSafe());
        }
        /**
         * Flush SKTable from last to first for split SKTables
         */
        SKTableMem* skt = GetLastSKTableInSortedList();
        while(skt) { // set sktable infomation.
            skt_id = skt->GetSKTableID();
            /*
             * Flush sktable if Update Count bigger than zero or
             * Last flushed trnasaction ID is less than current transaction ID.
             * Event if no explict update on sktable, transaction ID is changed means
             * there is possibility that uncompleted transation is changed to completed staus.
             */
            if (skt->IsKeymapLoaded()) {
                skt->DeleteSKTDeviceFormatCache(this);
            }
            if(skt->KeyQueueKeyCount() ){
                abort();
            } else if(IsNextSKTableDeleted(skt->GetSKTableSortedListNode())){
                abort();
            }else if (skt->IsFetchInProgress()) {
                /*
                 * TODO : need to check future for this situation
                 */
                abort();
            }
            /* 
             * Need to remove sktable from sktable skiplist and delete sktable
             */

            skt = PrevSKTableInSortedList(skt->GetSKTableSortedListNode());
        }
        skt = GetFirstSKTable();
        while(skt) { // set sktable infomation.
            skt_id = skt->GetSKTableID();
            assert(!skt->IsFetchInProgress());
            assert(!IsNextSKTableDeleted(skt->GetSKTableSortedListNode()));
            assert(!skt->KeyQueueKeyCount());
            assert(!skt->IsDeletedSKT());
            assert(skt_id != 0);
            PutFixed32(&contents, skt_id);
            PutFixed32(&contents, skt->GetKeymapNodeSize());
#ifdef USE_HASHMAP_RANDOMREAD
            PutFixed32(&contents, skt->GetKeymapHashSize());
#endif
            KeySlice beginkey = skt->GetBeginKeySlice();
            PutVarint32(&contents, beginkey.size());
            contents.append(beginkey.data(), beginkey.size());
#if 0
            /*No Save the Column Info*/
            uint32_t cur_size = contents.size();
            contents.append(sizeof(uint16_t),0);// to store column info size
            skt->GetBeginKeyColInfo(&contents);
            EncodeFixed16((const_cast<char*>(contents.data()) + cur_size), (uint16_t)(contents.size()-(cur_size+sizeof(uint16_t))));
#endif
            num_sktable_count++;
            /* 
             * Need to remove sktable from sktable skiplist and delete sktable
             */

            skt = skt->Next(this);
        }
#ifdef TRACE_MEM
        printf("!!!!! build Manifest skt count[%d] fetched stk count[%d]\n",
                num_sktable_count, num_fetched);
#endif

#ifdef CODE_TRACE
        printf(" BuildManifest current sequence number %ld\n", GetLastSequenceNumber());
#endif
        EncodeFixed64(&contents[MF_OFFSET_LAST_IKEY_SEQ], next_InSDBKey_seq_num_);
        EncodeFixed64(&contents[MF_OFFSET_LAST_SEQ], sequence_number_);
        EncodeFixed32(&contents[MF_OFFSET_LAST_SKT_SEQ], next_sktable_id_);
        EncodeFixed32(&contents[MF_OFFSET_NUM_SKT_INFO], num_sktable_count);

        /* calcualte crc and set */
        Slice crc_data(contents);
        crc_data.remove_prefix(8);
        uint32_t crc = crc32c::Value(crc_data.data(), crc_data.size());
        EncodeFixed32(const_cast<char*>(contents.data()) + 4, crc32c::Mask(crc));
        /* get size and set */
        mf_size = contents.size();
        EncodeFixed32(const_cast<char*>(contents.data()), mf_size);

        if (mf_size % kRequestAlignSize) { // add padding
            uint32_t padding_size = kRequestAlignSize - (mf_size % kRequestAlignSize);
            contents.append(padding_size, 0xff);
            mf_size = contents.size();
        }
        num_mf = (mf_size + kDeviceRquestMaxSize - 1) / kDeviceRquestMaxSize;

        char *data = &contents[0];
        remain_size = mf_size;

        InSDBKey mf_key = GetMetaKey(dbhash_, kManifest, 0);
        for (uint16_t i = 0; i < num_mf; i++) { // split and save request.
            mf_key.split_seq = i;
            req_size = (remain_size < kDeviceRquestMaxSize) ? remain_size : kDeviceRquestMaxSize;
            Slice value(data, req_size);
            s = env_->Put(mf_key, value, true);
            if (!s.ok()) return Status::IOError("Fail to write Manifest");
            remain_size -= req_size;
            data += req_size;
        }
        env_->Flush(kWriteFlush);
        return Status::OK();
    }

    /**
      <Super SKTable Device format>
      Super SKTable::
      crc (4B)
      size of DBName(2B)
      DBName(var)
      size of comparator name(2B)
      Comparator name(var)
      Max ivalue size(2B)
      count of colums(2B)
      First SKTable ID(4B)
      */
    Status Manifest::ReadSuperSKTable(SuperSKTable *super_skt) {
        Status s;
        int value_size = 0;
        char* buff = new char[kMaxSKTableSize];
        char* sskt_buf = buff;
        value_size = ReadSKTableMeta(sskt_buf, kSuperSKTable, 0, kSyncGet, kMaxSKTableSize);
        if (!value_size) {
            delete [] buff;
            return s.NotFound("super sktable is not exist");
        }
#if 0
        uint32_t crc = crc32c::Unmask(DecodeFixed32(sskt_buf));
#endif
        sskt_buf += sizeof(uint32_t);
        uint32_t offset = 0;
        uint16_t dbname_size = DecodeFixed16(sskt_buf);
        super_skt->dbname = std::string(sskt_buf + sizeof(uint16_t), dbname_size);
        offset = dbname_size + sizeof(uint16_t);
        uint16_t compName_size = DecodeFixed16(sskt_buf + offset);
        super_skt->compName = std::string(sskt_buf + offset + sizeof(uint16_t), (size_t)compName_size);
        offset += (compName_size + sizeof(uint16_t));
        super_skt->max_column_count = DecodeFixed16(sskt_buf + offset);
        offset += sizeof(uint16_t);
        super_skt->max_req_per_kb = DecodeFixed16(sskt_buf + offset);
        offset += sizeof(uint16_t);
        super_skt->max_request_size = DecodeFixed32(sskt_buf + offset);
        offset += sizeof(uint32_t);
        super_skt->max_sktable_size = DecodeFixed32(sskt_buf + offset);
        offset += sizeof(uint32_t);
        super_skt->max_internalkey_size = DecodeFixed32(sskt_buf + offset);
        offset += sizeof(uint32_t);
        super_skt->first_skt_id = DecodeFixed32(sskt_buf + offset);
        delete [] buff;
        return s.OK();
    }

    Status Manifest::GetSuperSKTable() {
        Status s;
        SuperSKTable super_skt;
        s = ReadSuperSKTable(&super_skt);
        if (!s.ok()) return s;
        if (super_skt.dbname != dbname_)
            return s.NotSupported("dbname is not matched: ", super_skt.dbname);
        std::string compNameOrig(options_.comparator->Name());
        if (super_skt.compName != compNameOrig)
            return s.NotSupported("comparator is not matched: ", super_skt.compName);
        if ( super_skt.max_column_count != kMaxColumnCount)
            return s.NotSupported("column count mismatched: ", std::to_string(super_skt.max_column_count));
        if (super_skt.max_request_size != kMaxRequestSize) { 
            kMaxRequestSize = super_skt.max_request_size;
        }
        if (kMaxRequestPerKeyBlock != super_skt.max_req_per_kb) {
            fprintf(stderr, "Not matche Reqeust per KeyBlock db %d  superblock %d", kMaxRequestPerKeyBlock, super_skt.max_req_per_kb);
            abort();
        }
        if (kMaxSKTableSize != super_skt.max_sktable_size) {
            fprintf(stderr, "Not matche Max SKTable size db %d  superblock %d", kMaxSKTableSize ,super_skt.max_sktable_size);
            abort();
        }
        if (kMaxInternalValueSize != super_skt.max_internalkey_size) {
            fprintf(stderr, "Not matche Max Internal Value size db %d  superblock %d", kMaxInternalValueSize ,super_skt.max_internalkey_size);
            abort();
        }
        return s.OK();
    }

    Status Manifest::BuildSuperSKTable() {
        Status s;
        std::string contents;
        contents.resize(0);
        contents.append(4, (char)0);
        PutFixed16(&contents, dbname_.size());
        contents.append(dbname_.data(), dbname_.size());
        std::string compName(options_.comparator->Name());
        PutFixed16(&contents, compName.size());
        contents.append(compName.data(), compName.size());
        PutFixed16(&contents, kMaxColumnCount);
        PutFixed16(&contents, kMaxRequestPerKeyBlock);
        PutFixed32(&contents, kMaxRequestSize);
        PutFixed32(&contents, kMaxSKTableSize);
        PutFixed32(&contents, kMaxInternalValueSize);
        uint32_t first_skt_id = 1;
        PutFixed32(&contents, first_skt_id);
        if (contents.size() % kRequestAlignSize) {
            uint32_t padding_size = kRequestAlignSize - (contents.size() % kRequestAlignSize);
            contents.append(padding_size, 0xff);
        }
        Slice crc_data(contents);
        crc_data.remove_prefix(4);
        uint32_t crc = crc32c::Value(crc_data.data(), crc_data.size());
        EncodeFixed32(const_cast<char*>(contents.data()), crc32c::Mask(crc));
        InSDBKey superkey = GetMetaKey(dbhash_, kSuperSKTable, 0);
        Slice value(contents);
        s = env_->Put(superkey, value);
        if (!s.ok()) return Status::IOError("fail to write SuperSKTable");
        return Status::OK();
    }

#ifdef USE_HASHMAP_RANDOMREAD
    Status Manifest::BuildFirstSKTable(int32_t skt_id, uint32_t &io_size, uint32_t &hash_data_size)
#else
    Status Manifest::BuildFirstSKTable(int32_t skt_id, uint32_t &io_size)
#endif
    {
        Status s;

        std::string contents;
        // FiX me  -- need to add option.
        Keymap* keymap = new Keymap(KeySlice(0), GetComparator());
        Arena* arena = new Arena(4096);
        keymap->InitArena(arena, options_.use_compact_key);
        keymap->InsertArena(arena);
         
#ifdef USE_HASHMAP_RANDOMREAD
        std::string hash_data;
        keymap->BuildHashMapData(hash_data);
        hash_data_size = hash_data.size();
        char* data = const_cast<char*>(hash_data.data());
        uint32_t crc = crc32c::Value(data + 4 /*crc 4B*/, hash_data_size -4/*crc 4B */);
        *((uint32_t*)data) = crc32c::Mask(crc);
        InSDBKey hashkey = GetMetaKey(GetDBHash(), kHashMap, skt_id);
        Slice hash_value(data, hash_data_size);
        s = env_->Put(hashkey, hash_value);
        if (!s.ok()) return s;
#endif

        uint32_t keymap_size = keymap->GetAllocatedSize();
        uint32_t peding_size = ((keymap_size % kRequestAlignSize) ? (kRequestAlignSize - (keymap_size % kRequestAlignSize)) : 0); /* 64 bit padding */
        if (keymap_size + peding_size > 4096) keymap_size = 4096;
        else keymap_size += peding_size;
#ifdef USE_HASHMAP_RANDOMREAD
        data = arena->GetBaseAddr();
#else
        char* data = arena->GetBaseAddr();
#endif
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)data;
        hdr->skt_id = skt_id;
        hdr->prealloc_skt_id = GenerateNextSKTableID();
        hdr->next_skt_id = 0;
        hdr->start_colinfo = 0;
        hdr->last_colinfo = 0;
        hdr->keymap_size = keymap_size - sizeof(KEYMAP_HDR); /* exclude KEMAP_HDR */
        /* calc CRC and set */
#ifdef USE_HASHMAP_RANDOMREAD
        crc = crc32c::Value(data + 4 /*crc 4B*/, hdr->keymap_size + sizeof(KEYMAP_HDR) -4/*crc 4B*/);
#else
        uint32_t crc = crc32c::Value(data + 4 /*crc 4B*/, hdr->keymap_size + sizeof(KEYMAP_HDR) -4/*crc 4B*/);
#endif
        hdr->crc = crc32c::Mask(crc);
        InSDBKey superkey = GetMetaKey(dbhash_, kSKTable, skt_id);
        Slice value(data, keymap_size);
        io_size = keymap_size;
        s = env_->Put(superkey, value);
        if (!s.ok()){
            printf("[%d :: %s]fail to write first SKTable\n", __LINE__, __func__);
        }
        delete keymap;
        return s;
    }

    Status Manifest::CreateNewInSDB() {
        Status s;
        s = BuildSuperSKTable();
        InitNextInSDBKeySeqNum(0, NULL);
        if (!s.ok()) return s;
        KeySlice zerokey(0);
        uint32_t first_skt_id = 1;
        uint32_t io_size = 0;
        SetNextSKTableID(first_skt_id);
#ifdef USE_HASHMAP_RANDOMREAD
        uint32_t hash_data_size = 0;
        s = BuildFirstSKTable(first_skt_id, io_size, hash_data_size);
#else
        s = BuildFirstSKTable(first_skt_id, io_size);
#endif
        if (!s.ok()) return s;
        Slice col_slice(0);
#ifdef CODE_TRACE
        printf("[%d :: %s]sktdf_size : %d\n", __LINE__, __func__,sktdf_size);
#endif
#ifdef USE_HASHMAP_RANDOMREAD
        SKTableMem* first_skt = new SKTableMem(this, zerokey, first_skt_id, io_size, hash_data_size);
#else
        SKTableMem* first_skt = new SKTableMem(this, zerokey, first_skt_id, io_size);
#endif


#ifdef CODE_TRACE
        printf("[%d :: %s]First SKTable : %p\n", __LINE__, __func__, first_skt);
#endif
#ifdef INSDB_GLOBAL_STATS
        g_new_skt_cnt++;
#endif 
        first_skt->LoadSKTableDeviceFormat(this);

        /*No lock because there is no other threads*/
#ifdef CODE_TRACE
        printf("[%d :: %s] Set Fetch for first SKT\n", __LINE__, __func__);
#endif
        /* There is no key stored yet. A new key will be stored after creating the 1st SKTable */
        InsertSKTableMemCache(first_skt, kCleanSKTCache/*kDirtySKTCache*/, SKTableMem::flags_in_clean_cache);
        return Status::OK();
    }


#if 1 //New(packing) implementation
    int Manifest::ReadSKTableMeta(char* buffer, InSDBMetaType type, uint32_t skt_id, GetType req_type, int buffer_size) {
        int value_size = buffer_size;
        bool from_readahead = false;
        Status s = env_->Get(GetMetaKey(dbhash_, type, skt_id), buffer, &value_size, req_type, IsIOSizeCheckDisabled()?Env::GetFlag_NoReturnedSize:0, &from_readahead);
        if (!s.ok()){
            return 0;
        }
        if(!from_readahead && req_type == kNonblokcingRead) return 0;

#ifdef TRACE_READ_IO
        if (req_type == kSyncGet) {
            g_sktable_sync ++;
            if (from_readahead)
                g_sktable_sync_hit++;
        }
#endif


#if 0
        if(req_type == kNonblokcingRead)
            printf("[ %d : %s ] Success SKT prefetch mechanism\n", __LINE__, __func__);
#endif
        if (!options_.disable_io_size_check) {
            if (type == kSuperSKTable) {
                    if(crc32c::Unmask(DecodeFixed32(buffer)) != crc32c::Value(buffer + sizeof(uint32_t), value_size - sizeof(uint32_t))){
                        fprintf(stderr, "[ %d : %s ]sktable size : %d(crc : actual = 0x%x : 0x%x) %s \n", __LINE__, __func__, value_size, crc32c::Unmask(DecodeFixed32(buffer)), crc32c::Value(buffer + sizeof(uint32_t), value_size - sizeof(uint32_t)), type == kSuperSKTable?"Super SKTable" : "Normal SKTable");
                        fprintf(stderr, "super sktable crs is mismatch\n");
                        return 0;
                    }
            }/*else(type == kSKTable){ SKTDFCacheNode will check the CRC } */
        } else {
            // TODO: compare with size info in SKTable
            value_size = buffer_size;
        }
        return value_size;
    }
#endif

#ifdef USE_HASHMAP_RANDOMREAD
    Arena* Manifest::LoadKeymapFromDevice(uint32_t skt_id, uint32_t keymap_size, uint32_t hash_data_size, KeyMapHashMap*& hashmap,  GetType type)
#else
    Arena* Manifest::LoadKeymapFromDevice(uint32_t skt_id, uint32_t keymap_size, GetType type)
#endif
    {
        /*
         * Asummption:
         * 1. Compressed Data will be saved as below format.
         * [KEYMAP_HDR| compressed data=(from orig src + KEYMAP_HDR to end )] 
         * - compressing start from src + sizeof(KEYMAP_HDR)
         * 2. keymap_hdr->keymap_size = total datasize - sizeof(KEYMAP_HDR);
         */
#ifdef USE_HASHMAP_RANDOMREAD
        std::string hash_data;
        hash_data.resize(hash_data_size);
        char* hash_buf = const_cast<char*>(hash_data.data());
        int hash_size = ReadSKTableMeta(hash_buf, kHashMap, skt_id, type, hash_data_size);
        if (hash_size) { 
            if (hash_size != hash_data_size) {
                printf("[%d :: %s] The SKT %u Hash size is different to recorded size in SKTableMem(actual size : %d || given size : %d)\n", __LINE__, __func__, skt_id,
                        hash_size, hash_data_size);
                hash_data_size = hash_size;
            }
            uint32_t orig_crc = crc32c::Unmask(*(uint32_t *)hash_buf); //get crc
            char* crc_buffer = hash_buf + 4; /* crc(4B) */
            uint32_t actual_crc = crc32c::Value(crc_buffer, hash_data_size - 4/*crc*/);
            if (orig_crc != actual_crc) {
                printf("[%d :: %s] crc mismatch chek this orig(%d) and actual(%d)\n", __LINE__, __func__, orig_crc, actual_crc);
                abort();
            }
            hashmap = BuildKeymapHashmapWithString(hash_data);
        }
#endif


        Arena *arena = nullptr;
        Slice io_buffer = AllocSKTableBuffer(keymap_size);  // It will be freed after submitting SKTable.
        if(io_buffer.size() < keymap_size) abort();
        char* buf = const_cast<char*>(io_buffer.data()); /* get bufer */
        int size = ReadSKTableMeta(buf, kSKTable, skt_id, type, keymap_size);
        if(size){ 
            KEYMAP_HDR *keymap_hdr = (KEYMAP_HDR*)buf;
            /* check size */
            if(keymap_size != size){
                printf("[%d :: %s] The SKTDF size is different to recorded size in SKTableMem(actual size : %d || given size : %d)\n", __LINE__, __func__, size, keymap_size);
                keymap_size = size;
            }
            /* check crc */
            uint32_t orig_crc = crc32c::Unmask(keymap_hdr->crc); //get crc
            char* crc_buffer = buf + 4; /* crc(4B) */
            uint32_t data_size =  (keymap_hdr->keymap_size & KEYMAP_SIZE_MASK);
            uint32_t total_data_size =  (keymap_hdr->keymap_size & KEYMAP_SIZE_MASK) + sizeof(KEYMAP_HDR);
            uint32_t actual_crc = crc32c::Value(crc_buffer, total_data_size-4/*crc*/);
            if (orig_crc != actual_crc) {
                printf("[%d :: %s] crc mismatch chek this orig(%d) and actual(%d)\n", __LINE__, __func__, orig_crc, actual_crc);
                abort();
            } 
            if (keymap_hdr->keymap_size & KEYMAP_COMPRESS) {
                /* It was compressed */
                size_t ulength = 0;
                char* table_buffer = buf + sizeof(KEYMAP_HDR); /* skip KEYMAP_HDR */
                if (!port::Snappy_GetUncompressedLength(table_buffer, data_size, &ulength)) {
                    printf("[%d :: %s] corrupted compressed data\n", __LINE__, __func__);
                    abort();
                } else {
                    uint32_t alloc_size = sizeof(KEYMAP_HDR) + ulength;
                    alloc_size += ((alloc_size % 4096) ? (4096 - (alloc_size % 4096)):0); /* 4KB base padding size */
                    arena = new Arena(alloc_size);
                    char *dest = arena->GetBaseAddr();
                    if (!port::Snappy_Uncompress(table_buffer, data_size, dest + sizeof(KEYMAP_HDR))) {
                        printf("[%d :: %s] corrupted compressed data\n", __LINE__, __func__);
                        abort();
                    }
                    memcpy(dest, buf, sizeof(KEYMAP_HDR));
                }
            }  else {
                uint32_t alloc_size = total_data_size + ((total_data_size % 4096) ? (4096 - (total_data_size % 4096)):0); /* 4KB base padding size */
                arena = new Arena(alloc_size);
                char *dest = arena->GetBaseAddr();
                memcpy(dest, buf, total_data_size); 
            } 
        }
        FreeSKTableBuffer(io_buffer);  // It will be freed after submitting SKTable.
        return arena;
    }


    Status Manifest::ReadAheadSKTableMeta(InSDBMetaType type, uint32_t skt_id, int buffer_size) {
        int value_size = buffer_size;
        InSDBKey skt_key = GetMetaKey(dbhash_, type, skt_id);
#ifdef TRACE_READ_IO
        g_sktable_async++;
#endif
        return env_->Get(skt_key, NULL, &value_size, kReadahead);
    }

    /**
     * TODO TODO  if delete_list & uncommitted trxn list could not be more than 1, nr_key is not necessary.
     * In this case, remove "for loop".
     */
    Status Manifest::DeleteSKTableMeta(InSDBMetaType type, uint32_t skt_id, uint16_t nr_key) {
        Status s;
        InSDBKey skt_key = GetMetaKey(dbhash_, type, skt_id);
        for (uint16_t i = 0; i < nr_key; i ++) {
            skt_key.split_seq = i;
            s = env_->Delete(skt_key);
            if (!s.ok()) return s;
        }
        return s;
    }

    Status Manifest::ReadSKTableInfo(uint32_t skt_id, SKTableInfo* skt_info) {
        int value_size = kMaxSKTableSize;
        char *buff = new char [value_size];
        Status s = env_->Get(GetMetaKey(dbhash_, kSKTable, skt_id), buff, &value_size);
        if (!s.ok()) {
            delete [] buff;
            return s;
        }
#ifdef TRACE_READ_IO
        g_sktable_sync++;
#endif
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)buff;
        skt_info->skt_id = skt_id;
        skt_info->next_skt_id = hdr->next_skt_id;
        skt_info->prealloc_skt_id = hdr->prealloc_skt_id;
        delete [] buff;
        return s.OK();
    }

    Status Manifest::CleanupPreallocSKTable(uint32_t prealloc_skt, uint32_t next_skt_id) {
        Status s;
        SKTableInfo recover;
        uint32_t new_next_skt_id = prealloc_skt;
        while(next_skt_id != new_next_skt_id) {
            s = ReadSKTableInfo(new_next_skt_id, &recover);
            if (s.ok()) {
                DeleteSKTableMeta(kSKTable, new_next_skt_id);
            } else return s;
            new_next_skt_id = recover.next_skt_id;
        }
        return s;
    }

    /**
      Recover DB
      */
    Status Manifest::RecoverInSDB(bool &bNeedInitManifest, bool create_if_missing) {
        bNeedInitManifest = true;
        DBRecovery recoveryWork(this);
        // load the log record
        Status s = recoveryWork.LoadLogRecordKey();
        if(s.ok()) {
            bNeedInitManifest = false;
            if (create_if_missing) {
                /*
                 * Trick to reduce time overhead for recover.
                 * if create_if_missing is set .. destroy db will be fater than recovery.
                 */
                DeleteInSDB();
                return Status::NotFound("Key does not exist");
            }
            // Log record exists. DB is dirty.
            s = recoveryWork.RunRecovery();
            if (!s.ok())
                return Status::Corruption("could not recover DB");
        }

        // return IO error
        return s;
    }

    #if 0
    Status Manifest::DeleteInSDBWithIter() {
        Status s;
        unsigned char iter_handle = 0;
#ifdef NEW_GENERIC_FIRMWARE
        char *buffer = new char[1024*4];
        char *data = NULL;
        bool b_found = false;
        bool b_finished = false;
        int i = 0;
        int sleep_microsecond = 100000; //100ms
        s = env_->IterReq(0xffffffff, dbhash_, &iter_handle, true, true);
        if (!s.ok()) goto exit;
        while(1) {
            env_->SleepForMicroseconds(sleep_microsecond);
            b_found = false;
            s = env_->GetLog(0xD0, buffer, 4096);
            if (!s.ok()) {
                printf("fail to GetLog: %s\n", s.ToString().data());
                goto exit;
            }
#if 0
            data = buffer;
            for (i = 0; i < 16; i++) {
                printf("nHandle(%02x) bOpened(%02x) nIterTyep(%02x) nKeySpaceId(%02x) niterValue(%08x) niterMaks(%08x) nfinished(%02x) \n",
                        data[0], data[1], data[2], data[3],
                        *((unsigned int *)&data[4]), *((unsigned int *)&data[8]), data[12]);
                data += 16;
            }
#endif
            data = buffer;
            for (i = 0; i < 16; i++) {
                if (data[i*16] == iter_handle) { /* check handle */
                    b_found = true;
                    if (data[i*16 + 12] == 0x01 /* check finished */) {
                        b_finished = true;
                    }
                    break;
                }
            }
            if (b_found != true) {
                // printf("iterate handle not exist 0x%02x\n", iter_handle);
                s = Status::NotFound("handle not found");
                goto exit;
            }
            if (b_finished) {
                //printf("End checking 0x%02x iterate delete with get log\n", iter_handle);
                s = Status::OK();
                break;
            }
        }
#else
        int iter_buff_size = 0;
        int read_count = 0;
        uint64_t total_keys_found = 0;
        uint64_t total_iter_read = 0;
        InSDBKey *key_array = NULL;
        char *buffer = new char[1024*32];
        std::set<InSDBKey, InSDBKeyCompare> keyslist;
        assert(buffer);
        int count = 0;
        s = env_->IterReq(0xffffffff, dbhash_, &iter_handle, true);
        if (!s.ok()) goto exit;
        /* iter read */
        while(1) {
            iter_buff_size = 1024*32;
            s = env_->IterRead(buffer, &iter_buff_size, iter_handle);
            if (s.ok()) {
                total_iter_read ++;
                if (!iter_buff_size) continue; /* iterator can report 0 size length */
                count = iter_buff_size/16;
                key_array = (InSDBKey*)buffer;
                keyslist.insert(key_array, key_array + count);
            } else break;
        }
        total_keys_found = keyslist.size();
close_exit:
        env_->IterReq(0xffffffff, dbhash_, &iter_handle, false);
        if (total_keys_found) {
            count = 0;
            for (auto it = keyslist.begin(); it != keyslist.end(); ++it) {
                //printf("%dth deleted entry db_id(%u) type(%d) split_seq(%d) seq(%lu)\n",
                //        count, (*it).db_id, (*it).type, (*it).split_seq, (*it).seq);
                env_->Delete(*it);
                //count++;
            }
            env_->Flush(kWriteFlush);
        }
        if (total_keys_found == 0) s = Status::NotFound("iter cannot read anything");
        else s = Status::OK();
#endif
exit:
        if (buffer) delete [] buffer;
        return s;
    }

#else
    Status Manifest::DeleteInSDBWithIter() {
        Status s;
        unsigned char iter_handle = 0;
        int iter_buff_size = 0;
        uint64_t total_keys_found = 0;
        uint64_t total_iter_read = 0;
#ifndef NEW_GENERIC_FIRMWARE
        InSDBKey *key_array = NULL;
        int count = 0;
#endif
        char *buffer = new char[1024*32];
        std::set<InSDBKey, InSDBKeyCompare> keyslist;
        assert(buffer);
        for (int dev_idx = 0; dev_idx < options_.kv_ssd.size(); dev_idx++) {
            s = env_->IterReq(0xffffffff, dbhash_, &iter_handle, dev_idx, true);
            if (!s.ok()) goto exit;
            /* iter read */
            while(1) {
                iter_buff_size = 1024*32;
                s = env_->IterRead(buffer, &iter_buff_size, iter_handle, dev_idx);
                if (s.ok()) {
                    total_iter_read ++;
                    if (!iter_buff_size) continue; /* iterator can report 0 size length */
    #ifdef NEW_GENERIC_FIRMWARE
                    char *data_buff = buffer;
                    uint32_t key_size = 0;
                    /* read key count */
                    uint32_t key_count = *((uint32_t *)data_buff);
                    iter_buff_size -= sizeof(uint32_t);
                    data_buff += sizeof(uint32_t);
                    for (uint32_t i = 0; i < key_count; i++) {
                        if (iter_buff_size < (int)sizeof(uint32_t)) {
                            printf("check Firmware : unexpected buffer size %s:%d\n", __FILE__, __LINE__);
                            goto close_exit;
                        }
                        /* read key size */
                        key_size = *((uint32_t *)data_buff);
                        iter_buff_size -= sizeof(uint32_t);
                        data_buff += sizeof(uint32_t);

                        if (key_size != sizeof(InSDBKey)) {
                            printf("check Firmware : unexpected buffer size %s:%d\n", __FILE__, __LINE__);
                            goto close_exit;
                        }
                        if (key_size >= 256) {
                            printf("check Firmware : unexpected buffer size %s:%d\n", __FILE__, __LINE__);
                            goto close_exit;
                        }
                        keyslist.insert(*((InSDBKey*)data_buff));
                        iter_buff_size -= sizeof(InSDBKey);
                        data_buff += sizeof(InSDBKey);
                    }
    #else
                    count = iter_buff_size/16;
                    key_array = (InSDBKey*)buffer;
                    keyslist.insert(key_array, key_array + count);
    #endif
                } else break;
            }
            total_keys_found = keyslist.size();
close_exit:
            env_->IterReq(0xffffffff, dbhash_, &iter_handle, dev_idx, false);
            for (auto it = keyslist.begin(); it != keyslist.end(); ++it) {
                //printf("%dth deleted entry db_id(%u) type(%d) split_seq(%d) seq(%lu)\n",
                //        count, (*it).db_id, (*it).type, (*it).split_seq, (*it).seq);
                env_->Delete(*it);
                //count++;
            }
            env_->Flush(kWriteFlush, dev_idx);
        }
        if (total_keys_found == 0) s = Status::NotFound("iter cannot read anything");
        else s = Status::OK();
exit:
        if (buffer) delete [] buffer;
        return s;
    }
#endif

    Status Manifest::DeleteInSDBWithManifest(uint16_t column_count) {
        Status s;
        int sleep_microsecond = 10000; //10ms
        uint32_t max_thread = 16;
        std::string mf_data;
        s = ReadManifest(&mf_data);
        if (!s.ok()) return s;
        SKTMetaInfo *skt_ids = NULL;
        const char* data = mf_data.data();
        uint16_t mf_size = DecodeFixed32(data);
#if 0
        uint64_t insdbkey_seq_num = DecodeFixed64(data + MF_OFFSET_LAST_IKEY_SEQ);
        uint64_t seq_num = DecodeFixed64(data + MF_OFFSET_LAST_SEQ);
        uint32_t last_sktable_id = DecodeFixed32(data + MF_OFFSET_LAST_SKT_SEQ);
#endif
        uint32_t sktinfo_count = DecodeFixed32(data + MF_OFFSET_NUM_SKT_INFO);
        int worker_count = DecodeFixed32(data + MF_OFFSET_NUM_WORKER);
        uint64_t skt_id = 0;
        uint32_t offset = MF_OFFSET_NEXT_WORKER_INFO;

        uint32_t padding_size = (mf_size % kRequestAlignSize) ? (kRequestAlignSize - (mf_size % kRequestAlignSize)) : 0;
        uint16_t num_mf = (mf_size + padding_size +  kDeviceRquestMaxSize- 1) / kDeviceRquestMaxSize;

        Slice parse(mf_data);
        parse.remove_prefix(offset);
        assert(worker_count);
        for (int i = 0; i < worker_count; i++) {
            DecodeFixed64(parse.data());
            parse.remove_prefix(8);
        }
#ifdef CODE_TRACE
        printf("delete InSDB skt count %d\n", sktinfo_count);
#endif
        /* get skt_id */
        if (sktinfo_count) {
            uint32_t key_size = 0;
            uint32_t sktdf_size = 0;
#ifdef USE_HASHMAP_RANDOMREAD
            uint32_t hash_size = 0;
#endif
            skt_ids = new SKTMetaInfo[sktinfo_count];
            for (uint32_t i = 0; i < sktinfo_count; i++) { //Parseing SKTable infomation.
                skt_id = DecodeFixed32(parse.data());
                parse.remove_prefix(4);
                skt_id &= ~MF_SKT_ID_MASK;
                skt_ids[i].skt_id = skt_id;
                sktdf_size = DecodeFixed32(parse.data());
                skt_ids[i].sktdf_size = sktdf_size;
                parse.remove_prefix(4);
#ifdef USE_HASHMAP_RANDOMREAD
                hash_size = DecodeFixed32(parse.data());
                skt_ids[i].hash_size = hash_size;
#endif
                parse.remove_prefix(4);
                if(!GetVarint32(&parse, &key_size))
                    port::Crash(__FILE__,__LINE__);
                parse.remove_prefix(key_size);
#if 0
                uint16_t col_info_size = DecodeFixed16(parse.data());
                parse.remove_prefix(2);
                parse.remove_prefix(col_info_size);
#endif
                /* send prefetch for sktable */
                InSDBKey skt_key = GetMetaKey(GetDBHash(), kSKTable, skt_id);
                int val_size = sktdf_size;
                env_->Get(skt_key, NULL, &val_size, kReadahead);
            }

            uint32_t thread_count = (sktinfo_count > max_thread) ? max_thread : sktinfo_count;
            uint32_t skt_count = sktinfo_count / thread_count;
            uint32_t mod_remains = sktinfo_count % thread_count;
            InSDBThreadsState state;
            state.num_running = thread_count;
            DeleteSKTableCtx *delctxs = new DeleteSKTableCtx[thread_count];
            uint32_t remains = sktinfo_count;
            uint32_t req = 0;
            uint32_t index = 0;
            for (uint32_t i = 0; i < thread_count; i++) {
                req = (remains > skt_count) ? skt_count : remains;
                if (mod_remains && i < mod_remains) req++;
                delctxs[i].mf = this;
                delctxs[i].state = &state;
                delctxs[i].num_skt = req; 
                delctxs[i].skt_ids = &skt_ids[index];
                env_->StartThread(Manifest::DeleteSKTableWrapper, (void *)&delctxs[i]);
                index += req;
                remains -= req;
            }
            while(1) {
                state.mu.Lock();
                int num = state.num_running;
                state.mu.Unlock();
                if (num == 0) {
                    break;
                }
                env_->SleepForMicroseconds(sleep_microsecond);
            }
            for (int i = skt_count -1; i >= 0; i--) {
                DeleteSKTableMeta(kSKTable, skt_ids[i].skt_id); // remove sktale
                DeleteSKTableMeta(kHashMap, skt_ids[i].skt_id); // remove sktale hash
                InSDBKey iterkey = GetMetaKey(GetDBHash(), kSKTIterUpdateList, skt_ids[i].skt_id);
                InSDBKey updatekey = GetMetaKey(GetDBHash(), kSKTUpdateList, skt_ids[i].skt_id);
                for (int j = 0; j < 10; j++) {
                    /*Delete remained iter kbm list written in previous flush */
                    iterkey.split_seq = j;
                    updatekey.split_seq = j;
                    Env::Default()->Delete(iterkey, true);//async
                    Env::Default()->Delete(updatekey, true);//async
                }
            }
            delete [] delctxs;
            delete [] skt_ids;
        }
        InSDBKey mf_key = GetMetaKey(dbhash_, kManifest, 0);
        for (int i = 0; i < num_mf; i++) {
            mf_key.split_seq = i;
            env_->Delete(mf_key);
        }
        DeleteSKTableMeta(kSuperSKTable, 0); //remove super sktable
        env_->Delete(GetMetaKey(dbhash_, kLogRecord));
        s = Env::Default()->Flush(kWriteFlush);
        if(!s.ok()){
            printf("[%d :: %s]Flush Fail \n", __LINE__, __func__);
            abort();
        }
        return s.OK();
    }

    /**
      Delete InSDB
      1. read super sktable and get sktble id.
      2. read sktable and them parse ikeys. And delete ikeys.
      3. check prealloc, and delete it.
      5. delete sktable .
      4. delete super sktable.
      */
    Status Manifest::DeleteInSDB() {
        Status s;
        SuperSKTable super_skt;
        uint32_t skt_id = 0;
        DeleteInSDBCtx d_ctx;
        std::vector<uint64_t>skt_ids;
        skt_ids.clear();
#ifdef USE_FIRMWARE_ITERATOR
        //FIX: fix Destroy DB return “not found”
        //return s after DeleteInSDBWithIter
        s = DeleteInSDBWithIter();
        return s;
        //FIX end
#endif
        s = ReadSuperSKTable(&super_skt);
        if (!s.ok()) return s;
        s = DeleteInSDBWithManifest(super_skt.max_column_count);
        if (s.ok()) return s;
        DeleteManifest(); // remove manifest
        skt_id = super_skt.first_skt_id;
        std::set<uint64_t> keyslist;
        while(skt_id != 0) {
            skt_ids.push_back(skt_id); 
#ifdef USE_HASHMAP_RANDOMREAD
            s = CleanUpSKTable(skt_id, kMaxSKTableSize, kMaxSKTableSize, &d_ctx, keyslist);  //cleanup sktable 
#else
            s = CleanUpSKTable(skt_id, kMaxSKTableSize, &d_ctx, keyslist);  //cleanup sktable 
#endif
            if (!s.ok()) break;;
            CleanupPreallocSKTable(d_ctx.prealloc_skt_id, d_ctx.next_skt_id); //remove prealloc
            skt_id = d_ctx.next_skt_id;
        }


        for (auto it = keyslist.begin(); it != keyslist.end(); ++it) {
            InSDBKey kbm_key = GetKBKey(GetDBHash(), kKBMeta, *it);
            env_->Delete(kbm_key);
            InSDBKey kb_key = GetKBKey(GetDBHash(), kKBlock, *it);
            env_->Delete(kb_key);
        }

        for (int i = skt_ids.size() - 1;  i >= 0; i--) {
            DeleteSKTableMeta(kSKTable, skt_ids[i]); // remove sktale
            DeleteSKTableMeta(kHashMap, skt_ids[i]); // remove sktale hash
            InSDBKey iterkey = GetMetaKey(GetDBHash(), kSKTIterUpdateList, skt_ids[i]);
            InSDBKey updatekey = GetMetaKey(GetDBHash(), kSKTUpdateList, skt_ids[i]);
            for (int j = 0; j < 10; j++) {
                /*Delete remained iter kbm list written in previous flush */
                iterkey.split_seq = j;
                updatekey.split_seq = j;
                Env::Default()->Delete(iterkey, true);//async
                Env::Default()->Delete(updatekey, true);//async
            }

        }
        DeleteSKTableMeta(kSuperSKTable, 0); //remove super sktable
        // remove log record
        s = env_->Delete(GetMetaKey(dbhash_, kLogRecord));
        s = Env::Default()->Flush(kWriteFlush);
        if(!s.ok()){
            printf("[%d :: %s]Flush Fail \n", __LINE__, __func__);
            abort();
        }
        return s;
    }

#ifdef USE_HASHMAP_RANDOMREAD
    Status Manifest::CleanUpSKTable(int skt_id, int keymap_size, int keymap_hash_size, DeleteInSDBCtx *Delctx, std::set<uint64_t> &keyslist)
#else
    Status Manifest::CleanUpSKTable(int skt_id, int keymap_size, DeleteInSDBCtx *Delctx, std::set<uint64_t> &keyslist)
#endif
        {
        memset(Delctx, 0, sizeof(*Delctx));
#ifdef USE_HASHMAP_RANDOMREAD
        KeyMapHashMap* hashmap = nullptr;
        Arena* arena = LoadKeymapFromDevice(skt_id, keymap_size, keymap_hash_size, hashmap, kSyncGet);
#else
        Arena* arena = LoadKeymapFromDevice(skt_id, keymap_size, kSyncGet);
#endif
        if (!arena) return Status::NotFound("skt does not exist");
#ifdef USE_HASHMAP_RANDOMREAD
        Keymap* table = new Keymap(arena, GetComparator(), hashmap);
#else
        Keymap* table = new Keymap(arena, GetComparator());
#endif
        KeyMapHandle handle = table->First();
        while(handle) {
            table->GetiKeySeqNum(handle, keyslist);
            handle = table->Next(handle);
        }
        KEYMAP_HDR *hdr = (KEYMAP_HDR*)(arena->GetBaseAddr());

        /* fillup Delctx */
        Delctx->next_skt_id = hdr->next_skt_id;
        Delctx->prealloc_skt_id = hdr->prealloc_skt_id;
        /* delete colinfo */
        for(uint32_t colinfo_id = hdr->start_colinfo; colinfo_id < hdr->last_colinfo; colinfo_id++) { 
            InSDBKey colinfo_key = GetColInfoKey(GetDBHash(), skt_id, colinfo_id);
            env_->Delete(colinfo_key);
        }
        delete table;
        return Status::OK();
    }

    void Manifest::DeleteSKTable(void *arg) {
        Status s;
        DeleteSKTableCtx *delctx = reinterpret_cast<DeleteSKTableCtx*>(arg);
        DeleteInSDBCtx d_ctx;
        std::set<uint64_t> keyslist;
        for (uint32_t i = 0; i < delctx->num_skt; i++) {
#ifdef USE_HASHMAP_RANDOMREAD
            s = CleanUpSKTable(delctx->skt_ids[i].skt_id, delctx->skt_ids[i].sktdf_size, delctx->skt_ids[i].hash_size, &d_ctx, keyslist);  //cleanup sktable 
#else
            s = CleanUpSKTable(delctx->skt_ids[i].skt_id, delctx->skt_ids[i].sktdf_size, &d_ctx, keyslist);  //cleanup sktable 
#endif
            if (s.ok()) {
                CleanupPreallocSKTable(d_ctx.prealloc_skt_id, d_ctx.next_skt_id); //remove prealloc
            }
        }
#ifdef CODE_TRACE
        printf("delete kb/kbpm count %ld\n", keyslist.size());
#endif 
        for (auto it = keyslist.begin(); it != keyslist.end(); ++it) {
            InSDBKey kbm_key = GetKBKey(GetDBHash(), kKBMeta, *it);
            env_->Delete(kbm_key);
            InSDBKey kb_key = GetKBKey(GetDBHash(), kKBlock, *it);
            env_->Delete(kb_key);
        }

        delctx->state->mu.Lock();
        delctx->state->num_running -= 1;
        delctx->state->mu.Unlock();
        return;
    }

    /////////////////////////////////////////// END  : Manifest Creation/Initializtion/Deletion ///////////////////////////////////////////

    /////////////////////////////////////////// START: Sorted list related functions ///////////////////////////////////////////
    SKTableMem* Manifest::GetSKTableMem(SimpleLinkedNode *node){ 
        return node ? class_container_of(node, SKTableMem, skt_sorted_node_) : NULL; 
    }


    SKTableMem* Manifest::GetLastSKTableInSortedList(){
        //return skt_sorted_list_.oldest()->GetOwner();
        skt_sorted_simplelist_lock_.Lock(); 
        SimpleLinkedNode *node = skt_sorted_simplelist_.oldest();
        skt_sorted_simplelist_lock_.Unlock();
        return GetSKTableMem(node);
    }

    SKTableMem* Manifest::NextSKTableInSortedList(SimpleLinkedNode* skt_node){ 
        skt_sorted_simplelist_lock_.Lock(); 
        SimpleLinkedNode *node = skt_sorted_simplelist_.next(skt_node);
        skt_sorted_simplelist_lock_.Unlock();
        return GetSKTableMem(node);
    }
    bool Manifest::IsNextSKTableDeleted(SimpleLinkedNode* skt_node){ 
        skt_sorted_simplelist_lock_.Lock(); 
        SimpleLinkedNode *node = skt_sorted_simplelist_.next(skt_node);
        skt_sorted_simplelist_lock_.Unlock();
        SKTableMem* skt = GetSKTableMem(node);
        return skt ? skt->IsDeletedSKT() : false;
    }
    SKTableMem* Manifest::PrevSKTableInSortedList(SimpleLinkedNode* skt_node){ 
        skt_sorted_simplelist_lock_.Lock(); 
        SimpleLinkedNode *node = skt_sorted_simplelist_.prev(skt_node);
        skt_sorted_simplelist_lock_.Unlock();
        return GetSKTableMem(node);
    }
    /////////////////////////////////////////// END  : Sorted list related functions ///////////////////////////////////////////

    /////////////////////////////////////////// START: Dirty/KBMUpdate/Clean Cache Management ///////////////////////////////////////////
    void Manifest::InsertSKTableMemCache(SKTableMem* sktm, SKTCacheType type, uint16_t cache) {
        if(!sktm->IsCached()){
            if(!sktm->SetCachingInProgress()){
                if(sktm->SetCacheBit(cache))
                    skt_cache_[type].PushBack(sktm);
                sktm->ClearCachingInProgress();
            }
        }
    }

    void Manifest::InsertSKTableMemCache(SKTableMem* sktm, SKTCacheType type) {
        skt_cache_[type].PushBack(sktm);
    }

    void Manifest::SpliceCacheList(std::list<SKTableMem*> &skt_list, SKTCacheType type, bool to_front){
        if(to_front)
            skt_cache_[type].SpliceToFront(skt_list);
        else
            skt_cache_[type].SpliceToBack(skt_list);
    }

    void Manifest::SwapSKTCacheList(std::list<SKTableMem*> &skt_list, SKTCacheType type) {
        assert(skt_list.empty());
        skt_cache_[type].SwapCacheList(skt_list);
    }


    void Manifest::SpliceKBPMCacheList(std::list<KeyBlockPrimaryMeta*> &kbpm_list){
        kbpm_update_list_.SpliceToBack(kbpm_list);
    }

    void Manifest::SwapKBPMCacheList(std::list<KeyBlockPrimaryMeta*> &kbpm_list){
        assert(kbpm_list.empty());
        kbpm_update_list_.SwapCacheList(kbpm_list);
    }



#if 0
    size_t Manifest::SKTCountInSKTableMemCache(SKTCacheType type) {
        return skt_cache_[type].GetCacheSize();
    }
    /*PopSKTableMem is not used anymore. We use SwapSKTCacheList instead of this function */
    SKTableMem* Manifest::PopSKTableMem(SKTCacheType type) {
#ifndef NDEBUG
        SKTableMem* sktm = skt_cache_[type].PopItem();
        if (sktm){
            if(type == kDirtySKTCache) assert(sktm->IsInDirtyCache());
            else assert(sktm->IsInCleanCache());
        }
        return sktm;
#else
        return skt_cache_[type].PopItem();
#endif
    }
#endif

    void Manifest::GetCurrentOrderKBPMList(std::map<SequenceNumber, KeyBlockPrimaryMeta*> &kbpm_order_list) {
        if (!kbpm_hashmap_->empty()) {
            for (auto it = kbpm_hashmap_->cbegin(); it != kbpm_hashmap_->cend(); ++it) {
                KeyBlockPrimaryMeta *kbpm = it->second;
                if (kbpm) {
                    kbpm_order_list.insert(std::make_pair(kbpm->GetiKeySequenceNumber(), kbpm));
                }
            }
        } 
    }
    void Manifest::ResetKBPMHashMap() {

        kb_cache_->Clear(this);
        kbpm_cache_->Clear(this);

        if (!kbpm_hashmap_->empty()) {
            for (auto it = kbpm_hashmap_->cbegin(); it != kbpm_hashmap_->cend();) {
                KeyBlockPrimaryMeta *kbpm = it->second;
                if (kbpm) {
#ifdef MEM_TRACE
                    DecKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif
                    FreeKBPM(kbpm);
                }
                it = kbpm_hashmap_->erase(it); 
            }
        }

        for (int i = 0; i< kbpm_pad_size_; ++i) {
            kbpm_pad_[i].store(nullptr, std::memory_order_relaxed);
            kb_pad_[i].store(nullptr, std::memory_order_relaxed);
        } 
    }

    void Manifest::ResetTrxn() {
        uncompleted_trxn_group_.clear();
        completed_trxn_group_list_.clear(); 
        completed_trxn_group_list_.push_front(TrxnGroupRange(0)); 
        completed_trxn_group_list_.push_back(TrxnGroupRange(0xFFFFFFFF)); 

    }

    /////////////////////////////////////////// END  : Dirty/KBMUpdate/Clean Cache Management ///////////////////////////////////////////

    /////////////////////////////////////////// START: Cache Threads(Dirty/KBM update/Clean/Log) related functions ///////////////////////////////////////////
    void Manifest::SignalSKTFlushThread()
    {

        /* 
         * We assume that this function is not called frequently.
         * Call it when update count exceeds watermark or update count of a sktable exceeds kMinUpdateCntForTableFlush
         * So, set count and check flag under lock
         */
        skt_flush_mu_.Lock();
        if(!IsInSKTableFlush())
            skt_flush_cv_.SignalAll();
        skt_flush_mu_.Unlock();
    }

    void Manifest::SignalKBPMUpdateThread()
    {

        /* It is called from Flush Thread that means calling frequency is not high. */
        kbpm_update_mu_.Lock();
        if( !IsInKBPMUpdate() )
            kbpm_update_cv_.SignalAll();
        kbpm_update_mu_.Unlock();
    }

    void Manifest::SignalSKTEvictionThread(bool move_to_dirty)
    {
        if(move_to_dirty) SetCleanToDirty();
        if ((MemUseHigh() && !IsInMemoryReclaim()) || cache_thread_shutdown_ /*|| move_to_dirty*/)
        {
            skt_eviction_mu_.Lock();
            skt_eviction_cv_.SignalAll();
            skt_eviction_mu_.Unlock();
        }
    }
    uint32_t WorkerData::ZeroNRSCount(){
        uint32_t count = 0;
        for(uint32_t i = 0  ; i < 2 ; i++){
            for(auto it = queue_[i].begin(); it != queue_[i].end() ; it++){
                KeyBlockPrimaryMeta* kbpm = *it;
                if(!(kbpm->GetNRSCount()))
                    count++;
            }
        }
        return count;
    }

    // Collect key block delete candidates for a worker
    void Manifest::WorkerLogCleanupInternal(WorkerData &logcleanup_data, std::list<SequenceNumber> &worker_next_ikey_seq, std::queue<KeyBlockPrimaryMeta*> &kbpm_cleanup, SequenceNumber &smallest_ikey)
    {
        bool first = true;
        SequenceNumber n_ikey = 0;
        std::deque<KeyBlockPrimaryMeta*> * logQ = logcleanup_data.GetInActiveQueue();
        KeyBlockPrimaryMeta* kbpm, *last_cleanup_kbpm=NULL;
scan:
        // iterate each KBML
        Spinlock *logLock = logcleanup_data.GetInActiveLock();
        logLock->Lock();
        while(!logQ->empty()){
            kbpm = logQ->front();
            if(kbpm->GetNRSCount()) {
                goto finish_this_worker;
            }
            // append the ikey seq num to the device format
            logQ->pop_front();
            // append kbpm to delete candidate list
            if(kbpm->GetiKeySequenceNumber() < smallest_ikey){
                /*For size reduction(diff : store "ikey - smallest_ikey")*/
                smallest_ikey = kbpm->GetiKeySequenceNumber(); 
            }
            kbpm_cleanup.push(kbpm);
            // store the last added ikey to determine worker's next ikey seq
            last_cleanup_kbpm = kbpm;
            n_ikey = last_cleanup_kbpm->GetNextIKeySeqNum();
            // collect KBMLs up to the max to prevent IO size overflow.
            // 2MB / 9 bytes
#if 0
            if (kbpm_cleanup.size() > 45000) {
#ifdef CODE_TRACE
                printf("[%d :: %s] Log Cleanup count is bigger than 45000(%ld)\n", __LINE__, __func__, kbpm_cleanup.size());
#endif
                goto finish_this_worker;
            }
#endif
        }
        // switch active queue when the inactive queue is drained.
        if(logQ->empty() && first){
            logLock->Unlock();
            // switch only once
            first = false;
            // switch
            logcleanup_data.ChangeActive();
            goto scan;
        }
finish_this_worker:
        logLock->Unlock();
        // add the worker's next ikey sequence
        if (last_cleanup_kbpm) {
            worker_next_ikey_seq.push_back(n_ikey);
            logcleanup_data.SetLastWorkerNextIKeySeq(n_ikey);
        } else {
            worker_next_ikey_seq.push_back(logcleanup_data.GetLastWorkerNextIKeySeq());
        }
    }

    void Manifest::WorkerLogCleanupAndSubmission(std::queue<KeyBlockPrimaryMeta*> kbpm_cleanup, SequenceNumber &smallest_ikey)
    {
        KeyBlockPrimaryMeta *kbpm;
        //std::list<SequenceNumber> deleteSeqList;
        //deleteSeqList.push_back(smallest_ikey);
        // Free KB, KBM, and KBML memory, and construct key names
        while(!kbpm_cleanup.empty()) {
            kbpm = kbpm_cleanup.front();
            kbpm_cleanup.pop();
            if (kbpm->IsDummy()) abort();
            // acquire submit protection
            while(kbpm->SetWriteProtection());
            kbpm->SetOutLog();

            // release submit protection
            kbpm->ClearWriteProtection(); 
            // add to to-be-deleted key list
        }

        // Set the number of deleted ikey sequence number
        //logkey.AppendIKeySeqList(deleteSeqList);

    }

    void Manifest::LogCleanupInternal(LogRecordKey &LogRecordkey)
    {
        std::queue<KeyBlockPrimaryMeta*> kbpm_cleanup;
        std::list<TrxnGroupRange> completed_trxn_groups;
        std::list<SequenceNumber> worker_next_ikey_seq;
        SequenceNumber smallest_ikey = ULONG_MAX;

        // Get completed transaction group
        CopyCompletedTrxnGroupRange(completed_trxn_groups);

        // Append zeroed header and the number of ikeys
        LogRecordkey.AppendHeaderSpace();

        // collect deleted ikeys in each worker queue
#ifdef CODE_TRACE
#define LOG_KBM_CNT
        uint32_t cnt = 0;
        uint32_t nrs_cnt = 0;
#endif
        for(int worker_idx = 0; worker_idx < kWriteWorkerCount; worker_idx++){
            WorkerLogCleanupInternal(logcleanup_data_[worker_idx], worker_next_ikey_seq, kbpm_cleanup, smallest_ikey);
#ifdef LOG_KBM_CNT
            cnt+=logcleanup_data_[worker_idx].Count();
            nrs_cnt+=logcleanup_data_[worker_idx].ZeroNRSCount();
#endif
        }
        // skip writing log record key if no key is deleted.
#ifdef LOG_KBM_CNT
        printf("[%d :: %s]# of cleanup KBM : %d(remained : %u(zero nrs : %u) :: diff : %u)\n", __LINE__, __func__, kbpm_cleanup.size(), cnt, nrs_cnt, cnt-nrs_cnt);
#endif
        if(!kbpm_cleanup.size()) {
            LogRecordkey.Reset();
            return;
        }
        /* Write Log data */
        // Append worker next ikey seq
        LogRecordkey.AppendIKeySeqList(worker_next_ikey_seq);
        // Append completed transaction groups
        LogRecordkey.AppendTrxnGroupList(completed_trxn_groups);
        // Build IO buffer. Compress if possible.
        LogRecordkey.BuildIOBuffer();

        // write the log record key
        const Slice io_buffer( LogRecordkey.GetIOBuffer() );
        // buffer limit is same as max sktable size
        if(io_buffer.size() > kDeviceRquestMaxSize) {
            port::Crash(__FILE__,__LINE__);
        }
        // write the log record key to the device
        env_->Put(LogRecordkey.GetInSDBKey() , io_buffer, false);

        // delete the internal keys
        WorkerLogCleanupAndSubmission(kbpm_cleanup, smallest_ikey);

        // reset the log record key object
        LogRecordkey.Reset();
    }

    uint32_t Manifest::InsertUpdatedKBPMToMap(std::list<KeyBlockPrimaryMeta*> *kbpm_list, std::map<uint64_t, KeyBlockPrimaryMeta*> *kbpm_map)
    {

        uint32_t size = 0;
        uint32_t kbpm_count = 0;
        std::deque<KeyBlockPrimaryMeta*> *pending_kbm_queue = NULL;
        KeyBlockPrimaryMeta *kbpm = NULL;
         
        while(!kbpm_list->empty())
        {
            kbpm = kbpm_list->front();
            kbpm_list->pop_front();

            /*Keep the selected SKT in order to insert to clean or dirty(flush) cache*/

            /* Get all KBPMs from update queue of the SKTable */
            /*if KBPM has not been added to KBPM map, insert the the map */
            if(!kbpm->SetUpdateKBMMap()){
                /* KBPM I/O size can be shrink, But can not be increased */
                /* 
                 * KBPM can be removed if the eviction thread evicts the SKT 
                 * So, we need to increase additional ref count until device update
                 */
                /*Estimated IO Size is not needed anymore because all update does not contain TRXN information that means we use fixed size(7byte)*/
                //size = SizeAlignment(kbpm->GetEstimatedBufferSize());
                kbpm_map->insert({kbpm->GetiKeySequenceNumber(), kbpm});
#ifdef CODE_TRACE
                printf("[%d :: %s]Insert KBM : 0x%p (ikey : %ld)\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber());
#endif
                kbpm_count++;
            }
            assert(kbpm->IsNeedToUpdate());
            kbpm->DecPendingUpdatedValueCnt();
            /* 
             * The updated column did not decrease the reference count in order to prevent evicting the KBPM.
             * So, we need to decrease the ref cnt here. But, the KBPM will not be evicted until submission 
             * because additional ref count has been increased during inserting to the map
             */
        }

        return kbpm_count;
    }

    uint32_t Manifest::BuildKBPMUpdateData(std::map<uint64_t, KeyBlockPrimaryMeta*> *kbpm_map, struct KBPMUpdateVector *kbpm_update, std::vector<InSDBKey> *delete_key_vec, std::deque<KeyBlockPrimaryMeta*> &updated_kbpm, char* kbpm_io_buf)
    {
        KeyBlockPrimaryMeta* kbpm = NULL;
        uint32_t size = 0;
        SequenceNumber ikey = 0;
        uint32_t kbpm_count = 0;

        auto it = kbpm_map->cbegin();
        kbpm_update->Init();
        delete_key_vec->resize(0);;
        if(it != kbpm_map->end()){
            while( it != kbpm_map->end() ){
                auto curr = it++; 
                ikey = curr->first;
                kbpm = curr->second;
                assert(ikey == kbpm->GetiKeySequenceNumber());

                if(!kbpm->SetWriteProtection()/* The other threads try to change KBPM */){
                    if(!kbpm->GetPendingUpdatedValueCnt() && !kbpm->IsInlog()){
#ifdef CODE_TRACE
                        printf("[%d :: %s]Before clear NeedToUpdate (kbpm : %p), ikey = %ld\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber());
#endif
                        kbpm->ClearUpdateKBMMap();
                        kbpm_map->erase(curr);
                        if( kbpm->TestKeyBlockBitmap()){
                            updated_kbpm.push_back(kbpm);
                            kbpm_count++;
                            size = SizeAlignment((kbpm->BuildDeviceFormatKeyBlockMeta(kbpm_io_buf)).size());
                            /*The size must not include the TRXN size*/
                            assert(size == 12);
#ifdef CODE_TRACE
                            printf("[%d :: %s]Actual size : %d\n", __LINE__, __func__,size);
#endif
                            kbpm->ClearWriteProtection();
                            kbpm_update->Insert(GetKBKey(GetDBHash(), kKBMeta, ikey), kbpm_io_buf, size);
                            kbpm_io_buf += size;
                            /* To update vector */
                        }else{
                            bool IsInCache = kbpm->IsCache();
                            kbpm->ClearNeedToUpdate();
                            kbpm->ClearWriteProtection();//It may not be needed because no one accesses the KBPM.
                            delete_key_vec->push_back(GetKBKey(GetDBHash(), kKBMeta, ikey));
                            if(kbpm->GetOriginalKeyCount()){
                                delete_key_vec->push_back(GetKBKey(GetDBHash(), kKBlock, ikey));
                            }

#ifdef CODE_TRACE
                            printf("[%d :: %s]Before check EvictColdKBPM() (kbpm : %p), ikey = %ld\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber());
#endif
                            if(!IsInCache && kbpm->EvictColdKBPM(this)){
#ifdef CODE_TRACE
                                printf("[%d :: %s]Free (kbpm : %p), ikey = %ld\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber());
#endif
                                FreeKBPM(kbpm);
                            }

                        }
                        
                    }else {
                        kbpm->ClearWriteProtection();//It may not be needed because no one accesses the KBPM.
                    }
                }
            }
        }

        return kbpm_count;
    }

#ifdef CODE_TRACE
#define CACHE_TRACE
    uint64_t dirty_wo_keymap_insert = 0;
    uint64_t evict_skt = 0;
    uint64_t dirty_insert = 0;
    uint64_t clean_insert = 0;
    uint64_t no_load_for_memory = 0;
    uint64_t evict_dirty_skt_loaded_for_read = 0;
#endif
    bool Manifest::SKTableReclaim(SKTableMem *skt) {
        bool flushable = false;
        uint32_t put_col = 0, del_col = 0;
        int64_t reclaim_size = 0;
        skt->SetEvictionInProgress();
        bool referenced = skt->SKTDFReclaim(this);

        // Eviction loop
        if(!options_.table_eviction || skt->IsSKTDirty(this) || skt->NeedToSplit()){
            //Do flush 
            flushable = true;
        } else {
            if (!skt->GetSKTRefCnt() && !skt->ClearReference()) {
                assert(!skt->IsKeyQueueEmpty());
                //Do not flush even if it has dirty userkey in key_queue_.
                skt->DeleteSKTDeviceFormatCache(this);
            }
            flushable = false;
        } 
        skt->ClearEvictionInProgress();
        return flushable;
    }

    void Manifest::SKTableReclaim(SKTableMem *skt, int64_t &reclaim_size, bool force_reclaim, std::list<SKTableMem*> &clean_list, std::list<SKTableMem*> &dirty_list){
        bool referenced = false;
        bool active = false;
        uint32_t put_col = 0, del_col = 0;
        skt->SetEvictionInProgress();
        referenced = skt->SKTDFReclaim(this, reclaim_size, active, force_reclaim, put_col, del_col);
        if (!active && put_col) {
            if (put_col < del_col) {
#ifdef CODE_TRACE
                printf("[%d :: %s] put : %d, del : %d\n", __LINE__, __func__, put_col, del_col);
#endif
                //int32_t orig_size = skt->GetAllocatedSize();
                int32_t orig_size = skt->GetBufferSize();
                int32_t new_size = 0;
#ifdef USE_HASHMAP_RANDOMREAD
                KeyMapHashMap* hashmap = new KeyMapHashMap(1024*32);
                Arena* new_arena = skt->MemoryCleanupSKTDeviceFormat(this, hashmap);
#else
                Arena* new_arena = skt->MemoryCleanupSKTDeviceFormat(this);
#endif
                if (new_arena) {
                    //new_size = new_arena->AllocatedMemorySize();
                    new_size = new_arena->GetBufferSize();
#ifdef MEM_TRACE
                    if (orig_size > new_size) DecSKTDF_MEM_Usage(orig_size - new_size);
                    else IncSKTDF_MEM_Usage(new_size - orig_size);
#endif                
                    IncreaseSKTDFNodeSize(new_size - orig_size);
                    reclaim_size -= (orig_size - new_size);
                    /* lock */
#ifdef USE_HASHMAP_RANDOMREAD
                    skt->InsertArena(new_arena, hashmap);
#else
                    skt->InsertArena(new_arena);
#endif
                    /* unlock */
                }
            }
        }
        // Eviction loop
        if(skt->IsSKTDirty(this)){
            if(skt->IsInCleanCache())
                skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
#ifdef CACHE_TRACE
            dirty_insert++;
#endif
            dirty_list.push_back(skt);
        }else if((!options_.table_eviction && !force_reclaim) || referenced || skt->GetSKTRefCnt()){
            if(skt->IsInDirtyCache())
                skt->ChangeStatus(SKTableMem::flags_in_dirty_cache/*From*/, SKTableMem::flags_in_clean_cache/*To*/);
#ifdef CACHE_TRACE
            clean_insert++;
#endif
            clean_list.push_back(skt);
        } else if (put_col == 0 && skt->IsKeyQueueEmpty() && skt->GetSKTableID() != 1) {
            /* delete sktable */
            //reclaim_size-=skt->GetAllocatedSize();
            reclaim_size-=skt->GetBufferSize();
            /*
             * Move to FlushSKTableMem to reduce additioanl load.
             * skt->DeleteSKTDeviceFormatCache(this, true);
             */
            skt->SetDeletedSKT(this);
            skt->ClearCached();
            while(skt->GetSKTRefCnt());
            RemoveSKTableMemFromSkiplist(skt);
            SKTableMem *prev = skt;
            while(1) {
                SKTableMem *prev_prev = prev;
                prev = PrevSKTableInSortedList(prev->GetSKTableSortedListNode());
                if (!prev->IsDeletedSKT()) {
                    /* Fetched status will be checked in PrefetchSKTable() */
                    if (!prev->IsKeymapLoaded()) {
                        prev->PrefetchSKTable(this);
                        assert(prev->IsCached());
                    } else if (!prev->IsCached()) {
                        printf("[%s:%d] the previous skt(%d) of deleted skt(%d) has not been cached\n",
                                __func__, __LINE__, prev->GetSKTableID(), skt->GetSKTableID());
                        InsertSKTableMemCache(prev, kDirtySKTCache, SKTableMem::flags_in_dirty_cache);
                    }
                    break;
                }
            } 
        } else if (skt->NeedToSplit()) {
            /* keymap is too big .... need to split */
            if(skt->IsInCleanCache())
                skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
#ifdef CACHE_TRACE
            dirty_insert++;
#endif
            dirty_list.push_back(skt);
        } else if (!skt->GetSKTRefCnt() && !skt->ClearReference()) {
#ifdef MEM_TRACE
            IncEvictedSKT();
#endif
            reclaim_size-=skt->GetBufferSize();
            skt->DeleteSKTDeviceFormatCache(this);
            if(!skt->IsKeyQueueEmpty()){
                if(skt->IsInCleanCache())
                    skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
#ifdef CACHE_TRACE
                dirty_wo_keymap_insert++;
#endif
                dirty_list.push_back(skt);
            }else{
#ifdef CACHE_TRACE
                evict_skt++;
#endif
                skt->ClearCached();
            }
            /* The SKTable has been added to SKTable cache */
        } else {
            clean_list.push_back(skt);
        }
        skt->ClearEvictionInProgress();
    }


    void Manifest::SKTableCleanup(SKTableMem *skt, int64_t &reclaim_size, std::list<SKTableMem*> &clean_list, std::list<SKTableMem*> &dirty_list){
        skt->SetEvictionInProgress();
#if 0
        if (skt->NeedToCleanup() && !skt->GetSKTRefCnt()) {

            int32_t orig_size = skt->GetBufferSize();
            int32_t new_size = 0;
#ifdef USE_HASHMAP_RANDOMREAD
            KeyHashMap* hashmap = new KeyMapHashMap(1024*32);
            Arena* new_arena = skt->MemoryCleanupSKTDeviceFormat(this, hashmap);
#else
            Arena* new_arena = skt->MemoryCleanupSKTDeviceFormat(this);
#endif

            if (new_arena) {
                new_size = new_arena->GetBufferSize();
#ifdef MEM_TRACE
                if (orig_size > new_size) DecSKTDF_MEM_Usage(orig_size - new_size);
                else IncSKTDF_MEM_Usage(new_size - orig_size);
#endif                
                IncreaseSKTDFNodeSize(new_size - orig_size);
                reclaim_size -= (orig_size - new_size);

#ifdef USE_HASHMAP_RANDOMREAD
                skt->InsertArena(new_arena, hashmap);
#else
                skt->InsertArena(new_arena);
#endif


            }
            if (skt->GetCount() == 0 && skt->IsKeyQueueEmpty() && skt->GetSKTableID() != 1) {
                /* delete sktable */
                //reclaim_size-=skt->GetAllocatedSize();
                reclaim_size-=skt->GetBufferSize();
                /*
                 * Move to FlushSKTableMem to reduce additioanl load.
                 * skt->DeleteSKTDeviceFormatCache(this, true);
                 */
                skt->SetDeletedSKT(this);
                skt->ClearCached();
                while(skt->GetSKTRefCnt());
                RemoveSKTableMemFromSkiplist(skt);
                SKTableMem *prev = skt;
                while(1) {
                    SKTableMem *prev_prev = prev;
                    prev = PrevSKTableInSortedList(prev->GetSKTableSortedListNode());
                    if (!prev->IsDeletedSKT()) {
                        /* Fetched status will be checked in PrefetchSKTable() */
                        if (!prev->IsKeymapLoaded()) {
                            prev->PrefetchSKTable(this);
                            assert(prev->IsCached());
                        } else if (!prev->IsCached()) {
                            printf("[%s:%d] the previous skt(%d) of deleted skt(%d) has not been cached\n",
                                    __func__, __LINE__, prev->GetSKTableID(), skt->GetSKTableID());
                            InsertSKTableMemCache(prev, kDirtySKTCache, SKTableMem::flags_in_dirty_cache);
                        }
                        break;
                    }
                } 
                skt->ClearEvictionInProgress();
                return;
            }
        }
#endif
        if (skt->IsSKTDirty(this) || !skt->IsKeyQueueEmpty()) {
            skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
            dirty_list.push_back(skt);
        } else if (!skt->GetSKTRefCnt() && !skt->ClearReference()) {
#ifdef MEM_TRACE
            IncEvictedSKT();
#endif
            reclaim_size-=skt->GetBufferSize();
            skt->DeleteSKTDeviceFormatCache(this);
            if(!skt->IsKeyQueueEmpty()){
                if(skt->IsInCleanCache())
                    skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
#ifdef CACHE_TRACE
                dirty_wo_keymap_insert++;
#endif
                dirty_list.push_back(skt);
            }else{
#ifdef CACHE_TRACE
                evict_skt++;
#endif
                skt->ClearCached();
            }

        } else {
            clean_list.push_back(skt);
        } 
        skt->ClearEvictionInProgress();
        return;
    }

    void Manifest::EvictUserKeys(std::list<SKTableMem*> &skt_list, int64_t &reclaim_size, bool cleanup)
    {
        // Start eviction with the oldest sktable
        SKTableMem *skt = NULL;
        bool force_reclaim = false;
        std::list<SKTableMem*> clean_list, dirty_list;

        while (!skt_list.empty() &&  (cleanup || (reclaim_size > 0 && !MemUseLow()))) {
            force_reclaim = MemUseHigh() ? true : false;
           // TODO: skip internal key sktable?
            skt = skt_list.front();
            skt_list.pop_front();
            assert(skt);
            if (skt->GetSKTRefCnt()) {
                if (skt->IsSKTDirty(this) || !skt->IsKeyQueueEmpty()) {
                    skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
                    dirty_list.push_back(skt);
                } else {
                    clean_list.push_back(skt);
                }
                continue;
            } else if (!skt->IsKeymapLoaded()) {
                if (skt->IsSKTDirty(this) || !skt->IsKeyQueueEmpty()) {
                    skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
                    dirty_list.push_back(skt);
                } else if (cleanup || force_reclaim) {
                    skt->ClearInCleanCache();
                    skt->DiscardSKTablePrefetch(this);
                } else {
                    clean_list.push_back(skt);
                }
                continue;
            }

#ifdef CODE_TRACE
            printf("[%d :: %s]skt_id : %d\n", __LINE__, __func__,skt->GetSKTableID());
#endif
            if (cleanup) {
                SKTableReclaim(skt, reclaim_size, cleanup || force_reclaim, clean_list, dirty_list);
            } else {
                SKTableCleanup(skt, reclaim_size, clean_list, dirty_list);
            }
            // Unlock sktables
            EvictKBCache();
            EvictKBPMCache();
        }

        while(!skt_list.empty()){
            /* To migrate dirty SKTable the dirty cache */
            skt = skt_list.front();
            skt_list.pop_front();
            // TODO: skip internal key sktable?
            assert(skt);
            if(skt->IsSKTDirty(this) || !skt->IsKeyQueueEmpty()){
                skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
                dirty_list.push_back(skt);
            } else {
                clean_list.push_back(skt);
            }
        }
        if(!clean_list.empty())
            SpliceCacheList(clean_list, kCleanSKTCache);
        if(!dirty_list.empty()){
            SpliceCacheList(dirty_list, kDirtySKTCache);
            if(update_cnt_ >= kSKTableFlushWatermark)
                SignalSKTFlushThread();
        }
    }

    uint16_t Manifest::GetOffloadFlushCount(){
        offload_cnt_.load(std::memory_order_relaxed);
    }
    void Manifest::DecOffloadFlushCount(){
        offload_cnt_.fetch_sub(1, std::memory_order_relaxed);
    }
    SKTableMem* Manifest::GetFlushOffloadSKTable(){
        SKTableMem* sktable = NULL;
        if(offload_cnt_.load(std::memory_order_relaxed)){
            offload_lock_.Lock();
            if(!flush_offload_queue_.empty()){
                sktable = flush_offload_queue_.front();
                flush_offload_queue_.pop();
            }
            offload_lock_.Unlock();
        }
        return sktable;

    }

    void Manifest::SKTFlushThread()
    {
        (void)pthread_setname_np(pthread_self(), "flush");

        SKTableMem* sktable = NULL;
        bool update_kbm = false;

        std::list<SKTableMem*> skt_list;
        std::list<SKTableMem*> clean_list;
        std::list<SKTableMem*> dirty_list;
        bool first_flush = false;

        // threshold to insert tables to update cache
        uint32_t skt_update_cache_threshold = 50000;

        char* dev_buffer = (char*)malloc(kDeviceRquestMaxSize);
        std::string col_info_table;
        col_info_table.reserve(kDeviceRquestMaxSize);
        std::string comp_col_info_table;
        comp_col_info_table.reserve(kDeviceRquestMaxSize);
        std::list<KeyBlockPrimaryMeta*> kbpm_list;
        std::queue<KeyBlockPrimaryMeta*> in_log_KBM_queue;
        KeyBlockPrimaryMeta *in_log_kbm = NULL;

        while(true){
            skt_flush_mu_.Lock();
            /** Wait for a signal */
            while(!cache_thread_shutdown_ && (IsSKTDirtyListEmpty() || update_cnt_ < kSKTableFlushWatermark)/* if update count during flushing SKTable exceeds watermard, Do flush again */)
            {
                ClearSKTableFlush();
                skt_flush_cv_.TimedWait(env_->NowMicros() + 1000000/*Wake up every 2 second*/);
                SetSKTableFlush();
                /* If Dirty Cache is no empty, do flush at least one time */
                if(!IsSKTDirtyListEmpty())
                    break;
            }
            skt_flush_mu_.Unlock();
            SignalSKTEvictionThread(true);
            if(CheckExitCondition()){
                /*Finish the Cache Thread if there is no more SKT in the clean cache list */
                break;
            }
            bool skt_force_load = false;
            if(/*update_cnt_ >= kSKTableFlushWatermark || CheckKeyEvictionThreshold() || MemoryUsage() > kMaxCacheSize ||*/ cache_thread_shutdown_.load(std::memory_order_relaxed)) {
                SignalSKTEvictionThread(true);
                skt_force_load = true;
            }
            SetFlushThreadBusy();
#if 1
            if(cache_thread_shutdown_ && !flush_offload_queue_.empty()){
                SKTableMem* sktable = NULL;
                while(sktable = GetFlushOffloadSKTable())
                    this->InsertSKTableMemCache(sktable, kDirtySKTCache); 
            }
#endif
            update_cnt_ = 0;
            bool del_skt = false;
#ifdef KV_TIME_MEASURE
            uint32_t flush_call = 0;
#endif
            if(!IsSKTDirtyListEmpty()){
                SwapSKTCacheList(skt_list, kDirtySKTCache);
                SKTableMem *flush_skt = NULL;
                while(!skt_list.empty()){
                    flush_skt = skt_list.front();
                    skt_list.pop_front();
#if 0
                    if(flush_skt->GetSKTRefCnt()){
#ifdef MEM_TRACE
                        IncMissFlush();
#endif
                        dirty_list.push_back(flush_skt);
                        continue;
                    }
#endif
#if 1
                    while((uk_count_.load(std::memory_order_relaxed) > (kCongestionControl >> 2)) && skt_list.size() > 1 && offload_cnt_.load(std::memory_order_relaxed) < GetWorkerCount()*2) {
                        sktable = skt_list.front();
                        skt_list.pop_front();
#if 0
                        if(sktable->GetSKTRefCnt()){
                            dirty_list.push_back(sktable);
                            continue;
                        }
#endif
                        /* Offload flush work to WorkerThread */
                        offload_lock_.Lock();
                        flush_offload_queue_.push(sktable);
                        offload_lock_.Unlock();
                        offload_cnt_.fetch_add(1, std::memory_order_relaxed);
                    }
#endif

                    if(offload_cnt_.load(std::memory_order_relaxed)){
                        SignalAllWorkerThread();
                    }
                    assert(flush_skt);

                    bool loaded_skt = true;
                    if(!flush_skt->IsKeymapLoaded()){
                        if(!flush_skt->IsFetchInProgress()){
                            if(/*flush_skt->KeyQueueKeyCount() > (kMinUpdateCntForTableFlush>>1) || */SKTMemoryUsage() < kMaxCacheSize || cache_thread_shutdown_.load(std::memory_order_relaxed))
                                flush_skt->PrefetchSKTable(this, true, true);
                            loaded_skt  = false;
#ifdef CACHE_TRACE
                            no_load_for_memory++;
#endif
                        }else{
                            loaded_skt = flush_skt->LoadSKTableDeviceFormat(this, skt_force_load?kSyncGet:kNonblokcingRead);
                            /* it must have been prefetched */
                            assert(!skt_force_load || loaded_skt);
#ifdef CODE_TRACE
                            printf(" load sktdf for skt %p\n", sktable);
#endif
                        }
                    }else if(!flush_skt->IsSKTDirty(this) && (SKTMemoryUsage() > kMaxCacheSize) && !cache_thread_shutdown_.load(std::memory_order_relaxed)){
                        if(!SKTableReclaim(flush_skt)){
                            loaded_skt  = false;
#ifdef CACHE_TRACE
                            evict_dirty_skt_loaded_for_read++;
#endif
                        }
                    }
                    /* sktdf must not be in cache */
                    if(!loaded_skt){
#ifdef MEM_TRACE
                        IncMissFlush();
#endif
                        dirty_list.push_back(flush_skt);
                    }else{
#ifdef MEM_TRACE
                        IncHitFlush();
#endif
#ifdef KV_TIME_MEASURE
                        flush_call++;
#endif
                        flush_skt->FlushSKTableMem(this, col_info_table, comp_col_info_table, in_log_KBM_queue);
                        assert(!flush_skt->IsDeletedSKT());
                        if (MemUseHigh()){
                            //flush_skt->EvictKBPM(this);
                            int64_t reclaim_size = 0;
                            SKTableReclaim(flush_skt, reclaim_size, true, clean_list, dirty_list);
                        }else if(flush_skt->IsSKTDirty(this)){
                            /* Don't need to change status */
                            dirty_list.push_back(flush_skt);
                        }else{
                            flush_skt->ChangeStatus(SKTableMem::flags_in_dirty_cache/*From*/, SKTableMem::flags_in_clean_cache/*To*/);
                            clean_list.push_back(flush_skt);
                        }
                    }
                    if (!GetOffloadFlushCount()){
                        FlushUserKey(in_log_KBM_queue, kbpm_list); //Cleanup flushed_key_hash_
                    }
                    bool wake_kbpm_updater = false;
                    while(!in_log_KBM_queue.empty()) {
                        in_log_kbm = in_log_KBM_queue.front();
#ifdef CODE_TRACE
                        printf("[%d :: %s];Before decrease NRS;%d;|;KBPM;%p;|;iKey;%ld;|\n", __LINE__, __func__,in_log_kbm->GetNRSCount(), in_log_kbm, in_log_kbm->GetiKeySequenceNumber());
#endif
                        if(in_log_kbm->DecNRSCount() == 1){
                            kbpm_list.push_back(in_log_kbm);
                            wake_kbpm_updater= true;
                            /*Use extra NRS count in order to prevent log cleaning*/
                            in_log_kbm->DecNRSCount();
                            kbpm_cache_->Insert(in_log_kbm->GetiKeySequenceNumber(), in_log_kbm);
                            /* 
                             * Increase Cleanupable count 
                             * If the count reaches a treshold, wake LogCleanupThread up 
                             */
                        }
                        in_log_KBM_queue.pop();
                    }
                    if(wake_kbpm_updater) SetLogCleanupRequest();

                    if(!kbpm_list.empty()){
                        wake_kbpm_updater= true;
                        SpliceKBPMCacheList(kbpm_list);
                    }
                    if(wake_kbpm_updater) SignalKBPMUpdateThread();

                }
                if(!clean_list.empty()){
                    SpliceCacheList(clean_list, kCleanSKTCache);
                    SignalSKTEvictionThread(del_skt);
                } else if (del_skt) {
                    SignalSKTEvictionThread(true);
                }

                if(!dirty_list.empty()){
                    SpliceCacheList(dirty_list, kDirtySKTCache);
                }
            }else{
            
                if(!flushed_key_hashmap_->empty()){
                    if (!GetOffloadFlushCount()){
                        FlushUserKey(in_log_KBM_queue, kbpm_list); //Cleanup flushed_key_hash_
                    }
                    bool log_stat_change = false;
                    while(!in_log_KBM_queue.empty()) {
                        in_log_kbm = in_log_KBM_queue.front();
#ifdef CODE_TRACE
                        printf("[%d :: %s];Before decrease NRS;%d;|;KBPM;%p;|;iKey;%ld;|\n", __LINE__, __func__,in_log_kbm->GetNRSCount(), in_log_kbm, in_log_kbm->GetiKeySequenceNumber());
#endif
                        if(in_log_kbm->DecNRSCount() == 1){
                            kbpm_list.push_back(in_log_kbm);
                            log_stat_change= true;
                            /*Use extra NRS count in order to prevent log cleaning*/
                            in_log_kbm->DecNRSCount();
                            kbpm_cache_->Insert(in_log_kbm->GetiKeySequenceNumber(), in_log_kbm);
                            /* 
                             * Increase Cleanupable count 
                             * If the count reaches a treshold, wake LogCleanupThread up 
                             */
                        }
                        in_log_KBM_queue.pop();
                    }
                    if(log_stat_change){
                        SetLogCleanupRequest();
                    }

                    if(!kbpm_list.empty())
                        SpliceKBPMCacheList(kbpm_list);
                    SignalKBPMUpdateThread();
                }
            }
#ifdef KV_TIME_MEASURE
            if(flush_call)
                printf("\n[%d :: %s](flush call : %u)UpdateKeyInfo : %lld ms(%u), InsertUserKeyToKeyMap : %lld ms(%u), Split SKT : %lld ms(%u), InsertUKduring Split : %lld ms(%u)\n", __LINE__, __func__, flush_call, cycle_to_msec(updatekeyinfo_cycle), updatekeyinfo_cnt, cycle_to_msec(insertuk_cycle), insertuk_cnt, cycle_to_msec(split_cycle), split_cnt, cycle_to_msec(insertuk_in_split_cycle), insertuk_in_split_cnt);
            updatekeyinfo_cycle = 0;
            split_cycle = 0;
            updatekeyinfo_cnt = 0;
            split_cnt = 0;
            insertuk_cycle = 0;
            insertuk_cnt = 0;
            insertuk_in_split_cycle = 0;
            insertuk_in_split_cnt = 0;
#endif
            assert(skt_list.empty() && clean_list.empty() && dirty_list.empty());
            ClearFlushThreadBusy();
        }

        if (dev_buffer) free(dev_buffer);
    }

    /* Get updated KBMs from all SKTable KBM of update chache */ 
    void Manifest::KBMUpdateThread()
    {
        // Set update thread name
        (void)pthread_setname_np(pthread_self(), "update");

        std::map<uint64_t, KeyBlockPrimaryMeta*> kbpm_map;
        SKTableMem *skt = NULL;
        std::list<KeyBlockPrimaryMeta*> kbpm_list;

        struct KBPMUpdateVector kbpm_update; 
        std::vector<InSDBKey> delete_key_vec;

        char *kbpm_io_buf = (char*)malloc(kDeviceRquestMaxSize);
        uint32_t kbpm_io_buf_size = kDeviceRquestMaxSize;
        uint32_t kbpm_io_actual_size = 0;

        uint32_t size = 0;

        uint64_t ikey = 0;
        KeyBlockPrimaryMeta* kbpm = NULL;

        uint32_t kbpm_count = 0;

        InSDBKey last_insdbkey;
        last_insdbkey.first = 0;
        last_insdbkey.second = 0;
        InSDBKey null_insdbkey;
        null_insdbkey.first = 0;
        null_insdbkey.second = 0;
        LogRecordKey Logkey(dbhash_);
        InSDBKey old_col_table_key;
        while(true){
            kbpm_update_mu_.Lock();
            /** Wait for a signal */
            /* Do KBM update just one time, and waiting more KBM update */
            bool timeout = false;
            bool logcleanup_queue = false;
            while(!cache_thread_shutdown_)
            {
                ClearKBPMUpdate();
                timeout = kbpm_update_cv_.TimedWait(env_->NowMicros() + 1000000/*Wake up every 1 second*/);
                SetKBPMUpdate();
                if(!IsSKTKBMUpdateListEmpty() || kbpm_map.empty() || (logcleanup_queue = !IsWorkerLogCleanupQueueEmpty()))
                    break;
            }
            kbpm_update_mu_.Unlock();

            if(CheckExitCondition() && kbpm_map.empty()){
                /*Finish the Cache Thread if there is no more SKT in the clean cache list */
                break;
            }

            bool logcleanup_requested = ClearLogCleanupRequest() || (timeout && logcleanup_queue);
            if(!IsSKTKBMUpdateListEmpty()  || !kbpm_map.empty() || logcleanup_requested || (cache_thread_shutdown_ && !IsWorkerLogCleanupQueueEmpty())){
                /*TODO : Flush last Column Info Table before cleanup the Log*/
                InSDBKey col_table_key = GetLastColInfoTableKey();
                if(col_table_key != old_col_table_key){
                    Env::Default()->Flush(kWriteFlush, -1, col_table_key);
                    old_col_table_key = col_table_key;
                }
                SetKBMUpdateThreadBusy();
                if(logcleanup_requested || (cache_thread_shutdown_ && !IsWorkerLogCleanupQueueEmpty()/*missing flag??*/))
                    LogCleanupInternal(Logkey);

#if 1 /* Get updated KBMs from all SKTable KBM of update chache */
                if(!IsSKTKBMUpdateListEmpty()){
                    SwapKBPMCacheList(kbpm_list);
                    kbpm_count = InsertUpdatedKBPMToMap(&kbpm_list, &kbpm_map);
                    assert(kbpm_list.empty());
                }
                kbpm_io_actual_size = kbpm_map.size()*12; // 8 = SizeAlignment(kbpm->GetEstimatedBufferSize() : KBPM_HEADER_SIZE);
#endif
#if 1 /* I/O Buffer Allocation */
                /* KBPM I/O Buffer Allocation */
                if(kbpm_io_actual_size > kbpm_io_buf_size){
                    kbpm_io_buf_size = kbpm_io_actual_size;
                    free(kbpm_io_buf);
                    kbpm_io_buf = (char*)calloc(1, kbpm_io_buf_size);
                }

#endif
#if 1 /* Build KBPM update list & KBPM vectors for device submission */
                std::deque<KeyBlockPrimaryMeta*> updated_kbpm;
                kbpm_count = BuildKBPMUpdateData(&kbpm_map, &kbpm_update, &delete_key_vec, updated_kbpm, kbpm_io_buf);
#endif

#if 1 /* Write update list, KBPM(update or delete)*/
                /* 
                 * Flush the most recently submitted KBPM before taking further KBPM update 
                 */
                if(last_insdbkey.first)
                    Env::Default()->Flush(kWriteFlush, -1, last_insdbkey);

                /*Write update list before update KBPMs*/

                last_insdbkey.first = 0;
                last_insdbkey.second = 0;

                if(!delete_key_vec.empty()) {
                    std::vector<Env::DevStatus> status;
                    Env::Default()->MultiDel(delete_key_vec, null_insdbkey, true, status);
                    last_insdbkey = delete_key_vec.back();
                }

                if(!kbpm_update.empty()){
                    Env::Default()->MultiPut(kbpm_update.key_vec, kbpm_update.data_vec, kbpm_update.size_vec, null_insdbkey, true);
                    last_insdbkey = kbpm_update.key_vec.back();
                }
                while(!updated_kbpm.empty()){
                    kbpm = updated_kbpm.front();
                    updated_kbpm.pop_front();
                    assert(kbpm->IsNeedToUpdate());
                    assert(!kbpm->IsInlog());
                    if(!kbpm->SetWriteProtection()/* The other threads try to change KBPM */){
                        if(!kbpm->GetPendingUpdatedValueCnt()){
                            bool IsInCache = kbpm->IsCache();
                            kbpm->ClearNeedToUpdate();
                            kbpm->ClearWriteProtection();

                            if(!IsInCache && kbpm->CheckColdKBPM(this)){
#ifdef CODE_TRACE
                                printf("[%d :: %s]Free (kbpm : %p), ikey = %ld\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber());
#endif
                                FreeKBPM(kbpm);
                            }

                        }  else {
                            kbpm->ClearWriteProtection();//It may not be needed because no one accesses the KBPM.
                        }
                    }
                }
#endif
                ClearKBMUpdateThreadBusy();
            }

        }
        free(kbpm_io_buf);
        //sem_post(&skt_eviction_shutdown_sem_);
    }

    void Manifest::SKTEvictionThread()
    {
        int ret = pthread_setname_np(pthread_self(), "eviction");
#if 0
        ret = nice(-20);
        if (!ret) {
            printf("nice ret %d\n", ret);
        }
#endif
        while (true) {
            skt_eviction_mu_.Lock();
            /* Make sure MemUseHighUpdate() is called on every evaluation
               since it internally updates mem_use_cached_ which in turn,
               is used by other parts to decide on memory pressure */
            while (((!MemUseHighUpdate() || IsSKTCleanListEmpty()) && !IsCleanToDirty()) &&
                    !cache_thread_shutdown_) {
                std::list<SKTableMem*> skt_list, dirty_list;

                ClearMemoryReclaim();
                skt_eviction_cv_.TimedWait(env_->NowMicros() + 500000/*Wake up every 0.5 second*/);
                SetMemoryReclaim();

                SwapSKTCacheList(skt_list, kCleanSKTCache);
                for (auto it = skt_list.begin(); it != skt_list.end(); ) {
                    SKTableMem *skt = (*it);
                    if (skt->IsSKTDirty(this) || !skt->IsKeyQueueEmpty()) {
                        it = skt_list.erase(it);
                        skt->ChangeStatus(SKTableMem::flags_in_clean_cache/*From*/, SKTableMem::flags_in_dirty_cache/*To*/);
                        dirty_list.push_back(skt);
                    } else {
                        it++;
                    }
                }
                if (!dirty_list.empty()) {
                    SpliceCacheList(dirty_list, kDirtySKTCache);
                    if(update_cnt_ >= kSKTableFlushWatermark)
                        SignalSKTFlushThread();
                }
                if(!skt_list.empty()){
                    SpliceCacheList(skt_list, kCleanSKTCache);
                }
                EvictKBCache();
                EvictKBPMCache();
#if 0
                printf("wakeup Memory usage %ld - kCacheSizeLowWatermark %ld  dirty_list %ld skt_list %ld-->\n",
                        MemoryUsage(), kCacheSizeLowWatermark, dirty_list.size(), skt_list.size());
#endif
            }
            skt_eviction_mu_.Unlock();
            if(CheckExitCondition()){
                /*Finish the Cache Thread if there is no more SKT in the clean cache list */
                break;
            }
            /*?? Is it needed??*/
            bool cleanup = cache_thread_shutdown_.load(std::memory_order_relaxed);
            if(cleanup) kCacheSizeLowWatermark = 0;
            bool to_dirty = ClearCleanToDirty();
            /* Make sure MemUseLowUpdate is called every time.
               See above comments for MemUseHighUpdate() */
            while ((!MemUseLowUpdate() || cleanup) && !IsSKTCleanListEmpty()) {
                SKTableMem *skt = NULL;
                std::list<SKTableMem*> dirty_list, skt_list;

                SetSKTEvictionThreadBusy();
                SwapSKTCacheList(skt_list, kCleanSKTCache);

                if (!skt_list.empty() && ((!MemUseLow()/*For memory reclaim*/ || cleanup))) {
                    int64_t reclaim_size = MemUse() - kCacheSizeLowWatermark;
#ifdef CODE_TRACE
                   printf("before cur Memory usage %ld - kCacheSizeLowWatermark %ld  reclaim_size %ld -->", MemUse(), kCacheSizeLowWatermark, reclaim_size);
#endif
                    EvictUserKeys(skt_list, reclaim_size, cleanup);
#ifdef CODE_TRACE
                    printf("after Memory usage %ld - reclaim size %ld\n", MemUseUpdate(), reclaim_size);
#endif
                } else if (!skt_list.empty()) {
                    SpliceCacheList(skt_list, kCleanSKTCache);
                }
                cleanup = cache_thread_shutdown_.load(std::memory_order_relaxed);
                if(cleanup) kCacheSizeLowWatermark = 0;
                ClearSKTEvictionThreadBusy();
            }
        }
        //sem_post(&skt_eviction_shutdown_sem_);
    }
#if 0
    /*
       Thread unsafe ... call only single context, eq, DB close.
       */
    void Manifest::CheckForceFlushSKT(void) {
        SKTableMem *skt = GetFirstSKTable();
        while(skt) {
            if(skt->KeyQueueKeyCount() || IsNextSKTableDeleted(skt->GetSKTableSortedListNode())){
                assert(0);
            } 
            if(!skt->IsKeyQueueEmpty())
                abort();
            skt = Next(skt);
        }
    }
#endif
    /*
       Thread unsafe ... call only single context, eq, DB close.
       */
    void Manifest::CheckForceFlushSKT() {

        std::list<KeyBlockPrimaryMeta*> kbpm_list;
        std::queue<KeyBlockPrimaryMeta*> in_log_KBM_queue;

        SKTableMem *skt = GetFirstSKTable();

        char* dev_buffer = (char*)malloc(kDeviceRquestMaxSize);
        std::string col_info_table;
        col_info_table.reserve(kDeviceRquestMaxSize);
        std::string comp_col_info_table;
        comp_col_info_table.reserve(kDeviceRquestMaxSize);
        while(skt) {
            if(!skt->IsCached() && skt->IsSKTDirty(this)) {
                abort();
                /* 
                 * It does not need to aquire read lock(sktdf_update_mu_)
                 * because this is last WorkerThread which means the skt is not in flushing by the other threads
                 */
                if(!skt->IsKeymapLoaded()){
                    skt->LoadSKTableDeviceFormat(this);
                }

                //#ifdef CODE_TRACE
                printf("[%d :: %s]Last minute flush SKT : dirty col cnt : %ld || %s (SKT addr : %p || ID : %d) flags 0x%04x\n", __LINE__, __func__, skt->KeyQueueKeyCount(), IsNextSKTableDeleted(skt->GetSKTableSortedListNode())? "Deleted Next SKT":"next skt is NOT deleted", skt, skt->GetSKTableID(), skt->GetFlags());
                //#endif

                skt->FlushSKTableMem(this, col_info_table, comp_col_info_table, in_log_KBM_queue);

                bool log_stat_change = false;
                KeyBlockPrimaryMeta *in_log_kbm = NULL;
                while(!in_log_KBM_queue.empty()) {
                    in_log_kbm = in_log_KBM_queue.front();
                    if(in_log_kbm->DecNRSCount() == 1){
                        kbpm_list.push_back(in_log_kbm);
                        log_stat_change= true;
                        /*Use extra NRS count in order to prevent log cleaning*/
                        in_log_kbm->DecNRSCount();
                        kbpm_cache_->Insert(in_log_kbm->GetiKeySequenceNumber(), in_log_kbm);
                        /* 
                         * Increase Cleanupable count 
                         * If the count reaches a treshold, wake LogCleanupThread up 
                         */
                    }
                    in_log_KBM_queue.pop();
                }
                if(log_stat_change){
                    SetLogCleanupRequest();
                }

                if(!kbpm_list.empty())
                    SpliceKBPMCacheList(kbpm_list);

                if(!skt->IsKeyQueueEmpty())
                    abort();
            } 
            skt = Next(skt);
        }
        free(dev_buffer);
    }




    void Manifest::TerminateCacheThread(){
        // Shut down Cache thread
        cache_thread_shutdown_ = true;
        skt_flush_cv_.SignalAll();
        skt_eviction_cv_.SignalAll();
        kbpm_update_cv_.SignalAll();
        if (skt_flush_thread_tid_)
            Env::Default()->ThreadJoin(skt_flush_thread_tid_);
        if (skt_eviction_thread_tid_)
            Env::Default()->ThreadJoin(skt_eviction_thread_tid_);
        if (kbm_update_thread_tid_)
            Env::Default()->ThreadJoin(kbm_update_thread_tid_);
    }
    /////////////////////////////////////////// END  : Cache Threads(Dirty/KBM update/Clean) related functions ///////////////////////////////////////////

    /////////////////////////////////////////// START: Snapshot/Iterator related functions ///////////////////////////////////////////
    
   SnapshotImpl* Manifest::GetSnapshotImpl(SimpleLinkedNode *node){ return node ? class_container_of(node, SnapshotImpl, snapshot_node_) : NULL; }

    /**
      <Manifest storage format>
      manifest::
      size(4B)
      crc(4B)
      last InSDB sequence(8B)
      last SKTable sequence(4B)
      count of SKTable info(4B)
      count of worker(2B)
      [SKTable information]

      SKTable information::
      SKTable ID(4B)
      size of begin key(var 32)
      begin user key(var)
      list of next ikey sequnce number(each var 64) 
      */
   Status Manifest::GetSnapshotInfo(std::vector<SnapInfo> *snapinfo_list) {
       SnapshotImpl* snapshot = NULL;
       SimpleLinkedNode* snapshot_node = NULL;
       SnapInfo snap_info;
       snapshot_simplelist_lock_.Lock();
       for(snapshot_node = snapshot_simplelist_.newest(); snapshot_node ; snapshot_node = snapshot_simplelist_.next(snapshot_node)) {
           snapshot = GetSnapshotImpl(snapshot_node);
           snap_info.cseq = snapshot->GetCurrentSequenceNumber();
           snap_info.exp = snapshot->GetCurrentTime();
           snapinfo_list->push_back(snap_info); 
       }
       snapshot_simplelist_lock_.Unlock();
       return Status::OK();
   }

   SequenceNumber Manifest::GetOldestSnapshotSequenceNumber() {
       SequenceNumber seq = 0;
       if (!snapshot_simplelist_.empty()) {
           SimpleLinkedNode* snapshot_node = NULL;
           snapshot_simplelist_lock_.Lock();
           if ((snapshot_node = snapshot_simplelist_.oldest())) 
               seq = GetSnapshotImpl(snapshot_node)->GetCurrentSequenceNumber();
           snapshot_simplelist_lock_.Unlock();
       }
       return seq;
   }

   SequenceNumber Manifest::GetLatestSnapshotSequenceNumber() {
       SequenceNumber seq = 0;
       if (!snapshot_simplelist_.empty()) {
           SimpleLinkedNode* snapshot_node = NULL;
           snapshot_simplelist_lock_.Lock();
           if ((snapshot_node = snapshot_simplelist_.newest())) 
               seq = GetSnapshotImpl(snapshot_node)->GetCurrentSequenceNumber();
           snapshot_simplelist_lock_.Unlock();
       }
       return seq;

   }

   SnapshotImpl* Manifest::__CreateSnapshot() {
       SnapshotImpl* snapshot = NULL;
       SimpleLinkedNode* snapshot_node = NULL;
       snapshot_simplelist_lock_.Lock();
       if ((snapshot_node = snapshot_simplelist_.newest())) {
           snapshot = GetSnapshotImpl(snapshot_node);
           assert(snapshot);
           if (snapshot->GetCurrentSequenceNumber() == GetLastSequenceNumber()) {
               snapshot->IncRefCount();
               //RefSnapshot(snap);
               snapshot_simplelist_lock_.Unlock();
#ifdef CODE_TRACE
        printf("[%d :: %s]snapshot(%p) shared %ld\n", __LINE__, __func__, snapshot, snapshot->GetCurrentSequenceNumber());
#endif
               return snapshot;
           }
       }
       snapshot = new SnapshotImpl(this, GetLastSequenceNumber(), env_->NowSecond());
        assert(snapshot);
        snapshot_node = snapshot->GetSnapshotListNode();
        snapshot->IncRefCount();
        //RefSnapshot(snap);
        snapshot_simplelist_.InsertFront(snapshot_node);
        snapshot_simplelist_lock_.Unlock();

#ifdef CODE_TRACE
        printf("[%d :: %s]snapshot(%p) new %ld\n", __LINE__, __func__, snapshot, snapshot->GetCurrentSequenceNumber());
#endif
        return snapshot;
    }

   
    Iterator* Manifest::CreateNewIterator(const ReadOptions& options, uint8_t col_id) {
        nr_iterator_.fetch_add(1, std::memory_order_relaxed);
        if (options.snapshot) {
            Snapshot* snapshot = const_cast<Snapshot*>(options.snapshot);
            SnapshotImpl* snap = reinterpret_cast<SnapshotImpl*>(snapshot);
            snap->IncRefCount();
            //RefSnapshot(snap);
            return snap->CreateNewIterator(col_id, options);
        }
        SnapshotImpl* snap = __CreateSnapshot();
        assert(snap);
        return snap->CreateNewIterator(col_id, options);
    }

    void Manifest::DeleteIterator(Iterator *iter) {
        delete iter;
    }


    bool Manifest::__DisconnectSnapshot(SnapshotImpl* snapshot) {
        snapshot_simplelist_lock_.Lock();
        if (snapshot->DecRefCount() <= 0 && snapshot->GetIterCount() == 0) {
            snapshot_simplelist_.Delete(snapshot->GetSnapshotListNode());
            snapshot_simplelist_lock_.Unlock();
            return true;
        }
        snapshot_simplelist_lock_.Unlock();
        return false;
    }

    Snapshot* Manifest::CreateSnapshot() {
        return reinterpret_cast<Snapshot*>(__CreateSnapshot());
    }

    void Manifest::DeleteSnapshot(Snapshot *handle) {
       SnapshotImpl* snap = reinterpret_cast<SnapshotImpl*>(handle);
       assert(snap);
        if(__DisconnectSnapshot(snap)){
            delete snap; 
        }
    }

    void Manifest::CleanUpSnapshot() {
        SnapshotImpl* snapshot = NULL;
        SimpleLinkedNode* snapshot_node = NULL;
        while((snapshot_node = snapshot_simplelist_.newest())) {
            snapshot = GetSnapshotImpl(snapshot_node);
            assert(snapshot);
            delete snapshot;
        }
    }

    /////////////////////////////////////////// END  : Snapshot related functions ///////////////////////////////////////////

    /////////////////////////////////////////// START: Memory Allocation/Deallocation ///////////////////////////////////////////
    /* It can be Used fo SKTableDFBuilderInfo & SKTable Device Format loading */
    /* The size is kMaxSKTableSize */
    Slice Manifest::AllocSKTableBuffer(uint32_t size)
    {
        Slice buf(0);
        free_skt_buffer_spinlock_.Lock();
        if(!free_skt_buffer_.empty()){
            /* Get recently freed node for CPU cache */
            buf = free_skt_buffer_.back();
            free_skt_buffer_.pop_back();
        }
        free_skt_buffer_spinlock_.Unlock();
        if(!buf.size()) buf = Slice((char*)malloc(size), size);
        else if(buf.size() < size) 
            buf = Slice((char*)realloc(const_cast<char*>(buf.data()), size), size);
        return buf;
    }

    void Manifest::FreeSKTableBuffer(Slice &buffer)
    {
        //assert(buffer.size() <= kMaxSKTableSize * 4);
#ifdef MEM_TRACE
        if(buffer.size() > kMaxSKTableSize * 4)
            printf("[%d :: %s]SKT Size : 0x%lx\n", __LINE__, __func__, buffer.size());
#endif
        free_skt_buffer_spinlock_.Lock();
        if(free_skt_buffer_.size()+1 == kFreeBufferSize)
            free(const_cast<char*>(buffer.data()));
        else
            free_skt_buffer_.push_front(buffer);
        free_skt_buffer_spinlock_.Unlock();
    }

    void Manifest::CleanupFreeSKTableBuffer() {
        Slice node(0);
        while(!free_skt_buffer_.empty()){
            node = free_skt_buffer_.front();
            free_skt_buffer_.pop_front();
            free(const_cast<char*>(node.data()));
        }

    }

#if 0
    void Manifest::RefillFreeUserValueCount(uint32_t size){
        uint64_t refill = free_uservalue_->NeedToRefill();
        while(refill > size){
            free_uservalue_->PushNode(new UserValue(size));
            refill-=size;
        }
        return;

    }
#endif

    UserValue* Manifest::AllocUserValue(uint32_t size)
    {
        return uv_pool_->Get(size);
    }

    void Manifest::FreeUserValue(UserValue* uv)
    {
        if (!uv->UnPin()) {
            uv->InitPinAndFlag();
            uv_pool_->Put(uv);
        }
        return;
    }
#if 0
    void Manifest::RefillFreeUserKeyCount(uint32_t key_size){
        size_t refill = free_userkey_->NeedToRefill();
        UserKey *uk = NULL;
        while(refill){
            uk = new UserKey(key_size);
            free_userkey_->PushNode(uk);
            refill-=uk->GetSize();
            if(refill < uk->GetSize()) break;
        }
    }
#endif

    UserKey* Manifest::AllocUserKey(uint32_t key_size)
    {
#ifdef MEM_TRACE
        IncUK_MEM_Usage(sizeof(UserKey) + kMaxColumnCount * sizeof(UserKey::ColumnData) + key_size);
#endif
#ifdef INSDB_GLOBAL_STATS
            g_new_ukey_cnt++;
#endif
        return uk_pool_->Get(key_size);
    }
    void Manifest::FreeUserKey(UserKey* ukey)
    {
#ifdef MEM_TRACE
        DecUK_MEM_Usage(sizeof(UserKey) + kMaxColumnCount * sizeof(UserKey::ColumnData) + ukey->GetKeySize());
#endif
#ifdef INSDB_GLOBAL_STATS
            g_del_ukey_cnt++;
#endif
        uk_pool_->Put(ukey);
        return;
    }

    ColumnNode* Manifest::AllocColumnNode()
    {
#ifdef MEM_TRACE
        IncCOL_MEM_Usage(sizeof(ColumnNode));
#endif
#ifdef INSDB_GLOBAL_STATS
                g_new_ikey_cnt++;
#endif
        return cn_pool_->Get();
    }
#define DEFAULT_COL_ENTRY_CNT       (1024*1024)
    void Manifest::FreeColumnNode(ColumnNode* col_node)
    {
        assert(!col_node->GetRequestNode());
#ifdef MEM_TRACE
        DecCOL_MEM_Usage(sizeof(ColumnNode));
#endif
#ifdef INSDB_GLOBAL_STATS
                g_del_ikey_cnt++;
#endif
        cn_pool_->Put(col_node);
        return;
    }

    /*
     * this is called from WriteBatchInternal::Store()
     */
    RequestNode* Manifest::AllocRequestNode()
    {
        return req_pool_->Get();
    }

    /* Worker Thread calls it after request submission */
    void Manifest::FreeRequestNode(RequestNode* req_node)
    {
        req_pool_->Put(req_node);
    }

    KeyBlock *Manifest::AllocKB() {
        KeyBlock *kb = kb_pool_->Get();
        kb->InitKB();
        return kb;
    }
    void Manifest::FreeKB(KeyBlock *node) {
        if(node->DecKBRefCnt()){
            if (node->ClearKBPrefetched() && node->GetIKeySeqNum()) {
                InSDBKey kbkey = GetKBKey(GetDBHash(), kKBlock, node->GetIKeySeqNum());
                (GetEnv())->DiscardPrefetch(kbkey);
            }
            kb_pool_->Put(node);
        }
    }
    void Manifest::ClearKBPMPad(KeyBlockPrimaryMeta *kbpm)
    {
        kbpm_pad_[kbpm->GetiKeySequenceNumber()%kbpm_pad_size_].compare_exchange_weak(kbpm, nullptr, std::memory_order_relaxed, std::memory_order_relaxed);
    }
    
    void Manifest::ClearKBPad(KeyBlock *kb)
    {
        kb_pad_[kb->GetIKeySeqNum()%kbpm_pad_size_].compare_exchange_weak(kb, nullptr, std::memory_order_relaxed, std::memory_order_relaxed);
    }

    /////////////////////////////////////////// END  : Memory Allocation/Deallocation ///////////////////////////////////////////
   

    /////////////////////////////////////////// START: User Key & Value[UserKey/SKTDF/KBPM/KBML] Manageement ///////////////////////////////////////////
    bool Manifest::LoadValue(uint64_t insdb_key_seq, uint32_t ivalue_size, bool *from_readahead, KeyBlock* kb, bool report_error)
    {
        Status s;
        int buffer_size = SizeAlignment(ivalue_size);
        InSDBKey insdb_key = GetKBKey(dbhash_, kKBlock, insdb_key_seq);

        Slice *io_buf = GetIOBuffer(kDeviceRquestMaxSize);
        assert(buffer_size <= io_buf->size());

        char *contents = const_cast<char*>(io_buf->data());

        s = env_->Get(insdb_key, contents, &buffer_size, kSyncGet, IsIOSizeCheckDisabled()?Env::GetFlag_NoReturnedSize:0, from_readahead);
        if (!s.ok()) {
            printf("[%d :: %s] key seq %lu size %u from_readahead %d\n", __LINE__, __func__, insdb_key_seq, buffer_size, *from_readahead);
            fprintf(stderr, "keyblock does not exist(ikey seq: %ld)\n", insdb_key_seq);
            if (report_error) {
                return false;
            }
            abort();
        }
#ifdef TRACE_READ_IO
        g_kb_sync++;
        if (from_readahead && *from_readahead)
            g_kb_sync_hit++;
#endif
#ifdef CODE_TRACE
        printf("[%d :: %s] key seq %lu size %u from_readahead %d\n", __LINE__, __func__, insdb_key_seq, buffer_size, *from_readahead);
#endif
        /////////////////
        uint32_t orig_crc = crc32c::Unmask(DecodeFixed32(contents)); /* get crc */
        uint32_t kb_keycnt_size_loc = *((uint32_t*)(contents + 4));
        char* kb_buffer = (char*)(contents + 8)/*CRC(4B) + # of Keys(1B) + Size of compressed KB(3B)*/;
        uint32_t actual_crc = crc32c::Value((contents + 4), (kb_keycnt_size_loc & KEYBLOCK_KB_SIZE_MASK) + 4/* 4 is for "kb_keycnt_size_loc" */);

        if (orig_crc != actual_crc) {
            printf("[%d :: %s](ikey : %ld | ivalue size : %d)crc mismatch check this orig (%d) actual(%d)\n", __LINE__, __func__, insdb_key_seq, ivalue_size, orig_crc, actual_crc);
            abort();
        }
        /////////////

        if(kb_keycnt_size_loc & KEYBLOCK_COMP_MASK){
            /*It was compressed.*/
            size_t ulength = 0;
            if (!port::Snappy_GetUncompressedLength(kb_buffer, kb_keycnt_size_loc & KEYBLOCK_KB_SIZE_MASK, &ulength)) {
                fprintf(stderr, "corrupted compressed data\n");
                abort();
            } else {
                kb->KeyBlockInit(ulength);  /* initially set aligned size */
                if (!port::Snappy_Uncompress(kb_buffer, kb_keycnt_size_loc & KEYBLOCK_KB_SIZE_MASK, const_cast<char*>(kb->GetRawData().data()))) {
                    fprintf(stderr, "corrupted compressed data\n");
                    abort();
                }
            }
        }else{
            uint32_t saved_size = kb_keycnt_size_loc & KEYBLOCK_KB_SIZE_MASK;
            assert(saved_size <= ivalue_size);
            kb->KeyBlockInit(saved_size);  /* initially set aligned size */
            memcpy(const_cast<char*>(kb->GetRawData().data()), kb_buffer, saved_size);
        }
        kb->SetKeyCnt((uint8_t)((kb_keycnt_size_loc & ~KEYBLOCK_COMP_MASK)>> KEYBLOCK_KEY_CNT_SHIFT));
        /////////////
        kb->ClearKBPrefetched();
#ifdef TRACE_READ_IO
        g_loadvalue_sktdf++;
#endif
#ifdef MEM_TRACE
        IncKBPM_MEM_Usage(kb->GetSize());
#endif
        return true;

    }

#ifdef INSDB_USE_HASHMAP
    /////////////////////////////// hopscotch hashmap ///////////////////////////////
#ifdef USE_HOTSCOTCH
    bool Manifest::InsertUserKeyToHopscotchHashMap(UserKey* uk) {
        KeyHashLock();
        auto it = hopscotch_key_hashmap_->insert({uk->GetKeySlice(), uk});
        KeyHashUnlock();
        if(it.second) uk_count_.fetch_add(1, std::memory_order_relaxed);
        return it.second;
    }

    /*Reture UserKey with increased referenced count*/
    UserKey* Manifest::FindUserKeyFromHopscotchHashMap(KeySlice& key) {
        UserKey* uk = NULL;
        while(!uk){
            KeyHashLock();
            auto it = hopscotch_key_hashmap_->find(key);
            if (it != hopscotch_key_hashmap_->end()) {
                KeyHashUnlock();
                uk = it.value();
                uk->IncreaseReferenceCount();
                //if(uk) uk = uk->IsSKTableEvictionInProgress();
            }else{
                KeyHashUnlock();
                return NULL;
            }
        }
        return uk;
    }

    bool Manifest::RemoveUserKeyFromHopscotchHashMap(KeySlice key) {
        uint32_t cnt = uk_count_.fetch_sub(1, std::memory_order_relaxed);
        if( congestion_control_  && cnt < (kCongestionControl - (kCongestionControl>>2))){
            MutexLock l(&congestion_control_mu_);
            congestion_control_= false;
            congestion_control_cv_.SignalAll();
        }
        KeyHashLock();
        bool ret = hopscotch_key_hashmap_->erase(key);
        KeyHashUnlock();

        return ret;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////// folly hashmap /////////////////////////////////
    bool Manifest::InsertUserKeyToHashMap(UserKey* uk) {
        auto it = key_hashmap_->insert(uk->GetKeySlice(), uk);
        if(it.second) uk_count_.fetch_add(1, std::memory_order_relaxed);
        return it.second;
    }

    /*Reture UserKey with increased referenced count*/
    UserKey* Manifest::FindUserKeyFromHashMap(KeySlice& key) {
        UserKey* uk = NULL;
        while(!uk){
            auto it = key_hashmap_->find(key);
            if (it != key_hashmap_->end()) {
                uk = it->second;
                uk->IncreaseReferenceCount();
                if(!uk->IsInHash()){
                    uk->DecreaseReferenceCount();
                    uk = NULL;
                }
            }else
                return NULL;
        }
        return uk;
    }

    bool Manifest::RemoveUserKeyFromHashMap(KeySlice key) {
        uint32_t cnt = uk_count_.fetch_sub(1, std::memory_order_relaxed);
        if( congestion_control_  && cnt < (kCongestionControl - (kCongestionControl>>2))){
#ifdef CODE_TRACE
            printf("[%d :: %s]count : %u\n", __LINE__, __func__, cnt);
#endif
            MutexLock l(&congestion_control_mu_);
            congestion_control_= false;
            congestion_control_cv_.SignalAll();
        }
        return key_hashmap_->erase(key);
    }
    /////////////////////////////////////////////////////////////////////////////////


    UserKey* Manifest::LookupUserKeyFromFlushedKeyHashMap(const KeySlice &key) {
        UserKey* uk = NULL;
        auto it = flushed_key_hashmap_->find(key);
        if (it != flushed_key_hashmap_->end()) {
            uk = it->second;
        }
        return uk;
    }

#if 0
    bool Manifest::InsertUserKeyToFlushedKeyHashMap(UserKey* uk) {
        auto it = flushed_key_hashmap_->insert(uk->GetKeySlice(), uk);
        return it.second;
    }

    bool Manifest::RemoveUserKeyFromFlushedKeyHashMap(const KeySlice& key) {
        return flushed_key_hashmap_->erase(key);
    }
#endif

    void Manifest::FlushedKeyHashMapFindAndMergeUserKey(UserKey* new_uk) {
        UserKey *exist_uk = NULL;
        if(!(exist_uk = LookupUserKeyFromFlushedKeyHashMap(new_uk->GetKeySlice()))){
            // note that the new key could be inserted after its sktable is evicted.
            if (!flushed_key_hashmap_->insert(new_uk->GetKeySlice(), new_uk).second)
                abort();
            flushed_key_cnt_.fetch_add(1, std::memory_order_relaxed);
        }else{
            exist_uk->UserKeyMerge(new_uk, true/*Merge to front*/);
            FreeUserKey(new_uk);
        }
        return;
    }
#ifdef CACHE_TRACE
    uint32_t nr_flush_call = 0;
#endif
    void Manifest::FlushUserKey(std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue, std::list<KeyBlockPrimaryMeta*> &kbpm_list){

        uint32_t remained_keys = 0;
        uint32_t free_keys= 0;
        bool enable_blocking_read = enable_blocking_read_; 
        enable_blocking_read_ = false;
#ifdef CACHE_TRACE
        nr_flush_call++;
#endif
        SimpleLinkedList* col_list = nullptr;
        ColumnNode* col_node = NULL;
        bool iter = false;
        bool dirty = false;
        uint64_t ikey = 0;
        std::vector<SnapInfo> snapinfo_list;
        GetSnapshotInfo(&snapinfo_list);
        int32_t nr_snap = snapinfo_list.size(); 
        if (!flushed_key_hashmap_->empty()) {
            for (auto it = flushed_key_hashmap_->cbegin(); it != flushed_key_hashmap_->cend();) {
                UserKey *uk = it->second;
                assert(uk); 
                dirty = false;
                for( uint8_t col_id = 0 ; col_id < kMaxColumnCount ; col_id++){
                    int32_t snap_index = 0;
                    /*
                     * ColumnLock may not be needed 
                     * dfb_info->ukey->ColumnLock(col_id);
                     */
                    uk->ColumnLock(col_id);
                    col_list = uk->GetColumnList(col_id);

                    col_node = NULL;
                    for(SimpleLinkedNode* col_list_node = col_list->newest(); col_list_node ; ) {
                        ColumnNode* valid_col_node = NULL;

                        col_node = uk->GetColumnNodeContainerOf(col_list_node);
                        /*
                           assert(valid_col_nodes.empty());
                         * col_node can be deleted.(col_list_node exists in col_node ).
                         * Therefore, keep next col_list_node first.
                         */
                        col_list_node = col_list->next(col_list_node);

                        /////////////////////////// Check Iterator /////////////////////////// 
                        iter = false;
                        while(snap_index < nr_snap && col_node->GetBeginSequenceNumber() <= snapinfo_list[snap_index].cseq ){
                            if(col_node->GetEndSequenceNumber() > snapinfo_list[snap_index].cseq && col_node->GetRequestType() == kPutType){
                                iter  = true;
                            }
                            ++snap_index;
                        }
                        ///////////////// Check whether col_node has been submitted or not ///////////////// 
                        /* it can be droped before submission */
                        ikey = col_node->GetInSDBKeySeqNum();
                        KeyBlockPrimaryMeta* kbpm = col_node->GetKeyBlockMeta();
                        ////////////////////////////////////////////////////////////////////// 
                        /* 
                         * It does nothing if the col node belongs to an iterator.
                         * We have a col info table which is a sort of command log.
                         * We can replay a list of commands in the col info table.
                         */
                        if(iter){
                            /* kbpm may not exist if it is Overwritten column*/
#if 0
                            if(kbpm)
                                kbpm->TryToEvictKB(this, col_node->GetKeyBlockOffset());
#endif
                            dirty = true;
                            continue;
                        }
                        ////////////////////////////////////////////////////////////////////// 

                        ////////////////////////////////////////////////////////////////////// 
                        /*
                         * If col_node has been overwrite merged, just delete the col node.
                         * It was alive because of the iterator.
                         */
                        if(col_node->IsOverwritten()){
                            goto delete_col;
                        }
                        assert(ikey);
                        ////////////////////////////////////////////////////////////////////// 
                        assert(kbpm); // kbpm must exist becuase it was prefetched in InsertUserKeyToKeyMapFromInactiveKeyQueue()->InsertColumnNodes()


                        if(kbpm->IsDummy()){
                            while(kbpm->SetWriteProtection());

                            char kbm_buf[kOutLogKBMSize] = {0,};
                            int kbm_buf_size = kOutLogKBMSize;
                            kbpm->SetInSDBKeySeqNum(ikey);
                            if(!enable_blocking_read && kbpm->IsKBMPrefetched()){
                                if(!kbpm->LoadKBPMDeviceFormat(this, kbm_buf, kbm_buf_size)){
                                    /*non blocking read has been fail(the data is not ready in the readahead buffer in device driver)*/
                                    dirty = true;
                                    kbpm->ClearWriteProtection();
                                    //kbpm->ClearKBMPrefetched();
                                    //kbpm->PrefetchKBPM(this);
                                    continue;
                                }
                            }else{
                                if(kbpm->IsDummy())
                                    kbpm->LoadKBPMDeviceFormat(this, kbm_buf, kbm_buf_size, false);
                            }
                            kbpm->InitWithDeviceFormat(this, kbm_buf, (uint16_t)kbm_buf_size);
                            kbpm->ClearWriteProtection();
                        }

                        if(col_node->IsTRXNCommitted()){

                            assert(col_node->GetRequestType() == kPutType);
                            /* update bitmap */
                            assert(kbpm);
                            while(kbpm->SetWriteProtection());
                            kbpm->IncPendingUpdatedValueCnt();
                            kbpm->ClearKeyBlockBitmap(col_node->GetKeyBlockOffset());
                            kbpm->ClearWriteProtection();
                            /*
                             * There is possiblity that previous sktdf node has set reference for those.
                             * To remove dangling reference, clear reference bit here.
                             */
                            kbpm_list.push_back(kbpm);

                            /////////////////////// End Spinlock for Write Protection /////////////////////// 
#ifdef CODE_TRACE
                            printf("[%d :: %s];Not inserted to Inlog;|;KBPM;%p;|;iKey;%ld;|;offset;%d;|\n", __LINE__, __func__,col_node->GetKeyBlockMeta(),col_node->GetInSDBKeySeqNum(),col_node->GetKeyBlockOffset());
#endif
                        }else if(!col_node->GetTGID() || IsTrxnGroupCompleted(col_node->GetTGID())){
                            ColumnStatus col_stat;
                            /* update bitmap */
                            if(col_node->GetRequestType() == kPutType){
                                assert(kbpm);
                                while(kbpm->SetWriteProtection());
                                kbpm->ClearKeyBlockBitmap(col_node->GetKeyBlockOffset());
                                /* It may not needed for in-log KBPM because the last column will be inserted to the pending update queue*/
                                //kbpm->IncPendingUpdatedValueCnt();
                                kbpm->ClearWriteProtection();
                                /*
                                 * There is possiblity that previous sktdf node has set reference for those.
                                 * To remove dangling reference, clear reference bit here.
                                 */
                                //kbpm->ClearRefBitmap(col_node->GetKeyBlockOffset());
                            }
                            /* in_log_KBM_queue is for DecNRSCount() after write updatedKBMList & SKTs*/
                            in_log_KBM_queue.push(kbpm);
#ifdef CODE_TRACE
                            printf("[%d :: %s];Inserted to Inlog;|;KBPM;%p;|;iKey;%ld;|;offset;%d;|\n", __LINE__, __func__,col_node->GetKeyBlockMeta(), col_node->GetInSDBKeySeqNum(), col_node->GetKeyBlockOffset());
#endif
                        }else{
                            /*
                             * KeyBlock has been flushed but the TRXN Group is not completed yet!
                             */
                            dirty = true;
                            continue;
                        }

delete_col:
                        col_list->Delete(col_node->GetColumnNodeNode());
                        col_node->InvalidateUserValue(this);
                        if(uk->GetLatestColumnNode(col_id) == col_node)
                            uk->SetLatestColumnNode(col_id, col_list_node ? uk->GetColumnNodeContainerOf(col_list_node): NULL);
#ifndef NDEBUG
                        if(col_node->GetRequestNode()) {
                            assert(col_node->GetRequestNode()->GetRequestNodeLatestColumnNode() != col_node);
                        }
#endif
                        col_node->SetRequestNode(nullptr);
                        FreeColumnNode(col_node);
                    }

                    /**
                     * The invalid list in a Column is not empty
                     */
                    uk->ColumnUnlock(col_id);
                }
                if(!dirty){
                    free_keys++;
                    it = flushed_key_hashmap_->erase(it);
                    flushed_key_cnt_.fetch_sub(1, std::memory_order_relaxed);
                    assert(!uk->GetReferenceCount());
                    FreeUserKey(uk);
                }else{
                    remained_keys++;
                    ++it;
                }
            }
        }
        if(remained_keys > free_keys){
            enable_blocking_read_ = true;
        }
#ifdef CACHE_TRACE
        if(!(nr_flush_call % 1000)){
            printf("[%d :: %s]nr_flush_call : %u || free_keys : %u || remained_keys : %u\ninsert(dirty:clean:dirty w/o keymap = %ld:%ld:%ld) evict : %ld || clean & dirty cache : %ld : %ld\nmem_usage : skt_usage = %.2fMB : %.2fMB || UK count in Hash : %d || Flush\nSkip Dirty SKT load due to memory: %lu || evict dirty SKT loaded for read : %lu\n", __LINE__, __func__, nr_flush_call, free_keys, remained_keys, dirty_insert, clean_insert, dirty_wo_keymap_insert, evict_skt, skt_cache_[1].GetCacheSize(), skt_cache_[0].GetCacheSize(), MemoryUsage()/1024.0/1024.0, SKTMemoryUsage()/1024.0/1024.0, GetHashSize(), no_load_for_memory, evict_dirty_skt_loaded_for_read);
            dirty_insert = 0;
            clean_insert = 0;
            dirty_wo_keymap_insert = 0;
            evict_skt = 0;
        }
#endif
    }


    void Manifest::FlushSingleSKTableMem(SKTableMem* sktable, std::string &col_info_table, std::string &comp_col_info_table){

        std::list<KeyBlockPrimaryMeta*> kbpm_list;
        std::queue<KeyBlockPrimaryMeta*> in_log_KBM_queue;

        if(!sktable->IsKeymapLoaded()){
#if 0
            if(!sktable->LoadSKTableDeviceFormat(this))
                abort();
#else
            sktable->LoadSKTableDeviceFormat(this);
#endif
        }
        sktable->FlushSKTableMem(this, col_info_table, comp_col_info_table, in_log_KBM_queue);

        bool log_stat_change = false;
        KeyBlockPrimaryMeta *in_log_kbm = NULL;
        while(!in_log_KBM_queue.empty()) {
            in_log_kbm = in_log_KBM_queue.front();
#ifdef CODE_TRACE
            printf("[%d :: %s];Before decrease NRS;%d;|;KBPM;%p;|;iKey;%ld;|\n", __LINE__, __func__,in_log_kbm->GetNRSCount(), in_log_kbm, in_log_kbm->GetiKeySequenceNumber());
#endif
            if(in_log_kbm->DecNRSCount() == 1){
                kbpm_list.push_back(in_log_kbm);
                log_stat_change= true;
                /*Use extra NRS count in order to prevent log cleaning*/
                in_log_kbm->DecNRSCount();
                kbpm_cache_->Insert(in_log_kbm->GetiKeySequenceNumber(), in_log_kbm);
                /* 
                 * Increase Cleanupable count 
                 * If the count reaches a treshold, wake LogCleanupThread up 
                 */
            }
            in_log_KBM_queue.pop();
        }
        if(log_stat_change){
            SetLogCleanupRequest();
        }

        if(!kbpm_list.empty())
            SpliceKBPMCacheList(kbpm_list);


        assert(!sktable->IsDeletedSKT());
#if 0
        if(sktable->IsSKTDirty(this)){
            /* Don't need to change status */
            this->InsertSKTableMemCache(sktable, kDirtySKTCache); 
        }else{
            sktable->ChangeStatus(SKTableMem::flags_in_dirty_cache/*From*/, SKTableMem::flags_in_clean_cache/*To*/);
            this->InsertSKTableMemCache(sktable, kCleanSKTCache); 
        }
#else
        if (MemUseHigh()) {
            //flush_skt->EvictKBPM(this);
            std::list<SKTableMem*> clean_list;
            std::list<SKTableMem*> dirty_list;
            int64_t reclaim_size = 0;
            SKTableReclaim(sktable, reclaim_size, true, clean_list, dirty_list);

            if(!clean_list.empty()){
                SpliceCacheList(clean_list, kCleanSKTCache);
            } 
            if(!dirty_list.empty()){
                SpliceCacheList(dirty_list, kDirtySKTCache);
            }
        }else if(sktable->IsSKTDirty(this)){
            /* Don't need to change status */
            this->InsertSKTableMemCache(sktable, kDirtySKTCache); 
        }else{
            sktable->ChangeStatus(SKTableMem::flags_in_dirty_cache/*From*/, SKTableMem::flags_in_clean_cache/*To*/);
            this->InsertSKTableMemCache(sktable, kCleanSKTCache); 
        }

#endif
#if 0
        /*For deleted SKTable. but new flush function never delete skt even if skt has no keys*/
        else{
            assert(sktable->IsCached());
            sktable->ClearInDirtyCache();
            mf_->SignalSKTEvictionThread(true);
            assert(!sktable->IsCached());
        }
#endif
        DecOffloadFlushCount();
    }

    void Manifest::WorkerThreadFlushSKTableMem(std::string &col_info_table, std::string &comp_col_info_table){
        SKTableMem* sktable = NULL;
        while(sktable = GetFlushOffloadSKTable())
            FlushSingleSKTableMem(sktable, col_info_table, comp_col_info_table);
    }


    void Manifest::DoCongestionControl() {
#if 1
        if(congestion_control_ || uk_count_.load(std::memory_order_relaxed) > kCongestionControl ){
#ifdef KV_TIME_MEASURE
            printf("\n[%d :: %s]Start Congestion Control\n", __LINE__, __func__);
            cycles_t t1, t2;
            t1 = get_cycles();
#endif
            SKTableMem *sktable = NULL;
            MutexLock l(&congestion_control_mu_);
            if (congestion_control_ || uk_count_.load(std::memory_order_relaxed) > kCongestionControl) {
                congestion_control_ = true;
                congestion_control_cv_.Wait();
            }
#ifdef KV_TIME_MEASURE 
            t2 = get_cycles(); 
            printf("[%d :: %s]Congestion Control Duration : %lld\n", __LINE__, __func__, time_msec_measure(t1,t2));
#endif
#if 0
            char* dev_buffer = (char*)malloc(kDeviceRquestMaxSize);
            std::string col_info_table;
            col_info_table.reserve(kDeviceRquestMaxSize);
            Slice comp_col_info_table(dev_buffer, kDeviceRquestMaxSize);
            while(congestion_control_){
                if(sktable = GetFlushOffloadSKTable()){
                    FlushSingleSKTableMem(sktable, col_info_table, comp_col_info_table);
                }
            }
            free(dev_buffer);
#endif

        }
#endif
    }


    void Manifest::ReleaseCongestionControl(){
        if( congestion_control_  && uk_count_.load(std::memory_order_relaxed) < (kCongestionControl  - (kCongestionControl>>2))/* 25% */){
            MutexLock l(&congestion_control_mu_);
            congestion_control_= false;
            congestion_control_cv_.SignalAll();
        }
    }
        


    bool Manifest::InsertKBPMToHashMap(KeyBlockPrimaryMeta *kbpm) {
        assert(kbpm->GetiKeySequenceNumber());
        auto it = kbpm_hashmap_->insert(kbpm->GetiKeySequenceNumber(), kbpm);
        kbpm_cnt_.fetch_add(1, std::memory_order_relaxed);
        return it.second;
    }

    /*Reture UserKey with increased referenced count*/
    KeyBlockPrimaryMeta* Manifest::FindKBPMFromHashMap(SequenceNumber ikey) {
        auto it = kbpm_hashmap_->find(ikey);

        if (it != kbpm_hashmap_->end()) {
            return it->second;
        }else
            return NULL;
    }

    void Manifest::RemoveKBPMFromHashMap(SequenceNumber ikey) {
        kbpm_cnt_.fetch_sub(1, std::memory_order_relaxed);
        kbpm_hashmap_->erase(ikey);
    }

    bool Manifest::KeyHashMapFindOrInsertUserKey(UserKey*& uk, const KeySlice& key, const uint8_t col_id ) {
#ifdef USE_HOTSCOTCH
        while(!(uk = FindUserKeyFromHopscotchHashMap(const_cast<KeySlice& >(key))))
#else
        while(!(uk = FindUserKeyFromHashMap(const_cast<KeySlice& >(key))))
#endif
        {
            // note that the new key could be inserted after its sktable is evicted.
            uk = AllocUserKey(key.size());
            uk->InitUserKey(key);
            uk->ColumnLock(col_id);/* Lock the column */
#ifdef USE_HOTSCOTCH
            if(InsertUserKeyToHopscotchHashMap(uk)){
                return true;// New UK has been successfully inserted.
            }
#else
            if (InsertUserKeyToHashMap(uk)) {
                return true;// New UK has been successfully inserted.
            }
#endif
            uk->ColumnUnlock(col_id);/* Unlock and Free the UK */
            FreeUserKey(uk);
        }
#ifdef USE_HOTSCOTCH
#if 0
        while(uk != FindUserKeyFromHopscotchHashMap(const_cast<KeySlice& >(key))){
            printf("[%d :: %s]UK does not exist in hopscotch\n", __LINE__, __func__);
        //    abort();
        }
#endif
#endif
        /*We found the key in the HashMap*/
        uk->ColumnLock(col_id);/* Lock the column */
        return false;//Don't need to insert the uk to unsorted key queue(skt->key_queue_[active])
    }
#endif

    UserKey* Manifest::GetFirstUserKey(){
        printf("Not implemented %s\n", __func__); 
        return nullptr;
    }

    UserKey* Manifest::GetLastUserKey(){ 
        return nullptr;
    }

    UserKey* Manifest::SearchUserKey(const Slice& tkey){ 
        printf("Not implemented %s\n", __func__); 
        return nullptr;
    }

    UserKey* Manifest::SearchLessThanEqualUserKey(const Slice& tkey){ 
        printf("Not implemented %s\n", __func__); 
        return nullptr;
    }

    bool Manifest::InsertUserKey(UserKey* ukey, bool begin_key) {
        printf("Not implemented %s\n", __func__); 
        return false;
    }

    /* 
     * Iterator call this function with ref_cnt_inc == false.
     * EvictUserKeys can not selete the SKTable to which KBPM belongs.
     */
    KeyBlockPrimaryMeta* Manifest::GetKBPMWithiKey(SequenceNumber seq, uint8_t offset) {
        KeyBlockPrimaryMeta* kbpm = NULL;
retry:
        // Try KBPM pad
        bool kbpm_found_in_pad;
        kbpm = kbpm_pad_[seq%kbpm_pad_size_].load(std::memory_order_relaxed);
        if (kbpm && kbpm->GetiKeySequenceNumber() == seq) {
            kbpm_found_in_pad = true;
            goto kbpm_found;
        } else {
            kbpm_found_in_pad = false;
        }

        /* search from hash table */
        kbpm = FindKBPMFromHashMap(seq);
        if (!kbpm) {
            /* create dummy and insert to hash table */
            kbpm = AllocKBPM();
            kbpm->DummyKBPMInit(seq);
            kbpm->SetDummy();
            kbpm->SetOutLog();
            kbpm->SetRefBitmap(offset);
#ifdef CODE_TRACE
            printf("[%d :: %s]Alloc (kbpm : 0x%p), ikey = %ld\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber());
#endif
            if(!InsertKBPMToHashMap(kbpm)) {
                kbpm_cnt_.fetch_sub(1, std::memory_order_relaxed);
#ifdef CODE_TRACE
                printf("[%d :: %s]Free (kbpm : 0x%p), ikey = %ld\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber());
#endif
                FreeKBPM(kbpm);
                goto retry;
            }
            kbpm_pad_[seq%kbpm_pad_size_].store(kbpm, std::memory_order_relaxed);

#ifdef MEM_TRACE
            IncKBPM_MEM_Usage(sizeof(KeyBlockPrimaryMeta));
#endif

        }else{
kbpm_found:
            /* It can already exist if con info has ref bit */
            //assert(!IsRefBitmapSet(offset));
            kbpm->SetRefBitmap(offset);
            if(kbpm->IsEvicting()){
#ifdef CODE_TRACE
                printf("[%d :: %s] evicting is set(kbpm : %p)\n", __LINE__, __func__, kbpm);
#endif
                kbpm->ClearRefBitmap(offset);
                goto retry;
            }
            if (!kbpm_found_in_pad)
                kbpm_pad_[seq%kbpm_pad_size_].store(kbpm, std::memory_order_relaxed);
        }
        return kbpm;
    }

    SequenceNumber Manifest::GetLatestSequenceFromKeymap(const KeySlice& key, uint8_t col_id, bool &deleted)
    { 
        SKTableMem* table = NULL;
        SequenceNumber seq_num = 0;

retry:
        table = FindSKTableMem(key);
        table->IncSKTRefCnt(this);

        if(Next(table) && (GetComparator()->Compare(Next(table)->GetBeginKeySlice(), key) <= 0)){
            /*The key belongs to next(or beyond) SKTable. */
            table->DecSKTRefCnt();
            goto retry;
        }
        /* Reference count prevents eviction */
        seq_num = table->GetLatestSequenceNumberFromSKTableMem(this, key, col_id, deleted);
        table->DecSKTRefCnt();
        return seq_num;
    }

    Status Manifest::GetValueFromKeymap(const KeySlice& key, std::string* value, PinnableSlice* pin_slice, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time, NonBlkState &non_blk_flag_)
    { 
        bool ra_rahit = false;
        KeyBlock* kb = NULL;
        ColumnScratchPad column = {nullptr, 0,0,0};
        SKTableMem* table = NULL;


        while(true){
            table = FindSKTableMem(key);
            table->IncSKTRefCnt(this);
            /* Reference count prevents eviction */
            if(table->GetColumnInfo(this, key, col_id, seq_num, cur_time, column, non_blk_flag_))
                break;
            table->DecSKTRefCnt();
        }

        if (column.uv) {
            if (value) {
                value->assign(column.uv->GetBuffer(), column.uv->GetValueSize());
            } else {
                column.uv->Pin();
                pin_slice->PinSlice(column.uv->GetValueSize(), PinnableSlice_UserValue_CleanupFunction, column.uv, this);
            }
            table->DecSKTRefCnt();
            return Status::OK();
        }

        if(!column.ikey_seq_num) {
            table->DecSKTRefCnt();
            if (non_blk_flag_ == kNonblockingAfter) {
                return Status::Incomplete("Not found in cache, no_io is set");
            }
            return Status::NotFound("Key does not exist"); /*No column exists*/
        }

        kb = FindKBCache(column.ikey_seq_num);
        assert(kb);
        if (kb->IsDummy()) {
            kb->KBLock();
            if (kb->IsDummy()) {
                LoadValue(column.ikey_seq_num, column.ivalue_size, &ra_rahit, kb);
                DummyKBToActive(column.ikey_seq_num, kb);
            }
            kb->KBUnlock();
        }

        /* get value from kb */
        Slice value_slice = kb->GetValueFromKeyBlock(this, column.offset);
        if (value) {
            value->assign(value_slice.data(), value_slice.size());
            FreeKB(kb);
        } else {
            /*
             * SKTable ref cnt has been increased and it will be decreased in pinnable cleanup function 
             * if SKTable has ref cnt, any key(KBPM & KB) in this SKT can not be evicted.
             */
            pin_slice->PinSlice(value_slice, PinnableSlice_KeyBlock_CleanupFunction, kb, this);
        }
        table->DecSKTRefCnt();
        return Status::OK();
    }
    /////////////////////////////////////////// END  : UserKey/KBPM/KBML Manageement ///////////////////////////////////////////

    /////////////////////////////////////////// START: Transaction(Group) Management ///////////////////////////////////////////

    bool Manifest::IsTrxnGroupCompleted(TrxnGroupID tgid){
        bool ret  = false;
        uctrxn_group_lock_.Lock();
        /* Find TRXN Group */
        std::unordered_map<TrxnGroupID, uint32_t>::iterator it = uncompleted_trxn_group_.find(tgid);
        if(it==uncompleted_trxn_group_.end()) 
            ret = true;
        uctrxn_group_lock_.Unlock();
        return ret;
    }

    TransactionID Manifest::GetTrxnID(uint32_t count){
        TransactionID trxn_id;
        uctrxn_group_lock_.Lock();
        /* Allocate TRXN ID */
        trxn_id = next_trxn_id_; 
        if(count > 4096){
            next_trxn_id_ = ((trxn_id >> TGID_SHIFT)+ 1) << TGID_SHIFT; // Move to next TRXN Group.
        }else
            next_trxn_id_++; 

        /* Add to TRXN Group */
        std::unordered_map<TrxnGroupID, uint32_t>::iterator it = uncompleted_trxn_group_.find(trxn_id >> TGID_SHIFT);
        if(it==uncompleted_trxn_group_.end()) 
            uncompleted_trxn_group_.insert({(trxn_id >> TGID_SHIFT), count});
        else 
            it->second += count;

        uctrxn_group_lock_.Unlock();
        return trxn_id;
    }

    /* It can be called from FlushSKTableMem() or Store() 
     * FlushSKTableMem() : call this function after Device Flush
     *                     But the ColumnNode is not recorded in the SKTable
     *                     This means the ColumnNode is still in Worker's Log.
     *                     If all trxns in the TRXN group are flushed, 
     *                     The trxn group node will be deleted from uncompleted_trxn_group_.
     *                     In this case, the ColumnNode will be recored in the SKTable during the next FlushSKTable
     *  Store() : If "delete command" is canceled in Store(), the "count" should be decremented by 1.
     *
     *  tgid is Transaction Group ID calculated by "trxn ID >> TGID_SHIFT"
     *  caller must use TRXN Group ID instead of TRXN ID.
     *
     *  return true if the TRXN Group is completed.(All TRXNs in the Group has been flushed)
     *  return false if the TRXN Group is NOT completed.
     */
    void Manifest::DecTrxnGroup(TrxnGroupID tgid, uint32_t cnt){
        bool result = false;
        uctrxn_group_lock_.Lock();

        std::unordered_map<TrxnGroupID, uint32_t>::iterator it = uncompleted_trxn_group_.find(tgid);
#ifdef ENABLE_SANITYCHECK
        if(it == uncompleted_trxn_group_.end()) abort();//The transaction group must exist.
#else
        if(it == uncompleted_trxn_group_.end()) { uctrxn_group_lock_.Unlock(); return; }
#endif
        else it->second -= cnt;
        if(!it->second){
            /* In this case, the trxn group is not fully used. 
             * This can occur when trxn requests rarely occur while there are many non-trxn requests.
             * For this, we don't have to wait for the Group to be completed.
             * We just make Group complete.
             */
            if(tgid  == (next_trxn_id_ >> TGID_SHIFT))
                next_trxn_id_ = (tgid + 1) << TGID_SHIFT; // Move to next TRXN Group.
            uncompleted_trxn_group_.erase(it);
            result = true;

        }
        uctrxn_group_lock_.Unlock();
        if(result) AddCompletedTrxnGroupRange(tgid);
        return;
    }

    bool Manifest::IsCompletedTrxnGroupRange(std::list<TrxnGroupRange> &completed_trxn_group_list, TrxnGroupID tgid){
        bool completed = false;
        for(auto it = completed_trxn_group_list.begin() ; it != completed_trxn_group_list.end() ; it++){
            if( it->start <= tgid &&  tgid <= it->end ) {
                completed = true;
                break;
            }
        }
        return completed;
    }

#if 0
    bool Manifest::IsCompletedTrxnGroupRange(TrxnGroupID tgid){
        bool completed = false;
        ctrxn_group_lock_.Lock();
        int i = 0; 

        for(auto it = completed_trxn_group_list_.begin() ; it != completed_trxn_group_list_.end() ; it++, i++){
            if( it->start <= tgid &&  tgid <= it->end ) {
                completed = true;
                break;
            }
        } 
        ctrxn_group_lock_.Unlock();
        return completed;
    }
#endif

    void Manifest::AddCompletedTrxnGroupRange(TrxnGroupID tgid){
        ctrxn_group_lock_.Lock();
        for(auto it = completed_trxn_group_list_.begin() ; it != completed_trxn_group_list_.end() ; it++){
            if(tgid == it->start - 1){//It means previous range cannot merge the range with this range 
                it->start = tgid; 
                break;
            }else if(tgid < it->start){ // last element has max value
                completed_trxn_group_list_.insert(it, TrxnGroupRange(tgid)); 
                break;
            }else if(it->end + 1 == tgid){
                /**
                 * Can not reach the end because end has maximum TRXN number(0xffff ffff)
                 */
                std::list<TrxnGroupRange>::iterator cur = it; 
                it++; 

                if( tgid == it->start - 1 ){ // Merge the range
                    cur->end = it->end;
                    completed_trxn_group_list_.erase(it);
                }else{ 
                    cur->end = tgid; 
                }
                break;
            }  
        } 
        ctrxn_group_lock_.Unlock();
    }

    void Manifest::CopyCompletedTrxnGroupRange(std::list<TrxnGroupRange> &completed_trxn_groups) {
        ctrxn_group_lock_.Lock();
        for(auto completed_trxn_group : completed_trxn_group_list_)
            completed_trxn_groups.push_back(completed_trxn_group);
        ctrxn_group_lock_.Unlock();
    }
    /////////////////////////////////////////// END  : Transaction(Group) Management ///////////////////////////////////////////

    bool Manifest::AddKBCache(SequenceNumber key, KeyBlock* kb) {
        kb->SetIKeySeqNum(key);
        kb_pad_[key%kbpm_pad_size_].store(kb, std::memory_order_relaxed);
        return kb_cache_->Insert(key, kb);
    }

    KeyBlock* Manifest::RefKBCache(SequenceNumber key) {
        return kb_cache_->Reference(key);
    }

    KeyBlock* Manifest::FindKBCache(SequenceNumber key) {
        KeyBlock* kb = nullptr;
        kb = kb_pad_[key%kbpm_pad_size_].load(std::memory_order_relaxed);
        if (kb && kb->GetIKeySeqNum() == key) {
            if (!kb->IsEvicting()) {
                kb->IncKBRefCnt();
                kb->SetReference();
                return kb; 
            }
        }   
try_agin:
        kb = kb_cache_->Find(key);
        if (!kb) {
            kb = AllocKB();
            kb->SetIKeySeqNum(key);
            kb->SetDummy();
            if (!kb_cache_->InsertHashOnly(key, kb)) {
                FreeKB(kb);
                kb = kb_cache_->Find(key);
                assert(kb);
                if (kb->IsEvicting()) {
                    FreeKB(kb);
                    goto try_agin;
                }
            } else {
                kb->IncKBRefCnt();
            }
        } else {
            if (kb->IsEvicting()) {
                FreeKB(kb);
                goto try_agin;
            }
            if (!kb->IsDummy()) kb_pad_[key%kbpm_pad_size_].store(kb, std::memory_order_relaxed);
        }
        return kb;
    }
   
    void Manifest::RemoveDummyKB(KeyBlock* kb) {
        if (!kb->SetEvicting()) {
            kb_cache_->RemoveHashOnly(kb->GetIKeySeqNum());
            FreeKB(kb);
        }
    }

    void Manifest::DummyKBToActive(SequenceNumber key, KeyBlock *kb) {
        kb->ClearDummy();
        kb_pad_[key%kbpm_pad_size_].store(kb, std::memory_order_relaxed);
        kb_cache_->AppendDataOnly(key, kb);
    }

    KeyBlock* Manifest::FindKBCacheWithoutRef(SequenceNumber key) {
        return kb_cache_->FindWithoutRef(key);
    }

    void Manifest::EvictKBCache() {
        kb_cache_->Evict(this);
    }

    void Manifest::CleanupKBCache() {
        kb_cache_->Clear(this);
    }

    void Manifest::EvictKBPMCache() {
        kbpm_cache_->Evict(this);
    }
    
    void Manifest::AddKBPMCache(SequenceNumber key, KeyBlockPrimaryMeta* meta) {
        kbpm_cache_->Insert(key, meta);
    }


    // Compress, align size, and add CRC
    void LogRecordKey::BuildIOBuffer() {
        // Align uncompressed buffer
        uint32_t unaligned_size = buffer_.size()%kRequestAlignSize;
        uint16_t buffer_padding_size = 0;
        if (unaligned_size)
        {
            buffer_padding_size = kRequestAlignSize - unaligned_size;
            buffer_.append(buffer_padding_size, 0);
        }

        // Try compression
#ifdef HAVE_SNAPPY
        // reserve io buffer by max compressed size
        uint32_t uncompressed_body_size = buffer_.size()- LOGRECKEY_OFFSET_BODY;
        io_buffer_.reserve( LOGRECKEY_OFFSET_BODY + snappy::MaxCompressedLength(uncompressed_body_size));
        // append zeroed header ( CRC, format flags )
        io_buffer_.append( LOGRECKEY_OFFSET_BODY, 0);
        // body compressed size
        size_t outlen = 0;

        // compress
        snappy::RawCompress(const_cast<char *>(buffer_.data())+ LOGRECKEY_OFFSET_BODY, uncompressed_body_size, const_cast<char *>(io_buffer_.data())+ LOGRECKEY_OFFSET_BODY, &outlen);
        io_buffer_.resize( LOGRECKEY_OFFSET_BODY+outlen);

        /*
         * compressed less than 12.5%, just store uncompressed form
         * This ratio is the same as that of LevelDB/RocksDB
         *
         * Regardless of the compression efficiency, We may not have any penalty even if we store compressed data.
         * But read have to decompress the data.
         */
        if(io_buffer_.size() < (buffer_.size()-(buffer_.size() / 8u))){
            // Align compressed size
            unaligned_size = io_buffer_.size()%kRequestAlignSize;
            uint16_t padding_size = kRequestAlignSize - unaligned_size;
            if (unaligned_size)
                io_buffer_.append(padding_size, 0);

            SetIOBufferPaddingSize(true, padding_size);
        }else
#endif
        {
            io_buffer_ = buffer1_;
            SetIOBufferPaddingSize(false, buffer_padding_size);
        }

        // Calc CRC excluding itself.
        uint32_t crc = crc32c::Value(io_buffer_.data() + LOGRECKEY_OFFSET_COMP_PADDING_SIZE, io_buffer_.size() - LOGRECKEY_OFFSET_COMP_PADDING_SIZE);
        EncodeFixed32(const_cast<char *>(io_buffer_.data()), crc32c::Mask(crc));
    }

    Status LogRecordKey::Load(Env *env) {
        Reset();
        buffer2_.reserve(kDeviceRquestMaxSize);
        buffer2_.append(kDeviceRquestMaxSize, 0x0);
        int size = kDeviceRquestMaxSize;
        Status s = env->Get(GetInSDBKey(), const_cast<char *>(buffer2_.data()), &size);
        if (s.ok()) buffer2_.resize(size);
        return s;
    }

    bool LogRecordKey::DecodeBuffer(std::vector<uint64_t> &workers_next_ikey_seqs, std::list<TrxnGroupRange> &completed_trxn_groups) {

        bool success;
        Slice buffer(buffer2_);
        Slice uncomp_buffer(0);

        // CRC
        if(buffer.size() < sizeof(uint32_t)) return false;
        uint32_t crc = crc32c::Unmask(DecodeFixed32(buffer.data()));
        buffer.remove_prefix(sizeof(uint32_t));

        // flags and padding size
        if(buffer.size() < sizeof(uint32_t)) return false;

        // CRC check
        uint32_t new_crc = crc32c::Value(buffer.data(), buffer.size());
        if(new_crc != crc) {
            printf("log recrod key crc mismatch stored crc %x calculated crc %x\n", crc, new_crc);
            return false;
        }

        uint16_t padding_size = DecodeFixed32(buffer.data());
        // discard 4 bytes including reserved space
        buffer.remove_prefix(sizeof(uint32_t));
        bool snappy_compress = (padding_size&LOGRECKEY_FORMAT_FLAGS_SNAPPY_COMPRESS) != 0;
        padding_size &= LOGRECKEY_FORMAT_PADDING_SIZE_MASK;

        if(padding_size >= kRequestAlignSize)
            return false;
        size_t actual_size = buffer.size()-padding_size;
#ifdef HAVE_SNAPPY
        if(snappy_compress) {
            size_t uncomress_len = 0;
            success = snappy::GetUncompressedLength(buffer.data(), actual_size, &uncomress_len);
            if(!success)
                return false;
            buffer1_.resize(uncomress_len);
            success = snappy::RawUncompress(buffer.data(), actual_size, const_cast<char*>(buffer1_.data()));
            if(!success)
                return false;
            // get the uncompressed buffer
            uncomp_buffer = Slice(buffer1_);
        } else {
            // get the uncompressed buffer
            uncomp_buffer = Slice(buffer.data(), buffer.size());
        }
#else
        if(snappy_compress)
            return false;
        else {
            // get the uncompressed buffer
            uncomp_buffer = Slice(buffer.data(), buffer.size());
        }

#endif

        // number of worker next ikeys
        uint32_t nr_workers;
        success = GetVarint32(&uncomp_buffer, &nr_workers);
        if(!success)
            return false;

        if(nr_workers != (uint32_t)kWriteWorkerCount) 
            printf("[%s:%d] nr_worker(%d) is not match kWriteWorkerCount(%d)\n", __func__, __LINE__, nr_workers, kWriteWorkerCount);

        // list of worker next ikeys
        uint32_t idx;
        for(idx=0; idx < nr_workers; idx++) {
            uint64_t ikey_seq = 0;
            success = GetVarint64(&uncomp_buffer, &ikey_seq);
            if(!success)
                return false;
            workers_next_ikey_seqs.push_back(ikey_seq);
        }

        // number of completed transaction groups
        uint32_t nr_completed_groups;
        success = GetVarint32(&uncomp_buffer, &nr_completed_groups);
        if(!success)
            return false;

        // list of completed transaction groups
        for(idx=0; idx < nr_completed_groups; idx++) {
            uint64_t trxn_start = 0, trxn_end = 0;
            success = GetVarint64(&uncomp_buffer, &trxn_start);
            if(!success)
                return false;
            success = GetVarint64(&uncomp_buffer, &trxn_end);
            if(!success)
                return false;
            TrxnGroupRange trxn_range(trxn_start, trxn_end);
            completed_trxn_groups.push_back(trxn_range);
        }
#if 0
        // number of deleted internal keys
        uint32_t nr_deleted_keys;
        success = GetVarint32(&uncomp_buffer, &nr_deleted_keys);
        if(!success)
            return false;

        // list of deleted internal keys
        uint64_t smallest_iseq;
        for(idx=0; idx < nr_deleted_keys; idx++) {
            uint64_t ikey_seq = 0;
            success = GetVarint64(&uncomp_buffer, &ikey_seq);
            if(!success)
                return false;

            // first iseq is the smallest ikey sequence.
            // add it to the next iseq numbers
            if (idx == 0)
                smallest_iseq = ikey_seq;
            else
                ikey_seq += smallest_iseq;

            deleted_ikey_seqs.push_back(ikey_seq);
        }
#endif
        return true;
    }
 

    void Manifest::PrintParam() {
#ifdef TRACE_MEM
        printf("#LF_skt_dirty[%ld] #LF_skt_kbm_clean[%ld]\n",
                skt_cache_[kDirtySKTCache].cache.size(),
                skt_cache_[kCleanSKTCache].cache.size()
              );
        printf("#Evicted Key[%lu] SKT[%lu] : Eviction Call[%lu] Dirty Cancelled[%lu] skt busy Canceled[%lu], next skt deleted Canceled[%lu] Prefetch SKT after evict[%lu] \n",
                g_evicted_key.load(),
                g_evicted_skt.load(),
                g_evict_call_cnt.load(),
                g_evict_dirty_canceled.load(),
                g_evict_skt_busy_canceled.load(),
                g_evict_skt_next_deleted_canceled.load(),
                g_prefetch_after_evict.load());
        printf("#sktable loading[%lu] flushing[%lu]\n",
                g_skt_loading.load(),
                g_skt_flushing.load());
        printf("#Device Format SKT status : hit[%lu] miss[%lu] Insert[%lu] evict[%lu]\n",
                g_hit_df_skt.load(),
                g_miss_df_skt.load(),
                g_insert_df_skt.load(),
                g_evict_df_skt.load()
              );
#ifdef KV_TIME_MEASURE
        printf("#EvictUserKey call[%lu] cum time[%llu usec] max time[%llu usec]\n",
                g_evict_count.load(), cycle_to_usec(g_evict_cum_time.load()), cycle_to_usec(g_evict_max_time.load()));
        printf("#SKTFlush star_end call[%lu:%lu] cum time[%llu usec] max time[%llu usec]\n",
                g_flush_start_count.load(), g_flush_end_count.load(), cycle_to_usec(g_flush_cum_time.load()), cycle_to_usec(g_flush_max_time.load()));
        printf("#DevFlushForSKT call[%lu] cum time[%llu usec] max time[%llu usec]\n",
                g_skt_flush_count.load(), cycle_to_usec(g_skt_flush_cum_time.load()), cycle_to_usec(g_skt_flush_max_time.load()));
        printf("#DevFlushForIo call[%lu] cum time[%llu usec] max time[%llu usec]\n",
                g_write_io_flush_count.load(), cycle_to_usec(g_write_io_flush_cum_time.load()), cycle_to_usec(g_write_io_flush_max_time.load()));
#endif
#endif


    }

    Slice KeyBlock::GetValueFromKeyBlock(Manifest* mf, uint8_t index) {
        uint32_t* offset = reinterpret_cast<uint32_t *>(raw_data_);/* The # of keys (2B) */
        uint32_t data_offset = (DecodeFixed32((char *)(offset + index))&KEYBLOCK_OFFSET_MASK);
        uint32_t data_size = (DecodeFixed32((char *)(offset + index + 1))&KEYBLOCK_OFFSET_MASK) - data_offset; /* #keys (2B) + (index + 1) */
        uint32_t value_size = 0;

        char* data = raw_data_ + data_offset;
        Slice parse(data, data_size);
        // parse key/value size
        if (!GetVarint32(&parse, &value_size)) {
            fprintf(stderr, "KeyBlock corruption - value size\n");
            abort();
        }
        /* get key size and value size */
        value_size = value_size >> KEYBLOCK_VALUE_SIZE_SHIFT;
        return Slice(parse.data(), value_size);
    }

    Slice KeyBlock::GetKeyFromKeyBlock(Manifest* mf, uint8_t index) {
        uint32_t* offset = reinterpret_cast<uint32_t *>(raw_data_);/* The # of keys (2B) */
        uint32_t data_offset = (DecodeFixed32((char *)(offset + index)) & KEYBLOCK_OFFSET_MASK);
        uint32_t data_size = (DecodeFixed32((char *)(offset + index + 1))&KEYBLOCK_OFFSET_MASK) - data_offset; /* #keys (2B) + (index + 1) */
        uint32_t value_and_key_size = 0;
        uint32_t value_size = 0;
        uint32_t key_size = 0;

        char* data = raw_data_ + data_offset;
        Slice parse(data, data_size);
        // parse key/value size
        if (!GetVarint32(&parse, &value_and_key_size)) {
            fprintf(stderr, "KeyBlock corruption - value size\n");
            abort();
        }
        /* get key size and value size */
        value_size = value_and_key_size >> KEYBLOCK_VALUE_SIZE_SHIFT;
        key_size = value_and_key_size & KEYBLOCK_KEY_SIZE_MASK;

        return Slice(parse.data() + value_size, key_size);
    }


    bool KeyBlock::DecodeAllKeyMeta(KeyBlockPrimaryMeta *kbpm, std::list<ColumnInfo> &key_list/*, uint64_t &largest_seq */) {
        assert(kbpm->IsInlog());
        uint8_t nr_key = GetKeyCnt();
        uint32_t* offset = reinterpret_cast<uint32_t *>(raw_data_);/* The # of keys (2B) */
        std::vector<ColumnInfo> col_list;
        /* parsing Key Block */
        assert(nr_key <= 64);
        for(uint8_t index=0; index < nr_key; index++) {
            uint32_t data_offset = DecodeFixed32((char *)(offset + index));
            bool ttl = data_offset & KEYBLOCK_TTL_MASK; /* get ttl */
            data_offset &= KEYBLOCK_OFFSET_MASK;
            uint32_t data_size = (DecodeFixed32((char *)(offset + index + 1))&KEYBLOCK_OFFSET_MASK) - data_offset; /* #keys (2B) + (index + 1) */

            ColumnInfo colinfo;
            char* data = raw_data_ + data_offset;
            Slice parse(data, data_size);
            uint32_t key_value_size = 0;
            if (!GetVarint32(&parse, &key_value_size)) {
                printf("[%s:%d] KeyBlock corruption - value size\n", __func__, __LINE__);
                abort();
            }
            uint32_t key_size = key_value_size & KEYBLOCK_KEY_SIZE_MASK;
            uint32_t value_size = key_value_size >> KEYBLOCK_VALUE_SIZE_SHIFT;
            parse.remove_prefix(value_size); /* skip value size */
            colinfo.key = KeySlice(parse.data(), key_size);
            parse.remove_prefix(key_size); /* skip key_size */
            colinfo.type = kPutType;
            colinfo.col_id = parse[0]; 
            assert(colinfo.col_id < kMaxColumnCount);
            parse.remove_prefix(1); /* skip col id */
            if (ttl) colinfo.ttl = DecodeFixed64(const_cast<char*>(parse.data()));
            colinfo.kb_index = index;
            colinfo.kb_size = data_size_;
            colinfo.kbpm = kbpm;
            colinfo.ikey_seq = kbpm->GetiKeySequenceNumber();
            col_list.push_back(colinfo);
        }
        /* Parsing KBPM */
        char* data = kbpm->GetRawData();
        uint32_t data_size = kbpm->GetRawDataSize();
        assert(data);
        assert(*data & KBM_INLOG_FLAG);
        uint8_t org_key_cnt = (*data & ~KBM_INLOG_FLAG);
        data += (KB_BITMAP_LOC + KB_BITMAP_SIZE);
        assert(data_size > KB_BITMAP_LOC + KB_BITMAP_SIZE);
        data_size -= (KB_BITMAP_LOC + KB_BITMAP_SIZE);
        uint32_t kbm_size = DecodeFixed32(data); /* kbm size */
        data += sizeof(uint32_t); /* skip kbm size */
        data += sizeof(uint64_t); /* skip next ikey seq */
        data_size -= (sizeof(uint32_t) + sizeof(uint64_t)); 
        Slice parse(data, data_size);
        for(uint8_t i = 0; i < org_key_cnt; i++)
            if (!GetVarint64(&parse, &col_list[i].iter_seq)) abort(); /* fill put iter sequence */

        uint8_t total_trxn = parse[0];
        parse.remove_prefix(1); /* skeip trxn count */

        for (uint8_t i = 0; i < total_trxn; i++) {
            TrxnInfo* t_info = nullptr;
            uint16_t merged_trxn_cnt = 0;
            uint8_t type_offset_col_id = parse[0];
            parse.remove_prefix(1); /* skip type, colid or index */
            if (type_offset_col_id & TRXN_DEL_REQ) {
                ColumnInfo c_info;
                /* delete type */
                c_info.ikey_seq = kbpm->GetiKeySequenceNumber();
                c_info.trxninfo = t_info;
                c_info.col_id =(type_offset_col_id & ~TRXN_DEL_REQ);
                assert(c_info.col_id < kMaxColumnCount);
                c_info.type = kDelType;
                c_info.kbpm = kbpm;
                uint16_t key_size = 0;
                if (!GetVarint16(&parse, &key_size)) abort();
                c_info.key = KeySlice(parse.data(), key_size);
                parse.remove_prefix(key_size);
                if (!GetVarint64(&parse, &c_info.iter_seq)) abort(); /* fill del iter sequence */
                if (!GetVarint16(&parse, &merged_trxn_cnt)) abort();
                if (merged_trxn_cnt) {
                    t_info = new TrxnInfo();
                    c_info.trxninfo = t_info;
                    t_info->col_id = c_info.col_id;
                    t_info->key = c_info.key;
                }
                key_list.push_back(c_info); /* insert del type to key_list*/ 
            } else {
                /* put type */
                assert(type_offset_col_id < col_list.size());
                assert(col_list[type_offset_col_id].kb_index == type_offset_col_id);
                if (!GetVarint16(&parse, &merged_trxn_cnt)) abort();
                assert(merged_trxn_cnt > 0);
                t_info = new TrxnInfo();
                col_list[type_offset_col_id].trxninfo = t_info;
                t_info->col_id = col_list[type_offset_col_id].col_id;
                t_info->keyoffset = col_list[type_offset_col_id].kb_index;
                t_info->key =  col_list[type_offset_col_id].key;
            }
            if (merged_trxn_cnt > 0) {
                t_info->nr_merged_trxn = merged_trxn_cnt - 1;
                if (merged_trxn_cnt > 1) t_info->merged_trxn_list = new Transaction[merged_trxn_cnt - 1];
                for (uint16_t j = 0; j < merged_trxn_cnt -1; j++) {  /* fill non head trxn */
                    uint64_t trxn_id = 0;
                    if (!GetVarint64(&parse, &trxn_id)) abort();
                    uint32_t trxn_seq = 0;
                    if (!GetVarint32(&parse, &trxn_seq)) abort();
                    uint32_t trxn_count = 0;
                    if (!GetVarint32(&parse, &trxn_count)) abort();
                    t_info->merged_trxn_list[i].id = trxn_id;
                    t_info->merged_trxn_list[i].count = trxn_count;
                    t_info->merged_trxn_list[i].seq_number = trxn_seq;
                }
                /* fill head */
                uint64_t trxn_id = 0;
                if (!GetVarint64(&parse, &trxn_id)) abort();
                uint32_t trxn_seq = 0;
                if (!GetVarint32(&parse, &trxn_seq)) abort();
                uint32_t trxn_count = 0;
                t_info->head.id = trxn_id;
                t_info->head.count = trxn_count;
                t_info->head.seq_number = trxn_seq;
            }
        }
        uint32_t col_info_size = col_list.size();
        for (uint32_t k = 0; k < col_info_size; k++) key_list.push_back(col_list[k]); /* insert put type to key_list */
        return true;
    }

    bool KeyBlockPrimaryMeta::DecodeAllDeleteKeyMeta(std::list<ColumnInfo> &key_list/*, uint64_t &largest_seq */) {
        assert(IsInlog());

        /* Parsing KBPM */
        char* data = GetRawData();
        uint32_t data_size = GetRawDataSize();
        assert(data);
        assert(*data & KBM_INLOG_FLAG);
        uint8_t org_key_cnt = (*data & ~KBM_INLOG_FLAG);
        data += (KB_BITMAP_LOC + KB_BITMAP_SIZE);
        assert(data_size > KB_BITMAP_LOC + KB_BITMAP_SIZE);
        data_size -= (KB_BITMAP_LOC + KB_BITMAP_SIZE);
        uint32_t kbm_size = DecodeFixed32(data); /* kbm size */
        data += sizeof(uint32_t); /* skip kbm size */
        data += sizeof(uint64_t); /* skip next ikey seq */
        data_size -= (sizeof(uint32_t) + sizeof(uint64_t)); 
        Slice parse(data, data_size);
        assert(org_key_cnt == 0);
        uint8_t total_trxn = parse[0];
        parse.remove_prefix(1); /* skeip trxn count */
        for (uint8_t i = 0; i < total_trxn; i++) {
            ColumnInfo c_info;
            uint8_t type_offset_col_id = parse[0];
            parse.remove_prefix(1); /* skip type, colid or index */
            assert(type_offset_col_id & TRXN_DEL_REQ);
            /* delete type */
            c_info.ikey_seq = GetiKeySequenceNumber();
            c_info.col_id =(type_offset_col_id & ~TRXN_DEL_REQ);
            c_info.type = kDelType;
            c_info.kbpm = this;
            uint16_t key_size = 0;
            if (!GetVarint16(&parse, &key_size)) abort();
            c_info.key =  KeySlice(parse.data(), key_size);
            parse.remove_prefix(key_size);
            if (!GetVarint64(&parse, &c_info.iter_seq)) abort(); /* fill del iter sequence */
            uint16_t merged_trxn_cnt = 0;
            if (!GetVarint16(&parse, &merged_trxn_cnt)) abort();
            if (merged_trxn_cnt) {
                TrxnInfo* t_info = new TrxnInfo();
                c_info.trxninfo = t_info;
                t_info->col_id = c_info.col_id;
                t_info->key = c_info.key;
                t_info->nr_merged_trxn = merged_trxn_cnt - 1;
                if (merged_trxn_cnt > 1) t_info->merged_trxn_list = new Transaction[merged_trxn_cnt - 1];
                for (uint16_t j = 0; j < merged_trxn_cnt -1; j++) {  /* fill non head trxn */
                    uint64_t trxn_id = 0;
                    if (!GetVarint64(&parse, &trxn_id)) abort();
                    uint32_t trxn_seq = 0;
                    if (!GetVarint32(&parse, &trxn_seq)) abort();
                    uint32_t trxn_count = 0;
                    if (!GetVarint32(&parse, &trxn_count)) abort();
                    t_info->merged_trxn_list[i].id = trxn_id;
                    t_info->merged_trxn_list[i].count = trxn_count;
                    t_info->merged_trxn_list[i].seq_number = trxn_seq;
                }
                /* fill head */
                uint64_t trxn_id = 0;
                if (!GetVarint64(&parse, &trxn_id)) abort();
                uint32_t trxn_seq = 0;
                if (!GetVarint32(&parse, &trxn_seq)) abort();
                uint32_t trxn_count = 0;
                t_info->head.id = trxn_id;
                t_info->head.count = trxn_count;
                t_info->head.seq_number = trxn_seq;
            }
            key_list.push_back(c_info); /* insert del type to key_list*/ 
        }
        return true;
    }




    void KeyBlockPrimaryMeta::InitWithDeviceFormat(Manifest  *mf, const char *buffer, uint32_t buffer_size) {

        assert(IsDummy());
        org_key_cnt_ = *buffer;
        /* The KBM must be out-log*/
        assert(!(org_key_cnt_ & KBM_INLOG_FLAG));
        buffer+=KB_BITMAP_LOC;
        // bitmap
        OverrideKeyBlockBitmap(DecodeFixed64(buffer));
        dev_data_ = NULL; 
        dev_data_size_ = 0; 
        ClearDummy();
        ClearKBMPrefetched();
        return;

    }

    void KeyBlockPrimaryMeta::InitWithDeviceFormatForInLog(Manifest  *mf, char *buffer, uint32_t buffer_size) {

        assert(IsDummy());
        assert(*buffer & KBM_INLOG_FLAG);
        org_key_cnt_ = (*buffer & ~KBM_INLOG_FLAG);
        /* The KBM must be in-log*/
        // bitmap
        OverrideKeyBlockBitmap(DecodeFixed64(buffer + KB_BITMAP_LOC));
        dev_data_ = buffer;
        uint32_t kbm_size = *((uint32_t *)(buffer + kOutLogKBMSize));
        assert(kbm_size <= buffer_size);
        dev_data_size_ = kbm_size;
        ClearDummy();
        ClearKBMPrefetched();
        SetInLog();
        return;

    }


    Slice KeyBlockPrimaryMeta::BuildDeviceFormatKeyBlockMeta(char* kbm_buffer){

        Slice ret;
        if(kbm_buffer){
            /* Build Out-Log KBM*/
            assert(!dev_data_ && !dev_data_size_);
            dev_data_ = kbm_buffer;
            *dev_data_ = org_key_cnt_;
            /*Skip 3 byte which is reserved for GC info(maybe?)*/
            EncodeFixed64((dev_data_+KB_BITMAP_LOC), kb_bitmap_.load(std::memory_order_relaxed));
            dev_data_size_ = kOutLogKBMSize ;//org_key_cnt_ + 3 byte(reserved) + kb_bitmap_
            assert(dev_data_);
            assert(dev_data_size_ == 12);
            ret = Slice(dev_data_, SizeAlignment(dev_data_size_));
            dev_data_ = NULL;
            dev_data_size_ = 0;
        }else{
            /* Build In-Log KBM*/
            /* Most of information has been added to the buffer*/
            *((uint32_t*)(dev_data_ + kOutLogKBMSize)) = dev_data_size_;
            assert(dev_data_);
            assert(dev_data_size_ >= 12);
            ret = Slice(dev_data_, SizeAlignment(dev_data_size_));
            /* Store next ikey to dev_data */
            dev_data_ = (char*)(*((uint64_t*)(dev_data_+ NEXT_IKEY_SEQ_LOC)));
            dev_data_size_ = USE_DEV_DATA_FOR_NEXT_IKEY_SEQ;

        }

        return ret;
    }

} //namespace
