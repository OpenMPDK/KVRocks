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
    uint64_t kFreeBufferSize = 256*1024*1024UL;

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
    uint64_t kMaxCacheSize = 2*1024*1024*1024UL;
    uint64_t kCacheSizeLowWatermark = 0;/*Set it to 80% of kMaxKeySize in Manifest*/
    // Max threshold size to flush
    // the accumulated size of requested user value exceeds this value, trigger SKTable flush. 
    uint32_t kMaxFlushThreshold= 1024*1024*128;
    uint32_t kForceFlushThreshold= 1024*1024*1024;
    //Minimun update count for triggering SKTableMem Flush
    uint32_t  kSlowdownTrigger = 64*1024;
    uint32_t  kSlowdownLowWaterMark = 4*1024;
    //uint32_t kMinUpdateCntForTableFlush= 32*1024;
    //uint32_t kUpdateCntForForceTableFlush= 64*1024;
    uint32_t  kMinUpdateCntForTableFlush = (kMaxSKTableSize*17UL)/512;
    uint32_t  kUpdateCntForForceTableFlush= kMinUpdateCntForTableFlush << 1;
    /* the kRequestAlignSize must be power of two*/
#ifdef NEW_GENERIC_FIRMWARE
    int kRequestAlignSize = 4; /**  */
#else
    int kRequestAlignSize = 4096; /**  */
#endif

    // KeyPrefixSize
    int kPrefixIdentifierSize = 0;

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
        Env *env_ = recovery_->GetEnv();
        Manifest *mf_ = recovery_->GetManifest();
        std::list<TrxnGroupRange> &completed_trxn_groups_ = recovery_->GetCompletedTrxnGroup();

        // get the beginning ikey sequence of the worker
        // note that the beginning ikey must be the beginning of transaction group.
        SequenceNumber ikey_seq = recovery_->GetWorkerNextIKeySeq(worker_id_);

        while(true) {
            // Read key block meta
            std::string buffer;
            buffer.reserve(kDeviceRquestMaxSize);
            int size = kDeviceRquestMaxSize;
            Status s = env_->Get(GetKBKey(mf_->GetDBHash(), kKBMeta, ikey_seq), const_cast<char *>(buffer.data()), &size);
            if(s.IsNotFound())
                break;
            else if(!s.ok()) {
                printf("recovery(%u): cannot read key block meta. %s\n", worker_id_, s.ToString().data());
                status_ = s;
                return;
            }
            buffer.resize(size);

            // Decode key block meta device format
            KeyBlockPrimaryMeta *kbpm = mf_->AllocKBPM();
            KeyBlockMetaLog *kbml;
            kbpm->Init(ikey_seq);
            bool success = kbpm->InitWithDeviceFormat(mf_, buffer.data(), buffer.size(), kbml, false);
            if(!success) {
                printf("recovery(%u): cannot decode key block meta\n", worker_id_);
                status_ = s;
                return;
            }
            mf_->InsertKBPMToHashMap(kbpm);
            // Worker->KBDLQueue.insert(KBDL);

            // Read key block from the device
            std::list<ColumnInfo> orphan_col_list;
            if(!kbpm->IsDeleteOnly()) {
                auto kb = new KeyBlock(mf_);

                kb->Init(kMaxInternalValueSize);
                size = kMaxInternalValueSize;
                s = env_->Get(GetKBKey(mf_->GetDBHash(), kKBlock, ikey_seq), const_cast<char *>(buffer.data()), &size);
                if(!s.ok()) {
                    printf("recovery(%u): cannot read key block. %s\n", worker_id_, s.ToString().data());
                    return;
                }
                kb->SetSize(size);

                // Decode key block device format
                // userkey_list receives user keys including valid and deleted user keys
                kb->DecodeAllKeyMeta(kbml, orphan_col_list);
                if(!success) {
                    printf("recovery(%u): cannot decode key block\n", worker_id_);
                    status_ = Status::Corruption("cannot decode key block");
                    return;
                }

                printf("Free KB %p in recovery\n", kb);
                delete kb;
            }

            // Recover keys in completed transaction groups
            for(auto colinfo : orphan_col_list){
                TrxnInfo *trxninfo = colinfo.trxninfo;
                TrxnGroupID tgid = trxninfo->GetTrxnGroupID();
                assert(trxninfo->GetColId() < kMaxColumnCount);

                // If in completed transaction group, add the user key to keymap
                if(mf_->IsCompletedTrxnGroupRange(completed_trxn_groups_, tgid)) {
                    recovery_->InsertRecoveredKeyToKeymap(colinfo);
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
            ikey_seq = kbml->GetNextIKeySeqNum();
        } //while(true)
    }

    DBRecovery::DBRecovery(Manifest *mf) : mf_(mf), env_(mf_->GetEnv()), logkey_(mf->GetDBHash()) { }
    Status DBRecovery::LoadLogRecordKey() { return logkey_.Load(mf_->GetEnv()); }

    Status DBRecovery::CleanupSktUpdateList(SKTableMem *skt, InSDBMetaType updatelist_type) {

        // Load update list
        char updatelist_buf[kDeviceRquestMaxSize];
        std::string contents;
        uint32_t updatelist_size = kDeviceRquestMaxSize;
        std::list<std::tuple<uint64_t, uint8_t/*offset*/, RequestType>> delete_list;
        std::list<std::tuple<uint64_t, uint8_t/*offset*/, RequestType>> update_list;

        InSDBKey key = GetMetaKey(mf_->GetDBHash(), updatelist_type, skt->GetSKTableID());
        Status s = Env::Default()->Get(key, updatelist_buf, (int*)&updatelist_size);
        if(s.IsNotFound())
            return Status::OK();
        else if(!s.ok())
            return s;
        contents.resize(0);
        contents.append(updatelist_buf, updatelist_size);

        uint32_t data_size = DecodeFixed32(updatelist_buf);
        uint16_t num_req = (data_size + kDeviceRquestMaxSize - 1)/ kDeviceRquestMaxSize;
        if (data_size > kDeviceRquestMaxSize) {
            uint32_t remain_size = data_size - updatelist_size;
            for (uint16_t i = 1; i < num_req; i++) {
                updatelist_size = (remain_size < kDeviceRquestMaxSize) ? remain_size : kDeviceRquestMaxSize;
                key.split_seq = i;
                Status s = Env::Default()->Get(key, updatelist_buf, (int*)&updatelist_size);
                if (!s.ok()) return s;
                contents.append(updatelist_buf, updatelist_size);
            }
        }
        // decode update list
        // update lists contain user keys or key blocks in completed transaction groups
        bool success = skt->DecodeUpdateList(delete_list, update_list, const_cast<char*>(contents.data()), data_size);
        if(!success)
            return Status::Corruption("cannot decode update list");

        // delete KB/KBM
        for(auto it : delete_list) {
            uint64_t ikey_seq = std::get<0>(it);
            s = Env::Default()->Delete(GetKBKey(mf_->GetDBHash(), kKBlock, ikey_seq));
            if(!s.IsNotFound() && !s.ok())
                return s;
            s = Env::Default()->Delete(GetKBKey(mf_->GetDBHash(), kKBMeta, ikey_seq));
            if(!s.IsNotFound() && !s.ok())
                return s;
        }


        // update KBM
        for(auto it : update_list) {
            uint64_t ikey_seq = std::get<0>(it);
            uint8_t offset = std::get<1>(it);
            RequestType optype = std::get<2>(it);

            // Load key block meta
            updatelist_size = kDeviceRquestMaxSize;
            s = Env::Default()->Get(GetKBKey(mf_->GetDBHash(), kKBMeta, ikey_seq), updatelist_buf, (int*)&updatelist_size);
            if(!s.ok())
                return s;
            // decode
            KeyBlockPrimaryMeta* kbpm = mf_->AllocKBPM();
            KeyBlockMetaLog* kbml = nullptr;
            kbpm->Init(ikey_seq);
            bool success = kbpm->InitWithDeviceFormat(mf_, updatelist_buf, updatelist_size, kbml, false);
            if(!success)
                return Status::Corruption("cannot decode KBM");

            // Update the key block meta
            kbml->ClearKeyBlockBitmap(offset, optype);

            // Build KBM
            char new_updatelist[kDeviceRquestMaxSize];
            uint32_t new_updatelist_size = kbml->BuildDeviceFormatKeyBlockMeta(new_updatelist);

            // Write key block meta
            Slice new_updatelist_buf(new_updatelist, new_updatelist_size);
            s = Env::Default()->Put(GetKBKey(mf_->GetDBHash(), kKBMeta, ikey_seq), new_updatelist_buf);
            if(!s.ok())
                return s;
        }


        // delete sktable update list key
        for (uint16_t i = 0; i < num_req; i++) {
            key.split_seq = i;
            s = Env::Default()->Delete(key, false);
        }
        if(!s.ok())
            return s;

        return Status::OK();
    }

    Status DBRecovery::CleanupSktUpdateList(SKTableMem *skt) {

        Status s = CleanupSktUpdateList(skt, kSKTUpdateList);
        if(!s.ok())
            return s;

        s = CleanupSktUpdateList(skt, kSKTIterUpdateList);
        if(!s.ok())
            return s;

        return Status::OK();
    }

    /**

*/
    Status DBRecovery::LoadBeginsKeysAndCLeanupSKTableMeta(uint32_t &sktid)
    {
        int size = 2048;
        char buffer[size];

        //
        // read partial sktable to insert begin key
        //
        Status s = env_->Get(GetMetaKey(mf_->GetDBHash(), kSKTable, sktid), buffer, &size);
        if(!s.ok())
            return s;
        if(size < 48)
            return Status::Corruption("sktable is invalid");

        //
        // parse necessary information
        //

#if 0
        // get crc2
        uint32_t crc2 = DecodeFixed32(buffer + SKTDF_CRC_2_OFFSET);
#endif
        // compress start offset
        uint32_t compress_offset = DecodeFixed32(buffer + SKTDF_COMP_START_OFFSET);
        if(compress_offset > 2048)
            return Status::NotSupported("first key is too big");
        uint32_t sktdf_size = compress_offset + DecodeFixed32(buffer + SKTDF_SKT_SIZE_OFFSET);

        // next skt
        uint32_t next_skt = DecodeFixed32(buffer + SKTDF_NEXT_ID_OFFSET);
        // prealloc skt
        uint32_t prealloc_skt = DecodeFixed32(buffer + SKTDF_PREALLOC_ID_OFFSET);

        //
        // load begin key name
        //

        const char *beginkey = buffer+SKTDF_BEGIN_KEY_OFFSET;

        // shared key name length
        uint16_t key_len;
        beginkey = GetVarint16Ptr(beginkey, buffer+size, &key_len);
        if(key_len)
            return Status::Corruption("begin key shared a portion");

        // non-shared key name length
        beginkey = GetVarint16Ptr(beginkey, buffer+size, &key_len);

        // create its sktable
        KeySlice key(beginkey, (uint32_t)key_len);

        /*TODO need to fix */
        Slice col_slice(0);
        SKTableMem *skt = new SKTableMem(mf_, key, sktid, sktdf_size);
#ifdef INSDB_GLOBAL_STATS
        g_new_skt_cnt++;
#endif
        // clean up sktable update list
        s = CleanupSktUpdateList(skt);
        if(!s.ok())
            return s;

        // try to read prealloc sktable.
        // if not found is returned, sktable is clean.
        SKTableInfo skt_info;
        s = mf_->ReadSKTableInfo(prealloc_skt, &skt_info);
        if(s.IsNotFound())
            mf_->InsertSKTableMemCache(skt, kCleanSKTCache);
        else if(s.ok()) {
            // Need to fix the sktable
            // Fully load
            // No eviction threads run. so we don't need to increase ref cnt
            skt->LoadSKTable(mf_);
            //return Status::IOError("sktable loading failed");
            // delete the preallocated sktable
            s = mf_->CleanupPreallocSKTable(prealloc_skt, next_skt);
            if(!s.ok() && !s.IsNotFound())
                return s;

            // remove the link to prealloc sktable
            skt->SetPreallocSKTableID(next_skt);
            mf_->InsertSKTableMemCache(skt, kDirtySKTCache);
        }

        // set next sktable ID
        sktid = next_skt;
        return Status::OK();
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

    void DBRecovery::InsertRecoveredKeyToKeymap(ColumnInfo &colinfo) {
        Slice key(colinfo.key);
        UserKey *existing_ukey = mf_->SearchUserKey(key);
        if(existing_ukey) {
            // if it exists, add only column node
            // if existing column is older than the orphan column, insert the column to the user key
            // otherwise, it's a bug because it should have been deleted steps above.
            auto existing_newest_node = existing_ukey->GetColumnList(colinfo.trxninfo->GetColId())->newest();
            if(!existing_newest_node || ColumnNode::GetColumnFromLink(existing_newest_node)->GetBeginSequenceNumber() < colinfo.ikey_seq) {
                auto col = new ColumnNode(colinfo.type, colinfo.ikey_seq, colinfo.ttl, colinfo.kb_size, colinfo.kb_index);
                existing_ukey->InsertColumnNode(col, colinfo.trxninfo->GetColId());
            } else {
                port::Crash(__FILE__,__LINE__);
            }
        } else {
            auto col = new ColumnNode(colinfo.type, colinfo.ikey_seq, colinfo.ttl, colinfo.kb_size, colinfo.kb_index);
            KeySlice keyslice(key);
            auto ukey = mf_->AllocUserKey(key.size());
            ukey->InitUserKey(keyslice);
            ukey->InsertColumnNode(col, colinfo.trxninfo->GetColId());

            bool success = mf_->InsertUserKey(ukey);
            if(!success) {
                printf("recovery: cannot insert user key\n");
                port::Crash(__FILE__,__LINE__);
            }
        }
    }

    /**
     *
     */
    bool DBRecovery::UpdateAllKBMsInOrder()
    {
        return false;
    }

    /**
    */
    Status DBRecovery::RunRecovery() {
        Status s;
        SuperSKTable super_skt;
        uint32_t skt_id = 0;

        // decode the log record
        // note that it's already loaded when checking log record existence.
        bool success = logkey_.DecodeBuffer(deleted_ikey_seqs_, workers_next_ikey_seqs_, completed_trxn_groups_);
        if(!success) {
            printf("cannot decode the log record\n");
            return Status::Corruption("could not decode log record");
        }

        // clean up global log record
        // delete key blocks from the device obtained from delete pending key list
        std::vector<InSDBKey> delete_list;
        uint64_t key_block_seq;
        while((key_block_seq = deleted_ikey_seqs_.front())) {
            deleted_ikey_seqs_.pop_front();
            delete_list.push_back(GetKBKey(mf_->GetDBHash(), kKBMeta, key_block_seq));
            delete_list.push_back(GetKBKey(mf_->GetDBHash(), kKBlock, key_block_seq));
        }
        InSDBKey null_insdbkey = { 0 };
        std::vector<Env::DevStatus> status;
        env_->MultiDel(delete_list, null_insdbkey, false, status);

        // prefetch sktable
        // TODO

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
        while(skt_id != 0) {
            s = LoadBeginsKeysAndCLeanupSKTableMeta(skt_id);
            if (!s.ok()) {
                return s;
            }
        }

        //
        // Redo logs and delete incomplete transactions
        //
        std::unordered_map<TransactionID, TransactionColCount> UncompletedTRXNList;
        std::list<ColumnInfo> UncompletedKeyList;

        // start worker log recovery threads
        DBRecoveryThread *recovery_threads[kWriteWorkerCount];
        for(int worker_idx = 0; worker_idx < kWriteWorkerCount; worker_idx++) {
            recovery_threads[worker_idx] = new DBRecoveryThread(this, worker_idx);
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
            }
            else
            {
                thread_status = recovery_threads[worker_idx]->GetStatus();
            }
            delete recovery_threads[worker_idx];
        }

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
                colinfo->kbml->ClearKeyBlockBitmap(colinfo->kb_index, colinfo->type);

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
            InsertRecoveredKeyToKeymap(colinfo);

        // Flush all key block meta updates
        //Do not delete KBM even if it has no valid bit in the bitmap
        // from each workers begin log to last
        success = UpdateAllKBMsInOrder();
        if(!success)
            return Status::IOError("could not update KBMs");

        return Status::OK();
    }

    void IterScratchPad::ClearIterScratchpad(Manifest *mf){
        assert(!ref_cnt_.load(std::memory_order_relaxed));
        assert(allocator_);
#ifdef CODE_TRACE
        printf("clear iter for skt id %d\n", skt_->GetSKTableID());
#endif
        skt_ = NULL; 
        next_skt_ = NULL; 
        begin_seq_ = 0;
        last_seq_ = 0;
        if(!use_org_sktdf_ && sktdf_){
            sktdf_->CleanUp(mf);
            delete sktdf_;
        }
        sktdf_ = NULL;
        key_entry_count_ = 0;

        std::queue<UserValue*> *uv_queue = &uv_queue_;
        while(!uv_queue->empty()){
            UserValue* uv = uv_queue->front();
            uv_queue->pop();
            mf->FreeUserValue(uv);
        }
        std::queue<KeyBlockPrimaryMeta*> *kbpm_queue = &kbpm_queue_;
        while(!kbpm_queue->empty()){
            KeyBlockPrimaryMeta* kbpm = kbpm_queue->front();
            kbpm_queue->pop();
            if (kbpm->DecRefCntAndTryEviction(mf)) {
                mf->FreeKBPM(kbpm);
            }
        }
    }

    void KeyMap::CleanUp(){
        UserKey* uk = First();
        uint32_t cnt = 0;
        while(uk) {
            cnt++;
            if(Remove(uk->GetKeySlice())){
                uk->UserKeyCleanup(mf_, true);
                mf_->FreeUserKey(uk);
                uk = First();
            }
            else{
                printf("[%d :: %s] Fail to remove a key from keymap_\n", __LINE__, __func__);
                abort();
            }
            /* if (uk) delete uk; delete will be done by GC */
        }
    }


    ObjectAllocator<IterScratchPad>* IterScratchPadAllocatorPool::GetIterScratchPadAllocator(){

        ObjectAllocator<IterScratchPad>* sp_allocator = reinterpret_cast<ObjectAllocator<IterScratchPad>*>(pthread_getspecific(pool_id_));
        if(!sp_allocator){
            uint16_t id = ++id_;
            if(id == kMaxAllocatorPool) id_ = 0;
            sp_allocator = mf_->GetIterScratchPadAllocator(id - 1);
            pthread_setspecific(pool_id_, sp_allocator);
        }
        return sp_allocator;
    }


    ObjectAllocator<UserValue>* UVAllocatorPool::GetUVAllocator(){

        ObjectAllocator<UserValue>* uv_allocator = reinterpret_cast<ObjectAllocator<UserValue>*>(pthread_getspecific(pool_id_));
        if(!uv_allocator){
            uint16_t id = ++id_;
            if(id == kMaxAllocatorPool) id_ = 0;
            uv_allocator = mf_->GetUVAllocator(id - 1);
            pthread_setspecific(pool_id_, uv_allocator);
        }
        return uv_allocator;
    }
    ObjectAllocator<UserKey>* UKAllocatorPool::GetUKAllocator(){

        ObjectAllocator<UserKey>* uk_allocator = reinterpret_cast<ObjectAllocator<UserKey>*>(pthread_getspecific(pool_id_));
        if(!uk_allocator){
            uint16_t id = ++id_;
            if(id == kMaxAllocatorPool) id_ = 0;
            uk_allocator = mf_->GetUKAllocator(id - 1);
            pthread_setspecific(pool_id_, uk_allocator);
        }
        return uk_allocator;
    }

    ObjectAllocator<RequestNode>* RequestNodeAllocatorPool::GetRequestNodeAllocator(){

        ObjectAllocator<RequestNode>* rn_allocator = reinterpret_cast<ObjectAllocator<RequestNode>*>(pthread_getspecific(pool_id_));
        if(!rn_allocator){
            uint16_t id = ++id_;
            if(id == kMaxAllocatorPool) id_ = 0;
            rn_allocator = mf_->GetRequestNodeAllocator(id - 1);
            pthread_setspecific(pool_id_, rn_allocator);
        }
        return rn_allocator;
    }


    Manifest::Manifest(const Options& option, std::string& dbname)
        : env_(option.env), options_(option), dbname_(dbname),
        worker_mu_(),
        worker_cv_(&worker_mu_),
        worker_cnt_(0), 
        worker_terminated_(false),
        worker_termination_mu_(),
        worker_termination_cv_(&worker_termination_mu_),
        kbm_update_list_(),
        nr_worker_in_running_(0),
        /* "0 ~ kUnusedNextInternalKey(kMaxUserCount + kMaxWriteWorkerCount)" is used for ColumnNodes being submitted*/
        next_InSDBKey_seq_num_(kUnusedNextInternalKey),
        sequence_number_(1),/* Must be stored in ~Manifest and reuse the number */
        next_trxn_id_(1 << TGID_SHIFT), // Trxn ID 0 is researved for non-TRXN request. and only this has 0 TGID.
        next_sktable_id_(0),
        memory_usage_(0),
        nr_iterator_(0),
        ukey_comparator_(option.comparator), gcinfo_(16), sktlist_(ukey_comparator_, &gcinfo_),/* keymap_(option.comparator, &gcinfo_, this),*/
#ifdef INSDB_USE_HASHMAP
        key_hashmap_(new KeyHashMap(1024*1024)),
        kbpm_hashmap_(new KBPMHashMap(1024*1024)), 
#endif
        skt_sorted_simplelist_lock_(),
        snapshot_simplelist_lock_(),
        skt_sorted_simplelist_(),
        snapshot_simplelist_(),

        coldness_rnd_(0xdeadbeef),
        skt_cache_lock_(),
        skt_cache_{skt_cache_lock_, skt_cache_lock_},
        kb_pool_(NULL),
        kbpm_pool_(NULL),
        kbml_pool_(NULL),

        key_cache_mu_(),
        clean_skt_cleanup_(false),
        key_cache_cv_(&key_cache_mu_),

        key_cache_shutdown_sem_(),
        flags_(0),
        key_cache_shutdown_(false),

        logcleanup_shutdown_(false),
        logcleanup_mu_(),
        logcleanup_cv_(&logcleanup_mu_),
        logcleanup_tid_(0),
        cleanable_kbm_cnt_(0),

        dio_buffer_(),
        /* Transaction Management */
        uctrxn_group_lock_(),
        uncompleted_trxn_group_(),
        ctrxn_group_lock_(),
        completed_trxn_group_list_(),

        max_iter_key_buffer_size_(kMinSKTableSize),
        max_iter_key_offset_buffer_size_(kMinSKTableSize),

        sp_allocator_(NULL),
        uv_allocator_(NULL),
        uk_allocator_(NULL),
        request_node_allocator_(NULL),
        sp_allocator_pool_(this),
        uv_allocator_pool_(this),
        uk_allocator_pool_(this),
        request_node_allocator_pool_(this),
        //free_request_node_spinlock_(NULL),
        //free_request_node_(NULL),

        logcleanup_data_(NULL),
        free_skt_buffer_spinlock_(),
        free_skt_buffer_(),
        thread_memory_pool_(),
        normal_start_(false)
        {
            if (option.max_request_size) {
                kMaxRequestSize = option.max_request_size;
            }
            if (option.num_column_count) kMaxColumnCount = option.num_column_count;
            if (option.num_write_worker) kWriteWorkerCount = option.num_write_worker;
            if (option.max_key_size) kMaxKeySize = option.max_key_size;
            if (option.max_table_size) kMaxSKTableSize = option.max_table_size;
            if (option.max_cache_size) kMaxCacheSize = option.max_cache_size;
            if (option.flush_threshold) kMaxFlushThreshold = option.flush_threshold;
#if 1 
            kMinUpdateCntForTableFlush = (kMaxSKTableSize*17UL)/512;
#else
            if (option.min_update_cnt_table_flush) kMinUpdateCntForTableFlush= option.min_update_cnt_table_flush;
#endif
            if (option.slowdown_trigger) kSlowdownTrigger = option.slowdown_trigger;
            if (option.align_size) kRequestAlignSize = option.align_size;
            if (option.split_prefix_bits) kPrefixIdentifierSize = option.split_prefix_bits/8;
            kSlowdownLowWaterMark = kSlowdownTrigger - (kSlowdownTrigger/2);
            kForceFlushThreshold = kMaxFlushThreshold << 1;
            kUpdateCntForForceTableFlush = kMinUpdateCntForTableFlush << 1;

            kCacheSizeLowWatermark = (kMaxCacheSize-(kMaxCacheSize/ 8u));

            TtlList_ = new uint64_t[kMaxColumnCount];
            memset(TtlList_, 0, kMaxColumnCount*8);

            TtlEnabledList_ = new bool[kMaxColumnCount];
            memset(TtlEnabledList_, 0, kMaxColumnCount);

            CreateMemoryPool();
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

            sp_allocator_ = new ObjectAllocator<IterScratchPad>[kMaxAllocatorPool];  
            uv_allocator_ = new ObjectAllocator<UserValue>[kMaxAllocatorPool];  
            uk_allocator_ = new ObjectAllocator<UserKey>[kMaxAllocatorPool];  
            request_node_allocator_ = new ObjectAllocator<RequestNode>[kMaxAllocatorPool];  

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
            dbhash_ = Hash(dbname_.c_str(), dbname_.size(), MF_HASH_SEED);

            /////////////////////****ADD FOR PACKING****////////////////////
            //Support 128 Billions(1 << (32+5)) TRXN per DB Open(Intialized to 1 << TGID_SHIFT )
            completed_trxn_group_list_.push_front(TrxnGroupRange(0)); 
            completed_trxn_group_list_.push_back(TrxnGroupRange(0xFFFFFFFF)); 
            ////////////////////////////////////////////////////////////////

            // Start SKTable cache thread
            sem_init(&key_cache_shutdown_sem_, 0, 0);
            env_->StartThread(&KeyCacheThread_, this);
            logcleanup_tid_ = env_->StartThread(&LogCleanupThreadFn_, this);
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
            if(skt->GetSKTableDeviceFormatNode()){
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

        nr_node = 0;

#if 0
        SKTCacheNode *cache_node = NULL;
        while(cache_node = lockfree_skt_cache_->PopNode()){
            assert(!cache_node->GetSKTableMemNode());
            delete cache_node;
        }

        SKTDFCacheNode *df_node = NULL;
        while(df_node = lockfree_skt_deviceformat_->PopNode()){
            delete df_node;
        }            
#endif

        //printf("[%d :: %s]%u nodes are deleted from lockfree_iter_buffer_\n", __LINE__, __func__, nr_node);


        if(nr_node || nr_stale_node)
            printf("[%d :: %s]%u nodes are deleted from lockfree_prefetch_buffer_ %u failed\n", __LINE__, __func__, nr_node, nr_stale_node);
#if 0
        delete lockfree_skt_cache_;
        delete lockfree_skt_dirty_;
        delete lockfree_skt_deviceformat_;
#endif
        delete [] sp_allocator_;
        delete [] uv_allocator_;
        delete [] uk_allocator_;
        delete [] request_node_allocator_;
        delete [] logcleanup_data_;
        // CleanupFreeRequestNode();
        CleanupFreeSKTableBuffer();
        //delete [] free_request_node_;
        //delete [] free_request_node_spinlock_;
        DestroyMemoryPool();
        Status s = Env::Default()->Flush(kWriteFlush);
        if(!s.ok()){
            printf("[%d :: %s]Flush Fail \n", __LINE__, __func__);
            abort();
        }

#ifdef CODE_TRACE
        printf("[%d :: %s]new alloc(%ld) pop alloc(%ld)\n", __LINE__, __func__,new_alloc, pop_alloc);
#endif

        if(!key_hashmap_->empty())
            abort();
        delete key_hashmap_;
#ifdef CODE_TRACE
        uint32_t total_kbpm_in_hash = 0;
        uint32_t total_kbpm_has_ref = 0;
        uint32_t total_kbpm_has_ref_bit = 0;
        uint32_t total_kbpm_has_prefetch = 0;
        uint32_t total_kbpm_has_none = 0;
        for (auto it = kbpm_hashmap_->cbegin(); it != kbpm_hashmap_->cend();) {
            KeyBlockPrimaryMeta *kbpm = it->second;
            if (kbpm) {
                KeyBlock* kb = kbpm->GetKeyBlockAddr();
                if (kb) delete kb;
                total_kbpm_in_hash++;
                if (kbpm->GetRefCnt()) total_kbpm_has_ref++;
                if (kbpm->CheckRefBitmap()) total_kbpm_has_ref_bit++;
                if (kbpm->IsKBPrefetched()) total_kbpm_has_prefetch++;
                if (!kbpm->GetRefCnt() && !kbpm->CheckRefBitmap()) total_kbpm_has_none++;
                delete kbpm;
                it = kbpm_hashmap_->erase(it);
            } else {
                ++it;
            }
        }
        if (total_kbpm_in_hash)
            printf("total kbpm in hash (%d): ref (%d) bit(%d) prefetch(%d) none(%d) \n", total_kbpm_in_hash, total_kbpm_has_ref, total_kbpm_has_ref_bit, total_kbpm_has_prefetch, total_kbpm_has_none);
#endif
        kbpm_hashmap_->clear();
        delete kbpm_hashmap_;
        delete[] TtlList_;
        delete[] TtlEnabledList_;
    }

    void Manifest::TerminateKeyCacheThread(){
        // Shut down Cache thread
        key_cache_shutdown_ = true;
        key_cache_cv_.SignalAll();
        sem_wait(&key_cache_shutdown_sem_);
        sem_destroy(&key_cache_shutdown_sem_);
    }

    void Manifest::CleanUpSnapshot() {
        SnapshotImpl* snapshot = NULL;
        SimpleLinkedNode* snapshot_node = NULL;
        while((snapshot_node = snapshot_simplelist_.newest())) {
            snapshot = GetSnapshotImpl(snapshot_node);
            assert(snapshot);
            delete snapshot;
#ifdef INSDB_GLOBAL_STATS
            g_del_snap_cnt++;
#endif
        }
    }


    /*
       Thread unsafe ... call only single context, eq, DB close.
       */
    void Manifest::CheckForceFlushSKT(uint16_t worker_id, SKTableDFBuilderInfo *dfb_info) {
        SKTableMem *skt = GetFirstSKTable();
        while(skt) {
            if(skt->GetApproximateDirtyColumnCount() || IsNextSKTableDeleted(skt->GetSKTableSortedListNode())){
                /* 
                 * It does not need to aquire read lock(sktdf_update_mu_)
                 * because this is last WorkerThread which means the skt is not in flushing by the other threads
                 */
                if(!skt->GetSKTableDeviceFormatNode()){
                    skt->LoadSKTableDeviceFormat(this);
                }
                do{
                    printf("[%d :: %s]Last minute flush SKT because of \"%s || %s\" (SKT addr : %p || ID : %d)\n", __LINE__, __func__,skt->GetApproximateDirtyColumnCount() ? "Dirty Columns" : "", IsNextSKTableDeleted(skt->GetSKTableSortedListNode())? "Deleted Next SKT":"", skt, skt->GetSKTableID());
                    skt->FlushSKTableMem(this, worker_id, dfb_info);
                }while(skt->GetApproximateDirtyColumnCount() || IsNextSKTableDeleted(skt->GetSKTableSortedListNode()));
                RemoveFromSKTableMemDirtyCache(skt);
            } 
            if(!skt->IsKeyQueueEmpty())
                abort();
                    skt = Next(skt);
        }
    }


    IterScratchPad* Manifest::AllocIterScratchPad(uint32_t size)
    {
        return sp_allocator_pool_.GetIterScratchPadAllocator()->Alloc(size);
    }
    void Manifest::FreeIterScratchPad(IterScratchPad *sp)
    {
        sp->GetAllocator()->Free(sp, 1024/* Maximun free IterScratchPad count */);
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

    UserValue*  Manifest::AllocUserValue(uint32_t size)
    {
        return uv_allocator_pool_.GetUVAllocator()->Alloc(size);
    }

    void Manifest::FreeUserValue(UserValue* uv)
    {
        if(!uv->UnPin()){
            uv->InitPinAndFlag();
            uv->GetAllocator()->Free(uv);
        }
        return;
    }

    ////////////////////////////////

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
        return uk_allocator_pool_.GetUKAllocator()->Alloc(key_size);
    }
    void Manifest::FreeUserKey(UserKey* ukey)
    {
        ukey->GetAllocator()->Free(ukey);
    }

    //////////////////////////////


    /* 
     * this is called from WriteBatchInternal::Store()
     */
    RequestNode* Manifest::AllocRequestNode()
    {
        return request_node_allocator_pool_.GetRequestNodeAllocator()->Alloc();
    }

    /* Worker Thread calls it after request submission */
    void Manifest::FreeRequestNode(RequestNode* req_node)
    {
        req_node->GetAllocator()->Free(req_node);
    }

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
        assert(buffer.size() <= kMaxSKTableSize);
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

    SKTableMem* Manifest::FindSKTableMem(const Slice& ukey) {
        return sktlist_.SearchLessThanEqual(ukey);
    }

    bool Manifest::InsertSKTableMem(SKTableMem* table) {
        return sktlist_.Insert(table->GetBeginKeySlice(), table);
    }

    SKTableMem* Manifest::RemoveSKTableMemFromCacheAndSkiplist(SKTableMem* table) {

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
        uint32_t crc = crc32c::Unmask(DecodeFixed32(buff + MF_OFFSET_CRC));
        contents->resize(0);
        contents->append(buff, value_size);
        /* calc buffer size */
        uint32_t padding_size = (mf_size % kRequestAlignSize) ? (kRequestAlignSize - (mf_size % kRequestAlignSize)) : 0;
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
        uint64_t sktdf_size = 0;
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
            sktdf_size = DecodeFixed32(parse.data());
            parse.remove_prefix(4);
            skt_id &= ~MF_SKT_ID_MASK;
            if(!(GetVarint32(&parse, &begin_key_size)))
                port::Crash(__FILE__,__LINE__);
            KeySlice begin_key(parse.data(), begin_key_size);
            parse.remove_prefix(begin_key_size);
            //uint16_t col_info_size = DecodeFixed16(parse.data());
            //parse.remove_prefix(2);
            //Slice col_slice(parse.data(), col_info_size);
            skt = new SKTableMem(this, begin_key, skt_id, sktdf_size); //parseing SKTable
            //parse.remove_prefix(col_info_size);
#ifdef INSDB_GLOBAL_STATS
            g_new_skt_cnt++;
#endif
            assert(skt);
            /* Randomly fill 60% of Cache with SKTableDF */
            if ( (rnd.Next()%sktinfo_count) < cacheable_skt_cnt )
                skt->PrefetchSKTable(this, false/*To clean cache*/);
        }

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
            if(skt->GetApproximateDirtyColumnCount() ){
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
            assert(!skt->GetApproximateDirtyColumnCount());
            assert(!skt->IsDeletedSKT());
            assert(skt_id != 0);
            PutFixed32(&contents, skt_id);
            PutFixed32(&contents, skt->GetSKTDFNodeSize());
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
            //kMinUpdateCntForTableFlush = kMaxRequestSize*1024/4096;
            //kUpdateCntForForceTableFlush = kMinUpdateCntForTableFlush << 1;
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

    uint32_t Manifest::BuildFirstSKTable(int32_t skt_id) {
        Status s;
        std::string contents;
        contents.resize(0);

        contents.append(SKTDF_BEGIN_KEY_OFFSET, (char)0); /* fill zero for 40 byte skt info 1 */

#if 0
        /* No store zerokey */
        KeySlice zerokey(0);
        PutFixed16(&contents, 0);/* Begin key prev offset diff */
        PutFixed16(&contents, 7);/* Begin key next offset diff : 7 = prev/next offset diff(4) + shared size(1) + non_shared size(1) + nr column(1)*/ 
        PutVarint16(&contents, (uint16_t)0); /* share size */
        PutVarint16(&contents, (uint16_t)0); /* non-shared size */
        contents.append(1, (char)0);
#endif
        uint32_t skt_info_2_offset = contents.size();

        /* skt info 2 */
        contents.append(2, (char)0); /* decording base key size space */
        EncodeFixed16(const_cast<char*>(contents.data()) +  skt_info_2_offset, 0); 
#if 0
        uint32_t key_hash_offset_start = contents.size();
        contents.append(8, (char)0); /* space for hash offset */
        const uint64_t hash_offset = ((uint64_t)zerokey.GetHashValue() << SKTDF_HASH_SHIFT) | (uint64_t)SKTDF_BEGIN_KEY_OFFSET;   
        EncodeFixed64(const_cast<char*>(contents.data()) + key_hash_offset_start, hash_offset);
#endif

        /* fill skt info 1 */
        EncodeFixed32(const_cast<char*>(contents.data()) + SKTDF_NR_KEY_OFFSET, (uint32_t)0/*No key exists*/);
        EncodeFixed32(const_cast<char*>(contents.data()) + SKTDF_INFO_2_OFFSET, skt_info_2_offset);
        EncodeFixed32(const_cast<char*>(contents.data()) + SKTDF_COMP_START_OFFSET, skt_info_2_offset);
        EncodeFixed32(const_cast<char*>(contents.data()) + SKTDF_SKT_SIZE_OFFSET, contents.size() - SKTDF_BEGIN_KEY_OFFSET/*No begin key exists*/);
        EncodeFixed64(const_cast<char*>(contents.data()) + SKTDF_LARGEST_SEQ_NUM, GenerateSequenceNumber());
        EncodeFixed32(const_cast<char*>(contents.data()) + SKTDF_NEXT_ID_OFFSET, 0);
        EncodeFixed32(const_cast<char*>(contents.data()) + SKTDF_PREALLOC_ID_OFFSET, GenerateNextSKTableID());

        /* fill crc */
        uint32_t crc = crc32c::Value(const_cast<char*>(contents.data())+SKTDF_NR_KEY_OFFSET, SKTDF_BEGIN_KEY_OFFSET-SKTDF_NR_KEY_OFFSET);
        EncodeFixed32(const_cast<char*>((contents.data())+SKTDF_CRC_2_OFFSET), crc32c::Mask(crc));

        crc = crc32c::Value(const_cast<char*>(contents.data())+SKTDF_CRC_2_OFFSET, contents.size()-SKTDF_CRC_2_OFFSET);
        EncodeFixed32(const_cast<char*>(contents.data())+SKTDF_CRC_1_OFFSET, crc32c::Mask(crc));

        if (contents.size() % kRequestAlignSize) {
            uint32_t padding_size = kRequestAlignSize - (contents.size() % kRequestAlignSize);
            contents.append(padding_size, 0xff);
        }
        InSDBKey superkey = GetMetaKey(dbhash_, kSKTable, skt_id);
        Slice value(contents);
        s = env_->Put(superkey, value);
        if (!s.ok()){
            printf("[%d :: %s]fail to write first SKTable\n", __LINE__, __func__);
            abort();
        }
        return value.size();
    }

    Status Manifest::CreateNewInSDB() {
        Status s;
        s = BuildSuperSKTable();
        InitNextInSDBKeySeqNum(0, NULL);
        if (!s.ok()) return s;
        KeySlice zerokey(0);
        uint32_t first_skt_id = 1;
        SetNextSKTableID(first_skt_id);
        uint32_t sktdf_size = BuildFirstSKTable(first_skt_id);
        Slice col_slice(0);
#ifdef CODE_TRACE
        printf("[%d :: %s]sktdf_size : %d\n", __LINE__, __func__,sktdf_size);
#endif
        SKTableMem* first_skt = new SKTableMem(this, zerokey, first_skt_id, sktdf_size);


#ifdef CODE_TRACE
        printf("[%d :: %s]First SKTable : %p\n", __LINE__, __func__, first_skt);
#endif
#ifdef INSDB_GLOBAL_STATS
        g_new_skt_cnt++;
#endif 
        first_skt->LoadSKTableDeviceFormat(this);
#ifdef CODE_TRACE
        {
            SKTDFCacheNode* skt_df_cache = first_skt->GetSKTableDeviceFormatNode();
            char* device_format = skt_df_cache->GetDeviceFormatData();
            uint32_t comp_data_start = DecodeFixed32(device_format+SKTDF_COMP_START_OFFSET);
            uint32_t skt_data_size = DecodeFixed32(device_format + SKTDF_SKT_SIZE_OFFSET);
            printf("[%d :: %s]comp_data_start : %d, skt_data_size : %d, key count: %d\n", __LINE__, __func__,comp_data_start, skt_data_size, skt_df_cache->GetKeyCount());
        }
#endif
        /*No lock because there is no other threads*/
#ifdef CODE_TRACE
        printf("[%d :: %s] Set Fetch for first SKT\n", __LINE__, __func__);
#endif
#if 0
        first_skt->SetForceFlush();
        SKTableDFBuilderInfo dfb_info(this);
        first_skt->FlushSKTableMem(this, 0, &dfb_info);
#endif
        /* There is no key stored yet. A new key will be stored after creating the 1st SKTable */
        InsertSKTableMemCache(first_skt, kCleanSKTCache/*kDirtySKTCache*/);
        return Status::OK();
    }


#if 1 //New(packing) implementation
    int Manifest::ReadSKTableMeta(char* buffer, InSDBMetaType type, uint32_t skt_id, GetType req_type, int buffer_size) {
        int value_size = buffer_size;
        Status s = env_->Get(GetMetaKey(dbhash_, type, skt_id), buffer, &value_size, req_type);
        if (!s.ok()){
            return 0; 
        }
        if(value_size == 0 && req_type == kNonblokcingRead) return 0; 
#if 0
        if(req_type == kNonblokcingRead)
            printf("[ %d : %s ] Success SKT prefetch mechanism\n", __LINE__, __func__);
#endif
        if (type == kSuperSKTable) { 
            if(crc32c::Unmask(DecodeFixed32(buffer)) != crc32c::Value(buffer + sizeof(uint32_t), value_size - sizeof(uint32_t))){
                fprintf(stderr, "[ %d : %s ]sktable size : %d(crc : actual = 0x%x : 0x%x) %s \n", __LINE__, __func__, value_size, crc32c::Unmask(DecodeFixed32(buffer)), crc32c::Value(buffer + sizeof(uint32_t), value_size - sizeof(uint32_t)), type == kSuperSKTable?"Super SKTable" : "Normal SKTable");
                fprintf(stderr, "super sktable crs is mismatch\n");
                return 0;
            }
        }/*else(type == kSKTable){ SKTDFCacheNode will check the CRC } */
        return value_size;
    }
#endif

    Status Manifest::ReadAheadSKTableMeta(InSDBMetaType type, uint32_t skt_id, int buffer_size) {
        int value_size = buffer_size;
        InSDBKey skt_key = GetMetaKey(dbhash_, type, skt_id);
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
        int comp_offset = DecodeFixed32(buff + SKTDF_COMP_START_OFFSET);
        if(comp_offset >= value_size) {
            delete [] buff;
            return Status::Corruption("invalid compress offset");
        }

        // beginkey CRC check
        uint32_t beginkey_crc = crc32c::Unmask(DecodeFixed32(buff + SKTDF_CRC_2_OFFSET));
        uint32_t new_begin_key_crc = crc32c::Value(buff+SKTDF_NR_KEY_OFFSET, comp_offset-SKTDF_NR_KEY_OFFSET);
        if(beginkey_crc != new_begin_key_crc) {
            delete [] buff;
            printf("begin key crc mismatch. stored %x calc %d\n", beginkey_crc, new_begin_key_crc);
            return Status::Corruption("begin key crc mismstch");
        }

        skt_info->skt_id = skt_id;
        skt_info->next_skt_id = DecodeFixed32(buff + SKTDF_NEXT_ID_OFFSET);
        skt_info->prealloc_skt_id = DecodeFixed32(buff + SKTDF_PREALLOC_ID_OFFSET);
        skt_info->largest_seq_num = DecodeFixed64(buff + SKTDF_LARGEST_SEQ_NUM);
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
    Status Manifest::RecoverInSDB() {
        DBRecovery recoveryWork(this);
        // load the log record
        Status s = recoveryWork.LoadLogRecordKey();
        if(s.ok()) {
            // Log record exists. DB is dirty.
            s = recoveryWork.RunRecovery();
            if (!s.ok())
                return Status::Corruption("could not recover DB");
        }

        // return IO error
        return s;
    }
    void Manifest::DeleteSnapshot(Snapshot *handle) {
        SnapshotImpl* snap = reinterpret_cast<SnapshotImpl*>(handle);
        assert(snap);
        if(__DisconnectSnapshot(snap)){
            delete snap; 
        }
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
                return snapshot;
            }
        }
        snapshot = new SnapshotImpl(this, GetLastSequenceNumber(), env_->NowSecond());
#ifdef INSDB_GLOBAL_STATS
        g_new_snap_cnt++;
#endif
        assert(snapshot);
        snapshot_node = snapshot->GetSnapshotListNode();
        snapshot->IncRefCount();
        //RefSnapshot(snap);
        snapshot_simplelist_.InsertFront(snapshot_node);
        snapshot_simplelist_lock_.Unlock();

        return snapshot;
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

    Iterator* Manifest::CreateNewIterator(const ReadOptions& options, uint16_t col_id) {
        nr_iterator_.fetch_add(1, std::memory_order_relaxed);
        if (options.snapshot) {
            Snapshot* snapshot = const_cast<Snapshot*>(options.snapshot);
            SnapshotImpl* snap = reinterpret_cast<SnapshotImpl*>(snapshot);
            snap->IncRefCount();
            //RefSnapshot(snap);
            return snap->CreateNewIterator(col_id);
        }
        SnapshotImpl* snap = __CreateSnapshot();
        assert(snap);
        return snap->CreateNewIterator(col_id);
    }

    void Manifest::DeleteIterator(Iterator *iter) {
        delete iter;
    }

    Status Manifest::CleanUpSKTable(int skt_id, DeleteInSDBCtx *Delctx) {
        memset(Delctx, 0, sizeof(*Delctx));
        return Status::OK();
    }

    void Manifest::DeleteSKTable(void *arg) {
        Status s;
        DeleteSKTableCtx *delctx = reinterpret_cast<DeleteSKTableCtx*>(arg);
        DeleteInSDBCtx d_ctx;
        s = Env::Default()->Flush(kWriteFlush);
        if(!s.ok()){
            printf("[%d :: %s]Flush Fail \n", __LINE__, __func__);
            abort();
        }
        for (uint32_t i = 0; i < delctx->num_skt; i++) {
            s = CleanUpSKTable(delctx->skt_ids[i], &d_ctx);  //cleanup sktable 
            if (s.ok()) {
                CleanupPreallocSKTable(d_ctx.prealloc_skt_id, d_ctx.next_skt_id); //remove prealloc
            }
        }
        delctx->state->mu.Lock();
        delctx->state->num_running -= 1;
        delctx->state->mu.Unlock();
        return;
    }

    Status Manifest::DeleteInSDBWithManifest(uint16_t column_count) {
        Status s;
        int sleep_microsecond = 10000; //10ms
        uint32_t max_thread = 16;
        std::string mf_data;
        s = ReadManifest(&mf_data);
        if (!s.ok()) return s;
        uint32_t *skt_ids = NULL;
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
        /* get skt_id */
        if (sktinfo_count) {
            uint32_t key_size = 0;
            uint32_t sktdf_size = 0;
            skt_ids = new uint32_t[sktinfo_count];
            for (uint32_t i = 0; i < sktinfo_count; i++) { //Parseing SKTable infomation.
                skt_id = DecodeFixed32(parse.data());
                parse.remove_prefix(4);
                skt_id &= ~MF_SKT_ID_MASK;
                skt_ids[i] = skt_id;
                sktdf_size = DecodeFixed32(parse.data());
                parse.remove_prefix(4);
                if(!GetVarint32(&parse, &key_size))
                    port::Crash(__FILE__,__LINE__);
                parse.remove_prefix(key_size);
                uint16_t col_info_size = DecodeFixed16(parse.data());
                parse.remove_prefix(2);
                parse.remove_prefix(col_info_size);
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
                DeleteSKTableMeta(kSKTable, skt_ids[i]); // remove sktale
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
        return s.OK();
    }

    class InSDBKeyCompare {
        public:
            bool operator() (const InSDBKey &lhs, const InSDBKey &rhs) const {
                if (lhs.type != rhs.type) return lhs.type < rhs.type;
                if (lhs.split_seq != rhs.split_seq) return lhs.split_seq < rhs.split_seq;
                if (lhs.seq != rhs.seq) return lhs.seq < rhs.seq;
                return false;
            }
    };
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
        s = env_->IterReq(0xffffffff, dbhash_, &iter_handle, true);
        if (!s.ok()) goto exit;
        /* iter read */
        while(1) {
            iter_buff_size = 1024*32;
            s = env_->IterRead(buffer, &iter_buff_size, iter_handle);
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
        env_->IterReq(0xffffffff, dbhash_, &iter_handle, false);
        if (total_keys_found) {
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
exit:
        if (buffer) delete [] buffer;
        return s;
    }
#endif
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
        while(skt_id != 0) {
            skt_ids.push_back(skt_id); 
            s = CleanUpSKTable(skt_id, &d_ctx);  //cleanup sktable 
            if (!s.ok()) break;;
            CleanupPreallocSKTable(d_ctx.prealloc_skt_id, d_ctx.next_skt_id); //remove prealloc
            skt_id = d_ctx.next_skt_id;
        }
        for (int i = skt_ids.size() - 1;  i >= 0; i--) {
            DeleteSKTableMeta(kSKTable, skt_ids[i]); // remove sktale
        }
        DeleteSKTableMeta(kSuperSKTable, 0); //remove super sktable
        // remove log record
        s = env_->Delete(GetMetaKey(dbhash_, kLogRecord));
        return s;
    }

    bool Manifest::EvictUserKeys(int64_t reclaim_size, char* begin_key_buffer, char* end_key_buffer,bool discard_prefetch)
    {
        // Start eviction with the oldest sktable
        SKTableMem *skt = NULL;
        bool cleanup_dirty_cache_needed = false;
        bool referenced = false;

#ifdef TRACE_MEM
#ifdef KV_TIME_MEASURE
        cycles_t st, e;
        bool b_check = false;
        st = get_cycles();
#endif
#endif
        SetKeyCacheThreadBusy();
        while(reclaim_size > 0|| discard_prefetch || clean_skt_cleanup_.load(std::memory_order_relaxed))
        {
#ifdef TRACE_MEM
#ifdef KV_TIME_MEASURE
            if (!b_check) b_check = true;
#endif
#endif
            // TODO: skip internal key sktable?
            skt = PopSKTableMem(kCleanSKTCache);
            if(!skt){
                cleanup_dirty_cache_needed = true;
                break; /*Clean Cache is Empty*/
            }else if(skt->GetSKTRefCnt()){
                if(skt->IsSKTDirty(this)){
                    InsertSKTableMemCache(skt, kDirtySKTCache, true/*Clear Eviction Flags*/);
                } else {
                    InsertSKTableMemCache(skt, kCleanSKTCache, true/*Clear Eviction Flags*/);
                }
                continue;
            }else if(!skt->GetSKTableDeviceFormatNode()){
                if(skt->IsSKTDirty(this)){
                    InsertSKTableMemCache(skt, kDirtySKTCache, true/*Clear Eviction Flags*/);
                }else if(discard_prefetch || clean_skt_cleanup_.load(std::memory_order_relaxed)){
                    skt->ClearEvictingSKT();
                    skt->DiscardSKTablePrefetch(this);
                }else{
                    InsertSKTableMemCache(skt, kCleanSKTCache, true/*Clear Eviction Flags*/);
                }
                continue;
            }

#ifdef CODE_TRACE
            printf("[%d :: %s]skt_id : %d\n", __LINE__, __func__,skt->GetSKTableID());
#endif

            assert(skt->GetSKTableDeviceFormatNode());

            bool evicted = false;
            referenced = skt->SKTDFReclaim(this, reclaim_size, evicted, clean_skt_cleanup_.load(std::memory_order_relaxed));
            referenced = skt->EvictCleanUserKeyFromKeymap(this, true, evicted, true);
            if(evicted){
                skt->IterLock();
                skt->ReturnIterScratchPad(this);
                skt->IncSKTDFVersion();
                skt->IterUnlock();
            }

            // Eviction loop
            if(skt->IsSKTDirty(this)){
                InsertSKTableMemCache(skt, kDirtySKTCache, true/*Clear Eviction Flags*/);
            }else if(referenced || skt->GetSKTRefCnt()){
                InsertSKTableMemCache(skt, kCleanSKTCache, true/*Clear Eviction Flags*/);
            }else{
                assert(!skt->IsFlushing() && !skt->IsInSKTableCache(this));
                DecreaseMemoryUsage(skt->GetSKTableDeviceFormatNode()->GetSize());
                reclaim_size-=skt->GetSKTableDeviceFormatNode()->GetSize();
                skt->IterLock();
                skt->ReturnIterScratchPad(this);
                skt->IncSKTDFVersion();
                skt->IterUnlock();
                skt->DeleteSKTDeviceFormatCache(this);
                /* The SKTable has been added to SKTable cache */
                skt->ClearEvictingSKT();
            }

            // Unlock sktables
        }
        // Check eviction on SKTable device format
        ClearKeyCacheThreadBusy();
#ifdef TRACE_MEM
#ifdef KV_TIME_MEASURE
        if (b_check) {
            g_evict_count++;
            e = get_cycles();
            uint64_t delta = time_cycle_measure(st,e);
            g_evict_cum_time.fetch_add(delta);
            if (g_evict_max_time.load() < delta) g_evict_max_time = delta;
        }
#endif
#endif
        return cleanup_dirty_cache_needed;
    }



    void Manifest::CleanUpCleanSKTable()
    {
        clean_skt_cleanup_ = true;
    }
    void Manifest::SignalKeyCacheThread()
    {

        if( (MemoryUsage() > kMaxCacheSize && !IsInMemoryReclaim()) || clean_skt_cleanup_)
        {
            key_cache_mu_.Lock();
            key_cache_cv_.SignalAll();
            key_cache_mu_.Unlock();
        }
    }

    void Manifest::KeyCacheThread()
    {
        char* begin_key_buffer = (char*) malloc(kMaxKeySize);
        char* end_key_buffer = (char*) malloc(kMaxKeySize);;
        bool cleanup = false;
        while(!key_cache_shutdown_){
            key_cache_mu_.Lock();
            /** Wait for a signal */
            while(!key_cache_shutdown_ && !clean_skt_cleanup_ && MemoryUsage() <= kCacheSizeLowWatermark )
            {
                ClearMemoryReclaim();
                uint64_t start_time = env_->NowMicros();
                key_cache_cv_.TimedWait(start_time + 2000000/*Wake up every 2 second*/);
                SetMemoryReclaim();
            }
            key_cache_mu_.Unlock();
            cleanup = clean_skt_cleanup_;
            if(cleanup)
                kCacheSizeLowWatermark = 0;
            if(key_cache_shutdown_ && !GetCountSKTableCleanList()){
                /*Finish the Cache Thread if there is no more SKT in the clean cache list */
                break;
            }
            while(((MemoryUsage() > kCacheSizeLowWatermark)/*For memory reclaim*/ || cleanup) && GetCountSKTableCleanList()/*For DB termination*/)
            {
                int64_t reclaim_size = MemoryUsage() - kCacheSizeLowWatermark;
                if(reclaim_size < 0 ) abort();
                if(EvictUserKeys(reclaim_size, begin_key_buffer, end_key_buffer, false)){
                    SignalWorkerThread(true);

                    /* 
                     * Wait for 0.5 Sec, WorkerThread will cleanup the SKT dirty cache and then wake eviction thread up.
                     */
                    key_cache_mu_.Lock();
                    uint64_t start_time = env_->NowMicros();
                    key_cache_cv_.TimedWait(start_time + 500000/*Wake up every 0.5 second*/);
                    key_cache_mu_.Unlock();
                }
            }
        }
        sem_post(&key_cache_shutdown_sem_);
        free(begin_key_buffer);
        free(end_key_buffer);
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
        int size = kDeviceRquestMaxSize;
        Status s = env->Get(GetInSDBKey(), const_cast<char *>(buffer2_.data()), &size);
        return s;
    }

    bool LogRecordKey::DecodeBuffer(std::list<uint64_t> &deleted_ikey_seqs, std::vector<uint64_t> &workers_next_ikey_seqs, std::list<TrxnGroupRange> &completed_trxn_groups) {

        bool success;
        Slice buffer(buffer2_);

        // CRC
        if(buffer.size() < sizeof(uint32_t)) return false;
        uint32_t crc = crc32c::Unmask(DecodeFixed32(buffer.data()));
        buffer.remove_prefix(sizeof(uint32_t));

        // flags and padding size
        if(buffer.size() < sizeof(uint16_t)) return false;
        uint16_t padding_size = DecodeFixed16(buffer.data());
        // discard 4 bytes including reserved space
        buffer.remove_prefix(sizeof(uint32_t));
        bool snappy_compress = (padding_size&LOGRECKEY_FORMAT_FLAGS_SNAPPY_COMPRESS) != 0;
        padding_size &= LOGRECKEY_FORMAT_PADDING_SIZE_MASK;

        if(padding_size >= kRequestAlignSize)
            return false;
        size_t actual_size = buffer.size()-padding_size;
        // CRC check
        uint32_t new_crc = crc32c::Value(buffer.data(), actual_size - LOGRECKEY_OFFSET_BODY);
        if(new_crc != crc) {
            printf("log recrod key crc mismatch stored crc %x calculated crc %x\n", crc, new_crc);
            return false;
        }

#ifdef HAVE_SNAPPY
        if(snappy_compress) {
            size_t uncomress_len = 0;
            success = snappy::GetUncompressedLength(buffer.data(), actual_size, &uncomress_len);
            if(!success)
                return false;
            buffer2_.resize(uncomress_len);
            success = snappy::RawUncompress(buffer.data(), actual_size, const_cast<char*>(buffer1_.data()));
            if(!success)
                return false;
        } else {
            buffer_ = buffer2_;
        }
#else
        if(snappy_compress)
            return false;
        else {
            buffer_ = buffer2_;
        }

#endif

        // get the uncompressed buffer
        Slice uncomp_buffer(buffer_);

        // number of worker next ikeys
        uint32_t nr_workers;
        success = GetVarint32(&uncomp_buffer, &nr_workers);
        if(!success)
            return false;
        if(nr_workers != (uint32_t)kWriteWorkerCount)
            port::Crash(__FILE__,__LINE__);

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
        if(nr_completed_groups != (uint32_t)kWriteWorkerCount)
            port::Crash(__FILE__,__LINE__);

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

        return true;
    }

    // Collect key block delete candidates for a worker
    void Manifest::WorkerLogCleanupInternal(WorkerData &logcleanup_data, std::list<SequenceNumber> &worker_next_ikey_seq, std::queue<KeyBlockMetaLog*> &kbml_cleanup, SequenceNumber &smallest_ikey)
    {
        bool first = true;
        std::queue<KeyBlockMetaLog*> * logQ = logcleanup_data.GetInActiveQueue();
        KeyBlockMetaLog* kbml, *last_cleanup_kbml=NULL;
scan:
        // iterate each KBML
        Spinlock *logLock = logcleanup_data.GetInActiveLock();
        logLock->Lock();
        while(!logQ->empty()){
            kbml = logQ->front();
            if(kbml->GetNRSCount()) {
                logLock->Unlock();
                goto finish_this_worker;
            }
            // append the ikey seq num to the device format
            logQ->pop();
            // append kbml to delete candidate list
            if(kbml->GetiKeySequenceNumber() < smallest_ikey)
                smallest_ikey = kbml->GetiKeySequenceNumber();
            kbml_cleanup.push(kbml);
            // collect KBMLs up to the max to prevent IO size overflow.
            // 2MB / 9 bytes
            if (kbml_cleanup.size() > 450000) {
                logLock->Unlock();
                goto finish_this_worker;
            }
            // store the last added ikey to determine worker's next ikey seq
            last_cleanup_kbml = kbml;
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
        logLock->Unlock();
finish_this_worker:
        // add the worker's next ikey sequence
        if (last_cleanup_kbml) {
            worker_next_ikey_seq.push_back(last_cleanup_kbml->GetNextIKeySeqNum());
            logcleanup_data.SetLastWorkerNextIKeySeq(last_cleanup_kbml->GetNextIKeySeqNum());
        } else {
            worker_next_ikey_seq.push_back(logcleanup_data.GetLastWorkerNextIKeySeq());
        }
    }

    void Manifest::WorkerLogCleanupDelete(LogRecordKey &logkey, std::queue<KeyBlockMetaLog*> kbml_cleanup, SequenceNumber &smallest_ikey)
    {
        KeyBlockMetaLog *kbml;
        KeyBlockPrimaryMeta *kbpm;
        KeyBlock *kb;
        std::list<SequenceNumber> deleteSeqList;
        std::vector<InSDBKey> deleteList;

        deleteSeqList.push_back(smallest_ikey);
        // Free KB, KBM, and KBML memory, and construct key names
        while(!kbml_cleanup.empty()) {
            kbml = kbml_cleanup.front();
            kbml_cleanup.pop();
            kbpm = kbml->GetKBPM();
            if (kbpm->IsDummy()) abort();
            // acquire submit protection
            while(kbpm->SetSubmitInProgress());
            kbpm->SetKBML(nullptr);

            if(!kbpm->GetKeyBlockBitmap() && kbpm->GetRefCnt() == 0 ) {
                kbpm->SetOutLogAndCheckEvicable();
                kbpm->ClearDeleteOnly();
                assert(kbml->GetValidTrxnCount(kPutType) == 0);
                assert(kbml->GetValidTrxnCount(kDelType) == 0);
                // release submit protection
                kbpm->ClearSubmitInProgress();

                kb = kbpm->GetKeyBlockAddr();

                // add to to-be-deleted key list
                deleteSeqList.push_back(kbpm->GetiKeySequenceNumber() - smallest_ikey);

#ifdef CODE_TRACE    
                printf("[%d :: %s]Delete Meta ikey : %ld\n", __LINE__, __func__, kbpm->GetiKeySequenceNumber());
#endif
                deleteList.push_back(GetKBKey(dbhash_, kKBMeta, kbpm->GetiKeySequenceNumber()));
                deleteList.push_back(GetKBKey(dbhash_, kKBlock, kbpm->GetiKeySequenceNumber()));
                // kbpm and kb are removed from hash and cache in flush sktable
                RemoveKBPMFromHashMap(kbpm->GetiKeySequenceNumber());
                if(kb){
                    DecreaseMemoryUsage(kb->GetSize());
#ifdef CODE_TRACE
                    printf("[%d :: %s]Free KeyBlock size(%d)\n", __LINE__, __func__, kb->GetSize());
#endif
                    FreeKB(kb);
                    kbpm->ClearKeyBlockAddr();
                }
                //else if (!kbpm->IsDeleteOnly()) abort();
                FreeKBPM(kbpm);
            } else {
                // release submit protection
                kbpm->ClearSubmitInProgress();
                if(kbpm->SetOutLogAndCheckEvicable() && CheckKeyEvictionThreshold()) {
                    if(!kbpm->SetEvicting()){
                        if(!kbpm->GetRefCnt() && !kbpm->CheckRefBitmap()) {
                            RemoveKBPMFromHashMap(kbpm->GetiKeySequenceNumber());
                            while(kbpm->GetRefCnt() || kbpm->CheckRefBitmap());/* Wait until deref */
                            kb = kbpm->GetKeyBlockAddr();
                            if(kb){
                                DecreaseMemoryUsage(kb->GetSize());
                                FreeKB(kb);
                                kbpm->ClearKeyBlockAddr();
                            }
                            //else abort();
                            FreeKBPM(kbpm);
                        }else
                            kbpm->ClearEvicting();
                    }
                }
            }
            FreeKBML(kbml);
        }

        if(deleteSeqList.size() == 1)
            return;

        // Set the number of deleted ikey sequence number
        logkey.AppendIKeySeqList(deleteSeqList);

        // Build IO buffer. Compress if possible.
        logkey.BuildIOBuffer();

        // write the log record key
        const Slice io_buffer( logkey.GetIOBuffer() );

        // buffer limit is same as max sktable size
        if(io_buffer.size() > kDeviceRquestMaxSize) {
            port::Crash(__FILE__,__LINE__);
        }

        // write the log record key to the device
        env_->Put(logkey.GetInSDBKey() , io_buffer, false);

        // Delete the keys from the device
        if(deleteList.size()) {
            InSDBKey null_insdbkey = { 0 };
            std::vector<Env::DevStatus> status;
            env_->MultiDel(deleteList, null_insdbkey, false, status);
        }
    }

    void Manifest::LogCleanupInternal(LogRecordKey &LogRecordkey)
    {
        std::queue<KeyBlockMetaLog*> kbml_cleanup;
        std::list<TrxnGroupRange> completed_trxn_groups;
        std::list<SequenceNumber> worker_next_ikey_seq;
        SequenceNumber smallest_ikey = ULONG_MAX;

        // Get completed transaction group
        CopyCompletedTrxnGroupRange(completed_trxn_groups);

        // Append zeroed header and the number of ikeys
        LogRecordkey.AppendHeaderSpace();

        // collect deleted ikeys in each worker queue
        for(int worker_idx = 0; worker_idx < kWriteWorkerCount; worker_idx++){
            WorkerLogCleanupInternal(logcleanup_data_[worker_idx], worker_next_ikey_seq, kbml_cleanup, smallest_ikey);
        }
        // skip writing log record key if no key is deleted.
        if(!kbml_cleanup.size()) {
            LogRecordkey.Reset();
            return;
        }

        // Append worker next ikey seq
        LogRecordkey.AppendIKeySeqList(worker_next_ikey_seq);

        // Append completed transaction groups
        LogRecordkey.AppendTrxnGroupList(completed_trxn_groups);

        // delete the internal keys
        WorkerLogCleanupDelete(LogRecordkey, kbml_cleanup, smallest_ikey);

        // reset the log record key object
        LogRecordkey.Reset();
    }

    bool Manifest::IsWorkerLogCleanupQueueEmpty()
    {
        for(int worker_id = 0; worker_id < kWriteWorkerCount; worker_id++){
            WorkerData *logcleanup_queue = logcleanup_data_ + worker_id;
            if(!logcleanup_queue->IsEmpty(0) || !logcleanup_queue->IsEmpty(1))
                return false;
        }
        return true;
    }

    // Signal log cleanup
    void Manifest::LogCleanupSignal()
    {
        if(!SetLogCleanupRequested())
        {
#ifdef CODE_TRACE
            printf("[%d :: %s]Wake Log Cleanup Thread up\n", __LINE__, __func__);
#endif
            logcleanup_mu_.Lock();
            logcleanup_cv_.Signal();
            logcleanup_mu_.Unlock();
        }
    }

    // log cleanup thread function
    void Manifest::LogCleanupThreadFn()
    {
        LogRecordKey Logkey(dbhash_);
        bool timeout;

        while(true){
            timeout = false;
            // Signal loop
            logcleanup_mu_.Lock();
            while(!logcleanup_shutdown_ && !timeout && !ClearLogCleanupRequested()) {
                timeout = logcleanup_cv_.TimedWait(env_->NowMicros() + 1000000/*Wake up every 1 second*/);
            }
#ifdef CODE_TRACE
            printf("[%d :: %s]Run Log Cleanup Thread\n", __LINE__, __func__);
#endif
            logcleanup_mu_.Unlock();
            // Exit only when all KBMLs are flushed.
            if(logcleanup_shutdown_ && IsWorkerLogCleanupQueueEmpty())
                break;
            // Process requests
            LogCleanupInternal(Logkey);
        }
        /*
           for(int i=0; i<2; i++) {
           LogCleanupInternal(Logkey);
           }
           */
        if(!IsWorkerLogCleanupQueueEmpty())
            port::Crash(__FILE__,__LINE__);
    }

    void Manifest::TerminateLogCleanupThread(){
        // Shut down Cache thread
        logcleanup_shutdown_= true;
        logcleanup_cv_.SignalAll();
        if (logcleanup_tid_)
            Env::Default()->ThreadJoin(logcleanup_tid_);
    }

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

    KeyBlock* Manifest::LoadValue(uint64_t insdb_key_seq, uint32_t ivalue_size, bool *from_readahead, KeyBlockPrimaryMeta* kbpm)
    {
        Status s;
        int buffer_size = SizeAlignment(ivalue_size);
        InSDBKey insdb_key = GetKBKey(dbhash_, kKBlock, insdb_key_seq);
        KeyBlock* kb = AllocKB();

        Slice *io_buf = GetIOBuffer(kDeviceRquestMaxSize);
        assert(buffer_size <= io_buf->size());
        Slice buffer = kb->GetRawData();

        char *contents = const_cast<char*>(io_buf->data());

        s = env_->Get(insdb_key, contents, &buffer_size, kSyncGet, from_readahead);
        if (!s.ok()) {
            fprintf(stderr, "keyblock does not exist(ikey seq: %ld)\n", insdb_key_seq);
            abort();
        }
#ifdef CODE_TRACE
        printf("[%d :: %s] key seq %lu size %u from_readahead %d\n", __LINE__, __func__, insdb_key_seq, buffer_size, *from_readahead);
#endif
        /////////////////
        uint32_t orig_crc = crc32c::Unmask(DecodeFixed32(contents)); /* get crc */
        uint32_t kb_keycnt_size_loc = *((uint32_t*)(contents + 4));
        char* kb_buffer = (char*)(contents + 8)/*CRC(4B) + # of Keys(1B) + Size of compressed KB(3B)*/;
        uint32_t actual_crc = crc32c::Value((contents + 4), (kb_keycnt_size_loc & KEYBLOCK_KB_SIZE_MASK) + 4/* 4 is for "kb_keycnt_size_loc" */);

        if (orig_crc != actual_crc) {
            printf("[%d :: %s]crc mismatch check this orig (%d) actual(%d)\n", __LINE__, __func__, orig_crc, actual_crc);
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
                kb->Init(ulength);  /* initially set aligned size */
                if (!port::Snappy_Uncompress(kb_buffer, kb_keycnt_size_loc & KEYBLOCK_KB_SIZE_MASK, const_cast<char*>(kb->GetRawData().data()))) {
                    fprintf(stderr, "corrupted compressed data\n");
                    abort();
                }
            }
        }else{
            kb->Init(ivalue_size);  /* initially set aligned size */
            memcpy(const_cast<char*>(kb->GetRawData().data()), kb_buffer, ivalue_size);
        }
        /////////////
        kbpm->ClearKBPrefetched();
        IncreaseMemoryUsage(kb->GetSize());
        return kb;

    }

    UserKey* Manifest::IncreaseUserKeyReference(UserKey* uk){

        if(uk->IsEvictionPrepared()){  
            return NULL;
        }
        uk->IncreaseReferenceCount();
        return uk;
    }
#ifdef INSDB_USE_HASHMAP
    bool Manifest::InsertUserKeyToHashMap(UserKey* uk) {
        auto it = key_hashmap_->insert(uk->GetKeySlice(), uk);
        return it.second;
    }

    /*Reture UserKey with increased referenced count*/
    UserKey* Manifest::FindUserKeyFromHashMap(KeySlice& key) {
        UserKey* uk = NULL;
        while(!uk){
            auto it = key_hashmap_->find(key);
            if (it != key_hashmap_->end()) {
                uk = IncreaseUserKeyReference(it->second);
                if(uk) uk = uk->IsSKTableEvictionInProgress();
            }else
                return NULL;
        }
        return uk;
    }
    bool Manifest::RemoveUserKeyFromHashMap(KeySlice& key) {
        return key_hashmap_->erase(key);
    }
    bool Manifest::KeyHashMapFindOrInsertUserKey(UserKey*& uk, const KeySlice& key, const uint16_t col_id ) {
        bool new_uk = false;
        while(!(uk = FindUserKeyFromHashMap(const_cast<KeySlice& >(key)))){
            // note that the new key could be inserted after its sktable is evicted.
            uk = AllocUserKey(key.size());
            uk->InitUserKey(key);

            uk->ColumnLock(col_id);/* Lock the column */
            if (InsertUserKeyToHashMap(uk)){
                new_uk = true;
                break;
            }
            uk->ColumnUnlock(col_id);/* Lock the column */
            uk->UserKeyCleanup(this);
            FreeUserKey(uk);
        }
        if(!new_uk)
            uk->ColumnLock(col_id);/* Lock the column */
        return new_uk;
    }

    bool Manifest::InsertKBPMToHashMap(KeyBlockPrimaryMeta *kbpm) {
        assert(kbpm->GetiKeySequenceNumber());
        auto it = kbpm_hashmap_->insert(kbpm->GetiKeySequenceNumber(), kbpm);
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
        kbpm_hashmap_->erase(ikey);
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





#if 0
    UserKey* Manifest::GetFirstUserKey(){ 

        UserKey* uk = NULL;

        /* The first SKTable is never evicted */ 
        while(!uk){
            /*First must exist even if it is First key(dummy) */
            uk = IncreaseUserKeyReference(keymap_.First());
            if(uk) uk = uk->IsSKTableEvictionInProgress();
        }
        return uk;
    }

    UserKey* Manifest::GetLastUserKey(){ 

        /* The last SKTable is never evicted */ 
        UserKey* uk = NULL;
        while(!uk){
            /*Last must exist even if it is First key(dummy) */
            uk = IncreaseUserKeyReference(keymap_.Last());
            if(uk) uk = uk->IsSKTableEvictionInProgress();
        }
        return uk;
    }

    UserKey* Manifest::SearchUserKey(const Slice& tkey){ 
        UserKey* uk = NULL;
        KeySlice key(tkey);
        /* Check Hash first */
        uk = GetKeyMap()->Search(key);
        if(uk) uk = IncreaseUserKeyReference(uk);
        if(uk) uk = uk->IsSKTableEvictionInProgress();
        if(uk && uk->IsFetched()) return uk;
        /* if the key does not exist, fetch first and then find the key */
        SKTableMem *skt = FindSKTableMem(key);
        //skt->IncSKTRefCnt();
        skt->LoadSKTable(this);
        while(!uk){
            /* FindSKTableMem & fecth function may need to be relocated to here in case of burst deletion */
            uk = keymap_.SearchGreaterOrEqual(key);
            if(!uk) break;
            uk = IncreaseUserKeyReference(uk);
            if(uk) uk = uk->IsSKTableEvictionInProgress();
        }
        //skt->DecSKTRefCnt();
#if 0
        if(CheckKeyEvictionThreshold() && (GetCountSKTableCleanList() > GetCountSKTableDirtyList()) && (GetCountSKTableCleanList()/4))
            SignalKeyCacheThread();//           EvictUserKeys(GetCountSKTableCleanList()/4);
#endif
        return uk;
    }

    UserKey* Manifest::SearchLessThanEqualUserKey(const Slice& tkey){ 
        UserKey* uk = NULL;
        KeySlice key(tkey);
        /* Check Hash first */
        uk = GetKeyMap()->Search(key);
        if(uk) uk = IncreaseUserKeyReference(uk);
        if(uk) uk = uk->IsSKTableEvictionInProgress();

        if(uk && uk->IsFetched()) return uk;
        /* if the key does not exist, fetch first and then find the key */
        SKTableMem *skt = FindSKTableMem(key);
        //skt->IncSKTRefCnt();
        skt->LoadSKTable(this);
        while(!uk){
            /* FindSKTableMem & fecth function may need to be relocated to here in case of burst deletion */
            uk = keymap_.SearchLessThanEqual(key);
            if(!uk) break;
            uk = IncreaseUserKeyReference(uk);
            if(uk) uk = uk->IsSKTableEvictionInProgress();
        }
        //skt->DecSKTRefCnt();
#if 0
        if(CheckKeyEvictionThreshold() && (GetCountSKTableCleanList() > GetCountSKTableDirtyList()) && (GetCountSKTableCleanList()/4))
            SignalKeyCacheThread();//           EvictUserKeys(GetCountSKTableCleanList()/4);
#endif
        return uk;
    }


    bool Manifest::InsertUserKey(UserKey* ukey, bool begin_key) {
        if(!keymap_.Insert(ukey->GetKeySlice(), ukey))
            return false;
        return true;
    }
#endif

    void Manifest::RemoveUserKeyFromHashMapWrapper(UserKey* ukey)
    {
        KeySlice key = ukey->GetKeySlice();
        bool active = false;
        if(!RemoveUserKeyFromHashMap(key))
            abort();/*given key must exist*/
        //Wait for Other access
        while(ukey->GetReferenceCount());
    }
    /* 
     * Iterator call this function with ref_cnt_inc == false.
     * EvictUserKeys can not selete the SKTable to which KBPM belongs.
     */
    KeyBlockPrimaryMeta* Manifest::GetKBPM(SequenceNumber seq, uint32_t size,  bool ref_cnt_inc) {
        KeyBlockPrimaryMeta* kbpm = NULL;
retry:
        /* search from hash table */
        kbpm = FindKBPMFromHashMap(seq);
        if (!kbpm) {
            /* create dummy and insert to hash table */
            kbpm = AllocKBPM();
            kbpm->Init(seq, size);
            kbpm->ClearDeleteOnly();
            kbpm->SetDummy();
            kbpm->SetOutLog();
            if(!InsertKBPMToHashMap(kbpm)) {
                FreeKBPM(kbpm);
                goto retry;
            }
        }
        if(ref_cnt_inc){
            kbpm->IncRefCnt();
            if(kbpm->IsEvicting()){
                kbpm->DecRefCnt();
                goto retry;
            }
        }

        return kbpm;
    }


    KeyBlockPrimaryMeta* Manifest::GetKBPM(ColumnNode* col_node) {
        KeyBlockPrimaryMeta* kbpm = NULL;
        KeyBlockMeta *kbm = col_node->GetKeyBlockMeta();
        if (kbm) {
            kbpm = kbm->GetKBPM();
        } else {
            /* search from hash table */
            /* 
             * The ref cnt is increased in GetKBPM(seq) for this ColumnNode 
             * The ref cnt will be decreased when evicting UserKey
             */
            kbpm = GetKBPM(col_node->GetInSDBKeySeqNum(), col_node->GetiValueSize());
            col_node->SetKeyBlockMeta(kbpm);
        }
        /* The caller should decrease the ref cnt. */
        kbpm->IncRefCnt();
        return kbpm;
    }

    /* return KBPM in order to decrease ref count*/
    KeyBlockPrimaryMeta* Manifest::GetValueUsingColumn(ColumnNode* col_node, Slice& value)
    { 
        UserValue *uv = NULL;
        bool ra_rahit = false;
        KeyBlock* kb = NULL;

        /**
          Search kbpm or create dummy kbpm.
          - ColumnNode can point kbpm/kbml during write process or it can be point null loading process.
          - During read operation, we check kbpm to get kb in cache. If it is not available, we need to
          create dummy kbpm/kb and fill kb from device and insert it to cache.
          */
        /* check and get kbm from ColumnNode */
        KeyBlockPrimaryMeta* kbpm = GetKBPM(col_node);
        /*KBPM is accessed using ColumnNode. In this case we do not SetRefBitmap(). SetRefBitmap() is set by columns in SKTDF */
        assert(kbpm);
#if 0
retry:
#endif
        if (!(kb = kbpm->GetKeyBlockAddr())) {
            kbpm->Lock();
            if (!(kb = kbpm->GetKeyBlockAddr())) {
                kb = LoadValue(col_node->GetInSDBKeySeqNum(), col_node->GetiValueSize(), &ra_rahit, kbpm);
                kbpm->SetKeyBlockAddr(kb);
            }
            kbpm->Unlock();
        }
#if 0
        /* Do we need this code?? */
        if (kbpm->GetKeyBlockAddr() != kb) {
            goto retry;
        }
#endif
        /* get value from kb */
        value = kb->GetValueFromKeyBlock(this, col_node->GetKeyBlockOffset());
        return kbpm;
    }

    bool Manifest::GetLatestSequenceUsingSKTDF(const KeySlice& key, uint8_t col_id, SequenceNumber* seq_num, uint64_t cur_time)
    { 
        SKTableMem* table = NULL;

        while(true){
            table = FindSKTableMem(key);
            /* Reference count prevents eviction */
            if(table->GetLatestSequenceNumber(this, key, col_id, seq_num, cur_time))
                break;
            table->DecSKTRefCnt();
        }
        table->DecSKTRefCnt();
        if(*seq_num == kMaxSequenceNumber)
            return false; /*No column exists*/
        return true;
    }

    KeyBlockPrimaryMeta* Manifest::GetValueUsingSKTDF(const KeySlice& key, Slice& value, uint8_t col_id, SequenceNumber seq_num, uint64_t cur_time)
    { 
        bool ra_rahit = false;
        KeyBlock* kb = NULL;
        ColumnScratchPad column = {0,0,0};
        SKTableMem* table = NULL;


        while(true){
            table = FindSKTableMem(key);
            /* Reference count prevents eviction */
            if(table->GetColumnInfo(this, key, col_id, seq_num, cur_time, column))
                break;
        }
        if(!column.ikey_seq_num) {
            return NULL; /*No column exists*/
        }

        KeyBlockPrimaryMeta* kbpm = GetKBPM(column.ikey_seq_num, column.ivalue_size);
        assert(kbpm);
        /* SKTDF eviction thread will call the ClearRefBitmap() if one of 2 bits of column entry's ref bit(non-atomic) is set */
        kbpm->SetRefBitmap(column.offset);
        //kbpm->ClearRefBitmap(column.offset);

#if 0
retry:
#endif
        if (!(kb = kbpm->GetKeyBlockAddr())) {
            kbpm->Lock();
            if (!(kb = kbpm->GetKeyBlockAddr())) {
                kb = LoadValue(column.ikey_seq_num, column.ivalue_size, &ra_rahit, kbpm);
                kbpm->SetKeyBlockAddr(kb);
            }
            kbpm->Unlock();
        }
#if 0
        if (kbpm->GetKeyBlockAddr() != kb) {
            kbpm->ClearRefBitmap(col_node->GetKeyBlockOffset());
            goto retry;
        }
#endif
        /* get value from kb */
        value = kb->GetValueFromKeyBlock(this, column.offset);
        //table->DecSKTRefCnt();
        return kbpm;
    }

    bool Manifest::CheckKeyEvictionThreshold(void){
        return (MemoryUsage() > kMaxCacheSize);
    }

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
        if(result)
            AddCompletedTrxnGroupRange(tgid);
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

    /*bool Manifest::IsCompletedTrxnGroupRange(TrxnGroupID tgid){
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
    }*/


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

    void Manifest::PrintParam() {
#ifdef TRACE_MEM
        printf("#LF_skt_cache[%ld] #LF_skt_dirty[%ld]\n",
                skt_cache_[kCleanSKTCache].cache.size(),
                skt_cache_[kDirtySKTCache].cache.size(),
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
        printf("#Device Format SKT status : hit[%lu] miss[%lu] Insert[%lu] evict[%lu] current cnt[%ld]\n",
                g_hit_df_skt.load(),
                g_miss_df_skt.load(),
                g_insert_df_skt.load(),
                g_evict_df_skt.load(),
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

    void Manifest::InsertSKTableMemCache(SKTableMem* sktm, SKTCacheType type, bool clear_stat, bool  wakeup) {
        skt_cache_[type].Lock();
        if(clear_stat){
            assert(sktm->GetLocation() == GetSKTCleanCache()->end());
            sktm->ClearFlushAndEvicting();
        }else if(sktm->GetLocation() != GetSKTCleanCache()->end() || sktm->IsFlushOrEvicting()){
            skt_cache_[type].Unlock();
            return;
        }
#ifdef CODE_TRACE
        printf("[%d :: %s]%p(ID : %d)\n", __LINE__, __func__, sktm, sktm->GetSKTableID());
#endif
        skt_cache_[type].PushItem(sktm, false);
        if(type == kDirtySKTCache) sktm->SetInDirtyCache();
        skt_cache_[type].Unlock();
        if(wakeup){
            assert(type == kCleanSKTCache);
            SignalKeyCacheThread();
        }
    }

    bool Manifest::RemoveFromSKTableMemDirtyCache(SKTableMem* sktm) {

        // set clean cache as defaul to acquire the lock
        // clean and dirty cache share one lock, it's ok to lock with clean cache and unlock with dirty cache.
        // SKTCache<SKTableMem> *skt_cache = &skt_cache_[kDirtySKTCache];

        skt_cache_lock_.Lock();
        if(/*sktm->GetLocation() == GetSKTCleanCache()->end() || */!sktm->IsInDirtyCache()) {
            /*If the sktm is in Clean Cache, it means the sktm has been flushed by the other thread already */
            skt_cache_lock_.Unlock();
            return false;
        }
#ifndef NDEBUG
        assert(!sktm->SetFlushing());
        assert(sktm->ClearInDirtyCache());
#else
        sktm->SetFlushing();
        sktm->ClearInDirtyCache();
#endif
#ifdef CODE_TRACE
        printf("[%d :: %s]%p(%d)\n", __LINE__, __func__, sktm, sktm->GetSKTableID());
#endif
        skt_cache_[kDirtySKTCache].DeleteItem(sktm); 
        sktm->SetLocation(GetSKTCleanCache()->end());
        skt_cache_lock_.Unlock();
        return true;
    }

    void Manifest::MigrateSKTableMemCache(SKTableMem* sktm) {
        skt_cache_lock_.Lock();
        if(sktm->GetLocation() != GetSKTCleanCache()->end()){
            if(!sktm->SetInDirtyCache()){ 
                /* If it was in the clean cache not in dirty cache */
                assert(!sktm->IsFlushOrEvicting());
                skt_cache_[kCleanSKTCache].DeleteItem(sktm);
#ifdef CODE_TRACE
                printf("[%d :: %s]%p(%d)\n", __LINE__, __func__, sktm, sktm->GetSKTableID());
#endif
                skt_cache_[kDirtySKTCache].PushItem(sktm, false);
            }

        }/*else{ The other thread already get this }*/
        skt_cache_lock_.Unlock();
#ifdef CODE_TRACE
        printf("[%d :: %s]sktm addr : %p || cache loc : 0x%lx\n", __LINE__, __func__, sktm, sktm->GetLocation());
#endif
    }

    size_t Manifest::SKTCountInSKTableMemCache(SKTCacheType type) {
        return skt_cache_[type].GetCacheSize();
    }

    SKTableMem* Manifest::PopSKTableMem(SKTCacheType type) {
        skt_cache_lock_.Lock();
        SKTableMem* sktm = skt_cache_[type].NextItem();
        if (sktm){
#ifdef CODE_TRACE
            printf("[%d :: %s]%p(ID : %d)\n", __LINE__, __func__, sktm, sktm->GetSKTableID());
#endif
            if(type == kDirtySKTCache){
                assert(!sktm->IsEvictingSKT());
                assert(!sktm->IsFlushing());
                sktm->SetFlushing();
                sktm->ClearInDirtyCache();
            }else{
                assert(!sktm->IsEvictingSKT());
                assert(!sktm->IsFlushing());
                assert(!sktm->IsInDirtyCache());
                sktm->SetEvictingSKT();
            }
            skt_cache_[type].PopNextItem(sktm->GetSize());
            sktm->SetLocation(GetSKTCleanCache()->end());
        }
        skt_cache_lock_.Unlock();
        return sktm;
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
        uint32_t data_offset = (DecodeFixed32((char *)(offset + index))&KEYBLOCK_OFFSET_MASK);
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


    bool KeyBlock::DecodeAllKeyMeta(KeyBlockMetaLog *kbml, std::list<ColumnInfo> &key_list) {
        uint16_t nr_key = *(reinterpret_cast<uint16_t *>(raw_data_));
        uint32_t* offset = reinterpret_cast<uint32_t *>(raw_data_+2);/* The # of keys (2B) */

        for(uint16_t index=0; index<nr_key; index++)
        {
            uint32_t data_offset = *(offset + index); /* # keys (2B)*/
            bool ttl = data_offset & KEYBLOCK_TTL_MASK; /* get ttl */
            bool comp = data_offset & KEYBLOCK_COMP_MASK; /* get cmp */
            uint16_t raw_data_size = 0;
            uint32_t value_size = 0;
            uint16_t key_size = 0;
            UserValue *uv = NULL;
            data_offset &= KEYBLOCK_OFFSET_MASK;

            ColumnInfo colinfo = { 0 };
            colinfo.kbml = kbml;
            colinfo.kb_index = index;
            colinfo.kb_size = data_size_;
            colinfo.kbpm = kbml->GetKBPM();
            key_list.push_back(colinfo);
        }

        return true;
    }

    uint32_t KeyBlockPrimaryMeta::GetEstimatedBufferSize(){
        assert(!IsDummy());
        if(kbml_) return kbml_->GetEstimatedBufferSize();
        return KBPM_HEADER_SIZE;
    }
    uint32_t KeyBlockPrimaryMeta::BuildDeviceFormatKeyBlockMeta(char* kbm_buffer){
        assert(!IsDummy());
        char* buf = kbm_buffer;
        uint32_t size = 0;
        if (kbml_) size = kbml_->BuildDeviceFormatKeyBlockMeta(kbm_buffer);
        else {
            size = BuildDeviceFormatKeyBlockMetaInternal(buf);
            *(buf+size++) = (uint8_t)0; //Trxn count of Put 
            *(buf+size++) = (uint8_t)0; //Trxn count of Del
        }
        return size;
    }

    bool KeyBlockPrimaryMeta::InitWithDeviceFormat(Manifest  *mf, const char *buffer, uint32_t buffer_size, KeyBlockMetaLog* &kbml, bool outlog) {

        assert(IsDummy());
        Slice df(buffer, buffer_size);
        uint8_t count[kNrRequestType] = {0}; // Max 32
        uint16_t merge_trxn_cnt[kNrRequestType][kMaxRequestPerKeyBlock] = {0};
        uint64_t next_ikey_seq_num = 0;
        uint32_t nr_trxns = 0;

        kbml = nullptr;

        // bitmap
        uint32_t bitmap = DecodeFixed32(buffer);
        df.remove_prefix(sizeof(uint32_t));
        // original key count
        uint8_t orig_keycnt = df.data()[0];
        df.remove_prefix(sizeof(uint8_t));

        // create KBPM
        // note that KBPM is initialized by original key count to keep key offset intact.
        assert(orig_keycnt);
        InitInDummyState(orig_keycnt);
        OverrideKeyBlockBitmap(bitmap);
        if(outlog)
            return true;

        // Put transaction list
        std::list<TrxnInfo*> trxn_list[2];
        bool success = TrxnInfo::DecodePutTransactionList(df, trxn_list[kPutType]);
        if(!success) goto error_out;

        // Del transaction list
        success = TrxnInfo::DecodeDelTransactionList(df, trxn_list[kDelType]);
        if(!success) goto error_out;

        // next ikey seq
        next_ikey_seq_num = DecodeFixed64(df.data());

        for(int i=0; i<kNrRequestType; i++)
            count[i] = trxn_list[i].size();

        // create KBML
        for(int i=0; i<kNrRequestType; i++) {
            int j=0;
            for(auto it : trxn_list[i]) {
                merge_trxn_cnt[i][j] = it->GetNrMergedTrxnList();
                j++;
                nr_trxns++;
            }
        }

        // if no transaction is recorded, skip creating KBML.
        if(!nr_trxns)
            goto return_success;

        kbml = mf->AllocKBML();
        kbml->Init(next_ikey_seq_num, this, count, merge_trxn_cnt);
        SetKBML(kbml);
        // fill KBML transaction lists
        for(int i=0; i<kNrRequestType; i++) {
            int index=0;
            for(auto it : trxn_list[i]) {
                if(i == kPutType) index = it->GetKeyOffset();
                Transaction trxn = it->GetTrxn();
                kbml->SetHeadTrxn(it->GetKey(), it->GetColId(), (insdb::RequestType)i, index, trxn);
                for(int merged_idx=0; merged_idx<it->GetNrMergedTrxnList(); merged_idx++)
                    kbml->InsertMergedTrxn(index, (insdb::RequestType)i, it->GetMergedTrxnList()[merged_idx], merged_idx);
                if( i != kPutType)
                    index++;
            }
        }
return_success:
        return true;

error_out:
        for(int i=0; i<kNrRequestType; i++) {
            for(auto it : trxn_list[i]) {
                delete it;
            }
        }

        if(kbml) mf->FreeKBML(kbml);
        return false;
    }

} //namespace
