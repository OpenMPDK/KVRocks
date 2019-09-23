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
#include "insdb/db.h"
#include "insdb/status.h"
#include "insdb/comparator.h"
#include "db/insdb_internal.h"
#include "db/skiplist.h"
#include "db/keyname.h"
#include "db/dbformat.h"
#include "db/keyname.h"
#include "util/coding.h"
namespace insdb {

    void UserKey::InitUserKey(const KeySlice& user_key){
        internal_node_.NoBarrier_Store(NULL);
        flags_ = 0;
        ref_cnt_.store(1, std::memory_order_relaxed);
        if(key_buffer_size_ < user_key.size()){
            free(key_buffer_);
            key_buffer_ = (char*)calloc(1, user_key.size());
            key_buffer_size_ = user_key.size();
        } 
        memcpy(key_buffer_, user_key.data(), user_key.size());
        user_key_ = KeySlice(key_buffer_, user_key.size(), user_key.GetHashValue()); /* key may need to destory ... */
        assert(col_data_);
    }

    ColumnNode* UserKey::GetColumnNodeContainerOf(SimpleLinkedNode *col_node){ return class_container_of(col_node, ColumnNode, col_list_node_); }

    bool UserKey::UserKeyCleanup(Manifest *mf, bool cache_evict){
        bool active = false;
        assert(!IsDirty());
        ColumnNode *col_node = NULL;
        SimpleLinkedNode *col_list_node = NULL;
        for(int i = 0 ; i < kMaxColumnCount ; i++){
            assert(col_data_[i].col_list_size() <= 1);
            if((col_list_node = col_data_[i].col_list_pop()))
            {
                active = true;
                /*If the key is deleted by Key Eviction Thread, It can happen */
                col_node = GetColumnNodeContainerOf(col_list_node);
                col_node->InvalidateUserValue(mf);
                /* it may not need cleanup_kbm flag because if it is called from DeviceFormatBuilder(), the UserKey should have no columns*/
                assert(!col_node->HasUserValue());
                KeyBlockMeta* kbm = col_node->GetKeyBlockMeta();
                if(kbm){
#ifndef NDEBUG
                    if(kbm->GetKeyBlockMetaType() == kKeyBlockPrimary) {
                        auto kbpm = (KeyBlockPrimaryMeta *)kbm;
                        if(!kbpm->IsDummy()) {
                            if(kbpm->IsDeleteOnly())
                                assert(!kbpm->GetOriginalKeyCount());
                            else
                                assert(kbpm->GetOriginalKeyCount());
                        }
                    } else {
                        auto kbml = (KeyBlockMetaLog *)kbm;
                        auto kbpm = kbml->GetKBPM();
                        if(kbpm) {
                            if(kbpm->IsDeleteOnly())
                                assert(!kbml->GetOriginalKeyCount());
                            else
                                assert(kbml->GetOriginalKeyCount());
                        }
                    }
#endif
                    KeyBlockPrimaryMeta* kbpm =  kbm->GetKBPM();
                    /* force clear reference */
                    if (cache_evict) kbpm->ClearRefBitmap(col_node->GetKeyBlockOffset());
                    if(kbpm->DecRefCntAndTryEviction(mf, cache_evict))
                        mf->FreeKBPM(kbpm);
                }
                delete col_node;
            }
        }
        user_key_ = KeySlice(0);
        return active;
    }

    void UserKey::FillIterScratchPad(Manifest *mf, IterScratchPad* iter_pad, char*& key_buffer_loc, const KeySlice* key, ColumnNode* col_node, uint8_t col_id){

        int cur_comp; 
        SequenceNumber ikey = 0;
        uint32_t ivalue_size = 0;
        uint8_t kb_offset;
        KeySlice& key_slice = user_key_;

        /* check and extend key buffer */
        uint32_t key_buffer_offset = (uint32_t)(key_buffer_loc - iter_pad->key_buffer_);
        if (iter_pad->key_buffer_size_ <= (key_buffer_offset + kKeyEntrySize /* 8B */)) {
            iter_pad->key_buffer_size_ = mf->IncMaxIterKeyBufferSize();
            iter_pad->key_buffer_ = (char*)realloc(iter_pad->key_buffer_, iter_pad->key_buffer_size_);
            key_buffer_loc = iter_pad->key_buffer_ + key_buffer_offset;
        }
        ColumnLock(col_id);
        UserValue* uv = col_node->GetUserValue();
        if(uv){
            uv->Pin();
            ColumnUnlock(col_id);
            iter_pad->uv_queue_.push(uv);//For free(FreeUserValue())
            auto cur_key_entry = (KeyBufferEntry*)key_buffer_loc;
            cur_key_entry->uv_kb = (uint64_t)((((uint64_t)key_slice.size()) << kKeySizeOffsetShift) | (uint64_t)uv | kKeySizeOffsetType);
            cur_key_entry->key = 0;
#ifdef CODE_TRACE
            printf("ukey .... userkey entry 0x%lx\n", *(uint64_t*)key_buffer_loc);
#endif
            key_buffer_loc += kUserValueAddrOrKBPMSize;/*8B*/

        }else{
            ikey = col_node->GetInSDBKeySeqNum();
            ivalue_size = col_node->GetiValueSize();
            kb_offset = col_node->GetKeyBlockOffset();
            ColumnUnlock(col_id);

            ////////////////////// Prefetch KB //////////////////////
            KeyBlockPrimaryMeta* kbpm = mf->GetKBPM(col_node);
            KeyBlock *kb;
            if (!(kb = kbpm->GetKeyBlockAddr()) && !kbpm->IsKBPrefetched()) {
                kbpm->Lock();
                if (!(kb = kbpm->GetKeyBlockAddr()) && !kbpm->IsKBPrefetched()) {
                    InSDBKey kbkey = GetKBKey(mf->GetDBHash(), kKBlock, ikey);
                    int buffer_size = SizeAlignment(ivalue_size);
                    Env::Default()->Get(kbkey, NULL, &buffer_size, kReadahead);
                    kbpm->SetKBPrefetched();
                }
                kbpm->Unlock();
            }
            /////////////////////////////////////////////////////////
            auto cur_key_entry = (KeyBufferEntry*)key_buffer_loc;
            cur_key_entry->uv_kb = (uint64_t)((((uint64_t)kb_offset) << kKeySizeOffsetShift) | (uint64_t)kbpm);
            cur_key_entry->key = 0;
#ifdef CODE_TRACE
            printf("ukey ...kbpm entry 0x%lx\n", *(uint64_t*)key_buffer_loc);
#endif
            key_buffer_loc+=kUserValueAddrOrKBPMSize;/*8B*/
            iter_pad->kbpm_queue_.push(kbpm);//For dec reference for kbpm
        }
        iter_pad->key_entry_count_++;
    }
    UserKey* UserKey::NextUserKey(Manifest *mf) {
        UserKey* uk;
        SKTableMem *table = mf->FindSKTableMem(GetKeySlice());
        SKTableMem* next_skt = NULL;
        //table->IncSKTRefCnt();
        assert(table);
        table->LoadSKTable(mf);
        do{
            /* Must be fetched for key search. check per every operation because it can be evicted while do this */
            if(!(uk = table->GetKeyMap()->Next(this)))
                goto done;
            uk = mf->IncreaseUserKeyReference(uk);
            if(uk) uk = uk->IsSKTableEvictionInProgress();

            if(uk){
                bool load = false;
                next_skt = mf->Next(table);
                if(uk->Compare(next_skt->GetBeginKeySlice(), mf) >= 0){
                    load = true;
                    table = next_skt;
                }
                if(load){

                    /* the next key belongs to next SKTable */
                    table->LoadSKTable(mf);
                    /* if SKTable has been changed during prev() the prev key must exist.*/
                    uk->DecreaseReferenceCount();
                    uk = NULL;
                }
            }

        }while(!uk);
        //table->DecSKTRefCnt();
done:
#if 0
        if(mf->CheckKeyEvictionThreshold() && (mf->GetCountSKTableCleanList() > mf->GetCountSKTableDirtyList()) && (mf->GetCountSKTableCleanList()/4))
            mf->SignalKeyCacheThread();
#endif
        return uk;

    }

    int UserKey::Compare(const Slice& key, Manifest *mf){
        return mf->GetComparator()->Compare(GetKeySlice(), key); 
    }

    /* It is called with lock */
    /*return true if col_node exists in the seq but the col_node was deleted*/
     ColumnNode* UserKey::GetColumnNode(uint16_t col_id, bool& deleted, uint64_t cur_time, SequenceNumber seq_num, uint64_t ttl) {
        assert(col_id < kMaxColumnCount);
        /* Find in latest column node without the list lock()*/
        ColumnNode* col_node = col_data_[col_id].latest_col_node_;
        if(!col_node){//It mean there is no ColumnNode in the list.
#ifdef CODE_TRACE
            SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
            printf("[%d :: %s] no latest(col_list->size() :%ld)\n", __LINE__, __func__, col_list->Size());
#endif
            return NULL;
        }
        if (col_node->GetBeginSequenceNumber() <= seq_num && ((col_node->GetEndSequenceNumber() == ULONG_MAX) || (seq_num < col_node->GetEndSequenceNumber())|| (col_node->GetRequestType() == kDelType))) {
            if (col_node->IsDropped() || (ttl && (col_node->GetTS() + ttl < cur_time)) || (col_node->GetRequestType() == kDelType)){

#ifdef CODE_TRACE
                printf("[%d :: %s]latest(target) is delete command\n", __LINE__, __func__);
#endif
                deleted=true;
                return NULL;
            }
#ifdef CODE_TRACE
            SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
            printf("[%d :: %s]target cmd is latest(col_list->size() :%ld)\n", __LINE__, __func__, col_list->Size());
#endif
            return col_node;
        }
        /* if latest is not the one, try to find in the list */
        ColumnListLock(col_id);
        if (!col_data_[col_id].latest_col_node_) {
            assert(col_data_[col_id].col_list_.empty());
            ColumnListUnlock(col_id);
            return NULL;
        }
        /* New Key is always inserted with column lock, the we can skip first one which is same to latest_col_node_*/
        SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
        SimpleLinkedNode* col_node_node = col_list->newest();
        while((col_node_node = col_list->next(col_node_node))) {
            col_node = GetColumnNodeContainerOf(col_node_node);
            assert(col_node);
            if (col_node->GetBeginSequenceNumber() <= seq_num && ((col_node->GetEndSequenceNumber() == ULONG_MAX) || (seq_num < col_node->GetEndSequenceNumber())|| (col_node->GetRequestType() == kDelType))) {
                if (col_node->IsDropped() || (ttl && (col_node->GetTS() + ttl < cur_time)) || (col_node->GetRequestType() == kDelType)){
                    ColumnListUnlock(col_id);
#ifdef CODE_TRACE
                    printf("[%d :: %s]target cmd is delete command\n", __LINE__, __func__);
#endif
                    deleted=true;
                    return NULL;
                }
#ifdef CODE_TRACE
                printf("[%d :: %s]target cmd is not latest\n", __LINE__, __func__);
#endif
                ColumnListUnlock(col_id);
                return col_node;
            }
        } 
        ColumnListUnlock(col_id);
#ifdef CODE_TRACE
        printf("[%d :: %s]no target col exists\n", __LINE__, __func__);
#endif
        return NULL;
    }

    /*  
     * If the last submitted ikey exists, return the ikey in order to reuse the InSDB key.
     * If the InSDB key can be reused, the new request could be merged in DD if the previous IK exists in pending Q in DD 
     *  - Discard new request(do not send update request to device) : both(new & submitted ikey) are del and not part of trxn
     *  - Reuse submitted ikey's InSDB key : the submitted ikey is not a part of trxn or the trxn has been committed && no trxn exists in the unsubmitted ikey list
     *  return true  : discard new request because old & new are del cmd & no part of trxn. 
     *  return false : need to submit
     */
    void UserKey::DoVerticalMerge(RequestNode *req_node, uint16_t col_id, uint8_t worker_id, uint8_t idx, Manifest *mf, KeyBlockMetaLog* kbml){
        ColumnNode *col_node = req_node->GetRequestNodeLatestColumnNode();
        assert(col_node);
        SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
        int32_t snap_index = 0;
        std::vector<SnapInfo> snapinfo_list;
        snapinfo_list.clear();
        mf->GetSnapshotInfo(&snapinfo_list);
        int32_t nr_snap = snapinfo_list.size();
        Transaction trxn = Transaction();
        TransactionID smallest_trxn_id = 0;

        assert(col_node);
        assert(col_node->GetInSDBKeySeqNum() == worker_id);

        while(snap_index < nr_snap && col_node->GetBeginSequenceNumber() <= snapinfo_list[snap_index].cseq ){
            ++snap_index;
        }
#if 0
        struct TrxnInfo{
            TrxnInfo(): head(), merged_list(){}
            Transaction head;
            std::vector<Transaction> merged_list;
        };
#endif
        RequestType type = col_node->GetRequestType();
        if(col_node->IsTransaction()){
            col_node->ClearTransaction();
            trxn = req_node->GetTrxn();
            req_node->SetTrxnGroupID(trxn.id >> TGID_SHIFT);
        }
        kbml->SetHeadTrxn(GetKeySlice(), col_id, type, idx, trxn);
        smallest_trxn_id = trxn.id;//doesn't matter whether it is trxn or non-trxn.

        bool iter;
        ColumnListLock(col_id);
        SimpleLinkedNode* col_list_node = col_node->GetColumnNodeNode();
        /* Start Iterator from latest_col_node_'s next(old) column */
        int i = 0;
        for(col_list_node = col_list->next(col_list_node); col_list_node ; ) {
            col_node = GetColumnNodeContainerOf(col_list_node);
            assert(col_node);
            /*
             * col_node can be deleted.(col_list_node exists in col_node ).
             * Therefore, keep next col_list_node first.
             */
            iter = false;

            /*the col_node can be deleted so get next_node before check current col_node*/
            col_list_node = col_list->next(col_list_node);
            /* 
             * WorkerThread only set latest_col_node->ikey_seq to worker_id
             * And the mergeable ColumnNode(to latest_col_node) must have "0" ikey_seq.
             * If it is not 0, that means the col_node has been submitted or an other thread is submitting this col_node.
             */
            /*
             * For merged column, we does not set ikey_seq_num to worker_id
             */
            if(col_node->GetInSDBKeySeqNum() || col_node->IsTGIDSplit()){
                col_node->ClearTGIDSplit();//If TGIDSplit has been set, it means this col_node belongs to another TRXN Group that is not submitted yet.
                break;
            }
            while(snap_index < nr_snap && col_node->GetBeginSequenceNumber() <= snapinfo_list[snap_index].cseq ){
                if(col_node->GetEndSequenceNumber() > snapinfo_list[snap_index].cseq && col_node->GetRequestType() == kPutType){
                    iter  = true;
                }
                ++snap_index;
            }

            /*
             * We need to consider following conditions.(not implemented yet)
             *
             if (col_node->IsDropped() || (col_node->GetTTL() && col_node->GetTTL()  <=  cur_time) || (col_node->GetRequestType() == kDelType)){
             break;
             }
             */
            if(col_node->IsTransaction()){
                col_node->ClearTransaction();
                /* 
                 * Pop the Transaction from RequestNode 
                 * ColumnNode & Transacion(in RequestNode) have same order. 
                 * Order is not important. but number of request & number of trxn in req_node can be differnet.
                 * So. pop a trxn only for "trxn request"
                 */
                trxn = req_node->GetTrxn();
                if(!smallest_trxn_id || smallest_trxn_id > trxn.id)
                    smallest_trxn_id = trxn.id;
                kbml->InsertMergedTrxn(idx, type, trxn, i++);
                /*
                 * We can decrease Trxn Group Count here(Not in FlushSKTableMem()).
                 * because it has same trxn group ID to header's trxn group ID.
                 * It means the this decreament can not make count 0 because of header.
                 */
                mf->DecTrxnGroup(trxn.id >> TGID_SHIFT, 1);
            }
            if(!iter){
                col_list->Delete(col_node->GetColumnNodeNode());
                if(col_node->GetRequestType() == kPutType){
                    /* Do not call InvalidateUserValue() because it has not been added to the cache */
                    assert(col_node->GetUserValue());
                    col_node->InvalidateUserValue(mf);
                }
                delete col_node;
#ifdef INSDB_GLOBAL_STATS
                g_del_ikey_cnt++;
#endif
            }else{
                /*
                 * the col_node belongs to one of the iterator. but it is already invaliated.
                 * So, we don't need to save it because the SKTable can not evicted.
                 */
                assert(col_node->GetUserValue());
                col_node->SetInSDBKeySeqNum(ULONG_MAX);
                /* 
                 * The vertical merged nodes can be deleted even if the TRXN Group is not completed.
                 * Following bit will be checked in SKTableMem::DeviceFormatBuilder() and calls CheckColumnStatus() 
                 */
                col_node->SetTRXNCommitted();
            }
        } 
        ColumnListUnlock(col_id);
        kbml->InsertSmallestTrxn(idx, type, smallest_trxn_id);
        return;
    }

    /* 
     * For new design(packing), we should assume that the caller can try to insert same key into the col list 
     * TODO : WE HAVE TO HANDLE THIS SITUATION.  I'll do it in near future work . 
     */

    InsertColumnNodeStatus UserKey::InsertColumnNode(ColumnNode* new_col_node, uint16_t col_id)
    {
        InsertColumnNodeStatus ret = kSuccessInsert;
        SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
        ColumnListLock(col_id);
        ColumnNode* old_col_node = NULL;
        SimpleLinkedNode* col_node_node = NULL;
        assert(new_col_node->GetInSDBKeySeqNum());/* Idempotent */
        assert(new_col_node->GetRequestType() == kPutType);
        if((col_node_node = col_list->oldest())){
            old_col_node = GetColumnNodeContainerOf(col_node_node);
            if (old_col_node->GetInSDBKeySeqNum() == new_col_node->GetInSDBKeySeqNum() && old_col_node->GetKeyBlockOffset() == new_col_node->GetKeyBlockOffset()){
                //printf("SAME KEY EXIST IN INVALID LIST 1\n");
                ColumnListUnlock(col_id);
                return kFailInsert;
            }
            new_col_node->SetEndSequenceNumber(old_col_node->GetBeginSequenceNumber());
            /*Don't need to set dirty. if this column has been updated, it means this column is already dirty*/
            //SetDirty(); 
            col_list->InsertBack(new_col_node->GetColumnNodeNode());
            /* No update current_ikey_ */
            ret = kInsertedToLast;
        }else{
            col_data_[col_id].latest_col_node_ = new_col_node;
            col_list->InsertFront(new_col_node->GetColumnNodeNode());
        }
        /* col_list is empty */
        ColumnListUnlock(col_id);
        return ret;
    }
    /* if the SKTable has not been flushed, discard sequnce number even if it has seq_num */
    void UserKey::InsertColumnNodes(Slice &col_info, bool valid_seq){
        ColumnNode *col_node = NULL;
        uint8_t nr_column = col_info[0];
        col_info.remove_prefix(1);
        uint8_t col_id;
        uint8_t offset;
        uint32_t ivalue_size = 0;
        uint64_t ikey_seq_num = 0;
        uint64_t seq_num = 0;
        uint64_t ttl = 0;
        InsertColumnNodeStatus stat;

        for( int i = 0 ; i < nr_column ; i++){ 
            col_id = col_info[0] & COLUMN_NODE_ID_MASK;
            col_info.remove_prefix(1);
            offset = col_info[0];
            col_info.remove_prefix(1);
            GetVarint64(&col_info, &ikey_seq_num);
            GetVarint32(&col_info, &ivalue_size);
            if(offset & COLUMN_NODE_HAS_SEQ_NUM_BIT){
                GetVarint64(&col_info, &seq_num);
                if(!valid_seq) seq_num = 0;
            }else seq_num = 0;
            if(offset & COLUMN_NODE_HAS_TTL_BIT) GetVarint64(&col_info, &ttl);
            else ttl = 0;
#ifdef CODE_TRACE
            printf("[%d :: %s] seq_num : %lu\n", __LINE__, __func__, seq_num);
#endif
            col_node = new ColumnNode(seq_num, ttl, ikey_seq_num, ivalue_size, offset&COLUMN_NODE_OFFSET_MASK);
            stat = InsertColumnNode(col_node, col_id);

            if (stat == kFailInsert){
                /* Do not need call InvalidateUserValue because it has NO UserValue(it is just created).*/
                delete col_node;
                /* The same ColumnNode must not exist becauese the Key has not been fetched!! */
                //abort();
            }
        }
    }


    InsertColumnNodeStatus  UserKey::UpdateColumn(ColumnNode* new_col_node, uint16_t col_id, bool force, Manifest *mf)
    {
        InsertColumnNodeStatus ret = kNeedToSchedule;
        SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
        ColumnListLock(col_id);
        ColumnNode* old_col_node = NULL;
        SimpleLinkedNode* col_list_node = NULL;
        assert(!new_col_node->GetInSDBKeySeqNum());

        if((col_list_node = col_list->newest())){
            old_col_node = GetColumnNodeContainerOf(col_list_node);
            if(old_col_node->GetRequestType() == kPutType && old_col_node->GetEndSequenceNumber() == ULONG_MAX){
                old_col_node->SetEndSequenceNumber(new_col_node->GetBeginSequenceNumber());
            }
#if 1
            /* Insert regardless of previous key. After that a Worker will determine whether the new_col_node could be discard or not */
            else if (new_col_node->GetRequestType() == kDelType && old_col_node->GetRequestType() == kDelType && !force){
                /* The columne is already deleted */
                ColumnListUnlock(col_id);
                return kFailInsert;
            }
#endif
            /* if old_col_node->GetInSDBKeySeqNum() return 0 which means previous ik have not been submitted yet and the col_list_ should be in pending queue, 
             * return false so that the user do not insert col_list_ into the pending queue. */
            if(!old_col_node->GetInSDBKeySeqNum())
                ret = kHasBeenScheduled;
            else if(old_col_node->GetInSDBKeySeqNum() > kUnusedNextInternalKey){
                // prefetch old column node
                KeyBlockMeta *kbm = old_col_node->GetKeyBlockMeta();
                if(!kbm){
                    kbm = mf->GetKBPM(old_col_node);
                    kbm->DecRefCnt(); 
                }
                if(kbm->IsDummy())
                    kbm->Prefetch(mf);
            }
        }
        if(new_col_node->GetRequestType() == kDelType){
            new_col_node->SetEndSequenceNumber(new_col_node->GetBeginSequenceNumber());
        }
        col_list->InsertFront(new_col_node->GetColumnNodeNode());
        ColumnListUnlock(col_id);
        SetDirty();
        return ret;
    }
    size_t UserKey::GetSize() { return (sizeof(UserKey) + sizeof(ColumnData[kMaxColumnCount]) + key_buffer_size_ + (sizeof(ColumnNode)*kMaxColumnCount));}

} //namespace
