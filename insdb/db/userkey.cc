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
#ifndef CODE_TRACE
    void UserKey::ScanColumnInfo(std::string &str){
        ColumnNode* col_node = NULL;
        SimpleLinkedList* col_list = NULL;
        SimpleLinkedNode* col_list_node = NULL;
//        printf("[%d :: %s] key(%s) key size(%d) uk(%p) \n", __LINE__, __func__, EscapeString(GetKeySlice()).c_str(), GetKeySize(), this);

        ///////////////////////////////////////////////////////////////////////
        for( uint8_t col_id = 0 ; col_id < kMaxColumnCount ; col_id++){
            /*
             * ColumnLock may not be needed 
             * dfb_info->ukey->ColumnLock(col_id);
             */
            ColumnLock(col_id);
            col_list = GetColumnList(col_id);

            col_node = NULL;
            for(SimpleLinkedNode* col_list_node = col_list->newest(); col_list_node ; ) {

                col_node = GetColumnNodeContainerOf(col_list_node);
                /*
                   assert(valid_col_nodes.empty());
                 * col_node can be deleted.(col_list_node exists in col_node ).
                 * Therefore, keep next col_list_node first.
                 */
                col_list_node = col_list->next(col_list_node);

                col_node->PrintColumnNodeInfo(str);
            }

            /**
             * The invalid list in a Column is not empty
             */
            ColumnUnlock(col_id);
        }
        ///////////////////////////////////////////////////////////////////////
    }
#endif
    bool UserKey::BuildColumnInfo(Manifest *mf, uint16_t &size, Slice buffer, SequenceNumber &largest_ikey, std::queue<KeyBlockPrimaryMeta*> &in_log_KBM_queue, bool &has_del){
        bool valid = false;
        ColumnNode* col_node = NULL;
        SimpleLinkedList* col_list = NULL;
        SimpleLinkedNode* col_list_node = NULL;
        std::string *col_info = NULL;
        size = 0;
        uint8_t nr_col = 0;
        char* key_info_data = const_cast<char*>(buffer.data()+1);// reserved for nr col
        uint16_t col_info_size = 1;
        ColumnLock();
        SequenceNumber ikey = GetLatestiKey();
        if(ikey > largest_ikey) largest_ikey = ikey;
        for( uint8_t col_id = 0 ; col_id < kMaxColumnCount ; col_id++){
            col_list = GetColumnList(col_id);
            col_list_node = col_list->newest();
            if(col_list_node) {
                col_node = GetColumnNodeContainerOf(col_list_node);
#ifdef CODE_TRACE
                printf("[%d :: %s]write to keymap col info ikey : %ld offset : %d\n", __LINE__, __func__, col_node->GetInSDBKeySeqNum(), col_node->GetKeyBlockOffset());
#endif
                col_info = col_node->GetColInfo()/*To store col info size*/;
                memcpy(key_info_data, col_info->data(), col_info->size()); /* col_info include col_size */

                /*
                 * If latest col is del, leave it in the column list to delete the column
                 * otherwise(put type), remove from list
                 */
                if(col_node->GetRequestType() != kDelType ){
                    /*
                     * Every ColumnNode must have its' KBPM and the KBPM has ref bit for the col node
                     * So, we need to set ref bit in col info in order to evict KBPM later.
                     */
                    if(!col_node->IsTRXNCommitted()){
#ifndef NDEBUG
                        KeyBlockPrimaryMeta* kbpm = col_node->GetKeyBlockMeta();
                        assert(kbpm); // kbpm must exist becuase it was prefetched in InsertUserKeyToKeyMapFromInactiveKeyQueue()->InsertColumnNodes()
                        /*in-log KBPM can not be dummy*/
                        assert(!kbpm->IsDummy());
#else
                        KeyBlockPrimaryMeta* kbpm = col_node->GetKeyBlockMeta();
                        assert(kbpm);
#endif
                        kbpm->ClearRefBitmap(col_node->GetKeyBlockOffset());
                        in_log_KBM_queue.push(kbpm);
#ifdef CODE_TRACE
                        printf("[%d :: %s];Inserted to Inlog;|;KBPM;%p;|;iKey;%ld;|;offset;%d;|\n", __LINE__, __func__,col_node->GetKeyBlockMeta(), col_node->GetInSDBKeySeqNum(), col_node->GetKeyBlockOffset());
#endif
                    }
                    /* Delete latest column from col list because it is stored in keymap_*/
                    col_list->Delete(col_node->GetColumnNodeNode());
                    col_node->InvalidateUserValue(mf);
                    assert(GetLatestColumnNode(col_id) == col_node);

                    col_list_node = col_list->newest();
                    if(col_list_node) 
                        SetLatestColumnNode(col_id, GetColumnNodeContainerOf(col_list_node));
                    else
                        SetLatestColumnNode(col_id, NULL);
#ifndef NDEBUG
                    if(col_node->GetRequestNode()) {
                        assert(col_node->GetRequestNode()->GetRequestNodeLatestColumnNode() != col_node);
                    }
#endif
                    col_node->SetRequestNode(nullptr);
                    mf->FreeColumnNode(col_node);
                }else{
                    has_del=true;
#ifdef CODE_TRACE
                    printf("[%d :: %s];Not inserted to InLog;|;KBPM;%p;|;iKey;%ld;|;offset;%d;|\n", __LINE__, __func__,col_node->GetKeyBlockMeta(), col_node->GetInSDBKeySeqNum(), col_node->GetKeyBlockOffset());
#endif
                }
                key_info_data+=col_info->size();
                col_info_size += col_info->size();
                nr_col++;
            }

            if(col_list->newest())  valid = true;
        }
        ColumnUnlock();
        assert(nr_col);
        key_info_data = const_cast<char*>(buffer.data());
        *key_info_data = nr_col-1;
        size = col_info_size;
        return valid;
    }

    void UserKey::InitUserKey(const KeySlice& user_key){
        //internal_node_.NoBarrier_Store(NULL);
        ref_cnt_.store(1, std::memory_order_relaxed);
        if (user_key.size() > key_buffer_size_) {
            alloc_->FreeBuf(key_buffer_, key_buffer_size_);
            key_buffer_ = (char*)alloc_->AllocBuf(user_key.size());
            key_buffer_size_ = user_key.size();
        } 
        memcpy(key_buffer_, user_key.data(), user_key.size());
        user_key_ = KeySlice(key_buffer_, user_key.size(), user_key.GetHashValue()); /* key may need to destory ... */
        latest_ikey_ = 0;
        in_hash = true;
        assert(col_data_);
#ifndef NDEBUG
        for(int i = 0 ; i < kMaxColumnCount ; i++){
            assert(!col_data_[i].col_list_size());
        }
#endif
    }

    ColumnNode* UserKey::GetColumnNodeContainerOf(SimpleLinkedNode *col_node){ return class_container_of(col_node, ColumnNode, col_list_node_); }

    int UserKey::Compare(const Slice& key, Manifest *mf){
        return mf->GetComparator()->Compare(GetKeySlice(), key); 
    }

    SequenceNumber UserKey::GetLatestColumnNodeSequenceNumber(uint8_t col_id, bool &deleted) {
        assert(col_id < kMaxColumnCount);
        /* Find in latest column node without the list lock()*/
        ColumnNode* col_node = col_data_[col_id].latest_col_node_;
        //There is no ColumnNode in the list.
        if(!col_node) return 0;
        if (col_node->GetRequestType() == kDelType) deleted = true;
        return col_node->GetBeginSequenceNumber();
    }


    // It is called with lock
    // TTL is ignored.
     SequenceNumber UserKey::GetSmallestSequence() {
        SequenceNumber smallest_seq = kMaxSequenceNumber;

        for(uint16_t col_id = 0 ; col_id < kMaxColumnCount; col_id++) {
            /* Find in latest column node without the list lock()*/
            ColumnNode* col_node = col_data_[col_id].latest_col_node_;
            //There is no ColumnNode in the list.
            if(!col_node) continue;

            SequenceNumber seq = col_node->GetBeginSequenceNumber();
            if (col_node->IsDropped() || (col_node->GetRequestType() == kDelType)){
                continue;
            }

            /* New Key is always inserted with column lock, the we can skip first one which is same to latest_col_node_*/
            SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
            if (col_list->Size() == 1) {
                smallest_seq = seq;
                continue;
            }

            SimpleLinkedNode* col_node_node = col_list->oldest();
            col_node = GetColumnNodeContainerOf(col_node_node);
            smallest_seq = col_node->GetBeginSequenceNumber();
        }

        return smallest_seq;
    }

    /* It is called with lock */
    /*return true if col_node exists in the seq but the col_node was deleted*/
     ColumnNode* UserKey::GetColumnNode(uint16_t col_id, bool& deleted, uint64_t cur_time, SequenceNumber seq_num, uint64_t ttl) {
        assert(col_id < kMaxColumnCount);
        /* Find in latest column node without the list lock()*/
        ColumnNode* col_node = col_data_[col_id].latest_col_node_;
        //There is no ColumnNode in the list.
        if(!col_node) return NULL;

        if (col_node->GetBeginSequenceNumber() <= seq_num ){
            if (col_node->IsDropped() || (ttl && (col_node->GetTS() + ttl <= cur_time)) || (col_node->GetRequestType() == kDelType) || (col_node->GetEndSequenceNumber() != ULONG_MAX && col_node->GetEndSequenceNumber() < seq_num)){

                deleted=true;
                return NULL;
            }
            return col_node;
        }
#if 0
        /* if latest is not the one, try to find in the list */
        if (!col_data_[col_id].latest_col_node_) {
            assert(col_data_[col_id].col_list_.empty());
            return NULL;
        }
#endif
        /* New Key is always inserted with column lock, the we can skip first one which is same to latest_col_node_*/
        SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
        SimpleLinkedNode* col_node_node = col_list->newest();
        while((col_node_node = col_list->next(col_node_node))) {
            col_node = GetColumnNodeContainerOf(col_node_node);
            assert(col_node);
            if (col_node->GetBeginSequenceNumber() <= seq_num){
                if (col_node->IsDropped() || (ttl && (col_node->GetTS() + ttl <= cur_time)) || (col_node->GetRequestType() == kDelType) || (col_node->GetEndSequenceNumber() != ULONG_MAX && col_node->GetEndSequenceNumber() < seq_num)){
                    deleted=true;
                    return NULL;
                }
                return col_node;
            }
        } 
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
    void UserKey::DoVerticalMerge(RequestNode *req_node, UserKey* uk, uint16_t col_id, uint8_t worker_id, uint8_t idx, Manifest *mf, KeyBlockPrimaryMeta* kbpm){
        ColumnNode *col_node = req_node->GetRequestNodeLatestColumnNode();
        assert(col_node);
        SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
        int32_t snap_index = 0;
        std::vector<SnapInfo> snapinfo_list;
        snapinfo_list.clear();
        mf->GetSnapshotInfo(&snapinfo_list);
        int32_t nr_snap = snapinfo_list.size();
        Transaction trxn = Transaction();
        std::string* kbm_df = NULL;
        TrxnGroupID tgid = col_node->GetTGID();

        assert(col_node);

        while(snap_index < nr_snap && col_node->GetBeginSequenceNumber() <= snapinfo_list[snap_index].cseq ){
            ++snap_index;
        }
        RequestType type = col_node->GetRequestType();
        if(col_node->GetTGID() && type == kPutType){
            kbpm->AddCharToDeviceFormat(idx);
            kbpm->AddVarint16ToDeviceFormat(req_node->GetTrxnCount());
            kbpm->AddStringToDeviceFormat(req_node->GetTrxn()->data(), req_node->GetTrxn()->size());
        }else if(type == kDelType){
            KeySlice key = GetKeySlice();
            kbpm->AddCharToDeviceFormat((col_id | TRXN_DEL_REQ));
            kbpm->AddVarint16ToDeviceFormat(key.size());
            kbpm->AddStringToDeviceFormat(key.data(), key.size());
            kbpm->AddVarint64ToDeviceFormat(col_node->GetBeginSequenceNumber());
            kbpm->AddVarint16ToDeviceFormat(req_node->GetTrxnCount());
            if(req_node->GetTrxnCount())
                kbpm->AddStringToDeviceFormat(req_node->GetTrxn()->data(), req_node->GetTrxn()->size());
        }

        bool iter;
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
            if(col_node->GetRequestNode() != req_node){
                break;
            }
            while(snap_index < nr_snap && col_node->GetBeginSequenceNumber() <= snapinfo_list[snap_index].cseq ){
                if(col_node->GetEndSequenceNumber() > snapinfo_list[snap_index].cseq && col_node->GetRequestType() == kPutType){
                    iter  = true;
                }
                ++snap_index;
            }

#if 0
            /*
             * We need to consider following conditions.(not implemented yet)
             */
            if (col_node->IsDropped() || (col_node->GetTTL() && col_node->GetTTL()  <=  cur_time) || (col_node->GetRequestType() == kDelType)){
                break;
            }
#endif
            
            if(tgid){
                /*
                 * We can decrease Trxn Group Count here(Not in FlushSKTableMem()).
                 * because it has same trxn group ID to header's trxn group ID.
                 * It means the this decreament can not make count 0 because of header.
                 */
                mf->DecTrxnGroup(tgid, 1);
            }
            if(!iter){
                col_node->SetRequestNode(nullptr);
                col_list->Delete(col_node->GetColumnNodeNode());
                if(col_node->GetRequestType() == kPutType){
                    /* Do not call InvalidateUserValue() because it has not been added to the cache */
                    assert(col_node->GetUserValue());
                    col_node->InvalidateUserValue(mf);
                }
#ifndef NDEBUG
                if(col_node->GetRequestNode()) {
                    assert(col_node->GetRequestNode()->GetRequestNodeLatestColumnNode() != col_node);
                }
#endif
                col_node->SetRequestNode(nullptr);
                mf->FreeColumnNode(col_node);
            }else{
                /*
                 * the col_node belongs to one of the iterator. but it is already invaliated.
                 * So, we don't need to save it because the SKTable can not evicted.
                 */
                assert(col_node->GetUserValue());
                col_node->SetOverwritten();
                /* 
                 * The vertical merged nodes can be deleted even if the TRXN Group is not completed.
                 * Following bit will be checked in SKTableMem::DeviceFormatBuilder() and calls CheckColumnStatus() 
                 */
                col_node->SetTRXNCommitted();
            }
           DecreaseReferenceCount();
        } 
        return;
    }

    /* insert by table loading
     * For new design(packing), we should assume that the caller can try to insert same key into the col list 
     * TODO : WE HAVE TO HANDLE THIS SITUATION.  I'll do it in near future work . 
     */

    bool UserKey::InsertColumnNode(ColumnNode* input_col_node, uint16_t col_id, bool &ignored, bool front_merge/*For the UserKey in iterator hash*/)
    {
        ignored = false; 
        SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
        ColumnNode* col_node = NULL;
        SimpleLinkedNode* col_node_node = NULL;
        if((col_node_node = front_merge? col_list->newest() : col_list->oldest())){
            col_node = GetColumnNodeContainerOf(col_node_node);
            if(front_merge){
                if (col_node->GetBeginSequenceNumber() >= input_col_node->GetBeginSequenceNumber() )  { 
                    ignored = true; 
                    return false;
                }
                if(col_node->GetEndSequenceNumber() == ULONG_MAX)
                    col_node->SetEndSequenceNumber(input_col_node->GetBeginSequenceNumber());
                SetLatestColumnNode(col_id, input_col_node);
                col_list->InsertFront(input_col_node->GetColumnNodeNode());
            }else{
                if (col_node->GetBeginSequenceNumber() == input_col_node->GetBeginSequenceNumber()) { 
                    ignored = true; 
                    return false; 
                }
                assert(col_node->GetBeginSequenceNumber() > input_col_node->GetBeginSequenceNumber());
                if(input_col_node->GetEndSequenceNumber() == ULONG_MAX)
                    input_col_node->SetEndSequenceNumber(col_node->GetBeginSequenceNumber());
                col_list->InsertBack(input_col_node->GetColumnNodeNode());
            }
            return true;//
            /*Don't need to set dirty. if this column has been updated, it means this column is already dirty*/
            /* No update current_ikey_ */
        }
        SetLatestColumnNode(col_id, input_col_node);
        col_list->InsertFront(input_col_node->GetColumnNodeNode());
        return false;
    }
    /* if the SKTable has not been flushed, discard sequnce number even if it has seq_num */
    bool UserKey::InsertColumnNodes(Manifest *mf, uint8_t nr_column, Slice &col_info, bool front_merge){
        ColumnNode *col_node = NULL;
        uint8_t size;
        uint8_t col_id;
        bool ref;
        bool ret = false;/*old column exists?*/

        for( int i = 0 ; i < nr_column ; i++){ 
            bool del = col_info[0] & DEL_FLAG;
            size = col_info[0] & COL_INFO_SIZE_MASK;
            if(!del || front_merge){
                col_id = col_info[1] & COLUMN_NODE_ID_MASK;
                Slice col_data(col_info.data(), size);

                /* Create ColumnNode & insert the ColumnNode to UserKey */
                col_node = mf->AllocColumnNode();
                col_node->ExistColInit(col_data, this);
                ColumnLock(col_id);
                bool ignore = false;
                if(InsertColumnNode(col_node, col_id, ignore, front_merge)){
                    /* Need to prefetch KBM, because the col_node is updated by new column */
                    assert(!col_node->GetKeyBlockMeta());//the col_node is just inserted. So, it can not have a KBPM.
                    /* search from hash table */
                    KeyBlockPrimaryMeta* kbpm = mf->GetKBPMWithiKey(col_node->GetInSDBKeySeqNum(), col_node->GetKeyBlockOffset());
#ifdef CODE_TRACE
        printf("[%d :: %s]After insert col_info to ColumnNode(kbpm : %p)(ikey:%ld)(offset:%d)\n", __LINE__, __func__, kbpm, kbpm->GetiKeySequenceNumber(), col_node->GetKeyBlockOffset());
#endif
                    assert(kbpm);
                    assert(kbpm->IsRefBitmapSet(col_node->GetKeyBlockOffset()));
                    col_node->SetKeyBlockMeta(kbpm);
                    while(kbpm->SetWriteProtection());
                    if(kbpm->IsDummy()) kbpm->PrefetchKBPM(mf);
                    kbpm->ClearWriteProtection();
                }else
                    ret = true;/* the given column is newly inserted, so that, the size of key info will be increased.*/
                if (ignore) mf->FreeColumnNode(col_node);
                ColumnUnlock(col_id);
            }
            col_info.remove_prefix(size);
        }
        return ret;
    }


    void UserKey::UserKeyMerge(UserKey *old_uk, bool front_merge/*For the UserKey in iterator hash*/){
        ColumnNode* col_node = NULL;
        for( uint8_t col_id = 0 ; col_id < kMaxColumnCount ; col_id++){
            uint32_t ref_cnt = 0;
            bool ignore = false;
            col_node = NULL;
            ColumnLock(col_id);
            old_uk->ColumnLock(col_id);
            auto col_list = old_uk->GetColumnList(col_id);
            for(/* iterate old key */SimpleLinkedNode* col_list_node = front_merge ? col_list->oldest() : col_list->newest(); col_list_node ; ) {
                col_node = old_uk->GetColumnNodeContainerOf(col_list_node);
                col_list_node = front_merge ? col_list->prev(col_list_node) : col_list->next(col_list_node);
                col_list->Delete(col_node->GetColumnNodeNode());
                col_node->UpdateUserKey(this);
                ignore = false;
                InsertColumnNode(col_node, col_id, ignore, front_merge);
                if (ignore) {
#ifndef CODE_TRACE
                printf("[%d :: %s] col_info not merged to ColumnNode(col_node: %p)(ikey:%ld)(offset:%d)\n", __LINE__, __func__, col_node, col_node->GetInSDBKeySeqNum(), col_node->GetKeyBlockOffset());
#endif
                    abort();
                }
                if(!col_node->IsSubmitted() && !col_node->IsOverwritten() && !col_node->IsTRXNCommitted()) ref_cnt++;
            }
            if(col_node && front_merge) 
                SetLatestColumnNode(col_id, col_node); 
            if(ref_cnt){
                old_uk->DecreaseReferenceCount(ref_cnt);
                IncreaseReferenceCount(ref_cnt);
            }
            old_uk->ColumnUnlock(col_id);
            ColumnUnlock(col_id);
        }
    }

    /* if the SKTable has not been flushed, discard sequnce number even if it has seq_num */

    bool UserKey::UpdateColumn(Manifest *mf, RequestNode*& req_node, ColumnNode* new_col_node, uint16_t col_id, bool force, TrxnGroupID &old_tgid)
    {
        SimpleLinkedList* col_list = &(col_data_[col_id].col_list_);
        ColumnNode* old_col_node = NULL;
        SimpleLinkedNode* col_list_node = NULL;
        assert(new_col_node->IsPendingRequest());

        if((col_list_node = col_list->newest())){
            old_col_node = GetColumnNodeContainerOf(col_list_node);
            if(old_col_node->GetEndSequenceNumber() == ULONG_MAX)
                old_col_node->SetEndSequenceNumber(new_col_node->GetBeginSequenceNumber());

#if 1
            /* Insert regardless of previous key. After that a Worker will determine whether the new_col_node could be discard or not */
            if (new_col_node->GetRequestType() == kDelType && old_col_node->GetRequestType() == kDelType && !force){
                /* The columne is already deleted */
                return false;
            }
#endif
            /* if old_col_node->GetInSDBKeySeqNum() return 0 which means previous ik have not been submitted yet and the col_list_ should be in pending queue, 
             * return false so that the user do not insert col_list_ into the pending queue. */
            if(old_col_node->IsPendingRequest()){
                /* If priv col has not been submitted, try to merge new col with priv col */
                req_node = old_col_node->GetRequestNode();
                old_tgid = old_col_node->GetTGID();
            }
#ifndef NDEBUG
            /* 
             * The UserKey only has new col node(can not fetch the col from keymap_ )
             * Because the UserKey is moved from unsorted key queue to keymap_ and the UserKey is deleted from the hash.
             */
            else if(old_col_node->IsSubmitted()){
                assert(old_col_node->GetKeyBlockMeta());
                assert(!(old_col_node->GetKeyBlockMeta()->IsDummy()));
            }/*Otherwise, Original KBM exists*/
#endif
        }
        col_list->InsertFront(new_col_node->GetColumnNodeNode());
        SetLatestColumnNode(col_id, new_col_node);
        //SetDirty();
        return true;
    }
    size_t UserKey::GetSize() { return (sizeof(UserKey) + sizeof(ColumnData[kMaxColumnCount]) + key_buffer_size_ + (sizeof(ColumnNode)*kMaxColumnCount));}
    SequenceNumber  ColumnNode::GetBeginSequenceNumber() {
        SequenceNumber seq = 0;
        Slice col_info(col_info_);
        col_info.remove_prefix(SEQNUM_LOC);
        GetVarint64(&col_info, &seq);
        return seq;
    }

    void ColumnNode::UpdateColumnNode(Manifest *mf, uint32_t kb_size, uint64_t ikey_seq, KeyBlockPrimaryMeta *kbpm){

        assert(kb_size < kDeviceRquestMaxSize);

        PutVarint64(&col_info_, ikey_seq);
        if(!(col_info_[0] & DEL_FLAG)){
#ifdef CODE_TRACE
            char tmpkey[17];
            strncpy(tmpkey, (GetUserKey()->GetKeySlice()).data() , (GetUserKey()->GetKeySlice()).size());
            tmpkey[(GetUserKey())->GetKeySlice().size()] = '\0';
            printf("[%d :: %s][put(%s)]offset : %d || ikey_seq : %ld || kb_size : %d\n", __LINE__, __func__, tmpkey, *(col_info_.data()+2), ikey_seq, kb_size);
#endif
            PutVarint32(&col_info_, kb_size);
            col_info_[0] = (uint8_t)col_info_.size();
        }else{
            col_info_[0] = (uint8_t)(col_info_.size() | DEL_FLAG);
#ifdef CODE_TRACE
            char tmpkey[17];
            strncpy(tmpkey, (GetUserKey()->GetKeySlice()).data() , (GetUserKey()->GetKeySlice()).size());
            tmpkey[(GetUserKey())->GetKeySlice().size()] = '\0';
            printf("[%d :: %s][del(%s)], offset : %d || ikey_seq : %ld || kb_size : %d\n", __LINE__, __func__, tmpkey, *(col_info_.data()+2), ikey_seq, kb_size);
#endif
        }

        UserValue* uv = GetUserValue();
        assert(uv || GetRequestType() == kDelType);
        /* Delete Command did not create a UserValue */
        /* Offest was already set in BuildKeyBlock(Meta)()*/
        uvalue_or_kbm_ =  ((uint64_t)kbpm);
        req_node_ = nullptr;
#ifdef CODE_TRACE
        std::string str("AFTER KB SUBMIT");
        PrintColumnNodeInfo(str);
#endif
        /* Delete Command did not create a UserValue */
        if(uv) mf->FreeUserValue(uv);
    }
    void ColumnNode::PrintColumnNodeInfo(std::string &str){
        printf("[%d : %s];%s;|;key;%s;|;key size;%d;|;uk;%p;|;%s;|;Seq;%ld;~;%ld;|;iKey;%ld;|;KB size;%u;|;offset;%u;|;%s;0x%lx|\n", __LINE__, __func__, str.c_str(), EscapeString(GetUserKey()->GetKeySlice()).c_str(), GetUserKey()->GetKeySize(), GetUserKey(), GetRequestType()==kPutType?"Put" : "Del", GetBeginSequenceNumber(), GetEndSequenceNumber(), GetInSDBKeySeqNum(), GetiValueSize(),  GetKeyBlockOffset(), HasUserValue()?"UVADDR : " : "KBPM ADDR : ", uvalue_or_kbm_ & ADDRESS_MASK);
    }


                            } //namespace
