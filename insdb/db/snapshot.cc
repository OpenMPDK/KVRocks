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


#include <vector>
#include "db/dbformat.h"
#include "db/insdb_internal.h"
#include "insdb/db.h"
#include "insdb/comparator.h"
#include "insdb/env.h"

namespace insdb {
    /**
      build Iterator for this Snapshot.
      InSDB use same mechanism for Iterator and Sanpshot. So it shares
      smae class.
      */
    SnapshotImpl::~SnapshotImpl() {
        assert(iterators_.size() == 0);
        IterScratchPad *pad = NULL;
        for(int i = 0 ; i < kMaxColumnCount ; i ++){
            std::unordered_map<uint32_t, IterScratchPad*> *iter_pad_map = &iter_pad_map_[i];
            for(auto it = iter_pad_map->begin() ; it!= iter_pad_map->end() ;){
                pad = it->second;
                it = iter_pad_map->erase(it);
                if(pad->DecIterScratchPadRef()){
                    pad->ClearIterScratchpad(mf_);
                    mf_->FreeIterScratchPad(pad);
                }
            }
        }
        if(iter_pad_lock_) delete [] iter_pad_lock_;
        if(iter_pad_map_) delete [] iter_pad_map_;
    }

    void SnapshotImpl::RemoveIterator(SnapshotIterator* iter) {
        mu_.Lock();
        iterators_.remove(iter);
        mu_.Unlock();
    }

    Iterator* SnapshotImpl::CreateNewIterator(uint16_t col_id) {
        SnapshotIterator* iter = new SnapshotIterator(mf_, this, col_id);
        mu_.Lock();
        iterators_.push_back(iter);
        mu_.Unlock();
        return reinterpret_cast<Iterator*>(iter); 
    }
    void SnapshotIterator::ReturnIterScratchPad(IterScratchPad*& iter_pad){
        iter_pad->DecActiveIterScratchPadRef();
        iter_pad->SetLastAccessTimeSec(mf_->GetEnv()->NowSecond());
        if(iter_pad->DecIterScratchPadRef()){
            iter_pad->ClearIterScratchpad(mf_);
            mf_->FreeIterScratchPad(iter_pad);
        }else if(iter_pad->skt_) ;
        iter_pad = NULL;
        cur_info_.offset = NULL;
    }

    SnapshotIterator::~SnapshotIterator() {
        if(iter_pad_) ReturnIterScratchPad(iter_pad_);
        snap_->RemoveIterator(this);
        if(mf_->__DisconnectSnapshot(snap_))
            delete snap_;
        mf_->DecIteratorCount();
    }


    int SnapshotIterator::Compare(const Slice& a, const Slice& b) {
        if (a.size() == 0 || b.size() == 0) {
            if (a.size() == 0 && b.size() == 0) return 0;
            if (a.size() == 0) return -1;
            if (b.size() == 0) return 1;
        }
        return comparator_->Compare(a, b);
    }

    bool SnapshotIterator::IsValidKeyRange(const Slice& key) {
        if (start_key_.size() && Compare(key, start_key_) < 0)  return false;
        if (last_key_.size() && Compare(key, last_key_) > 0) return false;
        return true;
    }


    bool SnapshotIterator::Valid() const {
        return cur_info_.offset != NULL;
    }
    Slice SnapshotIterator::GetKey(KeyBufferEntry *cur_offset) {
        if (cur_offset->key) {
            return Slice((char *)(cur_offset->key&0xffffffffffff), cur_offset->key>>48);
        }
        if (cur_offset->uv_kb & kKeySizeOffsetType) {
            /* uv */
            uint16_t key_size = (uint16_t)((cur_offset->uv_kb & kKeySizeOffsetMask) >> kKeySizeOffsetShift);
            UserValue *uv = (UserValue *)(cur_offset->uv_kb & kUserValueAddrOrKBPMMask);
            assert(uv);
            Slice key = uv->GetKeySlice(key_size);
            cur_offset->key = (uint64_t)key.data() | ((key.size())<<48);
            return key;
        } 
        /* kvpm */
        KeyBlock* kb = NULL;
        bool ra_hit = false;
        uint16_t kb_offset = (uint16_t)((cur_offset->uv_kb & kKeySizeOffsetMask) >> kKeySizeOffsetShift);
        KeyBlockPrimaryMeta *kbpm = (KeyBlockPrimaryMeta *)(cur_offset->uv_kb & kUserValueAddrOrKBPMMask);
        if (!(kb = kbpm->GetKeyBlockAddr())) {
            kbpm->Lock();
            if (!(kb = kbpm->GetKeyBlockAddr())){
                kb = mf_->LoadValue(kbpm->GetiKeySequenceNumber(), kbpm->GetKBSize(), &ra_hit, kbpm);
                kbpm->SetKeyBlockAddr(kb);
            }
            kbpm->Unlock();
        }
        Slice key = kb->GetKeyFromKeyBlock(mf_, kb_offset);
        cur_offset->key = (uint64_t)key.data() | ((key.size())<<48);
        return key;
    }

    void SnapshotIterator::SetCurrentKeyInfo(const Slice key, IterScratchPad* iter_pad, bool search_less_than){

        //////////////////////// Binary Search /////////////////////////

        uint32_t left = 0;
        uint32_t right = iter_pad->key_entry_count_;
        uint32_t mid = 0;
        KeyBufferEntry *cur_offset = NULL;
        auto key_buffer = (KeyBufferEntry*)iter_pad->key_buffer_;
        int comp = 0;
        while (left < right) {
            mid = (left + right) / 2;
            cur_offset = (key_buffer + mid);
            comp = Compare(key, GetKey(cur_offset));
            if( 0 < comp ) {
                // sktdf_hash at "sktdf_hash[mid]" is < "hash".  
                // Therefore all hash values at or before "mid" are uninteresting.
                left = mid + 1;
            }else if ( comp < 0) {
                // sktdf_hash at "sktdf_hash[mid]" is < "hash".  
                // Therefore all hash values after "mid" are uninteresting.
                right = mid;
            }else/*( sktdf_hash == hash)*/{
                cur_info_.offset = cur_offset;
                return;
            }
        }
        if(search_less_than){
            if(0 < comp){
                cur_info_.offset = cur_offset;
            }else{
                if(cur_offset != key_buffer)
                    cur_info_.offset = cur_offset-1;
            }
        }else{
            if(0 > comp){
                cur_info_.offset = cur_offset;
            }else{
                if(cur_offset != key_buffer +(iter_pad->key_entry_count_-1))
                    cur_info_.offset = cur_offset+1;
            }

        }


        /* Need to find the key in Prev/Next SKTable */
        return;
    }
#define MAX_SKTABLE_PREFETCH_CNT 8
    SKTableMem* SnapshotIterator::SingleSKTablePrefetch(SKTableMem *last_prefetched_skt, PrefetchDirection dir){
        SKTableMem *skt = last_prefetched_skt;
        if(dir==kNext) skt = mf_->Next(skt);
        else skt = mf_->Prev(skt);
        if(skt && !skt->GetSKTableDeviceFormatNode() && !skt->IsFetchInProgress()) 
            skt->PrefetchSKTable(mf_, false/*To clean cache*/);
        if(dir==kNext){
            next_prefetch_skt_ = skt;
        }else{
            prev_prefetch_skt_ = skt;
        }
        return skt;
    }

    SKTableMem* SnapshotIterator::SKTablePrefetch(SKTableMem *cur_skt, PrefetchDirection dir){
        SKTableMem *skt = cur_skt;
        uint16_t max_skt_prefetch = MAX_SKTABLE_PREFETCH_CNT;
        while(skt && max_skt_prefetch--){
            if(dir==kNext) skt = mf_->Next(skt);
            else skt = mf_->Prev(skt);
            if(skt && !skt->GetSKTableDeviceFormatNode() && !skt->IsFetchInProgress()) 
                skt->PrefetchSKTable(mf_, false/*To clean cache*/);
        }
        /* Prefetch only one SKTableMem in opposite direction. */
        if(dir==kNext){
            next_prefetch_skt_ = skt;
//            skt = mf_->Prev(cur_skt);
//            prev_prefetch_skt_ = skt;
        }else{
            prev_prefetch_skt_ = skt;
//            skt = mf_->Next(cur_skt);
//            next_prefetch_skt_ = skt;
        }
//        if(skt && !skt->GetSKTableDeviceFormatNode() && !skt->IsFetchInProgress()) 
//            skt->PrefetchSKTable(mf_, false/*To clean cache*/);
        return skt;
    }

    IterScratchPad *SnapshotIterator::SeekInternal(const KeySlice* key, SKTableMem*& skt, bool search_less_than/*or last(true)/first(false) key*/){
        /* Call this function only when the IterScratchPad does not exist */
        IterScratchPad* iter_pad = NULL;
        bool need_to_insert = false;
        cur_info_.InitIterCurrentKeyInfo();
        while(!cur_info_.offset){
retry:
            skt->BuildIterLock();
            skt->IterLock();
            if(!(iter_pad = skt->GetLatestIterPad(mf_, snap_->GetCurrentSequenceNumber(),column_id_))){
                skt->IterUnlock();
                /* If SKT does not have target pad */
                iter_pad = mf_->AllocIterScratchPad(mf_->GetMaxIterKeyBufferSize());/*Defalut is 4KB(kMinSKTableSize)*/
                /*Increase ref_cnt of iter_pad in InitIterScratchPad()*/
                iter_pad->InitIterScratchPad(mf_->GetMaxIterKeyBufferSize());
                cur_info_.InitIterCurrentKeyInfo();
                need_to_insert = skt->BuildIteratorScratchPad(mf_, iter_pad, key, column_id_, snap_->GetCurrentTime(), snap_->GetCurrentSequenceNumber());
                skt->BuildIterUnlock();
                iter_pad->IncActiveIterScratchPadRef();
                auto *key_buffer = (KeyBufferEntry*)iter_pad->key_buffer_;
                if(iter_pad->key_entry_count_){
                    if(key){
                        SetCurrentKeyInfo(Slice(key->data(), key->size()), iter_pad, search_less_than);
                    }else if(search_less_than){
                        cur_info_.offset = key_buffer + (iter_pad->key_entry_count_-1);
                    }else{
                        cur_info_.offset = key_buffer;
                    }
                }else
                    ReturnIterScratchPad(iter_pad);

#ifdef CODE_TRACE
                printf("[%d :: %s]Build New\n", __LINE__, __func__);
#endif

            }else{
#ifdef CODE_TRACE
                printf("[%d :: %s]IterScratchPad From SKTableMem\n", __LINE__, __func__);
#endif
                skt->IterUnlock();
                skt->BuildIterUnlock();
                skt->IncSKTRefCnt();
                if(skt->IsFlushing()){
                    skt->DecSKTRefCnt();
                    ReturnIterScratchPad(iter_pad);
                    goto retry;
                }
                skt->DecSKTRefCnt();
                need_to_insert = true;
                /*ref_cnt of iter_pad was increased in GetLatestIterPad()*/
                cur_info_.InitIterCurrentKeyInfo();
                auto *key_buffer = (KeyBufferEntry*)iter_pad->key_buffer_;
                if(key){
                    SetCurrentKeyInfo(Slice(key->data(), key->size()), iter_pad, search_less_than);
                }else if(search_less_than){
                    cur_info_.offset = key_buffer + (iter_pad->key_entry_count_-1);
                }else{
                    cur_info_.offset = key_buffer;
                }
            }
            /* 
             * If another iterator in the snap_ inserted IterScratchPad with same SKT ID,  this Insertion may fail 
             * If fail, use created iter_pad and free after using.
             *
             *       if(pad->DecIterScratchPadRef()){
             *           pad->ClearIterScratchpad(mf_);
             *           mf_->FreeIterScratchPad(pad);
             *       }
             */
            if(iter_pad && iter_pad->use_org_sktdf_ && need_to_insert && iter_pad->key_entry_count_)
                snap_->InsertIterScratchPad(iter_pad, column_id_);
            //pad->DecIterScratchPadRef();


            if(!cur_info_.offset){
                /*
                 * If pad has been created successfully even if offset is null, the pad has been inserted to SKTableMem & snap->iter_pad_map_ 
                 * In this case, ref_cnt could not be 0.
                 */
                if(iter_pad) ReturnIterScratchPad(iter_pad);
                /*The SKTable does not have valid column(seq&ttl)*/
                if(search_less_than){
                    if(prev_prefetch_skt_)
                        SingleSKTablePrefetch(prev_prefetch_skt_, kPrev);
                    skt = mf_->Prev(skt);
                }
                else{
                    if(next_prefetch_skt_)
                        SingleSKTablePrefetch(next_prefetch_skt_, kNext);
                    skt = mf_->Next(skt);
                }
                if(!skt) return NULL;
            }
        }
        return iter_pad;
    }

    void SnapshotIterator::KeyBlockPrefetch(SKTableMem* cur_skt, bool search_less_than){

        if (!search_less_than){
            bool incache = true;
            if(!next_kb_prefetch_skt_ || cur_skt == next_kb_prefetch_skt_)
                next_kb_prefetch_skt_ = mf_->Next(cur_skt);
            while(next_kb_prefetch_skt_ && next_kb_prefetch_skt_ != next_prefetch_skt_ && incache){
                //printf ("KeyBlockPrefetch next %p\n", next_kb_prefetch_skt_);
                next_kb_prefetch_skt_->IncSKTRefCnt();
                incache = next_kb_prefetch_skt_->PrefetchKeyBlocks(mf_, column_id_, snap_->GetCurrentTime(), snap_->GetCurrentSequenceNumber(), kNext);
                next_kb_prefetch_skt_->DecSKTRefCnt();
                if (incache)
                    next_kb_prefetch_skt_ = mf_->Next(next_kb_prefetch_skt_);
            }
        }else{
            bool incache = true;
            if(!prev_kb_prefetch_skt_ ||cur_skt ==  prev_kb_prefetch_skt_)
                prev_kb_prefetch_skt_ = mf_->Prev(cur_skt);
            while(prev_kb_prefetch_skt_ && prev_kb_prefetch_skt_ != prev_prefetch_skt_ && incache){
                //printf ("KeyBlockPrefetch prev %p\n", prev_kb_prefetch_skt_);
                prev_kb_prefetch_skt_->IncSKTRefCnt();
                incache = prev_kb_prefetch_skt_->PrefetchKeyBlocks(mf_, column_id_, snap_->GetCurrentTime(), snap_->GetCurrentSequenceNumber(), kPrev);
                prev_kb_prefetch_skt_->DecSKTRefCnt();
                if (incache)
                    prev_kb_prefetch_skt_ = mf_->Prev(prev_kb_prefetch_skt_);
            }
        }
    }

    void SnapshotIterator::SeekWrapper(SKTableMem* skt, KeySlice* key, bool search_less_than){

        if(iter_pad_) ReturnIterScratchPad(iter_pad_);
        if(search_less_than){
            if(prev_prefetch_skt_) SingleSKTablePrefetch(prev_prefetch_skt_, kPrev);
            else SKTablePrefetch(skt, kPrev);
        }else{
            if(next_prefetch_skt_) SingleSKTablePrefetch(next_prefetch_skt_, kNext);
            else SKTablePrefetch(skt, kNext);
        }
retry:
        iter_pad_ = snap_->GetIterScratchPad(skt->GetSKTableID(), column_id_);
        if(!iter_pad_){
            iter_pad_ = SeekInternal(key, skt, search_less_than);
        }else{
            
#ifdef CODE_TRACE
            printf("[%d :: %s]IterScratchPad From Snapshot\n", __LINE__, __func__);
#endif
            skt->IncSKTRefCnt();

            // load sktable from the device.
            if (!skt->GetSKTableDeviceFormatNode() || skt->GetSKTDFVersion() != iter_pad_->sktdf_version_ || skt->IsFlushing()) {
                skt->DecSKTRefCnt();
                snap_->ReleaseIterScratchPad(iter_pad_, column_id_);
                ReturnIterScratchPad(iter_pad_);
                goto retry;
            }
            skt->DecSKTRefCnt();
            /* If iter_pad exist, next/prev SKTables were prefetched */
            cur_info_.InitIterCurrentKeyInfo();
            auto key_buffer = (KeyBufferEntry*)iter_pad_->key_buffer_;
            if(key){
                SetCurrentKeyInfo(Slice(key->data(), key->size()), iter_pad_, search_less_than);
            }else if(search_less_than){
                cur_info_.offset = key_buffer + (iter_pad_->key_entry_count_-1);
            }else{
                cur_info_.offset = key_buffer;
            }
            if(!cur_info_.offset){
                /*
                 * If pad has been created successfully even if offset is null, the pad has been inserted to SKTableMem & snap->iter_pad_map_ 
                 * In this case, ref_cnt could not be 0.
                 */
                if(iter_pad_) ReturnIterScratchPad(iter_pad_);
                /*The SKTable does not have valid column(seq&ttl)*/

                if(search_less_than){
                    if(prev_prefetch_skt_)
                        SingleSKTablePrefetch(prev_prefetch_skt_, kPrev);
                    skt = mf_->Prev(skt);
                }
                else{
                    if(next_prefetch_skt_)
                        SingleSKTablePrefetch(next_prefetch_skt_, kNext);
                    skt = mf_->Next(skt);
                }

                if(!skt) return;
                goto retry;
            }
        }
        if(skt) KeyBlockPrefetch(skt, search_less_than);
    }

    void SnapshotIterator::ResetPrefetch(void){
        next_prefetch_skt_ = NULL;
        prev_prefetch_skt_ = NULL;
        next_kb_prefetch_skt_= NULL;
        prev_kb_prefetch_skt_= NULL;
    }
    void SnapshotIterator::Seek(const Slice& key) {
        ResetPrefetch();
        KeySlice hkey(key);
        SKTableMem *skt = mf_->FindSKTableMem(key);
        if (!skt)
            skt = mf_->GetFirstSKTable();
        SeekWrapper(skt, &hkey, false);
    }
    void SnapshotIterator::SeekForPrev(const Slice& key) {
        ResetPrefetch();
        KeySlice hkey(key);
        SKTableMem *skt = mf_->FindSKTableMem(key);
        if (!skt)
            skt = mf_->GetFirstSKTable();
        SeekWrapper(skt, &hkey, true);
    }


    void SnapshotIterator::SeekToFirst() {
        ResetPrefetch();
        SKTableMem *skt = NULL;
        KeySlice key_slice = KeySlice(GetStartKey());
        KeySlice *key = &key_slice;
        if(key->size())
            skt = mf_->FindSKTableMem(key->GetSlice());
        else
            key = NULL;
        if (!skt)
            skt = mf_->GetFirstSKTable();
        SeekWrapper(skt, key, false);
    }
    void SnapshotIterator::SeekToLast() {
        ResetPrefetch();
        SKTableMem *skt = NULL;
        KeySlice key_slice = KeySlice(GetStartKey());
        KeySlice *key = &key_slice;
        if(key->size())
            skt = mf_->FindSKTableMem(key->GetSlice());
        else
            key = NULL;
         if (skt == NULL)
            skt = mf_->GetLastSKTable();
        SeekWrapper(skt, key, true);
    }

    Slice SnapshotIterator::key() const {
        assert(Valid());
        return const_cast<SnapshotIterator*>(this)->GetKey(cur_info_.offset);
    }

    void SnapshotIterator::Next() {
        if (!cur_info_.offset) return;
        cur_info_.offset++;
        if(cur_info_.offset->uv_kb == 0){
            cur_info_.offset = NULL;
            if(iter_pad_->next_skt_){
                SeekWrapper(iter_pad_->next_skt_, NULL, false);
            }else
               ReturnIterScratchPad(iter_pad_);
        }
    }

    void SnapshotIterator::Prev() {

        if (!cur_info_.offset) return;
        if(cur_info_.offset == (KeyBufferEntry *)iter_pad_->key_buffer_/*this is first entry*/){
            SKTableMem *skt = mf_->Prev(iter_pad_->skt_);
            cur_info_.offset = NULL;
            if(skt){
                SeekWrapper(skt, NULL, true);
            }else
               ReturnIterScratchPad(iter_pad_);
        }else
            cur_info_.offset--;
    }

    Slice SnapshotIterator::value() const {
        assert(Valid());
        uint64_t cur_offset = cur_info_.offset->uv_kb;
        IterCurrentKeyInfo& mut_cur_info = const_cast<IterCurrentKeyInfo&>(cur_info_);
        auto mut_cur_iter = const_cast<IterScratchPad *>(iter_pad_);
        if (cur_offset & kKeySizeOffsetType) {
            /* uv */
            UserValue *uv = (UserValue *)(cur_offset & kUserValueAddrOrKBPMMask);
            assert(uv);
            return uv->ReadValue();
        } 
        /* kvpm */
        uint16_t kb_offset = (uint16_t)((cur_offset & kKeySizeOffsetMask) >> kKeySizeOffsetShift);
        KeyBlockPrimaryMeta *kbpm = (KeyBlockPrimaryMeta *)(cur_offset & kUserValueAddrOrKBPMMask);
        assert(kbpm);
        mut_cur_info.kbpm = kbpm;
        mut_cur_info.ikey = mut_cur_info.kbpm->GetiKeySequenceNumber();
        mut_cur_info.ivalue_size = mut_cur_info.kbpm->GetKBSize();

        KeyBlock* kb = NULL;
        bool ra_hit = false;
        if (!(kb = kbpm->GetKeyBlockAddr())) {
            kbpm->Lock();
            if (!(kb = kbpm->GetKeyBlockAddr())){
                kb = mf_->LoadValue(cur_info_.ikey, cur_info_.ivalue_size, &ra_hit, kbpm);
                kbpm->SetKeyBlockAddr(kb);
            }
            kbpm->Unlock();
        }
        /* get value from kb */
        return kb->GetValueFromKeyBlock(mf_, kb_offset);
    }
} //namespace insdb

