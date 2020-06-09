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
#ifdef INSDB_GLOBAL_STATS
       g_del_snap_cnt++;
#endif
    }

    void SnapshotImpl::RemoveIterator(SnapshotIterator* iter) {
        mu_.Lock();
        iterators_.remove(iter);
        mu_.Unlock();
    }

    Iterator* SnapshotImpl::CreateNewIterator(uint16_t col_id, const ReadOptions options) {
        SnapshotIterator* iter;
        if (options.iterator_upper_bound != NULL) {
            Slice target(options.iterator_upper_bound->data(),
                          options.iterator_upper_bound->size());
            iter = new SnapshotIterator(mf_, this, col_id, options.read_tier == kBlockCacheTier, target);
        } else {
            iter = new SnapshotIterator(mf_, this, col_id, options.read_tier == kBlockCacheTier);
        }
        mu_.Lock();
        iterators_.push_back(iter);
        mu_.Unlock();
        return reinterpret_cast<Iterator*>(iter);
    }

    SnapshotIterator::~SnapshotIterator() {
        if (kb_) {
            mf_->FreeKB(kb_);
            kb_ = NULL;
        }
        DestroyPrefetch();
        snap_->RemoveIterator(this);
        if (mf_->__DisconnectSnapshot(snap_))
            delete snap_;
        mf_->DecIteratorCount();
        cur_.Cleanup(mf_);
    }

    int SnapshotIterator::Compare(const Slice& a, const Slice& b) {
        return comparator_->Compare(a, b);
    }

    bool SnapshotIterator::IsValidKeyRange(const Slice& key) {
        if (start_key_.size() && Compare(key, start_key_) < 0) return false;
        if (last_key_.size() && Compare(key, last_key_) >= 0) return false;
        return true;
    }

    bool SnapshotIterator::Valid() const {
        return (cur_.keyhandle != 0);
    }

    void SnapshotIterator::PrefetchSktable(uint8_t dir, SKTableMem *prefetching_skt, uint8_t count) {
        if (dir == kPrev) return;

        uint32_t prefetched_cnt = 0;
        // skip already prefetched skt
        if (/* pf_hit_history_!= flags_pf_hit_none &&*/ prefetched_skt_ && prefetching_skt) {
            while (prefetching_skt != prefetched_skt_) {
                prefetching_skt = prefetching_skt->Next(mf_);
                prefetched_cnt++;
                if (prefetched_cnt > count)
                    return;
            }
        }
        // prefetch
        while (prefetching_skt) {
            prefetching_skt = prefetching_skt->Next(mf_);
            if (prefetching_skt) {
                if (prefetching_skt != prefetched_skt_) {
                    // Stop prefecting next prefetch target if prefix does not match
                    if (prefix_ && prefix_size_) {
                        Slice next_pf_target_keyname = prefetching_skt->GetBeginKeySlice();
                        if (next_pf_target_keyname.size() <= prefix_size_ || memcmp(next_pf_target_keyname.data(), prefix_, prefix_size_) != 0) {
                            break;
                        }
                    }

                    prefetching_skt->PrefetchSKTable(mf_, false, non_blocking_);
                    prefetched_skt_ = prefetching_skt;
                }
            } else {
                break;
            }
            prefetched_cnt++;
            if (prefetched_cnt > count)
                break;
        }
    }

    void SnapshotIterator::Prefetch(IterKey &iterkey, uint8_t dir, uint16_t pf_size, const char *prefix, uint16_t prefix_size) {
        bool stop = false;

        IterKey* scanned = nullptr;
        for (uint16_t i = 0; i < pf_size && !stop ; i++) {

            // get another key
            if (dir & flags_iter_next)
                SetIterKeyForPrefetch(iterkey.skt, nullptr, kNext, iterkey);
            else
                SetIterKeyForPrefetch(iterkey.skt, nullptr, kPrev, iterkey);

            // exit if no key available
            if (!iterkey.keyhandle)
                break;

            // Stop prefecting next prefetch target if prefix does not match
            if (prefix && prefix_size) {
                if (iterkey.userkey_buff.size() <= prefix_size || memcmp(iterkey.userkey_buff.data(), prefix, prefix_size) != 0) {
                    stop = true;
                    break;
                }
            }
            // validate range
            if (!IsValidKeyRange(iterkey.userkey_buff)) {
                stop = true;
                break;
            }

            // queue to the scanned queue
            scanned = new IterKey();
            *scanned = iterkey;
            scanned->skt_ref = 0;
            // remove references to  userkey_buff, value info
            // those will be freed by the scanned ikey
            iterkey.userkey_buff = Slice(0);
            iterkey.uv_or_kbpm = nullptr;

            // increase references if skt changes
            if (!prefetch_scanned_queue_.empty() && prefetch_scanned_queue_.back()->skt != scanned->skt) {
                prefetch_scanned_queue_.back()->IncSktRef(mf_);
            }
            prefetch_scanned_queue_.push(scanned);

            // prefetch KB
            if (!scanned->is_uv) {
                KeyBlock* kb = mf_->FindKBCache(iterkey.kb_ikey_seq);
                assert(kb);
                if (kb->IsDummy()) {
                    kb->KBLock();
                    if (kb->IsDummy() && !kb->SetKBPrefetched()) {
                        InSDBKey kbkey = GetKBKey(mf_->GetDBHash(), kKBlock, iterkey.kb_ikey_seq);
                        int buffer_size = SizeAlignment(scanned->kb_ivalue_size);
                        Env::Default()->Get(kbkey, NULL, &buffer_size, kReadahead);
                    }
                    kb->KBUnlock();
                }
                scanned->iter_kb = kb;
            }
        }

        // increase skt reference in the last scanned iterkey
        if (scanned) scanned->IncSktRef(mf_);
        iterkey.Cleanup(mf_);
    }

    void SnapshotIterator::DestroyPrefetch() {
        IterKey* pnode = NULL;
        /* drop all scanned node */
        while (!prefetch_scanned_queue_.empty()) {
            pnode = prefetch_scanned_queue_.front();
            prefetch_scanned_queue_.pop();
            pnode->CleanupSkt();
            pnode->CleanupUserkeyBuff();
            pnode->CleanupKB(mf_);
            // mf_->KBMDecPrefetchRefCount(pnode->GetColumnNode(), pnode->GetUserKey(), pnode->IsKBPMPrefetched());
            assert(pnode);
            FreeIterKey(pnode);
        }
    }

    void SnapshotIterator::ResetPrefetch() {
        iter_history_ = flags_iter_none;
        pf_hit_history_ = flags_pf_hit_none;
        prefetch_window_shift_ = flags_pf_shift_none;
        DestroyPrefetch();
    }

    void SnapshotIterator::SetIterKeyKeymap(SKTableMem *skt, const Slice *key, SeekDirection dir, IterKey& iterkey) {

        Keymap *keymap;
        KeyMapHandle keyhandle = 0;
        bool skt_move = false;
        if (dir == kNext || dir == kPrev) {
            keyhandle = iterkey.keyhandle;
        }

        do {
            // prevent split by reference count
            if (skt != iterkey.skt) {

                skt->KeyMapGuardLock();
                // increase skt, keymap reference count
                skt->IncSKTRefCnt(mf_);
                skt->SetReference();
                keymap = skt->GetKeyMap();
                keymap->IncRefCnt();
                // load keymap
                while (!skt->IsKeymapLoaded()) {
                    if (!non_blocking_) {
                        skt->LoadSKTableDeviceFormat(mf_);
                    }
                }
                /*
                 * uint32_t old_size = skt->GetKeyMap()->GetAllocatedSize();
                 */
                // check pending keys and keymap availability
                //int32_t old_size = skt->GetAllocatedSize();
                int32_t old_size = skt->GetBufferSize();
                skt->InsertUserKeyToKeyMapFromInactiveKeyQueue(mf_, true, snap_->GetSequenceNumber(), false);
                skt->InsertUserKeyToKeyMapFromInactiveKeyQueue(mf_, false, snap_->GetSequenceNumber(), false);
                //int32_t new_size = skt->GetAllocatedSize();
                int32_t new_size = skt->GetBufferSize();
#ifdef MEM_TRACE
                if(new_size > old_size)
                    mf_->IncSKTDF_MEM_Usage(new_size - old_size);
                else if(old_size - new_size)
                    mf_->DecSKTDF_MEM_Usage(old_size - new_size);
#endif
                mf_->IncreaseSKTDFNodeSize(new_size - old_size);

                /*
                 * account keymap size
                 * assert (skt->GetKeyMap()->GetAllocatedSize() >= old_size);
                 * if(skt->GetKeyMap()->GetAllocatedSize() > old_size) {
                 *  mf_->IncreaseMemoryUsage(skt->GetKeyMap()->GetAllocatedSize()-old_size);
                 * #ifdef MEM_TRACE
                 *  mf_->IncSKTDF_MEM_Usage(skt->GetKeyMap()->GetAllocatedSize()-old_size);
                 * #endif
                 * }
                 */

                skt->KeyMapGuardUnLock();
            } else {
                keymap = skt->GetKeyMap();
            }

            // shared lock
            skt->KeyMapRandomReadSharedLock();

    move_to_another_key:
            // Check shift iterator history and set direction
            // keymap lock
            switch (dir) {
            case kSeekNext:
                if (!keyhandle)
                    keyhandle = keymap->SearchGreaterOrEqual(*key);
                else
                    keyhandle = keymap->Next(keyhandle);
                break;
            case kSeekPrev:
                if (!keyhandle)
                    keyhandle = keymap->SearchLessThanEqual(*key);
                else
                    keyhandle = keymap->Prev(keyhandle);
                break;
            case kSeekFirst:
                // if key exists, it's start key
                if (!keyhandle) {
                    if (key)
                        keyhandle = keymap->SearchGreaterOrEqual(*key);
                    else
                        keyhandle = keymap->First();
                } else {
                    keyhandle = keymap->Next(keyhandle);
                }
                break;
            case kSeekLast:
                // if key exists, it's end key
                if (!keyhandle) {
                    if (key)
                        keyhandle = keymap->SearchLessThanEqual(*key);
                    else
                        keyhandle = keymap->Last();
                } else {
                    keyhandle = keymap->Prev(keyhandle);
                }
                break;
            case kNext:
            case kPrev:
                // Move from the current user key
                // if current skt is just moved, reset keyhandle to starting key
                if (skt_move) {
                    skt_move = false;
                    if (dir == kNext)
                        keyhandle = keymap->First();
                    else
                        keyhandle = keymap->Last();
                } else {
                    // current key is invalid
                    // return here
                    if (!keyhandle) {
                        skt->KeyMapRandomReadSharedUnLock();
                        return;
                    }
                   if (dir == kNext) // Next
                        keyhandle = keymap->Next(keyhandle);
                   else // Prev
                        keyhandle = keymap->Prev(keyhandle);
                }
               break;
            default:
                abort();
            }
            // Feat: iter upper bound function
            if (keyhandle && last_key_.size() && !IsValidKeyRange(keymap->GetKeySlice(keyhandle))) {
                if (dir == kNext)
                    keyhandle = 0;
                else
                    goto move_to_another_key;
            }
            // end
            // get another skt if a key is not found
            if (!keyhandle) {
another_skt:
                skt->KeyMapRandomReadSharedUnLock();
                SKTableMem *another_skt = nullptr;

                switch (dir) {
                case kNext:
                case kSeekNext:
                case kSeekLast:
                    another_skt = skt->Next(mf_);
                    break;
                case kPrev:
                case kSeekPrev:
                case kSeekFirst:
                    another_skt = skt->Prev(mf_);
                    break;
                }

                // release sktable and keymap if it's not in sktable
                if (skt != iterkey.skt) {
                    skt->DecSKTRefCnt();
                    if (keymap->DecRefCntCheckForDelete())
                        delete keymap;
                }

                if (another_skt) {
                    skt_move = true;
                    skt = another_skt;
                    continue;
                } else {
                    skt = nullptr;
                    keymap = nullptr;
                    keyhandle = 0;
                    // no more SKTable is available.
                    // exit without a key.
                    break;
                }
            }
            // find keynode belonging to the iterator(check seqnum, old key or next/prev key)
            void* col_uk = nullptr;
            Slice key;
            Slice col_info_data = keymap->GetColumnData(keyhandle, column_id_, col_uk, key);

            // UserKey
            UserKey *userkey = reinterpret_cast<UserKey *>(col_uk);
            bool valid_col = (col_uk || col_info_data.size());
            // find column node
            if (!valid_col) {
                if (key.size()) free((void*)key.data());
                goto move_to_another_key;
            }
            // UserKey
            bool found = true;
            bool deleted = false;
            bool need_lookup_colinfo = false;
            bool b_final = false;
            if (userkey) {
    lookup_column_node:
                userkey->ColumnLock(column_id_);
                ColumnNode* col_node = userkey->GetColumnNode(column_id_, deleted, snap_->GetCurrentTime(), snap_->GetSequenceNumber(), mf_->GetTtlFromTtlList(column_id_));
                if (col_node && !deleted) {
                    // update user key info in current context
                    iterkey.kb_index = col_node->GetKeyBlockOffset();
                    if (col_node->HasUserValue()) {
                        UserValue *uv = col_node->GetUserValue();
                        // pin uservalue
                        uv->Pin();
                        iterkey.kb_ikey_seq = 0;
                        iterkey.uv_or_kbpm = reinterpret_cast<void*>(uv);
                        iterkey.is_uv = 1;
                    } else {
                        KeyBlockPrimaryMeta *kbpm = col_node->GetKeyBlockMeta();
                        // reference kbpm
                        // kbpm->SetRefBitmap(iterkey.kb_index);
                        assert(kbpm->IsRefBitmapSet(iterkey.kb_index));
                        iterkey.kb_ikey_seq = col_node->GetInSDBKeySeqNum();
                        iterkey.uv_or_kbpm = reinterpret_cast<void*>(kbpm);
                        iterkey.is_uv = 0;
                    }
                    iterkey.kb_ivalue_size = col_node->GetiValueSize();
                    iterkey.userkey_buff = key;
                } else {
                    if (!deleted && !b_final) need_lookup_colinfo = true;
                    found = false;
                }
                userkey->ColumnUnlock(column_id_);
            } else {
                iterkey.uv_or_kbpm = nullptr;
                iterkey.is_uv = 0;
                ColumnData col_data;
                col_data.init();
                ColMeta* colmeta = reinterpret_cast<ColMeta*>(const_cast<char*> (col_info_data.data()));
                colmeta->ColInfo(col_data);
                // device format
                if (col_data.iter_sequence <= snap_->GetSequenceNumber() &&
                        (!col_data.has_ttl || mf_->GetTtlFromTtlList(column_id_) == 0 || (col_data.ttl + mf_->GetTtlFromTtlList(column_id_)) > snap_->GetCurrentTime())) {
                    if (!col_data.is_deleted) {
                        // update user key info in current context
                        iterkey.kb_index = col_data.kb_offset;
                        iterkey.kb_ikey_seq = col_data.ikey_sequence;
                        iterkey.kb_ivalue_size = col_data.ikey_size;
                        iterkey.userkey_buff = key;
                    } else {
                        deleted = true;
					}
                } else {
                    // reset keyhandle to fall through to old UserKey map
                    if (!deleted) need_lookup_colinfo = true;
                    need_lookup_colinfo = true;
                    found = false;
                }
            }
            // if deleted, skip to another.
            if (deleted) {
                if (key.size()) free((void*)key.data());
                goto move_to_another_key;
            }
            // look up SKTable hash table if key is not found in the keymap
            if (!found) {
                if (need_lookup_colinfo) {
                    userkey = mf_->LookupUserKeyFromFlushedKeyHashMap(key);
                    if (!userkey) {
                        free(reinterpret_cast<void*>(const_cast<char*>(key.data())));
                        key.clear();
                        goto move_to_another_key;
                    }
                    b_final = true;
                    found = true;
                    goto lookup_column_node;
                }
                if (key.size()) free((void*)key.data());
            }

            skt->KeyMapRandomReadSharedUnLock();
            if (found) break;
        } while (true);
exit:
        // decrease iterkey sktable reference count if it's being replaced.
        if (skt != iterkey.skt) {
            iterkey.CleanupSkt();
            // new skt is referenced so that reference will be cleared later.
            iterkey.skt_ref = 1;
        }
        // fill iterator context
        iterkey.skt = skt; /*No split allowed, allow flush*/
        iterkey.keymap = keymap; /* immutable */
        iterkey.keyhandle = keyhandle;
    }

    void SnapshotIterator::SetIterKey(SKTableMem *skt, const Slice *key, SeekDirection dir, IterKey& iterkey) {
        UserValue *prior_uv = nullptr;

        // clean up current userkey and value
        iterkey.CleanupValue(mf_);
        iterkey.CleanupUserkeyBuff();

        // add direction to history
        iter_history_ <<= flags_iter_shift;
        if (iterkey.keyhandle) {
            if (dir == kNext)
                iter_history_ |= flags_iter_next;
            else if (dir == kPrev)
                iter_history_ |= flags_iter_prev;
        }

        // Check whether iterator's direction has changed
        // If it is, remove all prefetch_scanned_queue_ and prefetched_queue_
        if ((iter_history_ & flags_iter_dir_history_mask) == flags_iter_next_prev_history ||
            (iter_history_ & flags_iter_dir_history_mask) == flags_iter_prev_next_history) {
            pf_hit_history_ = flags_pf_hit_none;
            prefetch_window_shift_ = flags_pf_shift_none;
            prefetched_skt_ = nullptr;
            DestroyPrefetch();
        }

        Keymap *keymap = nullptr;
        KeyMapHandle keymaphandle = 0;

        // prefetch sktable
        if (skt != iterkey.skt)
            PrefetchSktable(dir, skt, 8);

        // Get Next/Prev KV information.
        // If prefetch_scanned_queue_ exist, first entry is Next/Prev KV information.
        // else get Next/Prev information manually.
        if (prefetch_scanned_queue_.empty()) {
            // Set current iterator context by Keymap
            iterkey.iter_kb = nullptr;
            SetIterKeyKeymap(skt, key, dir, iterkey);
        } else {
            // get next keymap node from prefetch queue
            IterKey* pnode = prefetch_scanned_queue_.front();
            prefetch_scanned_queue_.pop();
            // free iterkey's references
            iterkey.CleanupSkt();

            // fill iterator context
            iterkey = *pnode;
            FreeIterKey(pnode);
        }
    }

    void SnapshotIterator::SetIterKeyForPrefetch(SKTableMem *skt, const Slice *key, SeekDirection dir, IterKey& iterkey) {
        // Set current iterator context by Keymap
        SetIterKeyKeymap(skt, key, dir, iterkey);
    }

    void SnapshotIterator::Seek(const Slice& key) {
        Slice target_key(key);
        if (!IsValidKeyRange(key)) {
            target_key = Slice(last_key_);
        }
        ResetPrefetch();
        SKTableMem* skt = NULL;
        while (1) {
            skt = mf_->FindSKTableMem(target_key);
            if (!skt)
                skt = mf_->GetFirstSKTable();
            // Lock down SKTable
            skt->IncSKTRefCnt(mf_);
            if (skt->IsDeletedSKT()) skt->DecSKTRefCnt();
            else
                break;
        }
        skt->SetReference();
        SetIterKey(skt, &target_key, kSeekNext, cur_);
        skt->DecSKTRefCnt();
    }

    void SnapshotIterator::SeekForPrev(const Slice& key) {
        Slice target_key(key);
        if (!IsValidKeyRange(key)) {
            target_key = Slice(last_key_);
        }
        ResetPrefetch();
        SKTableMem* skt = NULL;
        while (1) {
            skt = mf_->FindSKTableMem(target_key);
            if (!skt)
                skt = mf_->GetFirstSKTable();
            // Lock down SKTable
            skt->IncSKTRefCnt(mf_);
            if (skt->IsDeletedSKT()) skt->DecSKTRefCnt();
            else
                break;
        }
        skt->SetReference();
        SetIterKey(skt, &target_key, kSeekPrev, cur_);
        skt->DecSKTRefCnt();
    }

    void SnapshotIterator::SeekToFirst() {
        ResetPrefetch();
        SKTableMem *skt = NULL;
        KeySlice key_slice = KeySlice(GetStartKey());
        KeySlice *key = &key_slice;
        while (1) {
            if (key->size())
                skt = mf_->FindSKTableMem(key->GetSlice());
            else
                key = NULL;
            if (!skt)
                skt = mf_->GetFirstSKTable();
            // Lock down SKTable
            skt->IncSKTRefCnt(mf_);
            if (skt->IsDeletedSKT()) skt->DecSKTRefCnt();
            else
                break;
        }
        skt->SetReference();
        SetIterKey(skt, key, kSeekFirst, cur_);
        skt->DecSKTRefCnt();
    }

    void SnapshotIterator::SeekToLast() {
        ResetPrefetch();
        SKTableMem *skt = NULL;
        KeySlice key_slice = KeySlice(GetLastKey());
        KeySlice *key = &key_slice;
        while (1) {
            if (key->size())
                skt = mf_->FindSKTableMem(key->GetSlice());
            else
                key = NULL;
            if (skt == NULL)
                skt = mf_->GetLastSKTable();
            // Lock down SKTable
            skt->IncSKTRefCnt(mf_);
            if (skt->IsDeletedSKT()) skt->DecSKTRefCnt();
            else
               break;
        }
        skt->SetReference();
        SetIterKey(skt, key, kSeekLast, cur_);
        skt->DecSKTRefCnt();
    }

    Slice SnapshotIterator::key() const {
        assert(Valid());
        return cur_.userkey_buff;
    }

    void SnapshotIterator::Next() {
        SetIterKey(cur_.skt, nullptr, kNext, cur_);
    }

    void SnapshotIterator::Prev() {
       SetIterKey(cur_.skt, nullptr, kPrev, cur_);
    }

    Slice SnapshotIterator::value() const {
        SnapshotIterator *pthis = const_cast<SnapshotIterator*>(this);

        assert(Valid());

        // mark value is called
        pthis->iter_history_ |= flags_iter_value;
        
        bool cache_hit = true;
        bool ra_hit = false;
        KeyBlock *kb = nullptr;
        if (cur_.uv_or_kbpm && cur_.is_uv) {
            // user value
            UserValue *uv = reinterpret_cast<UserValue *>(cur_.uv_or_kbpm);
            assert(uv);
            return uv->GetValueSlice();
        } else {
            if (last_kb_ikey_seq_ != cur_.kb_ikey_seq || !kb_) {
                if (cur_.iter_kb) { 
                    kb = cur_.iter_kb; 
                } else {
                    kb = mf_->FindKBCache(cur_.kb_ikey_seq);
                }
                assert(kb);
                if (kb->IsDummy()) {
                    cache_hit = false;
#ifdef TRACE_READ_IO
                    if (kb->IsKBPrefetched()) {
                        g_itr_kb_prefetched++;
                    } else {
                        g_itr_kb++;
                    }
#endif
                    kb->KBLock();
                    if (kb->IsDummy()) {
                        bool b_load =  mf_->LoadValue(cur_.kb_ikey_seq, cur_.kb_ivalue_size, &ra_hit, kb);
                        assert(b_load);
                        mf_->DummyKBToActive(cur_.kb_ikey_seq, kb);
                    }
                    kb->KBUnlock();
                }
                // replace the current KB
                if (kb_!= kb) {
                    if (kb_) {
                        mf_->FreeKB(kb_);
                    }
                    pthis->kb_ = kb;
                }
                pthis->last_kb_ikey_seq_ = cur_.kb_ikey_seq;
            } else {
                if (cur_.iter_kb) mf_->FreeKB(cur_.iter_kb);
                kb = kb_;
            } 
        } 

        Slice value =  kb->GetValueFromKeyBlock(mf_, cur_.kb_index);
#ifdef BUILDITR_INCLUDE_DIRECTVALUE
        if (value.size() <= kKBDirectAllowMaxSize)
            *(reinterpret_cast<uint64_t*>(mut_cur_iter)->entry_info_buffer_ + cur_info_.offset) = (uint64_t)((((uint64_t)value.size()) << kKeySizeOffsetShift) | (uint64_t)value.data() | kKBDirectAccessType);
#endif

        if (mf_->IsPrefixDetectionEnabled()) {
            // compare the prefix to hint prefetch
#ifdef INSDB_TRACE_ITERATOR
            bool prefix_updated = false;
#endif
            if (prefix_size_) {
                while (prefix_size_) {
                    if (memcmp(cur_.userkey_buff.data(), prefix_, prefix_size_) != 0) {
                        prefix_size_--;
#ifdef INSDB_TRACE_ITERATOR
                        prefix_updated = true;
#endif
                        continue;
                    } else {
                        break;
					}
                }
            }
#ifdef INSDB_TRACE_ITERATOR
            if (prefix_updated) {
                dumpHex(reinterpret_cast<void*>(this), "cur_ukey", cur_uk_->GetKey().data(), cur_uk_->GetKey().size());
                dumpHex(reinterpret_cast<void*>(this), "updated prefix", prefix_, prefix_size_);
            }
#endif

            // If no prefix exists, start with max prefix.
            if (!prefix_size_) {
                prefix_size_ = cur_.userkey_buff.size() < 16?cur_.userkey_buff.size():16;
                if (prefix_size_ > 0)
                    prefix_size_--;
                if (prefix_size_) {
                    memcpy(prefix_, cur_.userkey_buff.data(), prefix_size_);
#ifdef INSDB_TRACE_ITERATOR
                    dumpHex(reinterpret_cast<void*>(this), "new prefix", prefix_, prefix_size_);
#endif
                } else {
#ifdef INSDB_TRACE_ITERATOR
                    printf("iter %p key name too short %lu\n", this, cur_uk_->GetKeySlice()->size());
#endif
                }
            }
        }

        bool need_prefetch = ((iter_history_ & flags_iter_dir_history_mask) != flags_iter_next_prev_history &&
                (iter_history_ & flags_iter_dir_history_mask) != flags_iter_prev_next_history) /* check sequential pattern */ &&
            (iter_history_ & flags_iter_value_mask) == flags_iter_value_mask; /* check get value pattern */
        if (need_prefetch) {
            bool need_to_inc = false;
            uint16_t pf_window = 0;
            /* update prefetch hit history */
            if (!cache_hit) {
                if (ra_hit) {
                    pthis->pf_hit_history_++;
                } else {
                    uint32_t old_hit_count = (pf_hit_history_ & flags_pf_prev_hit_mask) >> flags_pf_hit_shift;
                    uint32_t cur_hit_count = pf_hit_history_ & flags_pf_cur_hit_mask;
                    pthis->pf_hit_history_ = pf_hit_history_ << flags_pf_hit_shift; /* replace previous hit count */
                    if (old_hit_count <= cur_hit_count) need_to_inc = true;
                }
            }
            pf_window = pthis->GetPFWindowSize(ra_hit, need_to_inc);
            if (pf_window > prefetch_scanned_queue_.size())
                pf_window -= prefetch_scanned_queue_.size();
            else
                goto exit;

            /* get prefetch start information */
            IterKey start_iterkey;
            if (!prefetch_scanned_queue_.empty()) {
                IterKey* pnode = prefetch_scanned_queue_.back();
                assert(pnode);
                start_iterkey = *pnode;
            } else {
                start_iterkey = pthis->cur_;
            }
            // complete iterkey copy
            start_iterkey.InitAfterCopy(mf_);
            /* do prefetch */
            pthis->Prefetch(start_iterkey, (iter_history_ & flags_iter_dir_mask), pf_window, prefix_, prefix_size_);
        }
exit:
        /* get value from kb */
        return value;
    }
} // namespace insdb

