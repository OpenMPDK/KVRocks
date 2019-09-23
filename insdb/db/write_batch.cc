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


//
// WriteBatch::rep_ :=
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeMerge varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "db/dbformat.h"
#include "insdb/write_batch.h"
#include "insdb/db.h"
#include "util/coding.h"
#include "db/db_impl.h"

namespace insdb {

    // WriteBatch header has an 4-byte count.
    static const size_t kHeader = 4;

    WriteBatch::WriteBatch() {
        Clear();
    }

    WriteBatch::~WriteBatch() { }

    WriteBatch::Handler::~Handler() { }

    // FIX: Continue feature fix
    // Continue is called by WriteBatch::Iterate. If it returns false,
    // iteration is halted. Otherwise, it continues iterating. The default
    // implementation always returns true.
    bool WriteBatch::Handler::Continue() {
      return true;
    }
    // FIX: end

    void WriteBatch::Clear() {
        rep_.clear();
        rep_.resize(kHeader);
    }

    size_t WriteBatch::ApproximateSize() {
        return rep_.size();
    }

    size_t WriteBatch::Count() {
        return DecodeFixed32(rep_.data());
    }

    //Feat: For implementing RollbackToSavePoint feature
    //Set the count for WriteBatch
    void WriteBatch::SetCount(int n) {
        EncodeFixed32(&rep_[0], n);
    }
    //Feat end

    Status WriteBatch::Append(WriteBatch* src, const bool wal_only)
    {
        size_t src_len;

        src_len = src->rep_.size() - kHeader;

        EncodeFixed32(const_cast<char *>(rep_.data()), static_cast<unsigned int>(Count() + src->Count()));
        assert(src->rep_.size() >= kHeader);
        rep_.append(src->rep_.data() + kHeader, src_len);
        return Status::OK();

    }

#define VARTOFIXOPT
    Status WriteBatch::Iterate(Handler* handler){
        Slice input(rep_);
        if (input.size() < kHeader) {
            return Status::Corruption("malformed WriteBatch (too small)");
        }

        input.remove_prefix(kHeader);
        Slice key, value;
        uint16_t col_id;
        int found = 0;
        uint32_t len = 0;
        char tag = '\0';
        // FIX: Continue feature fix
        // Add Continue check
        while (!input.empty() && handler->Continue()) {
        // FIX: end
            found++;
            tag = input[0];
            input.remove_prefix(1);
#ifdef VARTOFIXOPT
            len = DecodeFixed32(input.data());
            assert(len <= input.size());
            input.remove_prefix(4);
            key = Slice(input.data(), len);
            //KeySlice key_with_hash = KeySlice(key.data(), key.size());
            input.remove_prefix(len);
#endif
            switch (tag) {
                case kTypeValue:
#ifdef VARTOFIXOPT
                    len = DecodeFixed32(input.data());
                    assert(len <= input.size());
                    input.remove_prefix(4);
                    value = Slice(input.data(), len);
                    input.remove_prefix(len);
                    col_id=DecodeFixed16(input.data());
                    input.remove_prefix(2);
                    handler->Put(key, value, col_id);
                    //((WriteBatchInternal*)handler)->Store(key_with_hash, value, col_id, kPutType);
#else
                    if (GetLengthPrefixedSlice(&input, &key) &&
                            GetLengthPrefixedSlice(&input, &value) &&
                            GetVarint16(&input, &col_id)) {
                        handler->Put(key, value, col_id);
                    } else {
                        return Status::Corruption("bad WriteBatch Put");
                    }
#endif
                    break;
                case kTypeMerge:
#ifdef VARTOFIXOPT
                    len = DecodeFixed32(input.data());
                    assert(len <= input.size());
                    input.remove_prefix(4);
                    value = Slice(input.data(), len);
                    input.remove_prefix(len);
                    col_id=DecodeFixed16(input.data());
                    input.remove_prefix(2);
                    {
                        Status s = handler->Merge(key, value, col_id);
                        if(!s.ok()) return s;
                    }
#else
                    if (GetLengthPrefixedSlice(&input, &key) &&
                            GetLengthPrefixedSlice(&input, &value)&&
                            GetVarint16(&input, &col_id)) {
                        Status s = handler->Merge(key, value, col_id);
                        if(!s.ok())
                            return s;
                    } else {
                        return Status::Corruption("bad WriteBatch Merge");
                    }
#endif
                    break;
                case kTypeDeletion:
#ifdef VARTOFIXOPT
                    col_id=DecodeFixed16(input.data());
                    input.remove_prefix(2);
                    handler->Delete(key, col_id);
#else
                    if (GetLengthPrefixedSlice(&input, &key) &&
                            GetVarint16(&input, &col_id)) {
                        handler->Delete(key, col_id);
                    } else {
                        return Status::Corruption("bad WriteBatch Delete");
                    }
#endif
                    break;
                default:
                    return Status::Corruption("unknown WriteBatch tag");
            }
        }
        if (found != DecodeFixed32(rep_.data())) {
            return Status::Corruption("WriteBatch has wrong count");
        } else {
            return Status::OK();
        }
    }

    void WriteBatch::Put(const Slice& key, const Slice& value, uint16_t col_id) {
        EncodeFixed32(&rep_[0], DecodeFixed32(rep_.data()) + 1);
        rep_.push_back(static_cast<char>(kTypeValue));
#ifndef VARTOFIXOPT
        PutLengthPrefixedSlice(&rep_, key);
        PutLengthPrefixedSlice(&rep_, value);
        PutVarint16(&rep_, col_id);
#else
        PutFixed32(&rep_, key.size());
        rep_.append(key.data(), key.size());
        PutFixed32(&rep_, value.size());
        rep_.append(value.data(), value.size());
        PutFixed16(&rep_, col_id);
#endif
    }
    void WriteBatch::Merge(const Slice& key, const Slice& value, uint16_t col_id) {
        EncodeFixed32(&rep_[0], DecodeFixed32(rep_.data()) + 1);
        rep_.push_back(static_cast<char>(kTypeMerge));
#ifndef VARTOFIXOPT
        PutLengthPrefixedSlice(&rep_, key);
        PutLengthPrefixedSlice(&rep_, value);
        PutVarint16(&rep_, col_id);
#else
        PutFixed32(&rep_, key.size());
        rep_.append(key.data(), key.size());
        PutFixed32(&rep_, value.size());
        rep_.append(value.data(), value.size());
        PutFixed16(&rep_, col_id);
#endif
    }
    void WriteBatch::Delete(const Slice& key, uint16_t col_id) {
        EncodeFixed32(&rep_[0], DecodeFixed32(rep_.data()) + 1);
        rep_.push_back(static_cast<char>(kTypeDeletion));
#ifndef VARTOFIXOPT
        PutLengthPrefixedSlice(&rep_, key);
        PutVarint16(&rep_, col_id);
#else
        PutFixed32(&rep_, key.size());
        rep_.append(key.data(), key.size());
        PutFixed16(&rep_, col_id);
#endif
    }
    Status WriteBatch::GetEntryFromDataOffset(size_t data_offset,
            WriteType* type, Slice* Key,
            Slice* value, Slice* blob,
            Slice* xid) const {
        if (type == nullptr || Key == nullptr || value == nullptr ||
                blob == nullptr || xid == nullptr) {
            return Status::InvalidArgument("Output parameters cannot be null");
        }

        if (data_offset == rep_.size()) {
            // reached end of batch.
            return Status::NotFound("reached end of batch");
        }

        if (data_offset > rep_.size()) {
            return Status::InvalidArgument("data offset exceed write batch size");
        }
        Slice input = Slice(rep_.data() + data_offset, rep_.size() - data_offset);
        char tag;
        uint16_t column_family;
        Status s = ReadRecordFromWriteBatch(&input, &tag, &column_family, Key, value,
                blob, xid);

        switch (tag) {
            case kTypeValue:
                *type = kPutRecord;
                break;
            case kTypeDeletion:
                *type = kDeleteRecord;
                break;
            case kTypeMerge:
                *type = kMergeRecord;
                break;
            default:
                return Status::Corruption("unknown WriteBatch tag");
        }
        return Status::OK();
    }

    size_t WriteBatch::GetFirstOffset()
    {
        return kHeader;
    }

    WriteBatchInternal::WriteBatchInternal( DBImpl *dbimpl, Manifest *mf, int32_t count, uint64_t ttl):
        dbimpl_(dbimpl),
        mf_(mf),
        ttl_(ttl){

            trxn_.count = count;
            trxn_.seq_number = 0;
            trxn_.id = 0;
        }

    void WriteBatchInternal::InitWBI( DBImpl *dbimpl, Manifest *mf, int32_t count, uint64_t ttl)
    {
        /**/
        dbimpl_ = dbimpl;
        mf_ = mf;
        ttl_= ttl;
        trxn_.count = count;
        trxn_.seq_number = 0;
        trxn_.id = 0;
    }
    inline void WriteBatchInternal::Put(const Slice& key, const Slice& value, const uint16_t col_id){
#ifdef INSDB_GLOBAL_STATS
        g_api_put_cnt++;
#endif
        KeySlice key_with_hash = KeySlice(key.data(), key.size());
        Store(key_with_hash, value, col_id, kPutType);
            return;
    }
    inline Status WriteBatchInternal::Merge(const Slice& key, const Slice& value, const uint16_t col_id){
        ReadOptions read_options;
        std::string read_value;
        std::string merged_value;
        bool success = false;

#ifdef INSDB_GLOBAL_STATS
        g_api_merge_cnt++;
#endif

        Status s = dbimpl_->Get(read_options, key, &read_value, col_id);
        if(s.ok())
        {
            Slice read_value_slice(read_value.data(), read_value.size());
            success = dbimpl_->CallMergeOperator(key, &read_value_slice, value, &merged_value);
        }
        /* Return an error with a merge request when the key is not available. */
        else if(s.IsNotFound())
        {
            success = dbimpl_->CallMergeOperator(key, nullptr, value, &merged_value);
        }
        else
        {
            return Status::IOError("Merge key lookup failed");
        }
        if(!success)
            return Status::InvalidArgument("Merge operator returned failure");

        /* use kPutType in the rest of process */
        KeySlice key_with_hash = KeySlice(key.data(), key.size());
        Store(key_with_hash, merged_value, col_id, kPutType);
        return Status::OK();
    }
    inline void WriteBatchInternal::Delete(const Slice& key, const uint16_t col_id){ 
#ifdef INSDB_GLOBAL_STATS
        g_api_delete_cnt++;
#endif
        KeySlice key_with_hash = KeySlice(key.data(), key.size());
        Store(key_with_hash, Slice(0), col_id, kDelType);
    }

#if 0
   char tmpkey[17];
   strncpy(tmpkey, uk->GetKeySlice().data() , 16);
   tmpkey[16] = '\0';
   printf("[%d :: %s]Delete CMD exist uk : %s || uk addr : %p\n", __LINE__, __func__, tmpkey, uk);
#endif
   void WriteBatchInternal::Store(const KeySlice& key, const Slice& value, const uint16_t col_id, RequestType type ){
       Status s;
       UserKey *uk = NULL; 
       UserValue *uv = NULL;
       SKTableMem *skt = NULL;

       /*SequenceNumber must be assigned after having ColumnLock in order to prevent sequence number inversion */
       //SequenceNumber seq_number = mf_->GenerateSequenceNumber();
       ColumnNode *col_node = new ColumnNode(type, (mf_->TtlIsEnabled(col_id)?ttl_:0));
       /*
        * The UserValue->buffer will be used for source of compression in BuildKeyBlock()
        * So we need to add "key + columnID + ttl(if exist)"
        */
       if(type == kPutType){
           uv = mf_->AllocUserValue(value.size() + key.size() + 1/*Column ID*/ + sizeof(ttl_));
           uv->InitUserValue(value, key, col_id, (mf_->TtlIsEnabled(col_id)?ttl_:0));
           col_node->InsertUserValue(uv);
       }
       if(mf_->KeyHashMapFindOrInsertUserKey(uk, key, col_id)){
           skt = mf_->FindSKTableMem(key);
           assert(skt);
           skt->PushUserKeyToActiveKeyQueue(uk);
           if(!skt->GetSKTableDeviceFormatNode() && !skt->IsFetchInProgress()) 
               skt->PrefetchSKTable(mf_, true);
#ifdef INSDB_GLOBAL_STATS
           g_new_ikey_cnt++;
#endif
       }
#ifndef NDEBUG
       else{
           mf_->SKTableCacheMemLock();
           skt = mf_->FindSKTableMem(key);
           assert((skt->GetSKTableDeviceFormatNode()) || skt->SetFetchInProgress());
           assert(skt->IsInSKTableCache(mf_) || skt->IsFlushOrEvicting());
           mf_->SKTableCacheMemUnlock();
           skt = NULL;
       }
#endif
       assert(uk);
       assert(uk->GetReferenceCount());

       /* Do not check whether the given key exists or not in new design(hashmap insertion only version) */
       /*In this case, don't need to check sktable whether is fetched because UserKey exists*/
#if 0
       if( type == kDelType && uk->IsColumnEmpty(col_id) && (trxn_.count == 1 || trxn_.seq_number < trxn_.count - 1)
               /* The request must not be a part of TRXN or not the last request of TRXN */){
           /* SKTableMem has been fetched but the column does not exist or the previous command is delete.*/
           uk->ColumnUnlock(col_id); /*uk->col_lock[col_id].Unlock();*/
           uk->DecreaseReferenceCount();
           if(trxn_.id) mf_->DecTrxnGroup(trxn_.id >> TGID_SHIFT, 1);
           --trxn_.count;
           delete col_node;
           return; 
       }
#endif

       SequenceNumber seq_number = mf_->GenerateSequenceNumber();
       col_node->UpdateSequenceNumber(seq_number);
       /* 
        * Last request in a TRXN should be written to the device regardless of its status 
        * The last arguemnt is for this.
        */
       InsertColumnNodeStatus sched_stat = uk->UpdateColumn(col_node, col_id, (trxn_.count > 1 && trxn_.seq_number == trxn_.count - 1), mf_);
       /* 
        * If return value is kHasBeenScheduled, it means the previous ColumnNode can be merged to this ColumnNode.
        * So, we need to save its Trxn Group ID. If it is different from its TGID, inc uk->col_data_->unsubmitted_trxn_group_cnt_++;
        */
       if(sched_stat  == kFailInsert){
           /* The previous & current col_node is a delete request */
           /* Now we lost one seq #.*/
           /* Check User Key whether it needs to be deleted */
           uk->ColumnUnlock(col_id); /*uk->col_lock[col_id].Unlock();*/
           uk->DecreaseReferenceCount();
           if(trxn_.id)
               mf_->DecTrxnGroup(trxn_.id >> TGID_SHIFT, 1);
           --trxn_.count;//if it is not a part of trxn, it can be 0xFFFFFFFF. but there is no more request in WriteBatchInternal. 
           delete col_node;
           return; 
       }

       if(trxn_.count > 1 && !trxn_.id){
           trxn_.id = mf_->GetTrxnID(trxn_.count); 
       }

       // Call AddUserKey callback
       /*
          if(dbimpl_->CallbackAvailable())
          {
          dbimpl_->CallAddUserKey(uk, uv, type, trxn_.seq_number, col_id, new_uk);
          }
          */

       /*
        * Main purpose of following logic is ..
        * If keys have different TGID, then we are not gonna vertically merge them.
        * If trxn_.id is zero(non-trxn), We consider Trxn Group 0. and It is always considered as Completed  
        */

       RequestNode *req_node = NULL;
       if(sched_stat == kNeedToSchedule){
           /*
            * This is first one(no vertical merge if no more rq is comming)
            * If this s non-trxn request, set unsubmitted_trxn_group_cnt_ to 0 
            * and the non-trxn request is considerted to have "0" Group ID.
            */
           assert(!uk->GetRequestNode(col_id));
           req_node = mf_->AllocRequestNode();
           req_node->Init(uk, col_id, trxn_);
           if(trxn_.id)
               col_node->SetTransaction();
           uk->SetLastTGID(col_id, trxn_.id >> TGID_SHIFT);
           uk->SetRequestNode(col_id, req_node);
           assert(uk->GetReferenceCount());
       }else if((trxn_.id >> TGID_SHIFT) != uk->SetLastTGID(col_id, trxn_.id >> TGID_SHIFT)){
           uk->GetRequestNode(col_id)->SetRequestNodeLatestColumnNode(uk->GetLatestColumnNode(col_id));
           uk->GetLatestColumnNode(col_id)->SetTGIDSplit();//For processing recently inserted RequestNode first
           req_node = mf_->AllocRequestNode();
           req_node->Init(uk, col_id, trxn_);
           if(trxn_.id)
               col_node->SetTransaction();
           uk->SetRequestNode(col_id, req_node);
           assert(uk->GetReferenceCount());

       }else{
           assert(sched_stat == kHasBeenScheduled);
           if(trxn_.id){
               uk->GetRequestNode(col_id)->InsertTrxn(trxn_);
               col_node->SetTransaction();
           }
           /* 
            * Decrease reference count for merged column 
            * The header increased the ref count. So, it can not evicted until submitting the header
            */
           uk->DecreaseReferenceCount();
           uk->SetLatestColumnNode(col_id, col_node);
           uk->ColumnUnlock(col_id); /*uk->col_lock[col_id].Unlock();*/
       }
       if(req_node/*sched_stat == kNeedToSchedule*/){
           uk->SetLatestColumnNode(col_id, col_node);
           uk->ColumnUnlock(col_id); /*uk->col_lock[col_id].Unlock();*/
           dbimpl_->PutColumnNodeToRequestQueue(req_node, skt);
       }
       trxn_.seq_number++;
       /** 
        * Decrease in the Worker after submitting the command 
        * uk->DecreaseReferenceCount();
        */
   }


    Status ReadRecordFromWriteBatch(Slice* input, char* tag,
            uint16_t* column_family, Slice* key,
            Slice* value, Slice* blob, Slice* xid) {
        assert(key != nullptr && value != nullptr);
        *tag = (*input)[0];
        input->remove_prefix(1);
        *column_family = 0;  // default
        uint32_t len = 0;
#ifdef VARTOFIXOPT
        len = DecodeFixed32(input->data());
        assert(len <= input->size());
        input->remove_prefix(4);
        *key = Slice(input->data(), len);
        input->remove_prefix(len);
#endif
        switch (*tag) {
            case kTypeValue:
#ifdef VARTOFIXOPT
                len = DecodeFixed32(input->data());
                assert(len <= input->size());
                input->remove_prefix(4);
                *value = Slice(input->data(), len);
                input->remove_prefix(len);
                *column_family = DecodeFixed16(input->data());
                input->remove_prefix(2);
#else
                if (!GetLengthPrefixedSlice(input, key) ||
                        !GetLengthPrefixedSlice(input, value)) {
                    return Status::Corruption("bad WriteBatch Put");
                }
                if (!GetVarint16(input, column_family)) {
                    return Status::Corruption("bad WriteBatch Put");
                }
#endif
                break;
            case kTypeDeletion:
#ifdef VARTOFIXOPT
                *column_family = DecodeFixed16(input->data());
                input->remove_prefix(2);
#else
                if (!GetLengthPrefixedSlice(input, key)) {
                    return Status::Corruption("bad WriteBatch Delete");
                }
                if (!GetVarint16(input, column_family)) {
                    return Status::Corruption("bad WriteBatch Delete");
                }
#endif
                break;
            case kTypeMerge:
#ifdef VARTOFIXOPT
                len = DecodeFixed32(input->data());
                assert(len <= input->size());
                input->remove_prefix(4);
                *value = Slice(input->data(), len);
                input->remove_prefix(len);
                *column_family = DecodeFixed16(input->data());
                input->remove_prefix(2);
#else
                if (!GetLengthPrefixedSlice(input, key) ||
                        !GetLengthPrefixedSlice(input, value)) {
                    return Status::Corruption("bad WriteBatch Merge");
                }
                if (!GetVarint16(input, column_family)) {
                    return Status::Corruption("bad WriteBatch Merge");
                }
#endif
                break;
            default:
                return Status::Corruption("unknown WriteBatch tag");
        }
        return Status::OK();
    }

    bool ReadKeyFromWriteBatchEntry(Slice* input, Slice* key, bool cf_record) {
        assert(input != nullptr && key != nullptr);
        // Skip tag byte
        input->remove_prefix(1);

#ifdef VARTOFIXOPT
        uint32_t len = DecodeFixed32(input->data());
        assert(len <= input->size());
        input->remove_prefix(4);
        *key = Slice(input->data(), len);
        input->remove_prefix(len);
        return true;
#else
        return GetLengthPrefixedSlice(input, key);
#endif
    }

}  // namespace insdb
