/*
 * NVM Express Key-value device driver
 * Copyright (c) 2016-2018, Samsung electronics.
 *
 * Author : Heekwon Park
 * E-mail : heekwon.p@samsung.com
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 */
#include <linux/kernel.h>
#include <linux/jiffies.h>

#include <asm/atomic.h>
#include <asm/bitops.h>
#include "kv_iosched.h"
/////////////////////////////////////////////////////////////
#if 0
static inline unsigned int fold_hash(unsigned long x, unsigned long y)
{
    y ^= x * GOLDEN_RATIO_64;
    y *= GOLDEN_RATIO_64;
    return y >> 32;
}
/*
 * This is for 16 bytes key in PHX
 */
uint128_t key;
if(rq->metadata_len > 16){
    key.m_low = (u64)full_name_hash(NULL,rq->metadata,rq->metadata_len);
    key.m_high = 0;
}else if(rq->metadata_len > 8){
    key.m_low = *((u64 *)(rq->metadata));
    key.m_high = *(((u64 *)(rq->metadata))+1);
}else{
    key.m_low = *((u64 *)(rq->metadata));
    key.m_high = 0;
}
#endif

static inline int longcmp(const u64 *l1, const u64  *l2, size_t count)
{
    size_t i;

    for (i = 0; i < count; i++) {
        if (l1[i] != l2[i])
            return -1;
    }
    return 0;
}

void kv_hash_init(struct kv_hash *hash)
{
    int i;

    for (i = 0; i < KV_HASH_SIZE; i++){
        INIT_HLIST_HEAD(&hash->hashtable[i]);
        spin_lock_init(&hash->hash_lock[i]);
    }
    atomic_set(&hash->nr_rq, 0);
}

static inline struct kv_async_request *kv_hash_search(struct hlist_head *head, struct kv_hash *hash, void *key, u32 key_length){

    struct kv_async_request *rq;

    u32 word_cnt = key_length / 8;//BIT_WORD(key_length);/* count in u64 */
    u32 remain = key_length & 7;//key_length & (BITS_PER_LONG-1); /* remaining value */

    hlist_for_each_entry(rq, head, hash) {
        if(rq->key_length != key_length)
            continue;
        if(word_cnt && longcmp((u64 *)rq->key_addr, (u64 *)key, word_cnt ))
            continue;
        if(remain && memcmp((char *)(((u64 *)rq->key_addr) + word_cnt), (char *)(((u64 *)key) + word_cnt), remain))
            continue;
        /* Increase reference counter to prevent freeing the request */
        atomic_inc(&rq->refcount);
        break;
    }
#if 0
    /* Move the following code into the above loop */
    if(rq) atomic_inc(&rq->refcount);
#endif
    return rq;
}

/*
 * If request hash does not have same key, insert a new async request 
 */
#define UNLOCK_AND_RESTART(rq, hash) \
        if(atomic_dec_and_test(&rq->refcount)) { \
            spin_unlock(&hash->hash_lock[rq->hash_key]); \
            free_kv_async_rq(rq); \
         } else { \
            spin_unlock(&rq->lock); \
            spin_unlock(&hash->hash_lock[rq->hash_key]); \
         } \
        rq=NULL; \
        goto restart;
struct kv_async_request *kv_hash_insert(struct kv_hash *hash, struct kv_async_request *rq)
{
    struct kv_async_request *buffered_rq;
    struct hlist_head *head = hash->hashtable + rq->hash_key;
    /*
     * Invalid rq can not exist in submission Q
     */
restart:
    spin_lock(&hash->hash_lock[rq->hash_key]);
    buffered_rq = kv_hash_search(head, hash, rq->key_addr, rq->key_length);
    if(!buffered_rq){
        /*
         * No same key exists
         * Insert the new async rq to RQ Hash 
         */
        hlist_add_head(&rq->hash, head);
        atomic_inc(&hash->nr_rq);
        spin_unlock(&hash->hash_lock[rq->hash_key]);
    }else{ 
        spin_lock(&buffered_rq->lock);
        /* The buffered request has been completed by completion daemon */
        if(test_bit(KV_async_completed, &buffered_rq->state)){
#ifndef NO_LOG
            atomic64_inc(&kv_hash_restart[KV_hash_com_insert]);
#endif
            UNLOCK_AND_RESTART(buffered_rq, hash);
            /* The buffered request has been invalidated by sync request */
        }else if(test_bit(KV_async_invalid_rq, &buffered_rq->state) ){
#ifndef NO_LOG
            atomic64_inc(&kv_hash_restart[KV_hash_inv_insert]);
#endif
            UNLOCK_AND_RESTART(buffered_rq, hash);
        /* The buffered request becomes blocking in-flight rq by another async request */
        }else if(test_bit(KV_async_blocking_rq, &buffered_rq->state)){
#ifndef NO_LOG
            atomic64_inc(&kv_hash_restart[KV_hash_blk_insert]);
#endif
            UNLOCK_AND_RESTART(buffered_rq, hash);
        }else if(test_bit(KV_async_inFlight_rq, &buffered_rq->state)){

           /*
           * if this code acquires lock, completion daemon should wait hash lock.
           * if completion daemon acquires lock, it means that the rq is not exist in RQ hash anymore.
           * In this case, the thread can not be here.
           */
           if(hlist_unhashed(&buffered_rq->hash)) 
               pr_err("[%d:%s]It is already Unhashed!\n", __LINE__, __func__);
           hlist_del_init(&buffered_rq->hash);
           hlist_add_head(&rq->hash, head);
           spin_unlock(&hash->hash_lock[rq->hash_key]);
           /*
            * Don't need to do following code
            * atomic_inc(&hash->nr_rq); => for new rq
            * atomic_dec(&hash->nr_rq); => for buffered_rq
            */

           set_bit(KV_async_blocking_rq, &buffered_rq->state);
           rq->blocking_rq = buffered_rq;
           /*
            * We don't know the memq where this request will be inserted.
            * We can check rq->blocking_rq to check whether it should be blocked or not after assign a memq.
            * atomic_inc(&rq->memq->nr_pending);
            */

           /*
            * The buffered rq will be unlocked after atomic_inc(memq->nr_pending)
            */
           buffered_rq = NULL;
        }else
            spin_unlock(&hash->hash_lock[rq->hash_key]);
        /* 
         * Otherwise(do not execute above if-else if statement), 
         * the buffered RQ is normal buffered or pending rq 
         * It already acquired the lock,
         * so that completion daemon cannot change the status 
         * until the data is updated
         */
    }
    return buffered_rq;
}

struct kv_async_request *kv_hash_insert_readahead(struct kv_hash *hash, struct kv_async_request *rq)
{
    struct kv_async_request *buffered_rq;
    struct hlist_head *head = hash->hashtable + rq->hash_key;
    /*
     * Invalid rq can not exist in submission Q
     */
    spin_lock(&hash->hash_lock[rq->hash_key]);
    buffered_rq = kv_hash_search(head, hash, rq->key_addr, rq->key_length);
#if 0
    if(!buffered_rq){
        /*
         * No same key exists
         * Insert the new async rq to RQ Hash 
         */
        spin_lock(&rq->iosched->ra_lock);
        list_add(&rq->lru, &rq->iosched->ra_list);
        spin_unlock(&rq->iosched->ra_lock);
        hlist_add_head(&rq->hash, head);
        atomic_inc(&hash->nr_rq);
        spin_unlock(&hash->hash_lock[rq->hash_key]);
    }else 
        spin_unlock(&hash->hash_lock[rq->hash_key]);
#else
    if(!buffered_rq){
        /*
         * No same key exists
         * Insert the new async rq to RQ Hash 
         */
        spin_lock(&rq->iosched->ra_lock);
        list_add(&rq->lru, &rq->iosched->ra_list);
        spin_unlock(&rq->iosched->ra_lock);
        hlist_add_head(&rq->hash, head);
        atomic_inc(&hash->nr_rq);
    }
    spin_unlock(&hash->hash_lock[rq->hash_key]);
#endif
    return buffered_rq;
}

/*
 * Look up the READAHEAD rq
 */
struct kv_async_request *kv_hash_find(struct kv_hash *hash, void *key, __u32 key_length, u32 hash_key, bool lock)
{
    struct kv_async_request *rq;

restart:
    spin_lock(&hash->hash_lock[hash_key]);
    rq = kv_hash_search(hash->hashtable + hash_key, hash, key, key_length);
    spin_unlock(&hash->hash_lock[hash_key]);
    if(rq){
        if(lock) spin_lock(&rq->lock);
        if(atomic_dec_and_test(&rq->refcount)){
            free_kv_async_rq(rq);
            rq = NULL;
            goto restart;
        }else if(lock) 
            /* Reture locked rq to implement SaS */
            spin_unlock(&rq->lock);
    }
    return rq;
}

/*
 * Look up a req hash table
 * Make sure the caller releases one reference acquired in kv_hash_search()
 */
struct kv_async_request *kv_hash_find_refcnt(struct kv_hash *hash, void *key, __u32 key_length, u32 hash_key)
{
    struct kv_async_request *rq;

    spin_lock(&hash->hash_lock[hash_key]);
    rq = kv_hash_search(hash->hashtable + hash_key, hash, key, key_length);
    spin_unlock(&hash->hash_lock[hash_key]);

    return rq;
}

/*
 * decrease req reference count.
 * set lock to false, if req lock is already acquired. set to true otherwise.
 * return true if the req is freed so that caller should not release its lock
 */
bool kv_decrease_req_refcnt(struct kv_async_request *rq, bool lock)
{
    if(lock)
        spin_lock(&rq->lock);
    if(atomic_dec_and_test(&rq->refcount)){
        free_kv_async_rq(rq);
        return true;
    }
    if(lock)
        spin_unlock(&rq->lock);
    return false;
}


/*
 * Check whether same key exists in the hash or not for sync request
 */
struct kv_async_request *kv_hash_find_and_lock(struct kv_hash *hash, void *key, u32 key_length, u32 hash_key, bool *inFlight)
{
    struct kv_async_request *rq;

restart:
    spin_lock(&hash->hash_lock[hash_key]);
    /*
     * Invalid or blocking(<in-flight>)rq can not exist in the request hash
     */
    rq = kv_hash_search(hash->hashtable + hash_key, hash, key, key_length);
    /*
     * Hash must be unlocked here
     * if in-flight request is completed 
     * && completion daemon has the in-flight rq's lock
     * && the in-flight rq has pending rq
     * => in this situation, the same hash lock will be used for get the pending rq in completion daemon.
     * => But, if it does not release the hash lock here and then try to get the in-flight rq's, it makes dead lock condition.
     *
     */
    if(rq){
        spin_lock(&rq->lock);
        if(test_bit(KV_async_completed, &rq->state)){
#ifndef NO_LOG
            atomic64_inc(&kv_hash_restart[KV_hash_com_sync]);
#endif
            UNLOCK_AND_RESTART(rq, hash);
        }else if(test_bit(KV_async_invalid_rq, &rq->state)){
#ifndef NO_LOG
            atomic64_inc(&kv_hash_restart[KV_hash_inv_sync]);
#endif
            UNLOCK_AND_RESTART(rq, hash);
        }else if(test_bit(KV_async_blocking_rq, &rq->state)){
#ifndef NO_LOG
            atomic64_inc(&kv_hash_restart[KV_hash_blk_sync]);
#endif
            UNLOCK_AND_RESTART(rq, hash);
        }else if(test_bit(KV_async_inFlight_rq, &rq->state)){
            spin_unlock(&rq->lock);
            spin_unlock(&hash->hash_lock[hash_key]);

            wait_on_bit(&rq->state, KV_async_inFlight_rq, TASK_UNINTERRUPTIBLE);

            spin_lock(&rq->lock);
            if(atomic_dec_and_test(&rq->refcount)){
                pr_err("[%d:%s]ERROR : retcount of inFlight_rq must not be 0\n", __LINE__, __func__);
                free_kv_async_rq(rq);
            }else {
                spin_unlock(&rq->lock);
            }
            /* 
             * Do not check RQ again because the others were requested after this request.
             * In this case, it is necessary to check RA hash again 
             * It is possible to submit the same key by async requested after sync & sync request.
             *  - To avoid this, do not clear the blocking_bit before sync rq is complete. 
             *  - But we do not avoid it for performance.
             *  - This situation is not only what happens here, but it can also be caused
             *    by any asynchronous operation with the same key requested after synchronization. 
             */
            *inFlight = true;
            rq = NULL;
        }else if(rq->blocking_rq){/* nvme_cmd_kv_store*/
            spin_unlock(&rq->lock);
            spin_unlock(&hash->hash_lock[hash_key]);


            wait_on_bit(&rq->blocking_rq->state, KV_async_inFlight_rq, TASK_UNINTERRUPTIBLE);

            /*
             * Must release lock at here for the completion daemon. 
             * The daemon need to acquire pending rq lock while having inFlight_rq lock.
             */
            spin_lock(&rq->lock);
            if(atomic_dec_and_test(&rq->refcount)){
                pr_err("[%d:%s]ERROR : retcount of pending rq must not be 0\n", __LINE__, __func__);
                free_kv_async_rq(rq);
            }else {
                spin_unlock(&rq->lock);
            }

            rq = NULL;
            goto restart;
        }else{
            /* 
             * This is a normal buffered RQ.
             * It will be invalidated and removed from the hash.
             */
            set_bit(KV_async_invalid_rq, &rq->state);
            hlist_del_init(&rq->hash);
            spin_unlock(&hash->hash_lock[hash_key]);
            atomic_dec(&hash->nr_rq);
        }
    }else
        spin_unlock(&hash->hash_lock[hash_key]);
    return rq;
}

struct kv_async_request *kv_hash_find_read_and_invalidate(struct kv_hash *hash, void *key, u32 key_length, u32 hash_key)
{

    struct kv_async_request *rq;

restart:
    spin_lock(&hash->hash_lock[hash_key]);
    /*
     * Invalid or blocking(<in-flight>)rq can not exist in the request hash
     */
    rq = kv_hash_search(hash->hashtable + hash_key, hash, key, key_length);
    /*
     * Hash must be unlocked here
     * if in-flight request is completed 
     * && completion daemon has the in-flight rq's lock
     * && the in-flight rq has pending rq
     * => in this situation, the same hash lock will be used for get the pending rq in completion daemon.
     * => But, if it does not release the hash lock here and then try to get the in-flight rq's, it makes dead lock condition.
     *
     */
    if(rq){
        spin_lock(&rq->lock);
        if(test_bit(KV_async_completed, &rq->state)){
#ifndef NO_LOG
            atomic64_inc(&kv_hash_restart[KV_hash_com_sync]);
#endif
            UNLOCK_AND_RESTART(rq, hash);
        }else if(test_bit(KV_async_invalid_rq, &rq->state)){
#ifndef NO_LOG
            atomic64_inc(&kv_hash_restart[KV_hash_inv_sync]);
#endif
            UNLOCK_AND_RESTART(rq, hash);
        }else if(test_bit(KV_async_blocking_rq, &rq->state)){
#ifndef NO_LOG
            atomic64_inc(&kv_hash_restart[KV_hash_blk_sync]);
#endif
            UNLOCK_AND_RESTART(rq, hash);
        }else if(test_bit(KV_async_inFlight_rq, &rq->state)){
            spin_unlock(&rq->lock);
            spin_unlock(&hash->hash_lock[hash_key]);

            wait_on_bit(&rq->state, KV_async_inFlight_rq, TASK_UNINTERRUPTIBLE);

            spin_lock(&rq->lock);
            if(atomic_dec_and_test(&rq->refcount)){
                pr_err("[%d:%s]ERROR : retcount of inFlight_rq must not be 0\n", __LINE__, __func__);
                free_kv_async_rq(rq);
            }else {
                spin_unlock(&rq->lock);
            }
            /* 
             * Do not check RQ again because the others were requested after this request.
             * In this case, it is necessary to check RA hash again 
             * It is possible to submit the same key by async requested after sync & sync request.
             *  - To avoid this, do not clear the blocking_bit before sync rq is complete. 
             *  - But we do not avoid it for performance.
             *  - This situation is not only what happens here, but it can also be caused
             *    by any asynchronous operation with the same key requested after synchronization. 
             */
            rq = NULL;
        }else if(rq->blocking_rq){/* nvme_cmd_kv_store*/
            spin_unlock(&rq->lock);
            spin_unlock(&hash->hash_lock[hash_key]);

            wait_on_bit(&rq->blocking_rq->state, KV_async_inFlight_rq, TASK_UNINTERRUPTIBLE);

            /*
             * Must release lock at here for the completion daemon. 
             * The daemon need to acquire pending rq lock while having inFlight_rq lock.
             */
            spin_lock(&rq->lock);
            if(atomic_dec_and_test(&rq->refcount)){
                pr_err("[%d:%s]ERROR : retcount of pending rq must not be 0\n", __LINE__, __func__);
                free_kv_async_rq(rq);
            }else {
                spin_unlock(&rq->lock); 
            }

            rq = NULL;
            goto restart;
        }else{
            /* 
             * This is a normal buffered RQ.
             * It will be invalidated and removed from the hash.
             */
            if(is_kv_retrieve_cmd(rq->c.common.opcode)){
                set_bit(KV_async_invalid_rq, &rq->state);
                hlist_del_init(&rq->hash);
                atomic_dec(&hash->nr_rq);
            }
            if(atomic_dec_and_test(&rq->refcount)){
                spin_unlock(&hash->hash_lock[hash_key]);
                pr_err("[%d:%s]ERROR : retcount of pending rq must not be 0\n", __LINE__, __func__);
                free_kv_async_rq(rq);
            }else{
                spin_unlock(&rq->lock); 
                spin_unlock(&hash->hash_lock[hash_key]);
            }
        }
    }else
        spin_unlock(&hash->hash_lock[hash_key]);
    return rq;
}


struct kv_async_request *kv_hash_find_and_lock_pending_rq(struct kv_hash *hash, void *key, u32 key_length, u32 hash_key)
{

    struct kv_async_request *rq;

    spin_lock(&hash->hash_lock[hash_key]);
    /*
     * Invalid rq can not exist in submission Q
     */
    rq = kv_hash_search(hash->hashtable + hash_key, hash, key, key_length);
    /* Hash unlock must be conducted before get rq lock, because new async rq may wait the in-flight is done */
    spin_unlock(&hash->hash_lock[hash_key]);
    if(rq) {
        spin_lock(&rq->lock);
#if 0
        /* 
         * This code is for debugging perpose 
         * Pending rq can not change its state.
         */
        if(test_bit(KV_async_completed, &rq->state)){
            pr_err("[%d:%s]ERROR : Pending RQ can not be completed(0x%lx)[key : %s(hash : 0x%x)]\n", __LINE__, __func__, rq->state, (char*)key, hash_key);
            if(atomic_dec_and_test(&rq->refcount))
                free_kv_async_rq(rq);
            else
                spin_unlock(&rq->lock);
            goto restart;
        }
        if(test_bit(KV_async_invalid_rq, &rq->state) || test_bit(KV_async_blocking_rq, &rq->state)){
            pr_err("[%d:%s]ERROR : The buffered rq found is in an unexpected state(0x%lx)[key : %s(hash : 0x%x)]\n", __LINE__, __func__, rq->state, (char*)key, hash_key);
            spin_unlock(&rq->lock);
            rq = NULL;
            goto restart;
        }
#endif
        if(!rq->blocking_rq)
            pr_err("[%d:%s]ERROR : The buffered RQ found is not pending rq(%d : %s)\n", __LINE__, __func__, current->pid, current->comm);

    }else
        pr_err("[%d:%s]ERROR : A buffered RQ having same key does not exist(%d : %s)\n", __LINE__, __func__, current->pid, current->comm);

    return rq;
}

struct kv_async_request *kv_hash_find_and_del_readahead(struct kv_hash *hash, void *key, u32 key_length, u32 hash_key)
{
    struct kv_async_request *rq;

    spin_lock(&hash->hash_lock[hash_key]);
    rq = kv_hash_search(hash->hashtable + hash_key, hash, key, key_length);
    if(rq){
        /* rq lock is not neccessary because it is readahead hash. it means the the rq update does not happen*/
        spin_lock(&rq->iosched->ra_lock);
        list_del_init(&rq->lru);
        spin_unlock(&rq->iosched->ra_lock);
        hlist_del_init(&rq->hash);
        atomic_dec(&hash->nr_rq);
        atomic_dec(&rq->refcount);
    }
    spin_unlock(&hash->hash_lock[hash_key]);

    return rq;
}

int kv_hash_delete_readahead(struct kv_hash *hash, struct kv_async_request *rq)
{
    int ret = 0;
    spin_lock(&hash->hash_lock[rq->hash_key]);
    if(hlist_unhashed(&rq->hash)){ //the in-flight can be unhashed by async request having same key
        ret = 1;
    }else{
        spin_lock(&rq->iosched->ra_lock);
        list_del_init(&rq->lru);
        spin_unlock(&rq->iosched->ra_lock);
        hlist_del_init(&rq->hash);
        atomic_dec(&hash->nr_rq);
    }
    spin_unlock(&hash->hash_lock[rq->hash_key]);
    return ret;
}
int kv_hash_delete(struct kv_hash *hash, struct kv_async_request *rq)
{
    int ret = 0;
    spin_lock(&hash->hash_lock[rq->hash_key]);
    if(hlist_unhashed(&rq->hash)){ //the in-flight can be unhashed by async request having same key
        ret = 1;
    }else{
        hlist_del_init(&rq->hash);
        atomic_dec(&hash->nr_rq);
    }
    spin_unlock(&hash->hash_lock[rq->hash_key]);
    return ret;
}

