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
#include <scsi/scsi_cmnd.h>
#include <linux/time.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include "nvme.h"
#include "linux_nvme_ioctl.h"
//#include "fabrics.h"
#include "kv_iosched.h"
#include "kv_fifo.h"

static void kv_end_io_sync_rq(struct request *rq, int error)
{
    struct completion *waiting = rq->end_io_data;

    rq->end_io_data = NULL;

    /*
     * complete last, if this is a stack request the process (and thus
     * the rq pointer) could be invalid right after this complete()
     */
    complete(waiting);
}

static void kv_end_io_async_rq(struct request *req, int error)
{
    struct kv_async_request *rq =(struct kv_async_request *) req->end_io_data;
    req->end_io_data = NULL;
  
    // req may not be referenced after it's queued to complete fifo.
    while(!kv_fifo_enqueue(&rq->iosched->complete_fifo, rq))
        pr_err("[%d:%s] :  complete_fifo has been full(key : %s)\n", __LINE__, __func__, (char *)rq->key_addr);

#ifndef NO_LOG
    atomic64_inc(&kv_total_async_interrupt);
#endif
    atomic64_inc(&rq->iosched->nr_async_interrupt);

    spin_lock(&rq->iosched->complete_lock);
#ifdef DELAYED_COMPLETE_WORK
    /* In order to 
     * 1. Avoid queue_work bug
     *    Occasionally, queue_work is in the running state after a job is finished!
     *    In this case, the completion daemon can not wake up again.
     * 2. Reduce the number of wake up times 
     */
    if(queue_delayed_work(rq->iosched->complete_wq, &rq->iosched->complete_dwork, rq->iosched->complete_delay)){
#ifndef NO_LOG
        atomic64_inc(&kv_completion_wakeup_by_int); 
#endif
    }
    if(test_bit(KV_async_flush_wakeup, &rq->state) || rq->iosched->complete_max_rq <= atomic_read(&rq->iosched->complete_fifo.nr_ele)){
        mod_delayed_work(rq->iosched->complete_wq, &rq->iosched->complete_dwork, 0);
    }
#else
    if(!work_busy(&rq->iosched->complete_work) && queue_work(rq->iosched->complete_wq, &rq->iosched->complete_work))
    {
#ifndef NO_LOG
        atomic64_inc(&kv_completion_wakeup_by_int); 
#endif
    }
#endif
    spin_unlock(&rq->iosched->complete_lock);
}


static void kv_blk_rq_bio_prep(struct request_queue *q, struct request *rq,
        struct bio *bio)
{
    req_set_op(rq, bio_op(bio));

    if (bio_has_data(bio))
        rq->nr_phys_segments = bio_phys_segments(q, bio);

    rq->__data_len = bio->bi_size;
    rq->bio = rq->biotail = bio;

    if (bio->bi_bdev)
        rq->rq_disk = bio->bi_bdev->bd_disk;
}



static inline void free_kv_sync_rq(struct kv_sync_request *rq)
{

    if(rq->key_length > KVCMD_INLINE_KEY_MAX && rq->key_addr){
        kfree(rq->key_addr);
        rq->key_addr = NULL;
    }
    if(rq->udata_addr)
        rq->udata_addr = NULL;
    rq->udata_length = 0;
    rq->key_length = 0;
    kv_sync_request_free(rq);
}
#define timeval_to_msec(__val)	\
	jiffies_to_msecs(timeval_to_jiffies(__val))
#define timeval_to_usec(__val)	\
	jiffies_to_usecs(timeval_to_jiffies(__val))
static int __nvme_submit_kv_user_cmd(struct kv_sync_request *sync_rq)
{
    ////////////// 
    struct request_queue *q = sync_rq->q; 
    struct nvme_command *cmd = &sync_rq->c;
    void __user *udata_addr= sync_rq->udata_addr; 
    unsigned udata_length = sync_rq->udata_length;
    void *key_addr = sync_rq->key_addr; 
    unsigned key_length = sync_rq->key_length;
    u32 *result = &sync_rq->result; 
    unsigned timeout = sync_rq->timeout;
    //////////////

    struct nvme_completion cqe;
    struct nvme_ns *ns = q->queuedata;
    struct gendisk *disk = ns ? ns->disk : NULL;
    struct request *req;
    struct bio *bio = NULL;
    int ret;

    if (!is_kv_cmd(cmd->common.opcode))
        return -EINVAL;
    if (!disk)
        return -ENODEV;
    req = nvme_alloc_request(q, cmd, 0, NVME_QID_ANY);
    if (IS_ERR(req))
        return PTR_ERR(req);

    req->timeout = timeout ? timeout : ADMIN_TIMEOUT;
    req->special = &cqe;

    /*
     * A sync request always uses user memory,
     * even if the request was async request which uses kernel buffer.
     * The kernel buffer has been copied to user buffer.
     */
    if (udata_addr && udata_length) {

        /* The request is from User */
        ret = blk_rq_map_user(q, req, NULL, udata_addr, udata_length,
                GFP_KERNEL);
        if (ret)
            goto out;
        bio = req->bio;

        bio->bi_bdev = bdget_disk(disk, 0);
        if (!bio->bi_bdev) {
            ret = -ENODEV;
            goto out_unmap;
        }
    }else{/* For Delete Command */
        // build temp_bio for meta_data
        // - need prvent to call blk_rq_unmap_user.
        bio = bio_kmalloc(GFP_KERNEL, 1);
        if (!bio) {
            ret = -ENODEV;
            goto out;
        }
#if 0
        ret = blk_rq_append_bio(req, bio);
        if(ret) {
            ret = -EFAULT;
            bio_put(bio);
            goto out;
        }
#else
        kv_blk_rq_bio_prep(req->q, req, bio);
#endif
    }


    /* An iterate command has NO memory for key */
    if (key_addr && key_length > KVCMD_INLINE_KEY_MAX) {
        struct bio_integrity_payload *bip;
        bip = bio_integrity_alloc(bio, GFP_KERNEL, 1);
        if (IS_ERR(bip)) {
            ret = PTR_ERR(bip);
            goto out_unmap;
        }

        bip->bip_size = key_length;
        bip->bip_size = 0;
        ret = bio_integrity_add_page(bio, virt_to_page(key_addr),
                key_length, offset_in_page(key_addr));
        if (ret != key_length) {
            ret = -ENOMEM;
            goto out_unmap;
        }
    }

#ifdef KV_NVME_TRACE
    pr_err("[%d:%s] :  opcode(%02x)\n", __LINE__, __func__, cmd->common.opcode);
#endif
    {
        DECLARE_COMPLETION_ONSTACK(wait);
        char sense[SCSI_SENSE_BUFFERSIZE];
#ifndef NO_LOG
    struct timeval wait_start, wait_stop;
#endif

        memset(sense, 0, sizeof(sense));
        req->sense = sense;
        req->sense_len = 0;

        req->end_io_data = &wait;
        
        blk_execute_rq_nowait(req->q, disk, req, 0, kv_end_io_sync_rq);

#ifndef NO_LOG
            do_gettimeofday(&wait_start);
#endif
        /* Prevent hang_check timer from firing at us during very long I/O */
        while (!wait_for_completion_io_timeout(&wait, 120* (HZ/2)));
#ifndef NO_LOG
            do_gettimeofday(&wait_stop);
            atomic64_inc(&kv_total_sync_wait_time);
            atomic64_add((timeval_to_usec(&wait_stop)-timeval_to_usec(&wait_start)), &kv_total_sync_wait_time);
#endif
#ifndef NO_LOG
        atomic64_inc(&kv_total_sync_complete); 
#endif
    }

    /* return success if the key does exist. set the error code in user command instead */
    if(req->errors == 0x310) {
        put_user(KVS_ERR_KEY_NOT_EXIST, &sync_rq->ucmd->status);
        ret = 0;
    } else {
        ret = req->errors;
    }
    if (result)
        *result = le32_to_cpu(cqe.result.u32);
        
    /*if (result)
		    *result = le32_to_cpu(nvme_req(req)->result.u32);*/

out_unmap:
    if (bio && udata_addr) {
        if (disk && bio->bi_bdev)
            bdput(bio->bi_bdev);
        if (is_kv_cmd(cmd->common.opcode) && 
                req->bio && bio_integrity(req->bio)) {
            bio_integrity_free(req->bio);
        }
        blk_rq_unmap_user(bio);
    }else if(bio)/*Delete command*/
        bio_put(bio);
out:
    blk_mq_free_request(req);
    return ret;
}

static inline void set_kv_cmd(struct nvme_command *c, struct nvme_passthru_kv_cmd *cmd) 
{
    c->common.opcode = cmd->opcode;
    c->common.flags = cmd->flags;
    c->common.nsid = cpu_to_le32(cmd->nsid);
    switch(cmd->opcode) {
        case nvme_cmd_kv_store:
            c->kv_store.option = (cpu_to_le32(cmd->cdw4) & 0xff);
            c->kv_store.offset = cpu_to_le32(cmd->cdw5);
            c->kv_store.key_len = cpu_to_le32(cmd->key_length -1); /* key len -1 */
            c->kv_store.value_len = cpu_to_le32(cmd->data_length >> 2);
            if (cmd->key_length <= KVCMD_INLINE_KEY_MAX)
                memcpy(c->kv_store.key, cmd->key, cmd->key_length);
            break;
        case nvme_cmd_kv_retrieve:
            c->kv_retrieve.option = (cpu_to_le32(cmd->cdw4) & 0xff);
            c->kv_retrieve.offset = cpu_to_le32(cmd->cdw5);
            c->kv_retrieve.key_len = cpu_to_le32(cmd->key_length -1); /* key len - 1 */
            c->kv_retrieve.value_len = cpu_to_le32(cmd->data_length >> 2);
            if (cmd->key_length <= KVCMD_INLINE_KEY_MAX)
                memcpy(c->kv_retrieve.key, cmd->key, cmd->key_length);
            break;
        case nvme_cmd_kv_delete:
            c->kv_delete.option = (cpu_to_le32(cmd->cdw4) & 0xff);
            c->kv_delete.key_len = cpu_to_le32(cmd->key_length -1);
            if (cmd->key_length <= KVCMD_INLINE_KEY_MAX)
                memcpy(c->kv_delete.key, cmd->key, cmd->key_length);
            break;
        case nvme_cmd_kv_iter_req:
            c->kv_iter_req.option = (cpu_to_le32(cmd->cdw4) & 0xff);
            c->kv_iter_req.iter_handle = (cpu_to_le32(cmd->cdw5) & 0xff);
            c->kv_iter_req.iter_val = cpu_to_le32(cmd->cdw12);
            c->kv_iter_req.iter_bitmask = cpu_to_le32(cmd->cdw13);
            break;
        case nvme_cmd_kv_iter_read:
            c->kv_iter_read.option = (cpu_to_le32(cmd->cdw4) & 0xff);
            c->kv_iter_read.iter_handle = (cpu_to_le32(cmd->cdw5) & 0xff);
            c->kv_iter_read.value_len = cpu_to_le32(cmd->data_length >> 2);
            break;
#if 0
        case nvme_cmd_kv_exist:
            c->kv_exist.key_len_flag = cpu_to_le32(cmd->cdw5); /* fixed or variable */
            c->kv_exist.num_keys = cpu_to_le32(cmd->cdw13); /* num key -1 */
            c->kv_exist.key_len = cpu_to_le32(cmd->cdw14); /* key len -1 */
            c->kv_exist.result_len = cpu_to_le32(cmd->key_length); /* exist result use key_lenth and key_addr */
            break;
        case nvme_cmd_kv_iterate:
            c->kv_iterate.key_len_flag = cpu_to_le32(cmd->cdw5);
            c->kv_iterate.iterator = cmd->cdw10 & 0x00ffffff;
            //c->kv_iterate.start = cmd->cdw11; /* not in spec */
            c->kv_iterate.bitmask = cmd->cdw14;
            c->kv_iterate.result_len = cpu_to_le32(cmd->data_length);
            break;
#endif
        default:
            break;
    }
}
static void init_kv_sync_request(struct kv_sync_request *rq, void *key, struct nvme_passthru_kv_cmd  *cmd, struct request_queue *queue) 
{
    rq->q = queue;

    memset(&rq->c, 0, sizeof(rq->c));
    set_kv_cmd(&rq->c, cmd); 
    switch(cmd->opcode) {
        case nvme_cmd_kv_store:
#ifndef NO_LOG
            atomic64_inc(&kv_sync_put_req);
#endif
            break;
        case nvme_cmd_kv_retrieve:
#ifndef NO_LOG
            atomic64_inc(&kv_sync_get_req);
#endif
            break;
        case nvme_cmd_kv_delete:
#ifndef NO_LOG
            atomic64_inc(&kv_sync_del_req);
#endif
            break;
        case nvme_cmd_kv_iter_req:
#ifndef NO_LOG
            atomic64_inc(&kv_sync_iter_req);
#endif
            break;
        case nvme_cmd_kv_iter_read:
#ifndef NO_LOG
            atomic64_inc(&kv_sync_iter_read_req);
#endif
            break;
#if 0
        case nvme_cmd_kv_exist:
            break;
        case nvme_cmd_kv_iterate:
            break;
#endif
        default:
            break;
    }





    if (cmd->timeout_ms)
        rq->timeout = msecs_to_jiffies(cmd->timeout_ms);
    rq->udata_addr= (void __user *)cmd->data_addr;
    rq->udata_length = cmd->data_length;
    rq->key_length= cmd->key_length;
    /* set metadata len on CDW10 */
    rq->key_addr = key;
    /*
     * the result must be copied to cmd->result after request is done.
     */
    rq->result = 0;//&cmd->result
    rq->next = NULL;
}

/*
 * If the the size is bigger than KVCMD_INLINE_KEY_MAX, 
 * call this function to get the key from user to kernel
 */
static inline bool kv_get_key(struct nvme_passthru_kv_cmd *cmd, int *status)
{
    bool ret = true;
    /*
     * Get key to check whether the same key exist in the hashs
     */
    void *key_addr = kmalloc(cmd->key_length, GFP_KERNEL);
    if (unlikely(!key_addr)){
        pr_err("[%d:%s]: Fail to allocate memory for Key \n", __LINE__, __func__);
        *status = -ENOMEM;
        cmd->result = KVS_ERR_NOMEM;
        ret = false;
        goto out;
    }

    if (unlikely(copy_from_user(key_addr, (void *)cmd->key_addr, cmd->key_length))) {
        pr_err("[%d:%s]: Fail to copy key\n", __LINE__, __func__);
        kfree(key_addr);
        *status = -EFAULT;
        cmd->result = KVS_ERR_FAULT;
        ret = false;
        goto out;
    }
    cmd->key_addr = (__u64)key_addr;
out:
    return ret;
}

static inline void kv_move_async_to_sync(struct kv_async_request *async, struct kv_sync_request *sync, void *key){

    sync->q = async->queue;
    memcpy(&sync->c, &async->c, sizeof(struct nvme_command));
    sync->timeout = async->timeout;
    if(async->data_length && async->data_addr){
        if(sync->udata_length != async->data_length){
            pr_err("[%d:%s]: The data length of buffered rq & sync read rq must be same\n", __LINE__, __func__);
            BUG();
        }
        copy_to_user((void __user *)sync->udata_addr, async->data_addr, sync->udata_length);
    }
    sync->key_length = async->key_length;
    sync->key_addr = key;
    sync->result = 0;
}

/*
 * If it is a "get" request, it gets the data from the readahead hash and returns true.
 * If it is a "put" request, it removes the data from the readahead hash and returns false.
 */
static inline bool kv_do_readahead(void *key,  u32 hash_key, struct kv_iosched_struct *iosched, struct nvme_passthru_kv_cmd *cmd){
    bool ret = false;
    struct kv_async_request *ra_rq = kv_hash_find_and_del_readahead(&iosched->kv_hash[KV_RA_HASH], key, cmd->key_length, hash_key);

    if(ra_rq){
        spin_lock(&ra_rq->lock);
        /* Remove the data from the readahead lru */

        /* If it is a "get" request, it copies the data to user buffer */
        if(is_kv_retrieve_cmd(cmd->opcode)){
            copy_to_user((void __user *)cmd->data_addr, ra_rq->data_addr, cmd->data_length);
            if (cmd->key_length > KVCMD_INLINE_KEY_MAX) kfree(key);
            cmd->result = ra_rq->result;
            ret = true;
            cmd->status = ra_rq->errors;
        }/* else{ Otherwise, cancel readahead }*/
        /* Free the rq if no one wait for it */
        if(atomic_dec_and_test(&ra_rq->refcount)){
            free_kv_async_rq(ra_rq);
        }else{
            pr_err("[%d:%s]Fail to free RA request because the refcount is not 0(it is %d)\n", __LINE__, __func__, atomic_read(&ra_rq->refcount));
            spin_unlock(&ra_rq->lock);
        }
    }
    return ret;

}
int nvme_kv_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd)
{
    struct nvme_passthru_kv_cmd cmd;
    int status=0;
    int merge_status=0;
    void *key = NULL;
    struct kv_iosched_struct *kv_iosched = ns->kv_iosched;
    struct kv_async_request *buffered_rq;
    struct kv_sync_request *sync_rq = NULL;
    bool inFlight = false;

    u32 hash_key = KV_HASH_SIZE;
#ifndef NO_LOG
    atomic64_inc(&kv_total_sync_request);
#endif
#ifdef KV_NVME_REQUIRE_SYS_ADMIN
    if (unlikely(!capable(CAP_SYS_ADMIN)))
        return -EACCES;
#endif
    if (unlikely(copy_from_user(&cmd, ucmd, sizeof(cmd))))
        return -EFAULT;
    if (unlikely(cmd.flags)){
        put_user(KVS_ERR_INVALID_FLAGS, &ucmd->result);
        return -EINVAL;
    }
    if (unlikely(!is_kv_cmd(cmd.opcode))){
        pr_err("[%d:%s]: The command is not supported! kv cmd flags(%d) opcode(%02x), data len(%d), key len(%d) key(%s)\n", __LINE__, __func__, cmd.flags, cmd.opcode, cmd.data_length, cmd.key_length, cmd.key);
        put_user(KVS_ERR_INVALID_OPCODE, &ucmd->result);
        return -EINVAL;
    }
    if (unlikely(is_kv_exist_cmd(cmd.opcode))) {
        pr_err("[%d:%s]: The command is not supported! kv cmd flags(%d) opcode(%02x), data len(%d), key len(%d) key(%s)\n", __LINE__, __func__, cmd.flags, cmd.opcode, cmd.data_length, cmd.key_length, cmd.key);
        put_user(KVS_ERR_INVALID_OPCODE, &ucmd->result);
        return -EINVAL;
    }
     
    /*  exist & iterate command, and then goto build_rq; */
    if (is_kv_special_cmd(cmd.opcode)) goto build_rq;

    if (cmd.key_length > KVCMD_INLINE_KEY_MAX){
        if(!kv_get_key(&cmd, &status)){
            put_user(cmd.result, &ucmd->result);
            return status;
        }
        key = (void *)cmd.key_addr;
    }else
        key = cmd.key; /* memcpy for key data is required on nvme_command initialization */
    hash_key = get_hash_key(key, cmd.key_length);
    /**
     * To return size of data for get command
     */
    cmd.result = 0;
    
    /* 
     * Check whether same key exists in READAHEAD hash or not 
     */
    if(kv_do_readahead(key, hash_key, kv_iosched, &cmd)){
        status = cmd.status;
        put_user(KVS_ERR_EXIST_IN_RA_HASH, &ucmd->status);
#ifndef NO_LOG
        atomic64_inc(&kv_total_ra_hit);
#endif
#ifdef INSDB_DEBUG
        pr_err("[%d:%s]Hit readahead key[%lld|%lld]\n", __LINE__, __func__,*((u64*)key),*(((u64*)key)+1));
#endif
        goto out;
    }
    /* 
     * Check whether same key exists in REQUEST hash or not 
     */
    buffered_rq = kv_hash_find_and_lock(&kv_iosched->kv_hash[KV_RQ_HASH], key, cmd.key_length, hash_key, &inFlight);
    if(inFlight){
        if(kv_do_readahead(key, hash_key, kv_iosched, &cmd)){
#ifndef NO_LOG
            atomic64_inc(&kv_total_ra_hit_in_flight);
#endif
            status = cmd.status;
            put_user(KVS_ERR_EXIST_IN_RA_HASH, &ucmd->status);
            goto out;
        }
    }else if(buffered_rq){
        /*
         * The same key exists in the request hash
         * The buffered RQ has been invalidated(see kv_hash_del_or_find_and_lock() function)
         */
        if(is_kv_retrieve_cmd(cmd.opcode)){
            if(is_kv_store_cmd(buffered_rq->c.common.opcode)){
                /*Move ASYNC request to SYNC request */
                sync_rq = kv_sync_request_alloc();
                if (!sync_rq ){
                    if(atomic_dec_and_test(&buffered_rq->refcount)){
                        pr_err("[%d:%s]ERROR : retcount of pending rq must not be 0\n", __LINE__, __func__);
                        free_kv_async_rq(buffered_rq);
                    }else
                        spin_unlock(&buffered_rq->lock);
                    if(cmd.key_length > KVCMD_INLINE_KEY_MAX) kfree(key);
                    put_user(KVS_ERR_NOMEM	, &ucmd->result);
                    return -ENOMEM;
                }
                /* Get User Memory Address */
                sync_rq->ucmd = ucmd;
                sync_rq->udata_addr = (void __user *)cmd.data_addr;
                sync_rq->udata_length = buffered_rq->data_length;//cmd.data_length;
                cmd.result = buffered_rq->data_length;
                /* If previous async request is del cmd, return error code*/
                kv_move_async_to_sync(buffered_rq, sync_rq, key);
#ifndef NO_LOG
                atomic64_inc(&kv_sync_get_merge_with_async_put);
#endif
            }else if(is_kv_delete_cmd(buffered_rq->c.common.opcode)){
                /*Move ASYNC request to SYNC request */
                sync_rq = kv_sync_request_alloc();
                if (!sync_rq ){
                    if(atomic_dec_and_test(&buffered_rq->refcount)){
                        pr_err("[%d:%s]ERROR : retcount of pending rq must not be 0\n", __LINE__, __func__);
                        free_kv_async_rq(buffered_rq);
                    }else
                        spin_unlock(&buffered_rq->lock);
                    if(cmd.key_length > KVCMD_INLINE_KEY_MAX) kfree(key);
                    put_user(KVS_ERR_NOMEM	, &ucmd->result);
                    return -ENOMEM;
                }
                /* Get User Memory Address */
                sync_rq->ucmd = ucmd;
                sync_rq->udata_addr = NULL;//(void __user *)cmd.data_addr;
                sync_rq->udata_length = 0;//cmd.data_length;
                cmd.result = 0;//cmd.data_length;
                /* If previous async request is del cmd, return error code*/
                kv_move_async_to_sync(buffered_rq, sync_rq, key);
#ifndef NO_LOG
                atomic64_inc(&kv_sync_get_merge_with_async_del);
#endif
                merge_status = 0x10;/*NotExistKey*/
            }
#ifndef NO_LOG
            else
                atomic64_inc(&kv_total_canceled_ra);
#endif
        }
        /* Free is done by submission daemon (The daemon check whether the request is invalid or not)*/
        if(atomic_dec_and_test(&buffered_rq->refcount)){
            pr_err("[%d:%s]ERROR : retcount of pending rq must not be 0\n", __LINE__, __func__);
            free_kv_async_rq(buffered_rq);
        }else
            spin_unlock(&buffered_rq->lock);

    }
build_rq:
    ////////// Allocate sync request node and insert it to list ///////////
    if(!sync_rq){ /* sync_rq might be filled using async request*/
        sync_rq = kv_sync_request_alloc();
        if (!sync_rq ){
            put_user(KVS_ERR_NOMEM, &ucmd->result);
            if(cmd.key_length > KVCMD_INLINE_KEY_MAX && key)
                kfree(key);
            return -ENOMEM;
        }
        init_kv_sync_request(sync_rq, key, &cmd, ns->queue);
        sync_rq->ucmd = ucmd;
    }
#ifdef KV_NVME_TRACE
    else
        pr_err("[%d:%s]sync is not null so we do not allocate the sync rq\n", __LINE__, __func__);
#endif

#ifdef KV_NVME_TRACE
    pr_err("[%d:%s]nvme_user_cmd: kv cmd opcode(%02x), data len(%d), meta len(%d)\n", __LINE__, __func__, cmd.opcode, cmd.data_length, cmd.key_length);
#endif
    /*
     * Congestion control code should be placed at here
     */
#ifndef NO_LOG
    atomic64_inc(&kv_total_sync_submit);
#endif
    status = __nvme_submit_kv_user_cmd(sync_rq);
    if(merge_status)
        status = merge_status;
#ifdef INSDB_DEBUG
        pr_err("[%d:%s]Sync Read key[%lld|%lld](error code : 0x%x)\n", __LINE__, __func__,*((u64*)key),*(((u64*)key)+1), status);
#endif
    if(!cmd.result)
        cmd.result = sync_rq->result;
    free_kv_sync_rq(sync_rq);

out:
#ifndef NO_LOG
    atomic64_inc(&kv_total_sync_finished);
#endif
    if (status >= 0) {
        if (put_user(cmd.result, &ucmd->result))
            return -EFAULT;
    }

    return status;
}


static int build_sync_rq(struct nvme_ns *ns, struct kv_iosched_struct *kv_iosched, struct nvme_passthru_kv_cmd __user *ucmd, struct kv_sync_request **sync_rq_ret) {
    struct nvme_passthru_kv_cmd cmd;
    struct kv_async_request *buffered_rq;
    struct kv_sync_request *sync_rq;
    void *key = NULL;
    __u32 hash_key;
    int ret = 0;
    bool inFlight = false;

    // init returning sync request pointer
    *sync_rq_ret = sync_rq = NULL;
    if (unlikely(copy_from_user(&cmd, ucmd, sizeof(cmd)))) {
        return -EINVAL;
    }
    if (unlikely(cmd.flags)){
        put_user(KVS_ERR_INVALID_FLAGS, &ucmd->result);
        return -EINVAL;
    }
    if (unlikely(!is_kv_cmd(cmd.opcode))){
        pr_err("[%d:%s]: The command is not supported! kv cmd flags(%d) opcode(%02x), data len(%d), key len(%d)\n", __LINE__, __func__, cmd.flags, cmd.opcode, cmd.data_length, cmd.key_length);
        put_user(KVS_ERR_INVALID_OPCODE, &ucmd->result);
        return -EINVAL;
    }
    if (unlikely(is_kv_exist_cmd(cmd.opcode))) {
        pr_err("[%d:%s]: The command is not supported! kv cmd flags(%d) opcode(%02x), data len(%d), key len(%d)\n", __LINE__, __func__, cmd.flags, cmd.opcode, cmd.data_length, cmd.key_length);
        put_user(KVS_ERR_INVALID_OPCODE, &ucmd->result);
        return -EINVAL;
    }

    /*  directly go to build_rq if exist & iterate command */
    if (is_kv_special_cmd(cmd.opcode)) goto build_rq;

    /* get key pointer */
    if (cmd.key_length > KVCMD_INLINE_KEY_MAX){
        if(!kv_get_key(&cmd, &ret)){
            put_user(cmd.result, &ucmd->result);
            return ret;
        }
        key = (void *)cmd.key_addr;
    }else
        key = cmd.key; /* memcpy for key data is required on nvme_command initialization */

    /* get hash value of the key */
    hash_key = get_hash_key(key, cmd.key_length);
    /**
     * To return size of data for get command
     */
    cmd.result = 0;

    /* check readahead hit */
    if(kv_do_readahead(key, hash_key, kv_iosched, &cmd)){
        ret = cmd.status;
        put_user(KVS_ERR_EXIST_IN_RA_HASH, &ucmd->status);
#ifndef NO_LOG
        atomic64_inc(&kv_total_ra_hit);
#endif
#ifdef INSDB_DEBUG
        pr_err("[%d:%s]Hit readahead key[%lld|%lld]\n", __LINE__, __func__,*((u64*)key),*(((u64*)key)+1));
#endif
        goto out;
    }
    /*
     * Check whether same key exists in REQUEST hash or not
     */
    buffered_rq = kv_hash_find_and_lock(&kv_iosched->kv_hash[KV_RQ_HASH], key, cmd.key_length, hash_key, &inFlight);
    if(inFlight){
        /* check readahead hit */
        if(kv_do_readahead(key, hash_key, kv_iosched, &cmd)){
#ifndef NO_LOG
            atomic64_inc(&kv_total_ra_hit_in_flight);
#endif
            put_user(KVS_ERR_EXIST_IN_RA_HASH, &ucmd->status);
            goto out;
        }
    }else if(buffered_rq){
        /*
         * The same key exists in the request hash
         * The buffered RQ has been invalidated(see kv_hash_del_or_find_and_lock() function)
         */
        if(is_kv_retrieve_cmd(cmd.opcode)){
            if(is_kv_store_cmd(buffered_rq->c.common.opcode)){
                /*Move ASYNC request to SYNC request */
                sync_rq = kv_sync_request_alloc();
                if (!sync_rq ){
                    if(atomic_dec_and_test(&buffered_rq->refcount)){
                        pr_err("[%d:%s]ERROR : retcount of pending rq must not be 0\n", __LINE__, __func__);
                        free_kv_async_rq(buffered_rq);
                    }else
                        spin_unlock(&buffered_rq->lock);
                    if(cmd.key_length > KVCMD_INLINE_KEY_MAX) kfree(key);
                    put_user(KVS_ERR_NOMEM  , &ucmd->result);
                    return -ENOMEM;
                }
                /* Get User Memory Address */
                sync_rq->ucmd = ucmd;
                sync_rq->udata_addr = (void __user *)cmd.data_addr;
                sync_rq->udata_length = buffered_rq->data_length;//cmd.data_length;
                cmd.result = buffered_rq->data_length;
                kv_move_async_to_sync(buffered_rq, sync_rq, key);
#ifndef NO_LOG
                atomic64_inc(&kv_sync_get_merge_with_async_put);
#endif
            }else if(is_kv_delete_cmd(buffered_rq->c.common.opcode)){
                /*Move ASYNC request to SYNC request */
                sync_rq = kv_sync_request_alloc();
                if (!sync_rq ){
                    if(atomic_dec_and_test(&buffered_rq->refcount)){
                        pr_err("[%d:%s]ERROR : retcount of pending rq must not be 0\n", __LINE__, __func__);
                        free_kv_async_rq(buffered_rq);
                    }else
                        spin_unlock(&buffered_rq->lock);
                    if(cmd.key_length > KVCMD_INLINE_KEY_MAX) kfree(key);
                    put_user(KVS_ERR_NOMEM  , &ucmd->result);
                    return -ENOMEM;
                }
                /* Get User Memory Address */
                sync_rq->ucmd = ucmd;
                sync_rq->udata_addr = NULL;//(void __user *)cmd.data_addr;
                sync_rq->udata_length = 0;//cmd.data_length;
                cmd.result = 0;//cmd.data_length;
                /* If previous async request is del cmd, return error code*/
                kv_move_async_to_sync(buffered_rq, sync_rq, key);
#ifndef NO_LOG
                atomic64_inc(&kv_sync_get_merge_with_async_del);
#endif
                put_user(KVS_ERR_KEY_NOT_EXIST, &ucmd->result);
            }
#ifndef NO_LOG
            else
                atomic64_inc(&kv_total_canceled_ra);
#endif
        }
        /* Free is done by submission daemon (The daemon check whether the request is invalid or not)*/
        if(atomic_dec_and_test(&buffered_rq->refcount)){
            pr_err("[%d:%s]ERROR : retcount of pending rq must not be 0\n", __LINE__, __func__);
            free_kv_async_rq(buffered_rq);
        }else
            spin_unlock(&buffered_rq->lock);

    }
build_rq:
    /*
     * Allocate sync request
     * sync_rq might be already created by a pending request
     */
    if(!sync_rq){
        sync_rq = kv_sync_request_alloc();
        if (!sync_rq ){
            put_user(KVS_ERR_NOMEM, &ucmd->result);
            if(cmd.key_length > KVCMD_INLINE_KEY_MAX && key)
                kfree(key);
            return -ENOMEM;
        }
        init_kv_sync_request(sync_rq, key, &cmd, ns->queue);
        sync_rq->ucmd = ucmd;
    }
    /* set returning sync req */
    *sync_rq_ret = sync_rq;
#ifdef KV_NVME_TRACE
    else
        pr_err("[%d:%s]sync is not null so we do not allocate the sync rq\n", __LINE__, __func__);
#endif
out:
#ifdef KV_NVME_TRACE
    pr_err("[%d:%s]nvme_user_cmd: kv cmd opcode(%02x), data len(%d), meta len(%d)\n", __LINE__, __func__, cmd.opcode, cmd.data_length, cmd.key_length);
#endif
    return ret;
}

static void kv_end_io_multi_sync_rq(struct request *rq, int error)
{
    struct multicmd_completion *mcmd_comp =rq->end_io_data;
    rq->end_io_data = NULL;

#ifdef KV_NVME_TRACE
    if(error) {
        pr_err("[%d:%s]error %d req errors %d\n", __LINE__, __func__, error, rq->errors);
    }
#endif

    /* signal completion if this is the last */
    if(atomic_dec_and_test(&mcmd_comp->pending_count))
        complete(mcmd_comp->comp);
}

static int nvme_submit_kv_multicmd(struct kv_sync_request *sync_rq_head, int count)
{
    struct kv_sync_request *sync_rq;
    DECLARE_COMPLETION_ONSTACK(comp);
    struct multicmd_completion mcmd_comp;
    int ret = 0;
#ifdef KV_NVME_TRACE
    int wait_cnt = 0;
#endif

    /* init completion context */
    mcmd_comp.comp = &comp;
    atomic_set(&mcmd_comp.pending_count, count);

    /* prepare block request for each sync request */
    for(sync_rq = sync_rq_head; sync_rq ; sync_rq = sync_rq->next) {

        struct request_queue *q = sync_rq->q;
        struct nvme_ns *ns = q->queuedata;
        struct gendisk *disk = ns ? ns->disk : NULL;
        struct request *req;
        struct bio *bio = NULL;

        if (!is_kv_cmd(sync_rq->c.common.opcode)) {
            ret = -EINVAL;
            break;
        }
        if (!disk) {
            ret = -ENODEV;
            break;
        }

        /* allocate device request */
        req = nvme_alloc_request(q, &sync_rq->c, 0, NVME_QID_ANY);
        if (IS_ERR(req)){
            ret = PTR_ERR(req);
            break;
        }
        /* save the request pointer to async request */
        sync_rq->req = req;

        req->timeout = sync_rq->timeout ? sync_rq->timeout : ADMIN_TIMEOUT;

        /*
         * A sync request always uses user memory,
         * even if the request was async request which uses kernel buffer.
         * The kernel buffer has been copied to user buffer.
         */
        if (sync_rq->udata_addr && sync_rq->udata_length) {

            /* The request is from User */
            ret = blk_rq_map_user(q, req, NULL, sync_rq->udata_addr, sync_rq->udata_length,
                    GFP_KERNEL);
            if (ret) {
                break;
            }
            bio = req->bio;

            bio->bi_bdev = bdget_disk(disk, 0);
            if (!bio->bi_bdev) {
                ret = -ENODEV;
                break;
            }
        }else{/* For Delete Command */
            // build temp_bio for meta_data
            // - need prvent to call blk_rq_unmap_user.
            bio = bio_kmalloc(GFP_KERNEL, 1);
            if (!bio) {
                ret = -ENODEV;
                break;
            }
            kv_blk_rq_bio_prep(req->q, req, bio);
        }
        sync_rq->bio = bio;

        /* An iterate command has NO memory for key */
        if (sync_rq->key_addr && sync_rq->key_length > KVCMD_INLINE_KEY_MAX) {
            struct bio_integrity_payload *bip;
            bip = bio_integrity_alloc(bio, GFP_KERNEL, 1);
            if (IS_ERR(bip)) {
                ret = PTR_ERR(bip);
                break;
            }

            bip->bip_size = sync_rq->key_length;
            bip->bip_sector = 0;
            ret = bio_integrity_add_page(bio, virt_to_page(sync_rq->key_addr),
                    sync_rq->key_length, offset_in_page(sync_rq->key_addr));
            if (ret != sync_rq->key_length) {
                ret = -ENOMEM;
                break;
            }
        }

    #ifdef KV_NVME_TRACE
        pr_err("[%d:%s] :  opcode(%02x)\n", __LINE__, __func__, cmd->common.opcode);
    #endif
    }

    if(ret) {
        pr_err("[%d:%s] :  could not build block requests. ret %d\n", __LINE__, __func__, ret);
        goto cleanup;
    }

    /* issue to the device */
    /* TODO: figure out sensedata is really needed */
    //char sense[SCSI_SENSE_BUFFERSIZE];
    //memset(sense, 0, sizeof(sense));

    for(sync_rq = sync_rq_head; sync_rq ; sync_rq = sync_rq->next) {
        struct request *req = sync_rq->req;
        struct nvme_ns *ns = sync_rq->q->queuedata;
        struct gendisk *disk = ns ? ns->disk : NULL;
        //req->sense = sense;
        //req->sense_len = 0;

        req->end_io_data = &mcmd_comp;
        req->special = &sync_rq->cqe;
        blk_execute_rq_nowait(req->q, disk, req, 0, kv_end_io_multi_sync_rq);
    }

    /*
     * wait until all requests are completed
     * Congestion control code should be placed at here
     */
#ifndef NO_LOG
    atomic64_add(count, &kv_total_sync_submit);
#endif

    /* Prevent hang_check timer from firing at us during very long I/O */
    while (!wait_for_completion_io_timeout(mcmd_comp.comp, 120* (HZ/2))) {
#ifdef KV_NVME_TRACE
        wait_cnt++;
        pr_err("[%d:%s] : wait %d\n", __LINE__, __func__, wait_cnt);
#endif
    }

#ifndef NO_LOG
    atomic64_add(count, &kv_total_sync_complete);
#endif

cleanup:
    /* get the result and clean up block requests */
    for(sync_rq = sync_rq_head; sync_rq ; sync_rq = sync_rq->next) {
        if(sync_rq->req) {
            struct request *req = sync_rq->req;
            struct bio *bio = sync_rq->bio;
            struct nvme_passthru_kv_cmd __user *ucmd = sync_rq->ucmd;

            if (bio && sync_rq->udata_addr) {
                struct request_queue *q = sync_rq->q;
                struct nvme_ns *ns = q->queuedata;
                struct gendisk *disk = ns ? ns->disk : NULL;
                if (disk && bio->bi_bdev)
                    bdput(bio->bi_bdev);
                if (is_kv_cmd(sync_rq->c.common.opcode) &&
                        bio && bio_integrity(bio)) {
                    bio_integrity_free(bio);
                }
                blk_rq_unmap_user(bio);
            }else if(bio) /*Delete command*/
                bio_put(bio);
            blk_mq_free_request(req);
            /* set result from the device */
            put_user(le32_to_cpu(sync_rq->cqe.result.u32), &ucmd->result);
            switch(req->errors) {
            case 0x310:
                put_user(KVS_ERR_KEY_NOT_EXIST, &ucmd->status);
                break;
            default:
                put_user((uint32_t)req->errors, &ucmd->status);
            }
        }
    }
    return ret;
}

int nvme_kv_multi_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd)
{
    struct kv_iosched_struct *kv_iosched = ns->kv_iosched;
    struct kv_sync_request *sync_rq_head = NULL, *sync_rq_last = NULL;
    struct kv_sync_request *sync_rq;
    uint32_t count;
    uint32_t dev_count = 0;
    int i;
    int ret = 0;

#ifdef KV_NVME_REQUIRE_SYS_ADMIN
    if (unlikely(!capable(CAP_SYS_ADMIN)))
        return -EACCES;
#endif

    if (unlikely(copy_from_user(&count, ucmd, sizeof(uint32_t)))) return -EFAULT;
#ifndef NO_LOG
    atomic64_add(count, &kv_total_sync_request);
#endif

    ucmd = (struct nvme_passthru_kv_cmd __user *)((__u8 *)ucmd + sizeof(uint32_t));
    /*
     * TODO: Copy the blocking key
     * The requests should be submitted after the blocking key is submitted
     */
    //if (unlikely(copy_from_user(blocking_key, ucmd, KVCMD_INLINE_KEY_MAX))) return -EFAULT;

    ucmd = (struct nvme_passthru_kv_cmd __user *)((__u8 *)ucmd + KVCMD_INLINE_KEY_MAX);

    /* build a list of sync requests */
    for( i = 0 ; i < count ; i++)
    {
        ret = build_sync_rq(ns, kv_iosched, ucmd+i, &sync_rq);
        if(ret) break;
        /*
         * link to the sync rq head
         * note that req is completed by the buffers if sync_rq is not returned.
         */
        if(sync_rq) {
            if(sync_rq_head) {
                sync_rq_last->next = sync_rq;
                sync_rq_last = sync_rq;
            } else {
                sync_rq_head = sync_rq_last = sync_rq;
            }
            dev_count++;
        }
    }
    if(ret) goto cleanup;

    // submit sync requests to the device
    ret = nvme_submit_kv_multicmd(sync_rq_head, dev_count);

cleanup:

#ifndef NO_LOG
     atomic64_add(count, &kv_total_sync_finished);
#endif

     for(sync_rq = sync_rq_head ; sync_rq ; sync_rq = sync_rq_last)
     {
         /* save the next pointer here because the current sync_rq will be freed */
         sync_rq_last = sync_rq->next;
         free_kv_sync_rq(sync_rq);
     }

    return ret;
}

static void bio_kern_endio(struct bio *bio, int x)
{
    bio_put(bio);
}

#define SUBMIT_ERROR_RETURN(req, rq, memq, msg, err) \
{ \
                pr_err("[%d:%s] ERROR: %s\n", __LINE__, __func__, msg); \
                blk_mq_free_request(req); \
                rq->req = NULL; \
                list_add(&rq->lru, &memq->rq); \
                return err; \
}
int __nvme_submit_iosched_kv_cmd(struct kv_memqueue_struct *memq, int qid)
{
    struct list_head *ele, *next_ele;

    int ret = 0;
    struct nvme_command *cmd = NULL;
    struct request *req = NULL;

    list_for_each_safe(ele, next_ele, &memq->rq){ 
        struct kv_async_request *rq = list_entry(ele, struct kv_async_request, lru);

        struct request_queue *q;
        void *data;
        unsigned data_length;

        void *key;
        unsigned key_length;

        struct nvme_ns *ns;
        struct gendisk *disk;
        struct bio *bio = NULL;
#if 0
        if(atomic_read(&rq->iosched->kv_hash[KV_RQ_HASH].nr_rq) < KV_PENDING_QUEU_LOW_WATERMARK){
            if(test_and_clear_bit(KV_iosched_queue_full, &rq->iosched->state))
                wake_up_bit(&rq->iosched->state, KV_iosched_queue_full);
        }
#endif

        if(rq->blocking_rq){
            /*This means it is a pending request*/
            spin_unlock(&rq->lock);
            rq->blocking_rq = NULL;
            pr_err("[%d:%s] ERROR: rq->blocking_rq must be empty!!\n", __LINE__, __func__);
            return -EINVAL;
        }

        q = rq->queue;
        cmd = &rq->c;

        if(!req){
            req = nvme_alloc_request(q, cmd, 0, NVME_QID_ANY);
            if (IS_ERR(req)){
                spin_unlock(&rq->lock);
                pr_err("[%d:%s] ERROR: nvme_alloc_request was failed!!\n", __LINE__, __func__);
                return PTR_ERR(req);
            }
        }

        /*
         * sync IO which has same key with the submission queue will wait
         */
        /////////////////////////////////////////// Get Lock //////////////////////////////////////////////
        spin_lock(&rq->lock);
        /* 
         * It is ok even if the cmd has been changed after req allocation because..
         * 1. nvme_is_write() always returns 1 for KV cmds 
         * 2. req->cmd is pointer variable.
         */
        list_del(&rq->lru);//deleted from Memory Queue list

        /* For prefetche's batch requests*/
        if(test_and_clear_bit(KV_async_waiting_submission, &rq->state)){
            wake_up_bit(&rq->state, KV_async_waiting_submission);
        }
        if(test_bit(KV_async_invalid_rq, &rq->state)){
            /*
             * This rq was removed from submission Q & read-ahead hash(summy key) when it became invalid key.
             */
            WARN_ON(!hlist_unhashed(&rq->hash));

            atomic_dec(&memq->nr_rq);

            /* For "flush" command */
            if(test_and_clear_bit(KV_async_flush_wakeup, &rq->state)){
#ifndef NO_LOG
                atomic64_inc(&kv_flush_wakeup_by_submit);
#endif
                wake_up_bit(&rq->state, KV_async_flush_wakeup);
            }
            if(atomic_dec_and_test(&rq->refcount)){
                free_kv_async_rq(rq);
            }else{
                pr_err("[%d:%s]ERROR : retcount of invalid rq must not be 0 but it is %d\n", __LINE__, __func__, atomic_read(&rq->refcount));
                spin_unlock(&rq->lock);
            }
            /* Reuse req */
            /* nvme req can be reuse if cmd is same */
            blk_mq_free_request(req);
            req = NULL;
            continue;
        }
        set_bit(KV_async_inFlight_rq, &rq->state);
        spin_unlock(&rq->lock);
        /////////////////////////////////////////// Relese Lock ///////////////////////////////////////////

        data = rq->data_addr;
        data_length = rq->data_length;

        key = rq->key_addr;
        key_length = rq->key_length;

        ns = q->queuedata;
        disk = ns ? ns->disk : NULL;
#if 0
        if (!disk) return -ENODEV;
#endif
        /* to free req */
        rq->req = req;

        req->timeout = rq->timeout ? rq->timeout : ADMIN_TIMEOUT;
        req->special = &rq->cqe;

        if (data && data_length) {

            ret = blk_rq_map_kern(q, req, data, data_length, GFP_KERNEL);
            if (ret) SUBMIT_ERROR_RETURN(req,rq,memq, "blk_rq_map_kern fail", ret);

            bio = req->bio;

            bio->bi_bdev = bdget_disk(disk, 0);
            if (!bio->bi_bdev) {
                bio_put(bio);
                SUBMIT_ERROR_RETURN(req,rq,memq, "bdget_disk fail", -ENODEV);
            }
        }else{/* For Delete Command */
            // build temp_bio for meta_data
            // - need prvent to call blk_rq_unmap_user.
            bio = bio_kmalloc(GFP_KERNEL, 1);
            if (!bio) 
                SUBMIT_ERROR_RETURN(req,rq,memq, "bio_kmalloc fail", -ENODEV);
#if 0
            ret = blk_rq_append_bio(req, bio);
            if(ret) {
                bio_put(bio);
                SUBMIT_ERROR_RETURN(req,rq,memq, "blk_rq_append_bio fail", -EFAULT);
            }
#else
            kv_blk_rq_bio_prep(req->q, req, bio);
#endif

            bio->bi_end_io = bio_kern_endio;
        }
        if (key && key_length > KVCMD_INLINE_KEY_MAX) {
            struct bio_integrity_payload *bip;

            bip = bio_integrity_alloc(bio, GFP_KERNEL, 1);
            if (IS_ERR(bip)) {
                bio_put(bio);
                SUBMIT_ERROR_RETURN(req,rq,memq, "bio_integrity_alloc fail", PTR_ERR(bip));
            }

            bip->bip_size = key_length;
            bip->bip_sector = 0;

            if (key_length != bio_integrity_add_page(bio, virt_to_page(key), 
                        key_length, offset_in_page(key))) {
                bio_put(bio);
                SUBMIT_ERROR_RETURN(req,rq,memq, "bio_integrity_add_page fail", -ENOMEM);
            }
        }
        req->sense = NULL;
        req->sense_len = 0;

        req->end_io_data = rq;

        /* Congestion control */
        rq->in_flight_rq = &(rq->iosched->in_flight_rq[smp_processor_id()]);
        if(/*rq->iosched->max_inflight_request*/ KV_MAX_INFLIGHT_REQUEST  < atomic_inc_return(rq->in_flight_rq)){
#ifndef NO_LOG
            atomic64_inc(&kv_total_blocked_submit);
#endif
            set_bit(KV_iosched_congested,  &rq->iosched->state);
            wait_on_bit(&rq->iosched->state, KV_iosched_congested, TASK_KILLABLE);
        }
        blk_execute_rq_nowait(req->q, disk, req, 0, kv_end_io_async_rq);

        atomic_dec(&memq->nr_rq);
#ifndef NO_LOG
        atomic64_inc(&kv_total_async_submit);
#endif
        req = NULL;
    }
    if(req) blk_mq_free_request(req);

    return ret;
}

/* Initialize kv_async_request */
static int init_kv_async_request(struct kv_iosched_struct *kv_iosched, struct kv_async_request *rq, void *key, struct nvme_passthru_kv_cmd *cmd, struct request_queue *queue, __u32	hash_key) 
{
    int status = 0;

    rq->queue = queue;

    memset(&rq->c, 0, sizeof(rq->c));
    set_kv_cmd(&rq->c, cmd); 
    switch(cmd->opcode) {
        case nvme_cmd_kv_store:
#ifndef NO_LOG
            atomic64_inc(&kv_async_put_req);
#endif
            break;
        case nvme_cmd_kv_retrieve:
#ifndef NO_LOG
            atomic64_inc(&kv_async_get_req);
#endif
            break;
        case nvme_cmd_kv_delete:
#ifndef NO_LOG
            atomic64_inc(&kv_async_del_req);
#endif
            break;
        default:
            break;
    }

    if (cmd->timeout_ms)
        rq->timeout = msecs_to_jiffies(cmd->timeout_ms);

    if(cmd->data_length){/*it can be 0 for del cmd*/
        rq->data_addr = kmalloc(cmd->data_length, GFP_KERNEL);
        if (unlikely(!rq->data_addr)){
            pr_err("[%d:%s]: fail to allocate data\n", __LINE__, __func__);
            status = -ENOMEM;
            cmd->result = KVS_ERR_NOMEM;
            goto out;
        }
        if(is_kv_store_cmd(cmd->opcode)) {
            if (unlikely(copy_from_user(rq->data_addr, (void __user *)cmd->data_addr,
                            cmd->data_length))) {
                status = -EFAULT;
                cmd->result = KVS_ERR_FAULT;
                pr_err("[%d:%s]: fail to copy data\n", __LINE__, __func__);
                goto data_free;
            }
        }
    }else
        rq->data_addr = NULL;
    /* The exist or iterator commands can not be here
     * They are synchronous requests. */
    rq->data_length = cmd->data_length;

    if (cmd->key_length <= KVCMD_INLINE_KEY_MAX)
        rq->key_addr = rq->c.kv_store.key;
    rq->key_length = cmd->key_length;

    rq->iosched = kv_iosched;
    rq->hash_key = hash_key;
    /* The refcount will be decreased in completion daemon(completed) or submission daemon(invalidated)*/
#if 0 /* Debugging code */
    if(rq->state))
        pr_err("[%d:%s]ERROR : New async rq's state should be not set(state : %d) (%d : %s)\n", __LINE__, __func__, rq->state, current->pid, current->comm);
    if(atomic_read(&rq->refcount))
        pr_err("[%d:%s]ERROR : refcount must be 0 for new rq(before set the refcount)(%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
#endif
    rq->state = 0;
    atomic_set(&rq->refcount, 1);

    return KVS_SUCCESS;
    /////////////// Build struct kv_async_request END ////////////////////////////
data_free:
    if(rq->data_addr){
        kfree(rq->data_addr);
        rq->data_addr = NULL;
    }
out:
    return status;
}

int nvme_kv_iosched_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd)
{
    struct kv_iosched_struct *kv_iosched = ns->kv_iosched;
    struct kv_async_request *async_rq, *buffered_rq;
    struct nvme_passthru_kv_cmd cmd;
    void    *key;
    __u32 hash_key;

    int status = 0;
    unsigned long flags;/* For spin_lock_irqsave */
    int request_type;
	
#if 1
#ifdef KV_NVME_REQUIRE_SYS_ADMIN
    if (unlikely(!capable(CAP_SYS_ADMIN)))
        return -EACCES;
#endif
    if (unlikely(copy_from_user(&cmd, ucmd, sizeof(cmd))))
        return -EFAULT;
    if (unlikely(cmd.flags)){
        put_user(KVS_ERR_INVALID_FLAGS, &ucmd->result);
        return -EINVAL;
    }
    /*
     * The IO scheduler processes only IO-related commands(put, get & del)
     * Not special commands: exist & iterate.
     */
    if (unlikely(!is_kv_io_cmd(cmd.opcode))){
        pr_err("[%d:%s]: The command is not supported in io scheduler! kv cmd flags(%d) opcode(%02x), data len(%d), key len(%d)\n", __LINE__, __func__, cmd.flags, cmd.opcode, cmd.data_length, cmd.key_length);
        put_user(KVS_ERR_INVALID_OPCODE, &ucmd->result);
        return -EINVAL;
    }

#else
    copy_from_user(&cmd, ucmd, sizeof(cmd));
#endif

    /* 
     * If key length is less than KVCMD_INLINE_KEY_MAX,
     * the key was copied to cmd via copy_from_user() above.
     * Otherwise, allocate kernel memory and copy the data from user to kernel
     * If the key size is less than KVCMD_INLINE_KEY_MAX,
     * unused memory for key should be filled with 0. 
     */
    if (cmd.key_length > KVCMD_INLINE_KEY_MAX){
        if(!kv_get_key(&cmd, &status)){
            put_user(cmd.result, &ucmd->result);
            return status;
        }
        /*
         * memcpy is not necessary because key_addr points 
         * kernel memory which is allocated via kmalloc
         */
        key = (void *)cmd.key_addr;
    }else{
        /* 
         * memcpy for key data is required(nvme_passthru -> nvme_command) 
         * on nvme_command initialization 
         */
        key = cmd.key;
    }
    hash_key = get_hash_key(key, cmd.key_length);

    /* If it is a get(retrieve) request, check readahead hash */
    if (is_kv_retrieve_cmd(cmd.opcode) && kv_hash_find(&kv_iosched->kv_hash[KV_RA_HASH], key, cmd.key_length, hash_key, true)){
        cmd.result = KVS_ERR_EXIST_IN_RA_HASH;
        goto key_free;
    }

    /* Allocate an async request */
    async_rq = kv_async_request_alloc();

    if (!unlikely(async_rq)){
        status = -ENOMEM;
        cmd.result = KVS_ERR_NOMEM;
        goto key_free; 
    }

    status = init_kv_async_request(kv_iosched, async_rq, key, &cmd, ns->queue, hash_key);
    if(unlikely(status)){
        kv_async_request_free(async_rq);
        goto key_free; 
    }

    /* Try to insert */
    spin_lock(&async_rq->lock);
    buffered_rq = kv_hash_insert(&kv_iosched->kv_hash[KV_RQ_HASH], async_rq);

    ////////////////////// START find same key in submission & read-ahead queues //////////////////////////
    /* If insertion is fail because same key exists and the key is not inflight or invalid */
    if(buffered_rq){
        if(is_kv_retrieve_cmd(cmd.opcode)){
            /* If same key exists in any hash tables, do nothing and just return */
            /* 
             * If the buffered command is not get(retrieve) command, 
             * this readahead command will be ignored,
             * which means the user may not get the data from readahead hash.
             */
            if(atomic_dec_and_test(&buffered_rq->refcount)){
                pr_err("[%d:%s]ERROR : The refcount is 0, but not desired (%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
                free_kv_async_rq(buffered_rq);
            }else{
                spin_unlock(&buffered_rq->lock);
            }
            cmd.result = KVS_ERR_EXIST_PENDING_KEY;
            if(atomic_dec_and_test(&async_rq->refcount)){
                free_kv_async_rq(async_rq);
            }else{
                pr_err("[%d:%s]ERROR : The refcount must be 0, but it isn't (%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
                spin_unlock(&async_rq->lock);
            }
#ifndef NO_LOG
            atomic64_inc(&kv_total_get_async_merge);
#endif

            goto out;
        }else{/*cmd.opcode == (nvme_cmd_kv_store) || (nvme_cmd_kv_delete) */
            /*
             * If offset R/W is supported, we need to check the offset 
             * in the hash related functions before doing something.
             */
            if(buffered_rq->data_length)
                kfree(buffered_rq->data_addr);
            buffered_rq->data_addr = async_rq->data_addr;
            buffered_rq->data_length = async_rq->data_length;
            memcpy(&buffered_rq->c, &async_rq->c, sizeof(struct nvme_command));
            if(atomic_dec_and_test(&buffered_rq->refcount)){
                pr_err("[%d:%s]ERROR : The refcount is 0, but not desired (%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
                free_kv_async_rq(buffered_rq);
            }else
                spin_unlock(&buffered_rq->lock);

            async_rq->data_addr = NULL;
            async_rq->data_length = 0 ;
            if(atomic_dec_and_test(&async_rq->refcount)){
                free_kv_async_rq(async_rq);
            }else{
                spin_unlock(&async_rq->lock);
                pr_err("[%d:%s]ERROR : The refcount of the new rq that failed to insert must be zero but non-zero.(%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
            }

            status = KVS_SUCCESS;
            cmd.result = KVS_ERR_EXIST_PENDING_KEY;
#ifndef NO_LOG
            atomic64_inc(&kv_total_put_del_async_merge);
#endif

            goto out;
        }
    }
    ////////////////////// END find same key in buffer queue //////////////////////////

    /*
     * If same key was inserted while creating the request, restart from searching same key in the hashs.
    */
    request_type = is_kv_retrieve_cmd(cmd.opcode) ? ACTIVE_READ : ACTIVE_WRITE;

    local_irq_save(flags);
    async_rq->memq = *this_cpu_ptr(kv_iosched->percpu_memq[request_type]);
    list_add(&async_rq->lru, &async_rq->memq->rq);
    atomic_inc(&async_rq->memq->nr_rq);
    if(async_rq->blocking_rq){
        atomic_inc(&async_rq->memq->nr_pending);
        /* Release the in-flight RQ lock */
        if(atomic_dec_and_test(&async_rq->blocking_rq->refcount)){
            pr_err("[%d:%s]ERROR : The refcount of the inflight rq which blocks new rq must be non-zero, but zero(%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
            free_kv_async_rq(async_rq->blocking_rq);
        }else
            spin_unlock(&async_rq->blocking_rq->lock);
    }
    spin_unlock_irqrestore(&async_rq->lock, flags);
    /* If nvme_cmd_kv_store or nvme_cmd_kv_delete */
    if(!is_kv_retrieve_cmd(cmd.opcode)){
        struct kv_async_request *ra_rq = kv_hash_find_and_del_readahead(&kv_iosched->kv_hash[KV_RA_HASH], async_rq->key_addr, async_rq->key_length, async_rq->hash_key);
        if(ra_rq){ // delete the readahead request from the LRU list
            spin_lock(&ra_rq->lock);

            if(atomic_dec_and_test(&ra_rq->refcount)){
                free_kv_async_rq(ra_rq);
            }else{
                pr_err("[%d:%s]ERROR : The refcount of deleted ra rq be zero but non-zero(%d).(%d : %s)\n", __LINE__, __func__, atomic_read(&ra_rq->refcount),current->pid, current->comm);
                spin_unlock(&ra_rq->lock);
            }
        }
    }

    /*
     * If the buffer Q was switched to submission Q 
     * while "kv_hash_insert" or wakeup delayed workueue
     * the delayed workqueue may do nothing when it wakes up.
     * In this case, the "nr_rq" must be 0
     */

#if 0
    //if(!work_busy(&kv_iosched->submit_daemon[request_type].submit_work) || kv_iosched->max_nr_req_per_memq  <= atomic_read(&(*this_cpu_ptr(kv_iosched->percpu_memq[request_type]))->nr_rq))
    if( (!kv_sched_daemon_busy(smp_processor_id(), kv_iosched, request_type) && !work_busy(&kv_iosched->submit_daemon[request_type].submit_work))|| kv_iosched->max_nr_req_per_memq  <= atomic_read(&(*this_cpu_ptr(kv_iosched->percpu_memq[request_type]))->nr_rq)){
        //printk("%d out of %d is filled", atomic_read(&(*this_cpu_ptr(kv_iosched->percpu_memq[request_type]))->nr_rq), kv_iosched->max_nr_req_per_memq);
        if(!kv_sched_wakeup_delayed(smp_processor_id(),kv_iosched, request_type, 0))
            kv_sched_wakeup_immediately(smp_processor_id(), kv_iosched, request_type);
    }else
        kv_sched_wakeup_delayed(smp_processor_id(),kv_iosched, request_type, KV_ASYNC_INTERVAL);
#else
    kv_sched_wakeup_delayed(smp_processor_id(),kv_iosched, request_type, KV_ASYNC_INTERVAL);
    if( (!kv_sched_daemon_busy(smp_processor_id(), kv_iosched, request_type) && !work_busy(&kv_iosched->submit_daemon[request_type].submit_work)) || kv_iosched->max_nr_req_per_memq  <= atomic_read(&(*this_cpu_ptr(kv_iosched->percpu_memq[request_type]))->nr_rq)){
        //printk("%d out of %d is filled", atomic_read(&(*this_cpu_ptr(kv_iosched->percpu_memq[request_type]))->nr_rq), kv_iosched->max_nr_req_per_memq);
        kv_sched_wakeup_immediately(smp_processor_id(), kv_iosched, request_type);
    }
#endif

    if(atomic_read(&kv_iosched->kv_hash[KV_RQ_HASH].nr_rq) > KV_PENDING_QUEU_HIGH_WATERMARK){
        set_bit(KV_iosched_queue_full,  &kv_iosched->state);
        wait_on_bit(&kv_iosched->state, KV_iosched_queue_full, TASK_KILLABLE);
    }

    status = KVS_SUCCESS;//kv_iosched->status;//__nvme_submit_iosched_kv_cmd(kv_iosched);
    cmd.result = KVS_SUCCESS;

    if (likely(status >= 0)) {
        if (put_user(cmd.result, &ucmd->result))
            status = -EFAULT;
    }
#ifndef NO_LOG
    atomic64_inc(&kv_total_async_request);
#endif
    return status;
key_free:
    if (cmd.key_length > KVCMD_INLINE_KEY_MAX)
        kfree(key);
out:
    if (status >= 0) {
        if (put_user(cmd.result, &ucmd->result))
            status = -EFAULT;
    }
    return status;
}


int nvme_kv_multi_iosched_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd, bool sync)
{
    struct kv_iosched_struct *kv_iosched = ns->kv_iosched;
    struct kv_async_request *async_rq, *buffered_rq;
    struct nvme_passthru_kv_cmd cmd;
    void    *key;
    __u32 hash_key;

    int status = 0;
    int request_type = 0, i;
    uint32_t  count;
    struct kv_memqueue_struct *memq = kv_memq_alloc();
#ifdef KV_NVME_REQUIRE_SYS_ADMIN
    if (unlikely(!capable(CAP_SYS_ADMIN)))
        return -EACCES;
#endif
    if (unlikely(copy_from_user(&count, ucmd, sizeof(uint32_t)))) return -EFAULT;
    ucmd = (struct nvme_passthru_kv_cmd __user *)((__u8 *)ucmd + sizeof(uint32_t));
    /* 
     * Copy the blocking key
     * The requests should be submitted after the blocking key is submitted 
     */
    if (unlikely(copy_from_user(memq->blocking_key, ucmd, KVCMD_INLINE_KEY_MAX))) return -EFAULT;
    ucmd = (struct nvme_passthru_kv_cmd __user *)((__u8 *)ucmd + KVCMD_INLINE_KEY_MAX);
    for( i = 0 ; i < count ; i++)
    {
        if (unlikely(copy_from_user(&cmd, ucmd+i, sizeof(cmd))))
            continue;
        /*
         * The IO scheduler processes only IO-related commands(put, get & del)
         * Not special commands: exist & iterate.
         */
        if (unlikely(!is_kv_io_cmd(cmd.opcode))){
            pr_err("[%d:%s]: The command is not supported in io scheduler! kv cmd flags(%d) opcode(%02x), data len(%d), key len(%d)\n", __LINE__, __func__, cmd.flags, cmd.opcode, cmd.data_length, cmd.key_length);
            put_user(KVS_ERR_INVALID_OPCODE, &(ucmd+i)->result);
            continue;
        }
        if(!request_type) request_type = is_kv_retrieve_cmd(cmd.opcode) ? BATCH_READ_DAEMON : BATCH_WRITE_DAEMON;

        /* 
         * If key length is less than KVCMD_INLINE_KEY_MAX,
         * the key was copied to cmd via copy_from_user() above.
         * Otherwise, allocate kernel memory and copy the data from user to kernel
         * If the key size is less than KVCMD_INLINE_KEY_MAX,
         * unused memory for key should be filled with 0. 
         */
        if (cmd.key_length > KVCMD_INLINE_KEY_MAX){
            if(!kv_get_key(&cmd, &status)){
                put_user(cmd.result, &(ucmd+i)->result);
                /* Out of memory */
                kv_memq_free(memq);
                return status;
            }
            /*
             * memcpy is not necessary because key_addr points 
             * kernel memory which is allocated via kmalloc
             */
            key = (void *)cmd.key_addr;
        }else{
            /* 
             * memcpy for key data is required(nvme_passthru -> nvme_command) 
             * on nvme_command initialization 
             */
            key = cmd.key;
        }
        hash_key = get_hash_key(key, cmd.key_length);

        /* If it is a get(retrieve) request, check readahead hash */
        if (is_kv_retrieve_cmd(cmd.opcode) && kv_hash_find(&kv_iosched->kv_hash[KV_RA_HASH], key, cmd.key_length, hash_key, true)){
            cmd.result = KVS_ERR_EXIST_IN_RA_HASH;
            if (cmd.key_length > KVCMD_INLINE_KEY_MAX)
                kfree(key);
            continue;
        }

        /* Allocate an async request */
        async_rq = kv_async_request_alloc();

        if (!unlikely(async_rq)){
            status = -ENOMEM;
            cmd.result = KVS_ERR_NOMEM;
            if (cmd.key_length > KVCMD_INLINE_KEY_MAX)
                kfree(key);
            continue;
        }

        status = init_kv_async_request(kv_iosched, async_rq, key, &cmd, ns->queue, hash_key);
        if(unlikely(status)){
            kv_async_request_free(async_rq);
            if (cmd.key_length > KVCMD_INLINE_KEY_MAX)
                kfree(key);
            continue;
        }

        /* Try to insert */
        spin_lock(&async_rq->lock);
        buffered_rq = kv_hash_insert(&kv_iosched->kv_hash[KV_RQ_HASH], async_rq);

        ////////////////////// START find same key in submission & read-ahead queues //////////////////////////
        /* If insertion is fail because same key exists and the key is not inflight or invalid */
        if(buffered_rq){
            if(is_kv_retrieve_cmd(cmd.opcode)){
                /* If same key exists in any hash tables, do nothing and just return */
                /* 
                 * If the buffered command is not get(retrieve) command, 
                 * this readahead command will be ignored,
                 * which means the user may not get the data from readahead hash.
                 */
                if(atomic_dec_and_test(&buffered_rq->refcount)){
                    pr_err("[%d:%s]ERROR : The refcount is 0, but not desired (%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
                    free_kv_async_rq(buffered_rq);
                }else{
                    spin_unlock(&buffered_rq->lock);
                }
                cmd.result = KVS_ERR_EXIST_PENDING_KEY;
                if(atomic_dec_and_test(&async_rq->refcount)){
                    free_kv_async_rq(async_rq);
                }else{
                    pr_err("[%d:%s]ERROR : The refcount must be 0, but it isn't (%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
                    spin_unlock(&async_rq->lock);
                }
#ifndef NO_LOG
                atomic64_inc(&kv_total_get_async_merge);
#endif
                continue;

            }else{/*cmd.opcode == (nvme_cmd_kv_store) || (nvme_cmd_kv_delete) */
                /*
                 * If offset R/W is supported, we need to check the offset 
                 * in the hash related functions before doing something.
                 */
                if(buffered_rq->data_length)
                    kfree(buffered_rq->data_addr);
                buffered_rq->data_addr = async_rq->data_addr;
                buffered_rq->data_length = async_rq->data_length;
                memcpy(&buffered_rq->c, &async_rq->c, sizeof(struct nvme_command));
                if(atomic_dec_and_test(&buffered_rq->refcount)){
                    pr_err("[%d:%s]ERROR : The refcount is 0, but not desired (%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
                    free_kv_async_rq(buffered_rq);
                }else
                    spin_unlock(&buffered_rq->lock);

                async_rq->data_addr = NULL;
                async_rq->data_length = 0 ;
                if(atomic_dec_and_test(&async_rq->refcount)){
                    free_kv_async_rq(async_rq);
                }else{
                    spin_unlock(&async_rq->lock);
                    pr_err("[%d:%s]ERROR : The refcount of the new rq that failed to insert must be zero but non-zero.(%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
                }

                status = KVS_SUCCESS;
                cmd.result = KVS_ERR_EXIST_PENDING_KEY;
#ifndef NO_LOG
                atomic64_inc(&kv_total_put_del_async_merge);
#endif
                continue;
            }
        }
        ////////////////////// END find same key in buffer queue //////////////////////////

        /*
         * If same key was inserted while creating the request, restart from searching same key in the hashs.
         */

        async_rq->memq = memq;
        list_add(&async_rq->lru, &async_rq->memq->rq);
        atomic_inc(&async_rq->memq->nr_rq);
        if(async_rq->blocking_rq){
            atomic_inc(&async_rq->memq->nr_pending);
            /* Release the in-flight RQ lock */
            if(atomic_dec_and_test(&async_rq->blocking_rq->refcount)){
                pr_err("[%d:%s]ERROR : The refcount of the inflight rq which blocks new rq must be non-zero, but zero(%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
                free_kv_async_rq(async_rq->blocking_rq);
            }else
                spin_unlock(&async_rq->blocking_rq->lock);
        }
        spin_unlock(&async_rq->lock);
        /* If nvme_cmd_kv_store or nvme_cmd_kv_delete, delete the request from readahead q if the request exists in the q */
        if(!is_kv_retrieve_cmd(cmd.opcode)){
            struct kv_async_request *ra_rq = kv_hash_find_and_del_readahead(&kv_iosched->kv_hash[KV_RA_HASH], async_rq->key_addr, async_rq->key_length, async_rq->hash_key);
            if(ra_rq){ // delete the readahead request from the LRU list
                spin_lock(&ra_rq->lock);

                if(atomic_dec_and_test(&ra_rq->refcount)){
                    free_kv_async_rq(ra_rq);
                }else{
                    pr_err("[%d:%s]ERROR : The refcount of deleted ra rq be zero but non-zero(%d).(%d : %s)\n", __LINE__, __func__, atomic_read(&ra_rq->refcount),current->pid, current->comm);
                    spin_unlock(&ra_rq->lock);
                }
            }
        }
    } /* for(i=0;i<count;i++) */

    /*
     * If the buffer Q was switched to submission Q 
     * while "kv_hash_insert" or wakeup delayed workueue
     * the delayed workqueue may do nothing when it wakes up.
     * In this case, the "nr_rq" must be 0
     */
    if(!list_empty(&memq->rq)){
        struct kv_work_struct *submitd = &kv_iosched->submit_daemon[request_type];

        spin_lock(&submitd->mumemq_lock);
        list_add_tail(&memq->memq_lru, &submitd->mumemq_list);
        spin_unlock(&submitd->mumemq_lock);

        spin_lock(&submitd->work_lock);
        queue_work(submitd->submit_wq, &submitd->submit_work);
        spin_unlock(&submitd->work_lock);
    }else{
        kv_memq_free(memq);
    }


    status = KVS_SUCCESS;//kv_iosched->status;//__nvme_submit_iosched_kv_cmd(kv_iosched);
    cmd.result = KVS_SUCCESS;

    if (likely(status >= 0)) {
        if (put_user(cmd.result, &ucmd->result))
            status = -EFAULT;
    }
#ifndef NO_LOG
    atomic64_inc(&kv_total_async_request);
#endif
    return status;
}
int nvme_kv_flush_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd)
{
    struct kv_iosched_struct *kv_iosched = ns->kv_iosched;
    struct kv_async_request *rq = NULL;
    struct kv_memqueue_struct *memq;
    int i;

    struct nvme_passthru_kv_cmd cmd;
    void    *key = NULL;

    int status = 0;
    enum memq_type type;
#ifndef NO_LOG
    struct timeval start, stop, wait_start, wait_stop;

    do_gettimeofday(&start);
#endif
#ifdef KV_NVME_REQUIRE_SYS_ADMIN
    if (unlikely(!capable(CAP_SYS_ADMIN)))
        return -EACCES;
#endif
    if (unlikely(copy_from_user(&cmd, ucmd, sizeof(cmd))))
        return -EFAULT;
    if (unlikely(cmd.flags)){
        put_user(KVS_ERR_INVALID_FLAGS, &ucmd->result);
        return -EINVAL;
    }
    type = is_kv_retrieve_cmd(cmd.opcode) ? ACTIVE_READ :  ACTIVE_WRITE;
    /* 
     * If key length is less than KVCMD_INLINE_KEY_MAX,
     * the key was copied to cmd via copy_from_user() above.
     * Otherwise, allocate kernel memory and copy the data from user to kernel
     * If the key size is less than KVCMD_INLINE_KEY_MAX,
     * unused memory for key should be filled with 0. 
     */
    if (cmd.key_length > KVCMD_INLINE_KEY_MAX){
        if(!kv_get_key(&cmd, &status)){
            put_user(cmd.result, &ucmd->result);
            return status;
        }
        /*
         * memcpy is not necessary because key_addr points 
         * kernel memory which is allocated via kmalloc
         */
        key = (void *)cmd.key_addr;
    }else if(cmd.key_length){
        /* 
         * memcpy for key data is required(nvme_passthru -> nvme_command) 
         * on nvme_command initialization 
         */
        key = cmd.key;
    }
#ifndef NO_LOG
    atomic64_inc(&kv_flush_statistics[KV_flush_begin]);
#endif
    if(key){

        /* If it is a get(retrieve) request, check readahead hash */
        rq = kv_hash_find_refcnt(&kv_iosched->kv_hash[KV_RQ_HASH], key, cmd.key_length, get_hash_key(key, cmd.key_length));
        if (!rq){
#ifndef NO_LOG
            atomic64_inc(&kv_flush_statistics[KV_flush_single_nokey_found]);
#endif
            if (cmd.key_length > KVCMD_INLINE_KEY_MAX)
                kfree(key);
            goto out;
        }
        spin_lock(&rq->lock);
        if(!test_bit(KV_async_completed, &rq->state)){
#ifndef NO_LOG
            atomic64_inc(&kv_flush_statistics[KV_flush_single]);
#endif
            if(type == ACTIVE_WRITE && !test_bit(KV_async_flush_wakeup, &rq->state)){
                set_bit(KV_async_flush_wakeup, &rq->state);
            }
            spin_unlock(&rq->lock);
            for_each_possible_cpu(i){
                memq = *per_cpu_ptr(kv_iosched->percpu_memq[type], i);
                if(atomic_read(&memq->nr_rq)){ 
                    //if(!kv_sched_wakeup_delayed(i,kv_iosched, type, 0))
                    kv_sched_wakeup_immediately(i, kv_iosched, type);
                }
            }
            kv_sched_flush_workqueue(kv_iosched, type);
#ifndef NO_LOG
            do_gettimeofday(&wait_start);
#endif
            if(type == ACTIVE_WRITE) 
                wait_on_bit_io(&rq->state, KV_async_flush_wakeup, TASK_KILLABLE);

            // Release rq reference count
            (void)kv_decrease_req_refcnt(rq, true);

#ifndef NO_LOG
            do_gettimeofday(&wait_stop);
            atomic64_inc(&kv_flush_statistics[KV_flush_done]);
            atomic64_add((timeval_to_msec(&wait_stop)-timeval_to_msec(&wait_start)), &kv_flush_statistics[KV_flush_wait_time]);
#endif
        }else{
            // Release rq reference count
            // note that the rq lock would be relased inside if it reaches zero when it returns true.
            if(kv_decrease_req_refcnt(rq, false) == false)
                spin_unlock(&rq->lock);
#ifndef NO_LOG
            atomic64_inc(&kv_flush_statistics[KV_flush_single_cancel]);
#endif
        }
    }else if(atomic_read(&kv_iosched->kv_hash[KV_RQ_HASH].nr_rq)){
        struct kv_work_struct *submitd = &kv_iosched->submit_daemon[WRITE_DAEMON]; 
        /* same key exists && the key is under submission */
        //unsigned long flags;
        /* Change active Meme Queue to immutable Mem Queue*/
        for_each_possible_cpu(i){
            memq = *per_cpu_ptr(kv_iosched->percpu_memq[type], i);
            if(atomic_read(&memq->nr_rq)){ 
                //if(!kv_sched_wakeup_delayed(i,kv_iosched, type, 0))
                kv_sched_wakeup_immediately(i, kv_iosched, type);
            }
        }
        kv_sched_flush_workqueue(kv_iosched, type);
        spin_lock(&submitd->mumemq_lock);
        if(!list_empty(&submitd->mumemq_list)){
            memq = list_last_entry(&submitd->mumemq_list, struct kv_memqueue_struct, memq_lru);
            if(!list_empty(&memq->rq)){
                rq = list_last_entry(&memq->rq, struct kv_async_request, lru);
#ifndef NO_LOG
                atomic64_inc(&kv_flush_statistics[KV_flush_all]);
#endif
                spin_lock(&rq->lock);
                if(type == ACTIVE_WRITE && !test_bit(KV_async_flush_wakeup, &rq->state)){
                    set_bit(KV_async_flush_wakeup, &rq->state);
                }
                spin_unlock(&rq->lock);
                spin_unlock(&submitd->mumemq_lock);
#ifndef NO_LOG
                do_gettimeofday(&wait_start);
#endif
                if(type == ACTIVE_WRITE) 
                    wait_on_bit(&rq->state, KV_async_flush_wakeup, TASK_KILLABLE);
#ifndef NO_LOG
                do_gettimeofday(&wait_stop);
                atomic64_inc(&kv_flush_statistics[KV_flush_done]);
                atomic64_add((timeval_to_msec(&wait_stop)-timeval_to_msec(&wait_start)), &kv_flush_statistics[KV_flush_wait_time]);
#endif
            }else{
                spin_unlock(&submitd->mumemq_lock);
                pr_err("[%d:%s] the memQ selected for flush is NULL\n", __LINE__, __func__);
            }
        }else{
            spin_unlock(&submitd->mumemq_lock);
#ifndef NO_LOG
            atomic64_inc(&kv_flush_statistics[KV_flush_all_cancel]);
#endif
        }
    }
out:
#ifndef NO_LOG
    do_gettimeofday(&stop);
    atomic64_add((timeval_to_msec(&stop)-timeval_to_msec(&start)), &kv_flush_statistics[KV_flush_entire_time]);

    atomic64_inc(&kv_flush_statistics[KV_flush_end]);
#endif
    if (put_user(KVS_SUCCESS, &ucmd->result))
        return -EFAULT;
    return 0;
}

int nvme_kv_discard_readahead_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd)
{
    struct kv_iosched_struct *kv_iosched = ns->kv_iosched;
    struct list_head *ra_data;
    struct list_head *next_ele;
    struct kv_hash *ra_hash = &kv_iosched->kv_hash[KV_RA_HASH];
    struct nvme_passthru_kv_cmd cmd;
    void    *key = NULL;

    int status = 0;

#ifdef KV_NVME_REQUIRE_SYS_ADMIN
    if (unlikely(!capable(CAP_SYS_ADMIN)))
        return -EACCES;
#endif
    if (unlikely(copy_from_user(&cmd, ucmd, sizeof(cmd))))
        return -EFAULT;
    if (unlikely(cmd.flags)){
        put_user(KVS_ERR_INVALID_FLAGS, &ucmd->result);
        return -EINVAL;
    }


    if (cmd.key_length > KVCMD_INLINE_KEY_MAX){
        if(!kv_get_key(&cmd, &status)){
            put_user(cmd.result, &ucmd->result);
            return status;
        }
        /*
         * memcpy is not necessary because key_addr points 
         * kernel memory which is allocated via kmalloc
         */
        key = (void *)cmd.key_addr;
    }else if(cmd.key_length){
        /* 
         * memcpy for key data is required(nvme_passthru -> nvme_command) 
         * on nvme_command initialization 
         */
        key = cmd.key;
    }
    if(key){
        struct kv_async_request *ra_rq = NULL;
        u32 hash_key = get_hash_key(key, cmd.key_length);
        kv_hash_find_read_and_invalidate(&kv_iosched->kv_hash[KV_RQ_HASH], key, cmd.key_length, hash_key);
        ra_rq = kv_hash_find_and_del_readahead(&kv_iosched->kv_hash[KV_RA_HASH], key, cmd.key_length, hash_key);
        if(ra_rq){ // delete the readahead request from the LRU list
            spin_lock(&ra_rq->lock);

            if(atomic_dec_and_test(&ra_rq->refcount)){
                free_kv_async_rq(ra_rq);
            }else{
                pr_err("[%d:%s]ERROR : The refcount of deleted ra rq be zero but non-zero.(%d : %s)\n", __LINE__, __func__, current->pid, current->comm);
                spin_unlock(&ra_rq->lock);
            }
#ifndef NO_LOG
            atomic64_inc(&kv_total_discard_ra);
#endif
        }

    }else{
        LIST_HEAD(tmp_ra_list);
        spin_lock(&kv_iosched->ra_lock);
        list_splice_init(&kv_iosched->ra_list, &tmp_ra_list);
        spin_unlock(&kv_iosched->ra_lock);
        list_for_each_safe(ra_data, next_ele, &tmp_ra_list) 
        {
            struct kv_async_request *rq = list_entry(ra_data, struct kv_async_request, lru);

            if(!kv_hash_delete_readahead(ra_hash, rq)){
                spin_lock(&rq->lock);
#ifndef NO_LOG
                atomic64_inc(&kv_total_discard_ra);
#endif
                if(atomic_dec_and_test(&rq->refcount))
                    free_kv_async_rq(rq);
                else{/* A request may access this request */
                    pr_err("[%d:%s]Fail to discard a request because the refcount is not 0(it is %d)\n", __LINE__, __func__, atomic_read(&rq->refcount));
                    spin_unlock(&rq->lock);
                }
            }
        }
    }
    if (put_user(KVS_SUCCESS, &ucmd->result))
        return -EFAULT;
    return 0;
}

#if 1
int nvme_kv_read_from_readahead_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd)
{
    struct nvme_passthru_kv_cmd cmd;
    int status=0;
    void *key = NULL;
    struct kv_iosched_struct *kv_iosched = ns->kv_iosched;


#ifndef NO_LOG
    atomic64_inc(&kv_total_sync_request);
#endif
#ifdef KV_NVME_REQUIRE_SYS_ADMIN
    if (unlikely(!capable(CAP_SYS_ADMIN)))
        return -EACCES;
#endif
    if (unlikely(copy_from_user(&cmd, ucmd, sizeof(cmd))))
        return -EFAULT;
    if (unlikely(cmd.flags)){
        put_user(KVS_ERR_INVALID_FLAGS, &ucmd->result);
        return -EINVAL;
    }
    if (unlikely(!is_kv_retrieve_cmd(cmd.opcode))){
        pr_err("[%d:%s]: The command is not supported! kv cmd flags(%d) opcode(%02x), data len(%d), key len(%d)\n", __LINE__, __func__, cmd.flags, cmd.opcode, cmd.data_length, cmd.key_length);
        put_user(KVS_ERR_INVALID_OPCODE, &ucmd->result);
        return -EINVAL;
    }

    if (cmd.key_length > KVCMD_INLINE_KEY_MAX){
        if(!kv_get_key(&cmd, &status)){
            put_user(cmd.result, &ucmd->result);
            return status;
        }
        key = (void *)cmd.key_addr;
    }else
        key = cmd.key; /* memcpy for key data is required on nvme_command initialization */
    /**
     * To return size of data for get command
     */
    cmd.result = 0;

    /* 
     * Check whether same key exists in READAHEAD hash or not 
     */
    if(kv_do_readahead(key, get_hash_key(key, cmd.key_length), kv_iosched, &cmd)){
        status = cmd.status;
        put_user(KVS_ERR_EXIST_IN_RA_HASH, &ucmd->status);
#ifndef NO_LOG
        atomic64_inc(&kv_total_ra_hit);
#endif
#ifdef INSDB_DEBUG
        pr_err("[%d:%s]Hit readahead key[%lld|%lld]\n", __LINE__, __func__,*((u64*)key),*(((u64*)key)+1));
#endif
    }
#ifndef NO_LOG
    else
        atomic64_inc(&kv_total_nonblocking_read_fail);
#endif
    if (status >= 0) {
        if (put_user(cmd.result, &ucmd->result))
            return -EFAULT;
    }

    return status;
}

#endif
