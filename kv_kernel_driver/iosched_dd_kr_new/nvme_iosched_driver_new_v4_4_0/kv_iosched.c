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
#include <linux/workqueue.h>
#include <linux/kernel.h>
#include <linux/jiffies.h>
#include <linux/slab.h>
#include <asm/bitops.h>
#include <asm/tsc.h>

#include "kv_iosched.h"
#include "kv_fifo.h"
#include "nvme.h"

#if 0
#ifdef KV_TIME_MEASUREMENT
    cycles_t s_time, e_time;
#endif
#ifdef KV_TIME_MEASUREMENT
    s_time = get_cycles();
#endif
#ifdef KV_TIME_MEASUREMENT
    e_time = get_cycles();
    pr_err("[%d:%s]SyncIO done for 1 rq : $%lld$us!!\n", __LINE__, __func__, (e_time - s_time) / 4000);
#endif
#endif

////////////////// SLAB allocator //////////////////
#ifndef NO_LOG
atomic64_t kv_total_async_request;
atomic64_t kv_total_get_async_merge;
atomic64_t kv_total_put_del_async_merge;
atomic64_t kv_total_async_submit;
atomic64_t kv_total_sync_request;
atomic64_t kv_total_sync_submit;
atomic64_t kv_total_blocked_submit;
atomic64_t kv_total_async_interrupt;
atomic64_t kv_total_async_complete;
atomic64_t kv_total_ra_hit;
atomic64_t kv_total_nonblocking_read_fail;
atomic64_t kv_total_ra_hit_in_flight;
atomic64_t kv_total_canceled_ra;
atomic64_t kv_total_discard_ra;
atomic64_t kv_total_discard_not_found_ra;
atomic64_t kv_total_discard_cancel_not_on_device;
atomic64_t kv_total_discard_on_device;
atomic64_t kv_total_sync_complete;
atomic64_t kv_total_sync_finished;
atomic64_t kv_completion_retry;
atomic64_t kv_completion_rq_empty;
atomic64_t kv_completion_wakeup_by_int;
atomic64_t kv_completion_wakeup_by_submit;
atomic64_t kv_flush_wakeup_by_submit;
atomic64_t kv_flush_wakeup_by_completion;
atomic64_t kv_flush_statistics[KV_flush_nr];
atomic64_t kv_hash_restart[KV_hash_restart_nr];
atomic64_t kv_async_get_req;
atomic64_t kv_async_put_req;
atomic64_t kv_async_del_req;
atomic64_t kv_sync_get_req;
atomic64_t kv_sync_get_merge_with_async_put;
atomic64_t kv_sync_get_merge_with_async_del;
atomic64_t kv_sync_put_req;
atomic64_t kv_sync_del_req;
atomic64_t kv_sync_iter_req;
atomic64_t kv_sync_iter_read_req;
atomic64_t kv_sync_get_log;
atomic64_t kv_async_error[8];

/*kv code tracer*/
char kvct_ioctl[8][255];
char kvct_sync_io[8][255];
char kvct_submission[NR_DAEMON][255];
char kvct_int_handle[255];
char kvct_completion[NR_KV_SSD][255];
char kvct_discard_readahead[8][255];
char kvct_flush[8][255];
#endif
#if 0 
/*Sync I/O trace*/
#ifdef KV_NVME_CT
    int trace_id = smp_processor_id();
#endif
#ifdef KV_NVME_CT
    sprintf(kvct_sync_io[trace_id], "[%d:%s][key = %s(0x%x)]\n", __LINE__, __func__, (char *)key, hash_key);
#endif

/* Submission daemon trace */
#ifdef KV_NVME_CT
    int type = submitd->type;
#endif
#ifdef KV_NVME_CT
        sprintf(kvct_submission[type], "[%d:%s]\n", __LINE__, __func__);
#endif
#ifdef KV_NVME_CT
    sprintf(kvct_submission[type], "[%d:%s][key = %s(0x%x)]\n", __LINE__, __func__, (char *)rq->key_addr, rq->hash_key);
#endif

/* Async I/O trace */
#ifdef KV_NVME_CT
    int trace_id = smp_processor_id();
#endif
#ifdef KV_NVME_CT
    sprintf(kvct_ioctl[trace_id], "[%d:%s][key = %s(0x%x)]\n", __LINE__, __func__, (char *)async_rq->key_addr, async_rq->hash_key);
#endif

/* Interrupt Handler trace*/
#ifdef KV_NVME_CT
    sprintf(kvct_int_handle, "[%d:%s][key = %s(0x%x)]\n", __LINE__, __func__, (char *)rq->key_addr, rq->hash_key);
#endif

/*Completion Daemon*/
#ifdef KV_NVME_CT
        sprintf(kvct_completion, "[%d:%s][key = %s(0x%x)]\n", __LINE__, __func__, (char *)rq->key_addr, rq->hash_key);
#endif
/*Discard ReadAhead*/
#ifdef KV_NVME_CT
    int trace_id = smp_processor_id();
#endif
#ifdef KV_NVME_CT
    sprintf(kvct_discard_readahead[trace_id], "[%d:%s]]\n", __LINE__, __func__);
#endif


/* Flush */
#ifdef KV_NVME_CT
    int trace_id = smp_processor_id();
#endif
#ifdef KV_NVME_CT
    sprintf(kvct_flush[trace_id], "[%d:%s]]\n", __LINE__, __func__);
#endif

#endif


static struct kmem_cache *kv_memqueue_struct_cachep;
static struct kmem_cache *kv_sync_request_cachep;
static struct kmem_cache *kv_async_request_cachep;

inline u32 get_hash_key(void *key, u32 key_length){
    if(key_length  > KVCMD_INLINE_KEY_MAX)
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,15,0)
        return hash_64((u64)full_name_hash(key, key_length), KV_HASH_BITS);
#else
        return hash_64((u64)full_name_hash(NULL, key, key_length), KV_HASH_BITS);
#endif
    else
        return  hash_128(*(uint128_t *)key, KV_HASH_BITS);
}
////////////// Memory Queue(Per CPU Buffer list) structure //////////////
void kv_memqueue_struct_init_once(struct kv_memqueue_struct *memq)
{
    memset(memq, 0, sizeof(*memq));
    INIT_LIST_HEAD(&memq->rq); /* Head of requests list*/
    atomic_set(&memq->nr_pending, 0);
    atomic_set(&memq->nr_rq, 0);
}

static void init_memq_once(void *foo)
{
    struct kv_memqueue_struct *rq = (struct kv_memqueue_struct*) foo;

    kv_memqueue_struct_init_once(rq);
}
struct kv_memqueue_struct *kv_memq_alloc(void)
{
    return kmem_cache_alloc(kv_memqueue_struct_cachep, GFP_KERNEL);
}

void kv_memq_free(struct kv_memqueue_struct *memq)
{
    memq->key_low = 0;
    memq->key_high = 0;
#if 0
    if(atomic_read(&memq->nr_pending) || atomic_read(&memq->nr_rq) || !list_empty(&memq->rq))
        BUG();
#endif
    kmem_cache_free(kv_memqueue_struct_cachep, memq);
}

////////////// Async IO request ///////////////////////
void kv_async_request_init_once(struct kv_async_request *rq)
{
    memset(rq, 0, sizeof(*rq));
    spin_lock_init(&rq->lock);
}

static void init_async_once(void *foo)
{
    struct kv_async_request *rq = (struct kv_async_request*) foo;

    kv_async_request_init_once(rq);
}
inline struct kv_async_request *kv_async_request_alloc(void)
{
    struct kv_async_request *ret = kmem_cache_alloc(kv_async_request_cachep, GFP_KERNEL);
    memset(ret, 0, sizeof(*ret));

	INIT_LIST_HEAD(&ret->lru);
    spin_lock_init(&ret->lock);
    return ret;
}
inline void kv_async_request_free(struct kv_async_request *rq)
{
    kmem_cache_free(kv_async_request_cachep, rq);
}

////////////// Sync IO request ///////////////////////
void kv_sync_request_init_once(struct kv_sync_request *rq)
{
    memset(rq, 0, sizeof(*rq));
}

static void init_sync_once(void *foo)
{
    struct kv_sync_request *rq = (struct kv_sync_request*) foo;

    kv_sync_request_init_once(rq);
}
inline struct kv_sync_request *kv_sync_request_alloc(void)
{
    return kmem_cache_alloc(kv_sync_request_cachep, GFP_KERNEL);;
}

inline void kv_sync_request_free(struct kv_sync_request *rq)
{
    kmem_cache_free(kv_sync_request_cachep, rq);
}
////////////////// End of SLAB allocator //////////////////


void kv_scheduling_workfn(struct work_struct *work)
{
    /*
     * Memory Queue migration should be handled atomically to avoid active_memq pointer corruption.
     * We don't need to have a lock. We prevent schedule-out using local_irq_save/restore instead of it.
     */

    struct kv_iosched_struct *kv_iosched = (struct kv_iosched_struct *)(container_of(to_delayed_work(work),
                struct kv_dwork_struct , dwork)->addr);
    int memq_id = (int)(container_of(to_delayed_work(work), struct kv_dwork_struct , dwork)->type);
    int reserved_memq = memq_id + GET_RESERVED;
    unsigned long flags;
    /*
     * Memory Queue migration should be handled atomically to avoid active_memq pointer corruption.
     * We don't need to have a lock. We prevent schedule-out using local_irq_save/restore instead of it.
     */
    struct kv_memqueue_struct *mutable_memq;
    struct kv_memqueue_struct *new_memq;
    //printk("%s(%d)\n", __func__, smp_processor_id());
    if(!*this_cpu_ptr(kv_iosched->percpu_memq[reserved_memq])){
        /* 
         * Memory pressure has been occured. 
         * So that, a new Memory Queue has not been secured(researved).
         */
        new_memq = kv_memq_alloc();
        if(!new_memq){
            pr_err("[%d:%s]Fail to allocate kv_memqueue_struct\n", __LINE__, __func__);
            return;
        }
        *this_cpu_ptr(kv_iosched->percpu_memq[reserved_memq]) = new_memq;
    }

    local_irq_save(flags);
    /* Get active memq from current CPU */
    /* The memq'll become mutable memq */
    mutable_memq = *this_cpu_ptr(kv_iosched->percpu_memq[memq_id]);

    *this_cpu_ptr(kv_iosched->percpu_memq[memq_id]) = *this_cpu_ptr(kv_iosched->percpu_memq[reserved_memq]);
    local_irq_restore(flags);
    /*
     * Allocate new kv_memqueue_struct for next scheduling(active->mutable Memory Queue)
     */
    new_memq = kv_memq_alloc();
    if(!new_memq)
        pr_err("[%d:%s]Fail to allocate kv_memqueue_struct for reserved_memq\n", __LINE__, __func__);
    *this_cpu_ptr(kv_iosched->percpu_memq[reserved_memq]) = new_memq;

    /*
     * Insert new mutable Memory Queue to the list
     */
    if(!list_empty(&mutable_memq->rq)){
        struct kv_work_struct *submitd = &kv_iosched->submit_daemon[memq_id]; 
        spin_lock(&submitd->mumemq_lock);
        list_add_tail(&mutable_memq->memq_lru, &submitd->mumemq_list);
        spin_unlock(&submitd->mumemq_lock);

        /*
         * Wake submission daemon up
         * We have multiple scheduling daemon, so lock is required.
         */
        spin_lock(&submitd->work_lock);
        /*
         * If we assign this CPU to the submission deamon, 
         * we may not utilize all CPUs when the number of application threads are less than number of CPUs.
         * In this case, the performance may be sacrificed because application & daemon run on same CPU 
         * even thought other CPUs are idle.
         * However, if number of threads are more than number of CPUs, 
         * it may be better choice to use queue_work_on for performance isolation.
         */
#if 0
        {

            // queue_work_on(smp_processor_id(), kv_iosched->submit_wq, &kv_iosched->submit_work);
            queue_work_on(cpu, kv_iosched->submit_wq, &kv_iosched->submit_work);
            if(++cpu == smp_processor_id())
                cpu++;
            if(cpu >= num_online_cpus())
                cpu = 0 ;

        }
#else
        queue_work(submitd->submit_wq, &submitd->submit_work);
#endif
        spin_unlock(&submitd->work_lock);

    }else{
        //pr_err("[%d:%s]The new mutable memQ has NO RQ( Empty!! NULL!! )\n", __LINE__, __func__);
        kv_memq_free(mutable_memq);
    }
}

/*
 *  KV cmd submission to the device
 */
void kv_submission_workfn(struct work_struct *work)
{

    struct kv_work_struct *submitd = (struct kv_work_struct *)container_of(work, struct kv_work_struct , submit_work);
    struct kv_iosched_struct *kv_iosched = (struct kv_iosched_struct *)(submitd->addr);

    int ret = 0;
    unsigned long flags;
    /*
     * For time measurement.
     cycles_t s_time, e_time;
     s_time = get_cycles();
     e_time = get_cycles();
     unsigned long nr;
     */
    struct list_head *list = &submitd->mumemq_list;
    struct kv_memqueue_struct *memq;
    while(!list_empty(list)){

        spin_lock(&submitd->mumemq_lock);
        memq = list_first_entry(list, struct kv_memqueue_struct, memq_lru);
        /*
         * Pending requests can be inserted only to active Memory Queue.
         * This means mutable Memory Queue does not take pending requests anymore.
         */
        if(atomic_read(&memq->nr_pending)){
            list_move_tail(&memq->memq_lru, list);
            spin_unlock(&submitd->mumemq_lock);
            if( atomic_read(&kv_iosched->complete_fifo.nr_ele) ){
                spin_lock_irqsave(&kv_iosched->complete_lock,flags);
#ifdef DELAYED_COMPLETE_WORK
                if(queue_delayed_work(kv_iosched->complete_wq, &kv_iosched->complete_dwork, 0))
                {
#ifndef NO_LOG
                    atomic64_inc(&kv_completion_wakeup_by_submit);
#endif
                }
#else
                if(!work_busy(&kv_iosched->complete_work) && queue_work(kv_iosched->complete_wq, &kv_iosched->complete_work))
                {
#ifndef NO_LOG
                    atomic64_inc(&kv_completion_wakeup_by_submit);
#endif
                }
#endif
                spin_unlock_irqrestore(&kv_iosched->complete_lock,flags);
            }
            /* 
             * spin unlock is necessary at this point 
             * to allow sched daemon to insert an active Memory Queue to mutable list
             */
            //set_bit(KV_iosched_need_to_wake_submit_daemon,  &kv_iosched->state);
            set_bit(KV_iosched_need_to_wake_write_submit_daemon,  &kv_iosched->state);
            switch(submitd->type){
                case READ_DAEMON:
                    set_bit(KV_iosched_need_to_wake_read_submit_daemon,  &kv_iosched->state);
                    break;
                case WRITE_DAEMON:
                    set_bit(KV_iosched_need_to_wake_write_submit_daemon,  &kv_iosched->state);
                    break;
                case BATCH_READ_DAEMON:
                    set_bit(KV_iosched_need_to_wake_multi_read_submit_daemon,  &kv_iosched->state);
                    break;
                case BATCH_WRITE_DAEMON:
                    set_bit(KV_iosched_need_to_wake_multi_write_submit_daemon,  &kv_iosched->state);
                    break;
                default:
                    pr_err("[%d:%s]Wrong daemon type\n", __LINE__, __func__);
                    break;
            }
            //wait_on_bit(&kv_iosched->state, KV_iosched_need_to_wake_submit_daemon, TASK_UNINTERRUPTIBLE);
            //continue;
            break;
        }
        list_del(&memq->memq_lru);
        spin_unlock(&submitd->mumemq_lock);
        ///////////////
        /*
         * Only BATCH_READ_DAEMON uses KV_async_waiting_submission for prefetch perposes
         */
        if(memq->key_low || memq->key_high){

            /* If it is a get(retrieve) request, check readahead hash */
            struct kv_async_request *rq = kv_hash_find_refcnt(&kv_iosched->kv_hash[KV_RQ_HASH], memq->blocking_key, KVCMD_INLINE_KEY_MAX, get_hash_key(memq->blocking_key, KVCMD_INLINE_KEY_MAX));
            if (rq){
                int i;
                spin_lock(&rq->lock);
                if(!test_bit(KV_async_completed, &rq->state) && !test_bit(KV_async_inFlight_rq, &rq->state)){
                    if(!test_bit(KV_async_waiting_submission, &rq->state)){
                        set_bit(KV_async_waiting_submission, &rq->state);
                    }
                    spin_unlock(&rq->lock);
                    for_each_possible_cpu(i){
                        if(rq->memq ==  *per_cpu_ptr(kv_iosched->percpu_memq[READ_DAEMON], i)){
                            struct kv_dwork_struct *schedd = per_cpu_ptr(kv_iosched->percpu_schedd[READ_DAEMON], i); 
                            if(atomic_read(&rq->memq->nr_rq)){ 
                                kv_sched_wakeup_immediately(i, kv_iosched, READ_DAEMON);
                            }
                            flush_delayed_work(&schedd->dwork);
                            break;
                        }
                    }

                    wait_on_bit_io(&rq->state, KV_async_waiting_submission, TASK_UNINTERRUPTIBLE);

                    // Release rq reference count
                    (void)kv_decrease_req_refcnt(rq, true);

                }else{
                    // Release rq reference count
                    // note that the rq lock would be relased inside if it reaches zero when it returns true.
                    if(kv_decrease_req_refcnt(rq, false) == false)
                        spin_unlock(&rq->lock);
                }
            }
        }
        /////////////////




        ret = __nvme_submit_iosched_kv_cmd(memq);
        if(atomic_read(&memq->nr_rq)){
            /*
             * nr_rq and submitted_rq should be same for most of time.
             * But if it is different(an error has occured), nr_rq must be bigger than submitted_rq.
             */
            pr_err("[%d:%s]Submission error return to daemon(remained rq : %d)\n", __LINE__, __func__, atomic_read(&memq->nr_rq));
            spin_lock(&submitd->mumemq_lock);
            list_add_tail(&memq->memq_lru, list);
            spin_unlock(&submitd->mumemq_lock);
            if(ret) BUG();
        }else
            kv_memq_free(memq);
    }
    /*
    for_each_possible_cpu(i){
        memq = *per_cpu_ptr(kv_iosched->percpu_memq[type], i);
        if(atomic_read(&memq->nr_rq)){ 
            if(!kv_sched_wakeup_delayed(i,kv_iosched, type, 0))
                kv_sched_wakeup_immediately(i, kv_iosched, type);
            restart = true;
        }
        if(restart) goto restart;
    }
    */
}
void kv_completion_workfn(struct work_struct *work){
#ifdef DELAYED_COMPLETE_WORK
    struct kv_iosched_struct *kv_iosched = container_of(to_delayed_work(work),
            struct kv_iosched_struct, complete_dwork);
#else
    struct kv_iosched_struct *kv_iosched = container_of(work,
            struct kv_iosched_struct, complete_work);
#endif
    struct kv_async_request *rq;
    bool has_it_been_run = false;

restart:
    while((rq = kv_fifo_dequeue(&kv_iosched->complete_fifo))!=NULL)
    {
        has_it_been_run = true;
#if 0
        /*For "flush" command*/
        if(test_and_clear_bit(KV_async_flush_wakeup, &rq->state)){
            atomic64_inc(&kv_flush_wakeup_by_completion);
            wake_up_bit(&rq->state, KV_async_flush_wakeup);
        }
#endif
        atomic_dec(&rq->iosched->in_flight_rq);
        /* For congestion control */
        if(test_and_clear_bit(KV_iosched_congested, &rq->iosched->state))
            wake_up_bit(&rq->iosched->state, KV_iosched_congested);

        kv_hash_delete(&rq->iosched->kv_hash[KV_RQ_HASH], rq);
        if(atomic_read(&rq->iosched->kv_hash[KV_RQ_HASH].nr_rq) < KV_PENDING_QUEU_LOW_WATERMARK){
            if(test_and_clear_bit(KV_iosched_queue_full, &rq->iosched->state))
                wake_up_bit(&rq->iosched->state, KV_iosched_queue_full);
        }
        spin_lock(&rq->lock);//The rq can wait at here til the new rq having same key releases this rq's lock. 
        set_bit(KV_async_completed, &rq->state);
        /*If KV_async_blocking_rq is set, it means that put request having same key is waiting for this request*/
        /*
         * KV_async_inFlight_rq is always on
         * So that, we don't need to use test_and_clear_bit.
         * Sync IOs may wait this bit.
         */
        
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,15,0)
        rq->errors=rq->req->errors;
#else
        rq->errors=nvme_req(rq->req)->status;
#endif
#ifndef NO_LOG
        switch(rq->errors){
            case 0x0:
                atomic64_inc(&kv_async_error[kv_async_success]);
                break;
            case 0x1:
            case 0x301:
                atomic64_inc(&kv_async_error[kv_async_invalidValueSize]);
                break;
            case 0x3:
            case 0x303:
                atomic64_inc(&kv_async_error[kv_async_invalidKeySize]);
                break;
            case 0x8:
            case 0x308:
                atomic64_inc(&kv_async_error[kv_async_misiAlignment]);
                break;
            case 0x11:
            case 0x311:
                atomic64_inc(&kv_async_error[kv_async_unRecovered]);
                break;
            case 0x12:
            case 0x312:
                atomic64_inc(&kv_async_error[kv_async_exceedDeviceCap]);
                break;
            case 0x80:
            case 0x380:
                atomic64_inc(&kv_async_error[kv_async_idempotentError]);
                break;
            default : 
                atomic64_inc(&kv_async_error[kv_async_unknownError]);
        }
#endif

        if(is_kv_retrieve_cmd(rq->c.common.opcode)) {
#if LINUX_VERSION_CODE < KERNEL_VERSION(4,15,0)
            rq->result = le32_to_cpu(rq->cqe.result);
#else
            rq->result = le32_to_cpu(rq->cqe.result.u32);
#endif
        }
        else
            rq->result = rq->data_length;

        if(is_kv_retrieve_cmd(rq->c.common.opcode)){
            struct kv_async_request *ra_rq;
            ra_rq = kv_hash_insert_readahead(&rq->iosched->kv_hash[KV_RA_HASH], rq);
            if(!ra_rq){
                set_bit(KV_async_in_readahead, &rq->state);
            }else {
                spin_lock(&ra_rq->lock);
                if(atomic_dec_and_test(&ra_rq->refcount))
                    free_kv_async_rq(ra_rq);
                else
                    spin_unlock(&ra_rq->lock);
            }
        }
        /*if(is_kv_store_cmd(rq->c.common.opcode)){
           for Save After Store
            ra_rq = kv_hash_insert_readahead(&rq->iosched->kv_hash[KV_RA_HASH], rq);
            if(!ra_rq){
                set_bit(KV_async_in_readahead, &rq->state);
            }
        }*/
        clear_bit(KV_async_inFlight_rq, &rq->state);
        /* Wake up sync IO threads having same key */
        wake_up_bit(&rq->state, KV_async_inFlight_rq);

        /*For "flush" command*/
        if(test_and_clear_bit(KV_async_flush_wakeup, &rq->state)){
            //    struct completion *waiting = rq->end_io_data;
#ifndef NO_LOG
            atomic64_inc(&kv_flush_wakeup_by_completion);
#endif

            //    rq->end_io_data = NULL;

            /*
             * complete last, if this is a stack request the process (and thus
             * the rq pointer) could be invalid right after this complete()
             */
            //complete(waiting);
            wake_up_bit(&rq->state, KV_async_flush_wakeup);
        }
        if(test_bit(KV_async_blocking_rq, &rq->state)){
            /*
             * If this rq has been blocking a request, unblocking the request at here.
             *   - Clear pending bit
             *   - decrease nr_pending of the Memory Queue where the pending request exists
             */
            struct kv_async_request *pend_rq;
            clear_bit(KV_async_blocking_rq, &rq->state);
            pend_rq= kv_hash_find_and_lock_pending_rq(&rq->iosched->kv_hash[KV_RQ_HASH], rq->key_addr, rq->key_length, rq->hash_key);
            /*
             * pend_rq can not be NULL, even if pend_rq becomes invalid.
             * blocking bit should be cleared when the pend_rq is invalidated
             */
            if(pend_rq){ 
                if(pend_rq->blocking_rq){
                    pend_rq->blocking_rq = NULL;
                    atomic_dec(&pend_rq->memq->nr_pending); 
                }
                if(atomic_dec_and_test(&pend_rq->refcount)){
                    pr_err("[%d:%s]ERROR : the refcount of the pending RQ can not be 0(0x%lx)[key : %s(hash : 0x%x)]\n", __LINE__, __func__, pend_rq->state, (char*)pend_rq->key_addr, pend_rq->hash_key);
                    free_kv_async_rq(pend_rq);
                }else
                    spin_unlock(&pend_rq->lock);
            }else
                pr_err("[%d:%s]ERROR : pend_rq must exists But it does not\n", __LINE__, __func__);
        }
        if(rq->req)
            blk_mq_free_request(rq->req);
        rq->req = NULL;
        /*For kv_iosched_destroy() */
        if(!test_bit(KV_iosched_registered, &rq->iosched->state) && !atomic_read(&rq->iosched->in_flight_rq))
            wake_up_atomic_t(&rq->iosched->in_flight_rq);

        if(test_bit(KV_async_in_readahead, &rq->state)){
            spin_unlock(&rq->lock);
        }else if(atomic_dec_and_test(&rq->refcount)){
            free_kv_async_rq(rq);
        }else /* A request may access this request */{
            spin_unlock(&rq->lock);
        }
#ifndef NO_LOG
        atomic64_inc(&kv_total_async_complete);
#endif
        atomic64_inc(&kv_iosched->nr_async_complete);
    }
    if(atomic64_read(&kv_iosched->nr_async_interrupt) != atomic64_read(&kv_iosched->nr_async_complete)){
#ifndef NO_LOG
        atomic64_inc(&kv_completion_retry);
#endif
        atomic_inc(&kv_iosched->nr_completion_retry);
        goto restart;
    }
    if(test_and_clear_bit(KV_iosched_need_to_wake_read_submit_daemon, &kv_iosched->state))
        queue_work(kv_iosched->submit_daemon[READ_DAEMON].submit_wq, &kv_iosched->submit_daemon[READ_DAEMON].submit_work);
    if(test_and_clear_bit(KV_iosched_need_to_wake_write_submit_daemon, &kv_iosched->state))
        queue_work(kv_iosched->submit_daemon[WRITE_DAEMON].submit_wq, &kv_iosched->submit_daemon[WRITE_DAEMON].submit_work);
    if(test_and_clear_bit(KV_iosched_need_to_wake_multi_read_submit_daemon, &kv_iosched->state))
        queue_work(kv_iosched->submit_daemon[BATCH_READ_DAEMON].submit_wq, &kv_iosched->submit_daemon[BATCH_READ_DAEMON].submit_work);
    if(test_and_clear_bit(KV_iosched_need_to_wake_multi_write_submit_daemon, &kv_iosched->state))
        queue_work(kv_iosched->submit_daemon[BATCH_WRITE_DAEMON].submit_wq, &kv_iosched->submit_daemon[BATCH_WRITE_DAEMON].submit_work);

#ifndef NO_LOG
    if(!has_it_been_run)
    {
        atomic64_inc(&kv_completion_rq_empty);
    }
#endif
    return;
}



///////////////////////////// Wakeup/flush  scheduling daemon /////////////////////////////////
/*
 * Flush the pending requests
 */
void kv_sched_flush_workqueue(struct kv_iosched_struct *kv_iosched, int type){
    int cpu;
    for_each_possible_cpu(cpu){
        struct kv_dwork_struct *schedd = per_cpu_ptr(kv_iosched->percpu_schedd[type], cpu); 
        flush_delayed_work(&schedd->dwork);
    }
}

#if 0
/*
 * delayed_work_pending - Find out whether a delayable work item is currently
 * pending
 */
int kv_iosched_io_pendig(struct kv_iosched_struct *kv_iosched)
{
    return delayed_work_pending(&kv_iosched->dwork);
}
#endif

/*
 * Delay for workqueue in msec
 * This interval value can be used for congestion control.
 */
bool kv_sched_daemon_busy(int cpu, struct kv_iosched_struct *kv_iosched, int request_type)
{
    struct kv_dwork_struct *schedd = per_cpu_ptr(kv_iosched->percpu_schedd[request_type], cpu); 
    return work_busy(&schedd->dwork.work);
}
bool kv_sched_wakeup_delayed(int cpu, struct kv_iosched_struct *kv_iosched, int request_type, unsigned int msec)
{
    unsigned long timeout;
    bool ret=false;
    struct kv_dwork_struct *schedd = per_cpu_ptr(kv_iosched->percpu_schedd[request_type], cpu); 

    timeout = msecs_to_jiffies(msec);
    if (test_bit(KV_iosched_registered, &kv_iosched->state))
        ret = queue_delayed_work_on(cpu, schedd->wq, &schedd->dwork, timeout);
    return ret;
}

void kv_sched_wakeup_immediately(int cpu, struct kv_iosched_struct *kv_iosched, int request_type)
{
    struct kv_dwork_struct *schedd = per_cpu_ptr(kv_iosched->percpu_schedd[request_type], cpu); 
    if (test_bit(KV_iosched_registered, &kv_iosched->state))
        mod_delayed_work_on(cpu, schedd->wq, &schedd->dwork, 0);
}

void kv_sched_cancel_work(int cpu, struct kv_iosched_struct *kv_iosched, int request_type)
{
    struct kv_dwork_struct *schedd = per_cpu_ptr(kv_iosched->percpu_schedd[request_type], cpu); 
    if (test_bit(KV_iosched_registered, &kv_iosched->state))
        cancel_delayed_work(&schedd->dwork);
}

/////////////////////////////////////////////////////////////////////////////////////////
/*
 * Workqueue initialization
 * - sched_wq moves a Memory Queue from active list to mutable list, 
 *   and wake submission daemon up.
 * - submit_wq submits request from mutable Memory Queue 
 *   completed requests in the submitted Memory Queue.
 */
static int __init init_kv_workqueue(struct kv_iosched_struct *kv_iosched)
{
    int cpu;
    int i;

    for( i = ACTIVE_READ; i < NR_ACTIVE_MEMQ_TYPE; i++){
        /* Per CPU Scheduling Daemon */
        char buf[20];
        kv_iosched->percpu_schedd[i] = alloc_percpu(struct kv_dwork_struct);
        for_each_possible_cpu(cpu){
            struct kv_dwork_struct *schedd = per_cpu_ptr(kv_iosched->percpu_schedd[i], cpu); 
            sprintf(buf, "kv_sched_%s%d\n", i==READ_DAEMON ? "read_":"write", cpu);
            schedd->wq = create_workqueue(buf);
            if (!schedd->wq) return -ENOMEM;
            INIT_DELAYED_WORK(&schedd->dwork, kv_scheduling_workfn);
            schedd->addr = (void *)kv_iosched;
            schedd->type = i;
        }
    }

    for( i = READ_DAEMON ; i < NR_DAEMON  ; i++){
        /* Per CPU Scheduling Daemon */
        struct kv_work_struct *submitd = &kv_iosched->submit_daemon[i]; 
        char buf[20];
        /* Submission Daemon */
        switch(i){
            case READ_DAEMON:
                sprintf(buf, "kv_single_put");
                break;
            case WRITE_DAEMON:
                sprintf(buf, "kv_single_get");
                break;
            case BATCH_READ_DAEMON:
                sprintf(buf, "kv_multi__put");
                break;
            case BATCH_WRITE_DAEMON:
                sprintf(buf, "kv_multi__get");
                break;
            default:
                sprintf(buf, "none??");
                break;
        }
        submitd->submit_wq = create_workqueue(buf);
        if (!submitd->submit_wq) return -ENOMEM;
        INIT_WORK(&submitd->submit_work, kv_submission_workfn);
        submitd->addr = (void *)kv_iosched;
        submitd->type = i;
        INIT_LIST_HEAD(&submitd->mumemq_list); /* Head of Mutable Memory Queue list */
        spin_lock_init(&submitd->mumemq_lock); //for Mutable Memory Queue list
        spin_lock_init(&submitd->work_lock); // for submission workqueue lock in scheduling daemon context
    }

    /*Completion Daemon*/
    kv_iosched->complete_wq = create_workqueue("kv_finisher");
    if (!kv_iosched->complete_wq) return -ENOMEM;
#ifdef DELAYED_COMPLETE_WORK
    INIT_DELAYED_WORK(&kv_iosched->complete_dwork, kv_completion_workfn);
#define COMPLETE_DELAY 1
#define COMPLETE_MAX_RQ 256
    kv_iosched->complete_delay = msecs_to_jiffies(COMPLETE_DELAY);
    kv_iosched->complete_max_rq= COMPLETE_MAX_RQ;
#else
    INIT_WORK(&kv_iosched->complete_work, kv_completion_workfn);
#endif

    return 0;
}

/*
 * Initialize "struct kv_iosched_struct"
 *  - Initialize member variables
 *  - Create an iosched io delayed workqueue thread
 *  - the "node" is device's node.
 */
inline void *init_kv_cachep(char *name, int size, void (*ctor)(void *)){
    return kmem_cache_create(name, size, 0, 
            (SLAB_RECLAIM_ACCOUNT|SLAB_MEM_SPREAD),
            ctor);

}

int __init kv_core_init(void)
{
#ifndef NO_LOG
    int i;
#endif
    int err = 0;

    /*
     * SLAB Initialization
     * kv_memqueue_struct, kv_async_request, and kv_sync_request
     */
    kv_memqueue_struct_cachep = init_kv_cachep("kv_memqueue_struct", sizeof(struct kv_memqueue_struct), init_memq_once);
    kv_async_request_cachep = init_kv_cachep("kv_async_request", sizeof(struct kv_async_request), init_async_once);
    kv_sync_request_cachep = init_kv_cachep("kv_sync_request", sizeof(struct kv_sync_request), init_sync_once);
    if (!kv_memqueue_struct_cachep || !kv_async_request_cachep || !kv_sync_request_cachep){
        err = -ENOMEM;
        goto out;
    }
#ifdef KV_NVME_TRACE
    pr_err("[%d:%s] memQ size : %lu || async_rq size : %lu || sync_rq size : %lu\n", __LINE__, __func__, sizeof(struct kv_memqueue_struct), sizeof(struct kv_async_request), sizeof(struct kv_sync_request));
#endif

#ifndef NO_LOG
    atomic64_set(&kv_total_async_request, 0);
    atomic64_set(&kv_total_get_async_merge, 0);
    atomic64_set(&kv_total_put_del_async_merge, 0);
    atomic64_set(&kv_total_async_submit, 0);
    atomic64_set(&kv_total_sync_request, 0);
    atomic64_set(&kv_total_sync_submit, 0);
    atomic64_set(&kv_total_blocked_submit, 0);
    atomic64_set(&kv_total_async_complete, 0);
    atomic64_set(&kv_total_ra_hit, 0);
    atomic64_set(&kv_total_nonblocking_read_fail, 0);
    atomic64_set(&kv_total_ra_hit_in_flight, 0);
    atomic64_set(&kv_total_canceled_ra, 0);
    atomic64_set(&kv_total_discard_ra, 0);
    atomic64_set(&kv_total_discard_not_found_ra, 0);
    atomic64_set(&kv_total_discard_cancel_not_on_device, 0);
    atomic64_set(&kv_total_discard_on_device,0);
    atomic64_set(&kv_total_sync_complete, 0);
    atomic64_set(&kv_total_sync_finished, 0);
    atomic64_set(&kv_total_async_interrupt, 0);
    atomic64_set(&kv_completion_retry, 0);
    atomic64_set(&kv_completion_rq_empty, 0);
    atomic64_set(&kv_completion_wakeup_by_int, 0);
    atomic64_set(&kv_completion_wakeup_by_submit, 0);
    atomic64_set(&kv_flush_wakeup_by_submit, 0);
    atomic64_set(&kv_flush_wakeup_by_completion, 0);
    for(i = kv_async_success; i <= kv_async_unknownError; i++){
        atomic64_set(&kv_async_error[i], 0);
    }
    for(i = KV_flush_begin ; i < KV_flush_nr ; i++){
        atomic64_set(&kv_flush_statistics[i], 0);
    }
    for(i = KV_hash_com_insert; i < KV_hash_restart_nr; i++){
        atomic64_set(&kv_hash_restart[i], 0);
    }
    for(i = READ_DAEMON; i < NR_DAEMON; i++)
        sprintf(kvct_submission[i], "init");
    for(i = 0 ; i < NR_KV_SSD; i++)
        sprintf(kvct_completion[i], "init");
    sprintf(kvct_int_handle, "init");
    for(i = 0 ; i < 8 ;i++){
        sprintf(kvct_ioctl[i], "init");
        sprintf(kvct_sync_io[i], "init");
        sprintf(kvct_discard_readahead[i], "init");
        sprintf(kvct_flush[i], "init");
    }
#endif
out:
    return err;
}


void kv_core_exit(void)
{
    kmem_cache_destroy(kv_memqueue_struct_cachep);
    kmem_cache_destroy(kv_async_request_cachep);
    kmem_cache_destroy(kv_sync_request_cachep);
}

int kv_iosched_init(struct kv_iosched_struct *kv_iosched, int node)
{
    int err = 0;
    int i, cpu;

    /*
     * Create workqueues & initialize the works
     *  - Scheduling daemons(as many as number of cpus)
     *  - A submission daemon
     *  - A Completion daemon
     */
    err = init_kv_workqueue(kv_iosched);
    if(err) return err;
    spin_lock_init(&kv_iosched->complete_lock); // for completion workqueue lock
    init_kv_fifo(&kv_iosched->complete_fifo);


    /*
     * Allocate & initialize memory for active & reserved Memory Queue pointer
     */
    for( i = ACTIVE_READ ; i < NR_MEMQ_TYPE ; i++){
        kv_iosched->percpu_memq[i] = alloc_percpu(struct kv_memqueue_struct *);
        if (!kv_iosched->percpu_memq[i]) {
            err = -ENOMEM;
            goto out;
        }

        for_each_possible_cpu(cpu){
            *per_cpu_ptr(kv_iosched->percpu_memq[i], cpu) = kv_memq_alloc();
            if (!*per_cpu_ptr(kv_iosched->percpu_memq[i], cpu)){
                err = -ENOMEM;
                goto out;
            }
        }
    }

    /*
     * Initialize Mutable Memory Queue list
     */

    for(i = 0 ; i < NR_KV_HASH; i++)
        kv_hash_init(&kv_iosched->kv_hash[i]);
    spin_lock_init(&kv_iosched->ra_lock); //for read-ahead lru list
    INIT_LIST_HEAD(&kv_iosched->ra_list); /* Head of read-ahead lru list*/
    atomic_set(&kv_iosched->nr_congested, 0);
    atomic_set(&kv_iosched->in_flight_rq, 0);
    atomic_set(&kv_iosched->nr_flush_rq, 0);
    atomic64_set(&kv_iosched->nr_async_interrupt, 0);
    atomic64_set(&kv_iosched->nr_async_complete, 0);
    atomic_set(&kv_iosched->nr_completion_retry, 0);
    kv_iosched->max_nr_req_per_memq = DEFAULT_NR_REQ_PER_MEMQ; 
    set_bit(KV_iosched_registered, &kv_iosched->state);
#ifdef KV_NVME_TRACE
    pr_err("[%d:%s]Finish kv_iosched_struct initialization\n", __LINE__, __func__);
#endif

out:
    return err;
}

/*
 * Destroy "struct kv_iosched_struct"
 *  - Flush remained works
 *  - Destroy workqueue(stop kthread)
 */
void kv_iosched_destroy(struct kv_iosched_struct *kv_iosched)
{
    int i, cpu;
    struct kv_hash *ra_hash = &kv_iosched->kv_hash[KV_RA_HASH];
    struct list_head *ra_data;
    struct list_head *next_ele;

    /* Make sure nobody queues further work */
    //spin_lock_bh(&kv_iosched->work_lock);
    if (!test_and_clear_bit(KV_iosched_registered, &kv_iosched->state)) {
    //    spin_unlock_bh(&kv_iosched->work_lock);
        return;
    }
    //spin_unlock_bh(&kv_iosched->work_lock);
    /*
     * Clean-up active Memory Queue
     * Flush & destroy scheduling workqueues
     */
    for( i = ACTIVE_READ ; i < NR_ACTIVE_MEMQ_TYPE; i++){
        for_each_possible_cpu(cpu){
            struct kv_dwork_struct *schedd = per_cpu_ptr(kv_iosched->percpu_schedd[i], cpu); 
            flush_delayed_work(&schedd->dwork);// flush remained works(submission buffer);
            destroy_workqueue(schedd->wq);

        }
    }

    for( i = ACTIVE_READ ; i < NR_MEMQ_TYPE/*NR_ACTIVE_MEMQ_TYPE*/; i++){
        struct kv_memqueue_struct *memq;
        for_each_possible_cpu(cpu){
            memq = *per_cpu_ptr(kv_iosched->percpu_memq[i], cpu);
            if(memq && atomic_read(&memq->nr_rq))/* Reserved memq's nr_rq must be 0 */ 
                list_add_tail(&memq->memq_lru, &kv_iosched->submit_daemon[i].mumemq_list);
            else if(memq)
                kv_memq_free(memq);
        }
    }
    for( i = ACTIVE_READ; i < NR_ACTIVE_MEMQ_TYPE; i++)
        /* Per CPU Scheduling Daemon */
        free_percpu(kv_iosched->percpu_schedd[i]);
    for( i = ACTIVE_READ ; i < NR_MEMQ_TYPE/*NR_ACTIVE_MEMQ_TYPE*/; i++)
        free_percpu(kv_iosched->percpu_memq[i]);

    /*
     * Flush & destroy submission workqueue
     * The submit daemon will submit all remained requests.
     * If Pending request exists, submit daemon should be wait 
     * until interrupt handler makes the bit clear.
     * It will be automatically processed by the mechanism.
     */
    for( i = READ_DAEMON ; i < NR_DAEMON  ; i++){
        struct kv_work_struct *submitd = &kv_iosched->submit_daemon[i]; 
        queue_work(submitd->submit_wq, &submitd->submit_work);
        flush_work(&submitd->submit_work);// flush remained works(submission buffer)
        destroy_workqueue(submitd->submit_wq);
    }

    /*
     * Wait all in-flight request are completed.
     */
    wait_on_atomic_t(&kv_iosched->in_flight_rq, kv_wait_atomic_killable, TASK_KILLABLE);

#ifdef DELAYED_COMPLETE_WORK
    queue_delayed_work(kv_iosched->complete_wq, &kv_iosched->complete_dwork, 0);
    flush_delayed_work(&kv_iosched->complete_dwork);// flush remained works(submission buffer)
#else
    queue_work(kv_iosched->complete_wq, &kv_iosched->complete_work);
    flush_work(&kv_iosched->complete_work);// flush remained works(submission buffer)
#endif
    destroy_workqueue(kv_iosched->complete_wq);

    destroy_kv_fifo(&kv_iosched->complete_fifo);


    /*
     * Clean up the read-ahead hash.
     */
    list_for_each_safe(ra_data, next_ele, &kv_iosched->ra_list) 
    {
        struct kv_async_request *rq = list_entry(ra_data, struct kv_async_request, lru);

        kv_hash_delete(ra_hash, rq);
        spin_lock(&kv_iosched->ra_lock);
        list_del_init(&rq->lru);
        spin_unlock(&kv_iosched->ra_lock);
        spin_lock(&rq->lock);
        if(atomic_dec_and_test(&rq->refcount))
            free_kv_async_rq(rq);
        else{/* A request may access this request */
            pr_err("[%d:%s]Fail to free a request because the refcount is not 0\n", __LINE__, __func__);
            spin_unlock(&rq->lock);
        }
    }

    kfree(kv_iosched);
    return;
}
