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
#ifndef _KV_IOSCHED_H
#define _KV_IOSCHED_H
#include <linux/version.h>
#include <linux/blkdev.h>
#include <linux/spinlock.h>
#include <linux/types.h>
#include <linux/freezer.h>

#if LINUX_VERSION_CODE >= KERNEL_VERSION(4,15,0)
#include <linux/sched/signal.h>
#endif

#include "kv_fifo.h"
#include "nvme.h"
#include "linux_nvme_ioctl.h"
#define KV_HASH_BITS	17
#define KV_HASH_SIZE	(1UL << KV_HASH_BITS)

#define KV_KEY_SUCCESS 0
#define KV_KEY_EXIST 1

#define DELAYED_COMPLETE_WORK

enum memq_type{
    ACTIVE_READ = 0,
    ACTIVE_WRITE = 1,
    NR_ACTIVE_MEMQ_TYPE = 2,
    GET_RESERVED = 2,
    RESERVED_READ = 2,
    RESERVED_WRITE = 3,
    NR_MEMQ_TYPE = 4,
};

enum sched_daemon_type{
    READ_DAEMON= ACTIVE_READ,
    WRITE_DAEMON= ACTIVE_WRITE,
    BATCH_READ_DAEMON = READ_DAEMON + 2,
    BATCH_WRITE_DAEMON= WRITE_DAEMON + 2,
    NR_DAEMON = BATCH_WRITE_DAEMON + 1,
};
//#define NO_LOG
#ifndef NO_LOG
enum{
    KV_flush_begin = 0,
    KV_flush_single_nokey_found = 1,
    KV_flush_single = 2,
    KV_flush_all = 3,
    KV_flush_done= 4,
    KV_flush_all_cancel= 5,
    KV_flush_single_cancel= 6,
    KV_flush_end = 7,
    KV_flush_wait_time = 8,
    KV_flush_entire_time = 9,
    KV_flush_nr = 10,
};
enum{
    KV_hash_com_insert= 0,
    KV_hash_inv_insert= 1,
    KV_hash_blk_insert= 2,
    KV_hash_com_sync= 3,
    KV_hash_inv_sync= 4,
    KV_hash_blk_sync= 5,
    KV_hash_restart_nr= 6,
};

extern atomic64_t kv_total_async_request;
extern atomic64_t kv_total_get_async_merge;
extern atomic64_t kv_total_put_del_async_merge;
extern atomic64_t kv_total_async_submit;
extern atomic64_t kv_total_sync_request;
extern atomic64_t kv_total_sync_submit;
extern atomic64_t kv_total_blocked_submit;
extern atomic64_t kv_total_async_interrupt;
extern atomic64_t kv_total_async_complete;
extern atomic64_t kv_total_ra_hit;
extern atomic64_t kv_total_nonblocking_read_fail;

extern atomic64_t kv_total_ra_hit_in_flight;
extern atomic64_t kv_total_canceled_ra;
extern atomic64_t kv_total_discard_ra;
extern atomic64_t kv_total_discard_not_found_ra;
extern atomic64_t kv_total_discard_cancel_not_on_device;
extern atomic64_t kv_total_discard_on_device;
extern atomic64_t kv_total_sync_complete;
extern atomic64_t kv_total_sync_finished;
extern atomic64_t kv_completion_retry;
extern atomic64_t kv_completion_rq_empty;
extern atomic64_t kv_completion_wakeup;
extern atomic64_t kv_completion_wakeup_by_submit;
extern atomic64_t kv_completion_wakeup_by_int;
extern atomic64_t kv_flush_wakeup_by_submit;
extern atomic64_t kv_flush_wakeup_by_completion;
extern atomic64_t kv_flush_statistics[KV_flush_nr];
extern atomic64_t kv_hash_restart[KV_hash_restart_nr];
extern atomic64_t kv_async_get_req;
extern atomic64_t kv_async_put_req;
extern atomic64_t kv_async_del_req;
extern atomic64_t kv_sync_get_req;
extern atomic64_t kv_sync_get_merge_with_async_put;
extern atomic64_t kv_sync_get_merge_with_async_del;
extern atomic64_t kv_sync_put_req;
extern atomic64_t kv_sync_del_req;
extern atomic64_t kv_sync_iter_req;
extern atomic64_t kv_sync_iter_read_req;
extern atomic64_t kv_sync_get_log;

enum{
    kv_async_success= 0,
    kv_async_invalidValueSize = 1,
    kv_async_invalidKeySize  = 2,
    kv_async_misiAlignment= 3,
    kv_async_unRecovered= 4,
    kv_async_exceedDeviceCap= 5,
    kv_async_idempotentError= 6,
    kv_async_unknownError=7, 
};

extern atomic64_t kv_async_error[8];

extern char kvct_sync_io[8][255];
extern char kvct_ioctl[8][255];
extern char kvct_submission[NR_DAEMON][255];
extern char kvct_int_handle[255];
extern char kvct_completion[NR_KV_SSD][255];
extern char kvct_discard_readahead[8][255];
extern char kvct_flush[8][255];
#endif


/*
 * Hash 0(KV_RQ_HASH)     : Request HASH
 * Hash 1(KV_RA_HASH)     : Read-ahead HASH
 */
enum{
    KV_RQ_HASH		= 0,
    KV_RA_HASH      = 1,
    NR_KV_HASH		= 2,
};

/*
 * Hash table
 */
struct kv_hash{
    struct hlist_head hashtable[KV_HASH_SIZE];
    spinlock_t hash_lock[KV_HASH_SIZE];
    /*
     * Number of entries in this hashtable
     *  - Decide size of hash table ; if entries are too many, the size should be increased.
     *  - Tracking number of read-ahead data for page reclaiming
     * TODO
     * nr_rq may not be neccessary
     */
    atomic_t nr_rq; 
    /*
     * Only buffer Q can be congested
     * because the submission Q is immutable 
     * which means do not take requests anymore
     * So, we move congesion indicator to kv_iosched_struct->state:KV_iosched_congested
     *
     * We also move number of congestion to kv_iosched_struct.
     */
#if 0
    atomic_t congestion;
    atomic_t nr_congested;
#endif
};


enum kv_iosched_state {
	KV_iosched_registered,		/* kv_iosched_init() was done */
	KV_iosched_congested,	/* The limit number of in-flight request has been reached */
	KV_iosched_queue_full,	/* The limit number of in-flight request has been reached */
	KV_iosched_flush,	/* The limit number of in-flight request has been reached */
	KV_iosched_need_to_wake_read_submit_daemon,	/* The limit number of in-flight request has been reached */
	KV_iosched_need_to_wake_write_submit_daemon,	/* The limit number of in-flight request has been reached */
	KV_iosched_need_to_wake_multi_read_submit_daemon,	/* The limit number of in-flight request has been reached */
	KV_iosched_need_to_wake_multi_write_submit_daemon,	/* The limit number of in-flight request has been reached */
};

enum kv_async_rq_state{
    KV_async_invalid_rq = 0, 
    KV_async_blocking_rq = 1,
    KV_async_inFlight_rq = 2,
    KV_async_SaS = 3, //Save after Store : (move to readahead hash)
    KV_async_flush_wakeup = 4, //Wake the flush daemon which is waiting for this rq when this rq is completed.
    KV_async_waiting_submission = 5, //Wake the flush daemon which is waiting for this rq when this rq is completed.
    KV_async_in_readahead = 6, //The resquest has been inserted to readahead hash
    KV_async_completed= 7, // The request has been completed.
#if 0
    KV_async_inUse_rq = 3, 
    KV_async_read_rq = 1, 
    KV_async_write_rq = 2, 
#endif
};

#if 1
/*
 * This is for 16 bytes embed key of PHX
 */
typedef struct {
	u64 m_low;
	u64 m_high;
} uint128_t;
////////////// code for 16 byte embedded  key /////////////////
/*
 * This is for 16 bytes key in PHX
 */
static inline u64 __hash_64(u64 val)
{
    return val * GOLDEN_RATIO_64;
}
static __always_inline u64 hash_128(uint128_t val, unsigned int bits){
    /* Hash 128 bits using only 64x64-bit multiply. */
    return hash_64((u64)val.m_low ^ __hash_64(val.m_high), bits);
}

extern inline u32 get_hash_key(void *key, u32 key_length);


#endif

#define KV_SUCCESS 0

/*unit : ms*/
#define KV_ASYNC_INTERVAL 32
/*
 *
 */
#if 0
#define KV_MAX_INFLIGHT_REQUEST 128 
#elif 0
/* 
 * The device does not hang when requesting get in single threaded async mode(burst I/O)  
 */
#define KV_MAX_INFLIGHT_REQUEST 240
#elif 0
/* 
 * The device does not hang when requesting get in single threaded async mode(burst I/O)  
 */
#define KV_MAX_INFLIGHT_REQUEST 272
#elif 0
/* 
 * The device hangs when requesting get in async mode(burst I/O) 
 */
#define KV_MAX_INFLIGHT_REQUEST 384
#elif 0
/* 
 * The device hangs when requesting get in async mode(burst I/O) 
 * But no requests are blocked in this configuration
 */
#define KV_MAX_INFLIGHT_REQUEST 1024
#else
/* 
 * The device hangs when requesting get in async mode(burst I/O) 
 * But no requests are blocked in this configuration
 */
#define KV_MAX_INFLIGHT_REQUEST 512
#endif
#define DEFAULT_NR_REQ_PER_MEMQ 256

#define KV_PENDING_QUEU_LOW_WATERMARK (KV_MAX_INFLIGHT_REQUEST  << 3)
#define KV_PENDING_QUEU_HIGH_WATERMARK (KV_PENDING_QUEU_LOW_WATERMARK + (KV_MAX_INFLIGHT_REQUEST >> 1))


#if 0
#define ACTIVE_READ 0
#define ACTIVE_WRITE 1
#define RESERVED_READ 2
#define RESERVED_WRITE 3
#define NR_MEMQ_TYPE 4
#endif

//#define KV_TIME_MEASUREMENT

/*
 * Memory Queue structure
 * * Active MemQueue
 *  - Buffering new requests 
 *  - Allow to modify existing requests
 * * Mutable MemQueue
 *  - Does not receive new requests 
 *  - Allow to modify existing requests
 *  - Waiting for submission 
 *
 * 
 */
struct kv_memqueue_struct{
    struct list_head memq_lru; /* linked list for mutable Memory Queue list */
    struct list_head rq; /* linked list for user requests */
    atomic_t nr_pending;
    atomic_t nr_rq; /* Number of entries in the Memory Queue list*/
    /*
     * How many sync operations are wait for completion of the Memory Queue.
     * If sync_wait is not 0, completion daemon do not move forward.
     *  : Do not clear pending status
     * It means that sync operations are waiting for this Memory Queue 
     * because they have same key with a request in the Memory Queue.
     * The daemon should be wait the sync_wait to be 0.
     */
    /* 
     * Blocking key 
     * It is used for batch async request which need to wait a key for submission.
     * */
    unsigned long flags;
    union {
        struct {
            __u64 key_low;
            __u64 key_high;
        };
        __u8 blocking_key[16];
    };
}__packed __aligned(64);

struct kv_async_request{
    ////////////// Set in user context to insert to hash list //////////
    struct	request_queue *queue;
	//__u8			opcode;
    struct	nvme_command c;
    unsigned	timeout;
    unsigned long state;

    void	*data_addr;
    __u32	data_length;
    /*
     * If the data size is larger than 
     * KVCMD_INLINE_KEY_MAX(PHX = 16 bytes), 
     * metadata stores base address of the data and set metadata_len. 
     * Otherwise, the metadata is saved in c.common.metadata. 
     * In this case, the metadata_len is set to 0.
     */
    void	*key_addr; 
    __u32	key_length;
    ////////////// end  //////////

    ////////////// set in flush daemon before/after completion  //////////
    /*
     * Keep the "struct request *" in order to free the structure
     */
    struct request *req;

    struct nvme_completion cqe;

    /*
     * result = le32_to_cpu(cqe.result)
     */
    __u32	result; 
    /*
     * struct request *req;
     * ret = req->errors;
     */
    int errors;
    /*
     * Point to the request blocking this request.
     */
    struct kv_async_request *blocking_rq;
    spinlock_t lock;		/* protects data */

    //for re-insertion	:  the same key already exist in one of Qs
    //for migration	: submission => readahead q
    u32 hash_key;

    /*
     * For add it to hash list(Submission or Read-Hhead hash)
     */
    struct hlist_node hash;
    /*
     * For add it to memq_struct
     * lru in readahead queue
     */
    struct list_head lru;
    struct kv_memqueue_struct *memq;
	atomic_t refcount;
    struct kv_iosched_struct *iosched;
	void *end_io_data;
};

struct multicmd_completion {
    struct completion *comp;
    atomic_t pending_count;
};

struct kv_sync_request{
    struct request_queue *q;
    struct nvme_command c;
    void __user *udata_addr;   // from User
    unsigned udata_length;
    void *key_addr;
    unsigned key_length;
    u32 result;
    unsigned timeout;
    /* sync multicommand support */
    struct request *req;
    struct bio *bio;
    struct kv_sync_request *next;
    struct nvme_passthru_kv_cmd __user *ucmd;
    struct nvme_completion cqe;
};

struct kv_dwork_struct{
    struct workqueue_struct *wq;
    struct delayed_work dwork;
    void *addr;
    int type; /*Read(0) or Write(1)*/
};

struct kv_work_struct{
    struct workqueue_struct *submit_wq;
    spinlock_t work_lock;/* protects work_list & dwork scheduling */
    struct work_struct submit_work;
    void *addr;
    int type; /*Read(0) or Write(1)*/
    struct list_head mumemq_list; /* Mutable Memory Queue list */
    spinlock_t mumemq_lock;
};

struct kv_iosched_struct{

    /* 
     * Scheduling daemon.
     * Create the daemons as many as the # of CPUs
     * We cannot use "container_of" macro for __percpu data structure.
     * That's why we define "kv_dwork_struct"
     */
    struct kv_dwork_struct __percpu *percpu_schedd[NR_DAEMON];
    /*
     * Submission & compleation daemons
     * These daemons are not delayed work queue, scheduled immediately
     */
    struct kv_work_struct submit_daemon[NR_DAEMON];

    struct workqueue_struct *complete_wq;
#ifdef DELAYED_COMPLETE_WORK
    struct delayed_work complete_dwork;
    unsigned long complete_delay;
    unsigned int complete_max_rq;
#else
    struct work_struct complete_work;
#endif
    spinlock_t complete_lock;/* protects work_list & dwork scheduling */
    struct kv_fifo complete_fifo;

    /*
     * Per CPU active list
     */
    /* active_read_memq always high priority over active_write_memq. */
    struct kv_memqueue_struct **__percpu percpu_memq[NR_MEMQ_TYPE];
    int max_nr_req_per_memq;



    /* Submission & read-ahead hash tables */
    struct kv_hash kv_hash[NR_KV_HASH];
    /* 
     * This lru is only used in read-ahead hash.
     */
    spinlock_t ra_lock; /* read ahead list locks */
    struct list_head ra_list; /* read ahead list for reclaim */

    atomic_t nr_congested;
    atomic_t in_flight_rq;
    atomic_t nr_flush_rq;
    atomic64_t nr_async_interrupt;
    atomic64_t nr_async_complete;
    atomic_t nr_completion_retry;
    unsigned long state;	/* workqueue & congestion status. */
    int kv_ssd_id;
};//kv_iosched_struct

/*
 * Workqueue init/destroy function
 */
int __init kv_core_init(void);
void kv_core_exit(void);
extern int kv_iosched_init(struct kv_iosched_struct *, int);
extern void kv_iosched_destroy(struct kv_iosched_struct *);

/*
 * Workqueue related functions
 */
extern void kv_sched_flush_workqueue(struct kv_iosched_struct *kv_iosched, int type);
extern bool kv_sched_daemon_busy(int cpu, struct kv_iosched_struct *kv_iosched, int request_type);
extern bool kv_sched_wakeup_delayed(int cpu, struct kv_iosched_struct *kv_iosched, int request_type, unsigned int msec);
extern void kv_sched_wakeup_immediately(int cpu, struct kv_iosched_struct *kv_iosched, int request_type);
extern void kv_sched_cancel_work(int cpu, struct kv_iosched_struct *kv_iosched, int request_type);


/*
 * IO Schedule core function
 */
extern int __nvme_submit_iosched_kv_cmd(struct kv_memqueue_struct *memq);

/*
 * Allocate a data structure from SLAB
 */
extern inline struct kv_async_request *kv_async_request_alloc(void);
extern inline void kv_async_request_free(struct kv_async_request *rq);
extern inline struct kv_sync_request *kv_sync_request_alloc(void);
extern inline void kv_sync_request_free(struct kv_sync_request *rq);

extern void kv_hash_init(struct kv_hash *);
extern struct kv_async_request * kv_hash_insert(struct kv_hash *, struct kv_async_request *);
extern struct kv_async_request *kv_hash_insert_readahead(struct kv_hash *hash, struct kv_async_request *rq);
extern struct kv_async_request *kv_hash_find(struct kv_hash *, void *, __u32, u32, bool);
extern struct kv_async_request *kv_hash_find_refcnt(struct kv_hash *hash, void *key, __u32 key_length, u32 hash_key);
extern bool kv_decrease_req_refcnt(struct kv_async_request *rq, bool lock);
extern int kv_hash_delete_readahead(struct kv_hash *hash, struct kv_async_request *rq);
extern int kv_hash_delete(struct kv_hash *, struct kv_async_request *);
extern struct kv_async_request *kv_hash_find_and_lock(struct kv_hash *hash, void *key, u32 key_length, u32 hash_key, bool *inFlight);
extern struct kv_async_request *kv_hash_find_read_and_invalidate(struct kv_hash *hash, void *key, u32 key_length, u32 hash_key);
extern struct kv_async_request *kv_hash_find_and_lock_pending_rq(struct kv_hash *, void *, __u32 , u32);
extern struct kv_async_request *kv_hash_find_and_del_readahead(struct kv_hash *hash, void *key, u32 key_length, u32 hash_key);

extern struct kv_memqueue_struct *kv_memq_alloc(void);
extern void kv_memq_free(struct kv_memqueue_struct *memq);

static inline void free_kv_async_rq(struct kv_async_request *rq){

    if(rq->key_length > KVCMD_INLINE_KEY_MAX && rq->key_addr){
        kfree(rq->key_addr);
        rq->key_addr = NULL;
    }
    if(rq->data_addr){
        kfree(rq->data_addr);
        rq->data_addr = NULL;
    }
    rq->key_length = 0;
    rq->data_length = 0;
    rq->hash_key = KV_HASH_SIZE;
    rq->req = NULL;
    rq->state = 0;
    rq->memq = NULL;
    rq->errors = 0;
    spin_unlock(&rq->lock);
    kv_async_request_free(rq);

}

static inline int kv_wait_killable(int mode)
{
    freezable_schedule_unsafe();
    if (signal_pending_state(mode, current))
        return -ERESTARTSYS;
    return 0;
}

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,15,0)
static inline int kv_wait_atomic_killable(atomic_t *p)
#else
static inline int kv_wait_atomic_killable(atomic_t *p, unsigned int mode)
#endif
{
    return kv_wait_killable(TASK_KILLABLE);
}

#endif
