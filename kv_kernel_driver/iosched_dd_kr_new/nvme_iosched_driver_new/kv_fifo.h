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
#ifndef __KV_FIFO_H
#define __KV_FIFO_H
#include <linux/slab.h>
#include <linux/types.h>
#include <asm/atomic.h>
/* The queue depth(NVME_Q_DEPTH) is defined at linux/drivers/nvme/host/pci.c*/
#define NVME_CQ_SHIFT   17 
#define NVME_CQ_DEPTH    (_AC(1,UL) << NVME_CQ_SHIFT)
#define NVME_CQ_MASK     (NVME_CQ_DEPTH-1)
/* Declaration of circular queue. */
struct kv_fifo{
    atomic_t head, tail;
    unsigned int order;
    /* To prevent overflow */
    unsigned int mask;
    unsigned long *ele;
    atomic_t nr_ele;
    spinlock_t lock;
};
/*Check Queue is full or not*/
static inline bool kv_fifo_isFull(struct kv_fifo * q)
{
    if(atomic_read(&q->head) == ((atomic_read(&q->tail)+1) & (q->mask)))
        return true;   
    return false;
}

/*Check Queue is empty or not*/
static inline int kv_fifo_isEmpty(struct kv_fifo * q)
{
    if(atomic_read(&q->head) == atomic_read(&q->tail))
        return true;  
    return false;
}


/*Initailization of circular queue.*/
/* The size must be a power of two */
static inline void init_kv_fifo(struct kv_fifo * q)
{
    q->ele = kzalloc(NVME_CQ_DEPTH * sizeof(void *), GFP_KERNEL);
    q->mask = NVME_CQ_MASK;
    atomic_set(&q->head, 0);
    atomic_set(&q->tail, 0);
    atomic_set(&q->nr_ele, 0);
    spin_lock_init(&q->lock);
}
static inline void destroy_kv_fifo(struct kv_fifo * q)
{
    BUG_ON(!kv_fifo_isEmpty(q));
    kfree(q->ele);
}

/*To insert item into circular queue.*/
static inline bool kv_fifo_enqueue(struct kv_fifo * q, void *addr)
{
    if( kv_fifo_isFull(q) )
        return false;

	spin_lock(&q->lock);
    q->ele[atomic_read(&q->tail)] = (unsigned long)addr;
    atomic_set(&q->tail,((atomic_read(&q->tail)+1) & q->mask));
	spin_unlock(&q->lock);
    atomic_inc(&q->nr_ele);

    return true;
}

/*To delete item from queue.*/
static inline void *kv_fifo_dequeue(struct kv_fifo * q)
{
    void *addr;
	unsigned long flags;
    if( kv_fifo_isEmpty(q) )
        return NULL;

	spin_lock_irqsave(&q->lock, flags);
    addr = (void *)q->ele[atomic_read(&q->head)];
    atomic_set(&q->head,((atomic_read(&q->head)+1) & q->mask));
	spin_unlock_irqrestore(&q->lock, flags);
    atomic_dec(&q->nr_ele);

    return addr;
}
#endif
