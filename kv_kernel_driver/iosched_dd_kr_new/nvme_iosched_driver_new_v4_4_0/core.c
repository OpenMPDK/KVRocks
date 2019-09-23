/*
 * NVM Express device driver
 * Copyright (c) 2011-2014, Intel Corporation.
 *
 * Modified by Heekwon Park from Samsung Electronics.
 * See KV_NVME_SUPPORT to check modification.
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

#include <linux/blkdev.h>
#include <linux/blk-mq.h>
#include <linux/delay.h>
#include <linux/errno.h>
#include <linux/hdreg.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/list_sort.h>
#include <linux/slab.h>
#include <linux/types.h>
#include <linux/pr.h>
#include <linux/ptrace.h>
#ifdef KV_NVME_SUPPORT
#include "linux_nvme_ioctl.h"
#else
#include "linux/nvme_ioctl.h"
#endif
#include <linux/t10-pi.h>
#include <linux/pm_qos.h>
#include <scsi/sg.h>
#include <asm/unaligned.h>

#include "nvme.h"
#ifdef KV_NVME_SUPPORT
#include <linux/proc_fs.h>
#include "kv_iosched.h"
#else
#include <linux/nvme_ioctl.h>
#endif

#define NVME_MINORS		(1U << MINORBITS)

static int nvme_major;
module_param(nvme_major, int, 0);

static int nvme_char_major;
module_param(nvme_char_major, int, 0);

static unsigned long default_ps_max_latency_us = 100000;
module_param(default_ps_max_latency_us, ulong, 0644);
MODULE_PARM_DESC(default_ps_max_latency_us,
		 "max power saving latency for new devices; use PM QOS to change per device");

static LIST_HEAD(nvme_ctrl_list);
DEFINE_SPINLOCK(dev_list_lock);

static struct class *nvme_class;

static void nvme_free_ns(struct kref *kref)
{
	struct nvme_ns *ns = container_of(kref, struct nvme_ns, kref);

	if (ns->type == NVME_NS_LIGHTNVM)
		nvme_nvm_unregister(ns->queue, ns->disk->disk_name);

	spin_lock(&dev_list_lock);
	ns->disk->private_data = NULL;
	spin_unlock(&dev_list_lock);

	nvme_put_ctrl(ns->ctrl);
	put_disk(ns->disk);
	kfree(ns);
}

static void nvme_put_ns(struct nvme_ns *ns)
{
	kref_put(&ns->kref, nvme_free_ns);
}

static struct nvme_ns *nvme_get_ns_from_disk(struct gendisk *disk)
{
	struct nvme_ns *ns;

	spin_lock(&dev_list_lock);
	ns = disk->private_data;
	if (ns && !kref_get_unless_zero(&ns->kref))
		ns = NULL;
	spin_unlock(&dev_list_lock);

	return ns;
}

void nvme_requeue_req(struct request *req)
{
	unsigned long flags;

	blk_mq_requeue_request(req);
	spin_lock_irqsave(req->q->queue_lock, flags);
	if (!blk_queue_stopped(req->q))
		blk_mq_kick_requeue_list(req->q);
	spin_unlock_irqrestore(req->q->queue_lock, flags);
}

struct request *nvme_alloc_request(struct request_queue *q,
		struct nvme_command *cmd, unsigned int flags, int qid)
{
	struct request *req;

	if (qid == NVME_QID_ANY) {
		req = blk_mq_alloc_request(q, nvme_is_write(cmd), flags);
	} else {
		req = blk_mq_alloc_request_hctx(q, nvme_is_write(cmd), flags,
				qid ? qid - 1 : 0);
	}
	if (IS_ERR(req))
		return req;

	req->cmd_type = REQ_TYPE_DRV_PRIV;
	req->cmd_flags |= REQ_FAILFAST_DRIVER;
	req->__data_len = 0;
	req->__sector = (sector_t) -1;
	req->bio = req->biotail = NULL;

	req->cmd = (unsigned char *)cmd;
	req->cmd_len = sizeof(struct nvme_command);

	return req;
}

/*
 * Returns 0 on success.  If the result is negative, it's a Linux error code;
 * if the result is positive, it's an NVM Express status code
 */
int __nvme_submit_sync_cmd(struct request_queue *q, struct nvme_command *cmd,
		struct nvme_completion *cqe, void *buffer, unsigned bufflen,
		unsigned timeout, int qid, int at_head, int flags)
{
	struct request *req;
	int ret;

	req = nvme_alloc_request(q, cmd, flags, qid);
	if (IS_ERR(req))
		return PTR_ERR(req);

	req->timeout = timeout ? timeout : ADMIN_TIMEOUT;
	req->special = cqe;

	if (buffer && bufflen) {
		ret = blk_rq_map_kern(q, req, buffer, bufflen, GFP_KERNEL);
		if (ret)
			goto out;
	}

	blk_execute_rq(req->q, NULL, req, at_head);
	ret = req->errors;
 out:
	blk_mq_free_request(req);
	return ret;
}
EXPORT_SYMBOL_GPL(__nvme_submit_sync_cmd);

int nvme_submit_sync_cmd(struct request_queue *q, struct nvme_command *cmd,
		void *buffer, unsigned bufflen)
{
	return __nvme_submit_sync_cmd(q, cmd, NULL, buffer, bufflen, 0,
			NVME_QID_ANY, 0, 0);
}

int __nvme_submit_user_cmd(struct request_queue *q, struct nvme_command *cmd,
		void __user *ubuffer, unsigned bufflen,
		void __user *meta_buffer, unsigned meta_len, u32 meta_seed,
		u32 *result, unsigned timeout)
{
	bool write = nvme_is_write(cmd);
	struct nvme_completion cqe;
	struct nvme_ns *ns = q->queuedata;
	struct gendisk *disk = ns ? ns->disk : NULL;
	struct request *req;
	struct bio *bio = NULL;
	void *meta = NULL;
	int ret;

	req = nvme_alloc_request(q, cmd, 0, NVME_QID_ANY);
	if (IS_ERR(req))
		return PTR_ERR(req);

	req->timeout = timeout ? timeout : ADMIN_TIMEOUT;
	req->special = &cqe;

	if (ubuffer && bufflen) {
		ret = blk_rq_map_user(q, req, NULL, ubuffer, bufflen,
				GFP_KERNEL);
		if (ret)
			goto out;
		bio = req->bio;

		if (!disk)
			goto submit;
		bio->bi_bdev = bdget_disk(disk, 0);
		if (!bio->bi_bdev) {
			ret = -ENODEV;
			goto out_unmap;
		}

		if (meta_buffer) {
			struct bio_integrity_payload *bip;

			meta = kmalloc(meta_len, GFP_KERNEL);
			if (!meta) {
				ret = -ENOMEM;
				goto out_unmap;
			}

			if (write) {
				if (copy_from_user(meta, meta_buffer,
						meta_len)) {
					ret = -EFAULT;
					goto out_free_meta;
				}
			}

			bip = bio_integrity_alloc(bio, GFP_KERNEL, 1);
			if (IS_ERR(bip)) {
				ret = PTR_ERR(bip);
				goto out_free_meta;
			}

			bip->bip_iter.bi_size = meta_len;
			bip->bip_iter.bi_sector = meta_seed;

			ret = bio_integrity_add_page(bio, virt_to_page(meta),
					meta_len, offset_in_page(meta));
			if (ret != meta_len) {
				ret = -ENOMEM;
				goto out_free_meta;
			}
		}
	}
 submit:
	blk_execute_rq(req->q, disk, req, 0);
	ret = req->errors;
	if (result)
		*result = le32_to_cpu(cqe.result);
	if (meta && !ret && !write) {
		if (copy_to_user(meta_buffer, meta, meta_len))
			ret = -EFAULT;
	}
 out_free_meta:
	kfree(meta);
 out_unmap:
	if (bio) {
		if (disk && bio->bi_bdev)
			bdput(bio->bi_bdev);
		blk_rq_unmap_user(bio);
	}
 out:
	blk_mq_free_request(req);
	return ret;
}

int nvme_submit_user_cmd(struct request_queue *q, struct nvme_command *cmd,
		void __user *ubuffer, unsigned bufflen, u32 *result,
		unsigned timeout)
{
	return __nvme_submit_user_cmd(q, cmd, ubuffer, bufflen, NULL, 0, 0,
			result, timeout);
}

int nvme_identify_ctrl(struct nvme_ctrl *dev, struct nvme_id_ctrl **id)
{
	struct nvme_command c = { };
	int error;

	/* gcc-4.4.4 (at least) has issues with initializers and anon unions */
	c.identify.opcode = nvme_admin_identify;
	c.identify.cns = cpu_to_le32(1);

	*id = kmalloc(sizeof(struct nvme_id_ctrl), GFP_KERNEL);
	if (!*id)
		return -ENOMEM;

	error = nvme_submit_sync_cmd(dev->admin_q, &c, *id,
			sizeof(struct nvme_id_ctrl));
	if (error)
		kfree(*id);
	return error;
}

static int nvme_identify_ns_list(struct nvme_ctrl *dev, unsigned nsid, __le32 *ns_list)
{
	struct nvme_command c = { };

	c.identify.opcode = nvme_admin_identify;
	c.identify.cns = cpu_to_le32(2);
	c.identify.nsid = cpu_to_le32(nsid);
	return nvme_submit_sync_cmd(dev->admin_q, &c, ns_list, 0x1000);
}

int nvme_identify_ns(struct nvme_ctrl *dev, unsigned nsid,
		struct nvme_id_ns **id)
{
	struct nvme_command c = { };
	int error;

	/* gcc-4.4.4 (at least) has issues with initializers and anon unions */
	c.identify.opcode = nvme_admin_identify,
	c.identify.nsid = cpu_to_le32(nsid),

	*id = kmalloc(sizeof(struct nvme_id_ns), GFP_KERNEL);
	if (!*id)
		return -ENOMEM;

	error = nvme_submit_sync_cmd(dev->admin_q, &c, *id,
			sizeof(struct nvme_id_ns));
	if (error)
		kfree(*id);
	return error;
}

int nvme_get_features(struct nvme_ctrl *dev, unsigned fid, unsigned nsid,
		      void *buffer, size_t buflen, u32 *result)
{
	struct nvme_command c;
	struct nvme_completion cqe;
	int ret;

	memset(&c, 0, sizeof(c));
	c.features.opcode = nvme_admin_get_features;
	c.features.nsid = cpu_to_le32(nsid);
	c.features.fid = cpu_to_le32(fid);

	ret = __nvme_submit_sync_cmd(dev->admin_q, &c, &cqe, buffer, buflen, 0,
			NVME_QID_ANY, 0, 0);
	if (ret >= 0 && result)
		*result = le32_to_cpu(cqe.result);
	return ret;
}

int nvme_set_features(struct nvme_ctrl *dev, unsigned fid, unsigned dword11,
		      void *buffer, size_t buflen, u32 *result)
{
	struct nvme_command c;
	struct nvme_completion cqe;
	int ret;

	memset(&c, 0, sizeof(c));
	c.features.opcode = nvme_admin_set_features;
	c.features.fid = cpu_to_le32(fid);
	c.features.dword11 = cpu_to_le32(dword11);

	ret = __nvme_submit_sync_cmd(dev->admin_q, &c, &cqe,
			buffer, buflen, 0, NVME_QID_ANY, 0, 0);
	if (ret >= 0 && result)
		*result = le32_to_cpu(cqe.result);
	return ret;
}

int nvme_get_log_page(struct nvme_ctrl *dev, struct nvme_smart_log **log)
{
	struct nvme_command c = { };
	int error;

	c.common.opcode = nvme_admin_get_log_page,
	c.common.nsid = cpu_to_le32(0xFFFFFFFF),
	c.common.cdw10[0] = cpu_to_le32(
			(((sizeof(struct nvme_smart_log) / 4) - 1) << 16) |
			 NVME_LOG_SMART),

	*log = kmalloc(sizeof(struct nvme_smart_log), GFP_KERNEL);
	if (!*log)
		return -ENOMEM;

	error = nvme_submit_sync_cmd(dev->admin_q, &c, *log,
			sizeof(struct nvme_smart_log));
	if (error)
		kfree(*log);
	return error;
}

int nvme_set_queue_count(struct nvme_ctrl *ctrl, int *count)
{
	u32 q_count = (*count - 1) | ((*count - 1) << 16);
	u32 result;
	int status, nr_io_queues;

	status = nvme_set_features(ctrl, NVME_FEAT_NUM_QUEUES, q_count, NULL, 0,
			&result);
	if (status)
		return status;

	nr_io_queues = min(result & 0xffff, result >> 16) + 1;
	*count = min(*count, nr_io_queues);
	return 0;
}

static int nvme_submit_io(struct nvme_ns *ns, struct nvme_user_io __user *uio)
{
	struct nvme_user_io io;
	struct nvme_command c;
	unsigned length, meta_len;
	void __user *metadata;

	if (copy_from_user(&io, uio, sizeof(io)))
		return -EFAULT;

	switch (io.opcode) {
	case nvme_cmd_write:
	case nvme_cmd_read:
	case nvme_cmd_compare:
		break;
	default:
		return -EINVAL;
	}

	length = (io.nblocks + 1) << ns->lba_shift;
	meta_len = (io.nblocks + 1) * ns->ms;
	metadata = (void __user *)(uintptr_t)io.metadata;

	if (ns->ext) {
		length += meta_len;
		meta_len = 0;
	} else if (meta_len) {
		if ((io.metadata & 3) || !io.metadata)
			return -EINVAL;
	}

	memset(&c, 0, sizeof(c));
	c.rw.opcode = io.opcode;
	c.rw.flags = io.flags;
	c.rw.nsid = cpu_to_le32(ns->ns_id);
	c.rw.slba = cpu_to_le64(io.slba);
	c.rw.length = cpu_to_le16(io.nblocks);
	c.rw.control = cpu_to_le16(io.control);
	c.rw.dsmgmt = cpu_to_le32(io.dsmgmt);
	c.rw.reftag = cpu_to_le32(io.reftag);
	c.rw.apptag = cpu_to_le16(io.apptag);
	c.rw.appmask = cpu_to_le16(io.appmask);

	return __nvme_submit_user_cmd(ns->queue, &c,
			(void __user *)(uintptr_t)io.addr, length,
			metadata, meta_len, io.slba, NULL, 0);
}

static int nvme_user_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
			struct nvme_passthru_cmd __user *ucmd)
{
	struct nvme_passthru_cmd cmd;
	struct nvme_command c;
	unsigned timeout = 0;
	int status;

#ifndef KV_NVME_SUPPORT
	if (!capable(CAP_SYS_ADMIN))
		return -EACCES;
#endif
	if (copy_from_user(&cmd, ucmd, sizeof(cmd)))
		return -EFAULT;
#ifdef KV_NVME_SUPPORT
        if (!capable(CAP_SYS_ADMIN))
        {
            if (ns == NULL && cmd.opcode == 0x02 && (cpu_to_le32(cmd.cdw10) & 0x000000ff) == 0xd0)
            {
                /* skip capability check to allow InSDB to delete DB with device iterator. */
            }
            else
                return -EACCES;
        }
#endif

	memset(&c, 0, sizeof(c));
	c.common.opcode = cmd.opcode;
	c.common.flags = cmd.flags;
	c.common.nsid = cpu_to_le32(cmd.nsid);
	c.common.cdw2[0] = cpu_to_le32(cmd.cdw2);
	c.common.cdw2[1] = cpu_to_le32(cmd.cdw3);
	c.common.cdw10[0] = cpu_to_le32(cmd.cdw10);
	c.common.cdw10[1] = cpu_to_le32(cmd.cdw11);
	c.common.cdw10[2] = cpu_to_le32(cmd.cdw12);
	c.common.cdw10[3] = cpu_to_le32(cmd.cdw13);
	c.common.cdw10[4] = cpu_to_le32(cmd.cdw14);
	c.common.cdw10[5] = cpu_to_le32(cmd.cdw15);

	if (cmd.timeout_ms)
		timeout = msecs_to_jiffies(cmd.timeout_ms);

	status = nvme_submit_user_cmd(ns ? ns->queue : ctrl->admin_q, &c,
			(void __user *)(uintptr_t)cmd.addr, cmd.data_len,
			&cmd.result, timeout);
	if (status >= 0) {
		if (put_user(cmd.result, &ucmd->result))
			return -EFAULT;
	}

#ifdef KV_NVME_SUPPORT
    if (ns == NULL && c.common.opcode == 0x02 && (c.common.cdw10[0] & 0x000000ff) == 0xD0) {
#ifndef NO_LOG
        atomic64_inc(&kv_sync_get_log);
#endif
    }
#endif
	return status;
}
#ifdef KV_NVME_SUPPORT
int nvme_kv_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_cmd __user *ucmd);
int nvme_kv_multi_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_cmd __user *ucmd);

int nvme_kv_iosched_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_cmd __user *ucmd);
int nvme_kv_multi_iosched_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd);

int nvme_kv_flush_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_cmd __user *ucmd);
int nvme_kv_discard_readahead_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd);
int nvme_kv_read_from_readahead_cmd(struct nvme_ctrl *ctrl, struct nvme_ns *ns,
        struct nvme_passthru_kv_cmd __user *ucmd);
#endif
static int nvme_ioctl(struct block_device *bdev, fmode_t mode,
		unsigned int cmd, unsigned long arg)
{
	struct nvme_ns *ns = bdev->bd_disk->private_data;

	switch (cmd) {
	case NVME_IOCTL_ID:
		force_successful_syscall_return();
		return ns->ns_id;
	case NVME_IOCTL_ADMIN_CMD:
		return nvme_user_cmd(ns->ctrl, NULL, (void __user *)arg);
	case NVME_IOCTL_IO_CMD:
		return nvme_user_cmd(ns->ctrl, ns, (void __user *)arg);
#ifdef KV_NVME_SUPPORT
        case NVME_IOCTL_KV_NONBLOCKING_READ_CMD:
            return nvme_kv_read_from_readahead_cmd(ns->ctrl, ns, (void __user *)arg);
        case NVME_IOCTL_KV_SYNC_CMD:
            return nvme_kv_cmd(ns->ctrl, ns, (void __user *)arg);
        case NVME_IOCTL_KV_MULTISYNC_CMD:
            return nvme_kv_multi_cmd(ns->ctrl, ns, (void __user *)arg);
        case NVME_IOCTL_KV_ASYNC_CMD:
            return nvme_kv_iosched_cmd(ns->ctrl, ns, (void __user *)arg);
        case NVME_IOCTL_KV_MULTIASYNC_CMD:
            return nvme_kv_multi_iosched_cmd(ns->ctrl, ns, (void __user *)arg);
        case NVME_IOCTL_KV_FLUSH_CMD:
            return nvme_kv_flush_cmd(ns->ctrl, ns, (void __user *)arg);
        case NVME_IOCTL_KV_DISCARD_RA_CMD:
            return nvme_kv_discard_readahead_cmd(ns->ctrl, ns, (void __user *)arg);
#endif
	case NVME_IOCTL_SUBMIT_IO:
		return nvme_submit_io(ns, (void __user *)arg);
#ifdef CONFIG_BLK_DEV_NVME_SCSI
	case SG_GET_VERSION_NUM:
		return nvme_sg_get_version_num((void __user *)arg);
	case SG_IO:
		return nvme_sg_io(ns, (void __user *)arg);
#endif
	default:
		return -ENOTTY;
	}
}

#ifdef CONFIG_COMPAT
static int nvme_compat_ioctl(struct block_device *bdev, fmode_t mode,
			unsigned int cmd, unsigned long arg)
{
	switch (cmd) {
	case SG_IO:
		return -ENOIOCTLCMD;
	}
	return nvme_ioctl(bdev, mode, cmd, arg);
}
#else
#define nvme_compat_ioctl	NULL
#endif

static int nvme_open(struct block_device *bdev, fmode_t mode)
{
	return nvme_get_ns_from_disk(bdev->bd_disk) ? 0 : -ENXIO;
}

static void nvme_release(struct gendisk *disk, fmode_t mode)
{
	nvme_put_ns(disk->private_data);
}

static int nvme_getgeo(struct block_device *bdev, struct hd_geometry *geo)
{
	/* some standard values */
	geo->heads = 1 << 6;
	geo->sectors = 1 << 5;
	geo->cylinders = get_capacity(bdev->bd_disk) >> 11;
	return 0;
}

#ifdef CONFIG_BLK_DEV_INTEGRITY
static void nvme_init_integrity(struct nvme_ns *ns)
{
	struct blk_integrity integrity;

	switch (ns->pi_type) {
	case NVME_NS_DPS_PI_TYPE3:
		integrity.profile = &t10_pi_type3_crc;
		break;
	case NVME_NS_DPS_PI_TYPE1:
	case NVME_NS_DPS_PI_TYPE2:
		integrity.profile = &t10_pi_type1_crc;
		break;
	default:
		integrity.profile = NULL;
		break;
	}
	integrity.tuple_size = ns->ms;
	blk_integrity_register(ns->disk, &integrity);
	blk_queue_max_integrity_segments(ns->queue, 1);
}
#else
static void nvme_init_integrity(struct nvme_ns *ns)
{
}
#endif /* CONFIG_BLK_DEV_INTEGRITY */

static void nvme_config_discard(struct nvme_ns *ns)
{
	struct nvme_ctrl *ctrl = ns->ctrl;
	u32 logical_block_size = queue_logical_block_size(ns->queue);

	if (ctrl->quirks & NVME_QUIRK_DISCARD_ZEROES)
		ns->queue->limits.discard_zeroes_data = 1;
	else
		ns->queue->limits.discard_zeroes_data = 0;

	ns->queue->limits.discard_alignment = logical_block_size;
	ns->queue->limits.discard_granularity = logical_block_size;
	blk_queue_max_discard_sectors(ns->queue, 0xffffffff);
	queue_flag_set_unlocked(QUEUE_FLAG_DISCARD, ns->queue);
}

static int nvme_revalidate_disk(struct gendisk *disk)
{
	struct nvme_ns *ns = disk->private_data;
	struct nvme_id_ns *id;
	u8 lbaf, pi_type;
	u16 old_ms;
	unsigned short bs;

	if (test_bit(NVME_NS_DEAD, &ns->flags)) {
		set_capacity(disk, 0);
		return -ENODEV;
	}
	if (nvme_identify_ns(ns->ctrl, ns->ns_id, &id)) {
		dev_warn(ns->ctrl->dev, "%s: Identify failure nvme%dn%d\n",
				__func__, ns->ctrl->instance, ns->ns_id);
		return -ENODEV;
	}
	if (id->ncap == 0) {
		kfree(id);
		return -ENODEV;
	}

	if (nvme_nvm_ns_supported(ns, id) && ns->type != NVME_NS_LIGHTNVM) {
		if (nvme_nvm_register(ns->queue, disk->disk_name)) {
			dev_warn(ns->ctrl->dev,
				"%s: LightNVM init failure\n", __func__);
			kfree(id);
			return -ENODEV;
		}
		ns->type = NVME_NS_LIGHTNVM;
	}

	if (ns->ctrl->vs >= NVME_VS(1, 1))
		memcpy(ns->eui, id->eui64, sizeof(ns->eui));
	if (ns->ctrl->vs >= NVME_VS(1, 2))
		memcpy(ns->uuid, id->nguid, sizeof(ns->uuid));

	old_ms = ns->ms;
	lbaf = id->flbas & NVME_NS_FLBAS_LBA_MASK;
	ns->lba_shift = id->lbaf[lbaf].ds;
	ns->ms = le16_to_cpu(id->lbaf[lbaf].ms);
	ns->ext = ns->ms && (id->flbas & NVME_NS_FLBAS_META_EXT);

	/*
	 * If identify namespace failed, use default 512 byte block size so
	 * block layer can use before failing read/write for 0 capacity.
	 */
	if (ns->lba_shift == 0)
		ns->lba_shift = 9;
	bs = 1 << ns->lba_shift;
	/* XXX: PI implementation requires metadata equal t10 pi tuple size */
	pi_type = ns->ms == sizeof(struct t10_pi_tuple) ?
					id->dps & NVME_NS_DPS_PI_MASK : 0;

	blk_mq_freeze_queue(disk->queue);
	if (blk_get_integrity(disk) && (ns->pi_type != pi_type ||
				ns->ms != old_ms ||
				bs != queue_logical_block_size(disk->queue) ||
				(ns->ms && ns->ext)))
		blk_integrity_unregister(disk);

	ns->pi_type = pi_type;
	blk_queue_logical_block_size(ns->queue, bs);

	if (ns->ms && !blk_get_integrity(disk) && !ns->ext)
		nvme_init_integrity(ns);
	if (ns->ms && !(ns->ms == 8 && ns->pi_type) && !blk_get_integrity(disk))
		set_capacity(disk, 0);
	else
		set_capacity(disk, le64_to_cpup(&id->nsze) << (ns->lba_shift - 9));

	if (ns->ctrl->oncs & NVME_CTRL_ONCS_DSM)
		nvme_config_discard(ns);
	blk_mq_unfreeze_queue(disk->queue);

	kfree(id);
	return 0;
}

static char nvme_pr_type(enum pr_type type)
{
	switch (type) {
	case PR_WRITE_EXCLUSIVE:
		return 1;
	case PR_EXCLUSIVE_ACCESS:
		return 2;
	case PR_WRITE_EXCLUSIVE_REG_ONLY:
		return 3;
	case PR_EXCLUSIVE_ACCESS_REG_ONLY:
		return 4;
	case PR_WRITE_EXCLUSIVE_ALL_REGS:
		return 5;
	case PR_EXCLUSIVE_ACCESS_ALL_REGS:
		return 6;
	default:
		return 0;
	}
};

static int nvme_pr_command(struct block_device *bdev, u32 cdw10,
				u64 key, u64 sa_key, u8 op)
{
	struct nvme_ns *ns = bdev->bd_disk->private_data;
	struct nvme_command c;
	u8 data[16] = { 0, };

	put_unaligned_le64(key, &data[0]);
	put_unaligned_le64(sa_key, &data[8]);

	memset(&c, 0, sizeof(c));
	c.common.opcode = op;
	c.common.nsid = cpu_to_le32(ns->ns_id);
	c.common.cdw10[0] = cpu_to_le32(cdw10);

	return nvme_submit_sync_cmd(ns->queue, &c, data, 16);
}

static int nvme_pr_register(struct block_device *bdev, u64 old,
		u64 new, unsigned flags)
{
	u32 cdw10;

	if (flags & ~PR_FL_IGNORE_KEY)
		return -EOPNOTSUPP;

	cdw10 = old ? 2 : 0;
	cdw10 |= (flags & PR_FL_IGNORE_KEY) ? 1 << 3 : 0;
	cdw10 |= (1 << 30) | (1 << 31); /* PTPL=1 */
	return nvme_pr_command(bdev, cdw10, old, new, nvme_cmd_resv_register);
}

static int nvme_pr_reserve(struct block_device *bdev, u64 key,
		enum pr_type type, unsigned flags)
{
	u32 cdw10;

	if (flags & ~PR_FL_IGNORE_KEY)
		return -EOPNOTSUPP;

	cdw10 = nvme_pr_type(type) << 8;
	cdw10 |= ((flags & PR_FL_IGNORE_KEY) ? 1 << 3 : 0);
	return nvme_pr_command(bdev, cdw10, key, 0, nvme_cmd_resv_acquire);
}

static int nvme_pr_preempt(struct block_device *bdev, u64 old, u64 new,
		enum pr_type type, bool abort)
{
	u32 cdw10 = nvme_pr_type(type) << 8 | abort ? 2 : 1;
	return nvme_pr_command(bdev, cdw10, old, new, nvme_cmd_resv_acquire);
}

static int nvme_pr_clear(struct block_device *bdev, u64 key)
{
	u32 cdw10 = 1 | (key ? 1 << 3 : 0);
	return nvme_pr_command(bdev, cdw10, key, 0, nvme_cmd_resv_register);
}

static int nvme_pr_release(struct block_device *bdev, u64 key, enum pr_type type)
{
	u32 cdw10 = nvme_pr_type(type) << 8 | key ? 1 << 3 : 0;
	return nvme_pr_command(bdev, cdw10, key, 0, nvme_cmd_resv_release);
}

static const struct pr_ops nvme_pr_ops = {
	.pr_register	= nvme_pr_register,
	.pr_reserve	= nvme_pr_reserve,
	.pr_release	= nvme_pr_release,
	.pr_preempt	= nvme_pr_preempt,
	.pr_clear	= nvme_pr_clear,
};

static const struct block_device_operations nvme_fops = {
	.owner		= THIS_MODULE,
	.ioctl		= nvme_ioctl,
	.compat_ioctl	= nvme_compat_ioctl,
	.open		= nvme_open,
	.release	= nvme_release,
	.getgeo		= nvme_getgeo,
	.revalidate_disk= nvme_revalidate_disk,
	.pr_ops		= &nvme_pr_ops,
};

static int nvme_wait_ready(struct nvme_ctrl *ctrl, u64 cap, bool enabled)
{
	unsigned long timeout =
		((NVME_CAP_TIMEOUT(cap) + 1) * HZ / 2) + jiffies;
	u32 csts, bit = enabled ? NVME_CSTS_RDY : 0;
	int ret;

	while ((ret = ctrl->ops->reg_read32(ctrl, NVME_REG_CSTS, &csts)) == 0) {
		if ((csts & NVME_CSTS_RDY) == bit)
			break;

		msleep(100);
		if (fatal_signal_pending(current))
			return -EINTR;
		if (time_after(jiffies, timeout)) {
			dev_err(ctrl->dev,
				"Device not ready; aborting %s\n", enabled ?
						"initialisation" : "reset");
			return -ENODEV;
		}
	}

	return ret;
}

/*
 * If the device has been passed off to us in an enabled state, just clear
 * the enabled bit.  The spec says we should set the 'shutdown notification
 * bits', but doing so may cause the device to complete commands to the
 * admin queue ... and we don't know what memory that might be pointing at!
 */
int nvme_disable_ctrl(struct nvme_ctrl *ctrl, u64 cap)
{
	int ret;

	ctrl->ctrl_config &= ~NVME_CC_SHN_MASK;
	ctrl->ctrl_config &= ~NVME_CC_ENABLE;

	ret = ctrl->ops->reg_write32(ctrl, NVME_REG_CC, ctrl->ctrl_config);
	if (ret)
		return ret;

	if (ctrl->quirks & NVME_QUIRK_DELAY_BEFORE_CHK_RDY)
		msleep(NVME_QUIRK_DELAY_AMOUNT);

	return nvme_wait_ready(ctrl, cap, false);
}

int nvme_enable_ctrl(struct nvme_ctrl *ctrl, u64 cap)
{
	/*
	 * Default to a 4K page size, with the intention to update this
	 * path in the future to accomodate architectures with differing
	 * kernel and IO page sizes.
	 */
	unsigned dev_page_min = NVME_CAP_MPSMIN(cap) + 12, page_shift = 12;
	int ret;

	if (page_shift < dev_page_min) {
		dev_err(ctrl->dev,
			"Minimum device page size %u too large for host (%u)\n",
			1 << dev_page_min, 1 << page_shift);
		return -ENODEV;
	}

	ctrl->page_size = 1 << page_shift;

	ctrl->ctrl_config = NVME_CC_CSS_NVM;
	ctrl->ctrl_config |= (page_shift - 12) << NVME_CC_MPS_SHIFT;
	ctrl->ctrl_config |= NVME_CC_ARB_RR | NVME_CC_SHN_NONE;
	ctrl->ctrl_config |= NVME_CC_IOSQES | NVME_CC_IOCQES;
	ctrl->ctrl_config |= NVME_CC_ENABLE;

	ret = ctrl->ops->reg_write32(ctrl, NVME_REG_CC, ctrl->ctrl_config);
	if (ret)
		return ret;
	return nvme_wait_ready(ctrl, cap, true);
}

int nvme_shutdown_ctrl(struct nvme_ctrl *ctrl)
{
	unsigned long timeout = SHUTDOWN_TIMEOUT + jiffies;
	u32 csts;
	int ret;

	ctrl->ctrl_config &= ~NVME_CC_SHN_MASK;
	ctrl->ctrl_config |= NVME_CC_SHN_NORMAL;

	ret = ctrl->ops->reg_write32(ctrl, NVME_REG_CC, ctrl->ctrl_config);
	if (ret)
		return ret;

	while ((ret = ctrl->ops->reg_read32(ctrl, NVME_REG_CSTS, &csts)) == 0) {
		if ((csts & NVME_CSTS_SHST_MASK) == NVME_CSTS_SHST_CMPLT)
			break;

		msleep(100);
		if (fatal_signal_pending(current))
			return -EINTR;
		if (time_after(jiffies, timeout)) {
			dev_err(ctrl->dev,
				"Device shutdown incomplete; abort shutdown\n");
			return -ENODEV;
		}
	}

	return ret;
}

static void nvme_set_queue_limits(struct nvme_ctrl *ctrl,
		struct request_queue *q)
{
	if (ctrl->max_hw_sectors) {
		u32 max_segments =
			(ctrl->max_hw_sectors / (ctrl->page_size >> 9)) + 1;

		blk_queue_max_hw_sectors(q, ctrl->max_hw_sectors);
		blk_queue_max_segments(q, min_t(u32, max_segments, USHRT_MAX));
	}
	if (ctrl->stripe_size)
		blk_queue_chunk_sectors(q, ctrl->stripe_size >> 9);
	if (ctrl->vwc & NVME_CTRL_VWC_PRESENT)
		blk_queue_flush(q, REQ_FLUSH | REQ_FUA);
	blk_queue_virt_boundary(q, ctrl->page_size - 1);
}

static void nvme_configure_apst(struct nvme_ctrl *ctrl)
{
	/*
	 * APST (Autonomous Power State Transition) lets us program a
	 * table of power state transitions that the controller will
	 * perform automatically.  We configure it with a simple
	 * heuristic: we are willing to spend at most 2% of the time
	 * transitioning between power states.  Therefore, when running
	 * in any given state, we will enter the next lower-power
	 * non-operational state after waiting 50 * (enlat + exlat)
	 * microseconds, as long as that state's exit latency is under
	 * the requested maximum latency.
	 *
	 * We will not autonomously enter any non-operational state for
	 * which the total latency exceeds ps_max_latency_us.  Users
	 * can set ps_max_latency_us to zero to turn off APST.
	 */

	unsigned apste;
	struct nvme_feat_auto_pst *table;
	int ret;

	/*
	 * If APST isn't supported or if we haven't been initialized yet,
	 * then don't do anything.
	 */
	if (!ctrl->apsta)
		return;

	if (ctrl->npss > 31) {
		dev_warn(ctrl->device, "NPSS is invalid; not using APST\n");
		return;
	}

	table = kzalloc(sizeof(*table), GFP_KERNEL);
	if (!table)
		return;

	if (ctrl->ps_max_latency_us == 0) {
		/* Turn off APST. */
		apste = 0;
	} else {
		__le64 target = cpu_to_le64(0);
		int state;

		/*
		 * Walk through all states from lowest- to highest-power.
		 * According to the spec, lower-numbered states use more
		 * power.  NPSS, despite the name, is the index of the
		 * lowest-power state, not the number of states.
		 */
		for (state = (int)ctrl->npss; state >= 0; state--) {
			u64 total_latency_us, exit_latency_us, transition_ms;

			if (target)
				table->entries[state] = target;

			/*
			 * Don't allow transitions to the deepest state
			 * if it's quirked off.
			 */
			if (state == ctrl->npss &&
			    (ctrl->quirks & NVME_QUIRK_NO_DEEPEST_PS))
				continue;

			/*
			 * Is this state a useful non-operational state for
			 * higher-power states to autonomously transition to?
			 */
			if (!(ctrl->psd[state].flags &
			      NVME_PS_FLAGS_NON_OP_STATE))
				continue;

			exit_latency_us =
				(u64)le32_to_cpu(ctrl->psd[state].exit_lat);
			if (exit_latency_us > ctrl->ps_max_latency_us)
				continue;

			total_latency_us =
				exit_latency_us +
				le32_to_cpu(ctrl->psd[state].entry_lat);

			/*
			 * This state is good.  Use it as the APST idle
			 * target for higher power states.
			 */
			transition_ms = total_latency_us + 19;
			do_div(transition_ms, 20);
			if (transition_ms > (1 << 24) - 1)
				transition_ms = (1 << 24) - 1;

			target = cpu_to_le64((state << 3) |
					     (transition_ms << 8));
		}

		apste = 1;
	}

	ret = nvme_set_features(ctrl, NVME_FEAT_AUTO_PST, apste,
				table, sizeof(*table), NULL);
	if (ret)
		dev_err(ctrl->device, "failed to set APST feature (%d)\n", ret);

	kfree(table);
}

static void nvme_set_latency_tolerance(struct device *dev, s32 val)
{
	struct nvme_ctrl *ctrl = dev_get_drvdata(dev);
	u64 latency;

	switch (val) {
	case PM_QOS_LATENCY_TOLERANCE_NO_CONSTRAINT:
	case PM_QOS_LATENCY_ANY:
		latency = U64_MAX;
		break;

	default:
		latency = val;
	}

	if (ctrl->ps_max_latency_us != latency) {
		ctrl->ps_max_latency_us = latency;
		nvme_configure_apst(ctrl);
	}
}

struct nvme_core_quirk_entry {
	/*
	 * NVMe model and firmware strings are padded with spaces.  For
	 * simplicity, strings in the quirk table are padded with NULLs
	 * instead.
	 */
	u16 vid;
	const char *mn;
	const char *fr;
	unsigned long quirks;
};

static const struct nvme_core_quirk_entry core_quirks[] = {
	{
		/*
		 * This Toshiba device seems to die using any APST states.  See:
		 * https://bugs.launchpad.net/ubuntu/+source/linux/+bug/1678184/comments/11
		 */
		.vid = 0x1179,
		.mn = "THNSF5256GPUK TOSHIBA",
		.quirks = NVME_QUIRK_NO_APST,
	}
};

/* match is null-terminated but idstr is space-padded. */
static bool string_matches(const char *idstr, const char *match, size_t len)
{
	size_t matchlen;

	if (!match)
		return true;

	matchlen = strlen(match);
	WARN_ON_ONCE(matchlen > len);

	if (memcmp(idstr, match, matchlen))
		return false;

	for (; matchlen < len; matchlen++)
		if (idstr[matchlen] != ' ')
			return false;

	return true;
}

static bool quirk_matches(const struct nvme_id_ctrl *id,
			  const struct nvme_core_quirk_entry *q)
{
	return q->vid == le16_to_cpu(id->vid) &&
		string_matches(id->mn, q->mn, sizeof(id->mn)) &&
		string_matches(id->fr, q->fr, sizeof(id->fr));
}

/*
 * Initialize the cached copies of the Identify data and various controller
 * register in our nvme_ctrl structure.  This should be called as soon as
 * the admin queue is fully up and running.
 */
int nvme_init_identify(struct nvme_ctrl *ctrl)
{
	struct nvme_id_ctrl *id;
	u64 cap;
	int ret, page_shift;
	u8 prev_apsta;

	ret = ctrl->ops->reg_read32(ctrl, NVME_REG_VS, &ctrl->vs);
	if (ret) {
		dev_err(ctrl->dev, "Reading VS failed (%d)\n", ret);
		return ret;
	}

	ret = ctrl->ops->reg_read64(ctrl, NVME_REG_CAP, &cap);
	if (ret) {
		dev_err(ctrl->dev, "Reading CAP failed (%d)\n", ret);
		return ret;
	}
	page_shift = NVME_CAP_MPSMIN(cap) + 12;

	if (ctrl->vs >= NVME_VS(1, 1))
		ctrl->subsystem = NVME_CAP_NSSRC(cap);

	ret = nvme_identify_ctrl(ctrl, &id);
	if (ret) {
		dev_err(ctrl->dev, "Identify Controller failed (%d)\n", ret);
		return -EIO;
	}

	if (!ctrl->identified) {
		/*
		 * Check for quirks.  Quirk can depend on firmware version,
		 * so, in principle, the set of quirks present can change
		 * across a reset.  As a possible future enhancement, we
		 * could re-scan for quirks every time we reinitialize
		 * the device, but we'd have to make sure that the driver
		 * behaves intelligently if the quirks change.
		 */

		int i;

		for (i = 0; i < ARRAY_SIZE(core_quirks); i++) {
			if (quirk_matches(id, &core_quirks[i]))
				ctrl->quirks |= core_quirks[i].quirks;
		}
	}

	ctrl->oncs = le16_to_cpup(&id->oncs);
	atomic_set(&ctrl->abort_limit, id->acl + 1);
	ctrl->vwc = id->vwc;
	memcpy(ctrl->serial, id->sn, sizeof(id->sn));
	memcpy(ctrl->model, id->mn, sizeof(id->mn));
	memcpy(ctrl->firmware_rev, id->fr, sizeof(id->fr));
	if (id->mdts)
		ctrl->max_hw_sectors = 1 << (id->mdts + page_shift - 9);
	else
		ctrl->max_hw_sectors = UINT_MAX;

	if ((ctrl->quirks & NVME_QUIRK_STRIPE_SIZE) && id->vs[3]) {
		unsigned int max_hw_sectors;

		ctrl->stripe_size = 1 << (id->vs[3] + page_shift);
		max_hw_sectors = ctrl->stripe_size >> (page_shift - 9);
		if (ctrl->max_hw_sectors) {
			ctrl->max_hw_sectors = min(max_hw_sectors,
							ctrl->max_hw_sectors);
		} else {
			ctrl->max_hw_sectors = max_hw_sectors;
		}
	}

	nvme_set_queue_limits(ctrl, ctrl->admin_q);

	ctrl->npss = id->npss;
	prev_apsta = ctrl->apsta;
	ctrl->apsta = (ctrl->quirks & NVME_QUIRK_NO_APST) ? 0 : id->apsta;
	memcpy(ctrl->psd, id->psd, sizeof(ctrl->psd));

	kfree(id);

	if (ctrl->apsta && !prev_apsta)
		dev_pm_qos_expose_latency_tolerance(ctrl->device);
	else if (!ctrl->apsta && prev_apsta)
		dev_pm_qos_hide_latency_tolerance(ctrl->device);

	nvme_configure_apst(ctrl);

	ctrl->identified = true;

	return ret;
}

static int nvme_dev_open(struct inode *inode, struct file *file)
{
	struct nvme_ctrl *ctrl;
	int instance = iminor(inode);
	int ret = -ENODEV;

	spin_lock(&dev_list_lock);
	list_for_each_entry(ctrl, &nvme_ctrl_list, node) {
		if (ctrl->instance != instance)
			continue;

		if (!ctrl->admin_q) {
			ret = -EWOULDBLOCK;
			break;
		}
		if (!kref_get_unless_zero(&ctrl->kref))
			break;
		file->private_data = ctrl;
		ret = 0;
		break;
	}
	spin_unlock(&dev_list_lock);

	return ret;
}

static int nvme_dev_release(struct inode *inode, struct file *file)
{
	nvme_put_ctrl(file->private_data);
	return 0;
}

static int nvme_dev_user_cmd(struct nvme_ctrl *ctrl, void __user *argp)
{
	struct nvme_ns *ns;
	int ret;

	mutex_lock(&ctrl->namespaces_mutex);
	if (list_empty(&ctrl->namespaces)) {
		ret = -ENOTTY;
		goto out_unlock;
	}

	ns = list_first_entry(&ctrl->namespaces, struct nvme_ns, list);
	if (ns != list_last_entry(&ctrl->namespaces, struct nvme_ns, list)) {
		dev_warn(ctrl->dev,
			"NVME_IOCTL_IO_CMD not supported when multiple namespaces present!\n");
		ret = -EINVAL;
		goto out_unlock;
	}

	dev_warn(ctrl->dev,
		"using deprecated NVME_IOCTL_IO_CMD ioctl on the char device!\n");
	kref_get(&ns->kref);
	mutex_unlock(&ctrl->namespaces_mutex);

	ret = nvme_user_cmd(ctrl, ns, argp);
	nvme_put_ns(ns);
	return ret;

out_unlock:
	mutex_unlock(&ctrl->namespaces_mutex);
	return ret;
}

static long nvme_dev_ioctl(struct file *file, unsigned int cmd,
		unsigned long arg)
{
	struct nvme_ctrl *ctrl = file->private_data;
	void __user *argp = (void __user *)arg;

	switch (cmd) {
	case NVME_IOCTL_ADMIN_CMD:
		return nvme_user_cmd(ctrl, NULL, argp);
	case NVME_IOCTL_IO_CMD:
		return nvme_dev_user_cmd(ctrl, argp);
	case NVME_IOCTL_RESET:
		dev_warn(ctrl->dev, "resetting controller\n");
		return ctrl->ops->reset_ctrl(ctrl);
	case NVME_IOCTL_SUBSYS_RESET:
		return nvme_reset_subsystem(ctrl);
	default:
		return -ENOTTY;
	}
}

static const struct file_operations nvme_dev_fops = {
	.owner		= THIS_MODULE,
	.open		= nvme_dev_open,
	.release	= nvme_dev_release,
	.unlocked_ioctl	= nvme_dev_ioctl,
	.compat_ioctl	= nvme_dev_ioctl,
};

static ssize_t nvme_sysfs_reset(struct device *dev,
				struct device_attribute *attr, const char *buf,
				size_t count)
{
	struct nvme_ctrl *ctrl = dev_get_drvdata(dev);
	int ret;

	ret = ctrl->ops->reset_ctrl(ctrl);
	if (ret < 0)
		return ret;
	return count;
}
static DEVICE_ATTR(reset_controller, S_IWUSR, NULL, nvme_sysfs_reset);

static ssize_t uuid_show(struct device *dev, struct device_attribute *attr,
								char *buf)
{
	struct nvme_ns *ns = dev_to_disk(dev)->private_data;
	return sprintf(buf, "%pU\n", ns->uuid);
}
static DEVICE_ATTR(uuid, S_IRUGO, uuid_show, NULL);

static ssize_t eui_show(struct device *dev, struct device_attribute *attr,
								char *buf)
{
	struct nvme_ns *ns = dev_to_disk(dev)->private_data;
	return sprintf(buf, "%8phd\n", ns->eui);
}
static DEVICE_ATTR(eui, S_IRUGO, eui_show, NULL);

static ssize_t nsid_show(struct device *dev, struct device_attribute *attr,
								char *buf)
{
	struct nvme_ns *ns = dev_to_disk(dev)->private_data;
	return sprintf(buf, "%d\n", ns->ns_id);
}
static DEVICE_ATTR(nsid, S_IRUGO, nsid_show, NULL);

static struct attribute *nvme_ns_attrs[] = {
	&dev_attr_uuid.attr,
	&dev_attr_eui.attr,
	&dev_attr_nsid.attr,
	NULL,
};

static umode_t nvme_attrs_are_visible(struct kobject *kobj,
		struct attribute *a, int n)
{
	struct device *dev = container_of(kobj, struct device, kobj);
	struct nvme_ns *ns = dev_to_disk(dev)->private_data;

	if (a == &dev_attr_uuid.attr) {
		if (!memchr_inv(ns->uuid, 0, sizeof(ns->uuid)))
			return 0;
	}
	if (a == &dev_attr_eui.attr) {
		if (!memchr_inv(ns->eui, 0, sizeof(ns->eui)))
			return 0;
	}
	return a->mode;
}

static const struct attribute_group nvme_ns_attr_group = {
	.attrs		= nvme_ns_attrs,
	.is_visible	= nvme_attrs_are_visible,
};

#define nvme_show_function(field)						\
static ssize_t  field##_show(struct device *dev,				\
			    struct device_attribute *attr, char *buf)		\
{										\
        struct nvme_ctrl *ctrl = dev_get_drvdata(dev);				\
        return sprintf(buf, "%.*s\n", (int)sizeof(ctrl->field), ctrl->field);	\
}										\
static DEVICE_ATTR(field, S_IRUGO, field##_show, NULL);

nvme_show_function(model);
nvme_show_function(serial);
nvme_show_function(firmware_rev);

static struct attribute *nvme_dev_attrs[] = {
	&dev_attr_reset_controller.attr,
	&dev_attr_model.attr,
	&dev_attr_serial.attr,
	&dev_attr_firmware_rev.attr,
	NULL
};

static struct attribute_group nvme_dev_attrs_group = {
	.attrs = nvme_dev_attrs,
};

static const struct attribute_group *nvme_dev_attr_groups[] = {
	&nvme_dev_attrs_group,
	NULL,
};

static int ns_cmp(void *priv, struct list_head *a, struct list_head *b)
{
	struct nvme_ns *nsa = container_of(a, struct nvme_ns, list);
	struct nvme_ns *nsb = container_of(b, struct nvme_ns, list);

	return nsa->ns_id - nsb->ns_id;
}

static struct nvme_ns *nvme_find_ns(struct nvme_ctrl *ctrl, unsigned nsid)
{
	struct nvme_ns *ns;

	lockdep_assert_held(&ctrl->namespaces_mutex);

	list_for_each_entry(ns, &ctrl->namespaces, list) {
		if (ns->ns_id == nsid)
			return ns;
		if (ns->ns_id > nsid)
			break;
	}
	return NULL;
}

#ifndef NO_LOG
struct kv_iosched_struct *proc_kv_iosched[12] = {NULL,};
#endif
int kv_ssd_id = 0;
static void nvme_alloc_ns(struct nvme_ctrl *ctrl, unsigned nsid)
{
	struct nvme_ns *ns;
	struct gendisk *disk;
	int node = dev_to_node(ctrl->dev);

	lockdep_assert_held(&ctrl->namespaces_mutex);

	ns = kzalloc_node(sizeof(*ns), GFP_KERNEL, node);
	if (!ns)
		return;

	ns->queue = blk_mq_init_queue(ctrl->tagset);
	if (IS_ERR(ns->queue))
		goto out_free_ns;
	queue_flag_set_unlocked(QUEUE_FLAG_NOMERGES, ns->queue);
	queue_flag_set_unlocked(QUEUE_FLAG_NONROT, ns->queue);
	ns->queue->queuedata = ns;
	ns->ctrl = ctrl;

	disk = alloc_disk_node(0, node);
	if (!disk)
		goto out_free_queue;

	kref_init(&ns->kref);
	ns->ns_id = nsid;
	ns->disk = disk;
	ns->lba_shift = 9; /* set to a default value for 512 until disk is validated */
#ifdef KV_NVME_SUPPORT
    /* need to create a new kv_iosched object */
    ns->kv_iosched = kzalloc_node(sizeof(*ns->kv_iosched), GFP_KERNEL, node);
    {
        int err = kv_iosched_init(ns->kv_iosched, node);
#ifndef NO_LOG
        proc_kv_iosched[kv_ssd_id] = ns->kv_iosched;
#endif
        ns->kv_iosched->kv_ssd_id = kv_ssd_id++;
        printk("[%d:%s] kv_iosched size = %ldKB(err : %d)(KVSSD ID : %d )\n",__LINE__, __func__, sizeof(*ns->kv_iosched)/1024, err, ns->kv_iosched->kv_ssd_id);
    }
#endif

	blk_queue_logical_block_size(ns->queue, 1 << ns->lba_shift);
	nvme_set_queue_limits(ctrl, ns->queue);

	disk->major = nvme_major;
	disk->first_minor = 0;
	disk->fops = &nvme_fops;
	disk->private_data = ns;
	disk->queue = ns->queue;
	disk->driverfs_dev = ctrl->device;
	disk->flags = GENHD_FL_EXT_DEVT;
	sprintf(disk->disk_name, "nvme%dn%d", ctrl->instance, nsid);

	if (nvme_revalidate_disk(ns->disk))
		goto out_free_disk;

	list_add_tail(&ns->list, &ctrl->namespaces);
	kref_get(&ctrl->kref);
	if (ns->type == NVME_NS_LIGHTNVM)
		return;

	add_disk(ns->disk);
	if (sysfs_create_group(&disk_to_dev(ns->disk)->kobj,
					&nvme_ns_attr_group))
		pr_warn("%s: failed to create sysfs group for identification\n",
			ns->disk->disk_name);
	return;
 out_free_disk:
	kfree(disk);
 out_free_queue:
	blk_cleanup_queue(ns->queue);
 out_free_ns:
	kfree(ns);
}

static void nvme_ns_remove(struct nvme_ns *ns)
{
	if (test_and_set_bit(NVME_NS_REMOVING, &ns->flags))
		return;

	if (ns->disk->flags & GENHD_FL_UP) {
		if (blk_get_integrity(ns->disk))
			blk_integrity_unregister(ns->disk);
		sysfs_remove_group(&disk_to_dev(ns->disk)->kobj,
					&nvme_ns_attr_group);
		del_gendisk(ns->disk);
		blk_mq_abort_requeue_list(ns->queue);
		blk_cleanup_queue(ns->queue);
	}
#ifdef KV_NVME_SUPPORT
    /*
     * Flush remained works and destroy data structure include
     */
    kv_iosched_destroy(ns->kv_iosched);
#endif
	mutex_lock(&ns->ctrl->namespaces_mutex);
	list_del_init(&ns->list);
	mutex_unlock(&ns->ctrl->namespaces_mutex);
	nvme_put_ns(ns);
}

static void nvme_validate_ns(struct nvme_ctrl *ctrl, unsigned nsid)
{
	struct nvme_ns *ns;

	ns = nvme_find_ns(ctrl, nsid);
	if (ns) {
		if (revalidate_disk(ns->disk))
			nvme_ns_remove(ns);
	} else
		nvme_alloc_ns(ctrl, nsid);
}

static int nvme_scan_ns_list(struct nvme_ctrl *ctrl, unsigned nn)
{
	struct nvme_ns *ns;
	__le32 *ns_list;
	unsigned i, j, nsid, prev = 0, num_lists = DIV_ROUND_UP(nn, 1024);
	int ret = 0;

	ns_list = kzalloc(0x1000, GFP_KERNEL);
	if (!ns_list)
		return -ENOMEM;

	for (i = 0; i < num_lists; i++) {
		ret = nvme_identify_ns_list(ctrl, prev, ns_list);
		if (ret)
			goto out;

		for (j = 0; j < min(nn, 1024U); j++) {
			nsid = le32_to_cpu(ns_list[j]);
			if (!nsid)
				goto out;

			nvme_validate_ns(ctrl, nsid);

			while (++prev < nsid) {
				ns = nvme_find_ns(ctrl, prev);
				if (ns)
					nvme_ns_remove(ns);
			}
		}
		nn -= j;
	}
 out:
	kfree(ns_list);
	return ret;
}

static void __nvme_scan_namespaces(struct nvme_ctrl *ctrl, unsigned nn)
{
	struct nvme_ns *ns, *next;
	unsigned i;

	lockdep_assert_held(&ctrl->namespaces_mutex);

	for (i = 1; i <= nn; i++)
		nvme_validate_ns(ctrl, i);

	list_for_each_entry_safe(ns, next, &ctrl->namespaces, list) {
		if (ns->ns_id > nn)
			nvme_ns_remove(ns);
	}
}

void nvme_scan_namespaces(struct nvme_ctrl *ctrl)
{
	struct nvme_id_ctrl *id;
	unsigned nn;

	if (nvme_identify_ctrl(ctrl, &id))
		return;

	mutex_lock(&ctrl->namespaces_mutex);
	nn = le32_to_cpu(id->nn);
	if (ctrl->vs >= NVME_VS(1, 1) &&
	    !(ctrl->quirks & NVME_QUIRK_IDENTIFY_CNS)) {
		if (!nvme_scan_ns_list(ctrl, nn))
			goto done;
	}
	__nvme_scan_namespaces(ctrl, le32_to_cpup(&id->nn));
 done:
	list_sort(NULL, &ctrl->namespaces, ns_cmp);
	mutex_unlock(&ctrl->namespaces_mutex);
	kfree(id);
}

void nvme_remove_namespaces(struct nvme_ctrl *ctrl)
{
	struct nvme_ns *ns, *next;

	list_for_each_entry_safe(ns, next, &ctrl->namespaces, list)
		nvme_ns_remove(ns);
}

static DEFINE_IDA(nvme_instance_ida);

static int nvme_set_instance(struct nvme_ctrl *ctrl)
{
	int instance, error;

	do {
		if (!ida_pre_get(&nvme_instance_ida, GFP_KERNEL))
			return -ENODEV;

		spin_lock(&dev_list_lock);
		error = ida_get_new(&nvme_instance_ida, &instance);
		spin_unlock(&dev_list_lock);
	} while (error == -EAGAIN);

	if (error)
		return -ENODEV;

	ctrl->instance = instance;
	return 0;
}

static void nvme_release_instance(struct nvme_ctrl *ctrl)
{
	spin_lock(&dev_list_lock);
	ida_remove(&nvme_instance_ida, ctrl->instance);
	spin_unlock(&dev_list_lock);
}

void nvme_uninit_ctrl(struct nvme_ctrl *ctrl)
 {
	device_destroy(nvme_class, MKDEV(nvme_char_major, ctrl->instance));

	spin_lock(&dev_list_lock);
	list_del(&ctrl->node);
	spin_unlock(&dev_list_lock);
}

static void nvme_free_ctrl(struct kref *kref)
{
	struct nvme_ctrl *ctrl = container_of(kref, struct nvme_ctrl, kref);

	put_device(ctrl->device);
	nvme_release_instance(ctrl);

	ctrl->ops->free_ctrl(ctrl);
}

void nvme_put_ctrl(struct nvme_ctrl *ctrl)
{
	kref_put(&ctrl->kref, nvme_free_ctrl);
}

/*
 * Initialize a NVMe controller structures.  This needs to be called during
 * earliest initialization so that we have the initialized structured around
 * during probing.
 */
int nvme_init_ctrl(struct nvme_ctrl *ctrl, struct device *dev,
		const struct nvme_ctrl_ops *ops, unsigned long quirks)
{
	int ret;

	INIT_LIST_HEAD(&ctrl->namespaces);
	mutex_init(&ctrl->namespaces_mutex);
	kref_init(&ctrl->kref);
	ctrl->dev = dev;
	ctrl->ops = ops;
	ctrl->quirks = quirks;

	ret = nvme_set_instance(ctrl);
	if (ret)
		goto out;

	ctrl->device = device_create_with_groups(nvme_class, ctrl->dev,
				MKDEV(nvme_char_major, ctrl->instance),
				dev, nvme_dev_attr_groups,
				"nvme%d", ctrl->instance);
	if (IS_ERR(ctrl->device)) {
		ret = PTR_ERR(ctrl->device);
		goto out_release_instance;
	}
	get_device(ctrl->device);
	dev_set_drvdata(ctrl->device, ctrl);

	spin_lock(&dev_list_lock);
	list_add_tail(&ctrl->node, &nvme_ctrl_list);
	spin_unlock(&dev_list_lock);

	/*
	 * Initialize latency tolerance controls.  The sysfs files won't
	 * be visible to userspace unless the device actually supports APST.
	 */
	ctrl->device->power.set_latency_tolerance = nvme_set_latency_tolerance;
	dev_pm_qos_update_user_latency_tolerance(ctrl->device,
		min(default_ps_max_latency_us, (unsigned long)S32_MAX));

	return 0;
out_release_instance:
	nvme_release_instance(ctrl);
out:
	return ret;
}

/**
 * nvme_kill_queues(): Ends all namespace queues
 * @ctrl: the dead controller that needs to end
 *
 * Call this function when the driver determines it is unable to get the
 * controller in a state capable of servicing IO.
 */
void nvme_kill_queues(struct nvme_ctrl *ctrl)
{
	struct nvme_ns *ns;

	mutex_lock(&ctrl->namespaces_mutex);
	list_for_each_entry(ns, &ctrl->namespaces, list) {
		if (!kref_get_unless_zero(&ns->kref))
			continue;

		/*
		 * Revalidating a dead namespace sets capacity to 0. This will
		 * end buffered writers dirtying pages that can't be synced.
		 */
		if (!test_and_set_bit(NVME_NS_DEAD, &ns->flags))
			revalidate_disk(ns->disk);

		blk_set_queue_dying(ns->queue);
		blk_mq_abort_requeue_list(ns->queue);
		blk_mq_start_stopped_hw_queues(ns->queue, true);

		nvme_put_ns(ns);
	}
	mutex_unlock(&ctrl->namespaces_mutex);
}

void nvme_stop_queues(struct nvme_ctrl *ctrl)
{
	struct nvme_ns *ns;

	mutex_lock(&ctrl->namespaces_mutex);
	list_for_each_entry(ns, &ctrl->namespaces, list) {
		spin_lock_irq(ns->queue->queue_lock);
		queue_flag_set(QUEUE_FLAG_STOPPED, ns->queue);
		spin_unlock_irq(ns->queue->queue_lock);

		blk_mq_cancel_requeue_work(ns->queue);
		blk_mq_stop_hw_queues(ns->queue);
	}
	mutex_unlock(&ctrl->namespaces_mutex);
}

void nvme_start_queues(struct nvme_ctrl *ctrl)
{
	struct nvme_ns *ns;

	mutex_lock(&ctrl->namespaces_mutex);
	list_for_each_entry(ns, &ctrl->namespaces, list) {
		queue_flag_clear_unlocked(QUEUE_FLAG_STOPPED, ns->queue);
		blk_mq_start_stopped_hw_queues(ns->queue, true);
		blk_mq_kick_requeue_list(ns->queue);
	}
	mutex_unlock(&ctrl->namespaces_mutex);
}
#ifdef KV_NVME_SUPPORT
/*
 * proc fs to check internal value
 */
struct gendisk *disk;
#ifndef NO_LOG
uint64_t kv_proc_total_async_request;
uint64_t kv_proc_total_get_async_merge;
uint64_t kv_proc_total_put_del_async_merge;
uint64_t kv_proc_total_async_submit;
uint64_t kv_proc_total_sync_request;
uint64_t kv_proc_total_sync_submit;
uint64_t kv_proc_total_blocked_submit;
uint64_t kv_proc_total_async_interrupt;
uint64_t kv_proc_total_async_complete;
uint64_t kv_proc_total_ra_hit;
uint64_t kv_proc_total_nonblocking_read_fail;
uint64_t kv_proc_total_ra_hit_in_flight;
uint64_t kv_proc_total_canceled_ra;
uint64_t kv_proc_total_discard_ra;
uint64_t kv_proc_total_discard_not_found_ra;
uint64_t kv_proc_total_discard_cancel_not_on_device;
uint64_t kv_proc_total_discard_on_device;
uint64_t kv_proc_total_sync_complete;
uint64_t kv_proc_total_sync_finished;
uint64_t kv_proc_completion_retry;
uint64_t kv_proc_completion_rq_empty;
uint64_t kv_proc_completion_wakeup_by_int;
uint64_t kv_proc_completion_wakeup_by_submit;
uint64_t kv_proc_flush_wakeup_by_submit;
uint64_t kv_proc_flush_wakeup_by_completion;
uint64_t kv_proc_flush_statistics[KV_flush_nr];
uint64_t kv_proc_hash_restart[KV_hash_restart_nr];
uint64_t kv_proc_async_get_req;
uint64_t kv_proc_async_put_req;
uint64_t kv_proc_async_del_req;
uint64_t kv_proc_sync_get_req;
uint64_t kv_proc_sync_get_merge_with_async_put;
uint64_t kv_proc_sync_get_merge_with_async_del;
uint64_t kv_proc_sync_put_req;
uint64_t kv_proc_sync_del_req;
uint64_t kv_proc_sync_iter_req;
uint64_t kv_proc_sync_iter_read_req;
uint64_t kv_proc_sync_get_log;
#endif



static int kv_proc_show(struct seq_file *p, void *v) {
#ifndef NO_LOG
    int i;
    bool print_out = false;

    uint64_t local_kv_total_async_request = atomic64_read(&kv_total_async_request);
    uint64_t local_kv_total_get_async_merge = atomic64_read(&kv_total_get_async_merge);
    uint64_t local_kv_total_put_del_async_merge = atomic64_read(&kv_total_put_del_async_merge);
    uint64_t local_kv_total_async_submit = atomic64_read(&kv_total_async_submit);
    uint64_t local_kv_total_sync_request = atomic64_read(&kv_total_sync_request);
    uint64_t local_kv_total_sync_submit = atomic64_read(&kv_total_sync_submit);
    uint64_t local_kv_total_blocked_submit = atomic64_read(&kv_total_blocked_submit);
    uint64_t local_kv_total_async_interrupt = atomic64_read(&kv_total_async_interrupt);
    uint64_t local_kv_total_async_complete = atomic64_read(&kv_total_async_complete);
    uint64_t local_kv_total_ra_hit = atomic64_read(&kv_total_ra_hit);
    uint64_t local_kv_total_nonblocking_read_fail = atomic64_read(&kv_total_nonblocking_read_fail);
    uint64_t local_kv_total_ra_hit_in_flight = atomic64_read(&kv_total_ra_hit_in_flight);
    uint64_t local_kv_total_canceled_ra = atomic64_read(&kv_total_canceled_ra);
    uint64_t local_kv_total_discard_ra = atomic64_read(&kv_total_discard_ra);
    uint64_t local_kv_total_discard_not_found_ra = atomic64_read(&kv_total_discard_not_found_ra);
    uint64_t local_kv_total_discard_cancel_not_on_device = atomic64_read(&kv_total_discard_cancel_not_on_device);
    uint64_t local_kv_total_discard_on_device = atomic64_read(&kv_total_discard_on_device);
    uint64_t local_kv_total_sync_complete = atomic64_read(&kv_total_sync_complete);
    uint64_t local_kv_total_sync_finished = atomic64_read(&kv_total_sync_finished);
    uint64_t local_kv_completion_retry = atomic64_read(&kv_completion_retry);
    uint64_t local_kv_completion_rq_empty = atomic64_read(&kv_completion_rq_empty);
    uint64_t local_kv_completion_wakeup_by_int = atomic64_read(&kv_completion_wakeup_by_int);
    uint64_t local_kv_completion_wakeup_by_submit = atomic64_read(&kv_completion_wakeup_by_submit);
    uint64_t local_kv_flush_wakeup_by_submit = atomic64_read(&kv_flush_wakeup_by_submit);
    uint64_t local_kv_flush_wakeup_by_completion = atomic64_read(&kv_flush_wakeup_by_completion);
    uint64_t local_kv_async_get_req = atomic64_read(&kv_async_get_req);
    uint64_t local_kv_async_put_req = atomic64_read(&kv_async_put_req);
    uint64_t local_kv_async_del_req = atomic64_read(&kv_async_del_req);
    uint64_t local_kv_sync_get_req = atomic64_read(&kv_sync_get_req);
    uint64_t local_kv_sync_get_merge_with_async_put = atomic64_read(&kv_sync_get_merge_with_async_put);
    uint64_t local_kv_sync_get_merge_with_async_del = atomic64_read(&kv_sync_get_merge_with_async_del);
    uint64_t local_kv_sync_put_req = atomic64_read(&kv_sync_put_req);
    uint64_t local_kv_sync_del_req = atomic64_read(&kv_sync_del_req);
    uint64_t local_kv_sync_iter_req = atomic64_read(&kv_sync_iter_req);
    uint64_t local_kv_sync_iter_read_req = atomic64_read(&kv_sync_iter_read_req);
    uint64_t local_kv_sync_get_log = atomic64_read(&kv_sync_get_log);

    uint64_t local_kv_dev_pending =  local_kv_total_async_submit -local_kv_total_async_interrupt;
    uint64_t local_kv_completion_pending = local_kv_total_async_interrupt -local_kv_total_async_complete;
    uint64_t local_kv_hash_restart[KV_hash_restart_nr];
    uint64_t local_kv_flush_statistics[KV_flush_nr];

    for(i = KV_hash_com_insert; i < KV_hash_restart_nr; i++){
        local_kv_hash_restart[i] = atomic64_read(&kv_hash_restart[i]);
    }
    for(i = KV_flush_begin ; i < KV_flush_nr; i++){
        local_kv_flush_statistics[i] = atomic64_read(&kv_flush_statistics[i]);
    }

    seq_printf(p, "============ Asynchronous IO ============ \n");
    seq_printf(p, "Total Async RQ                 : %llu : delta : %llu\n", local_kv_total_async_request, local_kv_total_async_request-kv_proc_total_async_request);
    seq_printf(p, "Total Merged RQ with Get RQ    : %llu : delta : %llu\n", local_kv_total_get_async_merge, local_kv_total_get_async_merge - kv_proc_total_get_async_merge);
    seq_printf(p, "Total Merged RQ with Pet/Del RQ: %llu : delta : %llu\n", local_kv_total_put_del_async_merge, local_kv_total_put_del_async_merge -kv_proc_total_put_del_async_merge);
    seq_printf(p, "Total Submitted Async RQ       : %llu : delta : %llu\n", local_kv_total_async_submit, local_kv_total_async_submit -kv_proc_total_async_submit);
    seq_printf(p, "Total Blokced Async Submission : %llu : delta : %llu\n", local_kv_total_blocked_submit, local_kv_total_blocked_submit-kv_proc_total_blocked_submit);
    seq_printf(p, "Total Async interrupt          : %llu : delta : %llu\n", local_kv_total_async_interrupt,local_kv_total_async_interrupt-kv_proc_total_async_interrupt);
    seq_printf(p, "Total Async Completed RQ       : %llu : delta : %llu\n", local_kv_total_async_complete, local_kv_total_async_complete-kv_proc_total_async_complete);
    for(i = 0 ; i < kv_ssd_id ; i++){
        if(proc_kv_iosched[i]){
            if(atomic_read(&proc_kv_iosched[i]->kv_hash[KV_RA_HASH].nr_rq) || atomic_read(&proc_kv_iosched[i]->kv_hash[KV_RQ_HASH].nr_rq)){
                seq_printf(p, "[KVSSDID : %d]Total # of Req. in Readahread RQ    : %d\n", i, atomic_read(&proc_kv_iosched[i]->kv_hash[KV_RA_HASH].nr_rq));
                seq_printf(p, "[KVSSDID : %d]Total # of Req. In Pending    RQ    : %d\n", i, atomic_read(&proc_kv_iosched[i]->kv_hash[KV_RQ_HASH].nr_rq));
            }
        }
    }
    seq_printf(p, "Total # Device Pending Req.         : %llu\n", local_kv_dev_pending);
    seq_printf(p, "Total # interrup -completed Req.   : %llu\n", local_kv_completion_pending);
    seq_printf(p, "The # of Async Get RQ               : %llu : delta : %llu\n", local_kv_async_get_req, local_kv_async_get_req -kv_proc_async_get_req);
    seq_printf(p, "The # of Async Put RQ               : %llu : delta : %llu\n", local_kv_async_put_req, local_kv_async_put_req-kv_proc_async_put_req);
    seq_printf(p, "The # of Async Del RQ               : %llu : delta : %llu\n", local_kv_async_del_req, local_kv_async_del_req-kv_proc_async_del_req);

    seq_printf(p, "============ Asynchronous Error Status============ \n");
    seq_printf(p, "Async Put Success    : %lu\n", atomic64_read(&kv_async_error[kv_async_success]));
    for(i = kv_async_invalidValueSize; i <= kv_async_unknownError; i++){
        seq_printf(p, "Async Put Fail(%d)       : %lu\n", i, atomic64_read(&kv_async_error[i]));
    }
    seq_printf(p, "============ Synchronous IO ============ \n");
    seq_printf(p, "Total Requested Sync RQ       : %llu : delta : %llu\n", local_kv_total_sync_request, local_kv_total_sync_request-kv_proc_total_sync_request);
    seq_printf(p, "Total Submitted Sync RQ       : %llu : delta : %llu\n", local_kv_total_sync_submit,local_kv_total_sync_submit-kv_proc_total_sync_submit);
    seq_printf(p, "Total Sync Completed RQ       : %llu : delta : %llu\n", local_kv_total_sync_complete,local_kv_total_sync_complete-kv_proc_total_sync_complete);
    seq_printf(p, "Total Sync Finished  RQ       : %llu : delta : %llu\n", local_kv_total_sync_finished,local_kv_total_sync_finished-kv_proc_total_sync_finished);
    seq_printf(p, "The # of RA hit                      : %llu : delta : %llu\n", local_kv_total_ra_hit,local_kv_total_ra_hit-kv_proc_total_ra_hit);
    seq_printf(p, "The # of non-blocking read faile     : %llu : delta : %llu\n", local_kv_total_nonblocking_read_fail,local_kv_total_nonblocking_read_fail-kv_proc_total_nonblocking_read_fail);
    seq_printf(p, "The # of canceled RA(And sync Get)   : %llu : delta : %llu\n", local_kv_total_canceled_ra,local_kv_total_canceled_ra-kv_proc_total_canceled_ra);
    seq_printf(p, "The # of discard RA Found            : %llu : delta : %llu\n", local_kv_total_discard_ra,local_kv_total_discard_ra-kv_proc_total_discard_ra);
    seq_printf(p, "The # of discard RA Not Found        : %llu : delta : %llu\n", local_kv_total_discard_not_found_ra,local_kv_total_discard_not_found_ra-kv_proc_total_discard_not_found_ra);
    seq_printf(p, "The # of discard Not on device       : %llu : delta : %llu\n", local_kv_total_discard_cancel_not_on_device,local_kv_total_discard_cancel_not_on_device-kv_proc_total_discard_cancel_not_on_device);
    seq_printf(p, "The # of discard on device           : %llu : delta : %llu\n", local_kv_total_discard_on_device,local_kv_total_discard_on_device-kv_proc_total_discard_on_device);
    seq_printf(p, "The # of in flight RA hit            : %llu : delta : %llu\n", local_kv_total_ra_hit_in_flight, local_kv_total_ra_hit_in_flight-kv_proc_total_ra_hit_in_flight);
    seq_printf(p, "The # of Sync Get RQ                 : %llu : delta : %llu\n", local_kv_sync_get_req, local_kv_sync_get_req-kv_proc_sync_get_req);
    seq_printf(p, "The # of Sync Get Merged To Async Put: %llu : delta : %llu\n", local_kv_sync_get_merge_with_async_put,local_kv_sync_get_merge_with_async_put-kv_proc_sync_get_merge_with_async_put);
    seq_printf(p, "The # of Sync Get Merged To Async Del: %llu : delta : %llu\n", local_kv_sync_get_merge_with_async_del,local_kv_sync_get_merge_with_async_del-kv_proc_sync_get_merge_with_async_del);
    seq_printf(p, "The # of Sync Put RQ                 : %llu : delta : %llu\n", local_kv_sync_put_req,local_kv_sync_put_req-kv_proc_sync_put_req);
    seq_printf(p, "The # of Sync Del RQ                 : %llu : delta : %llu\n", local_kv_sync_del_req,local_kv_sync_del_req-kv_proc_sync_del_req);
    seq_printf(p, "The # of Sync Iterate req RQ         : %llu : delta : %llu\n", local_kv_sync_iter_req,local_kv_sync_iter_req-kv_proc_sync_iter_req);
    seq_printf(p, "The # of Sync Iterate read RQ        : %llu : delta : %llu\n", local_kv_sync_iter_read_req,local_kv_sync_iter_read_req-kv_proc_sync_iter_read_req);
    seq_printf(p, "The # of Sync Getlog RQ        : %llu : delta : %llu\n", local_kv_sync_get_log, local_kv_sync_get_log-kv_proc_sync_get_log);


    seq_printf(p, "============ Wake Completion Work up============ \n");
    seq_printf(p, "by Interrupt Handler: %llu : delta : %llu\n", local_kv_completion_wakeup_by_int,local_kv_completion_wakeup_by_int-kv_proc_completion_wakeup_by_int);
    seq_printf(p, "by Submission Daemon: %llu : delta : %llu\n", local_kv_completion_wakeup_by_submit,local_kv_completion_wakeup_by_submit-kv_proc_completion_wakeup_by_submit);
    seq_printf(p, "by Completion Daemon: %llu : delta : %llu\n", local_kv_completion_retry,local_kv_completion_retry-kv_proc_completion_retry);
    seq_printf(p, "============ Completion Work ============ \n");
    seq_printf(p, "Empty completed rq list in Completion Daemon: %llu : delta : %llu\n", local_kv_completion_rq_empty,local_kv_completion_rq_empty-kv_proc_completion_rq_empty);
    seq_printf(p, "============ Flush ============ \n");
    seq_printf(p, "Flush begin :  %llu : delta : %llu\n", local_kv_flush_statistics[KV_flush_begin],local_kv_flush_statistics[KV_flush_begin] -kv_proc_flush_statistics[KV_flush_begin]);
    seq_printf(p, "Wake up by sumbmit: %llu : delta : %llu\n", local_kv_flush_wakeup_by_submit,local_kv_flush_wakeup_by_submit-kv_proc_flush_wakeup_by_submit);
    seq_printf(p, "Wake up by complete: %llu : delta : %llu\n", local_kv_flush_wakeup_by_completion,local_kv_flush_wakeup_by_completion-kv_proc_flush_wakeup_by_completion);

    seq_printf(p, "Sigle No key : %llu : delta : %llu\n", local_kv_flush_statistics[KV_flush_single_nokey_found],local_kv_flush_statistics[KV_flush_single_nokey_found]-kv_proc_flush_statistics[KV_flush_single_nokey_found]);
    seq_printf(p, "Sigle Flush : %llu : delta : %llu\n", local_kv_flush_statistics[KV_flush_single],local_kv_flush_statistics[KV_flush_single]-kv_proc_flush_statistics[KV_flush_single]);
    seq_printf(p, "All Flush : %llu : delta : %llu\n", local_kv_flush_statistics[KV_flush_all],local_kv_flush_statistics[KV_flush_all]-kv_proc_flush_statistics[KV_flush_all]);
    seq_printf(p, "Do Flush :     %llu : delta : %llu\n", local_kv_flush_statistics[KV_flush_done], local_kv_flush_statistics[KV_flush_done]-kv_proc_flush_statistics[KV_flush_done]);
    seq_printf(p, "All Cancel Flush : %llu : delta : %llu\n", local_kv_flush_statistics[KV_flush_all_cancel],local_kv_flush_statistics[KV_flush_all_cancel]-kv_proc_flush_statistics[KV_flush_all_cancel]);
    seq_printf(p, "Single Cancel Flush : %llu : delta : %llu\n", local_kv_flush_statistics[KV_flush_single_cancel],local_kv_flush_statistics[KV_flush_single_cancel]-kv_proc_flush_statistics[KV_flush_single_cancel]);
    seq_printf(p, "Flush end:     %llu : delta : %llu\n", local_kv_flush_statistics[KV_flush_end ],local_kv_flush_statistics[KV_flush_end]-kv_proc_flush_statistics[KV_flush_end]);
    seq_printf(p, "Flush Wait Time :     %llu : delta : %llumsec\n", local_kv_flush_statistics[KV_flush_wait_time],local_kv_flush_statistics[KV_flush_wait_time]-kv_proc_flush_statistics[KV_flush_wait_time]);
    seq_printf(p, "Total Flush Time:     %llu : delta : %llumsec\n", local_kv_flush_statistics[KV_flush_entire_time],local_kv_flush_statistics[KV_flush_entire_time]-kv_proc_flush_statistics[KV_flush_entire_time]);


    seq_printf(p, "============ Hash search restart reason ============ \n");
    seq_printf(p, " sync completed     : %llu : delta : %llu\n",     local_kv_hash_restart[KV_hash_com_sync],local_kv_hash_restart[KV_hash_com_sync]-kv_proc_hash_restart[KV_hash_com_sync]);
    seq_printf(p, " sync invalidated   : %llu : delta : %llu\n",     local_kv_hash_restart[KV_hash_inv_sync],local_kv_hash_restart[KV_hash_inv_sync]-kv_proc_hash_restart[KV_hash_inv_sync]);
    seq_printf(p, " sync  blocked      : %llu : delta : %llu\n",     local_kv_hash_restart[KV_hash_blk_sync],local_kv_hash_restart[KV_hash_blk_sync]-kv_proc_hash_restart[KV_hash_blk_sync]);
    seq_printf(p, " insert completed   : %llu : delta : %llu\n",     local_kv_hash_restart[KV_hash_com_insert],local_kv_hash_restart[KV_hash_com_insert]-kv_proc_hash_restart[KV_hash_com_insert]);
    seq_printf(p, " insert invalidated : %llu : delta : %llu\n",   local_kv_hash_restart[KV_hash_inv_insert],local_kv_hash_restart[KV_hash_inv_insert]-kv_proc_hash_restart[KV_hash_inv_insert]);
    seq_printf(p, " insert blocked     : %llu : delta : %llu\n",   local_kv_hash_restart[KV_hash_blk_insert],local_kv_hash_restart[KV_hash_blk_insert]-kv_proc_hash_restart[KV_hash_blk_insert]);

    kv_proc_total_async_request =                    local_kv_total_async_request ;
    kv_proc_total_get_async_merge =                  local_kv_total_get_async_merge ;
    kv_proc_total_put_del_async_merge =              local_kv_total_put_del_async_merge ;
    kv_proc_total_async_submit =                     local_kv_total_async_submit ;
    kv_proc_total_sync_request =                     local_kv_total_sync_request ;
    kv_proc_total_sync_submit =                      local_kv_total_sync_submit ;
    kv_proc_total_blocked_submit =                   local_kv_total_blocked_submit ;
    kv_proc_total_async_interrupt =                  local_kv_total_async_interrupt ;
    kv_proc_total_async_complete =                   local_kv_total_async_complete ;
    kv_proc_total_ra_hit =                           local_kv_total_ra_hit ;
    kv_proc_total_nonblocking_read_fail=                           local_kv_total_nonblocking_read_fail;
    kv_proc_total_ra_hit_in_flight =                 local_kv_total_ra_hit_in_flight ;
    kv_proc_total_canceled_ra =                      local_kv_total_canceled_ra ;
    kv_proc_total_discard_ra =                       local_kv_total_discard_ra;
    kv_proc_total_discard_not_found_ra =             local_kv_total_discard_not_found_ra;
    kv_proc_total_discard_cancel_not_on_device =     local_kv_total_discard_cancel_not_on_device;
    kv_proc_total_discard_on_device =         local_kv_total_discard_on_device;
    kv_proc_total_sync_complete =                    local_kv_total_sync_complete ;
    kv_proc_total_sync_finished =                    local_kv_total_sync_finished ;
    kv_proc_completion_retry =                       local_kv_completion_retry ;
    kv_proc_completion_rq_empty =                    local_kv_completion_rq_empty ;
    kv_proc_completion_wakeup_by_int =               local_kv_completion_wakeup_by_int ;
    kv_proc_completion_wakeup_by_submit =            local_kv_completion_wakeup_by_submit ;
    kv_proc_flush_wakeup_by_submit =                 local_kv_flush_wakeup_by_submit ;
    kv_proc_flush_wakeup_by_completion =             local_kv_flush_wakeup_by_completion ;
    kv_proc_async_get_req =                          local_kv_async_get_req ;
    kv_proc_async_put_req =                          local_kv_async_put_req ;
    kv_proc_async_del_req =                          local_kv_async_del_req ;
    kv_proc_sync_get_req =                           local_kv_sync_get_req ;
    kv_proc_sync_get_merge_with_async_put =          local_kv_sync_get_merge_with_async_put ;
    kv_proc_sync_get_merge_with_async_del =          local_kv_sync_get_merge_with_async_del ;
    kv_proc_sync_put_req =                           local_kv_sync_put_req ;
    kv_proc_sync_del_req =                           local_kv_sync_del_req ;
    kv_proc_sync_iter_req =                          local_kv_sync_iter_req ;
    kv_proc_sync_iter_read_req =                     local_kv_sync_iter_read_req ;
    kv_proc_sync_get_log =                           local_kv_sync_get_log;

    for(i = KV_hash_com_insert; i < KV_hash_restart_nr; i++){
        kv_proc_hash_restart[i] = local_kv_hash_restart[i];
    }
    for(i = KV_flush_begin ; i < KV_flush_nr; i++){
        kv_proc_flush_statistics[i] = local_kv_flush_statistics[i];
    }

    for(i = 0 ; i < 8 ;i++){
        if(strcmp(kvct_ioctl[i], "init")){

#if 1
            if(!print_out)
                seq_printf(p, "============ Asynchronous RQ Trace ============ \n");
            seq_printf(p, "Async ioctl code(id:%d): %s\n", i, kvct_ioctl[i]);
#else
            if(!print_out)
                seq_printf(p, "============ Flush memq lock Trace ============ \n");
            seq_printf(p, "Flush code(id:%d): %s\n", i, kvct_ioctl[i]);
#endif
            print_out = true;
        }
    }
    print_out = false;
    for(i = READ_DAEMON ; i < NR_DAEMON;i++){
        if(strcmp(kvct_submission[i], "init")){
            if(!print_out)
                seq_printf(p, "============ Daemon Trace ============ \n");
            seq_printf(p, "Submission(%d) code : %s\n", i, kvct_submission[i]);
            print_out = true;
        }
    }
    for(i = 0; i < NR_KV_SSD;i++){
        if(strcmp(kvct_completion[i], "init")){
            if(!print_out)
                seq_printf(p, "============ Daemon Trace ============ \n");
            seq_printf(p, "[%d]Completion code : %s\n", i, kvct_completion[i]);
            print_out = true;
        }
    }
    if(strcmp(kvct_int_handle, "init")){
        if(!print_out)
            seq_printf(p, "============ Daemon Trace ============ \n");
        seq_printf(p, "INT handle code : %s\n", kvct_int_handle);
        print_out = true;
    }
    print_out = false;
    for(i = 0 ; i < 8 ;i++){
        if(strcmp(kvct_sync_io[i], "init")){
#if 0
            if(!print_out)
                seq_printf(p, "============ Synchronous RQ Trace ============ \n");
            seq_printf(p, "Sync ioctl code(id:%d): %s\n", i, kvct_sync_io[i]);
#else
            if(!print_out)
                seq_printf(p, "============ memq lock Trace in scheduling daemon============ \n");
            seq_printf(p, "Scheduling daemon(id:%d): %s\n", i, kvct_sync_io[i]);
#endif
            print_out = true;
        }
    }
    for(i = 0 ; i < 8 ;i++){
        if(strcmp(kvct_discard_readahead[i], "init")){
            if(!print_out)
                seq_printf(p, "============ Discard Readahead Trace ============ \n");
            seq_printf(p, "Async ioctl code(id:%d): %s\n", i, kvct_discard_readahead[i]);
            print_out = true;
        }
    }
    for(i = 0 ; i < 8 ;i++){
        if(strcmp(kvct_flush[i], "init")){
            if(!print_out)
                seq_printf(p, "============ Flush Trace============ \n");
            seq_printf(p, "Flush code(id:%d): %s\n", i, kvct_flush[i]);
            print_out = true;
        }
    }
    seq_printf(p, "Complie DD with #define NO_LOG(kv_iosched.h) to disable logging\n");
#else
    seq_printf(p, "Complie DD without #define NO_LOG(kv_iosched.h) to see the log\n");
#endif
    return 0;
}
static int kv_proc_open(struct inode *inode, struct  file *file) {
    struct block_device *bdev = I_BDEV(file->f_mapping->host);
    disk = bdev->bd_disk;
    return single_open(file, kv_proc_show, NULL);
}

static ssize_t kv_proc_write(struct file *filp,const char *buf,size_t count,loff_t *offp)
{
#ifndef NO_LOG
    int i;

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
    atomic64_set(&kv_total_discard_on_device, 0);
    atomic64_set(&kv_total_sync_complete, 0);
    atomic64_set(&kv_total_sync_finished, 0);
    atomic64_set(&kv_total_async_interrupt, 0);
    atomic64_set(&kv_completion_retry, 0);
    atomic64_set(&kv_completion_rq_empty, 0);
    atomic64_set(&kv_completion_wakeup_by_int, 0);
    atomic64_set(&kv_completion_wakeup_by_submit, 0);
    atomic64_set(&kv_flush_wakeup_by_submit, 0);
    atomic64_set(&kv_flush_wakeup_by_completion, 0);
    atomic64_set(&kv_async_get_req, 0);
    atomic64_set(&kv_async_put_req, 0);
    atomic64_set(&kv_async_del_req, 0);
    atomic64_set(&kv_sync_get_req, 0);
    atomic64_set(&kv_sync_get_merge_with_async_put, 0);
    atomic64_set(&kv_sync_get_merge_with_async_del, 0);
    atomic64_set(&kv_sync_put_req, 0);
    atomic64_set(&kv_sync_del_req, 0);
    atomic64_set(&kv_sync_iter_req, 0);
    atomic64_set(&kv_sync_iter_read_req, 0);
    atomic64_set(&kv_sync_get_log, 0);


    for(i = kv_async_success; i <= kv_async_unknownError; i++){
        atomic64_set(&kv_async_error[i], 0);
    }

    for(i = KV_flush_begin ; i < KV_flush_nr ; i++){
        atomic64_set(&kv_flush_statistics[i], 0);
    }
    for(i = KV_hash_com_insert; i < KV_hash_restart_nr; i++){
        atomic64_set(&kv_hash_restart[i], 0);
    }
    for(i = READ_DAEMON ; i < NR_DAEMON;i++)
        sprintf(kvct_submission[i], "init");
    for(i = 0; i < NR_KV_SSD;i++)
        sprintf(kvct_completion[i], "init");
    sprintf(kvct_int_handle, "init");
    for(i = 0 ; i < 8 ;i++){
        sprintf(kvct_ioctl[i], "init");
        sprintf(kvct_sync_io[i], "init");
        sprintf(kvct_discard_readahead[i], "init");
        sprintf(kvct_flush[i], "init");
    }
#endif
    return count;
}

static const struct file_operations kv_proc_fops = {
    .owner = THIS_MODULE,
    .open = kv_proc_open,
    .read = seq_read,
    .write = kv_proc_write,
    .llseek = seq_lseek,
    .release = seq_release,
};
#endif

int __init nvme_core_init(void)
{
	int result;
#ifdef KV_NVME_SUPPORT
    result = kv_core_init();
    if(result < 0)
        return result;
#endif

	result = register_blkdev(nvme_major, "nvme");
	if (result < 0)
		return result;
	else if (result > 0)
		nvme_major = result;
#ifdef KV_NVME_SUPPORT

    proc_create("kv_proc", 0, NULL, &kv_proc_fops);
#endif

	result = __register_chrdev(nvme_char_major, 0, NVME_MINORS, "nvme",
							&nvme_dev_fops);
	if (result < 0)
		goto unregister_blkdev;
	else if (result > 0)
		nvme_char_major = result;

	nvme_class = class_create(THIS_MODULE, "nvme");
	if (IS_ERR(nvme_class)) {
		result = PTR_ERR(nvme_class);
		goto unregister_chrdev;
	}

	return 0;

 unregister_chrdev:
	__unregister_chrdev(nvme_char_major, 0, NVME_MINORS, "nvme");
 unregister_blkdev:
	unregister_blkdev(nvme_major, "nvme");
#ifdef KV_NVME_SUPPORT
    remove_proc_entry("kv_proc", NULL);
#endif
	return result;
}

void nvme_core_exit(void)
{
	unregister_blkdev(nvme_major, "nvme");
	class_destroy(nvme_class);
	__unregister_chrdev(nvme_char_major, 0, NVME_MINORS, "nvme");
#ifdef KV_NVME_SUPPORT
    remove_proc_entry("kv_proc", NULL);
    kv_core_exit();
#endif
}
