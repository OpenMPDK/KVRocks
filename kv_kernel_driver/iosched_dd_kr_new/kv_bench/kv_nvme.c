/*
 * kv_nvme.c 
 * Author : Ilgu hong, Modified by Heekwon Park
 * E-mail : heekwon.p@samsung.com
 *
 * Send a request to kv device driver using ioctl 
 */
#include <string.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include "linux_nvme_ioctl.h"
#include "kv_nvme.h"
#include "kv_trace.h"
#include "kv_bench.h"

#ifdef DUMP_ISSUE_CMD
static void dump_cmd(struct nvme_passthru_kv_cmd *cmd)
{
	printf("[dump issued cmd opcode (%02x)]\n", cmd->opcode);
	printf("\t opcode(%02x)\n", cmd->opcode);
	printf("\t flags(%02x)\n", cmd->flags);
	printf("\t rsvd1(%04d)\n", cmd->rsvd1);
	printf("\t nsid(%08x)\n", cmd->nsid);
	printf("\t cdw2(%08x)\n", cmd->cdw2);
	printf("\t cdw3(%08x)\n", cmd->cdw3);
	printf("\t cdw4(%08x)\n", cmd->cdw4);
	printf("\t cdw5(%08x)\n", cmd->cdw5);
	printf("\t data_addr(%p)\n",(void *)cmd->data_addr);
	printf("\t data_length(%08x)\n", cmd->data_length);
	printf("\t key_length(%08x)\n", cmd->key_length);
	printf("\t cdw10(%08x)\n", cmd->cdw10);
	printf("\t cdw11(%08x)\n", cmd->cdw11);
	printf("\t cdw12(%08x)\n", cmd->cdw12);
	printf("\t cdw13(%08x)\n", cmd->cdw13);
	printf("\t cdw14(%08x)\n", cmd->cdw14);
	printf("\t cdw15(%08x)\n", cmd->cdw15);
	printf("\t timeout_ms(%08x)\n", cmd->timeout_ms);
	printf("\t result(%08x)\n", cmd->result);
	printf("\n\n\n");
}
#endif


int nvme_kv_flush(int fd, unsigned int nsid){
	int ret = 0;
	struct nvme_passthru_kv_cmd cmd;
	memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
#ifndef BENCHMARK_OVERHEAD
	ret = ioctl(fd, NVME_IOCTL_KV_FLUSH_CMD, &cmd);
#endif
	if (ret) {
		LOG("Flush Error : opcode(%02x) error(%d) and cmd.result(%d) cmd.status(%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	} 
	return ret;
}

int nvme_kv_discard_readahead(int fd, unsigned int nsid){
	int ret = 0;
	struct nvme_passthru_kv_cmd cmd;
	memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
#ifndef BENCHMARK_OVERHEAD
	ret = ioctl(fd, NVME_IOCTL_KV_DISCARD_RA_CMD, &cmd);
#endif
	if (ret) {
		LOG("Flush Error : opcode(%02x) error(%d) and cmd.result(%d) cmd.status(%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	} 
	return ret;
}



int nvme_kv_store(int fd, unsigned int nsid,
		const char *key, int key_len,
		const char *value, int value_len,
		int offset, enum nvme_kv_store_option option, int io_type)
{
	int ret = 0;
	struct nvme_passthru_kv_cmd cmd;
	memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
	cmd.opcode = nvme_cmd_kv_store;
	cmd.nsid = nsid;
#ifdef IDEM_AND_OFFSET_IS_SUPPORTED
    cmd.cdw4 = option;
	cmd.cdw5 |= offset;
#else
	cmd.cdw5 = 0;
#endif
	cmd.data_addr = (__u64)value;
	cmd.data_length = value_len;
	cmd.key_length = key_len;
	if (key_len > KVCMD_INLINE_KEY_MAX) {
		cmd.key_addr = (__u64)key;
	} else {
		memcpy(cmd.key, key, key_len);
	}
	cmd.cdw11 = key_len - 1;
	cmd.cdw10 = (value_len >> 2);

    /*For ioctl code trace*/
    cmd.ctxid= offset;
#ifdef DUMP_ISSUE_CMD
	dump_cmd(&cmd);
#endif
#ifndef BENCHMARK_OVERHEAD
    if(io_type == ASYNC)
        ret = ioctl(fd, NVME_IOCTL_KV_ASYNC_CMD, &cmd);
    else if(io_type == SYNC)
        ret = ioctl(fd, NVME_IOCTL_KV_SYNC_CMD, &cmd);
    else
        LOG("Wrong IO type\n");
#endif

	if (ret) {
		LOG("opcode(%02x) error(%d) and cmd.result(%d) cmd.status(%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	} 
	return ret;
}


int nvme_kv_retrieve(int fd, unsigned int nsid,
		const char *key, int key_len,
		const char *value, int value_len,
		int offset, enum nvme_kv_retrieve_option option, int io_type)
{
	int ret = 0;
	struct nvme_passthru_kv_cmd cmd;
	memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
	cmd.opcode = nvme_cmd_kv_retrieve;
	cmd.nsid = nsid;
#ifdef IDEM_AND_OFFSET_IS_SUPPORTED
    cmd.cdw4 = option;
	cmd.cdw5 = offset;
#else
	cmd.cdw5 = 0;
#endif
	cmd.data_addr = (__u64)value;
	cmd.data_length = value_len;
	cmd.key_length = key_len;
	if (key_len > KVCMD_INLINE_KEY_MAX) {
		cmd.key_addr = (__u64)key;
	} else {
		memcpy(cmd.key, key, key_len);
	}
	cmd.cdw11 = key_len - 1;
	cmd.cdw10 = (value_len >> 2);


#ifdef DUMP_ISSUE_CMD
	dump_cmd(&cmd);
#endif
#ifndef BENCHMARK_OVERHEAD
    if(io_type == ASYNC)
        ret = ioctl(fd, NVME_IOCTL_KV_ASYNC_CMD, &cmd);
    else if(io_type == SYNC)
        ret = ioctl(fd, NVME_IOCTL_KV_SYNC_CMD, &cmd);
    else
        LOG("Wrong IO type\n");
#endif

#ifdef DUMP_ISSUE_CMD
	if (ret) {
		printf("opcode(%02x) error(%d) and cmd.result(%d) cmd.status(%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	} else {
		value_len = cmd.result;
		printf("opcode(%02x) ret (%d) and cmd.result(%d) cmd.status (%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	}
#endif
	return ret;
}


int nvme_kv_delete(int fd, unsigned int nsid,
		const char *key, int key_len, enum nvme_kv_delete_option option, int io_type)
{
	int ret = 0;
	struct nvme_passthru_kv_cmd cmd;
	memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
	cmd.opcode = nvme_cmd_kv_delete;
	cmd.nsid = nsid;
#ifdef IDEM_AND_OFFSET_IS_SUPPORTED
    cmd.cdw4 = option;
#endif
	cmd.key_length = key_len;
	if (key_len > KVCMD_INLINE_KEY_MAX) {
		cmd.key_addr = (__u64)key;
	} else {
		memcpy(cmd.key, key, key_len);
	}
	cmd.cdw11 = key_len - 1;


#ifdef DUMP_ISSUE_CMD
	dump_cmd(&cmd);
#endif
#ifndef BENCHMARK_OVERHEAD
    if(io_type == ASYNC)
        ret = ioctl(fd, NVME_IOCTL_KV_ASYNC_CMD, &cmd);
    else if(io_type == SYNC)
        ret = ioctl(fd, NVME_IOCTL_KV_SYNC_CMD, &cmd);
    else
        LOG("Wrong IO type\n");
#endif
#ifdef DUMP_ISSUE_CMD
	if (ret) {
		printf("opcode(%02x) error(%d) and cmd.result(%d) cmd.status(%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	} else {
		printf("opcode(%02x) ret (%d) and cmd.result(%d) cmd.status (%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	}
#endif
	return ret;
}


#if 0
int nvme_kv_exist(int fd, unsigned int nsid,
		const char *key_array, int key_array_len,
		const char *result, int result_len,
		int key_len, int numkey,
		unsigned char key_option)
{
	int ret = 0;
	struct nvme_passthru_kv_cmd cmd;
	memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
	cmd.opcode = nvme_cmd_kv_exist;
	cmd.nsid = nsid;
	if (key_option) {
		cmd.cdw5 = 1;
	}
	cmd.data_addr = (__u64)key_array;
	cmd.data_length = key_array_len;
	cmd.key_length = result_len;
	*((__u64 *)&cmd.cdw10) = (__u64)result;
	cmd.cdw13 = numkey - 1;
	cmd.cdw14 = key_len - 1;
	cmd.cdw15 = result_len;

#ifdef DUMP_ISSUE_CMD
	dump_cmd(&cmd);
#endif
	ret = ioctl(fd, NVME_IOCTL_IO_CMD, (struct nvme_passthru_cmd_general *)&cmd);
#ifdef DUMP_ISSUE_CMD
	if (ret) {
		printf("opcode(%02x) error(%d) and cmd.result(%d) cmd.status(%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	} else {
		printf("opcode(%02x) ret (%d) and cmd.result(%d) cmd.status (%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	}
#endif
	return ret;
}

int nvme_kv_iterate(int fd, unsigned int nsid,
		const char *result, int result_len,
		int *start, unsigned int iterator,
		unsigned int bitmask, unsigned char key_option)
{
	int ret = 0;
	struct nvme_passthru_kv_cmd cmd;
	memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
	cmd.opcode = nvme_cmd_kv_iterate;
	cmd.nsid = nsid;
	if (key_option) {
		cmd.cdw5 = 1;
	}
	cmd.data_addr = (__u64)result;
	cmd.data_length = result_len;
	cmd.cdw10 = iterator;
	//cmd.cdw11 = *start; /* not implemented yet */
	cmd.cdw14 = bitmask;
	cmd.cdw15 = result_len;

#ifdef DUMP_ISSUE_CMD
	dump_cmd(&cmd);
#endif
	ret = ioctl(fd, NVME_IOCTL_IO_CMD, (struct nvme_passthru_cmd_general *)&cmd);
#ifdef DUMP_ISSUE_CMD
	if (ret) {
		printf("opcode(%02x) error(%d) and cmd.result(%d) cmd.status(%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	} else {
		// *start = cmd.result;
		printf("opcode(%02x) ret (%d) and cmd.result(%d) cmd.status (%d).\n",cmd.opcode, ret, cmd.result, cmd.status);
	}
#endif
	return ret;
}
#endif
