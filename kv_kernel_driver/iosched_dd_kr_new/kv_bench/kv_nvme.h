/*
 * kv_nvme.h 
 * Author : Ilgu hong & modified by Heekwon Park
 * E-mail : heekwon.p@samsung.com
 *
 * KV device driver interface
 */
#ifndef _KV_NVME_H_
#define _KV_NVME_H_

//#define DUMP_ISSUE_CMD
#include <stdbool.h>
enum nvme_kv_store_option {
   STORE_OPTION_NOTHING = 0,
   STORE_OPTION_COMP = 1,
   STORE_OPTION_IDEMPOTENT = 2,
   STORE_OPTION_BGCOMP = 4
};

enum nvme_kv_retrieve_option {
    RETRIEVE_OPTION_NOTHING = 0,
    RETRIEVE_OPTION_DECOMP = 1,
    RETRIEVE_OPTION_ONLY_VALSIZE = 2
};

enum nvme_kv_delete_option {
    DELETE_OPTION_NOTHING = 0,
    DELETE_OPTION_CHECK_KEY_EXIST = 1
};

enum nvme_kv_iter_req_option {
    ITER_OPTION_NOTHING = 0,
    ITER_OPTION_OPEN = 1,
    ITER_OPTION_CLOSE = 2
};

//#define BENCHMARK_OVERHEAD
enum nvme_kv_opcode {
	nvme_cmd_kv_store   = 0x81,
	nvme_cmd_kv_append   = 0x83,
	nvme_cmd_kv_retrieve    = 0x90,
	nvme_cmd_kv_delete  = 0xA1,
    nvme_cmd_kv_iter_req = 0xB1,
    nvme_cmd_kv_iter_read = 0xB2,
#if 0
	nvme_cmd_kv_exist   = 0xB0,
	nvme_cmd_kv_iterate = 0xB2,
#endif
};

#define KVCMD_INLINE_KEY_MAX	(16)
#define KVCMD_MAX_KEY_SIZE		(255)
#define KVCMD_MIN_KEY_SIZE		(255)

int nvme_kv_discard_readahead(int fd, unsigned int nsid);
int nvme_kv_flush(int fd, unsigned int nsid);
int nvme_kv_store(int fd, unsigned int nsid,
		const char *key, int key_len,
		const char *value, int value_len,
		int offset, enum nvme_kv_store_option option, int);

int nvme_kv_retrieve(int fd, unsigned int nsid,
		const char *key, int key_len,
		const char *value, int value_len,
		int offset, enum nvme_kv_retrieve_option option, int io_type);

int nvme_kv_delete(int fd, unsigned int nsid,
		const char *key, int key_len, enum nvme_kv_delete_option option, int io_type);
#if 0
int nvme_kv_exist(int fd, unsigned int nsid,
		const char *key_array, int key_array_len,
		const char *result, int result_len,
		int key_len, int numkey,
		unsigned char key_option);

int nvme_kv_iterate(int fd, unsigned int nsid,
		const char *result, int result_len,
		int *start, unsigned int iterator,
		unsigned int bitmask, unsigned char key_option);
#endif

#endif /* #ifndef _KV_NVME_H_ */
