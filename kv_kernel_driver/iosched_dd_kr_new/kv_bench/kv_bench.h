/*
 * kv_bench.h
 * Author : Heekwon Park
 * E-mail : heekwon.p@samsung.com
 *
 * Define data structures
 */
#ifndef KV_BENCH_H
#define KV_BENCH_H

#define NAME_LEN                (255)
#define MAX_KEY_SIZE            (255)
#define MIN_VALUE_SIZE          (1024)
#define MAX_VALUE_SHIFT         (12)
#define MAX_VALUE_SIZE          (MIN_VALUE_SIZE << MAX_VALUE_SHIFT)
#define DEFAULT_VALUE_SIZE		(0x1000)
#define VALUE_ALIGNMENT         (4)
#define MAX_THREAD_CNT          (128)
#define TO_KBYTE                (1024)
#define TO_MBYTE                TO_KBYTE/TO_KBYTE

#define DEFAULT_DEV_PATH        "/dev/nvme0n1"
#define DEFAULT_KEY_FILE        "key.list"
#define DEFAULT_CONFIG          "thread.default.conf"
#define DEFAULT_KEY_SIZE        (16)

#define PAGE_SIZE (0x1000)

enum io_type{
    ASYNC       =   0,
    SYNC        =   1,
    MAX_IO_TYPE =   2
};

enum write_type{
    UNWRITTEN   =   0,
    WRITTEN     =   1,
    NR_WRITTEN_STATE    =   2
};

enum is_update{
    NEW = 0,
    UPDATE = 1,
    NR_UPDATE_TYPE= 2
};

enum cmd_type{
    PUT          =   0,
    GET          =   1,
    DEL          =   2,
    MAX_CMD_TYPE =   3
};
struct key_struct{
    /*
     * written_cnt = 0 : The key has not been written to KV devices
     * written_cnt = 1 : The key has been written
     * written_cnt > 1 : The key has been overwritten
     */
    char *key;
    unsigned long key_size;
    unsigned long value_size;
    int  written_cnt;
    /* The value of the key will be initialized (written_cnt+key) pattern. */
};

struct env_set{
    char dev[NAME_LEN];         
    int fd;
    unsigned int nsid;         
    char thread_config_file[NAME_LEN];    
    char key_file[NAME_LEN];    
    /* 
     * key size for newly added keys 
     * The keys in key file may have different key size.
     */
    int key_size;
    /* 
     * If value_size is 0, the size of the value can be chosen randomly.
     */
    unsigned long value_size;             	
    bool unique_value;

    unsigned long nr_shared_key;

    struct key_struct **exist_shared_key;
    unsigned long nr_exist_shared_key;
    struct key_struct **new_shared_key;
    unsigned long nr_new_shared_key;

    pthread_spinlock_t lock;
    /* Use unique key range for each threads */
};
/* 
 * Types of thread configuration
 * 1. Async Ratio
 * 2. Put ratio 
 * 3. Get ratio
 * 4. private key
 * 5. Sequntial Access Pattern
 * 6. Number of keys
 * 7. Number of requests
 */
enum config_id{
    ASYNC_RATIO     = 0,
    PUT_RATIO       = 1,
    GET_RATIO       = 2,
    PRIVATE_KEY     = 3,
    ACCESS_PATTERN  = 4,
    NR_KEYS         = 5,
    NR_REQ          = 6,
    NR_THREAD_CONFIG = 7
};
struct per_thread_config{
    /* (async ratio + sync ratio) must be same to 100 */
    int io_ratio[MAX_IO_TYPE];
    /* (put ratio + get ratio + del_ratio) must be same to 100. */
    int cmd_ratio[MAX_CMD_TYPE];
    /*
     * TRUE  : use private key
     * FALSE : use public(shared) key
     */
    bool use_private_key;
    /*
     * TRUE  : sequtial access
     * FALSE : random access
     */
    bool seq_access;
    unsigned long nr_key;
    unsigned long nr_req;   
    /* The key_list can point either private key or public key. */
    struct key_struct **exist_key;
    unsigned long *nr_exist_key;
    struct key_struct **new_key;
    unsigned long *nr_new_key;
};
struct performance_result{
    unsigned long long msec;
    float iops;
    float throughput;
};

struct thread_param{
    struct env_set *env;
    struct per_thread_config *config;
    struct performance_result exclude_flush;
    struct performance_result include_flush;
    int tid;
};

#endif /* KV_BENCH_H */
