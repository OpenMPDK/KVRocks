/*
 * kv_worker.c 
 * Author : Heekwon Park
 * E-mail : heekwon.p@samsung.com
 *
 * Pthread work function
 * Generate key-value pairs based on configuration and send the key-value via kv API. 
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
#include <limits.h>
#include <stdbool.h>
#include <pthread.h>
#include <linux/unistd.h>
#include <sys/syscall.h>

#include "linux_nvme_ioctl.h"
#include "kv_nvme.h"
#include "kv_trace.h"
#include "kv_bench.h"
#include "kv_worker.h"

static inline void fill_value(char *value, int value_size, char *pattern){
    unsigned long count, unit_size, pattern_size;
    pattern_size = strlen(pattern);

    count = value_size;
    do{
        unit_size = ((count > pattern_size) ?  pattern_size : count);
        memcpy(value, pattern, unit_size);
        count -= unit_size;
        value += unit_size;
    }while(count);
}

void *worker_func(void *data){
    struct thread_param *param = (struct thread_param*)data;
    struct env_set *env = param->env;
    struct per_thread_config *config = param->config;
    int tid = param->tid;
    char *value;
    unsigned long i;
    int ret = 0;
    cycles_t s_time, e_time_1, e_time_2;

    unsigned long new_to_exist_cnt = 0;
    unsigned long exist_to_new_cnt = 0;
    unsigned long out_of_keys = 0;
    if(!env->value_size){
        posix_memalign((void **)&value, PAGE_SIZE, MAX_VALUE_SIZE);
        memset(value,0,MAX_VALUE_SIZE);
    }else{
        posix_memalign((void **)&value, PAGE_SIZE, env->value_size);
        memset(value,0,env->value_size);
    }

    if(!env->unique_value){
        fill_value(value, env->value_size?env->value_size:MAX_VALUE_SIZE, "x");
    }
    srand(time(NULL));
    s_time =  get_cycles();
    for( i = 0 ; i < config->nr_req ; i++){
        int cmd = -1;
        int io_type = -1;
        struct key_struct *key;
        unsigned long key_id = -1;
        enum is_update update=NR_UPDATE_TYPE;
        /* Command setup */
        if(config->cmd_ratio[PUT] == 100){
            cmd = PUT;
        }else if(config->cmd_ratio[GET] == 100){
            cmd = GET;
        }else if(config->cmd_ratio[DEL] == 100){
            cmd = DEL;
        }else{
            cmd = rand() % 100;
            if(cmd < config->cmd_ratio[PUT]) cmd = PUT;
            else if(cmd < config->cmd_ratio[PUT]+config->cmd_ratio[GET]) cmd = GET;
            else cmd = DEL;
        }

        /*IO type setup */
        switch(config->io_ratio[ASYNC]){
            case 100:
                io_type = ASYNC;
                break;
            case 0:
                io_type = SYNC;
                break;
            default:
                io_type = rand() % 100;
                if(io_type < config->io_ratio[ASYNC]) io_type = ASYNC;
                else io_type = SYNC;
        }

        if(!config->use_private_key) pthread_spin_lock(&env->lock); 
        /* Get key list and the number of keys in the list */
        switch(cmd){
            case PUT:
                /* If new key does not exist, select a key on exist key list*/
                if(((config->seq_access? 0 : rand() % config->nr_key) < *config->nr_new_key) /*|| !(*config->nr_exist_key)*/){
                    if(config->seq_access)  key_id = (i-exist_to_new_cnt) % (*config->nr_new_key);
                    else                    key_id = rand() % (*(config->nr_new_key));
                    key = config->new_key[key_id];
                    update = NEW;
                    break;
                }
            case GET:
            case DEL:
                /* A key should be exist in exist key list*/
                if(*config->nr_exist_key){
                    if(config->seq_access)  key_id = (i-new_to_exist_cnt) % (*(config->nr_exist_key));
                    else                    key_id = rand() % (*(config->nr_exist_key));
                    key = config->exist_key[key_id];
                    update = UPDATE;
                    break;
                }else{
                    //LOG("Out of the exist keys(%ld)\n",i);
                    out_of_keys++;
                    if(!config->use_private_key) pthread_spin_unlock(&env->lock); 
                    continue;
                }
            default :
                ERRLOG("[Wrong CMD or out of the key range] tid : %d\n", tid);
        }

        if(cmd == PUT){
            char pattern[MAX_KEY_SIZE*2];
            key->written_cnt++;
            key->value_size = env->value_size?env->value_size:(rand()&(~(0x1000-1)))%MAX_VALUE_SIZE;
            if(update == NEW){
                config->exist_key[(*config->nr_exist_key)++] = key;
                config->new_key[key_id] = config->new_key[--(*config->nr_new_key)];
                new_to_exist_cnt++;
            }
            if(env->unique_value) sprintf(pattern,"%s%d", key->key, key->written_cnt);
            if(env->unique_value) fill_value(value, key->value_size, pattern);
        }else if(cmd == DEL){
            key->written_cnt = 0;
            key->value_size = 0;
            config->new_key[(*config->nr_new_key)++] = key;
            config->exist_key[key_id] = config->exist_key[--(*config->nr_exist_key)];
            exist_to_new_cnt++;
        }
        if(!config->use_private_key) pthread_spin_unlock(&env->lock); 

        switch(cmd){
            case PUT:
                ret = nvme_kv_store(env->fd, env->nsid, key->key, key->key_size, value, key->value_size, tid/*offset*/, STORE_OPTION_NOTHING/*non idempotent*/, io_type);/* true = async , false = sync */ 
                break;
            case GET:
                ret = nvme_kv_retrieve(env->fd, env->nsid, key->key, key->key_size, value, key->value_size, tid/*offset*/, RETRIEVE_OPTION_NOTHING, io_type);/* true = async , false = sync */ 
                break;
            case DEL:
                ret = nvme_kv_delete(env->fd, env->nsid, key->key, key->key_size, DELETE_OPTION_NOTHING, io_type);/* true = async , false = sync */ 
                break;
            default :
                ERRLOG("[Wrong CMD] tid : %d\n", tid);
        }
    }


    e_time_1 =  get_cycles();
    nvme_kv_flush(env->fd, env->nsid);
    e_time_2 =  get_cycles();
    sleep(1);
    nvme_kv_discard_readahead(env->fd, env->nsid);
    printf("Fail |%ld |times |because |no |more |key |exists\n", out_of_keys);
    if(ret)
        LOG("[Elapsed time(error)] before flush : %lld msec || after flush : %lld msec\n", time_msec_measure(s_time, e_time_1), time_msec_measure(s_time, e_time_2));
    param->exclude_flush.msec = time_msec_measure(s_time, e_time_1);
    param->include_flush.msec = time_msec_measure(s_time, e_time_2);
    param->exclude_flush.throughput = ((double)config->nr_req * ((double)env->value_size)/1024.0/1024.0)/((double)time_msec_measure(s_time, e_time_1)/1000.0);
    param->include_flush.throughput = ((double)config->nr_req * ((double)env->value_size/1024.0/1024.0))/((double)time_msec_measure(s_time, e_time_2)/1000.0);
    param->exclude_flush.iops = ((double)config->nr_req)/(time_msec_measure(s_time, e_time_1)/1000.0)/1000.0;
    param->include_flush.iops = ((double)config->nr_req)/(time_msec_measure(s_time, e_time_2)/1000.0)/1000.0;
    return NULL;

}

#if 0
    LOG("[Elapsed time] before flush : %lld msec || after flush : %lld msec\n", time_msec_measure(s_time, e_time_1), time_msec_measure(s_time, e_time_2));
    LOG("[%ldkb value Performance] before flush : %.2f MB/s|| after flush : %.2f MB/s\n",  env->value_size/1024, ((double)env->nr_request * ((double)env->value_size)/1024.0/1024.0)/((double)time_msec_measure(s_time, e_time_1)/1000.0) , ((double)env->nr_request * (env->value_size/1024.0/1024.0))/((double)time_msec_measure(s_time, e_time_2)/1000.0));
    LOG("[%ldkb value Performance] before flush : %.2f IOPS|| after flush : %.2f IOPS\n",  env->value_size/1024,  ((double)env->nr_request)/(time_msec_measure(s_time, e_time_1)/1000.0) , ((double)env->nr_request)/(time_msec_measure(s_time, e_time_2)/1000.0));
    LOG("[%ldkb value Performance] before flush : %.2fK IOPS|| after flush : %.2fK IOPS\n",  env->value_size/1024,  ((double)env->nr_request)/(time_msec_measure(s_time, e_time_1)/1000.0)/1024.0 , ((double)env->nr_request)/(time_msec_measure(s_time, e_time_2)/1000.0)/1024.0);
#endif

