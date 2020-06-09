/*
 * Original Code Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of the original source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 *
 * Modifications made 2019
 * Modifications Copyright (c) 2019, Samsung Electronics.
 *
 * Architect    : Heekwon Park(heekwon.p@samsung.com), Yangseok Ki(yangseok.ki@samsung.com)
 * Authors      : Heekwon Park, Ilgu Hong, Hobin Lee
 *
 * This modified version is distributed under a BSD-style license that can be
 * found in the LICENSE.insdb file  
 *                    
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  
 */


#include <sched.h>
#include <stdbool.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <deque>
#include <limits>
#include <set>
#include <vector>
#include "insdb/env.h"
#include "insdb/slice.h"
#include "insdb/kv_trace.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"
/**
  Definition for KV SSD linux kernel driver
  */
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include "util/linux_nvme_ioctl.h"
namespace insdb {

namespace {

#define EMBED_KEYSIZE  16
static const size_t kBufSize = 65536;

static Status PosixError(const std::string& context, int err_number) {
  if (err_number == ENOENT) {
    return Status::NotFound(context, strerror(err_number));
  } else {
    return Status::IOError(context, strerror(err_number));
  }
}

class PosixEnv : public Env {
 public:
  PosixEnv();
  virtual ~PosixEnv() {
    char msg[] = "Destroying Env::Default()\n";
    fwrite(msg, 1, sizeof(msg), stderr);
    for (const auto tid : threads_to_join_) {
      pthread_join(tid, nullptr);
    }
    abort();
  }


  virtual Status Open(std::vector<std::string>& kv_ssd_name) {
    Status s;
    if (kv_fd_.size() == 0) {
        for (auto kv_ssd : kv_ssd_name) {
            int kv_fd = open(kv_ssd.c_str(), O_RDWR| O_CREAT, 0666);
            if (kv_fd < 0) {
                s = PosixError(kv_ssd, errno);
                break;
            } else {
                // store FD
                kv_fd_.push_back(kv_fd);

                int kv_ns_id = ioctl(kv_fd, NVME_IOCTL_ID);
                if (kv_ns_id < 0 ) {
                    s = PosixError("fail to get name space for " + kv_ssd, errno);
                    break;
                }
                // store NSID
                kv_ns_id_.push_back(kv_ns_id);
            }
        }
        if (!s.ok()) {
            for (auto kv_fd : kv_fd_)
                close (kv_fd);
        }
    }
    return s;
  }

  virtual void Close() {
      for (auto kv_fd : kv_fd_)
          close (kv_fd);
      kv_fd_.clear();
      kv_ns_id_.clear();
  }

  virtual bool AcquireDBLock(const uint16_t db_name) {
    /**
    TODO: Need to Implement locking mechanism and API.
      */
    return true;
  }
  virtual void ReleaseDBLock(const uint16_t db_name) {
    /**
    TODO: Need to Implement unlocking mechanism and API.
      */
    return;
  }

#ifdef INSDB_IO_TRACE

#define INSDBKEY_STRING_FORMAT "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
#define INSDBKEY_BYTE_ARG(key) ((uint8_t*)&(key))[0],((uint8_t*)&(key))[1],((uint8_t*)&(key))[2],((uint8_t*)&(key))[3],((uint8_t*)&(key))[4],((uint8_t*)&(key))[5],((uint8_t*)&(key))[6],((uint8_t*)&(key))[7], \
        ((uint8_t*)&(key))[8],((uint8_t*)&(key))[9],((uint8_t*)&(key))[10],((uint8_t*)&(key))[11],((uint8_t*)&(key))[12],((uint8_t*)&(key))[13],((uint8_t*)&(key))[14],((uint8_t*)&(key))[15]

#endif

  /**
    Send (A)Synchronous Put command.
    */
  virtual Status Put(InSDBKey key, const Slice& value, bool async) {
#ifdef KV_TIME_MEASURE
      cycles_t s_t, e_t;
      s_t = get_cycles();
#endif
#if 0
      async = false;
#endif

#ifdef MEASURE_WAF
      g_acc_devwrite_keyvalue_size += value.size() + EMBED_KEYSIZE;
#if 0
          printf("app write %lu dev write %lu\n", g_acc_appwrite_keyvalue_size.load(std::memory_order_relaxed), g_acc_devwrite_keyvalue_size.load(std::memory_order_relaxed));
#endif
#endif

    if (value.size() > kDeviceRquestMaxSize) abort();
    // kernel driver does not support 1 byte alignment
    if (value.size() & 0x3) abort();


    Status s;
    int ret = 0;
    int dev_idx = GetDevIdx(key);
    key = EncodeInSDBKey(key);
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_store;
    cmd.nsid = kv_ns_id_[dev_idx];
    cmd.cdw5 = 0;
    cmd.data_addr = (__u64)value.data();
    cmd.data_length = (__u32)value.size();
    cmd.key_length = EMBED_KEYSIZE;
    memcpy(cmd.key, (char *)&key, EMBED_KEYSIZE);
    cmd.cdw11 = EMBED_KEYSIZE -1;
    cmd.cdw10 = ((__u32)value.size() >> 2);
    if(async)
        ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_ASYNC_CMD, &cmd);
    else
        ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_SYNC_CMD, &cmd);
#ifdef INSDB_IO_TRACE
      printf("PUT " INSDBKEY_STRING_FORMAT " async %u result %d status 0x%x ret %d\n", INSDBKEY_BYTE_ARG(key), async, cmd.result, cmd.status, ret);
#endif
    if (ret) {
        s = PosixError("fail to Put", errno);
    }
    return s;
  }


  /**
   * Send Flush Single/All command
   * if key is valid, it is single flush
   */
  virtual Status Flush(FlushType type, int dev_idx, InSDBKey key) {
#if 0
//    usleep(1000);
    return Status::OK();
#endif
    Status s;
    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = type;
    if(key.first || key.second){
        key = EncodeInSDBKey(key);
        cmd.key_length = EMBED_KEYSIZE;
        memcpy(cmd.key, (char *)&key, EMBED_KEYSIZE);
        cmd.cdw11 = EMBED_KEYSIZE -1;
        // TODO: find target key for each device
    }
    if (dev_idx == -1) {
        for(auto kv_fd : kv_fd_) {
            ret = ioctl(kv_fd, NVME_IOCTL_KV_FLUSH_CMD, &cmd);
            if (ret)
                break;
        }
    } else {
        ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_FLUSH_CMD, &cmd);
    }
#ifdef INSDB_IO_TRACE
      // note that FLUSH is not delivered to device. It flushes IO buffers in the driver.
      printf("FLUSH " INSDBKEY_STRING_FORMAT " type %u result %d status 0x%x ret %d\n", INSDBKEY_BYTE_ARG(key), type, cmd.result, cmd.status, ret);
#endif
    if (ret) {
        s = PosixError("fail to Flush", errno);
    }
    return s;
  }
  
  /**
    Send Synchronous Get command.
    */
  virtual Status Get(InSDBKey key, char* buf, int *size, GetType type, uint8_t flags, bool *from_readahead) {
#if 0
    if (readahead) {
        return Status::OK();
    }
#endif

    Status s;
    int ret = 0;
    int dev_idx = GetDevIdx(key);
    key = EncodeInSDBKey(key);
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_retrieve;
    cmd.nsid = kv_ns_id_[dev_idx];
    cmd.cdw5 = 0;
    if(type != kReadahead)
        cmd.data_addr = (__u64)buf;
    /*
    if(*size < 0x1000 && *size > 0 ){
        printf("size : %d\n", *size);
        port::Crash(__FILE__,__LINE__);
    }
    */
    // kernel driver does not support 1 byte alignment
    if (*size & 0x3) abort();

    cmd.data_length = (__u32)*size;
    cmd.key_length = EMBED_KEYSIZE;
    memcpy(cmd.key, (char *)&key, EMBED_KEYSIZE);
    cmd.cdw10 = ((__u32)*size >> 2);
    cmd.cdw11 = EMBED_KEYSIZE -1;
    // Set flags
    cmd.cdw11 |= (__u32)flags << 8;

    switch(type){
        case kSyncGet:
#if 0
            cpu_set_t org_mask, mask;
            CPU_ZERO(&org_mask);
            CPU_ZERO(&mask);
            if (sched_getaffinity(0, sizeof(cpu_set_t), &org_mask) == -1) {
                perror("sched_getaffinity");
                assert(false);
            }
            CPU_ZERO(&mask);
            CPU_SET(0, &mask);
            if (sched_setaffinity(0, sizeof(cpu_set_t), &mask) == -1) {
                perror("sched_setaffinity");
                assert(false);
            }

            ret = ioctl(kv_fd, NVME_IOCTL_KV_SYNC_CMD, &cmd);
            if (sched_setaffinity(0, sizeof(cpu_set_t), &org_mask) == -1) {
                perror("sched_setaffinity");
                assert(false);
            }
#endif
            ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_SYNC_CMD, &cmd);
            break;
        case kReadahead:
            ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_ASYNC_CMD, &cmd);
            break;
        case kNonblockingRead:
            ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_NONBLOCKING_READ_CMD, &cmd);
            break;
        default :
            printf("Command is not supported!!");
                abort();
    }

#ifdef INSDB_IO_TRACE
      printf("GET " INSDBKEY_STRING_FORMAT " t %u result %d status 0x%x ret %d\n", INSDBKEY_BYTE_ARG(key), type, cmd.result, cmd.status, ret);
#endif

      if (ret) {
#ifdef CODE_TRACE
          printf("[%d :: %s] Read Fail\n", __LINE__, __func__);
#endif
          s = PosixError("fail to Get", errno);
      }else if(type != kReadahead){
          *size = cmd.result;
          if (cmd.status == KVS_ERR_EXIST_IN_RA_HASH){
              if (from_readahead) *from_readahead = true;
          } else if (cmd.status == KVS_ERR_KEY_NOT_EXIST) {
              s = Status::NotFound("key does not exist");
          }
      }
      return s;

  }

#define MAX_MULTICMDS_IN_BATCH  100
  

struct dev_buffer {
    __u8 *data;
    struct nvme_passthru_kv_cmd *cmd;
    uint32_t cmdcnt;
    std::vector<int> cmd_idx;
};

  virtual Status MultiCmdOneOpcode(uint8_t opcode, std::vector<InSDBKey> &key, std::vector<char*> &buf, std::vector<int> &size, InSDBKey blocking_key, bool async, uint8_t flags, std::vector<DevStatus> &status) {
    Status s;
    int ret = 0;
#ifdef CODE_TRACE
    if(key.size()>MAX_MULTICMDS_IN_BATCH) printf("%s:%d cmds 0x%x %u\n",__func__, __LINE__, opcode, key.size());
#endif
    /*
     * Command format
     * -----------------------------------
     * | size(4B) | Blocking Key(16B) | cmd | cmd | ... | cmd|
     * -----------------------------------
     *  size : The number of cmd
     *  Blocking Key : If a blocking key exists, the cmds should be submitted after the blocking key has been submitted.
     */
    int max_cmds = MAX_MULTICMDS_IN_BATCH<=key.size()?MAX_MULTICMDS_IN_BATCH:key.size();
    size_t dev_batch_size = sizeof(uint32_t) + EMBED_KEYSIZE + (sizeof(struct nvme_passthru_kv_cmd) * max_cmds);
    __u8 *data_buf = (__u8 *)calloc(kv_fd_.size(), dev_batch_size);
    dev_buffer dev_buf[kv_fd_.size()];
    for (int i = 0 ; i < kv_fd_.size(); i++) {
        dev_buf[i].data = data_buf + i * dev_batch_size;
        memcpy((dev_buf[i].data + 4), (char *)&blocking_key, EMBED_KEYSIZE);
        dev_buf[i].cmd = (struct nvme_passthru_kv_cmd *)(dev_buf[i].data + 4 + EMBED_KEYSIZE);
        dev_buf[i].cmdcnt=0;
    }

    status.reserve(key.size());
    for(int i = 0 ; i < key.size() ; i++){
        int dev_idx = GetDevIdx(key[i]);
        key[i] = EncodeInSDBKey(key[i]);
        dev_buf[dev_idx].cmd_idx.push_back(i);
        dev_buf[dev_idx].cmd->opcode = opcode;
        dev_buf[dev_idx].cmd->nsid = kv_ns_id_[dev_idx];
        dev_buf[dev_idx].cmd->cdw5 = 0;
        if(i < buf.size())
            dev_buf[dev_idx].cmd->data_addr = (__u64)buf[i];
        else
            dev_buf[dev_idx].cmd->data_addr = 0;
        if(i < size.size())
            dev_buf[dev_idx].cmd->data_length = (__u32)size[i];
        else
            dev_buf[dev_idx].cmd->data_length = 0;
        dev_buf[dev_idx].cmd->key_length = EMBED_KEYSIZE;
        memcpy(dev_buf[dev_idx].cmd->key, (char *)&key[i], EMBED_KEYSIZE);
        dev_buf[dev_idx].cmd->cdw11 = EMBED_KEYSIZE -1;
#ifdef MEASURE_WAF
      if (opcode == nvme_cmd_kv_store) {
          g_acc_devwrite_keyvalue_size += size[i] + EMBED_KEYSIZE;
#if 0
          printf("app write %lu dev write %lu\n", g_acc_appwrite_keyvalue_size.load(std::memory_order_relaxed), g_acc_devwrite_keyvalue_size.load(std::memory_order_relaxed));
#endif
      }
#endif
        // Set flags
        if (opcode == nvme_cmd_kv_retrieve)
            dev_buf[dev_idx].cmd->cdw11 |= ((__u32)flags<<8);
        // Buffer size check
        if ( opcode == nvme_cmd_kv_store && size[i] > kDeviceRquestMaxSize) abort();

        if(i < size.size()) {
            dev_buf[dev_idx].cmd->cdw10 = ((__u32)size[i] >> 2);
            // kernel driver does not support 1 byte alignment
            if (size[i] & 0x3) abort();
        } else
            dev_buf[dev_idx].cmd->cdw10 = 0;

        // increase command count
        dev_buf[dev_idx].cmdcnt++;
        dev_buf[dev_idx].cmd++;

        // flush one batch if full or no more command
        if(i + 1 == key.size() || dev_buf[dev_idx].cmdcnt == MAX_MULTICMDS_IN_BATCH) {
            for (int flush_dev_idx = 0; flush_dev_idx < kv_fd_.size() ; flush_dev_idx++) {
                if (dev_buf[flush_dev_idx].cmdcnt == 0) continue;
                // reset command pointer
                dev_buf[flush_dev_idx].cmd = (struct nvme_passthru_kv_cmd *)(dev_buf[flush_dev_idx].data + 4 + EMBED_KEYSIZE);
                // nr commands
                *((uint32_t *)dev_buf[flush_dev_idx].data) = dev_buf[flush_dev_idx].cmdcnt;
                // send down the batch in sync or async
                if(async)
                    ret = ioctl(kv_fd_[flush_dev_idx], NVME_IOCTL_KV_MULTIASYNC_CMD, dev_buf[flush_dev_idx].data);
                else
                    ret = ioctl(kv_fd_[flush_dev_idx], NVME_IOCTL_KV_MULTISYNC_CMD, dev_buf[flush_dev_idx].data);
                if (ret) {
                    s = PosixError("fail to MultiCmdOneOpcode", errno);
                    goto error_out;
                } else {
            #ifdef INSDB_IO_TRACE
                    printf("MCMD%p Op 0x%x nr %lu blocking " INSDBKEY_STRING_FORMAT " ret %d\n", &key, opcode, key.size(), INSDBKEY_BYTE_ARG(blocking_key), ret);
            #endif
                    for(int r = 0; r < dev_buf[flush_dev_idx].cmdcnt; r++) {
            #ifdef INSDB_IO_TRACE
                  // note that FLUSH is not delivered to device. It flushes IO buffers in the driver.
                  printf("MCMD %p #%d " INSDBKEY_STRING_FORMAT " result %d status 0x%x\n", &key, i, INSDBKEY_BYTE_ARG(key[first_cmd_idx+r]), cmd[r].result, cmd[r].status);
            #endif
                        int cmd_idx = dev_buf[flush_dev_idx].cmd_idx[r];
                        if(cmd_idx < size.size())
                            size[cmd_idx] = dev_buf[flush_dev_idx].cmd[r].result;
                        switch(dev_buf[flush_dev_idx].cmd[r].status) {
                        case 0:
                            status[cmd_idx] = Success;
                            break;
                        case KVS_ERR_KEY_NOT_EXIST:
                            status[cmd_idx] = KeyNotExist;
                            break;
                        case KVS_ERR_EXIST_IN_RA_HASH:
                            status[cmd_idx] = ReadaheadHit;
                            break;
                        default:
                            status[cmd_idx] = Unknown;
                            break;
                        }
                    }
                }
                // reset device buffer context
                dev_buf[flush_dev_idx].cmdcnt = 0;
                dev_buf[flush_dev_idx].cmd_idx.clear();
            }
        }
    }
error_out:
    free(data_buf);
    return s;
  }

  virtual Status MultiPut(std::vector<InSDBKey> &key, std::vector<char*> &buf, std::vector<int> &size, InSDBKey blocking_key, bool async) {
    std::vector<DevStatus> status;
    return MultiCmdOneOpcode(nvme_cmd_kv_store, key, buf, size, blocking_key, async, 0, status);
  }

  virtual Status MultiGet(std::vector<InSDBKey> &key, std::vector<char*> &buf, std::vector<int> &size, InSDBKey blocking_key, bool readahead, uint8_t flags, std::vector<DevStatus> &status) {
    return MultiCmdOneOpcode(nvme_cmd_kv_retrieve, key, buf, size, blocking_key, readahead, flags, status);
  }

  virtual Status MultiDel(std::vector<InSDBKey> &key, InSDBKey blocking_key, bool async, std::vector<DevStatus> &status) {
      std::vector<int> empty_size;
      std::vector<char*> empty_buf;
    return MultiCmdOneOpcode(nvme_cmd_kv_delete, key, empty_buf, empty_size, blocking_key, async, 0, status);
  }


  /**
    Send Discard Single or All commands
    */
  virtual Status DiscardPrefetch(InSDBKey key) {
    Status s;
    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    if(key.first || key.second){
        int dev_idx = GetDevIdx(key);
        key = EncodeInSDBKey(key);
        cmd.key_length = EMBED_KEYSIZE;
        memcpy(cmd.key, (char *)&key, EMBED_KEYSIZE);
        cmd.cdw11 = EMBED_KEYSIZE -1;
        ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_DISCARD_RA_CMD, &cmd);
        if (ret) {
            s = PosixError("fail to Discard", errno);
        }
    } else {
        // clear all readahead buffers
        for (int dev_idx = 0; dev_idx < kv_fd_.size() ; dev_idx++) {
            ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_DISCARD_RA_CMD, &cmd);
            if (ret) {
                s = PosixError("fail to Discard", errno);
                break;
            }
        }
    }

    return s;
  }

  /**
    Delete key.
    */
  virtual Status Delete(InSDBKey key, bool async) {
#if 0
    async = false;
#endif
    Status s;
    int ret = 0;
    int dev_idx = GetDevIdx(key);
    key = EncodeInSDBKey(key);
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_delete;
    cmd.nsid = kv_ns_id_[dev_idx];
    cmd.cdw4 = 1;
    //cmd.cdw5 = 0;
    cmd.key_length = EMBED_KEYSIZE;
    memcpy(cmd.key, (char *)&key, EMBED_KEYSIZE);
    cmd.cdw11 = EMBED_KEYSIZE -1;
    if(async)
        ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_ASYNC_CMD, &cmd);
    else
        ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_SYNC_CMD, &cmd);
#ifdef INSDB_IO_TRACE
      printf("DEL " INSDBKEY_STRING_FORMAT " async %u result %d status 0x%x ret %d\n", INSDBKEY_BYTE_ARG(key), async, cmd.result, cmd.status, ret);
#endif
    if (ret) {
        s = PosixError("fail to delete", errno);
    }
    return s;
  }



  /**
    Check whether key exist or not.
    TODO:
    It maybe need to change as original exist command when firmware
    fully support exist/iterate.

    It's not clear whether current firmware support exist/iterate or not.
    So it uses retrieve command temporary.
    */
  virtual bool KeyExist(InSDBKey key) {
    int ret = 0;
    int dev_idx = GetDevIdx(key);
    key = EncodeInSDBKey(key);
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_retrieve;
    cmd.nsid = kv_ns_id_[dev_idx];
    cmd.cdw5 = 0;
    cmd.data_length = (__u32)0;
    cmd.key_length = EMBED_KEYSIZE;
    memcpy(cmd.key, (char *)&key, EMBED_KEYSIZE);
    cmd.cdw11 = EMBED_KEYSIZE -1;
    cmd.cdw10 = 0;
    ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_SYNC_CMD, &cmd);
    if (ret) {
        return false;
    }
    return true;
  }


  virtual Status IterReq(uint32_t mask, uint32_t iter_val, unsigned char *iter_handle, int dev_idx, bool open = false, bool rm = false) {
    Status s;
    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_iter_req;
    cmd.nsid = kv_ns_id_[dev_idx];
    if (rm) {
        cmd.cdw4 = 0x01 | 0x10;
    } else if (open) {
        cmd.cdw4 = 0x01 | 0x04;
    } else {
        cmd.cdw4 = 0x02;
        cmd.cdw5 = *iter_handle;
    }
    cmd.cdw12 = iter_val;
    cmd.cdw13 = mask;
    ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_SYNC_CMD, &cmd);
#ifdef INSDB_IO_TRACE
      printf("ITRREQ mask 0x%x itr_val 0x%x open %u rm %u result %d status 0x%x ret %d\n", mask, iter_val, open, rm, cmd.result, cmd.status, ret);
#endif
    if (ret) {
        s = PosixError("fail to IterReq", errno);
    }
    if (open) {
      *iter_handle = cmd.result & 0xff;
    }
    return s;
  }

  virtual Status IterRead(char* buf, int *size, unsigned char iter_handle, int dev_idx) {
    Status s;
    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_iter_read;
    cmd.nsid = kv_ns_id_[dev_idx];
    cmd.cdw5 = iter_handle;
    cmd.data_addr = (__u64)buf;
    cmd.data_length = (__u32)*size;
    cmd.cdw10 = ((__u32)*size >> 2);
    ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_KV_SYNC_CMD, &cmd);
#ifdef INSDB_IO_TRACE
      printf("ITRREAD 0x%x result %d status 0x%x ret %d\n", iter_handle, cmd.result, cmd.status, ret);
#endif
    *size = cmd.result & 0xffff;
    if (((ret & 0xffff) == 0x0393) && *size) {
        ret = 0;
    }
    if (ret) {
        s = PosixError("fail to IterRead", errno);
    }
    return s;
  }

  virtual Status GetLog(char pagecode, char* buf, int bufflen, int dev_idx) {
    Status s;
    int ret = 0;
    struct nvme_passthru_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_cmd));
    cmd.opcode = 0x02;
    cmd.nsid = 0xffffffff;
    cmd.cdw10 = (__u32)(((bufflen >> 2) -1) << 16) | ((__u32)pagecode & 0x000000ff);
    cmd.addr = (__u64)buf;
    cmd.data_len = (__u32)bufflen;
    ret = ioctl(kv_fd_[dev_idx], NVME_IOCTL_ADMIN_CMD, &cmd);
#ifdef INSDB_IO_TRACE
      printf("GETLOG 0x%x result %d ret %d\n", pagecode, cmd.result, ret);
#endif
    if (ret) {
        s = PosixError("fail to GetLog", errno);
    }
    return s;
  }

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual uint64_t StartThread(void (*function)(void* arg), void* arg);

  virtual void ThreadJoin(uint64_t tid) override;

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return PosixError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PosixEnv::gettid);
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual uint64_t NowSecond() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec);
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      abort();
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;

  int GetDevIdx(InSDBKey key) { return key.seq%kv_ns_id_.size(); }

  std::vector<int> kv_fd_;
  std::vector<int> kv_ns_id_;
  std::vector<pthread_t> threads_to_join_;

};


PosixEnv::PosixEnv()
    : started_bgthread_(false),
      kv_fd_(),
      kv_ns_id_() {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

uint64_t PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
    pthread_t t;
    StartThreadState* state = new StartThreadState;
    state->user_function = function;
    state->arg = arg;
    PthreadCall(
        "start thread", pthread_create(&t, nullptr, &StartThreadWrapper, state));
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    return reinterpret_cast<uint64_t>(t);
}

void PosixEnv::ThreadJoin(uint64_t tid) {
    pthread_join(reinterpret_cast<pthread_t>(tid), nullptr);
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

}  // namespace insdb
