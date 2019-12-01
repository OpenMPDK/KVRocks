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

#ifndef _INSDB_GLOBAL_STATS_
#define _INSDB_GLOBAL_STATS_
#include <atomic>
namespace insdb {

//#define MEASURE_WAF


#ifdef INSDB_GLOBAL_STATS
    extern std::atomic<uint64_t> g_new_ikey_cnt;
    extern std::atomic<uint64_t> g_del_ikey_cnt;
    extern std::atomic<uint64_t> g_new_uservalue_io_cnt;
    extern std::atomic<uint64_t> g_del_uservalue_io_cnt;
    extern std::atomic<uint64_t> g_new_uservalue_cache_cnt;
    extern std::atomic<uint64_t> g_del_uservalue_cache_cnt;
    extern std::atomic<uint64_t> g_new_ukey_cnt;
    extern std::atomic<uint64_t> g_del_ukey_cnt;
    extern std::atomic<uint64_t> g_new_col_cnt;
    extern std::atomic<uint64_t> g_del_col_cnt;
    extern std::atomic<uint64_t> g_new_skt_cnt;
    extern std::atomic<uint64_t> g_del_skt_cnt;
    extern std::atomic<uint64_t> g_new_keymap_cnt;
    extern std::atomic<uint64_t> g_del_keymap_cnt;
    extern std::atomic<uint64_t> g_new_arena_cnt;
    extern std::atomic<uint64_t> g_del_arena_cnt;
    extern std::atomic<uint64_t> g_new_snap_cnt;
    extern std::atomic<uint64_t> g_del_snap_cnt;
    extern std::atomic<uint64_t> g_new_itr_cnt;
    extern std::atomic<uint64_t> g_del_itr_cnt;
    extern std::atomic<uint64_t> g_pinslice_uv_cnt;
    extern std::atomic<uint64_t> g_unpinslice_uv_cnt;
    extern std::atomic<uint64_t> g_pin_uv_cnt;
    extern std::atomic<uint64_t> g_unpin_uv_cnt;
    extern std::atomic<uint64_t> g_new_slnode_cnt;
    extern std::atomic<uint64_t> g_del_slnode_cnt;
    extern std::atomic<uint64_t> g_api_merge_cnt;
    extern std::atomic<uint64_t> g_api_put_cnt;
    extern std::atomic<uint64_t> g_api_delete_cnt;
    extern std::atomic<uint64_t> g_api_get_cnt;
    extern std::atomic<uint64_t> g_api_itr_seek_cnt;
    extern std::atomic<uint64_t> g_api_itr_next_cnt;
    extern std::atomic<uint64_t> g_api_itr_key_cnt;
    extern std::atomic<uint64_t> g_api_itr_value_cnt;
    extern std::atomic<uint64_t> g_api_itr_value_ikey_cnt;
    extern std::atomic<uint64_t> g_api_itr_value_dup_cnt;
    extern std::atomic<uint64_t> g_findikey_in_invalidlist_cnt;
    extern std::atomic<uint64_t> g_findikey_loop_in_invalidlist_cnt;
    extern std::atomic<uint64_t> g_api_itr_move_nullikey_cnt;
    extern std::atomic<uint64_t> g_api_write_ttl_cnt;
    extern std::atomic<uint64_t> g_skiplist_newnode_retry_cnt;
    extern std::atomic<uint64_t> g_skiplist_nextwithnode_retry_cnt;
    extern std::atomic<uint64_t> g_skiplist_prevwithnode_retry_cnt;
    extern std::atomic<uint64_t> g_skiplist_estimate_retry_cnt;
    extern std::atomic<uint64_t> g_skiplist_find_prevandsucess_retry_cnt;
    extern std::atomic<uint64_t> g_skiplist_insert_retry_cnt;
    extern std::atomic<uint64_t> g_skiplist_search_retry_cnt;
    extern std::atomic<uint64_t> g_skiplist_searchlessthanequal_retry_cnt;
    extern std::atomic<uint64_t> g_skiplist_searchgreaterequal_retry_cnt;
    void printGlobalStats(const char *header);
#endif

#ifdef TRACE_READ_IO
    extern std::atomic<uint64_t> g_sktable_sync;
    extern std::atomic<uint64_t> g_sktable_sync_hit;
    extern std::atomic<uint64_t> g_sktable_async;
    extern std::atomic<uint64_t> g_kb_sync;
    extern std::atomic<uint64_t> g_kb_sync_hit;
    extern std::atomic<uint64_t> g_kb_async;
    extern std::atomic<uint64_t> g_kbm_sync;
    extern std::atomic<uint64_t> g_kbm_sync_hit;
    extern std::atomic<uint64_t> g_kbm_async;
    extern std::atomic<uint64_t> g_itr_kb;
    extern std::atomic<uint64_t> g_itr_kb_prefetched;
    extern std::atomic<uint64_t> g_loadvalue_column;
    extern std::atomic<uint64_t> g_loadvalue_sktdf;
#endif

#ifdef MEASURE_WAF
    extern std::atomic<uint64_t> g_acc_appwrite_keyvalue_size;
    extern std::atomic<uint64_t> g_acc_devwrite_keyvalue_size;
#endif

#ifdef TRACE_MEM
    extern std::atomic<uint64_t> g_unloaded_skt_cnt;
    extern std::atomic<uint64_t> g_io_cnt;
    extern std::atomic<uint64_t> g_evicted_key;
    extern std::atomic<uint64_t> g_evicted_skt;
    extern std::atomic<uint64_t> g_evict_dirty_canceled;
    extern std::atomic<uint64_t> g_evict_skt_busy_canceled;
    extern std::atomic<uint64_t> g_evict_skt_next_deleted_canceled;
    extern std::atomic<uint64_t> g_prefetch_after_evict;
    extern std::atomic<uint64_t> g_evict_call_cnt;
    extern std::atomic<uint64_t> g_skt_loading;
    extern std::atomic<uint64_t> g_skt_flushing;
    extern std::atomic<uint64_t> g_hit_df_skt;
    extern std::atomic<uint64_t> g_miss_df_skt;
    extern std::atomic<uint64_t> g_insert_df_skt;
    extern std::atomic<uint64_t> g_evict_df_skt;
#ifdef KV_TIME_MEASURE
    extern std::atomic<uint64_t> g_flush_cum_time;
    extern std::atomic<uint64_t> g_flush_start_count;
    extern std::atomic<uint64_t> g_flush_end_count;
    extern std::atomic<uint64_t> g_flush_max_time;
    extern std::atomic<uint64_t> g_evict_cum_time;
    extern std::atomic<uint64_t> g_evict_count;
    extern std::atomic<uint64_t> g_evict_max_time;
    extern std::atomic<uint64_t> g_write_io_flush_cum_time;
    extern std::atomic<uint64_t> g_write_io_flush_count;
    extern std::atomic<uint64_t> g_write_io_flush_max_time;
    extern std::atomic<uint64_t> g_skt_flush_cum_time;
    extern std::atomic<uint64_t> g_skt_flush_count;
    extern std::atomic<uint64_t> g_skt_flush_max_time;
#endif
#endif
}

#endif
