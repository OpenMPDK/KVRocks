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


#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/keymap.h"
#include <set>
#include "insdb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/mutexlock.h"
#include "port/port.h"

namespace insdb {
    class KeymapTest { };
    struct COL {
        uint8_t col_size;
        uint8_t col_id;
        uint8_t offset_in_kb;
        char rep_[1];
    } __attribute__((packed)); /* size 23 */

    struct KeyCol{
        uint8_t nr_col;
        COL col[1];
    } __attribute__((packed)); /* size minimum 24 */


    TEST(KeymapTest ,Insert_Replace_Test) {
        char begin_key_buf[32];
        sprintf(begin_key_buf, "%016d", 0);
        KeySlice beginkey(begin_key_buf, 16);

        Keymap table(beginkey, BytewiseComparator());
        Arena* table_arena = new Arena();
        table.InitArena(table_arena);
        table.InsertArena(table_arena);

        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        int key_count = 100;
        uint32_t insert = 0, insert_fail = 0;

        printf( "---------------------------------------\n");
        printf( "write %d key to tabel\n", key_count);

        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            char keyinfo_str[64];
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].col_id = 0;
            keycol->col[0].offset_in_kb =5;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, i); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, i+2); /* ikey sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, 2048); /* iter size */
            offset += (addr2 - addr);
            keycol->col[0].col_size = offset -1;
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            uint32_t est_size = offset;
            KeyMapHandle new_handle;
            Slice keyinfo = table.Insert(key, est_size, new_handle);
            if (keyinfo.size()) {
                if (keyinfo.size() < est_size) abort();
                insert++;
                char* addr = const_cast<char*>(keyinfo.data());
                memcpy(addr, keyinfo_str, est_size); 
            }
            else insert_fail++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "insert result s(%lu) f(%lu) micros(%lu) insert(%d) insert_fail(%d) op_count(%ld) cmp_count(%ld)\n",
                start_time, finish_time, microsec, insert, insert_fail, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "read %d key to tabel\n", key_count);
        KeyMapHandle handle = table.First();
        std::string search_str;
        char key_str[32];
        while(handle) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(handle, 0, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            handle = table.Next(handle);

        }
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "replace %d key to tabel\n", key_count);
        handle = table.First();
        uint64_t start_seq = key_count + 20;
        while(handle) {
            search_str.resize(0);
            table.GetKeyString(handle, search_str);
            if (search_str.size() > 16) abort();
            Slice keyinfo = table.GetKeyInfo(handle);
            if (search_str.size() > 16) abort();
            memcpy(key_str, search_str.data(), search_str.size());
            key_str[search_str.size()] = 0;
            char keyinfo_str[64];
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].offset_in_kb = 0x83;
            keycol->col[0].col_id = 1;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, start_seq); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, start_seq + 10000); /* ttl sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, start_seq + 2); /* ikey sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, 4096); /* iter size */
            offset += (addr - addr2);
            keycol->col[0].col_size = offset -1;
            start_seq++;
            uint32_t est_size = offset;
            /* get next allocate offset */
            uint32_t new_handle = table.GetNextAllocOffset(est_size);
            if (!new_handle) abort();
            /* modify existing keyinfo - relocation */
            if (keyinfo.size() ==  0) abort();
            addr = const_cast<char*>(keyinfo.data());
            addr[0] |= 0x80; /* set relocation */
            *((uint32_t*)(addr + 1)) = new_handle;
            /* allocate memory */
            char* mem = table.AllocateMemory(est_size, new_handle);
            memcpy(mem, keyinfo_str, offset);
            handle = table.Next(handle);

        }
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "read %d key to tabel\n", key_count);
        handle = table.First();
        while(handle) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(handle, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            handle = table.Next(handle);

        }
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "read reverse %d key to tabel\n", key_count);
        handle = table.Last();
        while(handle) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(handle, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            handle = table.Prev(handle);

        }
        table.DumpState();


        char search_key_str[32];
        printf( "---------------------------------------\n");
        printf( "search less than equal on tabel\n");
        for (int i = key_count; i > key_count -10; i--) {
            ColumnData coldata; 
            sprintf(search_key_str, "%016d", i);
            search_key_str[16] = 0;
            KeySlice key(search_key_str, 16);
            handle  = table.SearchLessThanEqual(key); 
            if ( handle && table.GetKeyColumnData(handle, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Searching key(%s), Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        search_key_str, key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
       }

        printf( "---------------------------------------\n");
        printf( "search less on tabel\n");
        for (int i = key_count; i > key_count -10; i--) {
            ColumnData coldata; 
            sprintf(search_key_str, "%016d", i);
            search_key_str[16] = 0;
            KeySlice key(search_key_str, 16);
            handle  = table.SearchLessThan(key); 
            if ( handle && table.GetKeyColumnData(handle, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Searching key(%s), Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        search_key_str, key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
       }

        printf( "---------------------------------------\n");
        printf( "search greater on tabel\n");
        for (int i = 0; i < key_count; i++) {
            ColumnData coldata; 
            sprintf(search_key_str, "%016d", i);
            search_key_str[16] = 0;
            KeySlice key(search_key_str, 16);
            handle  = table.SearchGreater(key); 
            if ( handle && table.GetKeyColumnData(handle, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Searching key(%s), Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        search_key_str, key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
       }

        std::set<uint64_t> KeyList;
        printf( "---------------------------------------\n");
        printf( "Search IkeyList %d key to tabel\n", key_count);
        handle = table.First();
        while(handle) {
            table.GetiKeySeqNum(handle, KeyList);
            handle = table.Next(handle);
        }
        printf( "----------------- [Search ikey seq] ------------------\n");
        uint32_t ikey_count = 0;
        for (auto it = KeyList.begin(); it != KeyList.end(); ++it) {
            ikey_count++;
            if (!(ikey_count % 10)) printf("%ld\n", *it);
            else printf("%ld\t", *it);
        }
        printf( "------------------------------------------------------\n");
    }


    TEST(KeymapTest ,Cleanup_test) {
        char begin_key_buf[32];
        sprintf(begin_key_buf, "%016d", 0);
        KeySlice beginkey(begin_key_buf, 16);

        Keymap table(beginkey, BytewiseComparator());
        Arena* table_arena = new Arena();
        table.InitArena(table_arena);
        table.InsertArena(table_arena);

        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        int key_count = 1000000;
        uint32_t insert = 0, insert_fail = 0;
        int count = 0;

        printf( "---------------------------------------\n");
        printf( "write %d key to tabel\n", key_count);

        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            char keyinfo_str[64];
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].col_id = 0;
            keycol->col[0].offset_in_kb =5;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, i); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, i+2); /* ikey sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, 2048); /* iter size */
            offset += (addr2 - addr);
            keycol->col[0].col_size = offset -1;
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            uint32_t est_size = offset;
            KeyMapHandle new_handle;
            Slice keyinfo = table.Insert(key, est_size, new_handle);
            if (keyinfo.size()) {
                if (keyinfo.size() < est_size) abort();
                insert++;
                char* addr = const_cast<char*>(keyinfo.data());
                memcpy(addr, keyinfo_str, est_size); 
            }
            else insert_fail++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "insert result s(%lu) f(%lu) micros(%lu) insert(%d) insert_fail(%d) op_count(%ld) cmp_count(%ld)\n",
                start_time, finish_time, microsec, insert, insert_fail, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        KeyMapHandle result = 0;
        uint32_t found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  

        printf( "---------------------------------------\n");
        printf( "read 100 key column 1 to tabel\n");
        char key_str[32];
        result = table.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(result, 0, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table.Next(result);
            if (++count > 100) break;
        }

        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "replace %d key to tabel\n", key_count);
        KeyMapHandle handle = table.First();
        uint64_t start_seq = key_count + 20;
        std::string search_str;
        char keyinfo_str[64];
        while(handle) {
            search_str.resize(0);
            table.GetKeyString(handle, search_str);
            if (search_str.size() > 16) abort();
            Slice keyinfo = table.GetKeyInfo(handle);
            if (search_str.size() > 16) abort();
            memcpy(key_str, search_str.data(), search_str.size());
            key_str[search_str.size()] = 0;
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].offset_in_kb = 0x83;
            keycol->col[0].col_id = 1;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, start_seq); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, start_seq + 10000); /* ttl sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, start_seq + 2); /* ikey sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, 4096); /* iter size */
            offset += (addr - addr2);
            keycol->col[0].col_size = offset -1;
            start_seq++;
            uint32_t est_size = offset;
            /* get next allocate offset */
            uint32_t new_handle = table.GetNextAllocOffset(est_size);
            if (!new_handle) abort();
            /* modify existing keyinfo - relocation */
            if (keyinfo.size() ==  0) abort();
            addr = const_cast<char*>(keyinfo.data());
            addr[0] |= 0x80; /* set relocation */
            *((uint32_t*)(addr + 1)) = new_handle;
            /* allocate memory */
            char* mem = table.AllocateMemory(est_size, new_handle);
            memcpy(mem, keyinfo_str, offset);
            handle = table.Next(handle);
        }
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        result = 0;
        found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  

        printf( "---------------------------------------\n");
        printf( "read 100 key column 1 to tabel\n");
        result = table.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(result, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table.Next(result);
            if (++count > 100) break;
        }
        table.DumpState();



        printf( "---------------------------------------\n");
        printf( "Memory Cleanup\n");
        KeyMapHashMap* hashmap = new KeyMapHashMap(1024*32);
        Arena* new_arena = table.MemoryCleanup(nullptr, hashmap);
        table.InsertArena(new_arena, hashmap);
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        result = 0;
        found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result = table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  

        printf( "---------------------------------------\n");
        printf( "read 100 key column 1 to tabel\n");
        result = table.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(result, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table.Next(result);
            if (++count > 100) break;
        }
        table.DumpState();


        printf( "---------------------------------------\n");
        printf( "Copy Arena and Create New table2 - Migration form device emulation\n");
        Arena *arena = new Arena(table.GetBufferSize());
        table.CopyMemory(arena->GetBaseAddr(), table.GetAllocatedSize());
        std::string hash_data;
        table.BuildHashMapData(hash_data);
        hashmap = BuildKeymapHashmapWithString(hash_data);
        Keymap table2(beginkey, BytewiseComparator());
        table2.InsertArena(arena, hashmap);

        printf( "---------------------------------------\n");
        printf( "search %d key to tabel2\n", key_count);
        result = 0;
        found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result = table2.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table2.GetOpCnt(), table2.GetCmpCnt());  

        printf( "---------------------------------------\n");
        printf( "read 100 key column 1 to tabel2\n");
        result = table2.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table2.GetKeyColumnData(result, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table.Next(result);
            if (++count > 100) break;
        }
        table2.DumpState();
    }


    TEST(KeymapTest ,Simple_Migration_test) {
        char begin_key_buf[32];
        sprintf(begin_key_buf, "%016d", 0);
        KeySlice beginkey(begin_key_buf, 16);

        Keymap table(beginkey, BytewiseComparator());
        Arena* table_arena = new Arena();
        table.InitArena(table_arena);
        table.InsertArena(table_arena);

        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        int key_count = 1000000;
        uint32_t insert = 0, insert_fail = 0;
        int count = 0;

        printf( "---------------------------------------\n");
        printf( "write %d key to tabel\n", key_count);

        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            char keyinfo_str[64];
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].col_id = 0;
            keycol->col[0].offset_in_kb =5;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, i); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, i+2); /* ikey sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, 2048); /* iter size */
            offset += (addr2 - addr);
            keycol->col[0].col_size = offset -1;
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            uint32_t est_size = offset;
            KeyMapHandle new_handle;
            Slice keyinfo = table.Insert(key, est_size, new_handle);
            if (keyinfo.size()) {
                if (keyinfo.size() < est_size) abort();
                insert++;
                char* addr = const_cast<char*>(keyinfo.data());
                memcpy(addr, keyinfo_str, est_size); 
            }
            else insert_fail++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "insert result s(%lu) f(%lu) micros(%lu) insert(%d) insert_fail(%d) op_count(%ld) cmp_count(%ld)\n",
                start_time, finish_time, microsec, insert, insert_fail, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();


        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        uint32_t result = 0;
        uint32_t found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();

        char key_str[32];
        result = table.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(result, 0, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table.Next(result);
            if (++count > 100) break;
        }

        printf( "---------------------------------------\n");
        printf( "replace %d key to tabel\n", key_count);
        KeyMapHandle handle = table.First();
        uint64_t start_seq = key_count + 20;
        std::string search_str;
        char keyinfo_str[64];
        while(handle) {
            search_str.resize(0);
            table.GetKeyString(handle, search_str);
            if (search_str.size() > 16) abort();
            Slice keyinfo = table.GetKeyInfo(handle);
            if (search_str.size() > 16) abort();
            memcpy(key_str, search_str.data(), search_str.size());
            key_str[search_str.size()] = 0;
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].offset_in_kb = 0x83;
            keycol->col[0].col_id = 1;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, start_seq); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, start_seq + 10000); /* ttl sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, start_seq + 2); /* ikey sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, 4096); /* iter size */
            offset += (addr - addr2);
            keycol->col[0].col_size = offset -1;
            start_seq++;
            uint32_t est_size = offset;
            /* get next allocate offset */
            uint32_t new_handle = table.GetNextAllocOffset(est_size);
            if (!new_handle) abort();
            /* modify existing keyinfo - relocation */
            if (keyinfo.size() ==  0) abort();
            addr = const_cast<char*>(keyinfo.data());
            addr[0] |= 0x80; /* set relocation */
            *((uint32_t*)(addr + 1)) = new_handle;
            /* allocate memory */
            char* mem = table.AllocateMemory(est_size, new_handle);
            memcpy(mem, keyinfo_str, offset);
            handle = table.Next(handle);
        }
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        result = 0;
        found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();

        result = table.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(result, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table.Next(result);
            if (++count > 100) break;
        }



        printf( "---------------------------------------\n\n");
        printf( "Memory Migration all data\n\n");
        Arena *new_arena = new Arena(0x200000);
        Keymap::KeyMigrationCtx ctx;
        memset(&ctx, 0,  sizeof(Keymap::KeyMigrationCtx));
        ctx.first = true;
        ctx.begin_key = beginkey;
        ctx.max_arena_size = 0;
        ctx.hashmap = new KeyMapHashMap(1024*32);
        table.MemoryMigration(nullptr, new_arena, ctx);
        Keymap table2(beginkey, BytewiseComparator());
        table2.InsertArena(new_arena, ctx.hashmap);

        table2.DumpState();

        printf( "---------------------------------------\n");
        printf( "search migrated db %d key to tabel2\n", key_count);
        result = 0;
        found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table2.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table2.GetOpCnt(), table2.GetCmpCnt());  

        printf( "---------------------------------------\n");
        printf( "read 100 key column 1 to tabel\n");
        /*
           result = table2.First();
           count = 0;
           while(result) {
           ColumnData coldata; 
           if (table2.GetKeyColumnData(result, 0, coldata)) {
           if (coldata.key.size() > 16) abort();
           memcpy(key_str, coldata.key.data(), coldata.key.size());
           key_str[coldata.key.size()] = 0;
           free(const_cast<char*>(coldata.key.data()));
           printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
           "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
           key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
           coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
           }
           result = table2.Next(result);
           if (++count > 100) break;
           }
           */
        printf( "read 100 key column 2 to tabel\n");
        result = table2.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table2.GetKeyColumnData(result, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table2.Next(result);
            if (++count > 100) break;
        }
        table2.DumpState();


        printf( "---------------------------------------\n\n");
        printf( "Memory Migration only 0x200000 2M data\n\n");
        Arena *new_arena2 = new Arena(0x200000);
        Keymap::KeyMigrationCtx ctx2;
        memset(&ctx2, 0,  sizeof(Keymap::KeyMigrationCtx));
        ctx2.first = true;
        ctx2.begin_key = beginkey;
        ctx2.max_arena_size = 0x200000;
        ctx2.hashmap = new KeyMapHashMap(1024*32);
        table.MemoryMigration(nullptr, new_arena2, ctx2);
        Keymap table3(beginkey, BytewiseComparator());
        table3.InsertArena(new_arena2, ctx2.hashmap);

        table3.DumpState();

        printf( "---------------------------------------\n");
        printf( "search migrated db %d key to tabel3\n", key_count);
        result = 0;
        found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table3.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table3.GetOpCnt(), table3.GetCmpCnt());  
        table3.DumpState();

        printf( "read 100 key column 2 to tabel\n");
        result = table3.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table3.GetKeyColumnData(result, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table3.Next(result);
            if (++count > 100) break;
        }
        table2.DumpState();
    }

    TEST(KeymapTest , SplitTest) {
        char begin_key_buf[32];
        sprintf(begin_key_buf, "%016d", 0);
        KeySlice beginkey(begin_key_buf, 16);

        Keymap table(beginkey, BytewiseComparator());
        Arena* table_arena = new Arena();
        table.InitArena(table_arena);
        table.InsertArena(table_arena);

        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        int key_count = 1200000;
        uint32_t insert = 0, insert_fail = 0;
        int count = 0;

        printf( "---------------------------------------\n");
        printf( "write %d key to tabel\n", key_count);

        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            char keyinfo_str[64];
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].col_id = 0;
            keycol->col[0].offset_in_kb =5;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, i); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, i+2); /* ikey sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, 2048); /* iter size */
            offset += (addr2 - addr);
            keycol->col[0].col_size = offset -1;
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            uint32_t est_size = offset;
            KeyMapHandle new_handle;
            Slice keyinfo = table.Insert(key, est_size, new_handle);
            if (keyinfo.size()) {
                if (keyinfo.size() < est_size) abort();
                insert++;
                char* addr = const_cast<char*>(keyinfo.data());
                memcpy(addr, keyinfo_str, est_size); 
            }
            else insert_fail++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "insert result s(%lu) f(%lu) micros(%lu) insert(%d) insert_fail(%d) op_count(%ld) cmp_count(%ld)\n",
                start_time, finish_time, microsec, insert, insert_fail, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();


        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        uint32_t result = 0;
        uint32_t found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();



        printf( "---------------------------------------\n");
        printf( "replace %d key to tabel\n", key_count);
        KeyMapHandle handle = table.First();
        uint64_t start_seq = key_count + 20;
        std::string search_str;
        char key_str[32];
        char keyinfo_str[64];
        while(handle) {
            search_str.resize(0);
            table.GetKeyString(handle, search_str);
            if (search_str.size() > 16) abort();
            Slice keyinfo = table.GetKeyInfo(handle);
            memcpy(key_str, search_str.data(), search_str.size());
            key_str[search_str.size()] = 0;
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].offset_in_kb = 0x83;
            keycol->col[0].col_id = 1;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, start_seq); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, start_seq + 10000); /* ttl sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, start_seq + 2); /* ikey sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, 4096); /* iter size */
            offset += (addr - addr2);
            keycol->col[0].col_size = offset -1;
            start_seq++;
            uint32_t est_size = offset;
            /* get next allocate offset */
            uint32_t new_handle = table.GetNextAllocOffset(est_size);
            if (!new_handle) abort();
            /* modify existing keyinfo - relocation */
            if (keyinfo.size() ==  0) abort();
            addr = const_cast<char*>(keyinfo.data());
            addr[0] |= 0x80; /* set relocation */
            *((uint32_t*)(addr + 1)) = new_handle;
            /* allocate memory */
            char* mem = table.AllocateMemory(est_size, new_handle);
            memcpy(mem, keyinfo_str, offset);
            handle = table.Next(handle);
        }
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        result = 0;
        found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "Split keymap with 2M \n");
        std::vector<Keymap*> sub_keymaps;
        table.SplitKeymap(nullptr, sub_keymaps, 0x200000);

        table.DumpState();
        for (int i = 0; i < sub_keymaps.size(); i++) (sub_keymaps[i])->DumpState();


        handle = table.First();
        std::string table_key_str;
        char table_key_buffer[32];
        while(handle) {
            table_key_str.resize(0);
#if 0
            table.GetKeyStr(handle, table_key_str);
            sprintf(table_key_buffer, table_key_str.data(), 16);
            printf("key:%s found, size %ld\n", table_key_str.size());
#endif
            handle = table.Next(handle);
            count++;
        }
        printf("table %p found %d node\n", &table, count);

        Keymap *sub = nullptr;
        for (int i = 0; i < sub_keymaps.size(); i++) { 
            sub = sub_keymaps[i];
            KeyMapHandle handle = sub->First();
            count = 0;
            while(handle) {
                table_key_str.resize(0);
#if 0
                sub->GetKeyStr(handle, table_key_str);
                sprintf(table_key_buffer, table_key_str.data(), 16);
                printf("key:%s found, size %ld\n", table_key_str.size());
#endif
                handle = sub->Next(handle);
                count++;
            }
            printf("table %p found %d node\n", sub, count); 
        }

        printf( "---------------------------------------\n");
        printf( "Merge keymap with 2M \n");

        Keymap *sub_main = sub_keymaps[0]; /* get sub main */
        sub_keymaps.erase(sub_keymaps.cbegin()); /* remove sub main */

        Keymap* new_keymap = sub_main->MergeKeymap(nullptr, sub_keymaps);
        new_keymap->DumpState();

        handle = new_keymap->First();
        count = 0;
        while(handle) {
            table_key_str.resize(0);
#if 0
            table.GetKeyStr(handle, table_key_str);
            sprintf(table_key_buffer, table_key_str.data(), 16);
            printf("key:%s found, size %ld\n", table_key_str.size());
#endif
            handle = new_keymap->Next(handle);
            count++;
        }
        printf("table %p found %d node\n", new_keymap, count);


        for (int i = 0; i < sub_keymaps.size(); i++) { 
            delete sub_keymaps[i]; 
        }
        delete sub_main;
        delete new_keymap;
    }
    TEST(KeymapTest , DeleteTest) {
        char begin_key_buf[32];
        sprintf(begin_key_buf, "%016d", 0);
        KeySlice beginkey(begin_key_buf, 16);

        Keymap table(beginkey, BytewiseComparator());
        Arena* table_arena = new Arena();
        table.InitArena(table_arena);
        table.InsertArena(table_arena);

        uint64_t start_time;
        uint64_t finish_time;
        uint64_t microsec;
        int key_count = 1200000;
        uint32_t insert = 0, insert_fail = 0;
        int count = 0;


        printf( "---------------------------------------\n");
        printf( "write %d key to tabel\n", key_count);

        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            char keyinfo_str[64];
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].col_id = 0;
            keycol->col[0].offset_in_kb =5;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, i); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, i+2); /* ikey sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, 2048); /* iter size */
            offset += (addr2 - addr);
            keycol->col[0].col_size = offset -1;
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            uint32_t est_size = offset;
            KeyMapHandle new_handle;
            Slice keyinfo = table.Insert(key, est_size, new_handle);
            if (keyinfo.size()) {
                if (keyinfo.size() < est_size) abort();
                insert++;
                char* addr = const_cast<char*>(keyinfo.data());
                memcpy(addr, keyinfo_str, est_size); 
            }
            else insert_fail++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "insert result s(%lu) f(%lu) micros(%lu) insert(%d) insert_fail(%d) op_count(%ld) cmp_count(%ld)\n",
                start_time, finish_time, microsec, insert, insert_fail, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();


        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        uint32_t result = 0;
        uint32_t found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "relocated %d key to tabel\n", key_count);
        KeyMapHandle handle = table.First();
        uint64_t start_seq = key_count + 20;
        std::string search_str;
        char key_str[32];
        char keyinfo_str[128];
        while(handle) {
            search_str.resize(0);
            table.GetKeyString(handle, search_str);
            if (search_str.size() > 16) abort();
            Slice keyinfo = table.GetKeyInfo(handle);
            if (search_str.size() > 16) abort();
            memcpy(key_str, search_str.data(), search_str.size());
            key_str[search_str.size()] = 0;
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 0;
            keycol->col[0].offset_in_kb = 0x83;
            keycol->col[0].col_id = 1;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, start_seq); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, start_seq + 10000); /* ttl sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, start_seq + 2); /* ikey sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, 4096); /* iter size */
            offset += (addr - addr2);
            keycol->col[0].col_size = offset -1;
            start_seq++;
            uint32_t est_size = offset;
            /* get next allocate offset */
            uint32_t new_handle = table.GetNextAllocOffset(est_size);
            if (!new_handle) abort();
            /* modify existing keyinfo - relocation */
            if (keyinfo.size() ==  0) abort();
            addr = const_cast<char*>(keyinfo.data());
            addr[0] |= 0x80; /* set relocation */
            *((uint32_t*)(addr + 1)) = new_handle;
            /* allocate memory */
            char* mem = table.AllocateMemory(est_size, new_handle);
            memcpy(mem, keyinfo_str, offset);
            handle = table.Next(handle);
        }
        table.DumpState();
        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        result = 0;
        found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "Replace Test tabel\n");

        printf( "---------------------------------------\n");
        printf( "replace %d key to tabel\n", key_count);
        handle = table.First();

        start_seq = key_count + 20;
        for(int i =0; i < key_count; i++) {
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            KeyCol *keycol = (KeyCol*)keyinfo_str;
            keycol->nr_col = 1;
            keycol->col[0].offset_in_kb = 0x83;
            keycol->col[0].col_id = 0;
            uint32_t offset = 4;
            char* addr = keyinfo_str + offset;
            char* addr2 = EncodeVarint64(addr, start_seq); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, start_seq + 10000); /* ttl sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, start_seq + 2); /* ikey sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, 4096); /* iter size */
            offset += (addr - addr2);
            keycol->col[0].col_size = offset -1;
            COL *next_col = (COL*)(keyinfo_str + offset);
            next_col->offset_in_kb = 0x85;
            next_col->col_id = 1;
            offset += 3;
            addr = keyinfo_str + offset;
            addr2 = EncodeVarint64(addr, start_seq*2); /* iter sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, start_seq*2 + 10000); /* ttl sequence */
            offset += (addr - addr2);
            addr2 = EncodeVarint64(addr, start_seq*2 + 2); /* ikey sequence */
            offset += (addr2 - addr);
            addr = EncodeVarint64(addr2, 4096); /* iter size */
            offset += (addr - addr2); 
            next_col->col_size = (addr - (char*)next_col);
            start_seq++;
            uint32_t est_size = offset;
            KeyMapHandle new_handle;
            Slice keyinfo = table.ReplaceOrInsert(key, est_size, new_handle);
            if (keyinfo.size() == 0) abort(); 
            memcpy(const_cast<char*>(keyinfo.data()), keyinfo_str, est_size); 
        }
        table.DumpState();

        printf( "---------------------------------------\n");
        printf( "search %d key to tabel\n", key_count);
        result = 0;
        found = 0, not_found = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32];
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (result =table.Search(key)) found++;
            else not_found++;
        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "search result s(%lu) f(%lu) micros(%lu) found(%d) not_found(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, found, not_found, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();


        printf( "---------------------------------------\n");
        printf( "read 100 key column 1 to tabel\n");
        result = table.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(result, 0, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table.Next(result);
            if (++count > 100) break;
        }
        printf( "read 100 key column 2 to tabel\n");
        result = table.First();
        count = 0;
        while(result) {
            ColumnData coldata; 
            if (table.GetKeyColumnData(result, 1, coldata)) {
                if (coldata.key.size() > 16) abort();
                memcpy(key_str, coldata.key.data(), coldata.key.size());
                key_str[coldata.key.size()] = 0;
                free(const_cast<char*>(coldata.key.data()));
                printf("Find key(%s) col_size(%d)  ref(%d):IsDelete(%d):HasTTL(%d)\n"
                        "             column id(%d) kb_offset(%d) iter_seq(%ld) ttl(%ld) ikey_seq(%ld) ikey_size(%d)\n\n",
                        key_str, coldata.col_size, coldata.referenced,coldata.is_deleted, coldata.has_ttl,
                        coldata.column_id, coldata.kb_offset, coldata.iter_sequence, coldata.ttl, coldata.ikey_sequence, coldata.ikey_size);
            }
            result = table.Next(result);
            if (++count > 100) break;
        }
        table.DumpState();





        printf( "---------------------------------------\n");
        printf( "Delete Test tabel\n");
        printf( "---------------------------------------\n");


        printf( "---------------------------------------\n");
        printf( "remove %d key to tabel\n", key_count);
        uint32_t removed = 0, remove_failed = 0;
        start_time = Env::Default()->NowMicros();
        for (int i = 0; i < key_count; i++) {
            char key_str[32]; 
            sprintf(key_str, "%016d", i);
            KeySlice key(key_str, 16);
            if (table.Remove(key)) removed++;
            else remove_failed++;

        }
        finish_time = Env::Default()->NowMicros();
        microsec = (finish_time - start_time);
        printf( "Remove result s(%lu) f(%lu) micros(%lu) removed(%d) remove_fail(%d) op_count(%ld), cmp_count(%ld)\n",
                start_time, finish_time, microsec, removed, remove_failed, table.GetOpCnt(), table.GetCmpCnt());  
        table.DumpState();

    }



}  // namespace insdb
int main(int argc, char** argv) {
    return insdb::test::RunAllTests();
}
