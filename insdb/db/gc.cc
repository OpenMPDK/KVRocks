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


#include <stdint.h>
#include <cstring>
#include "db/insdb_internal.h"
#include "db/skiplist.h"

namespace insdb {

    class UserKey;
    /*
       per thread key destructor
       Just set gc->set as false.
       Freelist will be reused by other thread.
       */
    void GCInfo::DestroyGC(void * arg) {
        GC_FreeList *gc = reinterpret_cast<GC_FreeList *>(arg);
        gc->set = 0;
        return;
    }

    GCInfo::~GCInfo() {
        GC_FreeList *gc = NULL, *tmp = NULL;
        CleanupGCInfo();/* remove remmained internal node */
        gc = gc_free_list_;
        while(gc) {
            tmp = gc;
            gc = gc->next;
            delete tmp;
        }
    }

    bool GCInfo::InitGCInfo() {
        /* create thread key */
        if (pthread_key_create(&GC_key_, DestroyGC)) return false;
        return true;
    }

    /* get gc if not exist, build new one */
    GC_FreeList * GCInfo::GetGC() {
        GC_FreeList *gc = NULL, *next = NULL;
        gc = reinterpret_cast<GC_FreeList *>(pthread_getspecific(GC_key_));
        if (NULL == gc) {
            gc = gc_free_list_;
            for(; NULL != gc; gc = gc->next) { /* reuse one */
                if ((0 == gc->set) && (0 == SYNC_CAS(&gc->set, 0, 1))) break;
            }
            if (NULL == gc) {
                gc = new GC_FreeList;
                gc->scale = scale_;
                gc->scale_down_threshold = ((1 <<scale_) - 10);
                gc->next = NULL;
                gc->set = 1;
                gc->node_free_list = new FreeList;
                gc->node_free_list->head = 0;
                gc->node_free_list->tail = 0;
                gc->node_free_list->count = 0;
                gc->node_free_list->x = new void *[1 <<scale_];
                gc->ukey_free_list = new FreeList;
                gc->ukey_free_list->head = 0;
                gc->ukey_free_list->tail = 0;
                gc->ukey_free_list->count = 0;
                gc->ukey_free_list->x = new void *[1 <<scale_];
                do {
                    next = gc_free_list_;
                    gc->next = next;
                } while (next != SYNC_CAS(&gc_free_list_, next, gc));
            }
            pthread_setspecific(GC_key_, gc); /* set key with thread specific gc */
        }
        return gc;
    }

    bool GCInfo::CleanupGCInfo() {
        GC_FreeList *gc = NULL;
        FreeList* nlist = NULL, *ulist = NULL;
        void *del_item;
        UserKey* ukey_del_item;
        for (gc = gc_free_list_; gc != NULL; gc = gc->next) {
            nlist = gc->node_free_list;
            if (nlist && nlist->x) {
                while (nlist->count > 0) { /* remove item from freelist */
                    uint32_t i = MOD_SCALE(nlist->tail, gc->scale);
                    del_item = nlist->x[i];
                    free(del_item);
#ifdef INSDB_GLOBAL_STATS
                    g_del_slnode_cnt++;
#endif
                    nlist->tail++;
                    nlist->count--;
                }
                delete[] nlist->x;
                nlist->x = NULL;
                delete nlist;
                gc->node_free_list = NULL;
            }
            ulist = gc->ukey_free_list;
            if (ulist && ulist->x) {
                while (ulist->count > 0) { /* remove item from freelist */
                    uint32_t i = MOD_SCALE(ulist->tail, gc->scale);
                    ukey_del_item = reinterpret_cast<UserKey*>(ulist->x[i]);
                    delete ukey_del_item;
#ifdef INSDB_GLOBAL_STATS
                    g_del_ukey_cnt++;
#endif
                    ulist->tail++;
                    ulist->count--;
                }
                delete[] ulist->x;
                ulist->x = NULL;
                delete ulist;
                gc->ukey_free_list = NULL;
            }
        }
        if (pthread_key_delete(GC_key_)) return false;
        return true;
    }

    void GCInfo::AddItemNode(void *item) {
        GC_FreeList *gc = GetGC();
        FreeList* nlist = gc->node_free_list;
        /* check queue full */
        if (MOD_SCALE(nlist->head + 1, gc->scale) == MOD_SCALE(nlist->tail, gc->scale)) {
            assert(nlist->count > gc->scale_down_threshold);
            /* move data until threshold */
            void* mv_item = NULL;
            while (nlist->count > gc->scale_down_threshold) {
                /* remove item from freelist */
                uint32_t i = MOD_SCALE(nlist->tail, gc->scale);
                mv_item = nlist->x[i];
#ifndef NDEBUG
                memset(mv_item, 0xfc, 32);
#endif
                free(mv_item);
#ifdef INSDB_GLOBAL_STATS
                g_del_slnode_cnt++;
#endif
                nlist->tail++;
                nlist->count--;
            }
        }
        /* insert item to freelist */
        uint32_t i = MOD_SCALE(nlist->head, gc->scale);
        nlist->x[i] = item;
        nlist->head++;
        nlist->count++;
        return;
    }

    void GCInfo::AddItemUserKey(void *item) {
        GC_FreeList *gc = GetGC();
        FreeList* ulist = gc->ukey_free_list;
        /* check queue full */
        if (MOD_SCALE(ulist->head + 1, gc->scale) == MOD_SCALE(ulist->tail, gc->scale)) {
            assert(ulist->count > gc->scale_down_threshold);
            /* move data until threshold */
            UserKey* mv_item = NULL;
            while (ulist->count > gc->scale_down_threshold) {
                /* remove item from freelist */
                uint32_t i = MOD_SCALE(ulist->tail, gc->scale);
                mv_item = reinterpret_cast<UserKey*>(ulist->x[i]);
                delete mv_item;
                ulist->tail++;
                ulist->count--;
#ifdef INSDB_GLOBAL_STATS
                g_del_ukey_cnt++;
#endif
            }
        }
        /* insert item to freelist */
        uint32_t i = MOD_SCALE(ulist->head, gc->scale);
        ulist->x[i] = item;
        ulist->head++;
        ulist->count++;
        return;
    }
} //namespace insdb
