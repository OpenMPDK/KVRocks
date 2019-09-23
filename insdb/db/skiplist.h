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


#ifndef STORAGE_INSDB_DB_SKIPLIST_H_
#define STORAGE_INSDB_DB_SKIPLIST_H_

/**
  <Thread safety>

  Multithread safe for read/write/delete with CAS(Compare And Swap) operation with simple
  Garbage collector, which based on "Practical lock-freedom" UCAM-CL-TR-579 ISSN 14762986,
  Keir Fraser(http://www.cl.cam.ac.uk/research/srg/netos/lock-free/).

  Becase removed internal nodes are still need to be accessible, garbage collector needs
  to keep amount of removed internal node, which is passed as parameter on GCInfo constructor.

  <Data consideration>

  Insdb supports traversal between nodes with current Data. To support this skiplist assumes
  Data derived from SKLData<Key, Comparator> class. which support SetNode()/GetNode() function.
  Each internal node has prev_ backpointer, which each to traverse list as backword.
  */
#include <assert.h>
#include <stdlib.h>
#include <cstdlib>
#include <mutex>
#include "db/gc.h"
#include "db/skldata.h"
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"
#include "pthread.h"
#include "db/global_stats.h"

namespace insdb {
    class UserKey;

    typedef size_t markable_t;
    /**
      Atomic operation.
      Which provide basic premise for lockfree skiplist.
      */
#define SYNC_SWAP(addr, x)          __sync_lock_test_and_set(addr, x)
#define SYNC_CAS(addr, old, x)      __sync_val_compare_and_swap(addr, old, x)
#define SYNC_ADD(addr, x)           __sync_add_and_fetch(addr, x)
#define SYNC_FETCH_AND_OR(addr, x)  __sync_fetch_and_or(addr, x)

#define EXPECT_TRUE(x)      __builtin_expect(!!(x), 1)
#define EXPECT_FALSE(x)     __builtin_expect(!!(x), 0)

    /**
      Macro for Remove Marker.
      Remove Marker provides hint during traversal skiplist, whether current node
      will be removed or not.
      */
#define TAG_VALUE(v, tag)   (reinterpret_cast<markable_t>((v)) | tag)
#define IS_TAGGED(v, tag)   (reinterpret_cast<markable_t>((v)) & tag)
#define STRIP_TAG(v, tag)   (reinterpret_cast<markable_t>((v)) & ~tag)

#define MARK_NODE(x)    TAG_VALUE((x), 0x1)
#define HAS_MARK(x)     (IS_TAGGED((x), 0x1) == 0x1)
#define GET_NODE(x)     reinterpret_cast<Node *>((x))
#define STRIP_MARK(x)   reinterpret_cast<Node *>(STRIP_TAG((x), 0x1))

#define DOSE_NOT_EXIST  (0)
    template<typename Data, typename Key, class Comparator>
        class SkipList {
            private:
                struct Node;
            public:
                /**
                  @brief Create SkipList object, cmp is comparator of Key, and
                  gcinfo is gabage collector.
                  @param  [in]    cmp Comparator.
                  @param  [in]    gcinfo  garbage collector.
                  */
                explicit SkipList(Comparator cmp, GCInfo *gcinfo, uint16_t maxheight=12, uint16_t branching=4);
                ~SkipList(); 

                void SkipListCleanUp();
                /**
                  @brief Insert key of data, if fullstack is true, internal
                  node for this data has max_height_. It is for building partial search
                  start pointer, see manifest.
                  @param  [in]    key data's key.
                  @param  [in]    data    object extend from SKLData<Key, Comaprator>.
                  @return true if success, false if failure.

                  @note
                  InSDB SkipList does not allow overwrite for exist key.
                  */
                bool Insert(const Key& key, Data  data);
                /**
                  @brief Remove key.
                  @param  [in]    key
                  @return removed data if success, NULL if failure.
                  */
                Data  Remove(const Key& key);

                /**
                  @brief check whether key exist or not.
                  @param  [in]    key
                  @return true if key exist, false if key does not exist.
                  */
                bool Contains(const Key& key);

                /**
                  @brief Search key and get data.
                  @param  [in]    key
                  @return data if key exist, NULL if key does not exist.
                  */
                Data Search(const Key& key);

                /**
                  @brief Search less than equal key.
                  @param  [in]    key
                  @return data if key exist, NULL if key does not exist.

                  @note
                  This function searches key which is less than equal given key.
                  It will be useful to find some representive key  in a range.
                  eq, it used to find sktable instance in manifest which uses smallest key
                  of sktable intance as key.
                  */
                Data SearchLessThanEqual(const Key& key);

                /**
                  @brief Search greater than equal key.
                  @param  [in]    key
                  @return data if key exist, NULL if key does not exist.
                  */
                Data SearchGreaterOrEqual(const Key& key);

                /**
                  @brief get next data in the skiplist.
                  @param  [in]    data    current data
                  @return data if success, NULL if failure.

                  @note
                  if next node is NULL, it mean it reach end of skiplist.
                  */
                Data Next(Data data);

                /**
                  @brief get previous data in the skiplist.
                  @param  [in]    data    current data
                  @return data if success, NULL if failure.

                  @note
                  if next node is NULL, it mean it reach beginning of skiplist.
                  */
                Data Prev(Data data);

                /**
                  @brief get first data in the skiplist.
                  @return data if exist, NULL if it is empty.
                  */
                Data First();
                /**
                  @brief get last data in the skiplist.
                  @return data if exist, NULL if it is empty.
                  */
                Data Last();

                /**
                  @brief return key for data
                  @param  [in]    data
                  @return Key
                  */
                Key GetKeySlice(Data data);
                /*
                 * In order to find a node having max height
                 * Sub-SKTable's UserKey should be one of the UserKey with the max height.
                 */
                int GetMaxHeight(){ return max_height_; }
                size_t GetCount() { return count_; }

                Data GetDataFromNode(void *anon_node);
                uint64_t EstimateCount(const Key& key) const;
                void GCFreeUserKey(Data item);

            private:
                uint16_t kBranching = 4;
                enum unlink {               // skiplist traversal option
                    FORCE_UNLINK,
                    ASSIST_UNLINK,
                    DONT_UNLINK
                };


                Comparator const compare_;
                Node* head_;  // internal node head
                int max_height_;   // Height of the entire list
                int cur_height_;
                Random rnd_; // determine random height.
                GCInfo *gcinfo_; //garbage collector
                std::atomic<size_t> count_;
                port::Mutex lock_;
                inline int GetHeight() const { return cur_height_;}

                /**
                  @brief create internal node.
                  @param  [in]    key
                  @param  [in]    data
                  @param  [in]    height
                  @return node if success, NULL if failure.
                  */
                Node* NewNode(const Key& key, Data data, int height);

                /**
                  @brief Remove internal node.
                  @param  [in]    item    internal node

                  @note
                  FreeNode register internal node to garbage collector.
                  Garbage collector matain per thread structure to keep internal node.
                  */
                void FreeNode(Node* item);
                int RandomHeight();
                bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0);}
                int Comp(const Key& a, const Key& b) const { return compare_(a, b);}

                /**
                  @brief find earliest node that comes at or after key.
                  @param  [in] key
                  @return node if success, NULL failure.
                  */
                Node* FindGreaterOrEqual(const Key& key);

                /**
                  @brief find previous and next node stacks in skiplist with given tree.
                  @param  [in/out]    prev    array of node* to keep previous link.
                  @param  [in/out]    succs   array of node* to keep next link.
                  @param  [in]    level   count of array entry.
                  @param  [in]    key
                  @param  [in]    action  skiplist traversal option.
                  @param  [in]    b_las   boolean flag to indicate search last node.
                  @param  [in]    start   node* which is starting point of search.
                  @return node if find exist key, NULL otherwise.
                  */
                Node* FindPrevAndSuccess(Node** prev, Node** succs, int level, const Key& key, enum unlink action, bool b_last);

                /**
                  @brief Find next data
                  @param  [in]    node    current node
                  @return data if success, NULL if failure.
                  */
                Data NextWithNode(Node *node);

                /**
                  @brief Find Previous data
                  @param  [in]    node    current node
                  @return data if success, NULL if failure.
                  */
                Data PrevWithNode(Node *node);
                // disable copy
                SkipList(const SkipList&);
                void operator=(const SkipList&);
        };

    /**
      <Internal Node>
      rep:: key
      data
      height
      prev_
      next_[hegith]
      */
    template<typename Data, typename Key, class Comparator>
        struct SkipList<Data,Key,Comparator>::Node {
            explicit Node(const Key& k, Data  d, int h) : height(h), data(d) { }
            int const height;
            Data  data;
            Node* prev_;
            Node* next_[1];
        };

    template<typename Data, typename Key, class Comparator>
        typename SkipList<Data,Key,Comparator>::Node*
        SkipList<Data,Key,Comparator>::NewNode(const Key& key, Data data, int height) {
            int retry_count = 0;
retry:
            char *mem;
            /**
              allocate internal node with 8bytes alignment.
              Because skiplist use node address with special marker, it must be aligned.
              */
            size_t len = sizeof(Node) + sizeof(Node *) * (height - 1);
            if (posix_memalign((void **)&mem,  32, len)) {
                if (++retry_count > 5) {
                    assert(0);
                    return NULL;
                }
#ifdef INSDB_GLOBAL_STATS
                g_skiplist_newnode_retry_cnt++;
#endif
                goto retry;
            }
#ifndef NDEBUG
            memset(mem,0xfc,len);
#endif
#ifdef INSDB_GLOBAL_STATS
            g_new_slnode_cnt++;
#endif

            return new (mem) Node(key, data, height);
        }

    template<typename Data, typename Key, class Comparator>
        void SkipList<Data,Key,Comparator>::FreeNode(Node* item) {
            return gcinfo_->AddItemNode(reinterpret_cast<void*>(item));
        }

    template<typename Data, typename Key, class Comparator>
        void SkipList<Data,Key,Comparator>::GCFreeUserKey(Data item) {
            return gcinfo_->AddItemUserKey(reinterpret_cast<void*>(item));
        }

    template<typename Data, typename Key, class Comparator>
        int SkipList<Data,Key,Comparator>::RandomHeight() {
            int height = 1;
            while (height <max_height_ && ((rnd_.Next() % kBranching) == 0)) height++;
            assert(height > 0);
            assert(height <= max_height_);
            if (height > cur_height_) {
                height = SYNC_ADD(&cur_height_, 1);
            }
            return height;
        }
    /* Skiplist Internal Use Only*/
    template<typename Data, typename Key, class Comparator>
        uint64_t SkipList<Data,Key,Comparator>::EstimateCount(const Key& key) const {
retry:
            uint64_t count = 0;
            Node* x = head_;
            int level = max_height_ - 1;
            while (true) {
                auto *data = x->data;
#ifndef NDEBUG
                data = x->data;
                if(!data)
                    assert(x == head_ || compare_(data->GetKeySlice(), key) < 0);
#endif
                // Get vaild next node
                Node* next = x->next_[level];
                if(next)
                {
                    if(HAS_MARK(next))
                    {
                        goto retry;
#ifdef INSDB_GLOBAL_STATS
                        g_skiplist_estimate_retry_cnt++;
#endif
                    }
                    data = next->data;
                    if(!data)
                    {
#ifdef INSDB_GLOBAL_STATS
                        g_skiplist_estimate_retry_cnt++;
#endif
                        goto retry;
                    }
                }
                // move to the next level
                if (next == nullptr || compare_(data->GetKeySlice(), key) >= 0) {
                    if (level == 0) {
                        return count;
                    } else {
                        // Switch to next list
                        count *= kBranching;
                        level--;
                    }
                } else {
                    x = next;
                    count++;
                }
            }
        }

    template<typename Data, typename Key, class Comparator>
        typename SkipList<Data,Key,Comparator>::Node*
        SkipList<Data,Key,Comparator>::FindGreaterOrEqual(const Key& key) {
            Node *succs = NULL;
            Node* x = FindPrevAndSuccess(NULL, &succs, 1, key, DONT_UNLINK, false);
            if (x) return x;
            else return succs;
        }

    template<typename Data, typename Key, class Comparator>
        SkipList<Data,Key,Comparator>::SkipList(Comparator cmp, GCInfo *gcinfo, uint16_t maxheight, uint16_t branching)
        : compare_(cmp),
        max_height_(maxheight),
        kBranching(branching),
        cur_height_(1),
        rnd_(0xdeadbeef),
        gcinfo_(gcinfo),
        count_(0),
        lock_(){
            head_ = NewNode(0, 0 /* any key will do */, max_height_);
            assert(head_ != NULL);
            for (int i = 0; i <max_height_; i++)  {
                head_->next_[i] =  NULL;
            }
            head_->prev_ = NULL;

        }

    template<typename Data, typename Key, class Comparator>
        SkipList<Data,Key,Comparator>::~SkipList() {
            Node *node = NULL, *tmp = NULL;
            char* mv_item = NULL;
            if (head_) { /* remove internal node */
                node = head_->next_[0];
                while (node != NULL) {
                    while (node != NULL && HAS_MARK(node->next_[0]))  node = STRIP_MARK(node->next_[0]);
                    if (node == NULL) break;
                    tmp = node;
                    node = node->next_[0];
                    if (tmp->data && std::is_same<Data, UserKey*>::value){
                        abort();
                        GCFreeUserKey(tmp->data);
                    }
                    FreeNode(tmp); /* free */
                }
                mv_item = reinterpret_cast<char *>(head_);
                free(mv_item);
            }
        }

    template<typename Data, typename Key, class Comparator>
        void  SkipList<Data,Key,Comparator>::SkipListCleanUp() {
            Node *node = NULL, *tmp = NULL;
            char* mv_item = NULL;
            if (head_) { /* remove internal node */
                node = head_->next_[0];
                while (node != NULL) {
                    while (node != NULL && HAS_MARK(node->next_[0]))  node = STRIP_MARK(node->next_[0]);
                    if (node == NULL) break;
                    tmp = node;
                    node = node->next_[0];
                    if (tmp->data && std::is_same<Data, UserKey*>::value){
                        abort();
                        GCFreeUserKey(tmp->data);
                    }
                    FreeNode(tmp); /* free */
                }
                mv_item = reinterpret_cast<char *>(head_);
                free(mv_item);
                head_= NULL;
            }

        }



    template<typename Data, typename Key, class Comparator>
        typename SkipList<Data,Key,Comparator>::Node*
        SkipList<Data,Key,Comparator>::FindPrevAndSuccess(Node** prev, Node** succs, int n, const Key& key, enum unlink action, bool b_last)
        {
retry:
            Node *pred = head_;
            Node *item = NULL, *next = NULL;
            int level = GetHeight() - 1;
            int delta = 0;
            for (; level >= 0; --level) {
                next = pred->next_[level];
                if (next == NULL && level >= n) continue; /* goto next level .. */
                if (EXPECT_FALSE(HAS_MARK(next))) {
#ifdef INSDB_GLOBAL_STATS
                    g_skiplist_find_prevandsucess_retry_cnt++;
#endif
                    goto retry; /** pred will be gone ... retry */
                }
                item = GET_NODE(next);
                while (item != NULL) { /* search through sibling link */
                    next = item->next_[level];
                    while (EXPECT_FALSE(HAS_MARK(next))) { /* item will be gone ... find next from sibling */
                        if (action == DONT_UNLINK) { /* skip over .. logically removed items */
                            item = STRIP_MARK(next);
                            if (EXPECT_FALSE(item == NULL)) break; /* end of link */
                            next = item->next_[level];
                        } else { /* unlink logically removed items */
                            Node* other = SYNC_CAS(&pred->next_[level], item, STRIP_MARK(next));
                            if (other == item) {
                                item = STRIP_MARK(next);
                                if (level == 0) { /* update previous link .. try best but don't care */
                                    if (item) {
                                        Node *next_prev = item->prev_;
                                        if (SYNC_FETCH_AND_OR(&pred->next_[0], 0) == item) SYNC_CAS(&item->prev_, next_prev, pred);
                                    }
                                }
                            } else {
                                if (HAS_MARK(other)) {
#ifdef INSDB_GLOBAL_STATS
                                    g_skiplist_find_prevandsucess_retry_cnt++;
#endif
                                    goto retry; /* someone has done too many things... retry */
                                }
                                item = GET_NODE(other);
                            }
                            next = (item != NULL) ? item->next_[level] : NULL;
                        }
                    }
                    if (EXPECT_FALSE(item == NULL)) break;
                    if (EXPECT_TRUE(b_last == false)) {
                        /* here we have valid item */
                        if (item == head_) {
                            delta = -1;
                        } else {
                            Data data= item->data;
                            if (data == NULL) {
#ifdef INSDB_GLOBAL_STATS
                                g_skiplist_find_prevandsucess_retry_cnt++;
#endif
                                goto retry;
                            }
                            else if(!data->CompareKey(compare_, key, delta))
                                goto retry;

                        }
                        if (delta > 0) break;
                        if (delta == 0 && action != FORCE_UNLINK) break;
                    }
                    pred = item;
                    item = GET_NODE(next);
                }
                if (level <n) { /* fillup previous and successor */
                    if (prev != NULL) prev[level] = pred;
                    if (succs != NULL) succs[level] = item;
                }
            }
            if (EXPECT_FALSE(b_last == true)) return pred;
            if (delta == 0) return item; /* found exact match node or no item on skiplist */ 
            return NULL; /* found node create or eqaul or no items in the skiplist */
        }



    template<typename Data, typename Key, class Comparator>
        bool SkipList<Data,Key,Comparator>::Insert(const Key& key, Data data) {
            int height = RandomHeight(); 
            Node* new_item = NULL;
            lock_.Lock();
            (static_cast<SKLData<Key, Comparator>*>(data))->SetNode((void *)nullptr); /* clear node link in data */
retry:
            Node* prevs[max_height_];
            Node* nexts[max_height_];
            /* find existing one */
            Node* old_item = FindPrevAndSuccess(prevs, nexts, height, key, ASSIST_UNLINK, false);
            if (old_item != NULL) { lock_.Unlock(); return false; } /* not allow update */
            if (new_item == NULL) { /* create new node */
                new_item = NewNode(key, data, height);
                if (new_item == NULL) { lock_.Unlock(); return false; }
                (static_cast<SKLData<Key, Comparator>*>(data))->SetNode((void *)new_item); /* add node link to data */
            }
            /* fill next link of new node */
            Node* next = new_item->next_[0] =  nexts[0];
            for (int level = 1; level <new_item->height; ++level) {
                new_item->next_[level] = nexts[level];
            }

            /* update lowest previous link */
            Node* prev = prevs[0];
            new_item->prev_ = prev;
            if (next != SYNC_CAS(&prev->next_[0], next, new_item)) {
#ifdef INSDB_GLOBAL_STATS
                g_skiplist_insert_retry_cnt++;
#endif
                goto retry; /* fail to update previous link */
            }
            if (next) { /* update next prev ..don't care ...whether it is correct or not */
                Node *next_prev = next->prev_;
                if (SYNC_FETCH_AND_OR(&new_item->next_[0], 0) == next) SYNC_CAS(&next->prev_, next_prev, new_item);
            }

            for (int level = 1; level <new_item->height; ++level) { /* update remaining previous links */
                do {
                    prev = prevs[level];
                    if (nexts[level] == SYNC_CAS(&prev->next_[level], nexts[level], new_item)) break; /* update success, try next level */
                    FindPrevAndSuccess(prevs, nexts, new_item->height, key, ASSIST_UNLINK, false); /* retry to get previous and successor link */
                    for (int i = level; i <new_item->height; ++i) {
                        Node* old_next = new_item->next_[i];
                        if (nexts[i] == old_next) continue; /* there is no change in nexts */
                        Node* other = SYNC_CAS(&new_item->next_[i], old_next, nexts[i]);
                        if (HAS_MARK(other)){ /* some other guys try to remove new_item node.. stop. */
                            FindPrevAndSuccess(NULL, NULL, 0, key, FORCE_UNLINK, false);
                            lock_.Unlock();
                            return false;
                        }
                    }
                } while (1);
            }
            if (HAS_MARK(new_item->next_[new_item->height - 1])) { /* some other guys try to remove new_item node.. stop. */
                FindPrevAndSuccess(NULL, NULL, 0, key, FORCE_UNLINK, false);
                lock_.Unlock();
                return false;
            }
            lock_.Unlock();
            count_++;
            return true;
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::Remove(const Key& key) {
            lock_.Lock();
            Node* item = FindPrevAndSuccess(NULL, NULL, 0, key, ASSIST_UNLINK, false);
            if (item == NULL) { lock_.Unlock(); return DOSE_NOT_EXIST; } /* key does not exist */
            assert(item->height <= max_height_);
            if (item == head_) { lock_.Unlock(); return DOSE_NOT_EXIST; }
            Node* old_next = NULL;
            // Mark next pointers in previous nodes
            for(int level = item->height -1; level >= 0; --level) {
                Node* next = NULL;
                old_next = item->next_[level];
                do {
                    next = old_next;
                    old_next = SYNC_CAS(&item->next_[level], next, MARK_NODE(next));
                    if (HAS_MARK(old_next)) { /* node is already marked by others.. stop */
                        if (level == 0) { lock_.Unlock(); return DOSE_NOT_EXIST; }
                        break;
                    }
                } while (next != old_next);
            }
            Data  old_data = SYNC_SWAP(&item->data, DOSE_NOT_EXIST); /* replace data as DOSE_NOT_EXIST */
            FindPrevAndSuccess(NULL, NULL, 0, key, FORCE_UNLINK, false); /* unlink */
            /* remove node link from data */
            if (old_data) (static_cast<SKLData<Key, Comparator>*>(old_data))->SetNode((void *)NULL);
            lock_.Unlock();
            count_--;
            FreeNode(item); /* free */
            return old_data;
        }


    template<typename Data, typename Key, class Comparator>
        bool SkipList<Data,Key,Comparator>::Contains(const Key& key) {
            Node* x = FindPrevAndSuccess(NULL, NULL, 0, key, DONT_UNLINK, false);
            if (x != NULL && x->data != DOSE_NOT_EXIST) return true;
            else return false;
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::Search(const Key& key) {
retry:
            Node* x = FindPrevAndSuccess(NULL, NULL, 0, key, DONT_UNLINK, false);
            if (x != NULL) {
                Data  val = x->data;
                if (val != DOSE_NOT_EXIST) return val;
                /*Double check with ilgu for retry*/
#ifdef INSDB_GLOBAL_STATS
                g_skiplist_search_retry_cnt++;
#endif
                goto retry;
            }
            return NULL;
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::SearchLessThanEqual(const Key& key) {
retry:
            Node *prev = NULL;
            Node *x = FindPrevAndSuccess(&prev, NULL, 1, key, DONT_UNLINK, false);
            if (x!= NULL) { /* find exact match */
                Data  val = x->data;
                if (val != DOSE_NOT_EXIST) return val;
            }
            if (prev != head_ && prev != NULL) { /* check previous node */
                Data  val = prev->data;
                if (val != DOSE_NOT_EXIST) return val;
#ifdef INSDB_GLOBAL_STATS
                g_skiplist_searchlessthanequal_retry_cnt++;
#endif
                goto retry; /* retry */
            }
            return NULL;
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::SearchGreaterOrEqual(const Key& key) {
retry:
            Node* node = FindGreaterOrEqual(key);
            while (node != NULL && HAS_MARK(node->next_[0])) node = STRIP_MARK(node->next_[0]);
            if (node != head_ && node != NULL) {
                Data val = node->data;
                if (val != DOSE_NOT_EXIST) return val;
#ifdef INSDB_GLOBAL_STATS
                g_skiplist_searchgreaterequal_retry_cnt++;
#endif
                goto retry; /* retry */
            }
            return NULL;
        }


    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::NextWithNode(Node *tnode) {
            Node* node = NULL;
retry:
            node = tnode->next_[0];
            while (node != NULL && HAS_MARK(node->next_[0])) node = STRIP_MARK(node->next_[0]);
            if (node == head_) node = NULL;
            if (node != NULL) {
                Data  val = node->data;
                if (val != DOSE_NOT_EXIST) return val;
#ifdef INSDB_GLOBAL_STATS
                g_skiplist_nextwithnode_retry_cnt++;
#endif
                goto retry; /* retry */
            }
            return NULL;
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::PrevWithNode(Node *tnode) {
            Node* node = NULL;
retry:
            node = tnode->prev_;
            while (node != NULL && HAS_MARK(node->next_[0])) node = node->prev_;
            if (node == head_) node = NULL;
            if (node != NULL) {
                Data  val = node->data;
                if (val != DOSE_NOT_EXIST) return val;
#ifdef INSDB_GLOBAL_STATS
                g_skiplist_prevwithnode_retry_cnt++;
#endif
                goto retry; /* retry */
            }
            return NULL;
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::Next(Data data) {
            if (data) {
                Node *node =(Node *)(static_cast<SKLData<Key, Comparator>*>(data))->GetNode();
                /* can be during update link .. search with key*/
                if (!node) {
                    return SearchGreaterOrEqual(data->GetKeySlice());
                }
                return NextWithNode(node);
            }
            return NULL;
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::Prev(Data data) {
            if (data) {
                Node *node =(Node *)(static_cast<SKLData<Key, Comparator>*>(data))->GetNode();
                /* can be during update link .. search with key*/
                if (!node) {
                    return SearchLessThanEqual(data->GetKeySlice());
                }
                return PrevWithNode(node);
            }
            return NULL;
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::First() {
            Node *node = head_->next_[0];
retry:
            while (node != NULL && HAS_MARK(node->next_[0])) node = STRIP_MARK(node->next_[0]);
            if (node == head_) node = NULL;
            if (node != NULL) {
                Data  val = node->data;
                if (val != DOSE_NOT_EXIST) return val;
                goto retry; /* retry */
            }
            return NULL;
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::Last(){
            Node *node = NULL;
retry:
            node = FindPrevAndSuccess(NULL, NULL, 0, 0, DONT_UNLINK, true);
            while (node != NULL && HAS_MARK(node->next_[0])) node = node->prev_;
            if (node == head_) node = NULL;
            if (node != NULL) {
                Data  val = node->data;
                if (val != DOSE_NOT_EXIST) return val;
                goto retry; /* retry */
            }
            return NULL;
        }

    template<typename Data, typename Key, class Comparator>
        Key SkipList<Data,Key,Comparator>::GetKeySlice(Data data) {
            assert(data); 
            Node *node =(Node *)(static_cast<SKLData<Key, Comparator>*>(data))->GetNode();
            assert(node);
            return node->data->GetKeySlice();
        }

    template<typename Data, typename Key, class Comparator>
        Data SkipList<Data,Key,Comparator>::GetDataFromNode(void *anon_node) {
            assert(anon_node);
            Node *node =(Node *)anon_node;
            return node->data;
        }


}  // namespace insdb

#endif // STORAGE_INSDB_DB_SKIPLIST_H_
