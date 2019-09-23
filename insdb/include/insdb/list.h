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


#ifndef INSDB_DB_LIST_H_
#define INSDB_DB_LIST_H_

#include "insdb/status.h"
#include "db/insdb_internal.h"

namespace insdb{

    // C++ version of container_of
    template<class P, class M>
    size_t class_offsetof(const M P::*member)
    {
        return (size_t) &( reinterpret_cast<P*>(0)->*member);
    };

    template<class P, class M>
    P* class_container_of_impl(M* ptr, const M P::*member)
    {
        return (P*)( (char*)ptr - class_offsetof(member));
    };

#define class_container_of(ptr, type, member) \
     class_container_of_impl (ptr, &type::member)

    // Simple double linked list
    struct SimpleLinkedNode
    {
        SimpleLinkedNode() : next_(nullptr), prev_(nullptr) {}
        SimpleLinkedNode * next_;
        SimpleLinkedNode * prev_;
    };

    /* Lock allocates extra memory by compiler. Therefore. The lock is relocated to the user of this list */
    /* 
     * NOTE
     * If you want to use "data_" for user space address for some the other purpose,
     * The total number of nodes must be less than 65565 (2Byte)
     *
     */
    class SimpleLinkedList { //24 byte
        public:
            explicit SimpleLinkedList() : list_(), size_(0) {
                list_.prev_ = &list_;
                list_.next_ = &list_;
            }

            //const SimpleLinkedNode* end() const { return &list_; }
            bool empty() const { return list_.next_ == &list_; }
            SimpleLinkedNode* oldest() const { return list_.prev_ == &list_ ? NULL: list_.prev_; }
            SimpleLinkedNode* newest() const { return list_.next_ == &list_ ? NULL: list_.next_; }
            SimpleLinkedNode* next(SimpleLinkedNode *cache) const { return cache->next_ == &list_ ? NULL: cache->next_; }
            SimpleLinkedNode* prev(SimpleLinkedNode *cache) const { return cache->prev_ == &list_ ? NULL: cache->prev_; }
            uint64_t Size(){ return size_; }

            void InsertFront(SimpleLinkedNode *cache, size_t size = 1) {
                cache->prev_ = &list_;
                cache->next_ = list_.next_;
                cache->prev_->next_ = cache;
                cache->next_->prev_ = cache;
                size_+=size;
            }

            void InsertBack(SimpleLinkedNode *cache, size_t size = 1) {
                cache->next_ = &list_;
                cache->prev_ = list_.prev_;
                cache->prev_->next_ = cache;
                cache->next_->prev_ = cache;
                size_+=size;
            }

            void InsertToNext(SimpleLinkedNode *cache, SimpleLinkedNode *base, size_t size = 1) {
                cache->prev_ = base;
                cache->next_ = base->next_;
                cache->prev_->next_ = cache;
                cache->next_->prev_ = cache;
                size_+=size;
            }

            void Delete(SimpleLinkedNode *cache, SimpleLinkedNode *poison = nullptr, size_t size = 1) {
                cache->prev_->next_ = cache->next_;
                cache->next_->prev_ = cache->prev_;
                cache->next_ = cache->prev_ = poison;
                size_-=size;
            }

            /* Return olest */
            SimpleLinkedNode* Pop(SimpleLinkedNode *poison = nullptr, size_t size = 1) {
                if(list_.prev_ == &list_)
                    return NULL;
                SimpleLinkedNode *node = list_.prev_; 
                node->prev_->next_ = node->next_;
                node->next_->prev_ = node->prev_;
                node->next_ = node->prev_ = poison;
                size_-=size;
                return node; 
            }

            void Splice(const struct SimpleLinkedList *new_list)
            {
                SimpleLinkedNode *prev = &list_;
                SimpleLinkedNode *next = newest();
                SimpleLinkedNode *first = new_list->newest();
                SimpleLinkedNode *last = new_list->oldest();

                first->prev_ = prev;
                prev->next_ = first;

                last->next_ = next;
                next->prev_ = last;
            }

        private:
            SimpleLinkedNode list_;
            uint64_t size_;
    };


}  // namespace leveldb

#endif  // INSDB_DB_LIST_H_
