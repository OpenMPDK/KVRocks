// Copyright (c) 2011 The insdb Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_INSDB_DB_SKIPLIST2_H_
#define STORAGE_INSDB_DB_SKIPLIST2_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList2 will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList2 is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList2.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>
#include "db/skldata.h"
#include "util/random.h"

namespace insdb {
    struct SLSplitCtx {
        static const uint16_t kMergeMaxHeight = 12;
        void *src_prev[kMergeMaxHeight];
        void *target_next[kMergeMaxHeight];
        void *src_cur_node;
        void *src_cur_del_node;
        SLSplitCtx() {
            for (uint16_t i = 0; i < kMergeMaxHeight; i++) {
                src_prev[i] = target_next[i] = nullptr;
            }
            src_cur_node = nullptr;
            src_cur_del_node = nullptr;
        }
    };

    template <typename Data, typename Key, class Comparator>
        class SkipList2 {
            private:
                struct Node;

            public:
                // Create a new SkipList2 object that will use "cmp" for comparing keys
                explicit SkipList2(Comparator cmp);

                SkipList2(const SkipList2&) = delete;
                SkipList2& operator=(const SkipList2&) = delete;
                ~SkipList2();
                // Insert key into the list.
                // REQUIRES: nothing that compares equal to key is currently in the list.
                bool Insert(const Key& key, Data data);

                Data ReplaceOrInsert(const Key& key, Data data);

                Data Remove(const Key& key);

                Data Search(const Key& key, SLSplitCtx *m_ctx = nullptr) const;

                Data SearchLessThanEqual(const Key& key) const;

                Data SearchGreaterOrEqual(const Key& key, SLSplitCtx *m_ctx = nullptr) const;

                Data Next(Data data, SLSplitCtx *m_ctx = nullptr) const;

                Data Prev(Data data) const;

                Data First(SLSplitCtx *m_ctx = nullptr) const;

                void Append(SLSplitCtx *m_ctx);
                
                Data Remove(SLSplitCtx *m_ctx);
                
                void InitSplitTarget(SLSplitCtx *m_ctx);

                Data Last() const;

                Key GetKeySlice(Data data);

                inline int GetMaxHeight() const {
                    return max_height_;
                }

                inline size_t GetCount() const {
                    return count_;
                }

                // Returns true iff an entry that compares equal to key is in the list.
                bool Contains(const Key& key) const;

                // Iteration over the contents of a skip list
                class Iterator {
                    public:
                        // Initialize an iterator over the specified list.
                        // The returned iterator is not valid.
                        explicit Iterator(const SkipList2* list);

                        // Returns true iff the iterator is positioned at a valid node.
                        bool Valid() const;

                        // Returns the key at the current position.
                        // REQUIRES: Valid()
                        const Key& key() const;

                        // Advances to the next position.
                        // REQUIRES: Valid()
                        void Next();

                        // Advances to the previous position.
                        // REQUIRES: Valid()
                        void Prev();

                        // Advance to the first entry with a key >= target
                        void Seek(const Key& target);

                        // Position at the first entry in list.
                        // Final state of iterator is Valid() iff list is not empty.
                        void SeekToFirst();

                        // Position at the last entry in list.
                        // Final state of iterator is Valid() iff list is not empty.
                        void SeekToLast();

                    private:
                        const SkipList2* list_;
                        Node* node_;
                        // Intentionally copyable
                };

            private:
                enum { kMaxHeight = 12 };

                Node* NewNode(const Key& key, Data data, int height);
                int RandomHeight();
                bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

                // Return true if key is greater than the data stored in "n"
                bool KeyIsAfterNode(const Key& key, Node* n) const;

                // Return the earliest node that comes at or after key.
                // Return nullptr if there is no such node.
                //
                // If prev is non-null, fills prev[level] with pointer to previous
                // node at "level" for every level in [0..max_height_-1].
                Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

                // Return the latest node with a key < key.
                // Return head_ if there is no such node.
                Node* FindLessThan(const Key& key) const;

                // Return the last node in the list.
                // Return head_ if list is empty.
                Node* FindLast() const;

                Data NextWithNode(Node *node, SLSplitCtx *m_ctx = nullptr) const;

                Data PrevWithNode(Node *node) const;

                // Immutable after construction
                Comparator const compare_;

                Node* const head_;

                // Modified only by Insert().  Read racily by readers, but stale
                // values are ok.
                int max_height_;  // Height of the entire list
                size_t count_;
                // Read/written only by Insert().
                Random rnd_;
        };

    // Implementation details follow
    template <typename Data, typename Key, class Comparator>
        struct SkipList2<Data, Key, Comparator>::Node {
            explicit Node(const Key& k, Data d, int h) : height(h), data(d) {}

            // Accessors/mutators for links.  Wrapped in methods so we can
            // add the appropriate barriers as necessary.
            Node* Next(int n) {
                assert(n >= 0);
                // Use an 'acquire load' so that we observe a fully initialized
                // version of the returned Node.
                return next_[n];
            }
            void SetNext(int n, Node* x) {
                assert(n >= 0);
                // Use a 'release store' so that anybody who reads through this
                // pointer observes a fully initialized version of the inserted node.
                next_[n] = x;
            }

            Key GetKey() {
                assert(data);
                return data->GetKeySlice();
            }

            Data GetData() {
                return data;
            }

            int GetHeight() {
                return height;
            }

            Node* Prev() {
                return prev;
            }

            void SetPrev(Node* x) {
                //assert(x);
                prev = x;
            }

            private:
            int const height;
            Data data;
            Node* prev;
            // Array of length equal to the node height.  next_[0] is lowest level link.
            Node* next_[1];
        };

    template <typename Data, typename Key, class Comparator>
        typename SkipList2<Data, Key, Comparator>::Node* SkipList2<Data, Key, Comparator>::NewNode(
                const Key& key, Data data, int height) {
            int retry_count = 0;
retry:
            char *mem = nullptr;
            size_t len = sizeof(Node) + sizeof(Node*) * (height - 1);
            if (posix_memalign((void **)&mem, 32, len)) {
                if (++retry_count > 5) {
                    assert(0);
                    return nullptr;
                }
                goto retry;
            }
            return new (mem) Node(key, data, height);
        }

    template <typename Data, typename Key, class Comparator>
        inline SkipList2<Data, Key, Comparator>::Iterator::Iterator(const SkipList2* list) {
            list_ = list;
            node_ = nullptr;
        }

    template <typename Data, typename Key, class Comparator>
        inline bool SkipList2<Data, Key, Comparator>::Iterator::Valid() const {
            return node_ != nullptr;
        }

    template <typename Data, typename Key, class Comparator>
        inline const Key& SkipList2<Data, Key, Comparator>::Iterator::key() const {
            assert(Valid());
            return node_->key;
        }

    template <typename Data, typename Key, class Comparator>
        inline void SkipList2<Data, Key, Comparator>::Iterator::Next() {
            assert(Valid());
            node_ = node_->Next(0);
        }

    template <typename Data, typename Key, class Comparator>
        inline void SkipList2<Data, Key, Comparator>::Iterator::Prev() {
            // Instead of using explicit "prev" links, we just search for the
            // last node that falls before key.
            assert(Valid());
            node_ = list_->FindLessThan(node_->GetKey());
            if (node_ == list_->head_) {
                node_ = nullptr;
            }
        }

    template <typename Data, typename Key, class Comparator>
        inline void SkipList2<Data, Key, Comparator>::Iterator::Seek(const Key& target) {
            node_ = list_->FindGreaterOrEqual(target, nullptr);
        }

    template <typename Data, typename Key, class Comparator>
        inline void SkipList2<Data, Key, Comparator>::Iterator::SeekToFirst() {
            node_ = list_->head_->Next(0);
        }

    template <typename Data, typename Key, class Comparator>
        inline void SkipList2<Data, Key, Comparator>::Iterator::SeekToLast() {
            node_ = list_->FindLast();
            if (node_ == list_->head_) {
                node_ = nullptr;
            }
        }

    template <typename Data, typename Key, class Comparator>
        int SkipList2<Data, Key, Comparator>::RandomHeight() {
            // Increase height with probability 1 in kBranching
            static const unsigned int kBranching = 4;
            int height = 1;
            while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
                height++;
            }
            assert(height > 0);
            assert(height <= kMaxHeight);
            return height;
        }

    template <typename Data, typename Key, class Comparator>
        bool SkipList2<Data, Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
            // null n is considered infinite
            return (n != nullptr) && (compare_(n->GetKey(), key) < 0);
        }

    template <typename Data, typename Key, class Comparator>
        typename SkipList2<Data, Key, Comparator>::Node*
        SkipList2<Data, Key, Comparator>::FindGreaterOrEqual(const Key& key,
                Node** prev) const {
            Node* x = head_;
            int level = GetMaxHeight() - 1;
            while (true) {
                Node* next = x->Next(level);
                if (KeyIsAfterNode(key, next)) {
                    // Keep searching in this list
                    x = next;
                } else {
                    if (prev != nullptr) prev[level] = x;
                    if (level == 0) {
                        return next;
                    } else {
                        // Switch to next list
                        level--;
                    }
                }
            }
        }

    template <typename Data, typename Key, class Comparator>
        typename SkipList2<Data, Key, Comparator>::Node*
        SkipList2<Data, Key, Comparator>::FindLessThan(const Key& key) const {
            Node* x = head_;
            int level = GetMaxHeight() - 1;
            while (true) {
                assert(x == head_ || compare_(x->GetKey(), key) < 0);
                Node* next = x->Next(level);
                if (next == nullptr || compare_(next->GetKey(), key) >= 0) {
                    if (level == 0) {
                        return x;
                    } else {
                        // Switch to next list
                        level--;
                    }
                } else {
                    x = next;
                }
            }
        }

    template <typename Data, typename Key, class Comparator>
        typename SkipList2<Data, Key, Comparator>::Node* SkipList2<Data, Key, Comparator>::FindLast()
        const {
            Node* x = head_;
            int level = GetMaxHeight() - 1;
            while (true) {
                Node* next = x->Next(level);
                if (next == nullptr) {
                    if (level == 0) {
                        return x;
                    } else {
                        // Switch to next list
                        level--;
                    }
                } else {
                    x = next;
                }
            }
        }

    template <typename Data, typename Key, class Comparator>
        SkipList2<Data, Key, Comparator>::SkipList2(Comparator cmp)
        : compare_(cmp),
        head_(NewNode(0, 0 /* any key will do */, kMaxHeight)),
        max_height_(1),
        count_(0),
        rnd_(0xdeadbeef) {
            for (int i = 0; i < kMaxHeight; i++) {
                head_->SetNext(i, nullptr);
            }
            head_->SetPrev(nullptr);
        }

    template <typename Data, typename Key, class Comparator>
        SkipList2<Data, Key, Comparator>::~SkipList2() {
            Node *node = nullptr, *temp = nullptr;
            Data data = nullptr;
            if (head_) {
                node = head_->Next(0);
                while (node != nullptr) {
                    temp = node;
                    node = node->Next(0);
                    data = temp->GetData();
                    (static_cast<SKLData<Key, Comparator>*>(data))->SetNode((void*)nullptr);
                    free(temp);
                }
                free(head_);
            }
        }

    template <typename Data, typename Key, class Comparator>
        bool SkipList2<Data, Key, Comparator>::Insert(const Key& key, Data data) {
            // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
            // here since Insert() is externally synchronized.
            Node* prev[kMaxHeight];
            Node* x = FindGreaterOrEqual(key, prev);

            // Our data structure does not allow duplicate insertion
            assert(x == nullptr || !Equal(key, x->GetKey()));
            if (x  && Equal(key, x->GetKey())) return false;

            int height = RandomHeight();
            if (height > GetMaxHeight()) {
                for (int i = GetMaxHeight(); i < height; i++) {
                    prev[i] = head_;
                }
                // It is ok to mutate max_height_ without any synchronization
                // with concurrent readers.  A concurrent reader that observes
                // the new value of max_height_ will see either the old value of
                // new level pointers from head_ (nullptr), or a new value set in
                // the loop below.  In the former case the reader will
                // immediately drop to the next level since nullptr sorts after all
                // keys.  In the latter case the reader will use the new node.
                max_height_ = height;
            }

            x = NewNode(key, data, height);
            for (int i = 0; i < height; i++) {
                // Next() suffices since we will add a barrier when
                // we publish a pointer to "x" in prev[i].
                x->SetNext(i, prev[i]->Next(i));
                prev[i]->SetNext(i, x);
            }
            x->SetPrev(prev[0]);
            if (x->Next(0)) (x->Next(0))->SetPrev(x);
            (static_cast<SKLData<Key, Comparator>*>(data))->SetNode((void*)x);
            count_++;
            return true;
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::ReplaceOrInsert(const Key& key, Data data) {
            // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
            // here since Insert() is externally synchronized.
            Node* prev[kMaxHeight];
            Node* x = FindGreaterOrEqual(key, prev);

            if (x  && Equal(key, x->GetKey())) {
                Data old_data = x->GetData();
                x->SetData(data);
                (static_cast<SKLData<Key, Comparator>*>(old_data))->SetNode((void*)nullptr);
                return old_data;
            }

            int height = RandomHeight();
            if (height > GetMaxHeight()) {
                for (int i = GetMaxHeight(); i < height; i++) {
                    prev[i] = head_;
                }
                // It is ok to mutate max_height_ without any synchronization
                // with concurrent readers.  A concurrent reader that observes
                // the new value of max_height_ will see either the old value of
                // new level pointers from head_ (nullptr), or a new value set in
                // the loop below.  In the former case the reader will
                // immediately drop to the next level since nullptr sorts after all
                // keys.  In the latter case the reader will use the new node.
                max_height_ = height;
            }

            x = NewNode(key, data, height);
            for (int i = 0; i < height; i++) {
                // Next() suffices since we will add a barrier when
                // we publish a pointer to "x" in prev[i].
                x->SetNext(i, prev[i]->Next(i));
                prev[i]->SetNext(i, x);
            }
            x->SetPrev(prev[0]);
            (static_cast<SKLData<Key, Comparator>*>(data))->SetNode((void*)x);
            count_++;
            return nullptr;
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::Remove(const Key& key) {
            Node* prev[kMaxHeight];
            Node* x = FindGreaterOrEqual(key, prev);
            if (!x) return nullptr;
            if (!Equal(key, x->GetKey())) return nullptr;
            int height = x->GetHeight();
            for (int i = 0; i < height; i++)
                prev[i]->SetNext(i, x->Next(i));
            if (x->Next(0)) (x->Next(0))->SetPrev(prev[0]);
            Data data = x->GetData();
            (static_cast<SKLData<Key, Comparator>*>(data))->SetNode((void*)nullptr);
            free(x);
            count_--;
            return data;
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::Search(const Key& key, SLSplitCtx *m_ctx) const {
            Node* x = NULL; 
            Node* prev[kMaxHeight];
            if (m_ctx) {
                for (int i = 0; i < kMaxHeight; i++) prev[i] = head_;
                x = FindGreaterOrEqual(key, prev);
            } else {
                x = FindGreaterOrEqual(key, nullptr);
            }
            if (x != nullptr && Equal(key, x->GetKey())) {
                if (m_ctx) {
                    for (int i = 0; i < kMaxHeight; i++) {
                        m_ctx->src_prev[i] = (void *)prev[i];
                        m_ctx->src_cur_node = x;
                    }
                }
                return x->GetData();
            } else {
                return nullptr;
            }
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::SearchLessThanEqual(const Key& key) const {
            Node* node = FindLessThan(key);
            if (node == head_) {
                return nullptr;
            }
            return node->GetData();
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::SearchGreaterOrEqual(const Key& key, SLSplitCtx *m_ctx) const {
            Node* x = NULL; 
            Node* prev[kMaxHeight];
            if (m_ctx) {
                for (int i = 0; i < kMaxHeight; i++) prev[i] = head_;
                x = FindGreaterOrEqual(key, prev);
            } else {
                x = FindGreaterOrEqual(key, nullptr);
            }
            if (x != nullptr) {
                if (m_ctx) {
                    for (int i = 0; i < kMaxHeight; i++) {
                        m_ctx->src_prev[i] = (void *)prev[i];
                        m_ctx->src_cur_node = x;
                    }
                }
                return x->GetData(); /* greater or equal */
            }
            return nullptr;
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::NextWithNode(Node* node, SLSplitCtx *m_ctx) const {
            Node *next = NULL;
            if (m_ctx) {
                if (m_ctx->src_cur_node) {
                    node = reinterpret_cast<Node*>(m_ctx->src_cur_node);
                    next = node->Next(0);
                } else { 
                    /*
                     * src_cun_node was removed by previous Remove() call.
                     * - src_prev[] link already updated.
                     * - we just need to update src_cur_node. 
                     */
                    next = (reinterpret_cast<Node *>(m_ctx->src_prev[0]))->Next(0);
                    m_ctx->src_cur_node = next;
                    if (next && next != head_) return next->GetData();
                    else return nullptr;
                }
            } else next = node->Next(0);

            if (next == nullptr) return nullptr;
            if (m_ctx) {
                int height = node->GetHeight();
                for(int i = 0; i < height; i++)
                    m_ctx->src_prev[i] = node;
                m_ctx->src_cur_node = next;
            }
            return next->GetData();
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::Next(Data data, SLSplitCtx *m_ctx) const {
            if (m_ctx) return NextWithNode(nullptr, m_ctx);
            if (data) {
                Node* node = (Node *)(static_cast<SKLData<Key, Comparator> *>(data))->GetNode();
                if (!node) {
                    return SearchGreaterOrEqual(data->GetKeySlice(), m_ctx);
                }
                return NextWithNode(node, nullptr);
            }
            return nullptr;
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::PrevWithNode(Node* node) const {
            Node *prev = NULL;
            prev = node->Prev();
            if (prev == nullptr || prev == head_) return nullptr;
            return prev->GetData();
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::Prev(Data data) const {
            if (data) {
                Node* node = (Node *)(static_cast<SKLData<Key, Comparator> *>(data))->GetNode();
                if (!node) {
                    return SearchLessThanEqual(data->GetKeySlice());
                }
                return PrevWithNode(node);
            }
            return nullptr;
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::First(SLSplitCtx *m_ctx) const {
            Node* node = head_->Next(0);
            if (node == nullptr) {
                return nullptr;
            }
            if (m_ctx) {
                for(int i = 0; i < kMaxHeight; i++)
                    m_ctx->src_prev[i] = head_;
                m_ctx->src_cur_node = node;
            }
            return node->GetData();
        }

    template <typename Data, typename Key, class Comparator>
        Data SkipList2<Data, Key, Comparator>::Last() const {
            Node* node = FindLast();
            if (node == nullptr || node == head_) {
                return nullptr;
            }
            return node->GetData();
        }

    template <typename Data, typename Key, class Comparator>
        bool SkipList2<Data, Key, Comparator>::Contains(const Key& key) const {
            Node* x = FindGreaterOrEqual(key, nullptr);
            if (x != nullptr && Equal(key, x->GetKey())) {
                return true;
            } else {
                return false;
            }
        }

    template<typename Data, typename Key, class Comparator>
        Key SkipList2<Data,Key,Comparator>::GetKeySlice(Data data) {
            return (static_cast<SKLData<Key, Comparator>*>(data))->GetKeySlice();
            
        }
    
    template<typename Data, typename Key, class Comparator>
        void SkipList2<Data,Key,Comparator>::Append(SLSplitCtx *m_ctx) {
            Node* x = reinterpret_cast<Node *>(m_ctx->src_cur_del_node);
            if (!x) {
                printf("try to unremoved node to insert\n");
                return;
            }
            m_ctx->src_cur_del_node = nullptr;
            int height = x->GetHeight();
            /*
             * Update link with target_next[].
             * - target_next[] keep virtual link pointers of latest inserted node.
             * - step.
             *   1. update last inserted node's link with newly appended node.
             *   2. update target_next[] virtual link pointer.
             *   3. Set previous link of newly inserted node.
             */
            for (int i = 0; i < height; i++) {
                Node *t = reinterpret_cast<Node *>(m_ctx->target_next[i]);
                if (t) t->SetNext(i, x); /* update target's next node as newly appended one*/
                m_ctx->target_next[i] = x; /* update target_next */
                x->SetNext(i, nullptr); /* clear next of newly appened one */
                if (i == 0) x->SetPrev(t); 
            }
            count_++;
            if (max_height_ <  height) max_height_ = height;
        }
    
    template<typename Data, typename Key, class Comparator>
        Data SkipList2<Data,Key,Comparator>::Remove(SLSplitCtx *m_ctx) {
            Node* x = reinterpret_cast<Node *>(m_ctx->src_cur_node);
            if (!x) return nullptr;
            int height = x->GetHeight();
            /*
             * Update link with src_prev[].
             * - src_prev[] keep virual link pointers of previous node.
             * - step.
             *   1. update prev node's link with next link of removing node.
             *   2. Set previous link of next node of remving node.
             */
            for (int i = 0; i < height; i++) {
                Node *t = reinterpret_cast<Node *>(m_ctx->src_prev[i]);
                if (t) t->SetNext(i, x->Next(i)); /* update target next as newly appended one*/
                x->SetNext(i, nullptr); /* clear next of newly appened one */
                if (i == 0 && x->Next(i)) (x->Next(i))->SetPrev(t);
            }
            count_--;
            m_ctx->src_cur_node = nullptr; /* informe it is deleted. only remove can nullify this as nullptr. */
            m_ctx->src_cur_del_node = (void *)x;
            return x->GetData();
        }
 
    template<typename Data, typename Key, class Comparator>
        void SkipList2<Data,Key,Comparator>::InitSplitTarget(SLSplitCtx *m_ctx) {
            /* initialize target next []  with skiplist. assume it is empty() */
            assert(!GetCount());
            for (int i = 0; i < kMaxHeight; i++)
               m_ctx->target_next[i] = (void *)head_; 
        }

}  // namespace insdb

#endif  // STORAGE_INSDB_DB_SKIPLIST2_H_
