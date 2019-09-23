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


#include "insdb/c.h"

#include <stdlib.h>
#include <unistd.h>
#include "insdb/cache.h"
#include "insdb/comparator.h"
#include "insdb/db.h"
#include "insdb/env.h"
#include "insdb/filter_policy.h"
#include "insdb/iterator.h"
#include "insdb/options.h"
#include "insdb/status.h"
#include "insdb/write_batch.h"

using insdb::Cache;
using insdb::Comparator;
using insdb::CompressionType;
using insdb::DB;
using insdb::Env;
using insdb::FileLock;
using insdb::FilterPolicy;
using insdb::Iterator;
using insdb::kMajorVersion;
using insdb::kMinorVersion;
using insdb::Logger;
using insdb::NewBloomFilterPolicy;
using insdb::NewLRUCache;
using insdb::Options;
using insdb::RandomAccessFile;
using insdb::Range;
using insdb::ReadOptions;
using insdb::SequentialFile;
using insdb::Slice;
using insdb::Snapshot;
using insdb::Status;
using insdb::WritableFile;
using insdb::WriteBatch;
using insdb::WriteOptions;

extern "C" {

    struct insdb_t              { DB*               rep; };
    struct insdb_iterator_t     { Iterator*         rep; };
    struct insdb_writebatch_t   { WriteBatch        rep; };
    struct insdb_snapshot_t     { const Snapshot*   rep; };
    struct insdb_readoptions_t  { ReadOptions       rep; };
    struct insdb_writeoptions_t { WriteOptions      rep; };
    struct insdb_options_t      { Options           rep; };
    struct insdb_cache_t        { Cache*            rep; };
    struct insdb_seqfile_t      { SequentialFile*   rep; };
    struct insdb_randomfile_t   { RandomAccessFile* rep; };
    struct insdb_writablefile_t { WritableFile*     rep; };
    struct insdb_logger_t       { Logger*           rep; };
    struct insdb_filelock_t     { FileLock*         rep; };

    struct insdb_comparator_t : public Comparator {
        void* state_;
        void (*destructor_)(void*);
        int (*compare_)(
                void*,
                const char* a, size_t alen,
                const char* b, size_t blen);
        const char* (*name_)(void*);

        virtual ~insdb_comparator_t() {
            (*destructor_)(state_);
        }

        virtual int Compare(const Slice& a, const Slice& b) const {
            return (*compare_)(state_, a.data(), a.size(), b.data(), b.size());
        }

        virtual const char* Name() const {
            return (*name_)(state_);
        }

        // No-ops since the C binding does not support key shortening methods.
        virtual void FindShortestSeparator(std::string*, const Slice&) const { }
        virtual void FindShortSuccessor(std::string* key) const { }
    };

    struct insdb_filterpolicy_t : public FilterPolicy {
        void* state_;
        void (*destructor_)(void*);
        const char* (*name_)(void*);
        char* (*create_)(
                void*,
                const char* const* key_array, const size_t* key_length_array,
                int num_keys,
                size_t* filter_length);
        unsigned char (*key_match_)(
                void*,
                const char* key, size_t length,
                const char* filter, size_t filter_length);

        virtual ~insdb_filterpolicy_t() {
            (*destructor_)(state_);
        }

        virtual const char* Name() const {
            return (*name_)(state_);
        }

        virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const {
            std::vector<const char*> key_pointers(n);
            std::vector<size_t> key_sizes(n);
            for (int i = 0; i < n; i++) {
                key_pointers[i] = keys[i].data();
                key_sizes[i] = keys[i].size();
            }
            size_t len;
            char* filter = (*create_)(state_, &key_pointers[0], &key_sizes[0], n, &len);
            dst->append(filter, len);
            free(filter);
        }

        virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const {
            return (*key_match_)(state_, key.data(), key.size(),
                    filter.data(), filter.size());
        }
    };

    struct insdb_env_t {
        Env* rep;
        bool is_default;
    };

    static bool SaveError(char** errptr, const Status& s) {
        assert(errptr != NULL);
        if (s.ok()) {
            return false;
        } else if (*errptr == NULL) {
            *errptr = strdup(s.ToString().c_str());
        } else {
            // TODO(sanjay): Merge with existing error?
            free(*errptr);
            *errptr = strdup(s.ToString().c_str());
        }
        return true;
    }

    static char* CopyString(const std::string& str) {
        char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
        memcpy(result, str.data(), sizeof(char) * str.size());
        return result;
    }

    insdb_t* insdb_open(
            const insdb_options_t* options,
            const char* name,
            char** errptr) {
        DB* db;
        if (SaveError(errptr, DB::Open(options->rep, std::string(name), &db))) {
            return NULL;
        }
        insdb_t* result = new insdb_t;
        result->rep = db;
        return result;
    }

    void insdb_close(insdb_t* db) {
        delete db->rep;
        delete db;
    }

    void insdb_put(
            insdb_t* db,
            const insdb_writeoptions_t* options,
            const char* key, size_t keylen,
            const char* val, size_t vallen,
            char** errptr) {
        SaveError(errptr,
                db->rep->Put(options->rep, Slice(key, keylen), Slice(val, vallen)));
    }

    void insdb_delete(
            insdb_t* db,
            const insdb_writeoptions_t* options,
            const char* key, size_t keylen,
            char** errptr) {
        SaveError(errptr, db->rep->Delete(options->rep, Slice(key, keylen)));
    }


    void insdb_write(
            insdb_t* db,
            const insdb_writeoptions_t* options,
            insdb_writebatch_t* batch,
            char** errptr) {
        SaveError(errptr, db->rep->Write(options->rep, &batch->rep));
    }

    char* insdb_get(
            insdb_t* db,
            const insdb_readoptions_t* options,
            const char* key, size_t keylen,
            size_t* vallen,
            char** errptr) {
        char* result = NULL;
        std::string tmp;
        Status s = db->rep->Get(options->rep, Slice(key, keylen), &tmp);
        if (s.ok()) {
            *vallen = tmp.size();
            result = CopyString(tmp);
        } else {
            *vallen = 0;
            if (!s.IsNotFound()) {
                SaveError(errptr, s);
            }
        }
        return result;
    }

    insdb_iterator_t* insdb_create_iterator(
            insdb_t* db,
            const insdb_readoptions_t* options) {
        insdb_iterator_t* result = new insdb_iterator_t;
        result->rep = db->rep->NewIterator(options->rep);
        return result;
    }

    const insdb_snapshot_t* insdb_create_snapshot(
            insdb_t* db) {
        insdb_snapshot_t* result = new insdb_snapshot_t;
        result->rep = db->rep->GetSnapshot();
        return result;
    }

    void insdb_release_snapshot(
            insdb_t* db,
            const insdb_snapshot_t* snapshot) {
        db->rep->ReleaseSnapshot(snapshot->rep);
        delete snapshot;
    }

    char* insdb_property_value(
            insdb_t* db,
            const char* propname) {
        std::string tmp;
        if (db->rep->GetProperty(Slice(propname), &tmp)) {
            // We use strdup() since we expect human readable output.
            return strdup(tmp.c_str());
        } else {
            return NULL;
        }
    }

    void insdb_approximate_sizes(
            insdb_t* db,
            int num_ranges,
            const char* const* range_start_key, const size_t* range_start_key_len,
            const char* const* range_limit_key, const size_t* range_limit_key_len,
            uint64_t* sizes) {
        Range* ranges = new Range[num_ranges];
        for (int i = 0; i < num_ranges; i++) {
            ranges[i].start = Slice(range_start_key[i], range_start_key_len[i]);
            ranges[i].limit = Slice(range_limit_key[i], range_limit_key_len[i]);
        }
        db->rep->GetApproximateSizes(ranges, num_ranges, sizes);
        delete[] ranges;
    }
    /*
       void insdb_compact_range(
       insdb_t* db,
       const char* start_key, size_t start_key_len,
       const char* limit_key, size_t limit_key_len) {
       Slice a, b;
       db->rep->CompactRange(
// Pass NULL Slice if corresponding "const char*" is NULL
(start_key ? (a = Slice(start_key, start_key_len), &a) : NULL),
(limit_key ? (b = Slice(limit_key, limit_key_len), &b) : NULL));
}
*/
void insdb_destroy_db(
        const insdb_options_t* options,
        const char* name,
        char** errptr) {
    SaveError(errptr, DestroyDB(name, options->rep));
}

void insdb_repair_db(
        const insdb_options_t* options,
        const char* name,
        char** errptr) {
    SaveError(errptr, RepairDB(name, options->rep));
}

void insdb_iter_destroy(insdb_iterator_t* iter) {
    delete iter->rep;
    delete iter;
}

unsigned char insdb_iter_valid(const insdb_iterator_t* iter) {
    return iter->rep->Valid();
}

void insdb_iter_seek_to_first(insdb_iterator_t* iter) {
    iter->rep->SeekToFirst();
}

void insdb_iter_seek_to_last(insdb_iterator_t* iter) {
    iter->rep->SeekToLast();
}

void insdb_iter_seek(insdb_iterator_t* iter, const char* k, size_t klen) {
    iter->rep->Seek(Slice(k, klen));
}

void insdb_iter_next(insdb_iterator_t* iter) {
    iter->rep->Next();
}

void insdb_iter_prev(insdb_iterator_t* iter) {
    iter->rep->Prev();
}

const char* insdb_iter_key(const insdb_iterator_t* iter, size_t* klen) {
    Slice s = iter->rep->key();
    *klen = s.size();
    return s.data();
}

const char* insdb_iter_value(const insdb_iterator_t* iter, size_t* vlen) {
    Slice s = iter->rep->value();
    *vlen = s.size();
    return s.data();
}

void insdb_iter_get_error(const insdb_iterator_t* iter, char** errptr) {
    SaveError(errptr, iter->rep->status());
}

insdb_writebatch_t* insdb_writebatch_create() {
    return new insdb_writebatch_t;
}

void insdb_writebatch_destroy(insdb_writebatch_t* b) {
    delete b;
}

void insdb_writebatch_clear(insdb_writebatch_t* b) {
    b->rep.Clear();
}

void insdb_writebatch_put(
        insdb_writebatch_t* b,
        const char* key, size_t klen,
        const char* val, size_t vlen) {
    b->rep.Put(Slice(key, klen), Slice(val, vlen));
}

void insdb_writebatch_merge(
        insdb_writebatch_t* b,
        const char* key, size_t klen,
        const char* val, size_t vlen) {
    b->rep.Merge(Slice(key, klen), Slice(val, vlen));
}

void insdb_writebatch_delete(
        insdb_writebatch_t* b,
        const char* key, size_t klen) {
    b->rep.Delete(Slice(key, klen));
}

void insdb_writebatch_iterate(
        insdb_writebatch_t* b,
        void* state,
        void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
        void (*merge)(void*, const char* k, size_t klen, const char* v, size_t vlen),
        void (*deleted)(void*, const char* k, size_t klen)) {
    class H : public WriteBatch::Handler {
        public:
            void* state_;
            void (*put_)(void*, const char* k, size_t klen, const char* v, size_t vlen);
            void (*merge_)(void*, const char* k, size_t klen, const char* v, size_t vlen);
            void (*deleted_)(void*, const char* k, size_t klen);
            virtual void Put(const Slice& key, const Slice& value, const uint16_t col_id = 0) {
                (*put_)(state_, key.data(), key.size(), value.data(), value.size());
            }
            virtual Status Merge(const Slice& key, const Slice& value, const uint16_t col_id = 0) {
                (*merge_)(state_, key.data(), key.size(), value.data(), value.size());
                return Status::OK();
            }
            virtual void Delete(const Slice& key, const uint16_t col_id = 0 ) {
                (*deleted_)(state_, key.data(), key.size());
            }
    };
    H handler;
    handler.state_ = state;
    handler.put_ = put;
    handler.merge_ = merge;
    handler.deleted_ = deleted;
    b->rep.Iterate(&handler);
}

insdb_options_t* insdb_options_create() {
    return new insdb_options_t;
}

void insdb_options_destroy(insdb_options_t* options) {
    delete options;
}

void insdb_options_set_comparator(
        insdb_options_t* opt,
        insdb_comparator_t* cmp) {
    opt->rep.comparator = cmp;
}

void insdb_options_set_filter_policy(
        insdb_options_t* opt,
        insdb_filterpolicy_t* policy) {
    opt->rep.filter_policy = policy;
}

void insdb_options_set_create_if_missing(
        insdb_options_t* opt, unsigned char v) {
    opt->rep.create_if_missing = v;
}

void insdb_options_set_error_if_exists(
        insdb_options_t* opt, unsigned char v) {
    opt->rep.error_if_exists = v;
}

void insdb_options_set_paranoid_checks(
        insdb_options_t* opt, unsigned char v) {
    opt->rep.paranoid_checks = v;
}

void insdb_options_set_env(insdb_options_t* opt, insdb_env_t* env) {
    opt->rep.env = (env ? env->rep : NULL);
}

void insdb_options_set_info_log(insdb_options_t* opt, insdb_logger_t* l) {
    opt->rep.info_log = (l ? l->rep : NULL);
}

void insdb_options_set_write_buffer_size(insdb_options_t* opt, size_t s) {
    opt->rep.write_buffer_size = s;
}

void insdb_options_set_max_open_files(insdb_options_t* opt, int n) {
    opt->rep.max_open_files = n;
}

void insdb_options_set_cache(insdb_options_t* opt, insdb_cache_t* c) {
    opt->rep.block_cache = c->rep;
}

void insdb_options_set_block_size(insdb_options_t* opt, size_t s) {
    opt->rep.block_size = s;
}

void insdb_options_set_block_restart_interval(insdb_options_t* opt, int n) {
    opt->rep.block_restart_interval = n;
}

void insdb_options_set_max_file_size(insdb_options_t* opt, size_t s) {
    opt->rep.max_file_size = s;
}

void insdb_options_set_compression(insdb_options_t* opt, int t) {
    opt->rep.compression = static_cast<CompressionType>(t);
}

insdb_comparator_t* insdb_comparator_create(
        void* state,
        void (*destructor)(void*),
        int (*compare)(
            void*,
            const char* a, size_t alen,
            const char* b, size_t blen),
        const char* (*name)(void*)) {
    insdb_comparator_t* result = new insdb_comparator_t;
    result->state_ = state;
    result->destructor_ = destructor;
    result->compare_ = compare;
    result->name_ = name;
    return result;
}

void insdb_comparator_destroy(insdb_comparator_t* cmp) {
    delete cmp;
}

insdb_filterpolicy_t* insdb_filterpolicy_create(
        void* state,
        void (*destructor)(void*),
        char* (*create_filter)(
            void*,
            const char* const* key_array, const size_t* key_length_array,
            int num_keys,
            size_t* filter_length),
        unsigned char (*key_may_match)(
            void*,
            const char* key, size_t length,
            const char* filter, size_t filter_length),
        const char* (*name)(void*)) {
    insdb_filterpolicy_t* result = new insdb_filterpolicy_t;
    result->state_ = state;
    result->destructor_ = destructor;
    result->create_ = create_filter;
    result->key_match_ = key_may_match;
    result->name_ = name;
    return result;
}

void insdb_filterpolicy_destroy(insdb_filterpolicy_t* filter) {
    delete filter;
}

insdb_filterpolicy_t* insdb_filterpolicy_create_bloom(int bits_per_key) {
    // Make a insdb_filterpolicy_t, but override all of its methods so
    // they delegate to a NewBloomFilterPolicy() instead of user
    // supplied C functions.
    struct Wrapper : public insdb_filterpolicy_t {
        const FilterPolicy* rep_;
        ~Wrapper() { delete rep_; }
        const char* Name() const { return rep_->Name(); }
        void CreateFilter(const Slice* keys, int n, std::string* dst) const {
            return rep_->CreateFilter(keys, n, dst);
        }
        bool KeyMayMatch(const Slice& key, const Slice& filter) const {
            return rep_->KeyMayMatch(key, filter);
        }
        static void DoNothing(void*) { }
    };
    Wrapper* wrapper = new Wrapper;
    wrapper->rep_ = NewBloomFilterPolicy(bits_per_key);
    wrapper->state_ = NULL;
    wrapper->destructor_ = &Wrapper::DoNothing;
    return wrapper;
}

insdb_readoptions_t* insdb_readoptions_create() {
    return new insdb_readoptions_t;
}

void insdb_readoptions_destroy(insdb_readoptions_t* opt) {
    delete opt;
}

void insdb_readoptions_set_verify_checksums(
        insdb_readoptions_t* opt,
        unsigned char v) {
    opt->rep.verify_checksums = v;
}

void insdb_readoptions_set_fill_cache(
        insdb_readoptions_t* opt, unsigned char v) {
    opt->rep.fill_cache = v;
}

void insdb_readoptions_set_snapshot(
        insdb_readoptions_t* opt,
        const insdb_snapshot_t* snap) {
    opt->rep.snapshot = (snap ? snap->rep : NULL);
}

insdb_writeoptions_t* insdb_writeoptions_create() {
    return new insdb_writeoptions_t;
}

void insdb_writeoptions_destroy(insdb_writeoptions_t* opt) {
    delete opt;
}

void insdb_writeoptions_set_sync(
        insdb_writeoptions_t* opt, unsigned char v) {
    opt->rep.sync = v;
}

insdb_cache_t* insdb_cache_create_lru(size_t capacity) {
    insdb_cache_t* c = new insdb_cache_t;
    c->rep = NewLRUCache(capacity);
    return c;
}

void insdb_cache_destroy(insdb_cache_t* cache) {
    delete cache->rep;
    delete cache;
}

insdb_env_t* insdb_create_default_env() {
    insdb_env_t* result = new insdb_env_t;
    result->rep = Env::Default();
    result->is_default = true;
    return result;
}

void insdb_env_destroy(insdb_env_t* env) {
    if (!env->is_default) delete env->rep;
    delete env;
}

void insdb_free(void* ptr) {
    free(ptr);
}

int insdb_major_version() {
    return kMajorVersion;
}

int insdb_minor_version() {
    return kMinorVersion;
}

}  // end extern "C"
