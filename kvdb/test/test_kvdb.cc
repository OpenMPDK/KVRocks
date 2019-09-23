#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>


#include "insdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "db/db_impl.h"
#include "rocksdb/utilities/options_util.h"

#define workload_check (0)
#define METABUF_MAXLEN (256)
extern int64_t DATABUF_MAXLEN;

using namespace rocksdb;
using namespace std;

DB *db = NULL;

class TestLogger : public Logger {
    using Logger::Logv;
    void Logv(const char* format, va_list ap) override {
        vprintf(format, ap);
        putchar('\n');
    }
};

int main(int argc, char **argv) {
    std::shared_ptr<Logger> logger( new TestLogger() );
    Options options;
    // not a real path, just a unique name
    std::string dbname("/tmp/test_kvdb10");
    Status status;

    status = DestroyDB(dbname, options);

    cout << "KVDB test started" << endl;

    // Block cache setting
    BlockBasedTableOptions table_options;
    table_options.block_cache = NewLRUCache(16384, 6, false, 0.0);
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    options.table_factory.reset(
        NewBlockBasedTableFactory(table_options));

    // create the DB if it's not already present
    options.create_if_missing = true;

    // Logger
    options.info_log = logger;
    options.create_missing_column_families = true;
    options.fail_if_options_file_error = true;

    // List column
    std::vector<std::string> column_families_names;
    status = DB::ListColumnFamilies(static_cast<DBOptions>(options), dbname, &column_families_names);
    if(!status.ok())
        cout << "DB ListColumnFamilies " << status.ToString() << " errno " << errno << endl;
    else
    {
        cout << "DB ListColumnFamilies " << status.ToString() << " count " << column_families_names.size() << endl;
        for (auto each_cf : column_families_names)
        {
            cout << each_cf.c_str() << endl;
        }
        cout << endl;
    }


    // open DB without column family
    status = DB::Open(options, dbname, &db);
    cout << "DB open " << status.ToString() << endl;
    assert(status.ok());

    // close DB
    delete db;
    cout << "DB closed" << endl;

    // List column
    column_families_names.clear();
    status = DB::ListColumnFamilies(static_cast<DBOptions>(options), dbname, &column_families_names);
    if(!status.ok())
        cout << "DB ListColumnFamilies " << status.ToString() << " errno " << errno << endl;
    else
    {
        cout << "DB ListColumnFamilies " << status.ToString() << " count " << column_families_names.size() << endl;
        for (auto each_cf : column_families_names)
        {
            cout << each_cf.c_str() << endl;
        }
        cout << endl;
    }

    // Load options.
    // default column family gets loaded created in the previous open.
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    DBOptions dboptions(options);
    status = LoadLatestOptions(dbname, rocksdb::Env::Default(), &dboptions,
                            &column_families, false);
    cout << "LoadLatestOptions " << status.ToString() << endl;
    assert(status.ok());

    // open DB with default column family
    std::vector<ColumnFamilyHandle*> handles;
    status = DB::Open(dboptions, dbname, column_families, &handles, &db);
    cout << "DB open with default column family " << status.ToString() << endl;
    assert(status.ok());
    assert(handles.size()==1);
    assert(handles[0]->GetID()==0);
    assert(kDefaultColumnFamilyName.compare(handles[0]->GetName()) == 0);

    db->DestroyColumnFamilyHandle(handles[0]);
    delete db;
    cout << "DB closed" << endl;

    // open DB with cf1 column family
    ColumnFamilyOptions cfoptions;
    ColumnFamilyDescriptor cf1("cf1", cfoptions);
    column_families.push_back(cf1);
    status = DB::Open(dboptions, dbname, column_families, &handles, &db);
    cout << "DB open with cf1 column family " << status.ToString() << endl;
    assert(status.ok());
    assert(handles.size()==2);
    assert(handles[0]->GetID()==0);
    assert(kDefaultColumnFamilyName.compare(handles[0]->GetName()) == 0);
    assert(handles[1]->GetID()==1);
    assert(std::string("cf1").compare(handles[1]->GetName()) == 0);

    // Delete cf1
    status = db->DropColumnFamily(handles[1]);
    assert(status.ok());
    db->DestroyColumnFamilyHandle(handles[1]);
    cout << "cf1 is destroyed " << status.ToString() << endl;

    ColumnFamilyHandle *cf2;
    status = db->CreateColumnFamily(cfoptions, "cf2", &cf2);
    assert(status.ok());
    // because cf1 is deleted, its ID should be reused for cf2.
    assert(cf2->GetID() == 1);

#if 0
    ColumnFamilyHandle *cf3;
    status = db->CreateColumnFamily(cfoptions, "cf3", &cf3);
    assert(status.ok());

    ColumnFamilyHandle *cf3_dup;
    status = db->CreateColumnFamily(cfoptions, "cf3", &cf3_dup);
    assert(!status.ok());

    db->DestroyColumnFamilyHandle(cf3);
    }
#endif

    // Leave 3 column families for the next test.
    // Close DB
    db->DestroyColumnFamilyHandle(handles[0]);
    db->DestroyColumnFamilyHandle(cf2);
    delete db;
    cout << "DB closed" << endl;

    // Load options.
    // default, 'cf2' and 'cf3' column family gets loaded created in the previous open.
    column_families.clear();
    status = LoadLatestOptions(dbname, rocksdb::Env::Default(), &dboptions,
                            &column_families, false);
    cout << "LoadLatestOptions " << status.ToString() << endl;
    assert(status.ok());
#if 0
    assert(column_families.size() == 3);
#else
    assert(column_families.size() == 2);
#endif

    // Open the DB with three column families
    handles.clear();
    status = DB::Open(dboptions, dbname, column_families, &handles, &db);
    assert(status.ok());
#if 0
    assert(handles.size()==3);
#else
    assert(handles.size()==2);
#endif
    // put key-value
    status = db->Put(WriteOptions(), "key2", "value");
    cout << "Put " << status.ToString() << endl;
    assert(status.ok());

    // get value
    std::string value;
    status = db->Get(ReadOptions(), "key2", &value);
    cout << "Get " << status.ToString() << endl;
    if (status.ok())
    	cout << "  Value: " << value << endl;
    assert(status.ok());
    assert(value == "value");

    // delete key
    status = db->Delete(WriteOptions(), "key2");
    cout << "Delete " << status.ToString() << endl;
    assert(status.ok());

    // put key-value with column 1
    status = db->Put(WriteOptions(), handles[1], "key2", "value");
    cout << "Put in CF " << status.ToString() << endl;
    assert(status.ok());

    // get value
    PinnableSlice pin_value;
    status = db->Get(ReadOptions(), handles[1], "key2", &pin_value);
    cout << "Get with PinnableSlice in CF " << status.ToString() << endl;
    if (status.ok())
        cout << "  Value: " << pin_value.ToString() << endl;
    assert(status.ok());
    assert(pin_value.ToString() == "value");
    pin_value.Reset();

    // delete key
    status = db->Delete(WriteOptions(), handles[1], "key2");
    cout << "Delete in CF " << status.ToString() << endl;
    assert(status.ok());

    // atomically apply a set of updates
	WriteBatch batch;

	batch.Put("key1", "value_for_key1");
	batch.Delete("key1");
	batch.Put("key3", "value_for_key3");

	batch.Put(handles[1], "col1_key1", "value_for_col1_key1");
    batch.Delete(handles[1], "col1_key1");
    batch.Put(handles[1], "col1_key3", "value_for_col1_key3");

#if 0
    batch.Put(handles[2], "col2_key1", "value_for_col2_key1");
    batch.Delete(handles[2], "col2_key1");
    batch.Put(handles[2], "col2_key3", "value_for_col2_key3");
#endif

    // Put some key/values to column families
    batch.Put(handles[1], "col1_keyA", "value_for_col1_keyA");
    batch.Put(handles[1], "col1_keyB", "value_for_col1_keyB");
    batch.Put(handles[1], "col1_keyC", "value_for_col1_keyC");
    batch.Put(handles[1], "col1_keyD", "value_for_col1_keyD");
    batch.Put(handles[1], "col1_keyE", "value_for_col1_keyE");
#if 0
    batch.Put(handles[2], "col2_keyA", "value_for_col2_keyA");
    batch.Put(handles[2], "col2_keyB", "value_for_col2_keyB");
    batch.Put(handles[2], "col2_keyC", "value_for_col2_keyC");
    batch.Put(handles[2], "col2_keyD", "value_for_col2_keyD");
    batch.Put(handles[2], "col2_keyE", "value_for_col2_keyE");
#endif


	// print out write batch
	cout << "Iterate write batch" << endl;
	class batchHandler : public WriteBatch::Handler {
	    Status PutCF(uint32_t column_family_id, const Slice& key, const Slice& value)
		{
	    	cout << "Col " << column_family_id << " Put " << key.ToString() << " value" << value.ToString() << endl;
	    	return Status::OK();
		}
	    Status DeleteCF(uint32_t column_family_id, const Slice& key) {
	    	cout << "Col " << column_family_id << " Delete " << key.ToString() << endl;
            return Status::OK();
	    }
	};
	batch.Iterate(new batchHandler());

	status = db->Write(WriteOptions(), &batch);
    cout << "Write " << status.ToString() << endl;
    assert(status.ok());

    status = db->Get(ReadOptions(), "key1", &value);
    cout << "Get " << status.ToString() << endl;
    assert(status.IsNotFound());

    status = db->Get(ReadOptions(), "key3", &value);
    cout << "Get " << status.ToString() << endl;
    if (status.ok())
    	cout << "  Value: " << value << endl;
    assert(status.ok());
    assert(value == "value_for_key3");

    // DB snapshot open
    const Snapshot *ss = db->GetSnapshot();
    cout << "Snapshot: seq " << ss->GetSequenceNumber() << endl;

    // DB iterate

    for(auto cfhandle : handles)
    {
        // Put and delete some keys, which should not affect iteration.
        status = db->Put(WriteOptions(), cfhandle, "key1_after_snapshot", "value1");
        assert(status.ok());
        status = db->Put(WriteOptions(), cfhandle, "key2_after_snapshot", "value2");
        assert(status.ok());
        if(cfhandle->GetName().compare("default"))
            status = db->Delete(WriteOptions(), "key3");
        if(cfhandle->GetName().compare("cf2"))
            status = db->Delete(WriteOptions(), "col1_key3");
        if(cfhandle->GetName().compare("cf3"))
            status = db->Delete(WriteOptions(), "col2_key3");
        assert(status.ok());

        cout << endl << "Iterate " << cfhandle->GetName() << endl;

        ReadOptions readoptions;
        readoptions.snapshot = ss;
        rocksdb::Iterator* rit = db->NewIterator(readoptions, cfhandle);
        rocksdb::Slice startkey = rocksdb::Slice("col");

        rit->Seek(startkey);

        while (rit->Valid()) {
            cout << "Key: " << rit->key().ToString() << " Value: " << rit->value().ToString() << endl;
            assert(rit->key().ToString().compare("key1_after_snapshot") != 0);
            assert(rit->key().ToString().compare("key2_after_snapshot") != 0);
            rit->Next();
        }

        delete rit;
    }

    // DB snapshot close
    db->ReleaseSnapshot(ss);

    // close DB
    db->DestroyColumnFamilyHandle(handles[0]);
    db->DestroyColumnFamilyHandle(handles[1]);
#if 0
    db->DestroyColumnFamilyHandle(handles[2]);
#endif
    delete db;

    status = DestroyDB(dbname, options);
    assert(status.ok());

    cout << "KVDB test done" << endl;
    return 0;
}

