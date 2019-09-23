#pragma once

#include "rocksdb/kvdb_config.h"
#include "insdb/db.h"
#include "insdb/write_batch.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace rocksdb {

extern insdb::WriteBatch *WrapperToInsdbWriteBatch(void *wrapper_context);
extern WriteType convert_insdb_write_type(insdb::WriteType write_type);
extern Status convert_insdb_status(insdb::Status status);
extern insdb::Status convert_to_insdb_status(Status status);
extern insdb::WriteOptions convert_write_options_to_insdb(WriteOptions write_options);
extern insdb::ReadOptions convert_read_options_to_insdb(ReadOptions read_options);

//#define KVDB_ENABLE_IOTRACE
#ifdef KVDB_ENABLE_IOTRACE

extern void init_iotrace();
extern void flush_iotrace();
extern void log_iotrace(const char *opname, uint32_t colid, const char *keyname, uint32_t keysize, uint32_t valuesize);

#endif

}
