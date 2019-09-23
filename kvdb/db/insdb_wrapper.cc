#include <time.h>
#include <sys/time.h>
#include <memory>
#include "db/insdb_wrapper.h"
#include "db/dbformat.h"
#include "db/snapshot_impl.h"


namespace rocksdb {

insdb::WriteBatch *WrapperToInsdbWriteBatch(void *wrapper_context)
{
    return (insdb::WriteBatch *)wrapper_context;
}

WriteType convert_insdb_write_type(insdb::WriteType write_type)
{
    switch(write_type)
    {
    case insdb::kPutRecord: return kPutRecord;
    case insdb::kMergeRecord: return kMergeRecord;
    case insdb::kDeleteRecord: return kDeleteRecord;
    case insdb::kSingleDeleteRecord: return kSingleDeleteRecord;
    case insdb::kDeleteRangeRecord: return kDeleteRangeRecord;
    case insdb::kLogDataRecord: return kLogDataRecord;
    case insdb::kXIDRecord: return kXIDRecord;
    default:
        throw std::runtime_error("unknown write type");
    }
}

//
// Convert insdb status to rocksdb status
//

Status convert_insdb_status(insdb::Status status)
{
	if (status.ok())
		return Status::OK();
	else if (status.IsNotFound())
		return Status::NotFound(status.ToString());
	else if (status.IsCorruption())
		return Status::Corruption(status.ToString());
	else if (status.IsIOError())
		return Status::IOError(status.ToString());
	else if (status.IsNotSupportedError())
		return Status::NotSupported(status.ToString());
	// no matching error code is found. return TimedOut instead
	else if (status.IsNotAvailable())
		return Status::TimedOut(status.ToString());
	else if (status.IsInvalidArgument())
		return Status::InvalidArgument(status.ToString());

	// return Incomplete if the code is unknown
	return Status::Incomplete(status.ToString());
}

//
// Convert rocksdb status to insdb status
//

insdb::Status convert_to_insdb_status(Status status)
{
    if (status.ok())
        return insdb::Status::OK();
    else if (status.IsNotFound())
        return insdb::Status::NotFound(status.ToString());
    else if (status.IsCorruption())
        return insdb::Status::Corruption(status.ToString());
    else if (status.IsIOError())
        return insdb::Status::IOError(status.ToString());
    else if (status.IsNotSupported())
        return insdb::Status::NotSupported(status.ToString());
    // no matching error code is found. return TimedOut instead
    else if (status.IsTimedOut())
        return insdb::Status::NotAvailable(status.ToString());
    else if (status.IsInvalidArgument())
        return insdb::Status::InvalidArgument(status.ToString());

    // return Incomplete if the code is unknown
    return insdb::Status::IOError(status.ToString());
}

insdb::WriteOptions convert_write_options_to_insdb(WriteOptions write_options)
{

    insdb::WriteOptions insdb_write_options;
    insdb_write_options.sync = write_options.sync;

    return insdb_write_options;
}

insdb::ReadOptions convert_read_options_to_insdb(ReadOptions read_options)
{
    insdb::ReadOptions insdb_read_options;
    insdb_read_options.verify_checksums = read_options.verify_checksums;
    insdb_read_options.fill_cache = read_options.fill_cache;
    if (read_options.snapshot)
    {
        const SnapshotImpl* casted_s = reinterpret_cast<const SnapshotImpl*>(read_options.snapshot);
        insdb_read_options.snapshot = casted_s->insdb_snapshot_;
    }

    return insdb_read_options;
}

#ifdef KVDB_ENABLE_IOTRACE

bool iotrace_inited = false;
unique_ptr<WritableFile> IoTraceFile;

// init IO trace
void init_iotrace()
{
    if (iotrace_inited)
        return;
    // Create a file name using the current time
   int strbuf_len = 128;
   char strbuf[strbuf_len];
   time_t rawtime;
   struct tm *timeinfo;
   time (&rawtime);
   timeinfo = localtime(&rawtime);
   int char_idx = strftime(strbuf, strbuf_len, "/tmp/kvdb_iotrace-%b%d-%T-%Y.csv", timeinfo);
   assert(char_idx >= 0);

   Env *env = Env::Default();
   EnvOptions envopt;
   Status status = env->NewWritableFile(strbuf, &IoTraceFile, envopt);
   assert(status.ok());


   IoTraceFile->Append(Slice("thread,time,column ID,operation,key name,key name size, value size\n"));

   iotrace_inited = true;
}

// flush IO trace
void flush_iotrace()
{
    if(iotrace_inited)
    {
        IoTraceFile->Flush();
        IoTraceFile->Sync();
    }
}

// log an io trace
void log_iotrace(const char *opname, uint32_t colid, const char *keyname, uint32_t keysize, uint32_t valuesize)
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    uint64_t now = static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;

    int strbuf_len = keysize*2+128;
    char strbuf[strbuf_len];
    char *buf_loc = strbuf;
    int char_idx = snprintf(buf_loc, strbuf_len, "%lu,%lu,%u,%s,", now, Env::Default()->GetThreadID(), colid, opname);
    assert (char_idx >= 1);

    buf_loc += char_idx;
    strbuf_len -= char_idx;
    assert ( strbuf_len >= 1);

    for (uint32_t i = 0; i < keysize; i++)
    {
        char_idx = snprintf(buf_loc, strbuf_len, "%02x", (unsigned char)keyname[i]);
        assert ( char_idx >= 1);
        buf_loc += char_idx;
        strbuf_len -= char_idx;
        assert ( strbuf_len >= 1);
    }
    char_idx = snprintf(buf_loc, strbuf_len, ",%u,%u\n", keysize, valuesize);
    assert ( char_idx >= 1);
    buf_loc += char_idx;

    IoTraceFile->Append(Slice(strbuf, buf_loc - strbuf));
}

#endif

}
