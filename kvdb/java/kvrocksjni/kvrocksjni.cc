#include "jni.h"
#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>
#include <fcntl.h>

#include "../include/org_kvrocksdb_KVRocksDB.h"
#include "../include/org_kvrocksdb_KVRocksIterator.h"
#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
/*
 * Class:     org_kvrocksdb_KVRocksDB
 * Method:    nativeopen
 * Signature: (Ljava/lang/String;)J
 */

jlong rocksdb_open_helper(JNIEnv* env, jstring jdb_path,
    std::function<rocksdb::Status(
      const rocksdb::Options&, const std::string&, rocksdb::DB**)> open_fn
    ) {
  rocksdb::Options options;
  options.create_if_missing = true;
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if(db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  rocksdb::DB* db = nullptr;
  rocksdb::Status s = open_fn(options, db_path, &db);

  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    return reinterpret_cast<jlong>(db);
  } else {
    printf("open db failed %s\n", s.ToString().c_str());
    return 0;
  }
}

JNIEXPORT jlong JNICALL Java_org_kvrocksdb_KVRocksDB_openDB
  (JNIEnv *env, jclass jcls, jstring jdb_path){
  return rocksdb_open_helper(env, jdb_path,
    (rocksdb::Status(*)
      (const rocksdb::Options&, const std::string&, rocksdb::DB**)
    )&rocksdb::DB::Open);
  }

static jbyteArray copyBytes(JNIEnv* env, std::string bytes) {
  const jsize jlen = static_cast<jsize>(bytes.size());
  jbyteArray jbytes = env->NewByteArray(jlen);
  if(jbytes == nullptr) {
    return nullptr;
  }

  env->SetByteArrayRegion(jbytes, 0, jlen,
    const_cast<jbyte*>(reinterpret_cast<const jbyte*>(bytes.c_str())));
  if(env->ExceptionCheck()) {
    env->DeleteLocalRef(jbytes);
    return nullptr;
  }
  return jbytes;
}

/*
 * Class:     org_kvrocksdb_KVRocksDB
 * Method:    put
 * Signature: (J[BII[BII)V
 */
bool rocksdb_put_helper(JNIEnv* env, rocksdb::DB* db,
                        const rocksdb::WriteOptions& write_options,
                        rocksdb::ColumnFamilyHandle* cf_handle, jbyteArray jkey,
                        jint jkey_off, jint jkey_len, jbyteArray jval,
                        jint jval_off, jint jval_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if(env->ExceptionCheck()) {
    delete [] key;
    return false;
  }

  jbyte* value = new jbyte[jval_len];
  env->GetByteArrayRegion(jval, jval_off, jval_len, value);
  if(env->ExceptionCheck()) {
    delete [] value;
    delete [] key;
    return false;
  }

  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  rocksdb::Slice value_slice(reinterpret_cast<char*>(value), jval_len);

  rocksdb::Status s;
  if (cf_handle != nullptr) {
    s = db->Put(write_options, cf_handle, key_slice, value_slice);
  } else {
    // backwards compatibility
    s = db->Put(write_options, key_slice, value_slice);
  }

  // cleanup
  delete [] value;
  delete [] key;

  if (s.ok()) {
    return true;
  } else {
    printf("put failed %s\n", s.ToString().c_str());
    return false;
  }
}

JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksDB_put
  (JNIEnv *env, jobject jdb, jlong jdb_handle,
  jbyteArray jkey, jint jkey_off,
  jint jkey_len, jbyteArray jval,
  jint jval_off, jint jval_len){
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options = rocksdb::WriteOptions();
  rocksdb_put_helper(env, db, default_write_options, nullptr, jkey, jkey_off,
                     jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_kvrocksdb_KVRocksDB
 * Method:    get
 * Signature: (J[BII[BII)I
 */
jint rocksdb_get_helper(JNIEnv* env, rocksdb::DB* db,
                        const rocksdb::ReadOptions& read_options,
                        rocksdb::ColumnFamilyHandle* column_family_handle,
                        jbyteArray jkey, jint jkey_off, jint jkey_len,
                        jbyteArray jval, jint jval_off, jint jval_len,
                        bool* has_exception) {
  static const int kNotFound = -1;
  static const int kStatusError = -2;

  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if(env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    delete [] key;
    *has_exception = true;
    return kStatusError;
  }
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  // TODO(yhchiang): we might save one memory allocation here by adding
  // a DB::Get() function which takes preallocated jbyte* as input.
  std::string cvalue;
  rocksdb::Status s;
  if (column_family_handle != nullptr) {
    s = db->Get(read_options, column_family_handle, key_slice, &cvalue);
  } else {
    // backwards compatibility
    s = db->Get(read_options, key_slice, &cvalue);
  }

  // cleanup
  delete [] key;

  if (s.IsNotFound()) {
    *has_exception = false;
    return kNotFound;
  } else if (!s.ok()) {
    *has_exception = true;
    printf("get kStatusError %s\n", s.ToString().c_str());
    // Return a dummy const value to avoid compilation error, although
    // java side might not have a chance to get the return value :)
    return kStatusError;
  }

  const jint cvalue_len = static_cast<jint>(cvalue.size());
  const jint length = std::min(jval_len, cvalue_len);

  env->SetByteArrayRegion(jval, jval_off, length,
                          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(cvalue.c_str())));
  if(env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    *has_exception = true;
    return kStatusError;
  }

  *has_exception = false;
  return cvalue_len;
}

JNIEXPORT jint JNICALL Java_org_kvrocksdb_KVRocksDB_get__J_3BII_3BII
  (JNIEnv *env, jobject jdb, jlong jdb_handle, jbyteArray jkey, jint jkey_off, jint jkey_len,
  jbyteArray jval, jint jval_off, jint jval_len){
  bool has_exception = false;
  return rocksdb_get_helper(env, reinterpret_cast<rocksdb::DB*>(jdb_handle),
                            rocksdb::ReadOptions(), nullptr, jkey, jkey_off,
                            jkey_len, jval, jval_off, jval_len,
                            &has_exception);
}
/*
 * Class:     org_kvrocksdb_KVRocksDB
 * Method:    get
 * Signature: (J[BII)[B
 */
jbyteArray rocksdb_get_helper(
    JNIEnv* env, rocksdb::DB* db, const rocksdb::ReadOptions& read_opt,
    rocksdb::ColumnFamilyHandle* column_family_handle, jbyteArray jkey,
    jint jkey_off, jint jkey_len) {

  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if(env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete [] key;
    return nullptr;
  }

  rocksdb::Slice key_slice(
      reinterpret_cast<char*>(key), jkey_len);

  std::string value;
  rocksdb::Status s;
  if (column_family_handle != nullptr) {
    s = db->Get(read_opt, column_family_handle, key_slice, &value);
  } else {
    // backwards compatibility
    s = db->Get(read_opt, key_slice, &value);
  }

  // cleanup
  delete [] key;

  if (s.IsNotFound()) {
    return nullptr;
  }

  if (s.ok()) {
    jbyteArray jret_value = copyBytes(env, value);
    if(jret_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    return jret_value;
  }
    printf("get failed %s\n", s.ToString().c_str());
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL Java_org_kvrocksdb_KVRocksDB_get__J_3BII
  (JNIEnv *env, jobject jdb, jlong jdb_handle, jbyteArray jkey, jint jkey_off, jint jkey_len){
  return rocksdb_get_helper(env,
      reinterpret_cast<rocksdb::DB*>(jdb_handle),
      rocksdb::ReadOptions(), nullptr,
      jkey, jkey_off, jkey_len);
}
/*
 * Class:     org_kvrocksdb_KVRocksDB
 * Method:    delete
 * Signature: (J[BII)V
 */

bool rocksdb_delete_helper(
    JNIEnv* env, rocksdb::DB* db, const rocksdb::WriteOptions& write_options,
    rocksdb::ColumnFamilyHandle* cf_handle, jbyteArray jkey, jint jkey_off,
    jint jkey_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if(env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete [] key;
    return false;
  }
  rocksdb::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  rocksdb::Status s;
  if (cf_handle != nullptr) {
    s = db->Delete(write_options, cf_handle, key_slice);
  } else {
    // backwards compatibility
    s = db->Delete(write_options, key_slice);
  }

  // cleanup
  delete [] key;

  if (s.ok()) {
    return true;
  }
  return false;
}

JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksDB_delete
  (JNIEnv *env, jobject jdb, jlong jdb_handle, jbyteArray jkey, jint jkey_off, jint jkey_len){
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  static const rocksdb::WriteOptions default_write_options = rocksdb::WriteOptions();
  rocksdb_delete_helper(env, db, default_write_options, nullptr,
      jkey, jkey_off, jkey_len);
}

/*
 * Class:     org_kvrocksdb_KVRocksDB
 * Method:    destroyDB
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_kvrocksdb_KVRocksDB_destroyDB
  (JNIEnv *env, jclass jcs, jstring jdb_path){
  rocksdb::Options options;
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if(db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  rocksdb::Status status = rocksdb::DestroyDB(db_path, options);;
  if (status.ok()) {
  return 1;
  } else {
  return 0;
  }
}

/*
 * Class:     org_kvrocksdb_KVRocksDB
 * Method:    iterator
 * Signature: (J)J
 */
jlong rocksdb_iterator_helper(
    rocksdb::DB* db, rocksdb::ReadOptions read_options,
    rocksdb::ColumnFamilyHandle* cf_handle) {
  rocksdb::Iterator* iterator = nullptr;
  if (cf_handle != nullptr) {
    iterator = db->NewIterator(read_options, cf_handle);
  } else {
    iterator = db->NewIterator(read_options);
  }
  return reinterpret_cast<jlong>(iterator);
}

JNIEXPORT jlong JNICALL Java_org_kvrocksdb_KVRocksDB_iterator
  (JNIEnv *env, jobject jdb, jlong jdb_handle){
  auto* db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  return rocksdb_iterator_helper(db, rocksdb::ReadOptions(),
      nullptr);
}

JNIEXPORT jboolean JNICALL Java_org_kvrocksdb_KVRocksIterator_isValid0
  (JNIEnv *env, jobject jdb, jlong handle){
  return reinterpret_cast<rocksdb::Iterator*>(handle)->Valid();
}

JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksIterator_seekToFirst0
  (JNIEnv *env, jobject jdb, jlong handle){
  reinterpret_cast<rocksdb::Iterator*>(handle)->SeekToFirst();
}

JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksIterator_seekToLast0
  (JNIEnv *env, jobject jdb, jlong handle){
  reinterpret_cast<rocksdb::Iterator*>(handle)->SeekToLast();
}

JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksIterator_next0
  (JNIEnv *env, jobject jdb, jlong handle){
  reinterpret_cast<rocksdb::Iterator*>(handle)->Next();
}

JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksIterator_prev0
  (JNIEnv *env, jobject jdb, jlong handle){
  reinterpret_cast<rocksdb::Iterator*>(handle)->Prev();
}

JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksIterator_seek0
    (JNIEnv* env, jobject jobj, jlong handle,
    jbyteArray jtarget, jint jtarget_len) {
  jbyte* target = env->GetByteArrayElements(jtarget, nullptr);
  if(target == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  rocksdb::Slice target_slice(
      reinterpret_cast<char*>(target), jtarget_len);

  auto* it = reinterpret_cast<rocksdb::Iterator*>(handle);
  it->Seek(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksIterator_status0
  (JNIEnv *env, jobject jdb, jlong handle){
  auto* it = reinterpret_cast<rocksdb::Iterator*>(handle);
  rocksdb::Status s = it->status();

  if (s.ok()) {
    return;
  }

}

JNIEXPORT jbyteArray JNICALL Java_org_kvrocksdb_KVRocksIterator_key0
  (JNIEnv *env, jobject jdb, jlong handle){
  auto* it = reinterpret_cast<rocksdb::Iterator*>(handle);
  rocksdb::Slice key_slice = it->key();

  jbyteArray jkey = env->NewByteArray(static_cast<jsize>(key_slice.size()));
  if(jkey == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetByteArrayRegion(jkey, 0, static_cast<jsize>(key_slice.size()),
                          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(key_slice.data())));
  return jkey;
}

JNIEXPORT jbyteArray JNICALL Java_org_kvrocksdb_KVRocksIterator_value0
  (JNIEnv *env, jobject jdb, jlong handle){
  auto* it = reinterpret_cast<rocksdb::Iterator*>(handle);
  rocksdb::Slice value_slice = it->value();

  jbyteArray jkeyValue =
      env->NewByteArray(static_cast<jsize>(value_slice.size()));
  if(jkeyValue == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetByteArrayRegion(jkeyValue, 0, static_cast<jsize>(value_slice.size()),
                          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value_slice.data())));
  return jkeyValue;
}

/*
 * Class:     org_kvrocksdb_KVRocksDB
 * Method:    disposeInternal
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksDB_disposeInternal
  (JNIEnv *env, jobject jdb, jlong jhandle){
  auto* db = reinterpret_cast<rocksdb::DB*>(jhandle);
  assert(db != nullptr);
  delete db;
  db = nullptr;
}
JNIEXPORT void JNICALL Java_org_kvrocksdb_KVRocksIterator_disposeInternal
  (JNIEnv *env, jobject jdb, jlong handle){
  auto* it = reinterpret_cast<rocksdb::Iterator*>(handle);
  assert(it != nullptr);
  delete it;
  it = nullptr;
}