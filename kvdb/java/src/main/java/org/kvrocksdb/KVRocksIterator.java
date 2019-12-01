// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.kvrocksdb;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * <p>An iterator that yields a sequence of key/value pairs from a source.
 * Multiple implementations are provided by this library.
 * In particular, iterators are provided
 * to access the contents of a Table or a DB.</p>
 *
 * <p>Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.</p>
 *
 * @see org.rocksdb.RocksObject
 */
public class KVRocksIterator{
  protected KVRocksIterator(KVRocksDB kvrocksDB, long nativeHandle) {
    // parent must point to a valid RocksDB instance.
    assert (kvrocksDB != null);
    this.parent_ = kvrocksDB;
    this.nativeHandle_ = nativeHandle;
    this.owningHandle_ = new AtomicBoolean(true);
  }

  private final AtomicBoolean owningHandle_;
  protected final long nativeHandle_;
  final KVRocksDB parent_;

  public boolean isOwningHandle() {
    return owningHandle_.get();
  }

  protected final void disOwnNativeHandle() {
    owningHandle_.set(false);
  }

  /**
   * <p>Return the key for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   *
   * @return key for the current entry.
   */
  public byte[] key() {
    assert (isOwningHandle());
    return key0(nativeHandle_);
  }

  /**
   * <p>Return the value for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: !AtEnd() &amp;&amp; !AtStart()</p>
   * @return value for the current entry.
   */
  public byte[] value() {
    assert (isOwningHandle());
    return value0(nativeHandle_);
  }

  public boolean isValid() {
    assert (isOwningHandle());
    return isValid0(nativeHandle_);
  }

  public void seekToFirst() {
    assert (isOwningHandle());
    seekToFirst0(nativeHandle_);
  }

  public void seekToLast() {
    assert (isOwningHandle());
    seekToLast0(nativeHandle_);
  }

  public void seek(byte[] target) {
    assert (isOwningHandle());
    seek0(nativeHandle_, target, target.length);
  }

  public void next() {
    assert (isOwningHandle());
    next0(nativeHandle_);
  }

  public void prev() {
    assert (isOwningHandle());
    prev0(nativeHandle_);
  }

  public void status() throws Exception {
    assert (isOwningHandle());
    status0(nativeHandle_);
  }

  public void dispose() {
    disOwnNativeHandle();
    disposeInternal(nativeHandle_);
  }

  final native boolean isValid0(long handle);
  final native void seekToFirst0(long handle);
  final native void seekToLast0(long handle);
  final native void next0(long handle);
  final native void prev0(long handle);
  final native void seek0(long handle, byte[] target, int targetLen);
  final native void status0(long handle) throws Exception;

  private native byte[] key0(long handle);
  private native byte[] value0(long handle);
  protected final native void disposeInternal(final long handle);
}

