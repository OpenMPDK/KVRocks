package org.kvrocksdb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;

public class KVRocksDB {
  protected final long nativeHandle_;
  protected final String dbname_;

  static {
    KVRocksDB.loadLibrary();
  }

  protected KVRocksDB() {
    this.nativeHandle_ = 0;
    this.dbname_ = null;
  }

  protected KVRocksDB(final long nativeHandle, final String dbname) {
    this.nativeHandle_ = nativeHandle;
    this.dbname_ = dbname;
  }

  public static void loadLibrary() {
    try {
      URL url = KVRocksDB.class.getProtectionDomain().getCodeSource().getLocation();
      String tmpdir = java.net.URLDecoder.decode(url.getPath(), "utf-8");
      tmpdir = tmpdir.substring(0, tmpdir.lastIndexOf('/') + 1) + "lib/";
      String filename = "libkvrocks.so";

      File dir = new File(tmpdir);
      if (!dir.exists()) {
        dir.mkdirs();
      }
      File file = new File(tmpdir + filename);
      if (!file.exists() && !file.createNewFile()) {
        return;
      }
      byte[] buffer = new byte[1024];
      int readBytes;

      InputStream is = KVRocksDB.class.getClass().getResourceAsStream("/kvrocksjni/libkvrocks.so");
      if (is == null) {
        throw new FileNotFoundException("File /kvrocksjni/libkvrocks.so was not found inside JAR.");
      }
      OutputStream os = new FileOutputStream(file);
      try {
        while ((readBytes = is.read(buffer)) != -1) {
          os.write(buffer, 0, readBytes);
        }
      } finally {
        os.close();
        is.close();
      }
      try {
        System.load(file.getAbsolutePath());
      } finally {
        file.deleteOnExit();
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to load the KVRocksDB shared library" + e);
    }
  }

  public static KVRocksDB open(final String path) throws Exception {
    File dir = new File(path.substring(0, path.lastIndexOf('/') + 1));
    if (!dir.exists()) {
      dir.mkdirs();
    }
    final KVRocksDB db = new KVRocksDB(openDB(path), path);
    return db;
  }

  public void insert(final byte[] key, final byte[] value) throws Exception {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  // kNotFound = -1;kStatusError = -2;
  public int read(final byte[] key, final byte[] value) throws Exception {
    return get(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  public byte[] read(final byte[] key) throws Exception {
    return get(nativeHandle_, key, 0, key.length);
  }

  public void delete(final byte[] key) throws Exception {
    delete(nativeHandle_, key, 0, key.length);
  }

  public long destroy(final String dbname){
    return destroyDB(dbname);
  }

  public void close() throws Exception {
    disposeInternal(nativeHandle_);
  }

  public KVRocksIterator newIterator() {
    return new KVRocksIterator(this, iterator(nativeHandle_));
  }

  public ConcurrentHashMap<String, String> scan(byte[] start, int count) {
    try {
      final KVRocksIterator iterator = this.newIterator();
      iterator.seek(start);
      int num = 0;
      ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
      while (iterator.isValid() && num < count) {
        map.put(new String(iterator.key()), new String(iterator.value()));
        iterator.next();
        num++;
      }
      iterator.dispose();
      return map;
    } catch (Exception e) {
      return null;
    }
  }

  // native methods
  protected native static long openDB(final String path) throws Exception ;

  protected native void put(long handle, byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength) throws Exception ;

  protected native int get(long handle, byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
      int valueLength) throws Exception ;

  protected native byte[] get(long handle, byte[] key, int keyOffset, int keyLength) throws Exception ;

  protected native void delete(long handle, byte[] key, int keyOffset, int keyLength) throws Exception ;

  protected native static long destroyDB(final String path);

  protected native long iterator(long handle);

  protected final native void disposeInternal(final long handle);

  public static void main(String[] args) {
    System.out.println("============main===================");
    String dbname = "/tmp/ycsb/test";
    try{
      final KVRocksDB db = KVRocksDB.open(dbname);
      List<Thread> list = new ArrayList<Thread>();
      for(int i = 0; i < 8; i++) {
        Thread thread = new Thread(new KVRocksThreadTest(db, "Thread" + i));
        thread.start();
        list.add(thread);
      }
      try {
        for(Thread thread : list) {
          thread.join();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      db.close();
      System.out.println("============main end===================");
    } catch (Exception e) {
        System.out.println(e);
    }
  }
}

