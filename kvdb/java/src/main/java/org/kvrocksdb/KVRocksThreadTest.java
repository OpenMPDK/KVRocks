package org.kvrocksdb;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
public class KVRocksThreadTest implements Runnable {
  protected final KVRocksDB db_;
  protected final String message_;

  public KVRocksThreadTest(final KVRocksDB db, final String message) {
    this.db_ = db;
    this.message_ = message;
  }

  @Override
  public void run() {
    try{
      System.out.println(message_ + "=======insert========");
      int N = 10000;
      for (int i = 1; i <= N; i++) {
        String key = String.format("%016d", i);
        db_.insert(key.getBytes(), key.getBytes());
      }
      System.out.println(message_ + "=======read1========");
      for (int i = 1; i <= N; i++) {
        String key = String.format("%016d", i);
        byte[] value = db_.read(key.getBytes());
        if (value != null) {
          String v = new String(value);
          if(!key.equals(v))
            System.out.println("read1 !equals:" + key + ", " + v);
        } else {
          System.out.println(key + ", NOT FOUND");
        }
      }
      System.out.println(message_ + "=======scan========");
      String start = String.format("%016d", 3);
      ConcurrentHashMap<String, String> map = db_.scan(start.getBytes(), 5);
      for (Map.Entry<String, String> entry : map.entrySet()) {
        if(!(entry.getKey()).equals(entry.getValue()))
          System.out.println("scan !equals:" + "Key = " + entry.getKey() + ", Value = " + entry.getValue());
      }
      System.out.println(message_ + "=======delete========");
      for (int i = 1; i <= N; i = i + 2) {
        String key = String.format("%016d", i);
        db_.delete(key.getBytes());
      }
      System.out.println(message_ + "=======iter seekToFirst + next========");
      final KVRocksIterator iterator = db_.newIterator();
      iterator.seekToFirst();
      while (iterator.isValid()) {
        if(!(new String(iterator.key())).equals(new String(iterator.value())))
          System.out.println("iter !equals:" + new String(iterator.key()) + ", " + new String(iterator.value()));
        iterator.next();
      }
      System.out.println(message_ + "=======iter seekToLast + prev========");
      iterator.seekToLast();
      while (iterator.isValid()) {
        if(!(new String(iterator.key())).equals(new String(iterator.value())))
          System.out.println("iter !equals:" + new String(iterator.key()) + ", " + new String(iterator.value()));
        iterator.prev();
      }
      iterator.dispose();
    } catch (Exception e) {
      System.out.println(e);
    }
    System.out.println(message_ + "=======insert end========");
  }
}
