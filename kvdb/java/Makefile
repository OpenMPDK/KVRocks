OPT = -g
WARNINGS = -fpermissive -Wall -Wextra
CXXFLAGS = $(OPT) $(WARNINGS) -Wl,--no-as-needed -pthread -lrt -liberty -lsnappy -lfolly -ldl -lglog -ldouble-conversion -DROCKSDB_PLATFORM_POSIX

TOOL_DIR = deps
DEP_LIBS = ../build/libkvdb_static.a
DEP_LIBS += ../../insdb/out-static/libinsdb.a
DEP_LIBS += ../../snappy/google-snappy-b02bfa7/build/libsnappy.a
DEP_LIBS += ../../folly/build/libfolly.a
DEP_LIBS += $(TOOL_DIR)/libdouble-conversion.a
DEP_LIBS += $(TOOL_DIR)/libxml2.a
DEP_LIBS += $(TOOL_DIR)/libglog.so
DEP_LIBS += /usr/lib/x86_64-linux-gnu/libiberty.a

INSDB_INCLUDE_DIR = ../../insdb/include
KVDB_DIR = ../
KVDB_INCLUDE_DIR = ../include/

CXX = g++
CPPFLAGS:=-std=c++14 -fpic -DROCKSDB_PLATFORM_POSIX

JNI_DIR = kvrocksjni
JNI_NATIVE_SOURCES = $(JNI_DIR)/kvrocksjni.cc
JAVA_SOURCES_DIR = src/main/java/org/kvrocksdb
NATIVE_JAVA_SOURCES = $(JAVA_SOURCES_DIR)/*.java
NATIVE_JAVA_CLASSES = org.kvrocksdb.KVRocksDB

INCLUDES = -I/usr/lib/jvm/java-1.8.0-openjdk-amd64/include \
                        -I/usr/lib/jvm/java-1.8.0-openjdk-amd64/include/linux \
                        -I $(KVDB_DIR) -I $(KVDB_INCLUDE_DIR) -I $(INSDB_INCLUDE_DIR)

all:
	mkdir -p target/classes
	javac -d target/classes $(NATIVE_JAVA_SOURCES)
	javac $(NATIVE_JAVA_SOURCES) -h include
	$(CXX) $(INCLUDES) $(CPPFLAGS) -c $(JNI_NATIVE_SOURCES)
	mv kvrocksjni.o $(JNI_DIR)
	$(CXX) $(CXXFLAGS) -shared -Wl,-soname,$(JNI_DIR)/libkvrocks.so.1 -o $(JNI_DIR)/libkvrocks.so.1.0 $(JNI_DIR)/kvrocksjni.o -L. $(DEP_LIBS);
	cp $(JNI_DIR)/libkvrocks.so.1.0 $(JNI_DIR)/libkvrocks.so
	jar -cf kvrocks.jar $(JNI_DIR)/libkvrocks.so
	cd target/classes;jar -uf ../../kvrocks.jar org/kvrocksdb/*.class
	jar -uf kvrocks.jar MANIFEST.MF

clean :
	rm -f $(JNI_DIR)/*.so* $(JNI_DIR)/*.o *.jar $(JAVA_SOURCES_DIR)/*.class
	rm -rf include lib target

