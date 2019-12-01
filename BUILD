  * dependencies

     o Latest jemalloc is recommended
       https://github.com/jemalloc/jemalloc

  * build the kernel driver

      o Change directory into the kernel driver

      o Linux kernel 4.9.5

-----------------------------------------------------------------------------     
cd kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new
make
-----------------------------------------------------------------------------     

      o Linux kernel 4.4.0
-----------------------------------------------------------------------------     
cd kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new_v4_4_0
make
-----------------------------------------------------------------------------     

      o If you get the following error, uncomment nvme-core-y in Makefile

-----------------------------------------------------------------------------     
WARNING: "nvme_nvm_unregister" [/home/user/git/insdb/kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new/nvme-core.ko] undefined!
WARNING: "nvme_nvm_ns_supported" [/home/user/git/insdb/kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new/nvme-core.ko] undefined!
WARNING: "nvme_nvm_register" [/home/user/git/insdb/kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new/nvme-core.ko] undefined!
-----------------------------------------------------------------------------     
 
Makefile -----------------------------------------------------------------      
nvme-core-y                   += lightnvm.o
-----------------------------------------------------------------------------     

      o replace stock kernel drivers with kv kernel drivers

      o kernel v4.9.5
-----------------------------------------------------------------------------     
sudo rmmod nvme
sudo rmmod nvme-core
sudo insmod ./nvme-core.ko
sudo insmod ./nvme.ko
-----------------------------------------------------------------------------     

      o kernel v4.4
-----------------------------------------------------------------------------     
sudo rmmod nvme
sudo insmod ./nvme.ko
-----------------------------------------------------------------------------     

  * build snappy

-----------------------------------------------------------------------------     
cd snappy/google-snappy-b02bfa7
mkdir build && cd build
cmake ..
make -j$(nproc)
-----------------------------------------------------------------------------     

  * build folly library
    The library needs C++14 support with GCC 4.9+
    It depends on the following library. Each library also should be built and installed.

     https://github.com/google/googletest/archive/release-1.8.0.tar.gz
     https://github.com/facebook/zstd/archive/v1.3.5.tar.gz

    The libraries in the following is packaged and easily installable by apt-get or yum.

-----------------------------------------------------------------------------     
sudo apt-get install \
    g++ \
    cmake \
    libboost-all-dev \
    libevent-dev \
    libdouble-conversion-dev \
    libgoogle-glog-dev \
    libgflags-dev \
    libiberty-dev \
    liblz4-dev \
    liblzma-dev \
    libsnappy-dev \
    make \
    zlib1g-dev \
    binutils-dev \
    libjemalloc-dev \
    libssl-dev \
    libc6-dev \
    pkg-config
-----------------------------------------------------------------------------     

-----------------------------------------------------------------------------     
cd folly
mkdir _build && cd _build
cmake configure ..
make -j$(nproc)
make install
-----------------------------------------------------------------------------     

  * build insdb

-----------------------------------------------------------------------------     
cd insdb
make -j$(nproc)
-----------------------------------------------------------------------------     


  * build kvdb

       o kvdb requires higher version of gflags than Ubuntu 16.04 package.

-----------------------------------------------------------------------------     
cd gflags-v2.2.1
mkdir build && cd build
cmake ..
make -j$(nproc)
-----------------------------------------------------------------------------     

       o change directory to kvdb, and create a build directory

-----------------------------------------------------------------------------
cd kvdb
mkdir build && cd build
-----------------------------------------------------------------------------     

       o create cmake and build

-----------------------------------------------------------------------------     
cmake ..
make -j$(nproc)
-----------------------------------------------------------------------------     

==== udev rules ====

  * To allow non-root access to kvssd device in MyRocks, install udev rules ( 99-kv-nvme-dev.rules )
    The udev rule changes /dev/nvme permissions when it's created.

-----------------------------------------------------------------------------     
cp kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new/99-kv-nvme-dev.rules /etc/udev/rules.d
-----------------------------------------------------------------------------     

==== Build KvRocks plugin for Percona server ====

  * Clone Percona server from Github at the same directory level as KVRocks
  Check out version Percona-Server-5.7.20-19

-----------------------------------------------------------------------------     
git clone https://github.com/percona/percona-server.git
cd percona-server
git checkout Percona-Server-5.7.20-19
-----------------------------------------------------------------------------     

  * update dependent source code

-----------------------------------------------------------------------------     
git submodule init
git submodule update
-----------------------------------------------------------------------------     

  * apply KVRocks patch and add executable permission
-----------------------------------------------------------------------------     
patch -p1 < ../KVRocks/patches/percona-server/kvrocks-percona-server-5.7.20-19.patch
chmod +x ../KVRocks/patches/percona-server/kvrocks-percona-server-5.7.20-19.patch
-----------------------------------------------------------------------------     

  * CMake with KVRocks path

-----------------------------------------------------------------------------
mkdir build && cd build     
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_CONFIG=mysql_release -DFEATURE_SET=community -DWITH_EMBEDDED_SERVER=OFF -DCMAKE_INSTALL_PREFIX=/usr/mysql -DDOWNLOAD_BOOST=1 -DWITH_BOOST=../boost_1_59_0 -DKVROCKS_ROOT_DIR=../KVRocks
-----------------------------------------------------------------------------     

  * Build the source tree. the plugin gets created as storage/rocksdb/ha_rocksdb.so

-----------------------------------------------------------------------------     
make -j$(nproc)
-----------------------------------------------------------------------------     



==== Install Percona server and KVRocks plugin ====

  * Download Percona server 5.7.20-19 form Percona website

-----------------------------------------------------------------------------     
-rw-r--r--  1 user ads   1551042 Dec 28  2017 percona-server-client-5.7_5.7.20-19-1.trusty_amd64.deb
-rw-r--r--  1 user ads    207684 Dec 28  2017 percona-server-common-5.7_5.7.20-19-1.trusty_amd64.deb
-rw-r--r--  1 user ads  26621190 Dec 28  2017 percona-server-rocksdb-5.7_5.7.20-19-1.trusty_amd64.deb
-rw-r--r--  1 user ads  24588774 Dec 28  2017 percona-server-server-5.7_5.7.20-19-1.trusty_amd64.deb
-----------------------------------------------------------------------------     

  * Install the packages. use apt-get to install its dependency packages

-----------------------------------------------------------------------------     
sudo apt-get install ./*.deb
-----------------------------------------------------------------------------     

  * make a backup of existing rocksdb plugin, and install KVRocks plugin

-----------------------------------------------------------------------------     
sudo cp /usr/lib/mysql/plugin/ha_rocksdb.so .
sudo cp /home/user/git/percona-server/storage/rocksdb/ha_rocksdb.so /usr/lib/mysql/plugin/
-----------------------------------------------------------------------------     

  * apparmor blocks nvme device access in Ubuntu. switch to complaint mode to allow nvme device access.
  if apparmor is not installed, ignore this step

-----------------------------------------------------------------------------     
aa-complain /usr/sbin/mysqld
-----------------------------------------------------------------------------     

  * start Percona server
    note that delay might be needed to allow the kernel driver detects all nvme devices

-----------------------------------------------------------------------------     
sudo service mysql start
-----------------------------------------------------------------------------     

  * enable KVRocks plugin

-----------------------------------------------------------------------------     
sudo ps-admin --enable-rocksdb -u root
-----------------------------------------------------------------------------     

  * Install MyRocks configuration file
    Recommended KvRocks configuration is located in patches/percona-server/setup in KVRocks source code.
    mysqld should be restarted after config file is updated.

  TPC-C
-----------------------------------------------------------------------------     
cp kvmysqld.cnf.mt24k.mr2048k /etc/mysql/percona-server.conf.d/
-----------------------------------------------------------------------------     

  TPC-H
-----------------------------------------------------------------------------
cp kvmysqld.cnf.mt512k.mr2048k /etc/mysql/percona-server.conf.d/
-----------------------------------------------------------------------------
