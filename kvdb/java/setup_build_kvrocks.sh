#!/bin/bash -xe

java_dir=$(dirname $(readlink -f "$0"))
KVRocks_dir=${java_dir}/../..

# install prerequisite packages
set +e
sudo apt-get install \
    git \
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
    libreadline-dev \
    libncurses5-dev \
    libaio-dev \
    bison \
    pkg-config \
    default-jdk \
    maven

## Install folly dependency googletest and zstd v1.3.5
## download googletest and install
cd ${KVRocks_dir}/..
wget https://github.com/google/googletest/archive/release-1.8.0.tar.gz
tar -zxvf release-1.8.0.tar.gz
cd googletest-release-1.8.0
cmake .
make && make install

## downlodd zstd v1.3.5 and install
cd ${KVRocks_dir}/..
wget https://github.com/facebook/zstd/archive/v1.3.5.tar.gz
tar -zxvf v1.3.5.tar.gz
cd zstd-1.3.5
make && make install

set -e
## build folly first
cd ${KVRocks_dir}/folly
mkdir -p build && cd build
cmake ..
make -j $(nproc)
sudo make install

## build kdd 
kernel=`uname -a |awk '{print $3}' | awk -F "." '{printf "%s.%s", $1,$2}'`
if [[ "${kernel}" == "4.9" ]]
then
    cd ${KVRocks_dir}/kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new
    make clean
    make -j $(nproc)
elif [[ "${kernel}" == "4.4" ]]
then
    cd ${KVRocks_dir}/kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new_v4_4_0
    make clean
    make -j $(nproc)
    chmod +x re_insmod.sh
else
    echo "not supported kernel version: $kernel"
    exit 1
fi

## snappy
cd ${KVRocks_dir}/snappy/google-snappy-b02bfa7
mkdir -p build && cd build
cmake ..
make -j $(nproc)
sudo make install

## insdb
cd ${KVRocks_dir}/insdb
set +e
make clean
set -e
make -j $(nproc)

## gflags. kvdb requires higher version of gflags
cd ${KVRocks_dir}/gflags-v2.2.1
mkdir -p build && cd build
cmake ..
make -j $(nproc)

## kvdb
cd ${KVRocks_dir}/kvdb
set +e
mkdir -p build && cd build
make clean
set -e
cmake ..
make -j $(nproc)

## build java for ycsb.
cd ${java_dir}
make -j $(nproc)

## download YCSB code
cd ${KVRocks_dir}/..
git clone https://github.com/brianfrankcooper/YCSB.git
cd YCSB

## copy the java package for ycsb.
cp ${java_dir}/kvrocks.zip .
unzip kvrocks.zip
cp ${java_dir}/kvrocks.jar kvrocks

## add kvrocks related config.
sed -i '/<rocksdb.version>/a\    <kvrocks.version>0.1.0</kvrocks.version>' pom.xml
sed -i '/<module>rocksdb/a\    <module>kvrocks</module>' pom.xml
sed -i '/site.ycsb.db.rocksdb.RocksDBClient/a\    "kvrocks"      : "site.ycsb.db.KVRocksClient",' bin/ycsb
sed -i '/rocksdb:site.ycsb.db.rocksdb.RocksDBClient/a\kvrocks:site.ycsb.db.KVRocksClient' bin/bindings.properties

## compile ycsb for kvrocks
mvn -pl site.ycsb:kvrocks-binding -am clean package


## workloads test

#kernel=`uname -a |awk '{print $3}' | awk -F "." '{printf "%s.%s", $1,$2}'`
#if [[ "${kernel}" == "4.9" ]]
#then
#    cd ${KVRocks_dir}/kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new
#    sudo rmmod nvme
#    sudo rmmod nvme_core
#    sudo insmod nvme-core.ko
#    sudo insmod nvme.ko
#elif [[ "${kernel}" == "4.4" ]]
#then
#    cd ${KVRocks_dir}/kv_kernel_driver/iosched_dd_kr_new/nvme_iosched_driver_new_v4_4_0
#    sudo ./re_insmod.sh
#else
#    echo "not supported kernel version: $kernel"
#    exit 1
#fi
#sudo cp 99-kv-nvme-dev.rules /etc/udev/rules.d/
#sleep 20

#cd ${KVRocks_dir}/../YCSB
#./bin/ycsb load kvrocks -P workloads/workloada
