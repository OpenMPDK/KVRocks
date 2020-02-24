#!/bin/sh

PROJECT_DIR="/home/sqa69/hainan/1120/KVRocks"
INSDB_PATH="${PROJECT_DIR}/insdb"
FOLLY_PATH="${PROJECT_DIR}/folly"
SNAPPY_PATH="${PROJECT_DIR}/snappy/google-snappy-b02bfa7"
KVDB_PATH="${PROJECT_DIR}/kvdb"
GFLAG_PATH="${PROJECT_DIR}/gflags-v2.2.1/"
KERNEL_PATH="${PROJECT_DIR}/kv_kernel_driver/" 
UT_LIB="${KVDB_PATH}/third-party/deps/kvdb/lib/"


###build the kernel driver Linux kernel 4.9.5
echo "-----------------------------Linux Kernel------------------------------"    
cd ${KERNEL_PATH}/iosched_dd_kr_new/nvme_iosched_driver_new/
make clean
make  
sudo rmmod nvme
sudo rmmod nvme-core
sudo insmod ./nvme-core.ko
sudo insmod ./nvme.ko
echo "---------------------------Kernel build finish-------------------------"     


###build snappy
echo "----------------------------Build snappy-------------------------------"    
cd ${SNAPPY_PATH}
rm -rf build
mkdir build && cd build
cmake ..
make -j$(nproc)
ls
cp libsnappy.a ${UT_LIB}
echo "----------------------------snappy build finished----------------------"

###build folly library
echo "----------------------------build folly--------------------------------"    
cd ${FOLLY_PATH}
rm -rf build
mkdir build && cd build
cmake configure ..
make -j$(nproc)
make install
ls
cp libfolly.a ${UT_LIB}
echo "----------------------------folly build finished------------------------"    

###build insdb
echo "----------------------------Build Insdb---------------------------------"     
cd ${INSDB_PATH}
make clean
make -j$(nproc)
sleep 1
cd out-shared/
ls
cp libinsdb.so libinsdb.so.1 libinsdb.so.1.20 ${UT_LIB}
cd ../out-static/
ls
cp libinsdb.a ${UT_LIB}
echo "----------------------------Insdb build finished------------------------"    


####build kvdb,kvdb requires higher version of glags than Ubuntu 16.04 package.

echo "-------------------------Build gflag------------------------------------"    
cd ${GFLAG_PATH}
rm -rf build
mkdir build && cd build
cmake ..
make -j$(nproc)
echo "---------------------------Build kvdb-----------------------------------"
cd ${KVDB_PATH}
rm -rf build
mkdir build && cd build   
cmake ..
make -j$(nproc)
cp libkvdb.so ${UT_LIB}
cp libkvdb_static.a ${UT_LIB}
echo "--------------------------kvdb build finished---------------------------"  

echo "---------------------------Set Test Env.--------------------------------"
export LD_LIBRARY_PATH=${KVDB_PATH}/third-party/deps/kvdb/lib/ 
