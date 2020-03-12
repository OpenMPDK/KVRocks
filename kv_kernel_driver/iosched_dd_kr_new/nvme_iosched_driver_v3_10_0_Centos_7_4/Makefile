CONFIG_MODULE_SIG=n
CONFIG_MODULE_SIG_ALL=n
obj-m			+= nvme-core.o
obj-m			+= nvme.o
nvme-core-y		+= kv_hash.o
nvme-core-y		+= kv_iosched.o
nvme-core-y		+= kv_core.o
nvme-core-y		+= core.o
nvme-core-y		+= scsi.o
nvme-y			+= pci.o
#nvme-fabrics-y		+= fabrics.o
#nvme-rdma-y		+= rdma.o

all:
	make -C /lib/modules/`uname -r`/build M=`pwd` modules
clean:
	make -C /lib/modules/`uname -r`/build M=`pwd` clean
