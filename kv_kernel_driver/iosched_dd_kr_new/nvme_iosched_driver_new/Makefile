obj-m			+= nvme-core.o
obj-m			+= nvme.o
nvme-core-y		+= kv_hash.o
nvme-core-y		+= kv_iosched.o
nvme-core-y		+= kv_core.o
nvme-core-y		+= core.o
nvme-core-y		+= scsi.o
nvme-core-y			+= lightnvm.o
nvme-y			+= pci.o
#nvme-fabrics-y		+= fabrics.o
#nvme-rdma-y		+= rdma.o

KDIR    := /lib/modules/$(shell uname -r)/build
PWD     := $(shell pwd)

default:
	$(MAKE) -C $(KDIR) SUBDIRS=$(PWD) modules
clean:
	rm -rf *.ko
	rm -rf *.mod.*
	rm -rf .*.cmd
	rm -rf *.o
	rm -rf *~
