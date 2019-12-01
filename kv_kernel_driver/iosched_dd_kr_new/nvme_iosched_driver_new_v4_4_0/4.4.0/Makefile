obj-m		+= nvme.o
nvme-y		+= kv_hash.o
nvme-y		+= kv_iosched.o
nvme-y		+= kv_core.o
lightnvm-y	+= lightnvm.o
nvme-y		+= scsi.o
nvme-y		+= core.o pci.o $(lightnvm-y)

KDIR    := /lib/modules/$(shell uname -r)/build
PWD     := $(shell pwd)

default:
	$(MAKE) -C $(KDIR) SUBDIRS=$(PWD) modules EXTRA_CFLAGS=-DKV_NVME_SUPPORT=1
clean:
	rm -rf *.ko
	rm -rf *.mod.*
	rm -rf .*.cmd
	rm -rf *.o
	rm -rf *~
