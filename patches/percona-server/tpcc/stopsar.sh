#!/bin/bash -xe
set +e
#~/bin/nvme_smart.sh /dev/nvme0n1
#~/bin/nvme_smart.sh /dev/nvme1n1
killall sar
killall kvproc_schedule.py

cd ~/Downloads/vdisk/
./umount_vdisk.sh
sudo swapon
