#!/bin/bash -xe
set +e
~/bin/nvme_smart.sh /dev/nvme0n1 2>&1 | tee smart_stop0.log
~/bin/nvme_smart.sh /dev/nvme1n1 2>&1 | tee smart_stop1.log
sleep 20s
killall sar
killall kvproc_schedule.py
