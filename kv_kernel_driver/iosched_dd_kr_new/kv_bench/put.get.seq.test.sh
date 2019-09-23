sudo nvme format /dev/nvme0n1 -s 1
sleep 20

cp thread.put.get.seq.conf thread.default.conf 
echo > /proc/kv_proc
./kv_bench -d /dev/nvme0n1 -s 16 -v 68 -u y
