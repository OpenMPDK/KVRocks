cat kv_proc.log | grep "The # of Sync Get RQ" > sync_get.txt
cat kv_proc.log | grep "The # of Sync Put RQ" > sync_put.txt

cat kv_proc.log | grep "Async Get RQ" > async_get.txt
cat kv_proc.log | grep "Async Put RQ" > async_put.txt
cat kv_proc.log | grep "Async Del RQ" > async_del.txt

cat kv_proc.log | grep "Total Sync Completed RQ" > sync_completed.txt
cat kv_proc.log | grep "Total Submitted Sync RQ" > sync_submitted.txt

## total async request received by insdb driver
cat kv_proc.log | grep "Total Async RQ" > total_async_req_recv.txt
## total async request insdb driver submit to device
cat kv_proc.log | grep "Total Submitted Async RQ" > total_async_req_submitted.txt
cat kv_proc.log | grep "Total Async Completed RQ" > async_completed.txt
cat kv_proc.log | grep "canceled RA" > canceled.txt

cat kv_proc.log | grep "Total # of Req. In Pending" > driver_pending.txt
cat kv_proc.log | grep "Total # Device Pending Req" > device_pending.txt

cat kv_proc.log | grep "The # of RA hit" > rahit.txt
cat kv_proc.log | grep "The # of discard RA" > radiscard.txt

cat tpcc_* | grep CSV > trx.csv
