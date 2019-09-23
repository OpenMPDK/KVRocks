cnt=`sed -n '$=' ./key.list`
if [ "$cnt" > "0" ]
then
echo "The number of remaining key is ${cnt}"
    echo "100 0 0 t t ${cnt} ${cnt}" > ./thread.default.conf
    echo > /proc/kv_proc
    ./kv_bench -u f
else
echo "No key exists!"
fi
