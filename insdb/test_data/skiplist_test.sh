#!/bin/bash

tcount="1 2 3 4 5"
result_file="total_result.txt"
echo "[skiplist test result]" > ${result_file}

echo "start original skiplist test -----" 
echo "start original skiplist test -----" >> ${result_file} 
../out-static/skiplist0_test &> result0_1
sleep 1
../out-static/skiplist0_test &> result0_2
sleep 1
../out-static/skiplist0_test &> result0_3
sleep 1
../out-static/skiplist0_test &> result0_4
sleep 1
../out-static/skiplist0_test &> result0_5
sleep 1

for k in $tcount;
do
    echo "${k} round report---" >> ${result_file}
    cat result0_${k} | grep "==== Test\|execution" >> ${result_file}
done

echo "start original + spinlock skiplist test ---"
echo "start original + spinlock skiplist test ---" >> ${result_file}

../out-static/skiplist_test &> result_1
sleep 1
../out-static/skiplist_test &> result_2
sleep 1
../out-static/skiplist_test &> result_3
sleep 1
../out-static/skiplist_test &> result_4
sleep 1
../out-static/skiplist_test &> result_5
sleep 1


for k in $tcount;
do
    echo "${k} round report---" >> ${result_file}
    cat result_${k} | grep "==== Test\|execution" >> ${result_file}
done

echo "start nolock+malloc skiplist test ----"
echo "start nolock+malloc skiplist test ----" >> ${result_file}
../out-static/skiplist2_test &> result2_1
sleep 1
../out-static/skiplist2_test &> result2_2
sleep 1
../out-static/skiplist2_test &> result2_3
sleep 1
../out-static/skiplist2_test &> result2_4
sleep 1
../out-static/skiplist2_test &> result2_5
sleep 1


for k in $tcount;
do
    echo "${k} round report---" >> ${result_file}
    cat result2_${k} | grep "==== Test\|execution" >> ${result_file}
done

echo "start nolock+shared arena skiplist test ----"
echo "start nolock+shared arena skiplist test ----" >> ${result_file}
../out-static/skiplist3_test &> result3_1
sleep 1
../out-static/skiplist3_test &> result3_2
sleep 1
../out-static/skiplist3_test &> result3_3
sleep 1
../out-static/skiplist3_test &> result3_4
sleep 1
../out-static/skiplist3_test &> result3_5
sleep 1


for k in $tcount;
do
    echo "${k} round report---" >> ${result_file}
    cat result3_${k} | grep "==== Test\|execution" >> ${result_file}
done

echo "start nolock+arena skiplist test ----"
echo "start nolock+arena skiplist test ----" >> ${result_file}
../out-static/skiplist4_test &> result4_1
sleep 1
../out-static/skiplist4_test &> result4_2
sleep 1
../out-static/skiplist4_test &> result4_3
sleep 1
../out-static/skiplist4_test &> result4_4
sleep 1
../out-static/skiplist4_test &> result4_5
sleep 1


for k in $tcount;
do
    echo "${k} round report---" >> ${result_file}
    cat result4_${k} | grep "==== Test\|execution" >> ${result_file}
done




echo "start orig+malloc skiplist test ----"
echo "start orig+malloc skiplist test ----" >> ${result_file}
../out-static/skiplist5_test &> result5_1
sleep 1
../out-static/skiplist5_test &> result5_2
sleep 1
../out-static/skiplist5_test &> result5_3
sleep 1
../out-static/skiplist5_test &> result5_4
sleep 1
../out-static/skiplist5_test &> result5_5
sleep 1


for k in $tcount;
do
    echo "${k} round report---" >> ${result_file}
    cat result5_${k} | grep "==== Test\|execution" >> ${result_file}
done
