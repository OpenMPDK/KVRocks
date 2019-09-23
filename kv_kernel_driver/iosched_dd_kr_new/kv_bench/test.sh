#!/bin/bash 
#GET_TEST="Yes"
GET_TEST="No"
CONGESTION_CONTROL=1024
NR_THREAD=(1 4 8 16 32 64)
T_MAX=6
ASYNC=(100 0)
KEY_TYPE=(T F)
SEQ=(T F)
TOTAL_KEY=1600000

THREAD_IDX=0
IOTYPE_IDX=0
KEY_TYPE_IDX=0
SEQ_IDX=0
TOTAL_CNT=0
ITER=8
let TOTAL_LOOP=${T_MAX}*${ITER}*2*2*2

rm ./result.new.txt
rm ./result.update.txt
rm ./result.get.txt
rm ./result.del.txt
while [ $THREAD_IDX -lt $T_MAX ]; do
    IOTYPE_IDX=0
    while [ $IOTYPE_IDX -lt 2 ]; do
        KEY_TYPE_IDX=0
        while [ $KEY_TYPE_IDX -lt 2 ]; do
            SEQ_IDX=0
            while [ $SEQ_IDX -lt 2 ]; do
                # First PUT 
                COUNTER=0
                while [  $COUNTER -lt ${ITER} ]; do
                    TCNT=0
                    if [ $KEY_TYPE_IDX == 0 ] # Use Private Keys
                    then
                        let NR_KEY=$TOTAL_KEY/${NR_THREAD[${THREAD_IDX}]}
                    else # Use Shared Keys
                        let NR_KEY=$TOTAL_KEY
                    fi
                    let NR_REQ=$TOTAL_KEY/${NR_THREAD[${THREAD_IDX}]}
                    rm thread.default.conf
                    while [  $TCNT -lt ${NR_THREAD[${THREAD_IDX}]} ]; do
                        echo "${ASYNC[$IOTYPE_IDX]} 100 0 ${KEY_TYPE[${KEY_TYPE_IDX}]} ${SEQ[${SEQ_IDX}]} ${NR_KEY} ${NR_REQ}" >> thread.default.conf 
                        let TCNT=TCNT+1 
                    done

                    echo " NEW(${NR_THREAD[${THREAD_IDX}]} threads) | ${ASYNC[$IOTYPE_IDX]} |100 |0 |${KEY_TYPE[${KEY_TYPE_IDX}]} |${SEQ[${SEQ_IDX}]} |${NR_KEY} |${NR_REQ}"
                    echo " NEW(${NR_THREAD[${THREAD_IDX}]} threads) | ${ASYNC[$IOTYPE_IDX]} |100 |0 |${KEY_TYPE[${KEY_TYPE_IDX}]} |${SEQ[${SEQ_IDX}]} |${NR_KEY} |${NR_REQ}" >> result.new.txt
                    echo > /proc/kv_proc
                    ./kv_bench -u n >> result.new.txt
                    grep CONF ./result.new.txt | tail -3
                    tail -2 ./result.new.txt

                    # Update 
                    echo " UPDATE(${NR_THREAD[${THREAD_IDX}]} threads) | ${ASYNC[$IOTYPE_IDX]}| 100| 0| ${KEY_TYPE[${KEY_TYPE_IDX}]} |${SEQ[${SEQ_IDX}]} |${NR_KEY} |${NR_REQ}"
                    echo " UPDATE(${NR_THREAD[${THREAD_IDX}]} threads) | ${ASYNC[$IOTYPE_IDX]}| 100| 0| ${KEY_TYPE[${KEY_TYPE_IDX}]} |${SEQ[${SEQ_IDX}]} |${NR_KEY} |${NR_REQ}" >> result.update.txt
                    echo > /proc/kv_proc
                    ./kv_bench -u n >> result.update.txt
                    grep CONF ./result.update.txt | tail -3
                    tail -2 ./result.update.txt
                    if [ "${GET_TEST}" == "Yes" ]
                    then
                        TCNT=0
                        rm thread.default.conf
                        while [  $TCNT -lt ${NR_THREAD[${THREAD_IDX}]} ]; do
                            echo "${ASYNC[$IOTYPE_IDX]} 0 100 ${KEY_TYPE[${KEY_TYPE_IDX}]} ${SEQ[${SEQ_IDX}]} ${NR_KEY} ${NR_REQ}" >> thread.default.conf 
                            let TCNT=TCNT+1 
                        done

                        echo " GET(${NR_THREAD[${THREAD_IDX}]} threads) | ${ASYNC[$IOTYPE_IDX]}| 0| 100| ${KEY_TYPE[${KEY_TYPE_IDX}]}| ${SEQ[${SEQ_IDX}]}| ${NR_KEY}| ${NR_REQ}"
                        echo " GET(${NR_THREAD[${THREAD_IDX}]} threads) | ${ASYNC[$IOTYPE_IDX]}| 0| 100| ${KEY_TYPE[${KEY_TYPE_IDX}]}| ${SEQ[${SEQ_IDX}]}| ${NR_KEY}| ${NR_REQ}" >> result.get.txt

                        echo > /proc/kv_proc
                        ./kv_bench -u n >> result.get.txt
                        grep CONF ./result.get.txt | tail -3
                        tail -2 ./result.get.txt
                    fi
                    TCNT=0
                    rm thread.default.conf
                    while [  $TCNT -lt ${NR_THREAD[${THREAD_IDX}]} ]; do
                        echo "${ASYNC[$IOTYPE_IDX]} 0 0 ${KEY_TYPE[${KEY_TYPE_IDX}]} ${SEQ[${SEQ_IDX}]} ${NR_KEY} ${NR_REQ}" >> thread.default.conf 
                        let TCNT=TCNT+1 
                    done

                    echo " DEL(${NR_THREAD[${THREAD_IDX}]} threads) | ${ASYNC[$IOTYPE_IDX]}| 0| 0| ${KEY_TYPE[${KEY_TYPE_IDX}]}| ${SEQ[${SEQ_IDX}]}| ${NR_KEY}| ${NR_REQ}|"
                    echo " DEL(${NR_THREAD[${THREAD_IDX}]} threads) | ${ASYNC[$IOTYPE_IDX]}| 0| 0| ${KEY_TYPE[${KEY_TYPE_IDX}]}| ${SEQ[${SEQ_IDX}]}| ${NR_KEY}| ${NR_REQ}|" >> result.del.txt
                    echo > /proc/kv_proc
                    ./kv_bench -u n >> result.del.txt
                    grep CONF ./result.del.txt | tail -3
                    tail -2 ./result.del.txt
                    let TOTAL_CNT=TOTAL_CNT+1
                    echo "${TOTAL_CNT}/${TOTAL_LOOP}"
                    echo " Start clearing the remaining keys "
                    ./erase.all.sh 
                    echo " End clearing the remaining keys "
                    let COUNTER=COUNTER+1 
                done
                let SEQ_IDX=SEQ_IDX+1 
            done
            let KEY_TYPE_IDX=KEY_TYPE_IDX+1 
        done
        let IOTYPE_IDX=IOTYPE_IDX+1 
    done
    let THREAD_IDX=THREAD_IDX+1 
done
