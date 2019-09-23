#!/bin/bash -xe
#export LD_LIBRARY_PATH=/usr/local/mysql/lib/mysql/
HOST=$1
DBNAME=$2
WH=$3
PORT=$4

STEP=$5
#STEP=100

set +e
killall tpcc_load
killall tpcc_load

set -e
echo data loading started

./tpcc_load -P $PORT -h $HOST -d $DBNAME -uroot -proot -w $WH -l 1 -m 1 -n $WH  &

x=1

while [ $x -le $WH ]
do
 echo $x $(( $x + $STEP - 1 ))
./tpcc_load -P $PORT -h $HOST -d $DBNAME -u root -p root  -w $WH -l 2 -m $x -n $(( $x + $STEP - 1 ))  &
./tpcc_load -P $PORT -h $HOST -d $DBNAME -u root -p root -w $WH -l 3 -m $x -n $(( $x + $STEP - 1 ))  &
./tpcc_load -P $PORT -h $HOST -d $DBNAME -u root -p root -w $WH -l 4 -m $x -n $(( $x + $STEP - 1 ))  &
 x=$(( $x + $STEP ))
done

wait
echo data loading done
