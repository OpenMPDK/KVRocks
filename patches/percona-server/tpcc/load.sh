#!/bin/bash -xe
#export LD_LIBRARY_PATH=/usr/local/mysql/lib/mysql/
HOST=$1
DBNAME=$2
WH=$3

STEP=10

set +e
killall tpcc_load
killall tpcc_load
rm -rf log
mkdir log

set -e
echo data loading started

./tpcc_load -h $HOST -d $DBNAME -uroot -proot -w $WH -l 1 -m 1 -n $WH >> log/1.out &

x=1

while [ $x -le $WH ]
do
 echo $x $(( $x + $STEP - 1 ))
./tpcc_load -h $HOST -d $DBNAME -u root -p root  -w $WH -l 2 -m $x -n $(( $x + $STEP - 1 ))  >> log/2_$x.out &
./tpcc_load -h $HOST -d $DBNAME -u root -p root -w $WH -l 3 -m $x -n $(( $x + $STEP - 1 ))  >> log/3_$x.out &
./tpcc_load -h $HOST -d $DBNAME -u root -p root -w $WH -l 4 -m $x -n $(( $x + $STEP - 1 ))  >> log/4_$x.out &
 x=$(( $x + $STEP ))
done

wait
echo data loading done
