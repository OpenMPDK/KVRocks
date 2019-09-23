#!/bin/bash -xe
if [ "$#" -ne 4 ]
then
    echo "invalid # of argument"
    echo "use: $0 <mysql_server> <mysql_port> <warehouse#> <hours>"
    exit 1
fi
# run_tpcc.sh <mysql_server> <mysql_port> <warehouse#> <run_duration_in_Hours>

cd ${SRCTOP}/kvdb/patches/percona-server/tpcc

DBSERVER=$1
DBPORT=$2
WAREHOUSE=$3
TPCCHOUR=$4
TPCCTIME=$[$4*{$TPCCTIME}]

set +e
mysqladmin -P${DBPORT} -h ${DBSERVER} -uroot -proot -f DROP tpcc1000

set -e
mysqladmin -P${DBPORT} -h ${DBSERVER} -uroot -proot create tpcc1000
mysql -P${DBPORT} -h ${DBSERVER} -uroot -proot -f tpcc1000 < create_table.sql

# sequential load
#./tpcc_load -h ${DBSERVER}  -P${DBPORT} -d tpcc1000 -uroot -proot -w100  
#./tpcc_start -h ${DBSERVER} -d tpcc1000 -uroot -proot -w100 -c32 -r300 -l10800

# parallel load
#./load.sh ${DBSERVER} tpcc1000 100
ts=`date +"%Y%m%d_%H_%M_%S"`

# wearhouse# 100, port ${DBPORT}
./load_nolog.sh ${DBSERVER} tpcc1000 ${WAREHOUSE} ${DBPORT} 10
./tpcc_start -P${DBPORT} -h ${DBSERVER} -d tpcc1000 -uroot -proot -w${WAREHOUSE} -c32 -r60 -l${TPCCTIME}
