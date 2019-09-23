#!/bin/bash -xe
if [ "$#" -ne 7 ]
then
    echo "invalid # of argument"
    echo "use: $0 <kvrocks | myrocks> <mysql_server> <mysql_port> <output_dir> <warehouse#> <hours> <test_spec>"
    exit 1
fi

DBTYPE=$1
DBSERVER=$2
DBPORT=$3
LOGDIR=$4
WAREHOUSE=$5
TPCCHOUR=$6
TPCCTIME=$[$6*3600]
TEST_SPEC=$7

CLIENT=`hostname`
SERVER=`ssh jim.li@${DBSERVER} \"hostname\"`

cd /home/jim.li/src/tpcc

set +e
mysqladmin -P${DBPORT} -h ${DBSERVER} -uroot -proot -f DROP tpcc1000

set -e
mysqladmin -P${DBPORT} -h ${DBSERVER} -uroot -proot create tpcc1000
mysql -P${DBPORT} -h ${DBSERVER} -uroot -proot -f tpcc1000 < create_table.sql

# sequential load
#./tpcc_load -h ${DBSERVER}  -P${DBPORT} -d tpcc1000 -uroot -proot -w${WAREHOUSE}  
#./tpcc_start -h ${DBSERVER} -d tpcc1000 -uroot -proot -w${WAREHOUSE} -c32 -r300 -l10800

# parallel load
#./load.sh ${DBSERVER} tpcc1000 ${WAREHOUSE}
ts=`date +"%Y%m%d_%H_%M_%S"`

set +e
mkdir -p ${LOGDIR}/log.${CLIENT}_to_${SERVER}.${DBTYPE}_${TPCCHOUR}hr_${TEST_SPEC}_${ts}
sleep 5

set -e
ssh jim.li@${DBSERVER} "cd ${LOGDIR}/log.${CLIENT}_to_${SERVER}.${DBTYPE}_${TPCCHOUR}hr_${TEST_SPEC}_${ts} && ~/bin/sar_smart.sh ${DBTYPE} ${TPCCTIME} && sleep 10" 2>&1 > /dev/null &

./load_nolog.sh ${DBSERVER} tpcc1000 ${WAREHOUSE} ${DBPORT} 10
./tpcc_start -P${DBPORT} -h ${DBSERVER} -d tpcc1000 -uroot -proot -w${WAREHOUSE} -c32 -r60 -l${TPCCTIME} 2>&1 | tee ${LOGDIR}/log.${CLIENT}_to_${SERVER}.${DBTYPE}_${TPCCHOUR}hr_${TEST_SPEC}_${ts}/tpcc_w${WAREHOUSE}_${CLIENT}_to_${SERVER}.log

set +e
ssh jim.li@${DBSERVER} "cd ${LOGDIR}/log.${CLIENT}_to_${SERVER}.${DBTYPE}_${TPCCHOUR}hr_${TEST_SPEC}_${ts} && ~/bin/sar_smart_stop.sh" 2>&1 > /dev/null

ssh jim.li@${DBSERVER} "cd ${LOGDIR}/log.${CLIENT}_to_${SERVER}.${DBTYPE}_${TPCCHOUR}hr_${TEST_SPEC}_${ts} && sudo cp /etc/mysql/percona-server.conf.d/mysqld.cnf ." 2>&1 > /dev/null

cd ${LOGDIR}/log.${CLIENT}_to_${SERVER}.${DBTYPE}_${TPCCHOUR}hr_${TEST_SPEC}_${ts}
~/bin/kvproc.sh

## plot using R at msl-dpe-perf63 only
ssh jim.li@msl-dpe-perf63 "cd ${LOGDIR}/log.${CLIENT}_to_${SERVER}.${DBTYPE}_${TPCCHOUR}hr_${TEST_SPEC}_${ts} && ~/bin/tpcc_plot.R MR 4 KC ${TEST_SPEC} ." 2>&1 > /dev/null

#echo "*** branch master" >> ${LOGDIR}/log.${CLIENT}_to_${SERVER}.${DBTYPE}_${TPCCHOUR}hr_${TEST_SPEC}_${ts}/tpcc_${WAREHOUSE}_${CLIENT}_to_${SERVER}.log
