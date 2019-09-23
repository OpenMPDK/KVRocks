#!/bin/bash -xe
if [ "$#" -ne 3 ]
then
    echo "invalid # of argument"
    echo "use: $0 <kvrocks | myrocks> <warehouse#> <test_spec>"
    exit 1
fi

DBTYPE=$1
WAREHOUSE=$2
TESTSPEC=$3

# actual configuration is done through 
# ~/src/tpch/test-tools.kvrocks/config.properties

TPCHSRC=~/src/tpch/test-tools.kvrocks
cd ${TPCHSRC}

# data must be generated first
./dataMake.sh
./dataImport.sh

ts=`date +"%Y%m%d_%H_%M_%S"`
set +e
LOGDIR=~/log/tpch/log.${DBTYPE}_w${WAREHOUSE}_${TESTSPEC}_${ts}

mkdir -p ${LOGDIR}
sleep 2

cd ${LOGDIR}

## just set max 12 hours
~/bin/sar_smart.sh ${DBTYPE} 43200 &

cd ${TPCHSRC}

./queryExec_skipslow.sh ${DBTYPE} ${LOGDIR} 2>&1 | tee ${LOGDIR}/tpch_w${WAREHOUSE}_${TESTSPEC}_${ts}.log

cd ${LOGDIR}

# save config
cp ${TPCHSRC}/config.properties .
sudo cp /etc/mysql/percona-server.conf.d/mysqld.cnf .

sleep 20s
~/bin/sar_smart_stop.sh
~/bin/stopsar.sh
