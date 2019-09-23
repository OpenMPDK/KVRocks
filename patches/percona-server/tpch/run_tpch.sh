#!/bin/bash -xe
if [ "$#" -ne 2 ]
then
    echo "invalid # of argument"
    echo "use: $0 <kvrocks | myrocks> <logdir>"
    exit 1
fi

DBTYPE=$1
LOGDIR_BASE=$2

TPCHSRC=${SRCTOP}/kvdb/patches/percona-server/tpch/test-tools
cd ${TPCHSRC}

./dataMake.sh
./dataImport.sh

ts=`date +"%Y%m%d_%H_%M_%S"`
set +e
LOGDIR=${LOGDIR_BASE}/tpch/log.${DBTYPE}_${ts}

mkdir -p ${LOGDIR}
sleep 2

cd ${LOGDIR}

cd ${TPCHSRC}

./queryExec_skipslow.sh ${DBTYPE} ${LOGDIR} 2>&1 | tee ${LOGDIR}/tpch.log

cd ${LOGDIR}

# save config
cp ${TPCHSRC}/config.properties .
