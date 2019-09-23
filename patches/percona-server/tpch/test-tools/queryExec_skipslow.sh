#!/bin/bash
if [ "$#" -ne 2 ]
then
    echo "invalid # of argument"
    echo "use: $0 <kvrocks | myrocks> <output_dir>"
    exit 1
fi

if ! [[ "$1" == "kvrocks" || "$1" == "myrocks" ]]
then
    echo "invalid type: $1, use: kvrocks | myrocks"
    exit 1
fi

target_db_databaseNum=`cat config.properties|grep "target_db_databaseNum"|cut -d"=" -f2`
dbtype=$1
ts=`date +"%Y%m%d_%H_%M_%S"`
csvfile="$2/tpch_${ts}.csv"
echo "query# results also saved in $csvfile"
echo "query# format: dbnum, dbtype, date, utc, query, query_number, duration, unit"
echo "dbnum, dbtype, date, utc, query, query_number, duration, unit" >> $csvfile

# 数据库配置信息
target_db_ip=`cat config.properties|grep "target_db_ip"|cut -d"=" -f2`
target_db_port=`cat config.properties|grep "target_db_port"|cut -d"=" -f2`
target_db_user=`cat config.properties|grep "target_db_user"|cut -d"=" -f2`
target_db_password=`cat config.properties|grep "target_db_password"|cut -d"=" -f2`
target_db_name=`cat config.properties|grep "target_db_name"|cut -d"=" -f2`

TEST_TOOLS_HOME=`pwd`;
dbURL="-h $target_db_ip -P $target_db_port  -u$target_db_user"
if [ -z $target_db_password ];
then
  echo URL=$dbURL
else
  dbURL=$dbURL" -p$target_db_password"
  echo URL=$dbURL
fi

# arg1=start, arg2=end, format: %s.%N
function getTiming() {
    start=$1
    end=$2
    start_s=$(echo $start | cut -d '.' -f 1)
    start_ns=$(echo $start | cut -d '.' -f 2)
    end_s=$(echo $end | cut -d '.' -f 1)
    end_ns=$(echo $end | cut -d '.' -f 2)
    time=$(( ( 10#$end_s - 10#$start_s ) * 1000 + ( 10#$end_ns / 1000000 - 10#$start_ns / 1000000 ) ))
    echo "$time, ms"
}

for((i=1;i<=22;i++));
do
    if [[ $i -eq 13 || $i -eq 17 || $i -eq 20 || $i -eq 22 ]]
    then
        continue
    else
        echo "============================================================================"
        start=$(date +%s.%N)
        mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/queries/$i.sql
        end=$(date +%s.%N)
        ts=`date +"%Y%m%d-%H-%M-%S"`
        utc=`date +"%s"`
        echo $target_db_databaseNum, $dbtype, $ts, $utc, query# $i, $i, `getTiming $start $end`
        echo $target_db_databaseNum, $dbtype, $ts, $utc, query# $i, $i, `getTiming $start $end` >> $csvfile
    fi
done
