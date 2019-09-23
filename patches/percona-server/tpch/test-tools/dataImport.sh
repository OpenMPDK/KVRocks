#!/bin/bash

target_db_asynchronous=`cat config.properties|grep "target_db_asynchronous"|cut -d"=" -f2`
target_db_delete=`cat config.properties|grep "target_db_delete"|cut -d"=" -f2`

# 数据库配置信息
target_db_ip=`cat config.properties|grep "target_db_ip"|cut -d"=" -f2`
target_db_port=`cat config.properties|grep "target_db_port"|cut -d"=" -f2`
target_db_user=`cat config.properties|grep "target_db_user"|cut -d"=" -f2`
target_db_password=`cat config.properties|grep "target_db_password"|cut -d"=" -f2`
target_db_name=`cat config.properties|grep "target_db_name"|cut -d"=" -f2`


# arg1=start, arg2=end, format: %s.%N
function getTiming() {
    start=$1
    end=$2
    start_s=$(echo $start | cut -d '.' -f 1)
    start_ns=$(echo $start | cut -d '.' -f 2)
    end_s=$(echo $end | cut -d '.' -f 1)
    end_ns=$(echo $end | cut -d '.' -f 2)
    time=$(( ( 10#$end_s - 10#$start_s ) * 1000 + ( 10#$end_ns / 1000000 - 10#$start_ns / 1000000 ) ))
    echo "$time ms"
}


TEST_TOOLS_HOME=`pwd`;
dbURL="-h $target_db_ip -P $target_db_port  -u$target_db_user"
if [ -z $target_db_password ];
then
  echo URL=$dbURL
else
  dbURL=$dbURL" -p$target_db_password"
  echo URL=$dbURL
fi



#创建数据库
if [ "0" -eq "$target_db_delete" ];
then
echo "delete db $target_db_name"
mysql $dbURL <<EOF
DROP DATABASE IF  EXISTS $target_db_name;
EOF
fi

echo "crate db $target_db_name"
mysql $dbURL <<EOF
CREATE DATABASE IF NOT EXISTS $target_db_name;
EOF

# 创建表
echo "crate tables"
mysql $dbURL -D $target_db_name<$TEST_TOOLS_HOME/initdb/dss.sql

# 导入数据
echo "import datas"


if [ "0" -eq "$target_db_asynchronous" ];
then
    echo "import data target_db_asynchronous"
	nohup mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/CUSTOMER.sql&
	nohup mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/LINEITEM.sql&
	nohup mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/PART.sql&
	nohup mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/NATION.sql&
	nohup mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/ORDERS.sql&
	nohup mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/PARTSUPP.sql&
	nohup mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/REGION.sql&
	nohup mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/SUPPLIER.sql&
	echo "import data target_db_asynchronous running"
else
	start=$(date +%s.%N)
	mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/NATION.sql
	end=$(date +%s.%N)
	echo import NATION.sql end in `getTiming $start $end`

    start=$(date +%s.%N)
	mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/PART.sql
	end=$(date +%s.%N)
	echo import PART.sql end in `getTiming $start $end`

	start=$(date +%s.%N)
	mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/REGION.sql
	end=$(date +%s.%N)
	echo import REGION.sql end in `getTiming $start $end`

    start=$(date +%s.%N)
    mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/CUSTOMER.sql
    end=$(date +%s.%N)
    echo import CUSTOMER.sql end in `getTiming $start $end`

	start=$(date +%s.%N)
	mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/ORDERS.sql
	end=$(date +%s.%N)
	echo import ORDERS.sql end in `getTiming $start $end`

	start=$(date +%s.%N)
	mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/SUPPLIER.sql
	end=$(date +%s.%N)
	echo import SUPPLIER.sql end in `getTiming $start $end`

	start=$(date +%s.%N)
	mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/PARTSUPP.sql
	end=$(date +%s.%N)
	echo import PARTSUPP.sql end in `getTiming $start $end`

    start=$(date +%s.%N)
	mysql $dbURL  --local-infile=1 -D $target_db_name < $TEST_TOOLS_HOME/impdata/LINEITEM.sql
	end=$(date +%s.%N)
	echo import LINEITEM.sql end in `getTiming $start $end`

fi


