#!/bin/bash

target_db_databaseNum=`cat config.properties|grep "target_db_databaseNum"|cut -d"=" -f2`

# 清理之前的遗留数据
TEST_TOOLS_HOME=`pwd`;
rm -rf $TEST_TOOLS_HOME/impdata
mkdir $TEST_TOOLS_HOME/impdata


# 进行编译
cd ..
TPCH_MYSQL_HOME=`pwd`;
cd $TPCH_MYSQL_HOME/dbgen
rm -rf $TPCH_MYSQL_HOME/dbgen/*.o
rm -rf $TPCH_MYSQL_HOME/dbgen/dbgen
rm -rf $TPCH_MYSQL_HOME/dbgen/qgen
echo "make tools"
make

#开始生成数据
cp -rf $TPCH_MYSQL_HOME/dbgen/dbgen $TEST_TOOLS_HOME/impdata/
cp -rf $TPCH_MYSQL_HOME/dbgen/dists.dss $TEST_TOOLS_HOME/impdata/
cd $TEST_TOOLS_HOME/impdata/
echo "make tbl"
./dbgen -s $target_db_databaseNum

DIR=`pwd`
file=``
for tbl in `ls *.tbl`; do
    table=$(echo "${tbl%.*}" | tr '[:lower:]' '[:upper:]')
	name=${table#$prefix}
	file="$name.sql"
	if [ ! -f "$file" ] ; then
        touch "$file"
	fi
    echo "LOAD DATA LOCAL INFILE '$DIR/$tbl' INTO TABLE ${table#$prefix}" >> $file
    echo "FIELDS TERMINATED BY '|';" >> $file
done

rm -rf $TEST_TOOLS_HOME/impdata/dbgen
rm -rf $TEST_TOOLS_HOME/impdata/dists.dss

