#!/bin/bash
target_db_databaseNum=`cat config.properties|grep "target_db_databaseNum"|cut -d"=" -f2`

# 清理之前的遗留数据
TEST_TOOLS_HOME=`pwd`;
rm -rf $TEST_TOOLS_HOME/queries
mkdir $TEST_TOOLS_HOME/queries

# 进行编译
cd ..
TPCH_MYSQL_HOME=`pwd`;
cd $TPCH_MYSQL_HOME/dbgen
rm -rf $TPCH_MYSQL_HOME/dbgen/*.o
rm -rf $TPCH_MYSQL_HOME/dbgen/dbgen
rm -rf $TPCH_MYSQL_HOME/dbgen/qgen
echo "make tools"
make

#开始生成查询sql
cp -rf $TPCH_MYSQL_HOME/dbgen/queries $TEST_TOOLS_HOME/queries
cp -rf $TPCH_MYSQL_HOME/dbgen/qgen $TEST_TOOLS_HOME/queries/queries/
cp -rf $TPCH_MYSQL_HOME/dbgen/dists.dss $TEST_TOOLS_HOME/queries/queries/
cd $TEST_TOOLS_HOME/queries/queries/

for sqls in `ls *.sql`; do
    name=$(echo "${sqls%.*}" | tr '[:lower:]' '[:upper:]')
    ./qgen -d $name -s $target_db_databaseNum>$TEST_TOOLS_HOME/queries/$name.sql
done

rm -rf $TEST_TOOLS_HOME/queries/queries/;


