#!/bin/bash -xe
set +e
mysqladmin -uroot -proot -S /var/run/mysqld/mysqld.sock shutdown

sleep 10
sudo kill -9 $(pidof mysqld)
sleep 2

~/bin/stopsar.sh
