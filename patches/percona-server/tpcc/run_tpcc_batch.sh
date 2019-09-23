#!/bin/bash -xe
#~/bin/tpcc.sh <myrocks|kvrocks> <mysql_server> <mysql_port> <log_dir> <warehouse#> <hours> <test_spec>"

DBSERVER=10.10.140.211

rm -f /tmp/tpcc.log

#ssh jim.li@${DBSERVER} "cd ~/bin && ./setup_kvrocks_master_ali_96k_a4_kc25m.sh" 2>&1 >> /tmp/tpcc.log
#~/bin/tpcc.sh kvrocks ${DBSERVER} 3306 ~/log/tpcc 100 1 fwQ1127_mr96k_a4_kc25m
#ssh jim.li@${DBSERVER} "cd ~/bin && ./stopmysql.sh" 2>&1 >> /tmp/tpcc.log
set +e
ssh jim.li@${DBSERVER} "cd ~/bin && ./stopmysql.sh" 2>&1 >> /tmp/tpcc.log

ssh jim.li@${DBSERVER} "cd ~/bin && ./setup_kvrocks_master_ali_64k_a4_kc25m.sh" 2>&1 >> /tmp/tpcc.log
~/bin/tpcc.sh kvrocks ${DBSERVER} 3306 ~/log/tpcc 100 1 fwQ1127_mr64k_a4_kc25m_master
ssh jim.li@${DBSERVER} "cd ~/bin && ./stopmysql.sh" 2>&1 >> /tmp/tpcc.log

ssh jim.li@${DBSERVER} "cd ~/bin && ./setup_kvrocks_master_ali_48k_a4_kc25m.sh" 2>&1 >> /tmp/tpcc.log
~/bin/tpcc.sh kvrocks ${DBSERVER} 3306 ~/log/tpcc 100 1 fwQ1127_mr48k_a4_kc25m_master
ssh jim.li@${DBSERVER} "cd ~/bin && ./stopmysql.sh" 2>&1 >> /tmp/tpcc.log

ssh jim.li@${DBSERVER} "cd ~/bin && ./setup_kvrocks_master_ali_24k_a4_kc25m.sh" 2>&1 >> /tmp/tpcc.log
~/bin/tpcc.sh kvrocks ${DBSERVER} 3306 ~/log/tpcc 100 1 fwQ1127_mr24k_a4_kc25m_master
ssh jim.li@${DBSERVER} "cd ~/bin && ./stopmysql.sh" 2>&1 >> /tmp/tpcc.log

ssh jim.li@${DBSERVER} "cd ~/bin && ./setup_kvrocks_master_ali_8k_a4_kc25m.sh" 2>&1 >> /tmp/tpcc.log
~/bin/tpcc.sh kvrocks ${DBSERVER} 3306 ~/log/tpcc 100 1 fwQ1127_mr8k_a4_kc25m_master
ssh jim.li@${DBSERVER} "cd ~/bin && ./stopmysql.sh" 2>&1 >> /tmp/tpcc.log

ssh jim.li@${DBSERVER} "cd ~/bin && ./setup_kvrocks_master_ali_4k_a4_kc25m.sh" 2>&1 >> /tmp/tpcc.log
~/bin/tpcc.sh kvrocks ${DBSERVER} 3306 ~/log/tpcc 100 1 fwQ1127_mr4k_a4_kc25m_master
ssh jim.li@${DBSERVER} "cd ~/bin && ./stopmysql.sh" 2>&1 >> /tmp/tpcc.log

ssh jim.li@${DBSERVER} "cd ~/bin && ./setup_kvrocks_master_ali_2k_a4_kc25m.sh" 2>&1 >> /tmp/tpcc.log
~/bin/tpcc.sh kvrocks ${DBSERVER} 3306 ~/log/tpcc 100 1 fwQ1127_mr2k_a4_kc25m_master
ssh jim.li@${DBSERVER} "cd ~/bin && ./stopmysql.sh" 2>&1 >> /tmp/tpcc.log
