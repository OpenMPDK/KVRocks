#!/bin/bash -xe
# ./tpch.sh <kvrocks | myrocks> <warehouse#> <test_spec>

#~/bin/setup_kvrocks_master_ali_4k_a4_kc50m.sh
#
#~/bin/tpch.sh kvrocks 10 mr4k_a4_50m

set +e
~/bin/stopmysql.sh

~/bin/setup_kvrocks_master_ali_24k_a4_kc50m.sh

~/bin/tpch.sh kvrocks 10 mr24k_a4_50m_df20k

~/bin/stopmysql.sh


~/bin/setup_kvrocks_master_ali_64k_a4_kc50m.sh

~/bin/tpch.sh kvrocks 10 mr64k_a4_50m_df20k

~/bin/stopmysql.sh
