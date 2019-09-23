
TPCH查询SQL解析
=====

#### 目录
* [Q1价格统计报告查询](#q1价格统计报告查询)
* [Q2最小代价供货商查询](#q2最小代价供货商查询)
* [Q3运送优先级查询](#q3运送优先级查询)
* [Q4订单优先级查询](#q4订单优先级查询)
* [Q5某地区供货商为公司带来的收入查询](#q5某地区供货商为公司带来的收入查询)
* [Q6预测收入变化查询](#q6预测收入变化查询)
* [Q7货运盈利情况查询](#q7货运盈利情况查询)
* [Q8国家市场份额查询](#q8国家市场份额查询)
* [Q9产品类型利润估量查询](#q9产品类型利润估量查询)
* [Q10货运存在问题的查询](#q10货运存在问题的查询)
* [Q11库存价值查询](#q11库存价值查询)
* [Q12货运模式和订单优先级查询](#q12货运模式和订单优先级查询)
* [Q13消费者订单数量查询](#q13消费者订单数量查询)
* [Q14促销效果查询](#q14促销效果查询)
* [Q15头等供货商查询](#q15头等供货商查询)
* [Q16零件供货商关系查询](#q16零件供货商关系查询)
* [Q17小订单收入查询](#q17小订单收入查询)
* [Q18大订单顾客查询](#q18大订单顾客查询)
* [Q19折扣收入查询](#q19折扣收入查询)
* [Q20供货商竞争力查询](#q20供货商竞争力查询)
* [Q21不能按时交货供货商查询](#q21不能按时交货供货商查询)
* [Q22全球销售机会查询](#q22全球销售机会查询)

#### 定义说明
* SF:Scale Factor,数据库的比例因子。<br>
  TPC-H标准规定，测试数据库的比例因子必须从下列固定值中选择：1，10,30,100,1000,3000,10000 (相当于1GB，10GB,30GB,100GB,1000GB,3000GB,10000GB)。<br>
  数据库的大小缺省定义为1（例如：SF＝1；近似于1GB）。

#### Q1价格统计报告查询<br>
* Q1语句是查询lineItems的一个定价总结报告。在单个表lineitem上查询某个时间段内，对已经付款的、已经运送的等各类商品进行统计，包括业务量的计费、发货、折扣、税、平均价格等信息。<br>
* Q1语句的特点是：带有分组、排序、聚集操作并存的单表查询操作。这个查询会导致表上的数据有95%到97%行被读取到。<br>
````mysql
select
	l_returnflag,  #返回标志
	l_linestatus,
	sum(l_quantity) as sum_qty, #总的数量
	sum(l_extendedprice) as sum_base_price, #聚集函数操作
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order #每个分组所包含的行数
from
	lineitem
where
	l_shipdate <= date_sub('1998-12-01', interval 90 day) #时间段是随机生成的
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
````
[返回目录](#目录)
  
#### Q2最小代价供货商查询<br>
* Q2语句查询获得最小代价的供货商。得到给定的区域内，对于指定的零件（某一类型和大小的零件），哪个供应者能以最低的价格供应它，就可以选择哪个供应者来订货。<br>
* Q2语句的特点是：带有排序、聚集操作、子查询并存的多表查询操作。查询语句没有从语法上限制返回多少条元组，但是TPC-H标准规定，查询结果只返回前100行（通常依赖于应用程序实现）。<br>
````mysql
select
	s_acctbal, # 帐户余额
	s_name, # 名字
	n_name, # 国家
	p_partkey, # 零件的号码
	p_mfgr, # 生产者
	s_address, # 供应者的地址
	s_phone, # 电话号码
	s_comment # 备注信息
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 15 # 指定大小，在区间[1, 50]内随机选择
	and p_type like '%BRASS' # 指定类型，在TPC-H标准指定的范围内随机选择
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE' # 指定地区，在TPC-H标准指定的范围内随机选择
	and ps_supplycost = (
		select
			min(ps_supplycost) # 聚集函数
		from   # 与父查询的表有重叠
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'EUROPE'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;
````
[返回目录](#目录)

#### Q3运送优先级查询
* Q3语句查询得到收入在前10位的尚未运送的订单。在指定的日期之前还没有运送的订单中具有最大收入的订单的运送优先级（订单按照收入的降序排序）和潜在的收入（潜在的收入为l_extendedprice * (1-l_discount)的和）。
* Q3语句的特点是：带有分组、排序、聚集操作并存的三表查询操作。查询语句没有从语法上限制返回多少条元组，但是TPC-H标准规定，查询结果只返回前10行（通常依赖于应用程序实现）。
````mysql
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue, # 潜在的收入，聚集操作
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING' # 在TPC-H标准指定的范围内随机选择
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-03-15' # 指定日期段，在在[1995-03-01, 1995-03-31]中随机选择
	and l_shipdate > '1995-03-15'  # 指定日期段，在在[1995-03-01, 1995-03-31]中随机选择
group by
	l_orderkey, # 订单标识
	o_orderdate,  # 订单日期
	o_shippriority # 运输优先级
order by
	revenue desc, # 降序排序，把潜在最大收入列在前面
	o_orderdate
limit 10;
````
[返回目录](#目录)

#### Q4订单优先级查询
* Q4语句查询得到订单优先级统计值。计算给定的某三个月的订单的数量，在每个订单中至少有一行由顾客在它的提交日期之后收到。<br>
* Q4语句的特点是：带有分组、排序、聚集操作、子查询并存的单表查询操作。子查询是相关子查询。<br>

````mysql
select
	o_orderpriority, # 订单优先级
	count(*) as order_count # 订单优先级计数
from
	orders
where
	o_orderdate >= '1993-07-01'
	and o_orderdate < date_add('1993-07-01', interval '3' month) # 指定订单的时间段--某三个月，DATE是在1993年1月和1997年10月之间随机选择的一个月的第一天                                                                                        `````````````````````指定订单的时间段--某三个月，DATE是在1993年1月和1997年10月之间随机选择的一个月的第一天
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority # 按订单优先级分组
order by
	o_orderpriority; # 按订单优先级排序
````
[返回目录](#目录)

#### Q5某地区供货商为公司带来的收入查询
* Q5语句查询得到通过某个地区零件供货商而获得的收入（收入按sum(l_extendedprice * (1 -l_discount))计算）统计信息。可用于决定在给定的区域是否需要建立一个当地分配中心。
* Q5语句的特点是：带有分组、排序、聚集操作、子查询并存的多表连接查询操作。
````mysql
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA' # 指定地区，在TPC-H标准指定的范围内随机选择
	and o_orderdate >= '1994-01-01' # DATE是从1993年到1997年中随机选择的一年的1月1日
	and o_orderdate < date_add('1994-01-01', interval '1' year)
group by
	n_name # 按名字分组
order by
	revenue desc; # 按收入降序排序，注意分组和排序子句不同
````
[返回目录](#目录)

#### Q6预测收入变化查询
* Q6语句查询得到某一年中通过变换折扣带来的增量收入。这是典型的“what-if”判断，用来寻找增加收入的途径。预测收入变化查询考虑了指定的一年中折扣在“DISCOUNT-0.01”和“DISCOUNT＋0.01”之间的已运送的所有订单，求解把l_quantity小于quantity的订单的折扣消除之后总收入增加的数量。
* Q6语句的特点是：带有聚集操作的单表查询操作。查询语句使用了BETWEEN-AND操作符，有的数据库可以对BETWEEN-AND进行优化。
````mysql
select
	sum(l_extendedprice * l_discount) as revenue # 潜在的收入增加量
from
	lineitem
where
	l_shipdate >= '1994-01-01' # DATE是从[1993, 1997]中随机选择的一年的1月1日
	and l_shipdate < date_add('1994-01-01', interval '1' year) # 一年内
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24; # QUANTITY在区间[24, 25]中随机选择
````
[返回目录](#目录)

#### Q7货运盈利情况查询
* Q7语句是查询从供货商国家与销售商品的国家之间通过销售获利情况的查询。此查询确定在两国之间货运商品的量用以帮助重新谈判货运合同。
* Q7语句的特点是：带有分组、排序、聚集、子查询操作并存的多表查询操作。子查询的父层查询不存在其他查询对象，是格式相对简单的子查询。
````mysql
select
	supp_nation, # 供货商国家
	cust_nation, # 顾客国家
	l_year,
	sum(volume) as revenue # 年度、年度的货运收入
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and ( # NATION2和NATION1的值不同，表示查询的是跨国的货运情况
				(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
				or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
			)
			and l_shipdate between '1995-01-01' and '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
````
[返回目录](#目录)

#### Q8国家市场份额查询
* Q8语句是查询在过去的两年中一个给定零件类型在某国某地区市场份额的变化情况。
* Q8语句的特点是：带有分组、排序、聚集、子查询操作并存的查询操作。子查询的父层查询不存在其他查询对象，是格式相对简单的子查询，但子查询自身是多表连接的查询。
* TPC-H标准定义了Q8语句等价的变形SQL，与上述查询语句格式上基本相同，主要是目标列使用了不同的表达方式，在此不再赘述。
````mysql
select
	o_year, # 年份
	sum(case
		when nation = 'BRAZIL' then volume # 指定国家，在TPC-H标准指定的范围内随机选择
		else 0
	end) / sum(volume) as mkt_share # 市场份额：特定种类的产品收入的百分比；聚集操作
from
	(
		select
			extract(year from o_orderdate) as o_year, # 分解出年份
			l_extendedprice * (1 - l_discount) as volume, # 特定种类的产品收入
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'AMERICA' # 指定地区，在TPC-H标准指定的范围内随机选择
			and s_nationkey = n2.n_nationkey
			and o_orderdate between '1995-01-01' and '1996-12-31' # 只查95、96年的情况
			and p_type = 'ECONOMY ANODIZED STEEL' # 指定零件类型，在TPC-H标准指定的范围内随机选择
	) as all_nations
group by
	o_year # 按年分组
order by
	o_year; # 按年排序
````
[返回目录](#目录)

#### Q9产品类型利润估量查询
* Q9语句是查询每个国家每一年所有被定购的零件在一年中的总利润。
* Q9语句的特点是：带有分组、排序、聚集、子查询操作并存的查询操作。子查询的父层查询不存在其他查询对象，是格式相对简单的子查询，但子查询自身是多表连接的查询。子查询中使用了LIKE操作符，有的查询优化器不支持对LIKE操作符进行优化。
````mysql
select
	nation,
	o_year,
	sum(amount) as sum_profit # 每个国家每一年所有被定购的零件在一年中的总利润
from
	(
		select
			n_name as nation, # 国家
			extract(year from o_orderdate) as o_year, # 取出年份
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount # 利润
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%green%' # LIKE操作，查询优化器可能进行优化
	) as profit
group by
	nation, # 国家
	o_year  # 年份
order by
	nation, 
	o_year desc;
````
[返回目录](#目录)

#### Q10货运存在问题的查询
* Q10语句是查询每个国家在某时刻起的三个月内货运存在问题的客户和造成的损失。
* Q10语句的特点是：带有分组、排序、聚集操作并存的多表连接查询操作。查询语句没有从语法上限制返回多少条元组，但是TPC-H标准规定，查询结果只返回前10行（通常依赖于应用程序实现）。
````mysql
select
	c_custkey, # 客户信息
	c_name,    # 客户信息
	sum(l_extendedprice * (1 - l_discount)) as revenue, # 收入损失
	c_acctbal,
	n_name,   # 国家
	c_address,# 地址
	c_phone,  # 电话
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= '1993-10-01' # DATE是位于1993年一月到1994年十二月中任一月的一号
	and o_orderdate < date_add('1993-10-01', interval '3' month) # 3个月内
	and l_returnflag = 'R' # 货物被回退
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20;
````
[返回目录](#目录)

#### Q11库存价值查询
* Q11语句是查询库存中某个国家供应的零件的价值。
* Q11语句的特点是：带有分组、排序、聚集、子查询操作并存的多表连接查询操作。子查询位于分组操作的HAVING条件中。
````mysql
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value # 聚集操作，商品的总价值
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'GERMANY'
group by
	ps_partkey having # 带有HAVING子句的分组操作
		sum(ps_supplycost * ps_availqty) > ( # HAVING子句中包括有子查询
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000 # 子查询中存在聚集操作；FRACTION为0.0001/SF1
			from  # 与父查询的表连接一致
				partsupp,
				supplier,
				nation
			where # 与父查询的WHEWR条件一致
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'GERMANY'
		)
order by # 按商品的价值降序排序
	value desc;
````
[返回目录](#目录)

#### Q12货运模式和订单优先级查询
* Q12语句查询获得货运模式和订单优先级。可以帮助决策：选择便宜的货运模式是否会导致消费者更多的在合同日期之后收到货物，而对紧急优先命令产生负面影响。
* Q12语句的特点是：带有分组、排序、聚集操作并存的两表连接查询操作。
* TPC-H标准定义了Q12语句等价的变形SQL，与上述查询语句格式上基本相同，主要是目标列使用了不同的表达方式，在此不再赘述。
````mysql
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT' # OR运算，二者满足其一即可，选出URGENT或HIGH的
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT' # AND运算，二者都不满足，非URGENT非HIGH的
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('MAIL', 'SHIP') # 指定货运模式的类型，在TPC-H标准指定的范围内随机选择，SHIPMODE2必须有别于SHIPMODE1
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= '1994-01-01' # 从1993年到1997年中任一年的一月一号
	and l_receiptdate < date_add('1994-01-01', interval '1' year) # 1年内
group by
	l_shipmode
order by
	l_shipmode;
````
[返回目录](#目录)

#### Q13消费者订单数量查询
* Q13语句查询获得消费者的订单数量，包括过去和现在都没有订单记录的消费者。
* Q13语句的特点是：带有分组、排序、聚集、子查询、左外连接操作并存的查询操作。
TPC-H标准定义了Q13语句等价的变形SQL，与上述查询语句格式上不相同，上述语句使用子查询作为查询的对象，变形的SQL把子查询部分变为视图，然后基于视图做查询，这种做法的意义在于有些数据库不支持如上语法，但存在等价的其他语法
````mysql
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count
		from  # 子查询中包括左外连接操作
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%special%requests%' 
				# WORD1 为以下四个可能值中任意一个：special、pending、unusual、express
				# WORD2 为以下四个可能值中任意一个：packages、requests、accounts、deposits
		group by
			c_custkey
	) c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc;
````
如MySQL就不支持如上语法，需要使用如下等价形式。
````mysql
create view orders_per_cust:s (custkey, ordercount) as  # 创建视图，相当与标准Q13的子查询内容

select

c_custkey,

count(o_orderkey)

from

customer left outer join orders on

c_custkey = o_custkey

and o_comment not like '%:1%:2%'

group by

c_custkey;



select

ordercount,

count(*) as custdist

from

orders_per_cust:s # 对视图进行查询

group by

ordercount

order by

custdist desc,

ordercount desc;

drop view orders_per_cust:s;
````

[返回目录](#目录)

#### Q14促销效果查询
* Q14语句查询获得某一个月的收入中有多大的百分比是来自促销零件。用以监视促销带来的市场反应。
* Q14语句的特点是：带有分组、排序、聚集、子查询、左外连接操作并存的查询操作。
* TPC-H标准定义了Q14语句等价的变形SQL，与上述查询语句格式上基本相同，主要是目标列使用了不同的表达方式，在此不再赘述。
````mysql
select
	100.00 * sum(case
		when p_type like 'PROMO%' # 促销零件
			then l_extendedprice * (1 - l_discount) # 某一特定时间的收入
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= '1995-09-01' # DATE是从1993年到1997年中任一年的任一月的一号
	and l_shipdate < date_add('1995-09-01', interval '1' month);
````
[返回目录](#目录)

#### Q15头等供货商查询
* Q15语句查询获得某段时间内为总收入贡献最多的供货商（排名第一）的信息。可用以决定对哪些头等供货商给予奖励、给予更多订单、给予特别认证、给予鼓舞等激励。
* Q15语句的特点是：带有分排序、聚集、聚集子查询操作并存的普通表与视图的连接操作。
````mysql
create view revenue0 (supplier_no, total_revenue) as # 创建复杂视图（带有分组操作）
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount)) # 获取供货商为公司带来的总利润
	from
		lineitem
	where
		l_shipdate >= '1996-01-01' # DATE 是从1993年一月到1997年十月中任一月的一号
		and l_shipdate < date_add('1996-01-01', interval '3' month) # 3个月内
	group by # 分组键与查询对象之一相同
		l_suppkey;


select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue0  # 普通表与复杂视图进行连接操作
where
	s_suppkey = supplier_no # 聚集子查询
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue0 # 聚集子查询从视图获得数据
	)
order by
	s_suppkey;

drop view revenue0;
````


* TPC-H标准定义了Q15语句等价的变形SQL，与上述查询语句格式上不相同，上述查询语句首先定义了视图，然后用表与视图连接；变形的SQL定了WITH语句，然后用WITH的对象与表进行连接。变形SQL的语句如下：
````mysql
WITH revenue (supplier_no, total_revenue) as (

SELECT

l_suppkey,

SUM(l_extendedprice * (1-l_discount))

FROM

lineitem

WHERE

l_shipdate >= date ':1'

AND l_shipdate < date ':1' + interval '3' month

GROUP BY

l_suppkey

)



SELECT

s_suppkey,

s_name,

s_address,

s_phone,

total_revenue

FROM

supplier,

revenue

WHERE

s_suppkey = supplier_no

AND total_revenue = (

SELECT

MAX(total_revenue)

FROM

revenue

)

ORDER BY

s_suppkey;
````
[返回目录](#目录)


#### Q16零件/供货商关系查询
* Q16语句查询获得能够以指定的贡献条件供应零件的供货商数量。可用于决定在订单量大，任务紧急时，是否有充足的供货商。
* Q16语句的特点是：带有分组、排序、聚集、去重、NOT IN子查询操作并存的两表连接操作。
````mysql
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt # 聚集、去重操作
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#45' # BRAND＝Brand＃MN ，M和N是两个字母，代表两个数值，相互独立，取值在1到5之间
	and p_type not like 'MEDIUM POLISHED%' # 消费者不感兴趣的类型和尺寸
	and p_size in (49, 14, 23, 45, 19, 3, 36, 9) # TYPEX是在1到50之间任意选择的一组八个不同的值
	and ps_suppkey not in ( # NOT IN子查询，消费者排除某些供货商
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by  # 按数量降序排列，按品牌、种类、尺寸升序排列
	supplier_cnt desc,  
	p_brand,
	p_type,
	p_size;
````
[返回目录](#目录)

#### Q17小订单收入查询
* Q17语句查询获得比平均供货量的百分之二十还低的小批量订单。对于指定品牌和指定包装类型的零件，决定在一个七年数据库的所有订单中这些订单零件的平均项目数量（过去的和未决的）。如果这些零件中少于平均数20％的订单不再被接纳，那平均一年会损失多少呢？所以此查询可用于计算出如果没有没有小量订单，平均年收入将损失多少（因为大量商品的货运，将降低管理费用）。
* Q17语句的特点是：带有聚集、聚集子查询操作并存的两表连接操作。
````mysql
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#23' # 指定品牌。 BRAND＝’Brand#MN’ ，M和N是两个字母，代表两个数值，相互独立，取值在1到5之间
	and p_container = 'MED BOX' # 指定包装类型。在TPC-H标准指定的范围内随机选择
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	);
````
[返回目录](#目录)

#### Q18大订单顾客查询
* Q18语句查询获得比指定供货量大的供货商信息。可用于决定在订单量大，任务紧急时，验证否有充足的供货商。
* Q18语句的特点是：带有分组、排序、聚集、IN子查询操作并存的三表连接操作。查询语句没有从语法上限制返回多少条元组，但是TPC-H标准规定，查询结果只返回前100行（通常依赖于应用程序实现）。
````mysql
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity) # 订货总数
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 300 # QUANTITY是位于312到315之间的任意值
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 100;
````
[返回目录](#目录)

#### Q19折扣收入查询
* Q19语句查询得到对一些空运或人工运输零件三个不同种类的所有订单的总折扣收入。零件的选择考虑特定品牌、包装和尺寸范围。本查询是用数据挖掘工具产生格式化代码的一个例子。
* Q19语句的特点是：带有分组、排序、聚集、IN子查询操作并存的三表连接操作 
````mysql
select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#12' # 特定品牌。BRAND1、BRAND2、BRAND3＝‘Brand＃MN’，M和N是两个字母，代表两个数值，相互独立，取值在1到5之间 
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') # 包装范围
		and l_quantity >= 1 and l_quantity <= 1 + 10 # QUANTITY1 是1到10之间的任意取值
		and p_size between 1 and 5 # 尺寸范围
		and l_shipmode in ('AIR', 'AIR REG') # 运输模式，如下带有阴影的粗体表示的条件是相同的，存在条件化简的可能
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#23'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 10 and l_quantity <= 10 + 10 # QUANTITY2 是10到20之间的任意取值 
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#34'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 20 and l_quantity <= 20 + 10 # QUANTITY3 是20到30之间的任意取值
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	);
````
[返回目录](#目录)

#### Q20供货商竞争力查询
* Q20语句查询确定在某一年内，找出指定国家的能对某一零件商品提供更有竞争力价格的供货货。所谓更有竞争力的供货商，是指那些零件有过剩的供货商，超过供或商在某一年中货运给定国的某一零件的50％则为过剩。
* Q20语句的特点是：带有排序、聚集、IN子查询、普通子查询操作并存的两表连接操作。
````mysql
select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in ( # 第一层的IN子查询
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in ( # 第二层嵌套的IN子查询
				select
					p_partkey
				from
					part
				where
					p_name like 'forest%' # COLOR为产生P_NAME的值的列表中的任意值
			)
			and ps_availqty > ( # 第二层嵌套的子查询
				select
					0.5 * sum(l_quantity) # 聚集子查询
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= '1994-01-01' # DATE为在1993年至1997年的任一年的一月一号
					and l_shipdate < date_add('1994-01-01', interval '1' year) # 1年内
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'CANADA' # TPC-H标准定义的任意值
order by
	s_name;
````
[返回目录](#目录)

#### Q21不能按时交货供货商查询
* Q21语句查询获得不能及时交货的供货商。
* Q21语句的特点是：带有分组、排序、聚集、EXISTS子查询、NOT EXISTS子查询操作并存的四表连接操作。查询语句没有从语法上限制返回多少条元组，但是TPC-H标准规定，查询结果只返回前100行（通常依赖于应用程序实现）。
````mysql
select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists ( # EXISTS子查询
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (# NOT EXISTS子查询
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'SAUDI ARABIA' # TPC-H标准定义的任意值
group by
	s_name
order by
	numwait desc,
	s_name
limit 100;
````
[返回目录](#目录)

#### Q22全球销售机会查询
* Q22语句查询获得消费者可能购买的地理分布。本查询计算在指定的国家，比平均水平更持肯定态度但还没下七年订单的消费者数量。能反应出普通消费者的的态度，即购买意向。
* Q22语句的特点是：带有分组、排序、聚集、EXISTS子查询、NOT EXISTS子查询操作并存的四表连接操作。
````mysql
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	( # 第一层子查询
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				('13', '31', '23', '29', '30', '18', '17') # I1…I7是在TPC-H中定义国家代码的可能值中不重复的任意值
			and c_acctbal > ( # 第二层聚集子查询
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists ( # 第二层NOT EXISTS子查询
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
````
[返回目录](#目录)