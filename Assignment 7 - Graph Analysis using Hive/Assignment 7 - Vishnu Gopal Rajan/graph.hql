drop table graph;

create table graph(
	node int,
	link int )
row format delimited fields terminated by ',' stored as textfile;

load data local inpath  '${hiveconf:G}' overwrite into table graph;

select cnt, count(*) as n
from (select count(*) as cnt from graph group by node) as cnt1
group by cnt
order by n DESC;