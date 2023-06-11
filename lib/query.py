del_query_1_key = """
    delete from {{param.table_name}} where {{param.key_col}} = '{{param.key_val}}'
"""

top_10_area = """
SELECT region_name, count(1) as daily_minute_occurence
FROM ft_taxi
where timestamp = ( select max(timestamp) from dt_taxi)
group by 1
order by 2 desc 
limit 10;
"""

bottom_10_area = """
with data_exist as (
SELECT region_name, count(1) as daily_minute_occurence
FROM ft_taxi
where timestamp = ( select max(timestamp) from dt_taxi)
group by 1
order by 2)

select coalesce(a.region_name, b.name) as region_name, coalesce(a.daily_minute_occurence,0)as daily_minute_occurence
from data_exist a
full join dt_sg_region b
	on a.region_name = b.name
order by 2,1
limit 10;
"""

hourly_taxi_avail = """
with grouped_data AS (
	select date(timestamp) as date, split_part(split_part(timestamp, 'T', 2),':',1)::int as hour ,  timestamp, region_name
	from ft_taxi
	where date(timestamp) = '{{date}}'
),
ranked_timestamp AS (
	select timestamp  
	from (select date, hour, timestamp, row_number() OVER(partition by date, hour order by timestamp) as rn
	from grouped_data) a
	where rn = 1
)

select date, hour, region_name, count(1) as count_taxi
from grouped_data 
inner join ranked_timestamp
	using(timestamp)
group by 1,2,3
order by 1,2,4 desc;"""

hourly_taxi_avail_top_10 = """
with grouped_data AS (
	select date(timestamp) as date, split_part(split_part(timestamp, 'T', 2),':',1)::int as hour ,  timestamp, region_name
	from ft_taxi
	where date(timestamp) = '{{date}}'
),
ranked_timestamp AS (
	select timestamp  
	from (select date, hour, timestamp, row_number() OVER(partition by date, hour order by timestamp) as rn
	from grouped_data) a
	where rn = 1
),

sectioned_data AS (
	select date, hour, region_name, count(1) as count_taxi
	from grouped_data 
	inner join ranked_timestamp
		using(timestamp)
	group by 1,2,3
)
select date, hour, a.region_name, count_taxi
from sectioned_data a
left join (
    select region_name, sum(count_taxi) as sum_count
    from sectioned_data
    group by 1
    order by 2 desc
    limit 10) b
on a.region_name = b.region_name
where b.region_name is not null
order by 1,2,4 desc;"""


no_taxi_region = """
with data_exist as (
SELECT distinct region_name
FROM ft_taxi
where timestamp = ( select max(timestamp) from dt_taxi))

select a.name
from dt_sg_region a
left outer join data_exist b
	on a.name = b.region_name
order by 1;
"""