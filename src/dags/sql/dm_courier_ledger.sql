delete from  cdm.dm_courier_ledger where settlement_month =  extract ( month from cast ('{{ds}}' as timestamp)) 
and settlement_year = cast (substring ('{{ds}}', 1,4 ) as int);

with t1 as
	(select
		count (order_id) as orders_count,
		sum (sum) as orders_total_sum,
		avg (rate) as rate_avg,
		sum(tip_sum) as courier_tips_sum,
		dm_timestamps.month as order_month,
		dm_timestamps.year as order_year,
		courier_id
		from 
			dds.fct_deliveries
		left join dds.dm_timestamps on dm_timestamps.id = fct_deliveries.order_ts
		group by courier_id, order_year, order_month),
	t2 as
	(select
		fct_deliveries.courier_id,
		t1.order_month,
		t1.order_year,
		sum (case
			when t1.rate_avg < 4 and sum * 0.05 > 100  then sum * 0.05
			when t1.rate_avg < 4 and sum * 0.05 < 100  then 100
			when t1.rate_avg >= 4 and t1.rate_avg < 4 and sum * 0.07 > 150  then sum * 0.07
			when t1.rate_avg >= 4 and t1.rate_avg < 4 and sum * 0.07 < 150  then 150
			when t1.rate_avg >= 4.5 and t1.rate_avg < 4.9 and sum * 0.08 > 175  then sum * 0.08
			when t1.rate_avg >= 4.5 and t1.rate_avg < 4.9 and sum * 0.08 < 175  then 175
			when t1.rate_avg > 4.9 and sum * 0.1 > 200  then sum * 0.1
			when t1.rate_avg > 4.9 and sum * 0.1 < 200  then 200
			else 0
		end) as courier_order_sum		
		from dds.fct_deliveries
		left join dds.dm_timestamps on dm_timestamps.id = fct_deliveries.order_ts 
		left join t1 on t1.courier_id = fct_deliveries.courier_id and dm_timestamps.month = t1.order_month and dm_timestamps.year = t1.order_year		
		group by fct_deliveries.courier_id,t1.order_month, t1.order_year
		)

insert into cdm.dm_courier_ledger
(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
(select 
	c.id as courier_id,
	c.courier_name,
	t1.order_year as settlement_year,
	t1.order_month as settlement_month,
	t1.orders_count,
	t1.orders_total_sum,
	t1.rate_avg,
	t1.orders_total_sum * 0.25 as order_processing_fee,
	t2.courier_order_sum,
	t1.courier_tips_sum,
	t2.courier_order_sum + t1.courier_tips_sum * 0.95 as courier_reward_sum
from dds.dm_couriers as c
left join t1 on t1.courier_id = c.id
left join t2 on t1.courier_id = t2.courier_id and t1.order_month = t2.order_month and t1.order_year = t2.order_year
where t1.order_month =  extract ( month from cast ('{{ds}}' as timestamp)) 
and t1.order_year  = cast (substring ('{{ds}}', 1,4 ) as int));
