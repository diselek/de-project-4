insert into dds.fct_deliveries  
(order_id, order_key, order_ts ,delivery_key, courier_id, address, delivery_ts, rate, sum, tip_sum )
	select  
	dm_orders.id as order_id,
	t2.order_key as order_key,
	ts1.id as order_ts,
	t2.delivery_key as delivery_key,
	dm_couriers.id as courier_id, 
	t2.address as address,
	ts2.id as delivery_ts,
	t2.rate as rate, 
	t2.sum as sum,
	t2.tip_sum as tip_sum
	
	
		from
			(select
			replace (cast ((js::json->'order_id')as text),'"', '') as order_key,
			cast (replace (cast ((js::json->'order_ts')as text),'"', '')as timestamp) as order_ts,
			replace (cast ((js::json->'delivery_id')as text),'"', '') as delivery_key,
			replace (cast ((js::json->'courier_id')as text),'"', '') as courier_id,
			replace (cast ((js::json->'address')as text),'"', '') as address,
			cast (replace (cast ((js::json->'delivery_ts')as text),'"', '')as timestamp) as delivery_ts,
			cast (replace (cast ((js::json->'rate')as text),'"', '')as int) as rate,
			cast (replace (cast ((js::json->'sum')as text),'"', '')as numeric(14,2)) as sum,
			cast (replace (cast ((js::json->'tip_sum')as text),'"', '')as numeric(14,2)) as tip_sum
			from
				(select 
					cast (object_value as json) as js 
				from stg.deliverysystem_deliveries
				where load_dttm::Date = '{{ds}}') as t) t2
		left join dds.dm_orders on t2.order_key::text = dm_orders.order_key::text
		left join dds.dm_timestamps as ts1 on t2.order_ts = ts1.ts
		left join dds.dm_timestamps as ts2 on t2.delivery_ts = ts2.ts
		left join dds.dm_couriers on dm_couriers.courier_id = t2.courier_id
		
ON CONFLICT ON CONSTRAINT fct_deliveries_delivery_id_unique do update set courier_id = excluded.courier_id
				;
