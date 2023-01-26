insert into dds.dm_couriers 
(courier_id, courier_name)
	select  
	t2.courier_id,
	t2.courier_name
		from
			(select
			replace (cast ((js::json->'_id')as text),'"', '') as courier_id,
			replace (cast ((js::json->'name')as text),'"', '') as courier_name
			from
				(select 
					cast (object_value as json) as js 
				from stg.deliverysystem_couriers
				where load_dttm::Date = '{{ds}}') as t) t2
ON CONFLICT ON CONSTRAINT courier_id_inique do update 	set courier_name = excluded.courier_name
