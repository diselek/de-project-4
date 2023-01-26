insert into dds.dm_timestamps 
(ts,year,month,day, time, date )
select 
cast ( tss as timestamp) as ts,
cast (substring (tss, 1,4 ) as int) as year,
extract ( month from cast ( tss as timestamp)) as month,
extract ( day from cast ( tss as timestamp)) as day,
cast ( tss as time) as time,
cast ( tss as date) as time
from
	(select 
	replace (cast (js::json->'order_ts' as text),'"','' ) as tss
	from
		(select 
			cast (object_value as json) as js	
		from stg.deliverysystem_deliveries
		where load_dttm::Date = '{{ds}}') as t) t2
ON CONFLICT ON CONSTRAINT dm_timestamps_uindex do update set year = excluded.year;

insert into dds.dm_timestamps 
(ts,year,month,day, time, date )
select 
cast ( tss as timestamp) as ts,
cast (substring (tss, 1,4 ) as int) as year,
extract ( month from cast ( tss as timestamp)) as month,
extract ( day from cast ( tss as timestamp)) as day,
cast ( tss as time) as time,
cast ( tss as date) as time
from
	(select 
	replace (cast (js::json->'delivery_ts' as text),'"','' ) as tss
	from
		(select 
			cast (object_value as json) as js	
		from stg.deliverysystem_deliveries
		where load_dttm::Date = '{{ds}}') as t) t2
ON CONFLICT ON CONSTRAINT dm_timestamps_uindex do update set year = excluded.year;
