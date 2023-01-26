drop table if exists cdm.dm_courier_ledger cascade;
create table cdm.dm_courier_ledger
(
id serial4 primary key,
courier_id int not null,
courier_name text not null,
settlement_year int,
settlement_month int,
orders_count int,
orders_total_sum numeric (14,2),
rate_avg numeric (14,2),
order_processing_fee numeric (14,2),
courier_order_sum  numeric (14,2),
courier_tips_sum numeric (14,2),
courier_reward_sum numeric (14,2),
CONSTRAINT dm_courier_ledger_inique unique (courier_id,settlement_year,settlement_month)
);


--dds
drop table if exists dds.dm_couriers cascade;
create table dds.dm_couriers
(
id serial4 primary key,
courier_id text not null,
courier_name text not null,
CONSTRAINT courier_id_inique unique (courier_id)
);

drop table if exists dds.fct_deliveries cascade;
create table dds.fct_deliveries
(
id serial4 primary key,
order_id int ,
order_key text not null,
order_ts int not null,
delivery_key text not null,
courier_id int not null,
address text not null,
delivery_ts int not null, 
rate int not null, 
sum numeric (14,2) not null, 
tip_sum numeric (14,2) not null,
constraint fct_deliveries_delivery_id_unique unique (delivery_key)
);

--references dds.fct_deliveries
alter table dds.fct_deliveries add constraint fct_deliveries_order_id_fkey foreign key (order_id) references dds.dm_orders (id);
alter table dds.fct_deliveries add constraint fct_deliveries_order_ts_fkey foreign key (order_ts) references dds.dm_timestamps (id);
alter table dds.fct_deliveries add constraint fct_deliveries_courier_id_fkey foreign key (courier_id) references dds.dm_couriers (id);

-- stg
drop table if exists stg.deliverysystem_couriers cascade;
create table stg.deliverysystem_couriers
(id serial4 primary key,
courier_id text not null,
load_dttm timestamp,
object_value text not null,
CONSTRAINT courier_id_inique unique (courier_id)
);

drop table if exists stg.deliverysystem_deliveries cascade;
create table stg.deliverysystem_deliveries
(id serial4 primary key,
delivery_id text not null,
load_dttm timestamp,
delivery_ts timestamp,
object_value text not null,
CONSTRAINT delivery_id_inique unique (delivery_id)
);
