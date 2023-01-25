DROP TABLE IF EXISTS de.stg.deliverysystem_deliveries; 

-- CREATE TABLE stg.deliverysystem_deliveries

CREATE TABLE stg.deliverysystem_deliveries (
	id serial NOT NULL,
	order_id varchar(50) NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar(50) NOT NULL,
	courier_id varchar(50) NOT NULL,
	address text NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate numeric(14, 2) NOT NULL DEFAULT 0,
	sum numeric(14, 2) NOT NULL DEFAULT 0,
	tip_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT deliverysystem_deliveries_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS de.stg.deliverysystem_couriers;

-- CREATE TABLE stg.deliverysystem_couriers

CREATE TABLE stg.deliverysystem_couriers ( 
	id serial4 NOT NULL,
	courier_id varchar(50) NOT NULL,
	name text NOT NULL,
	CONSTRAINT deliverysystem_couriers_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS de.dds.dim_couriers;

-- CREATE TABLE dds.dim_couriers

CREATE TABLE dds.dim_couriers ( 
	id serial4 NOT NULL,
	courier_id varchar(50) NOT NULL,
	name text NOT NULL,
	CONSTRAINT dim_couriers_pkey PRIMARY KEY (id)
);


-- DROP TABLE
DROP TABLE IF EXISTS de.dds.fct_courier_tips; 

-- CREATE TABLE dds.fct_courier_tips

CREATE TABLE dds.fct_courier_tips (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_courier_tips_pkey PRIMARY KEY (id),
	CONSTRAINT fct_courier_tips_courier_id_fkey foreign key (courier_id) references dds.dim_couriers
);
	
-- DROP TABLE
DROP TABLE IF EXISTS de.dds.fct_order_rates; 

-- CREATE TABLE dds.fct_order_rates

CREATE TABLE dds.fct_order_rates (
	id serial4 NOT NULL,
	order_id varchar(50) NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar(50) NOT NULL,
	address text NOT NULL,
	delivery_ts timestamp NOT NULL,
	courier_id int4 NOT NULL,
	rate numeric(14, 2) NOT NULL DEFAULT 0,
	sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_order_rates_pkey PRIMARY KEY (id),
	CONSTRAINT fct_order_rates_courier_id_fkey foreign key (courier_id) references dds.dim_couriers
);

DROP TABLE IF EXISTS de.cdm.dm_courier_ledger; 

-- CREATE TABLE de.cdm.shipping_agreement

CREATE TABLE de.cdm.dm_courier_ledger (
id serial NOT NULL,
courier_id varchar(50) NOT NULL,
courier_name varchar(100) NOT NULL, 
settlement_year int2 NOT NULL,
settlement_month int2 NOT NULL,
orders_count int4  NOT NULL,
orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
rate_avg numeric(14, 2) NOT NULL DEFAULT 0,
order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0,
courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
CONSTRAINT dm_courier_ledger_rate_avg_check CHECK ((rate_avg >= (0)::numeric)),
CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum >= (0)::numeric)),
CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum >= (0)::numeric)),
CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum >= (0)::numeric))
);
