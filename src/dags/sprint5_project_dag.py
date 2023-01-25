from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
import psycopg2, psycopg2.extras
import pandas as pd
import json
import datetime
import requests

pg_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')

conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()

url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
nickname = "brok01"
cohort = "8"

headers = {
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
    "X-Nickname": nickname,
    "X-Cohort": str(cohort)
}

def load_raw_data_deliveries(**kwargs):

    sort_field = '_id'
    sort_direction = 'asc'
    limit = 50
    offset = 0
    id = 1 
    method_url = '/deliveries'
    filter = f'sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
    r = requests.get( url + method_url + filter, headers=headers)
    response = json.loads(r.content)

    # df = pd.DataFrame(response_dict)
    # df.to_csv("./raw_deliveries.csv" )

    # получаем название колонок
    columns = ','.join([i for i in response[0]])
    # SQL запрос на вставку
    sql = f"INSERT INTO {pg_schema}.{pg_table_source} (columns) VALUES %s"
    # делаем список списков из значений словаря. response - результат get-запроса в JSON формате
    values = [[value for value in response[i].values()] for i in range(len(response))]
    execute_values(cursor, sql, values) 

def load_raw_data_couriers(**kwargs): 
    method_url = '/couriers'
    r = requests.get( url + method_url, headers=headers)
    response_dict = json.loads(r.content)

    df = pd.DataFrame(response_dict)
    dfr = df.rename(columns={'_id': 'courier_id'})
    dfr.to_csv("./raw_couriers.csv" )
    
def load_file_to_pg(filename, pg_table, conn_args):
    df = pd.read_csv(f"./{filename}" ).iloc[: , 1:]

    cols = ','.join(list(df.columns))
    insert_stmt = f"INSERT INTO stg.{pg_table} ({cols}) VALUES %s"
    
    pg_conn = conn_args
    cur = pg_conn.cursor()

    cur.execute(f"TRUNCATE stg.{pg_table} RESTART IDENTITY CASCADE;")

    psycopg2.extras.execute_values(cur, insert_stmt, df.values)
    pg_conn.commit()

    cur.close()
    pg_conn.close()

def update_dim_couriers(ti):

    pg_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
    cur = conn.cursor()
        
    cur.execute("TRUNCATE dds.dim_couriers RESTART IDENTITY CASCADE;")
    cur.execute("""
                insert into dds.dim_couriers (courier_id, name)
				select 
					dc.courier_id,
					dc."name" 
				from stg.deliverysystem_couriers dc;
            """)
    conn.commit()

def update_fct_courier_tips(ti):

    pg_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
    cur = conn.cursor()
        
    cur.execute("TRUNCATE dds.fct_courier_tips RESTART IDENTITY CASCADE;")
    cur.execute("""
                insert into dds.fct_courier_tips (courier_id, courier_tips_sum)
                select 
                    dc.id as courier_id,
                    sum(dd.tip_sum) as courier_tips_sum
                from stg.deliverysystem_deliveries dd
                left join dds.dim_couriers dc ON dd.courier_id = dc.courier_id
                where dc.id is not null
                group by dc.id
                order by dc.id;
            """)
    conn.commit()

def update_fct_order_rates(ti):

    pg_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
    cur = conn.cursor()
    
    cur.execute("TRUNCATE dds.fct_order_rates RESTART IDENTITY CASCADE;")
    cur.execute("""
                insert into dds.fct_order_rates (order_id, order_ts, delivery_id, address, delivery_ts, courier_id, rate, sum)
                select 
                    order_id,
                    order_ts,
                    delivery_id,
                    address,
                    delivery_ts,
                    dc.id as courier_id,
                    rate,
                    sum
                from stg.deliverysystem_deliveries dd
                left join dds.dim_couriers dc ON dd.courier_id = dc.courier_id
                where dc.id is not null;
            """)
    conn.commit()

def update_dm_courier_ledger(ti):

    pg_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    conn = psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")
    cur = conn.cursor()

    cur.execute("TRUNCATE cdm.dm_courier_ledger RESTART IDENTITY CASCADE;")
    cur.execute("""
                INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, 
                settlement_year, settlement_month, orders_count, 
                orders_total_sum, rate_avg, order_processing_fee, 
                courier_order_sum, courier_tips_sum, courier_reward_sum)
                WITH order_sums AS (
                    SELECT
                        dc.courier_id,
                        dc."name" as courier_name,
                        extract('YEAR' from fct.order_ts) as settlement_year,
                        extract('MONTH' from fct.order_ts) as settlement_month,
                        count(fct.order_id)  as orders_count,
                        sum(fct.sum) as orders_total_sum,
                        avg(fct.rate) as rate_avg,
                        case 
                            when avg(fct.rate) < 4 then sum(fct.sum) * 0.05
                            when avg(fct.rate) <= 4 or avg(fct.rate) < 4.5 then sum(fct.sum) * 0.07
                            when avg(fct.rate) <= 4.5 or avg(fct.rate) < 4.9 then sum(fct.sum) * 0.08
                            when avg(fct.rate) >= 4.9 then sum(fct.sum) * 0.1
                        end as courier_order_sum,
                        sum(fct2.courier_tips_sum) as courier_tips_sum
                    FROM dds.fct_order_rates fct
                    left join dds.dim_couriers dc ON fct.courier_id = dc.id
                    left join dds.fct_courier_tips fct2 ON fct.courier_id = fct2.courier_id
                    group by 1, 2, 3, 4
                )
                select
                    os.courier_id,
                    os.courier_name,
                    os.settlement_year,
                    os.settlement_month,
                    os.orders_count,
                    os.orders_total_sum,
                    os.rate_avg,
                    os.orders_total_sum * 0.25 as order_processing_fee,
                    case 
                        when os.rate_avg < 4 and os.courier_order_sum < 100 then 100
                        when (os.rate_avg <= 4 or os.rate_avg < 4.5) and os.courier_order_sum < 150 then 150
                        when (os.rate_avg <= 4.5 or os.rate_avg < 4.9) and os.courier_order_sum < 175 then 175
                        when os.rate_avg >= 4.9 and os.courier_order_sum < 200 then 200 
                        else os.courier_order_sum
                    end as courier_order_sum,
                    os.courier_tips_sum,
                    courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
                from order_sums os;
            """)
    conn.commit()
    
with DAG(
    dag_id='sprint5_project_courier_ledger',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2023, 1, 25),
    catchup=False,
    tags=['sprint5', 'project', 'brok01'],
    params={"example_key": "example_value"},
) as dag:
   
    with TaskGroup(group_id="preprocessing_export") as export_raw_data:
    
        export_raw_deliveries = PythonOperator(  
            task_id='export_raw_deliveries',
            python_callable=load_raw_data_deliveries,
            op_kwargs={"X-Nickname": nickname, "X-Cohort": cohort},
            dag=dag 
        )  

        export_raw_couriers = PythonOperator(  
            task_id='export_raw_couriers',
            python_callable=load_raw_data_couriers,
            op_kwargs={"X-Nickname": nickname, "X-Cohort": cohort},
            dag=dag 
        )  

    export_raw_deliveries >> export_raw_couriers

    with TaskGroup(group_id="preprocessing_stage") as load_to_stg:
        load_deliveries_to_stg = PythonOperator(task_id='load_deliveries_to_stg',
                                            python_callable=load_file_to_pg,
                                            op_kwargs={'filename': 'raw_deliveries.csv', 
                                                        'pg_table': 'deliverysystem_deliveries', 
                                                        'conn_args': psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")},
                                            dag=dag)

        load_couriersto_stg = PythonOperator(task_id='load_couriersto_stg',
                                            python_callable=load_file_to_pg,
                                            op_kwargs={'filename': 'raw_couriers.csv', 
                                                        'pg_table': 'deliverysystem_couriers', 
                                                        'conn_args': psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'")},
                                            dag=dag)  
    load_deliveries_to_stg >> load_couriersto_stg

    with TaskGroup(group_id="preprocessing_dds") as load_to_dds:
        update_dim_couriers = PythonOperator(task_id='update_dim_couriers',
                                            python_callable=update_dim_couriers,
                                            dag=dag)
        
        update_fct_courier_tips = PythonOperator(task_id='update_fct_courier_tips',
                                            python_callable=update_fct_courier_tips,
                                            dag=dag)

        update_fct_order_rates = PythonOperator(task_id='update_fct_order_rates',
                                            python_callable=update_fct_order_rates,
                                            dag=dag)
    
    update_dim_couriers >> update_fct_courier_tips >> update_fct_order_rates

    update_dm_courier_ledger = PythonOperator(task_id='update_dm_courier_ledger',
                                            python_callable=update_dm_courier_ledger,
                                            dag=dag)

export_raw_data >> load_to_stg >> load_to_dds >> update_dm_courier_ledger
