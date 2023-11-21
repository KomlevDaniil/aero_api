from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from import_files.connect_to_pg import get_cursor_connection
from import_files.connect_to_pg import close_cursor_connection

import json
import psycopg2
import requests as rq

from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    dag_id='dag_api',
    start_date=datetime(2023,11,20),
    schedule_interval="0 */12 * * *",
    catchup=False
) as dag:


    dummy_start = DummyOperator(
        task_id='dummy_start',
        dag=dag
    )


    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_test",
        sql="sql/sql_create.sql",
    )


    @task
    def loader(**context):
        try:
            cur, conn = get_cursor_connection(connection_name='postgres_test')
            batch=10
            response_API = rq.get(f"https://random-data-api.com/api/cannabis/random_cannabis?size={batch}")
            data = str(response_API.text).replace("'", "''")
            data = json.loads(data)
            print(data)

            with open("sql/sql_query.sql") as sql_query:
                cur.execute(sql_query.read(), (json.dumps(data),))

        except Exception as e:
            print(str(e))
            print('Error in layer validation task')
            raise

        finally:
            close_cursor_connection(cur, conn)


    dummy_finish = DummyOperator(
        task_id='dummy_finish',
        dag=dag
    )


    #build tasks
    dummy_start >> create_table  >> loader() >> dummy_finish