import clickhouse_driver 
from airflow.models.variable import Variable
from airflow.models.param import Param
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook



def get_cursor_connection(connection_name='postgres_test'):

    hook = PostgresHook(postgres_conn_id='postgres_test')
    conn = hook.get_conn()
    conn.autocommit = True  
    print('Got connection:', conn)
    return conn.cursor(), conn


def close_cursor_connection(cur, conn):

    if cur:
        print('Cursor to close:', cur)
        cur.close()
    if conn:
        print('Connection to close:', conn) 
        conn.close()
    return 0
