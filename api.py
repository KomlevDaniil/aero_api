#!/usr/bin/env python3
import json
import psycopg2
import requests as rq

with psycopg2.connect(dbname='test', user='airflow', host='localhost', password='airflow') as conn:
    with conn.cursor() as cur:
            batch=10
            response_API = rq.get(f"https://random-data-api.com/api/cannabis/random_cannabis?size={batch}")
            data = str(response_API.text).replace("'", "''")
            data = json.loads(data)
            print(data)
            #cur.execute("""
            #            create TABLE  public.test 
            #            (id int, 
            #             uid text, 
            #             strain text, 
            #             cannabinoid_abbreviation text, 
            #             cannabinoid text, 
            #             terpene text, 
            #             medical_use text, 
            #             health_benefit text, 
            #             category text, 
            #             type text, 
            #             buzzword text, 
            #             brand text);
            #            """)
            query_sql = f""" insert into test
                select   id,
                         uid,
                         strain,
                         cannabinoid_abbreviation,
                         cannabinoid,
                         terpene,
                         medical_use,
                         health_benefit,
                         category,
                         type,
                         buzzword,
                         brand 
                from json_populate_recordset(NULL::test, %s) """
            cur.execute(query_sql, (json.dumps(data),))
