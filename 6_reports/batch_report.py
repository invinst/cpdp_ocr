#!/usr/bin/python3

import psycopg2
import csv
import re

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

conn = pg_conn()
cursor = conn.cursor()

batch_id = 5
query = f"""
  SELECT DISTINCT ON (page_count, cr_id) cr_id, page_count
  FROM cr_pdfs
  WHERE batch_id = {batch_id}
  ORDER BY page_count DESC, cr_id
"""

cursor.execute(query)
results = list(cursor.fetchall())

with open('./output/batch5.csv', 'w') as fh:
    w = csv.writer(fh)
    w.writerow(['crid', 'pages'])
    w.writerows(results)
