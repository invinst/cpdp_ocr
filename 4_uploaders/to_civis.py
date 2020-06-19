#!/usr/bin/python3

import psycopg2
import civis
import pandas as pd

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

conn = pg_conn()
cursor = conn.cursor()

api_key = 'f85dc4629c71072325161d0bb275f31bf7e5f0c934628b218b5240e052add7be'
client = civis.APIClient(api_key=api_key)
db_id = [d['id'] for d in client.databases.list() if d['name'] == 'Invisible Institute'][0]


tables = ['cr_foia_batch', 'cr_batch_data', 'cr_ocr_text', 'cr_ocr_tokens', 'cr_pdf_pages', 'cr_pdfs', 'cr_summary_data']

for table_name in tables:
    df = pd.read_sql("SELECT * FROM {}".format(table_name), conn)
    if df.empty:
        continue 

    civis_table_name = 'cpdp.{}'.format(table_name)
    civis.io.dataframe_to_civis(df, db_id, civis_table_name, api_key)
