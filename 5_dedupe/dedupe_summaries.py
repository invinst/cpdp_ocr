#!/usr/bin/python3

import psycopg2
import csv
import re
import pandas as pd
from pprint import pprint

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

conn = pg_conn()
cursor = conn.cursor()

query = f"""
SELECT 
  (round(similarity(sq1.text, sq2.text)::float * 100) / 100)::float sim_bracket
FROM cr_narratives sq1, cr_narratives sq2
WHERE sq1.cr_id = sq2.cr_id
AND sq1.section_name = sq2.section_name
AND sq1.column_name = sq2.column_name
AND sq1.page_id != sq2.page_id
GROUP BY sim_bracket
ORDER BY sim_bracket desc
"""
cursor.execute(query)
results = list(cursor.fetchall())

print("Narratives summary:")
print(pd.read_sql("select count(*), avg(length(text)), median(length(text)) from cr_narratives", conn))
print(pd.read_sql("select count(*), column_name, section_name from cr_narratives group by column_name, section_name", conn))


pprint("Brackets of similarities")
print(['count', 'avg_len', 'med_len', 'similarity_brack']) 
pprint(results[:15])

print("Deleting duplicates..")

cursor.execute("""delete from cr_summary_data where id in (
        SELECT sq2.summary_data_id
        FROM cr_narratives sq1, cr_narratives sq2
        WHERE sq1.cr_id = sq2.cr_id
        AND sq2.summary_data_id > sq1.summary_data_id
        AND (round(similarity(sq1.text, sq2.text)::float * 100) / 100)::float > .95) ;""")
conn.commit()
