#!/usr/bin/python3

import psycopg2
import csv
import re
from pprint import pprint

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

conn = pg_conn()
cursor = conn.cursor()

query = f"""
    SELECT  p.cr_id,
	    p.filename AS pdf_name,
	    count(sd.section_name),
            section_name,
	    p.batch_id
    
    FROM    cr_summary_data sd,
	    cr_pdfs p, 
	    cr_pdf_pages pp,
	    cr_foia_batch fb
	    
    WHERE   sd.page_id = pp.id
    AND     p.id = pp.pdf_id
    AND     sd.text IS NOT NULL
    AND     fb.id = p.batch_id
    AND (
      (section_name = 'Accused Members' AND column_name = 'Initial / Intake Allegation') 
       OR (section_name = 'Review Incident' AND 'col_name' = 'Remarks') 
       OR (section_name = 'Incident Finding / Overall Case Finding' and column_name = 'Finding') 
       OR (section_name = 'Current Allegations' and column_name = 'Allegation'))
    GROUP BY section_name, filename, cr_id, batch_id
    ORDER BY count desc, batch_id, cr_id, pdf_name, section_name
    LIMIT 200
"""

cursor.execute(query)
results = list(cursor.fetchall())

pprint(results)

with open('./output/narratives.csv', 'w') as fh:
    w = csv.writer(fh)
    #w.writerow(('cr_id', 'pdf_name', 'page_num', 'section_name', 
    #        'column_name', 'text', 'batch_id', 'dropbox_path', 
    #        'doccloud_url'))

    #w.writerows(results)
