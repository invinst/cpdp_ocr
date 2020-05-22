#!/usr/bin/python3

import psycopg2
import csv

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

sqlstr = """
  SELECT 
    p.cr_id,
    p.filename,
    pp.page_num,
    split_part(fb.dropbox_path, '/', 4),
    p.doccloud_url,
    ot.ocr_text 
  FROM 
    cr_ocr_text ot,
    cr_pdfs p, 
    cr_pdf_pages pp,
    cr_foia_batch fb
  WHERE pp.pdf_id = p.id 
  AND ot.page_id = pp.id
  AND fb.id = p.batch_id
  """

conn = pg_conn()
curs = conn.cursor()

curs.execute(sqlstr)

with open('./output/ocr_text.csv', 'w') as fh:
    w = csv.writer(fh)
    w.writerow(['cr_id', 'filename', 'page_num', 'batch_name', 'doccloud_url', 'ocr_text'])
    w.writerows(curs.fetchall())
