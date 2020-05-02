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

def page_summaries():
    query = f"""
      SELECT DISTINCT ON (page_count, cr_id) cr_id, page_count, batch_id
      FROM cr_pdfs
      ORDER BY page_count DESC, cr_id
    """

    cursor.execute(query)
    results = list(cursor.fetchall())

    with open('./output/batch5.csv', 'w') as fh:
        w = csv.writer(fh)
        w.writerow(['crid', 'pages'])
        w.writerows(results)

def needs_structural_parsing():
    query = """
        SELECT 
          p.cr_id,
          p.filename,
          pp.page_num,
          pp.page_classification,
          fb.dropbox_path,
          p.doccloud_url
        FROM 
          cr_pdf_pages pp,
          cr_pdfs p,
          cr_foia_batch fb
        WHERE 
          p.batch_id = fb.id 
        AND 
          pp.page_classification in (
              'WEB Complaint Detail',
              'ARREST Report',
              'Incident Report',
              'Watch Commander/OCIC Review',
              'Summary Report Digest'
            )
        AND p.id = pp.pdf_id
        ORDER BY 
          pp.page_classification,
          p.cr_id,
          p.filename,
          pp.page_num
        """
    cursor.execute(query)
    results = cursor.fetchall()

    output_fp = './output/needs_structural_parsing.csv'
    with open(output_fp, 'w') as fh:
        w = csv.writer(fh)
        header = ['cr_id', 'filename', 'page_num', 'page_classification',
                  'dropbox_path', 'doccloud_url']
        w.writerow(header)
        w.writerows(results)

needs_structural_parsing()
 
