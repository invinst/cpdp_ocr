#!/usr/bin/python3

import csv
import psycopg2
import re
from openpyxl import Workbook

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

conn = pg_conn()
curs = conn.cursor()

def run_sql(sqlstr):
    curs.execute(sqlstr)
    return curs.fetchall()

pdfs_per_batch = run_sql("""
select count(*), dropbox_path 
  from cr_batch fb, cr_pdfs p 
  where p.batch_id = fb.id
  group by dropbox_path, fb.id 
  UNION SELECT count(*), 'all'
  from cr_pdfs order by count ;""")


crid_pdfs_per_batch = run_sql("""
  SELECT dropbox_path, MEDIAN(count), MIN(count), MAX(count), AVG(count), SUM(count) 
  FROM (
    SELECT dropbox_path, COUNT(cr_id) 
    FROM cr_pdfs p, cr_batch fb 
    WHERE fb.id = p.batch_id 
    GROUP BY cr_id, dropbox_path) sq 
  GROUP BY dropbox_path
  UNION
  SELECT 'combined', MEDIAN(count), MIN(count), MAX(count), AVG(count), SUM(count) 
  FROM (SELECT COUNT(cr_id) FROM cr_pdfs GROUP BY cr_id) sq 
  ORDER BY sum;
""")

pages_per_batch = run_sql("""
  SELECT
    dropbox_path, MEDIAN(page_count), MIN(page_count), 
    MAX(page_count), AVG(page_count), SUM(page_count) 
  FROM cr_pdfs p, cr_batch fb 
  WHERE fb.id = p.batch_id 
  GROUP BY dropbox_path
  UNION
  SELECT 'combined', MEDIAN(page_count), MIN(page_count), 
    MAX(page_count), AVG(page_count), SUM(page_count) 
  FROM cr_pdfs p
  ORDER BY dropbox_path""")

pdf_count_of_count_all = run_sql("""
  select count, count(count) AS count_of_count 
  from (
    select count(cr_id) 
    from cr_pdfs 
    group by cr_id) sq 
  group by count 
  order by count_of_count desc, count asc ;""")

pdf_count_of_count_batches = run_sql("""
  select count, count(count) as count_of_count, fb.dropbox_path 
  from (
    select count(cr_id), batch_id 
    from cr_pdfs 
    group by cr_id, batch_id) sq,
    cr_batch fb 
  WHERE sq.batch_id = fb.id
  GROUP BY sq.count, fb.dropbox_path 
  ORDER BY dropbox_path, count_of_count desc, sq.count asc
  """)

def add_sheet(workbook, title, lines, header):
    ws = wb.create_sheet(title)
    ws.append(header)
    for line in lines:
        ws.append(line)

from openpyxl import Workbook

wb = Workbook()
wb.remove_sheet(wb.active)

add_sheet(wb, 'PDFs per batch', pdfs_per_batch, ['count', 'dropbox_path'])
add_sheet(wb, 'PDFs per CRID per batch', crid_pdfs_per_batch, ['dropbox_path', 'median', 'min', 'max', 'avg', 'sum'])
add_sheet(wb, 'Pages per batch', pages_per_batch, ['dropbox_path', 'median', 'min', 'max', 'avg', 'sum'])
add_sheet(wb, 'PDF # histo', pdf_count_of_count_all, ['count', 'Count of Count'])
add_sheet(wb, 'PDF # histo per batch', pdf_count_of_count_batches, ['count', 'Count of Count', 'dropbox_path'])

wb.save('batch_report.xlsx')
