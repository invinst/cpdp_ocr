#!/usr/bin/python3

import json
import tempfile
import psycopg2

from os import listdir
from doccano_api_client import DoccanoClient

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

unannotated_dir = '/home/matt/git/cpdp_parsers/3_parsers/output/'
filenames = listdir(unannotated_dir)

def insert_summary_data(fp='./input/summary_tables.json'):
    conn = pg_conn()
    curs = conn.cursor() 

    curs.execute("""CREATE TEMP TABLE cr_summary_data_temp (text TEXT, column_name TEXT, section_name TEXT, pdf_name TEXT, page_num INT)""")

    with open(fp, 'r') as fh:
        summary_data = json.load(fh)

    for pdf_page in summary_data:
        pdf_name = pdf_page['pdf_name']

        page_data = []
        text = []

        sections = pdf_page['sections']
        for section in sections:
            section_cols = section['columns']
            section_name = section['section_name']
            for column in section['columns']:
                col_name = column['col_name']
                col_text = column['col_text']

                sqlstr = """
                    INSERT INTO cr_summary_data_temp
                      (text, column_name, section_name, pdf_name, page_num)
                      VALUES (%s, %s, %s, %s, %s)
                    """
                vals = (col_text, col_name, section_name, pdf_name, pdf_page['page_num'])
                curs.execute(sqlstr, vals)
        conn.commit()

    sqlstr = """
        INSERT INTO cr_summary_data
          (text, column_name, section_name, pdf_id, page_id)
        SELECT sdt.text, sdt.column_name, sdt.section_name, p.id, pp.id
        FROM cr_pdfs p, cr_pdf_pages pp, cr_summary_data_temp sdt
        WHERE pp.pdf_id = p.id
        AND p.filename = sdt.pdf_name
        AND pp.page_num = sdt.page_num
      """
    curs.execute(sqlstr)
    conn.commit()
            
insert_summary_data()
