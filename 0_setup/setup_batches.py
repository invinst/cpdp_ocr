#!/usr/bin/python3

import requests
import dropbox
import psycopg2
import shutil
import pickle
from zipfile import ZipFile
import os
import re

from PyPDF2 import PdfFileReader

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

creds_fp = '{}/.invisible_inst_muckrock'.format(os.environ['HOME'])
class dropbox_handler:
    def __init__(self):
        creds_data = {}
        with open(creds_fp, 'r') as fh:
            for line in fh.readlines():
                key, val = line.strip().split(':')
                creds_data[key] = val

            self.auth_token = creds_data['DBX_AUTH_TOKEN']

        self.dbx = dropbox.Dropbox(self.auth_token)

    def list_files(self, dbx_path):
        print(f"Grabbing dropbox files from, {dbx_path}")
        files = []
        res = self.dbx.files_list_folder(dbx_path)
        if res.entries:
            files += [entry.name for entry in res.entries]

        while True:
            if not res.has_more:
                break

            res = self.dbx.files_list_folder_continue(res.cursor)
            files += [entry.name for entry in res.entries]

        return files

    def download_file(self, dbx_path, filename, out_dir='./output'):
        dbx_fp = f"{dbx_path}/{filename}"
        out_fp = f"{out_dir}/{filename}"

        if os.path.exists(out_fp):
            return out_fp

        print(f"Downloading {filename} from {dbx_path}")
        self.dbx.files_download_to_file(out_fp, dbx_fp)
        return out_fp

def insert_pdf_data(batch_id, pdf_name, pdf_fp):
    sqlstr = f"SELECT id FROM cr_pdfs WHERE filename = '{pdf_name}'"
    curs.execute(sqlstr)
    resp = curs.fetchone()
    if resp: #pdf id already exists
        print("PDF already inserted. Inserting old pdf..")
        #return resp[0]

    import PyPDF2
    pdf_h = PyPDF2.PdfFileReader(open(pdf_fp, 'rb'))

    sqlstr = """
      INSERT INTO cr_pdfs (batch_id, filename, page_count)
      VALUES (%s, %s, %s) returning id
    """

    curs.execute(sqlstr, (batch_id, pdf_name, pdf_h.getNumPages()))
    pdf_id = curs.fetchall()[0]

    return pdf_id

def get_batch_id(dbx_dir):
    sqlstr = f"SELECT id FROM cr_foia_batch WHERE dropbox_path = '{dbx_dir}'"

    curs.execute(sqlstr)
    resp = curs.fetchone()
    if resp: #batch id already exists
        print("Batch already exists. Using old batch id..")
        batch_id = resp[0]
    else:
        sqlstr = f"INSERT INTO cr_foia_batch (dropbox_path) VALUES ('{dbx_dir}') returning id"
        curs.execute(sqlstr, (dbx_dir))

        resp = curs.fetchone()
        batch_id = resp[0]

    return batch_id

def prepare_batches(fp='../batches.txt'):
    """Downloads batch files into ./output and updates db with pdf info"""

    with open('../batches.txt', 'r') as fh:
        dbx_dirs = list(map(str.strip, fh.readlines()))

    dbx_h = dropbox_handler()
    for dbx_dir in dbx_dirs:
        batch_id = get_batch_id(dbx_dir)
        dir_files = dbx_h.list_files(dbx_dir)
        dir_files = [d for d in dir_files if d.endswith('pdf')]

        for pdf_name in dir_files:
            pdf_fp = dbx_h.download_file(dbx_path=dbx_dir, filename=pdf_name)
            insert_pdf_data(batch_id, pdf_name, pdf_fp)

        conn.commit()

conn = pg_conn() #global
curs = conn.cursor() #global
                  
prepare_batches()
