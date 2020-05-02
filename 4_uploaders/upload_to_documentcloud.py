#!/usr/bin/python3

import requests

AUTH_URL = "https://accounts.muckrock.com/api/"
API_URL = "https://api.beta.documentcloud.org/api/"

import civis

import csv
import dropbox
import io
import numpy as np
import os
import pandas as pd
import psycopg2
import re
import sys
import tempfile

from multiprocessing import Pool

creds_fp = '{}/.invisible_inst_muckrock'.format(os.environ['HOME'])
with open(creds_fp, 'r') as fh:
    creds_data = {}
    for line in fh.readlines():
        key, val = line.strip().split()
        creds_data[key] = val

    DBX_AUTH_TOKEN = creds_data['DBX_AUTH_TOKEN']
    DOCUMENT_CLOUD_USERNAME = creds_data['DOCUMENT_CLOUD_USERNAME']
    DOCUMENT_CLOUD_PASSWORD = creds_data['DOCUMENT_CLOUD_PASSWORD']
    PROJECT_ID = creds_data['PROJECT_ID']

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

class dropbox_handler:
    def __init__(self):
        self.auth_token = DBX_AUTH_TOKEN
        self.dbx = dropbox.Dropbox(self.auth_token)

    def list_files(self, dbx_path):
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
    
    def upload_directory(self,local_path,dbx_path):
        ## local file list 
        filenames = [filename for filename in os.listdir(local_path) if filename[0]!='.']
        self.dbx.files
        for filename in filenames:
            f = open(local_path+filename, 'rb')
            self.dbx.put_file(dbx_path+filename,'f')
    
    def download_file(self,
                      dbx_path,
                      name,
                      return_sheets=False,
                      sheetname=None,
                      skip=None,
                      rows=None):

        name = name.lower()
        download_file = "{}/{}".format(dbx_path, name)
        print(download_file)

        local_path = f'/opt/data/green/{name}'
        self.dbx.files_download_to_file(local_path, download_file)

        return local_path

class DocumentCloud:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.access = None
        self.refresh = None
        self.get_tokens()

    def get_tokens(self):
        response = requests.post(
            f"{AUTH_URL}token/",
            json={"username": self.username, "password": self.password},
        )

        if response.status_code != requests.codes.ok:
            print("Incorrect password")
            sys.exit(-1)

        json = response.json()
        self.access = json["access"]
        self.refresh = json["refresh"]

    def refresh_tokens(self):
        response = requests.post(
            f"{AUTH_URL}refresh/",
            json={"username": self.username, "password": self.password},
        )

        if response.status_code != requests.codes.ok:
            self.get_tokens()
        else:
            json = response.json()
            self.access = json["access"]
            self.refresh = json["refresh"]

    def headers(self):
        return {"Authorization": f"Bearer {self.access}"}

    def _request(self, method, *args, **kwargs):
        kwargs["headers"] = self.headers()
        response = getattr(requests, method)(*args, **kwargs)
        if response.status_code == requests.codes.forbidden:
            self.refresh_tokens()
            return self._request(method, *args, **kwargs)

        return response

    def get(self, *args, **kwargs):
        return self._request("get", *args, **kwargs)

    def post(self, *args, **kwargs):
        return self._request("post", *args, **kwargs)

    def put(self, *args, **kwargs):
        return self._request("put", *args, **kwargs)

    def patch(self, *args, **kwargs):
        return self._request("patch", *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._request("delete", *args, **kwargs)

    def options(self, *args, **kwargs):
        return self._request("options", *args, **kwargs)

    def head(self, *args, **kwargs):
        return self._request("head", *args, **kwargs)

    def list_documents(self, user_id=100011, project_id=200010):
        documents = []
        for page_num in range(1, 1000000):
            print(f"Grabbing DocumentCloud file list, page {page_num}")
            resp = self.get("{}documents?page={}".format(API_URL, page_num)).json()
            if 'results' not in resp: 
                break
            results = resp['results']
            documents += [r for r in results if r['user'] == user_id]

        return documents

    def upload_file(self, title, fp, source, project_id=200010):
        file_ = open(fp, 'rb')
        response = self.post(f"{API_URL}documents/", json={"title": title, 'source': source, 'project': 200010})
        print(response.status_code)
        print(response.json())
        url = response.json()["presigned_url"]
        id = response.json()["id"]
        response = requests.put(url, data=file_.read())
        print(response.status_code)
        print(response.content)
        self.change_project(id, project_id)
        response = self.post(f"{API_URL}documents/{id}/process/")
        print(response.status_code)
        print(response.content)

    def upload_url(self, title, url):
        response = self.post(
            f"{API_URL}documents/", json={"title": title, "file_url": url}
        )
        print(response.status_code)
        print(response.content)
        
    def change_project(self, document_id, project_id):
        response = dc.post(f"{API_URL}projects/{project_id}/documents/", json={'document': document_id})
        if response.json() and not response.json()['edit_access']:
            print(f'Failed to move {document_id} to project, {project_id}')
        return response

def do_delete(doc_id):
    dc.delete(f"{API_URL}documents/{doc_id}")

def pending_uploads(dirs):
    print("Grabbing existing documentcloud files")
    doccloud_files = [r['title'].lower() for r in dc.list_documents()]
    print("Existing documentcloud file #: ", len(doccloud_files))
    dbx_files = []

    pending = []
    for dbx_dir in dirs:
        print(f"Checking # of pending files for {dbx_dir}")
        dir_files = dh.list_files(dbx_dir)

        pending_before = len(pending)
        for dir_file in dir_files:
            dbx_files.append((dbx_dir, dir_file))
            if os.path.basename(dir_file.lower()) not in doccloud_files:
                pending.append((dbx_dir, dir_file))
        print(f"Pending files for {dbx_dir}: ", len(pending) - pending_before)

    return pending

def delete_all():
    pool = Pool(processes=12)
    docs = dc.list_documents()
    to_delete = []
    for doc in docs:
        doc_id = doc['id']
        if doc['title'].startswith('CPD') or doc['title'].startswith('LOG'):
            to_delete.append(doc['id'])
    pool.map(do_delete, to_delete)

def dbx_to_doccloud(dbx_dir, fp, beta=False):
    dh_fp = dh.download_file(dbx_path=dbx_dir, name=fp)

    if not beta:
        curs.execute(f"SELECT cr_id from cr_pdfs where filename = '{fp}'")

        results = curs.fetchone()
        if results:
            cr_id = results[0]
            dc.documents.upload(dh_fp, access='public', title=f'CRID {cr_id}')

        else:
            print(fp, dh_fp)
    #dc.upload_file(fp, dh_fp, source=dbx_dir, project_id=PROJECT_ID)

def delete_duplicates():
    documents = dc.list_documents()
    existing = []

    for doc in documents:
        if doc['title'] in existing:
            do_delete(doc['id'])
        else:
            existing.append(doc['title'])

conn = pg_conn()
curs = conn.cursor()
def update_pdf_table(doc):
    doc_id = doc['id']
    doc_slug = doc['slug']

    document_url = f'https://beta.documentcloud.org/documents/{doc_id}-{doc_slug}'
    sqlstr = """
      UPDATE cr_pdfs set document_cloud_url = %s
      WHERE filename = %s
    """
    curs.execute(sqlstr, (document_url, doc['title']))
    conn.commit()


dirs =  [
  '/Green v. CPD FOIA Files/Raw documents from Green v CPD/Green 2019.12.30 Production',
  '/Green v. CPD FOIA Files/Raw documents from Green v CPD/Green 2019.12.02 Production',
  '/Green v. CPD FOIA Files/Raw documents from Green v CPD/Green 1.31.20 Production',
  '/Green v. CPD FOIA Files/Raw documents from Green v CPD/Green 2020.02.28 Production',
  '/Green v. CPD FOIA Files/Raw documents from Green v CPD/Green, C. 2019.09.03 Production',
  '/Green v. CPD FOIA Files/Raw documents from Green v CPD/Green 12.30.19 Production/Green 2019.12.30 Production']

dh = dropbox_handler()
print("Connected to dropbox")
#dc = DocumentCloud(DOCUMENT_CLOUD_USERNAME, DOCUMENT_CLOUD_PASSWORD)

pending = []
for dbx_dir in dirs:
    print(f"Checking # of pending files for {dbx_dir}")
    dir_files = dh.list_files(dbx_dir)

    pending_before = len(pending)
    for dir_file in dir_files:
        pending.append((dbx_dir, dir_file))

from documentcloud import DocumentCloud
dc = DocumentCloud(DOCUMENT_CLOUD_USERNAME, DOCUMENT_CLOUD_PASSWORD)

print("Connected to documentcloud")

#pending = pending_uploads(dirs)

#print("Pending # of files to upload: ", len(pending))
for_upload = [dbx_to_doccloud(*p) for p in pending]

#pool = Pool(processes=32)
#pool.starmap(dbx_to_doccloud, pending)
