#!/usr/bin/python3

import requests
import dropbox
import psycopg2
import shutil
import pickle
from zipfile import ZipFile
import os
import re

AUTH_URL = "https://accounts.muckrock.com/api/"
API_URL = "https://api.beta.documentcloud.org/api/"

from PyPDF2 import PdfFileReader

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

conn = pg_conn()
curs = conn.cursor()

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

        return response.content

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

class dropbox_handler:
    def __init__(self):
        self.auth_token = DBX_AUTH_TOKEN
        self.dbx = dropbox.Dropbox(self.auth_token)

    def list_files(self, dbx_path):
        print(f"Listing files from dropbox path, {dbx_path}")
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
        self.dbx.files_download_to_file('/tmp/{}'.format(name), download_file)

        return '/tmp/{}'.format(name)

def insert_pdf_data(batch_id, pdf_name, page_count, doccloud_id, doccloud_url):
    sqlstr = f"SELECT id FROM cr_pdfs WHERE filename = '{pdf_name}'"
    curs.execute(sqlstr)
    resp = curs.fetchone()
    if resp: #pdf id already exists
        print("PDF already inserted. Inserting old pdf..")
        return resp[0]

    sqlstr = """
      INSERT INTO cr_pdfs (batch_id, filename, page_count, doccloud_id, doccloud_url)
      VALUES (%s, %s, %s, %s, %s) returning id
    """

    curs.execute(sqlstr, (batch_id, pdf_name, page_count, doccloud_id, doccloud_url))
    pdf_id = curs.fetchall()[0]

    return pdf_id

def dbx_to_doccloud(dbx_dir, filename):
    dh_fp = dh.download_file(dbx_path=dbx_dir, name=filename)
    resp = dc.upload_file(fp, dh_fp, source=dbx_dir, project_id=PROJECT_ID)

    return resp

dbx_dirs =  [
          '/Green v. CPD FOIA Files/Green, C. 2019.09.03 Production',
          '/Green v. CPD FOIA Files/Green 2019.12.02 Production',
          '/Green v. CPD FOIA Files/Green 12.30.19 Production/Green 2019.12.30 Production',
          '/Green v. CPD FOIA Files/Green 1.31.20 Production',
          '/Green v. CPD FOIA Files/Green 2020.02.28 Production']

DBX_AUTH_TOKEN = ''
DOCUMENT_CLOUD_USERNAME = ''
DOCUMENT_CLOUD_PASSWORD = ''
PROJECT_ID = 200010

dh = dropbox_handler()
print("Connected to dropbox")

dc = DocumentCloud(DOCUMENT_CLOUD_USERNAME, DOCUMENT_CLOUD_PASSWORD)
print("Connected to documentcloud")

#documentcloud_docs = dc.list_documents()

with open('/home/matt/doccloud.pkl', 'rb') as fh:
    documentcloud_docs = pickle.load(fh)

dbx_dir_files = {}

for dbx_dir in dbx_dirs:
    dir_files = dh.list_files(dbx_dir)
    dir_files = [d for d in dir_files if d.endswith('pdf')]

    dbx_dir_files[dbx_dir] = dir_files

    sqlstr = f"SELECT id FROM cr_foia_batch WHERE dropbox_path = '{dbx_dir}'"

    curs.execute(sqlstr)
    resp = curs.fetchone()
    if resp: #batch id already exists
        print("Batch already exists. Using old batch id..")
        batch_id = resp[0]
    else:
        sqlstr = f"INSERT INTO cr_foia_batch (dropbox_path) VALUES ('{dbx_dir}') returning id"
        curs.execute(sqlstr, (dir_files))

        resp = curs.fetchone()

        batch_id = resp[0]

    for pdf_name in dir_files:
        print(f"Preparing {pdf_name}..")
        doccloud_data = None
        for doc in documentcloud_docs:
            if pdf_name == doc['title']:
                doccloud_data = doc
                break

        if not doccloud_data:
            doccloud_data = dbx_to_doccloud(dbx_dir, pdf_name)
            documentcloud_docs.append(doccloud_data)

        doccloud_url = 'https://beta.documentcloud.org/documents/{}-{}'
        doccloud_url = doccloud_url.format(doccloud_data['id'], doccloud_data['slug'])
        insert_pdf_data(
                batch_id,
                doccloud_data['title'], 
                doccloud_data['page_count'],
                doccloud_data['id'], 
                doccloud_url)

    conn.commit()
