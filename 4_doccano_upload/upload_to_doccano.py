#!/usr/bin/python3

from os import listdir
from doccano_api_client import DoccanoClient

# instantiate a client and log in to a Doccano instance
doccano_client = DoccanoClient(
    'http://127.0.0.1'
    'admin',
    'password',
)

def project_by_name(project_name):
    project_list = doccano_client.get_project_list().json()
    project_id = [i for i in project_list if i['name'] == project_name]

    if project_id:
        return project_id[0]

project_name = 'cpdp' 
project_dets = project_by_name(project_name)

if not project_dets:
    print('cannot find project.')
    exit(1)

if project_dets['resourcetype'] != 'SequenceLabelingProject':
    print('Project type must be created as "Sequence Labeling". Recreate project.')
    exit(1)

unannotated_dir = '/home/matt/git/cpdp_parsers/3_parsers/output/'
filenames = listdir(unannotated_dir)


for fn in filenames:
    print(fn)
    upload_resp = doccano_client.post_doc_upload(
            project_dets['id'],
            'json', 
            fn, 
            unannotated_dir)

    print(upload_resp.text)
