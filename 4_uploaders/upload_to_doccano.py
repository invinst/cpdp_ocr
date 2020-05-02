#!/usr/bin/python3

import json
import tempfile

from os import listdir
from doccano_api_client import DoccanoClient

# instantiate a client and log in to a Doccano instance
doccano_client = DoccanoClient(
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

def extract_summaries(project_id, fp='./input/summary_tables.json'):
    with open(fp, 'r') as fh:
        summary_data = json.load(fh)

    #oi wtf
    summary_data = [s for s in summary_data if s]

    for pdf_page in summary_data:
        pdf_num = str(pdf_page['pdf_num'])
        zeroes = ''.join(['0' for i in range(7 - len(pdf_num))])

        pdf_name = 'CPD {}{}.pdf'.format(zeroes, pdf_num)
        metadata = {
                'page_num': pdf_page['page_num'],
                'pdf_name': pdf_name,
                'cr_id': pdf_page['cr_id'],
            }

        page_data = []
        text = []

        ignores = ['(None Entered)']

        sections = pdf_page['sections']
        for section in sections:
            section_cols = section['columns']
            for to_extract in to_extract_map:
                section_to_extract = to_extract['section_name']
                col_to_extract = to_extract['col_name']
                if section['section_name'] == section_to_extract:
                    print('section_name', section_to_extract)
                    for col in section_cols:
                        col_text = col['col_text']
                        col_name = col['col_name']

                        if col_text in ignores:
                            continue

                        if col_name == col_to_extract and col_text:
                            print('col_name ', col_name)
                            annotate_dict = {'meta': metadata, 'text': col_text}
                            annotate_dict['meta']['Section'] = section_to_extract
                            annotate_dict['meta']['Column'] = col_to_extract

                            print(annotate_dict)
                            yield annotate_dict

to_extract_map = [
          {'section_name': 'Accused Members', 'col_name': "Initial / Intake Allegation"},
          {'section_name': 'Incident Finding / Overall Case Finding', 'col_name': 'Finding'},
          {'section_name': 'Current Allegations', 'col_name': 'Allegation'},
          {'section_name': 'Review Incident', 'col_name': 'Remarks'}
        ]

project_id = project_dets['id']

count = 0
for for_upload in extract_summaries(project_id):
    if count >= 10:
        break

    fh = open('/tmp/summaries.temp.json', 'w')
    json.dump(for_upload, fh)
    fh.close()

    upload_resp = doccano_client.post_doc_upload(
            project_id,
            'json', 
            '/tmp/summaries.temp.json',
            '/tmp')

    count += 1
