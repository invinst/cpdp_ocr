#!/usr/bin/python3

from pprint import pprint

from functools import reduce
import re
import os
import csv



#only keys are used here. the column names are not used in the code yet. 
col_based_headers = [
        ['Incident Finding / Overall Case Finding', ['Description of Incident', 'Finding', 'Entered By', 'Entered Date']],
        ['Reporting Party Information', ['Role', 'Name', 'Star No.', 'Emp No.', 'UOA / UOD', 'Position', 'Sex', 'Race', 'Address', 'Phone']],
        ['Incident Information', ['Incident From Date/Time', 'Address of Incident', 'Beat', 'Dist. Of Occurrence', 'Location Code', 'Location Description']],
        ['Accused Members', ['Role', 'Name', 'Star No.', 'Emp No.', 'UOA / UOD', 'Position', 'Status', 'Initial / Intake Allegation']],
        ['Other Involved Parties', ['Role', 'Name', 'Star No.', 'Emp No.', 'UOA / UOD', 'Position', 'Sex', 'Race', 'Address', 'Phone']],
        ['Involved Party Associations', ['Role', 'Rep. Party Name', 'Related Person', 'Relationship']],
        ['Incident Category List', ['Incident Category', 'Primary?', 'Initial?']],
        ['Investigator History', ['Investigator', 'Type', 'Assigned Team', 'Assigned Date', 'Scheduled End Date', 'Investigation End Date', 'No. of Days']],
        ['Extension History', ['Name', 'Previous Scheduled End Date', 'Extended Scheduled End Date', 'Date Certified Letter Sent', 'Reason Selected', 'Explination', 'Extension Report Date', 'Approved By', 'Approved Date', 'Approval Comments']],
        ['Current Allegations', ['Accused Name', 'Seq. No.', 'Allegation', 'Category', 'Subcategory', 'Finding']],
        ['Situations (Allegation Details)', ['Accused Name', 'Alleg. No.', 'Situation', 'Victim/Offender Armed?', 'Weapon Types', 'Weapon Other', 'Weapon Recovered?', 'Deceased?']],
        ['Status History', ['Resulting Status', 'Status Date/Time', 'Created By', 'Position', 'UOA / UOD', 'Comments']],
        ['Attachments', ['No.', 'Type', 'Related Person', 'No. of Pages', 'Narrative', 'Original in File', 'Entered By', 'Entered Date/Time', 'Status', 'Approve Content', 'Approve Inclusion']],
        ['Review Incident', ['Review Type', 'Accused/Involved Member Name', 'Result Type', 'Reviewed By', 'Position', 'Unit', 'Review Date', 'Remarks']],
        ['Review Accused', ['Review Type', 'Accused/Involved Member Name', 'Result Type', 'Reviewed By', 'Position', 'Unit', 'Review Date', 'Remarks']],
        ['Accused Finding History', ['Accused', 'Allegation', 'Reviewed By', 'Reviewed Date/Time', 'CCR?', 'Concur?', 'Finding', 'Finding COmments']],
        ['Accused Penalty History', ['Accused', 'Reviewed By', 'Reviewed Date/Time', 'CCR?', 'Concur?', 'Penalty', 'Penalty Comments']],
        ['Findings', ['Accused Name', 'Allegations', 'Category', 'Concur?', 'Findings', 'Comments']],]

#these are not being used yet.
row_based_headers  = ['Incident Details', [['CR Required?', 'Confidential?', 'Extraordinary Occurence', 'Police Shooting (U)?', 'Non Disciplinary Intervention:', 'Initial Assignment:', 'Notify IAD Immediately?', 'EEO Complaint No.:', 'Civil Suit No.:', 'Notify Chief Administator', 'Notify Coordinator?', 'Notification Other?', 'Notification Comments:'], ['Manner Incident Received?', 'Biased Language?', 'Bias Based Profiling?', 'Alcohol Related?', 'Pursuit Related?', 'Violence in Workplace?', 'Domestic Violence?', 'Civil Suit Settled Date:', 'Notify Chief?', 'Notification Does Not Apply?']]]

def find_label(label_txt, structured_tokens):
    #get text that matches token of label to narrow location
    possible_blocks = []
    split_label = [l.lower() for l in label_txt.split()]

    for token in structured_tokens:
        if not token['text']:
            continue

        if token['text'].lower() in split_label:
            possible_blocks.append(token['block_num'])

    #fill blocks datastructure
    block_tokens = dict([[b,[]] for b in possible_blocks])
    for token in structured_tokens:
        block_num = token['block_num']
        if block_num in possible_blocks:
            block_tokens[block_num].append(token)

    label_tokens = []
    for block_num, tokens in block_tokens.items():
        block_lines = dict([ ((t['line_num'],t['par_num']), []) for t in tokens])
        for token in tokens:
            line_num = token['line_num']
            par_num = token['par_num']

            block_lines[(line_num, par_num)].append(token)

        for block_num, line_toks in block_lines.items():
            if ' '.join([tok['text'] for tok in line_toks]).strip() == label_txt:
                [label_tokens.append(t) for t in line_toks]
                break

    left = None
    right = None
    top = None
    bottom = None
    height = None
    width = None

    for tok in label_tokens:
        tok_left = tok['left']
        tok_height = tok['height']
        tok_top = tok['top']

        if left == None or tok_left < left:
            left = tok_left

        if height == None or tok_height > height:
            height = tok_height

        if top == None or tok_top < top:
            top = tok_top

    width = None if not height else sum([tok['width'] for tok in label_tokens])
    if top and height:
        bottom = top + height
    if left and width:
        right = left + width

    return left, right, top, bottom, width, height

def tokens_in_bounds(left, right, top, bottom, tokens):
    in_bounds = []
    for token in tokens:
        token_left = token['left']
        token_top = token['top']
        token_width = token['width']
        token_height = token['height']
        token_right = token['left'] + token_width
        token_bottom = token_top + token_height

        if token_left >= left and token_right <= right and token_top >= top and token_bottom <= bottom:
            in_bounds.append(token)

    return in_bounds

def extract_table_rows(tokens):
    """Requires that all tokens are already in some sort of columnar structure"""

    #figure out why 10 is needed here..
    col_intervals = sorted([[t['left'], t['left'] + t['width']] for t in tokens if t['conf'] > .2])
    if not col_intervals:
        print("No tokens found")
        return []
    farthest_left = col_intervals[0][0]
    
    col_intervals.sort(key=lambda interval: interval[0])

    column_ranges = col_intervals[0:1]
    for current in col_intervals:
        previous = column_ranges[-1]
        if current[0] <= previous[1] or current[0] - previous[1] < 15:
            previous[1] = max(previous[1], current[1])
        else:
            column_ranges.append(current)

    column_ranges.sort(key=lambda interval: interval[0])

    row_intervals = sorted([[t['top']-5, t['top'] + t['height']+ 5] for t in tokens if t['conf'] > .2])

    row_intervals.sort(key=lambda interval: interval[0])
    row_ranges = [row_intervals[0]]
    for current in row_intervals:
        previous = row_ranges[-1]
        if current[0] - previous[1] > 45:
            continue
        if current[0] <= previous[1]:
            previous[1] = max(previous[1], current[1])
        else:
            row_ranges.append(current)

    rows = []
    for row_top, row_bottom in row_ranges:
        row = []
        for token in tokens:
            if token['top'] >= row_top and (token['top'] + token['height']) <= row_bottom:
                row.append(token)

        rows.append(row)

    columns = []
    for col_left, col_right in column_ranges:
        column = []
        for token in tokens:
            if token['left'] >= col_left and (token['left'] + token['width']) <= col_right:
                column.append(token)

        columns.append(column)

    row_cols = [[[] for r in columns ] for i in rows]
    for col_num in range(0, len(column_ranges)):
        col_left, col_right = column_ranges[col_num]
        for row_num in range(0, len(rows)):
            for row_token in rows[row_num]:
                row_right = row_token['left'] + row_token['width']
                if row_token['left'] >= col_left and row_right <= col_right:
                    row_cols[row_num][col_num].append(row_token)

    #define headers
    headers = [' '.join([c['text'] for c in col if c['conf'] > 50]).strip() for col in row_cols[0]]

    rows_results = []
    for row in row_cols[1:]: #skip header
        row_dict = {}
        col_num = 0
        for cols in row:
            col_field = headers[col_num]
            row_dict[col_field] = ' '.join([t['text'] for t in cols]).strip()

            col_num += 1
        rows_results.append(row_dict)

    return rows_results

def normalize_row(line):
    new_line = {}
    for key, val in line.items():
        if key != 'text':
            val = int(val)

        new_line[key] = val

    return new_line

def useful_tokens(tokens):
    """Gets rid of noise, where possible. Currently just removes 'AUTO CR - LOG SUMMARY' lines"""
    ret_tokens = []
    lines = {}
    for token in tokens:
        line_key = (token['line_num'], token['par_num'], token['block_num'])
        if line_key not in lines:
            lines[line_key] = []

        lines[line_key].append(token)

    for pos, toks in lines.items():
        line_text = ' '.join([t['text'] for t in toks if t['text']])
        if line_text.startswith('AUTO CR - LOG SUMMARY'):
            continue
        if line_text.startswith('CPD 00'):
            continue
        
        else:
            for tok in toks:
                ret_tokens.append(tok) 

    return ret_tokens

def parse_tsv_data(pdf_num, pdf_page=None):
    leading_zeroes = '0'.join(['' for i in range(0, 8-len(str(pdf_num)))])
    pdf_name = 'CPD {}{}.pdf'.format(leading_zeroes, str(pdf_num))

    ocrd_dir = '/opt/data/cpdp_pdfs/ocrd'


    tsv_fp = '{}/{}/{}.{}.tsv'.format(ocrd_dir, pdf_name, pdf_name, pdf_page)

    with open(tsv_fp, 'r') as fh:
        dr = csv.DictReader(fh, delimiter='\t')
        structured_tokens = [normalize_row(t) for t in dr]
        structured_tokens = useful_tokens(structured_tokens)

    page_height = structured_tokens[0]['height']
    page_width = structured_tokens[0]['width']

#must be a list to preserve order. consider using ordereddict
    label_boundaries = [(i[0], find_label(i[0], structured_tokens)) for i in col_based_headers]

    #move backwards and find boundaries between sections. labels *should* be in order
    prev_top = page_height
    section_positions = []
    for label, boundaries in label_boundaries[::-1]:
        left, right, top, bottom, width, height = boundaries
        if not top or not width:
            continue

        section_positions.append([label, top, prev_top])
        prev_top = top

    label_tokens = {}
    for label, top, bottom in section_positions:
        #grab label's bottom to remove from set of tokens
        label_bottom = [b[3] for l,b in label_boundaries if l == label][0]
        section_tokens = tokens_in_bounds(0, page_width, label_bottom, bottom, structured_tokens)
        label_tokens[label] = section_tokens

    out_fp = '{}/{}/{}.{}.tables.csv'.format(ocrd_dir, pdf_name, pdf_name, pdf_page)

    #flush old file
    with open(out_fp, 'w') as fh:
        pass

    for label, label_tokens in label_tokens.items():
        table_map = extract_table_rows(label_tokens)
        if not table_map:
            continue

        with open(out_fp, 'a') as fh:
            fh.write('####{}####\n'.format(label))
            writer = csv.DictWriter(fh, fieldnames=table_map[0].keys())

            writer.writeheader()
            writer.writerows(table_map)
            fh.write('\n')
            
with open('input/tagged_pages.csv', 'r') as fh:
    reader = csv.DictReader(fh)

    for line in reader:
        pdf_num = int(re.split('[ .]', line['pdf_name'])[1])
        page_num = int(line['page_num'])

        parse_tsv_data(pdf_num, page_num)
