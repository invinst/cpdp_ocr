#!/usr/bin/python3

import re
import csv

from strsimpy.normalized_levenshtein import NormalizedLevenshtein



from multiprocessing import Pool

#only keys are used here. the column names are not used in the code yet. 
col_based_headers = {
        'Incident Details': ['abcd'],
        'Incident Finding / Overall Case Finding': ['Description of Incident', 'Finding', 'Entered By', 'Entered Date'],
        'Reporting Party Information': ['Role', 'Name', 'Star No.', 'Emp No.', 'UOA / UOD', 'Position', 'Sex', 'Race', 'Address', 'Phone'],
        'Incident Information': ['Incident From Date/Time', 'Address of Incident', 'Beat', 'Dist. Of Occurrence', 'Location Code', 'Location Description'],
        'Accused Members': ['', 'Role', 'Name', 'Star No.', 'Emp No.', 'UOA / UOD', 'Position', 'Status', 'Initial / Intake Allegation'],
        'Other Involved Parties': ['Role', 'Name', 'Star No.', 'Emp No.', 'UOA / UOD', 'Position', 'Sex', 'Race', 'Address', 'Phone'],
        'Involved Party Associations': ['Role', 'Rep. Party Name', 'Related Person', 'Relationship'],
        'Incident Category List': ['Incident Category', 'Primary?', 'Initial?'],
        'Investigator History': ['Investigator', 'Type', 'Assigned Team', 'Assigned Date', 'Scheduled End Date', 'Investigation End Date', 'No. of Days'],
        'Extension History': ['Name', 'Previous Scheduled End Date', 'Extended Scheduled End Date', 'Date Certified Letter Sent', 'Reason Selected', 'Explination', 'Extension Report Date', 'Approved By', 'Approved Date', 'Approval Comments'],
        'Current Allegations': ['Accused Name', 'Seq. No.', 'Allegation', 'Category', 'Subcategory', 'Finding'],
        'Situations (Allegation Details)': ['Accused Name', 'Alleg. No.', 'Situation', 'Victim/Offender Armed?', 'Weapon Types', 'Weapon Other', 'Weapon Recovered?', 'Deceased?'],
        'Status History': ['Resulting Status', 'Status Date/Time', 'Created By', 'Position', 'UOA / UOD', 'Comments'],
        'Attachments': ['No.', 'Type', 'Related Person', 'No. of Pages', 'Narrative', 'Original in File', 'Entered By', 'Entered Date/Time', 'Status', 'Approve Content', 'Approve Inclusion'],
        'Review Incident': ['Review Type', 'Accused/Involved Member Name', 'Result Type', 'Reviewed By', 'Position', 'Unit', 'Review Date', 'Remarks'],
        'Review Accused': ['Review Type', 'Accused/Involved Member Name', 'Result Type', 'Reviewed By', 'Position', 'Unit', 'Review Date', 'Remarks'],
        'Accused Finding History': ['Accused', 'Allegation', 'Reviewed By', 'Reviewed Date/Time', 'CCR?', 'Concur?', 'Finding', 'Finding COmments'],
        'Accused Penalty History': ['Accused', 'Reviewed By', 'Reviewed Date/Time', 'CCR?', 'Concur?', 'Penalty', 'Penalty Comments'],
        'Findings': ['Accused Name', 'Allegations', 'Category', 'Concur?', 'Findings', 'Comments']
        }

#these are not being used yet.
row_based_headers  = ['Incident Details', [['CR Required?', 'Confidential?', 'Extraordinary Occurence', 'Police Shooting (U)?', 'Non Disciplinary Intervention:', 'Initial Assignment:', 'Notify IAD Immediately?', 'EEO Complaint No.:', 'Civil Suit No.:', 'Notify Chief Administator', 'Notify Coordinator?', 'Notification Other?', 'Notification Comments:'], ['Manner Incident Received?', 'Biased Language?', 'Bias Based Profiling?', 'Alcohol Related?', 'Pursuit Related?', 'Violence in Workplace?', 'Domestic Violence?', 'Civil Suit Settled Date:', 'Notify Chief?', 'Notification Does Not Apply?']]]

def find_label(label_txt, tokens):
    #get text that matches token of label to narrow location
    possible_blocks = []
    split_label = [l.lower() for l in label_txt.split()]

    for token in tokens:
        if not token['text']:
            continue

        if token['text'].lower() in split_label:
            possible_blocks.append(token['block_num'])

    #fill blocks datastructure
    block_tokens = dict([[b, []] for b in possible_blocks])
    for token in tokens:
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

        for _, line_toks in block_lines.items():
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

def lines_from_tokens(tokens):
    #extract readable lines from tokens, from tesseract tsv 'line' field
    lines = {}
    for token in tokens:
        line_key = (token['line_num'], token['par_num'], token['block_num'])
        if line_key not in lines:
            lines[line_key] = []

        #since this function only cares about readable text, removing this.
        if token['text']:
            lines[line_key].append(token)

    #lines.sort(key=lambda l: l['word_num'])
    return list(lines.values())

def last_column_header_bounds(tokens, col_key='Initial / Intake Allegation'):
    levenshtein = NormalizedLevenshtein()

    lines = lines_from_tokens(tokens)
    potential_tokens = [[]]

    for line in lines:
        lowest_distance = 2 #

        for line_token in line[::-1]:
            prev_potential_str = ' '.join([t['text'] for t in potential_tokens[-1]])
            potential_str = line_token['text'] + ' ' + prev_potential_str
            
            key_dist = levenshtein.distance(potential_str, col_key)
            if key_dist < lowest_distance:
                lowest_distance = key_dist
                potential_tokens[-1].insert(0, line_token)

            else:
                break

        potential_tokens.append([])

    most_similar = []
    lowest_dist = 2
    for tokens in potential_tokens:
        joined_str = ' '.join([t['text'] for t in tokens])
        key_dist = levenshtein.distance(joined_str, col_key)

        if key_dist <= lowest_dist:
            lowest_dist = key_dist
            most_similar = tokens

    if most_similar:
        left_point = min([t['left'] for t in most_similar])
        bottom_point = min([t['top'] + t['height'] for t in most_similar])
        top_point = max([t['top'] for t in most_similar])
        right_point = max([t['left'] + t['width'] for t in most_similar])

    else:
        left_point = 0
        bottom_point = 0
        top_point = 0
        right_point = 0

    return left_point, right_point, top_point, bottom_point

def tokens_column_intervals(tokens, headers=None, threshold=10):
    col_intervals = sorted([[t['left'], t['left'] + t['width']] for t in tokens if t['conf'] > .2])

    farthest_left = min([t['left'] for t in tokens])
    farthest_right = max([(t['left'] + t['width']) for t in tokens])

    if not col_intervals:
        return []

    col_intervals.sort(key=lambda interval: interval[0])

    column_ranges = [col_intervals[0]]
    for current in col_intervals[1:]:
        assert len(current) == 2

        previous = column_ranges[-1]
        xdiff = abs(current[0] - previous[1])
        if xdiff > threshold:
            previous[1] = max(previous[1], current[1])
        else:
            column_ranges.append(current)

    column_ranges.sort(key=lambda interval: interval[0])
    column_ranges.insert(0, (0, farthest_left-1))
    column_ranges.insert(len(column_ranges), (farthest_right, 9999))

    return column_ranges

def extract_table_header(tokens, fieldnames, threshold=5):
    token_intervals

def extract_table_rows(tokens):
    """Requires that all tokens are already in some sort of columnar structure"""

    lines = lines_from_tokens(tokens)

    thresh = 5
    within_range = []

    column_ranges = tokens_column_intervals(tokens)

    if not column_ranges:
        return []

    #creates acceptable ranges for columns
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
            row_dict[col_field] = ' '.join([t['text'] for t in cols if t['text']]).strip()

            col_num += 1
        rows_results.append(row_dict)

    return rows_results

def normalize_row(row):
    new_row = {}
    for key, val in row.items():
        if key != 'text':
            val = int(val)

        new_row[key] = val

    return new_row

def filter_toks(tokens, min_y, max_y_dist):
    """Currently just filters by max y distance between rows"""
    row_intervals = sorted([[t['top'], t['top'] + t['height']] for t in tokens if t['conf'] > .2])

    row_intervals.sort(key=lambda interval: interval[0])
    row_ranges = row_intervals
    for current in row_intervals:
        previous = row_ranges[-1]
        if current[0] - previous[1]:
            continue
        if current[0] <= previous[1]:
            previous[1] = max(previous[1], current[1])
        else:
            row_ranges.append(current)

    if len(row_ranges) <= 1:
        return tokens

    prev_top, prev_bottom = 0, min_y
    new_ranges = []
    for row_top, row_bottom in row_ranges:
        if abs(row_top - prev_bottom) > max_y_dist:
            break
        else:
            new_ranges.append([row_top, row_bottom])
            prev_top, prev_bottom = row_top, row_bottom


    new_tokens = []
    for token in tokens:
        for row_top, row_bottom in new_ranges:
            if token['top'] >= row_top and (token['top']+token['height']) <= row_bottom:
                new_tokens.append(token)
                break

    return new_tokens

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
        
        for tok in toks:
            ret_tokens.append(tok) 

    return ret_tokens

def pdf_name(pdf_num):
    leading_zeroes = '0'.join(['' for i in range(0, 8-len(str(pdf_num)))])
    name = 'CPD {}{}.pdf'.format(leading_zeroes, str(pdf_num))

    return name
 
def tsv_data(pdf_num, pdf_page, ocrd_dir='/opt/data/cpdp_pdfs/ocrd'):
    tsv_fp = '{}/{}/{}.{}.tsv'.format(ocrd_dir, pdf_name(pdf_num), pdf_name(pdf_num), pdf_page)

    with open(tsv_fp, 'r') as fh:
        fieldnames = ['level', 'page_num', 'block_num', 
                'par_num',  'line_num', 'word_num', 'left', 'top', 
                'width', 'height', 'conf', 'text']

        dr = csv.DictReader(fh, delimiter='\t', quoting=csv.QUOTE_NONE, fieldnames=fieldnames)
        dr.__next__() # ignore header; already defined
        tokens = [normalize_row(t) for t in dr]
        tokens = useful_tokens(tokens)

    return tokens, fieldnames

def parse_tsv_data(pdf_num, pdf_page=None, ocrd_dir='/opt/data/cpdp_pdfs/ocrd'):
    tokens, tsv_fieldnames = tsv_data(pdf_num, pdf_page, ocrd_dir=ocrd_dir)
    if not tokens:
        return

    page_height = tokens[0]['height']
    page_width = tokens[0]['width']

    #must be a list to preserve order. consider using ordereddict
    label_boundaries = [(i, find_label(i, tokens)) for i in col_based_headers.keys()]

    #move backwards and find boundaries between sections. labels *should* be in order
    prev_top = page_height
    section_positions = []

    for label, boundaries in label_boundaries[::-1]:
        _, _, top, _, width, _ = boundaries
        if not top or not width:
            continue

        section_positions.append([label, top, prev_top])
        prev_top = top

    label_tokens = {}
    for label, top, bottom in section_positions:
        #grab label's bottom to remove from set of tokens
        label_bottom = [b[3] for l,b in label_boundaries if l == label][0]

        min_left = 0
        min_right = page_width

        section_tokens = tokens_in_bounds(0, page_width, label_bottom, bottom, tokens)
        label_tokens[label] = section_tokens

        #iterate through each potential column header, going backwards
        label_columns = {}
        for col_name in col_based_headers[label][::-1]:
            header_bounds = last_column_header_bounds(section_tokens, col_name)
            header_left, header_right, header_top, header_bottom = header_bounds

            if not header_left:
                continue

            column_tokens = tokens_in_bounds(header_left-3, page_width, header_bottom, bottom, section_tokens)
            column_lines = lines_from_tokens(column_tokens)

            label_columns[col_name] = []
            for line in column_lines:
                for tok in line:
                    label_columns[col_name].append(tok)

            label_columns[col_name] = filter_toks(label_columns[col_name], min_y=header_bottom, max_y_dist=100)

            if label == 'Accused Members':
    #            ret_lines = lines_from_tokens(label_columns[col_name])
                ret_text = ' '.join([t['text'] for t in label_columns[col_name] if t['text']])

                if ret_text:
                    return pdf_num, pdf_page, ret_text

if __name__ == '__main__':
    params = []
    with open('input/tagged_pages.csv', 'r') as fh:
        reader = csv.DictReader(fh)

        for line in list(reader):
            pdf_num = int(re.split('[ .]', line['pdf_name'])[1])
            page_num = int(line['page_num'])

            params.append((pdf_num, page_num))

    pool = Pool(processes=13)
    results = pool.starmap(parse_tsv_data, params)
