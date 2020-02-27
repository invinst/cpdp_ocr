#!/usr/bin/python3

import re
import csv
import json

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

#narrative_fields = [
#        ['Incident Finding / Overall Case Finding', ['Description of Incident', 'Finding']],
#        ['G
#        ]

def tokens_to_text(tokens, join_str=' '):
    return join_str.join([t['text'] for t in tokens]).strip()

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
            if tokens_to_text(line_toks) == label_txt:
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
        line_key = (token['block_num'], token['par_num'], token['line_num'])
        if line_key not in lines:
            lines[line_key] = []

        #since this function only cares about readable text, removing this.
        if token['text']:
            lines[line_key].append(token)

    #already_used = []
    #ret = []
    #for token in tokens:
    #    line_key = (token['line_num'], token['par_num'], token['block_num'])
    #    if line_key in already_used:
    #        continue
    #    else:
    #        already_used.append(line_key)
#
#        ret.append(lines[line_key])

    #return ret
    return list(lines.values())

def last_column_header_bounds(tokens, col_key, threshold=.4):
    """The powerhouse of the script. Need to cleanup and document"""

    if not col_key:
        print('yo wtf this about')
        return

    levenshtein = NormalizedLevenshtein()

    lines = lines_from_tokens(tokens)

    potential_tokens = [[]]

    for line_num in range(0, len(lines)):
        line = lines[line_num]
        if col_key == 'Status':
            print('HMM', tokens_to_text(line))
        lowest_distance = 2 #distance will always be lower than this
        
        for line_token in line[::-1]:
            prev_potential_str = tokens_to_text(potential_tokens[-1])
            prev_key_dist = levenshtein.distance(prev_potential_str, col_key)

            potential_str = line_token['text'] + ' ' + prev_potential_str
            
            key_dist = levenshtein.distance(potential_str, col_key)

            len_diff = abs(len(potential_str) - len(col_key)) / len(col_key) 

            if key_dist < prev_key_dist:
                #if  key_dist < threshold and len_diff < .15:
                #    break

                lowest_distance = key_dist
                potential_tokens[-1].insert(0, line_token)

            else:
                break

        potential_tokens.append([])

    highest_toks = []
    for pot_toks in potential_tokens:
        orig_text = tokens_to_text(pot_toks)
        orig_dist = levenshtein.distance(col_key, orig_text)
        lowest_dist = 2
        lowest_toks = None

        for tok_idx in range(0, len(pot_toks)):
            sample_toks = pot_toks[-(tok_idx+1):]
            dist = levenshtein.distance(col_key, tokens_to_text(sample_toks))
            if dist < lowest_dist:
                lowest_dist = dist
                lowest_toks = sample_toks

        if lowest_toks:
            highest_toks.append(lowest_toks)

    potential_tokens = highest_toks

    #if col_key in col_based_headers['Attachments']:
    if col_key == 'Status':
        print('highest tokens', set([tokens_to_text(h) for h in highest_toks ]))

    potential_tokens_combined = []
    for p1 in range(len(highest_toks)):
        tmp_combined = []
        for p2 in range(len(highest_toks)):
            if p2 <= p1:
                continue

            tmp_combined += highest_toks[p1]
            tmp_combined += highest_toks[p2]
        potential_tokens_combined.append(tmp_combined)

    most_similar = []
    lowest_dist = 2
    for tokens in highest_toks + potential_tokens_combined:
        joined_str = tokens_to_text(tokens)

            
        key_dist = levenshtein.distance(joined_str, col_key)

        if key_dist < lowest_dist:
            #if col_key in col_based_headers['Attachments']:
            if col_key == 'Status':
                print('hmm', joined_str)
            lowest_dist = key_dist
            most_similar = tokens

    if most_similar: #and lowest_distance < threshold:
        left_point = min([t['left'] for t in most_similar])
        bottom_point = min([t['top'] + t['height'] for t in most_similar])
        top_point = max([t['top'] for t in most_similar])
        right_point = max([t['left'] + t['width'] for t in most_similar])

    else:
        left_point = 0
        bottom_point = 0
        top_point = 0
        right_point = 0

    col_str = ' '.join([t['text'] for t in most_similar])

    print('col bounds: ', col_key, left_point, right_point, top_point, bottom_point)

    return left_point, right_point, top_point, bottom_point

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

def parse_tsv_data(pdf_num, pdf_page=None, cr_id=None, ocrd_dir='/opt/data/cpdp_pdfs/ocrd'):
    tokens, tsv_fieldnames = tsv_data(pdf_num, pdf_page, ocrd_dir=ocrd_dir)
    if not tokens:
        return

    page_height = tokens[0]['height']
    page_width = tokens[0]['width']

    #must be a list to preserve order. consider using ordereddict
    label_boundaries = [(i, find_label(i, tokens)) for i in col_based_headers.keys()]
    label_boundaries = [(l, b) for l,b in label_boundaries if b[2]]
    
    section_tops = sorted([bounds[2] for key,bounds in label_boundaries if bounds[2]])
    section_tops.append(page_height)
    
    section_boundaries = []
    for label, label_bounds in label_boundaries:
        section_top = label_bounds[2]

        if section_top not in section_tops:
            print("yo wtf this shouldnt happen")
            continue

        top_idx = section_tops.index(section_top)

        section_bot = section_tops[top_idx + 1]
        section_boundaries.append([label, section_top, section_bot])

    label_tokens = {}

    sections = []
    for label, top, bottom in section_boundaries:
        #grab label's bottom to remove from set of tokens
        label_bottom = [b[3] for l,b in label_boundaries if l == label][0]
        label_top = [b[2] for l,b in label_boundaries if l == label][0]

        min_left = 0
        min_right = page_width

        section_tokens = tokens_in_bounds(0, page_width, label_bottom, bottom, tokens)
        label_tokens[label] = section_tokens

        #iterate through each potential column header, going backwards
        label_columns = {}

        is_first = True 
        section_columns = []
        prev_left = page_width
        prev_right = None
        for col_name in col_based_headers[label][::-1]:
            header_bounds = last_column_header_bounds(section_tokens, col_name)
            if not header_bounds:
                break
            if col_name in col_based_headers['Attachments']:
                print(col_name, header_bounds)

            header_left, header_right, header_top, header_bottom = header_bounds

            column_tokens = tokens_in_bounds(header_left, prev_left, header_bottom, bottom, section_tokens)
            column_lines = lines_from_tokens(column_tokens)

            label_columns[col_name] = []
            for line in column_lines:
                for tok in line:
                    label_columns[col_name].append(tok)

            label_columns[col_name] = filter_toks(label_columns[col_name], min_y=header_top, max_y_dist=9999)
            section_tokens = tokens_in_bounds(0, prev_left, label_bottom, bottom, tokens)

            ret_lines = lines_from_tokens(label_columns[col_name])

            ret_text = ''
            line_strs = []
            for line in ret_lines:
                line_strs.append(' '.join([l['text'] for l in line]))

            ret_text = '\n'.join(line_strs)

            if not ret_text or label == 'Incident Details':
                continue


            column = {'col_name': col_name, 'col_text': ret_text}
            section_columns.append(column)
            prev_left = header_left
            prev_right = header_right

        sections_dict = {
                'section_name': label,
                'columns': section_columns
                }
        sections.append(sections_dict)

    if not sections:
        return None

    ret_dict = {'pdf_num': pdf_num, 'page_num':pdf_page, 'sections': sections, 'cr_id': cr_id}

    return ret_dict

if __name__ == '__main__':
    params = []
    with open('input/tagged_pages.csv', 'r') as fh:
        reader = csv.DictReader(fh)

        for line in list(reader):
            pdf_num = int(re.split('[ .]', line['pdf_name'])[1])
            page_num = int(line['page_num'])
            cr_id = int(line['cr_id'])

            params.append((pdf_num, page_num, cr_id))

    pool = Pool(processes=13)
    results = pool.starmap(parse_tsv_data, params)

    results = [r for r in results if r]

    with open('./output/summary_tables.json', 'w') as fh:
        json.dump(results, fh)
