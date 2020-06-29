#!/usr/bin/python3

import cv2
import csv
import imutils
import os
import psycopg2
import pytesseract
import re

import numpy as np

from sklearn.cluster import KMeans 
import multiprocessing as mp
from sys import argv

from colormath.color_objects import sRGBColor, LabColor
from colormath.color_conversions import convert_color
from colormath.color_diff import delta_e_cmc

from smart_open import open

from pdf2image import convert_from_path
from PIL import Image

import logging

out_dir = './output'

bg_colors =  (('light_blue', sRGBColor(240, 247, 255)), ('white', sRGBColor(255,255,255)), ('light_gray', sRGBColor(230,230,230)))

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

conn = pg_conn()
curs = conn.cursor()
def ocr_page(page_image=None, fp=None):
    if fp:
        print("Returning early?")
        page_image = cv2.imread(fp)
        return ocr_page(page_image)

    print("Converting Image to data")
    unstructured_text = pytesseract.image_to_string(page_image)
    structured_text = pytesseract.image_to_data(page_image)

    print(structured_text)

    return structured_text, unstructured_text

def derive_luminance(rgb):
    r, g, b = rgb
    return (.2126*r) + (.587*g) + (.0722*b)

def token_colors(token, img):
    x1 = int(token['left']) - 2
    x2 = int(token['left']) + int(token['width']) + 2
    y1 = int(token['top']) - 2
    y2 = int(token['top']) + int(token['height']) + 2

    if x1 < 0: x1 = 0
    if y1 < 0: y1 = 0

    token_img = img[y1:y2, x1:x2]
    token_img = token_img.reshape((token_img.shape[0] * token_img.shape[1], 3))

    clt = KMeans(n_clusters=2)
    clt.fit(token_img)

    color_1 = clt.cluster_centers_[0]
    color_2 = clt.cluster_centers_[1]

    c1_luminance = derive_luminance(color_1)
    c2_luminance = derive_luminance(color_2)

    lum_diff = abs(c1_luminance - c2_luminance)
    if lum_diff < 100:
        print('luminance too close!')

    if c1_luminance < c2_luminance:
        text_color = color_1
        bg_color = color_2
    else:
        text_color = color_2
        bg_color = color_1

    bg_color_labcolor = convert_color(sRGBColor(*bg_color), LabColor)

    bg_canon = None
    lowest_delta_e = 999999
    for color_name, known_bg in bg_colors[::-1]:
        known_bg_labcolor = convert_color(known_bg, LabColor)

        delta_e = delta_e_cmc(known_bg_labcolor, bg_color_labcolor)
        if delta_e < lowest_delta_e:
            bg_canon = color_name
            lowest_delta_e = delta_e

    return bg_color, text_color, bg_canon

def tsv_sql_data(fp):
    """turns tsv data into sql-ready data. 
       also extracts colors of text and bg"""

    def normalize_row(row):
        new_row = {}
        for key, val in row.items():
            if key != 'text':
                val = int(val)

            new_row[key] = val

        return new_row

    tsv_lines = [l.split('\t') for l in raw_tsv_data.split('\n')]
    header = tsv_lines[0]
    tsv_tokens = [normalize_row(dict(zip(header, l))) for l in  tsv_lines[1:]]

    ret_tokens = []

    for token in tsv_tokens:
        bg_color, text_color, bg_canon = token_colors(token, img)
        token['background_color'] = list(bg_color)
        token['text_color'] = list(text_color)
        token['background_color_name'] = bg_canon

        ret_tokens.append(token)

    ret_tokens = tsv_tokens
    print(ret_tokens)

    return ret_tokens

def mod_brightcontrast(img, brightness, contrast):
    print("Increasing brightness/contrast", brightness, contrast)
    img = np.int16(img)
    img = img * (contrast/127+1) - contrast + brightness
    img = np.clip(img, 0, 255)
    img = np.uint8(img)

    return img

def remove_redactions(raw_image, bw_thresh=(85, 255), approx_val=.026):
    blurred_image = cv2.GaussianBlur(raw_image, (5,5), 0)
    gray_image = cv2.cvtColor(blurred_image, cv2.COLOR_BGR2GRAY)
    (thresh, bw_image) = cv2.threshold(gray_image, 85, bw_thresh[1], cv2.THRESH_BINARY)

    contours = cv2.findContours(bw_image, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
    contours = imutils.grab_contours(contours)

    redaction_contours = []
    for contour in contours:
        if cv2.contourArea(contour) < 300:
            continue

        peri = cv2.arcLength(contour, False)
        approx = cv2.approxPolyDP(contour, approx_val * peri, True)

        if len(approx) != 4:
            continue

        if cv2.contourArea(approx) < 350:
            continue

        contour_x, contour_y, contour_w, contour_h = cv2.boundingRect(approx)
        if contour_h == raw_image.shape[0] and contour_w == raw_image.shape[1]:
            continue

        redaction_contours.append(approx)

    for i in range(len(redaction_contours)):
        #draw white boxes on top of redaction boxes. second draw handles edges.
        cv2.drawContours(raw_image, redaction_contours, i, color=(255,255,255), thickness=cv2.FILLED)
        cv2.drawContours(raw_image, redaction_contours, i, color=(255,255,255), thickness=4)

    return raw_image


def ocr_file(filename, pages=[], brightness=30, contrast=30, no_redactions=True):
    pdf_path = f'./input/{filename}'

    file_basename = os.path.basename(pdf_path)
    pdf_outdir = '{}/{}'.format(out_dir, file_basename)

    os.makedirs(pdf_outdir, exist_ok=True)

    print("OCRing", pdf_path)

    page_images = convert_from_path(pdf_path)[:1]
    page_num = 1
    pdf_name = os.path.basename(pdf_path)
    for page_image in page_images:
        page_image = np.array(page_image)
        if brightness or contrast:
            clean_page_image = mod_brightcontrast(page_image, brightness, contrast)

        if no_redactions:
            clean_page_image = remove_redactions(page_image)

        #skip pages if list of pages not provided
        if pages and page_num not in pages:
            page_num += 1
            continue

        structured_text, unstructured_text = ocr_page(clean_page_image)

        out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'txt')
        print(out_fp)
        with open(out_fp, 'w') as fh:
            fh.write(unstructured_text)

        out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'tsv')
        print(out_fp)
        with open(out_fp, 'w') as fh:
            fh.write(structured_text)

        #out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'clean.png')
        #rint(out_fp)
        #v2.imwrite(out_fp, clean_page_image)

        out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'png')
        print(out_fp)
        cv2.imwrite(out_fp, page_image)

        page_num += 1
#    insert_ocr_file(filename, len(page_images))
    print('uh?')

def output_dir_finished(filename, page_count):
    output_dir = f'./output/{filename}'
    if not os.path.exists(output_dir):
        return False

    file_types = ['tsv', 'txt', 'png']
    existing_dir_fps = os.listdir(output_dir)

    for file_type in file_types:
        for page_num in range(1, page_count+1):
            file_type_fp = f'{filename}.{page_num}.{file_type}'
            if file_type_fp not in existing_dir_fps:
                print(f'Missing: {file_type_fp}, processing full file.')
                return False

    return True

def get_input_files():
    sqlstr = "SELECT filename, page_count FROM cr_pdfs"
    curs.execute(sqlstr)

    return list(curs.fetchall())

def get_pending_pdfs(input_files):
    existing_files = os.listdir('./output')
    pending_files = []
    for filename, page_count in input_files:
        is_finished = output_dir_finished(filename, page_count)

        if not is_finished:
            pending_files.append(filename)

    return pending_files


def insert_ocr_file(filename, page_count):
    conn = pg_conn()
    curs = conn.cursor()

    print('Inserting..', filename)
    for page_num in range(1, page_count+1):
        page_fp = f'./output/{filename}/{filename}.{page_num}.txt'
        with open(page_fp, 'r') as fh:
            ocr_text = fh.read()

        sqlstr = """INSERT INTO cr_pdf_pages (pdf_id, page_num)
        SELECT p.id, %s FROM cr_pdfs p where p.filename = %s
        ON CONFLICT DO NOTHING
        RETURNING cr_pdf_pages.id"""
        curs.execute(sqlstr, (page_num, filename))
        page_id = curs.fetchone()[0]

        sqlstr = f"""
        INSERT INTO cr_ocr_text (pdf_id, page_id, ocr_text)
        SELECT p.id, pp.id, %s
        FROM cr_pdfs p, cr_pdf_pages pp
        WHERE p.filename = %s
        AND pp.page_num = %s
        AND pp.pdf_id = p.id
        ON CONFLICT DO NOTHING
        RETURNING cr_ocr_text.id"""

        curs.execute(sqlstr, (ocr_text, filename, int(page_num)))
        resp = curs.fetchall()

        print('udfasdfa')

        #if len(resp) == 0:
        #    continue #skip if ocr text already there -- quick fix.

        sqlstr = """
           INSERT INTO cr_ocr_tokens 
            (pdf_id, page_id, lvl, page_num, block_num, par_num, line_num, word_num, 
             left_bound, top_bound, width_bound, height_bound, conf, text, background_color, text_color, background_color_name) 
             SELECT p.id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM cr_pdfs p
           WHERE p.filename = %s
           """
        print('udfasdfa')

        page_fp = f'./output/{filename}/{filename}.{page_num}.tsv'
        with open(page_fp, 'r') as fh:
            for line in csv.DictReader(fh, delimiter='\t'):
                vals = (page_id, line['level'], page_num,  line['block_num'],  line['par_num'],  line['line_num'], 
                line['word_num'],  line['left'],  line['top'],  line['width'], 
                line['height'],  line['conf'],  line['text'],  line['background_color'], line['text_color'], line['background_color_name'], filename)

                mog = curs.mogrify(sqlstr, vals).decode('utf8')
                print(mog)
                curs.execute(mog)

    print('got here?')
    conn.commit()
    conn.close()

def insert_ocr_files(input_files):
    pool = mp.Pool(processes=32)
    pool.map(insert_ocr_file, input_files, chunksize=8)

    conn.commit()

logger = mp.log_to_stderr
 
input_files = get_input_files()
pending_files = get_pending_pdfs(input_files)[:10]
print("Pending files: ", len(pending_files))

#pool = mp.Pool(processes=16)
#pool.map(ocr_file, pending_files, chunksize=1)

print(type(input_files))
#insert_ocr_files(input_files)   
