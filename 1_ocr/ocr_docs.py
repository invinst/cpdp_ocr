#!/usr/bin/python3

import cv2
import imutils
import os
import psycopg2
import pytesseract
import re

import numpy as np

from sklearn.cluster import KMeans 
from multiprocessing import Pool
from PIL import Image
from sys import argv

from colormath.color_objects import sRGBColor, LabColor
from colormath.color_conversions import convert_color
from colormath.color_diff import delta_e_cmc

from smart_open import open

from pdf2image import convert_from_path

out_dir = './output'

bg_colors =  (('light_blue', sRGBColor(240, 247, 255)), ('white', sRGBColor(255,255,255)), ('light_gray', sRGBColor(230,230,230)))

def pg_conn(db_name='cpdp', db_host='localhost', db_user='cpdp', db_pass='cpdp'):
    vals = dict(db_name=db_name, db_host=db_host, db_user=db_user, db_pass=db_pass)
    conn_str = "dbname={db_name} host={db_host} user={db_user} password={db_pass}".format(**vals)
    conn = psycopg2.connect(conn_str)

    return conn

conn = pg_conn()
def ocr_page(page_image=None, fp=None):
    if fp:
        print("Returning early?")
        page_image = cv2.imread(fp)
        return ocr_page(page_image)

    print("Converting Image to data")
    page_data = pytesseract.image_to_data(page_image)
    print(page_data)

    return page_data

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

def ocr_sql_data(raw_tsv_data, img, page_name):
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
        if cv2.contourArea(contour) < 150:
            continue

        peri = cv2.arcLength(contour, False)
        approx = cv2.approxPolyDP(contour, approx_val * peri, True)

        if len(approx) != 4:
            continue

        if cv2.contourArea(approx) < 150:
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


def ocr_file(path, pages=[], brightness=30, contrast=30, no_redactions=True):
    print("got here?")
    curs = conn.cursor()

    file_basename = os.path.basename(path)
    pdf_outdir = '{}/{}'.format(out_dir, file_basename)
    print("Attempting to create ", pdf_outdir)

    #TODO: remove this try/except
    try:
        os.makedirs(pdf_outdir)
        print("Created directory, ", pdf_outdir)
    except:
        print("Directory already made: {}".format(pdf_outdir))
        return

    print("OCRing", path)

    page_images = convert_from_path(path)
    page_num = 1
    pdf_name = os.path.basename(path)
    for page_image in page_images:
        print(pdf_name)
        page_image = np.array(page_image)
        if brightness or contrast:
            clean_page_image = mod_brightcontrast(page_image, brightness, contrast)

        if no_redactions:
            clean_page_image = remove_redactions(page_image)

        #skip pages if list of pages not provided
        if pages and page_num not in pages:
            page_num += 1
            continue

        structured_text = ocr_page(clean_page_image)
        sql_data = ocr_sql_data(structured_text, page_image, pdf_name)
        print(f'sql_data: {sql_data}')
        sqlstr = """
            INSERT INTO cr_ocr_tokens 
	      (pdf_id, lvl, page_num, block_num, par_num, line_num, word_num, 
              left_bound, top_bound, width_bound, height_bound, conf,
              text) 
            --, background_color, text_color, background_color_name)
            --SELECT p.id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM cr_pdfs p
              SELECT p.id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM cr_pdfs p
            WHERE p.filename = %s
             """ 
        sqlstr = """
            INSERT INTO cr_ocr_tokens 
	      (pdf_id, lvl, page_num, block_num, par_num, line_num, word_num, 
              left_bound, top_bound, width_bound, height_bound, conf, text) 
              SELECT p.id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM cr_pdfs p
            WHERE p.filename = %s
             """ 
        for line in sql_data:
            print('Creating sql data....')
            vals = (line['level'],  page_num,  line['block_num'],  line['par_num'],  line['line_num'], 
                line['word_num'],  line['left'],  line['top'],  line['width'], 
                line['height'],  line['conf'],  line['text'],  
               # str(line['background_color']),  str(line['text_color']),  line['background_color_name'], 
                pdf_name)

            mog = curs.mogrify(sqlstr, vals)
            curs.execute(mog.decode('utf8'))

        conn.commit()

        if not structured_text:
            print('[ERROR]', path, 'could not be tesseracted correctly')
            continue

        out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'tsv')
        print(out_fp)
        with open(out_fp, 'w') as fh:
            fh.write(structured_text)

        out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'clean.png')
        print(out_fp)
        cv2.imwrite(out_fp, clean_page_image)

        out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'png')
        print(out_fp)
        cv2.imwrite(out_fp, page_image)

        page_num += 1

    conn.commit()

if __name__ == '__main__':
    raw_pdf_path = './input'

    if len(argv) == 1:
        pdfs = ['{}/{}'.format(raw_pdf_path, f) for f in os.listdir(raw_pdf_path)]
        print(len(pdfs))

        pages = []

    elif len(argv) == 2:
        crs = argv[1].split(',')
    
        pages = []

        for cr in crs:
            zeroes = ''.join(['0' for i in range(0,7-len(cr))])
            pdf_path = '{}/CPD {}{}.pdf'.format(raw_pdf_path, zeroes, cr)
            pdfs.append(pdf_path)
    
    elif len(argv) == 3:
        crs = argv[1].split(',')
        pages =  list(map(int, argv[2].split('.')))
    
        for cr in crs:
            zeroes = ''.join(['0' for i in range(0,7-len(cr))])
            pdf_path = '{}/CPD {}{}.pdf'.format(raw_pdf_path, zeroes, cr)
            pdfs.append(pdf_path)
   
    ocr_file(pdfs[0])
