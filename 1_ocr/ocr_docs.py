#!/usr/bin/python3

import cv2
import imutils
import os
import pytesseract
import re

import numpy as np

from multiprocessing import Pool
from PIL import Image
from sys import argv


from pdf2image import convert_from_path

out_dir = '/opt/data/cpdp_pdfs/ocrd'

def ocr_page(page_image=None, fp=None):
    if fp:
        page_image = cv2.imread(fp)
        return ocr_page(page_image)

    try:
        page_str = pytesseract.image_to_string(page_image)
        page_data = pytesseract.image_to_data(page_image)
    except:
        print("Page not tesseractable: ")
        return None, None

    return page_str, page_data

#def save_page(data, extension, page_no):

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

    cimg = np.zeros_like(raw_image)
    for i in range(len(redaction_contours)):
        cimg = np.zeros_like(raw_image)
        #draw white boxes on top of redaction boxes. second draw handles edges.
        cv2.drawContours(raw_image, redaction_contours, i, color=(255,255,255), thickness=cv2.FILLED)
        cv2.drawContours(raw_image, redaction_contours, i, color=(255,255,255), thickness=4)

    return raw_image

def ocr_file(path, pages=[], brightness=30, contrast=30, no_redactions=True):
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

    page_images = convert_from_path(path)
    page_num = 1
    for page_image in page_images:
        if brightness or contrast:
            page_image = mod_brightcontrast(page_image, brightness, contrast)

        if no_redactions:
            page_image = remove_redactions(page_image)

        #skip pages if list of pages not provided
        if pages and page_num not in pages:
            page_num += 1
            continue

        unstructured_text, structured_text = ocr_page(page_image)
        if not unstructured_text or not structured_text:
            print('[ERROR]', path, 'could not be tesseracted correctly')
            continue

        out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'tsv')
        print(out_fp)
        with open(out_fp, 'w') as fh:
            fh.write(structured_text)

        out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'txt')
        print(out_fp)
        with open(out_fp, 'w') as fh:
            fh.write(unstructured_text)

        out_fp = '{}/{}.{}.{}'.format(pdf_outdir, file_basename, page_num, 'png')
        print(out_fp)
        cv2.imwrite(out_fp, page_image)

        page_num += 1

    print('done')

if __name__ == '__main__':
    raw_pdf_path = '/opt/data/cpdp_pdfs/pdfs'
    if os.path.exists('/dev/shm/pdfs'):
        raw_pdf_path = '/dev/shm/pdfs'

    if len(argv) == 1:
        pdfs = ['{}/{}'.format(raw_pdf_path, f) for f in os.listdir(raw_pdf_path)]

        pages = []

    elif len(argv) == 2:
        #if os.path.exists(argv[1]):
        #    ocr_file
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
   
    pool = Pool(processes=8)
    results = pool.imap_unordered(ocr_file, pdfs, chunksize=32)
