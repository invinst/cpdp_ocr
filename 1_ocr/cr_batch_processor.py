#!/usr/bin/python3

import os
import pytesseract
import re

from multiprocessing import Pool
from PIL import Image

from pdf2image import convert_from_path

out_dir = '/opt/data/cpdp_pdfs/ocrd'

def process_page(page_image):
    page_str = pytesseract.image_to_string(page_image)
    page_data = pytesseract.image_to_data(page_image)

    return page_str, page_data

def process_pdf(path):
    print(path)
    file_basename = os.path.basename(path)
    pdf_outdir = '{}/{}'.format(out_dir, file_basename)
    print("Attempting to create ", pdf_outdir)
    try:
        os.makedirs(pdf_outdir)
        print("Created directory, ", pdf_outdir)
    except:
        print("Skipping: {}".format(pdf_outdir))
        return

    processed_pages = []
    page_images = convert_from_path(path)
    page_num = 1
    for page_image in page_images:
        unstructured_text, structured_text = process_page(page_image)
        processed_pages.append(unstructured_text)

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
        page_image.save(out_fp)

        page_num+=1

    print('done')
    
raw_pdf_path = '/dev/shm/cpdp_pdfs/pdfs'

pdfs = ['{}/{}'.format(raw_pdf_path, f) for f in os.listdir(raw_pdf_path)]
print(pdfs)

pool = Pool(processes=8)

results = pool.map(process_pdf, pdfs)
