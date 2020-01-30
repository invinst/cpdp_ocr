#!/usr/bin/python3

import pytesseract


fp = '/opt/data/cpdp_pdfs/ocrd_cr_docs/CPD 0000416.pdf/CPD 0000416.pdf.1.png'

datas = pytesseract.image_to_data(fp)
