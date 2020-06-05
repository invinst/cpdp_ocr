#!/usr/bin/env python

import argparse
import os

import cv2
import numpy as np
import imutils
import pytesseract
from sklearn.cluster import KMeans
from colormath.color_conversions import convert_color
from colormath.color_diff import delta_e_cmc
from pdf2image import convert_from_path


def ocr_page(page_image=None, fp=None):
    if fp:
        print("Returning early?", flush=True)
        page_image = cv2.imread(fp)
        return ocr_page(page_image)

    print("Converting Image to data", flush=True)
    unstructured_text = pytesseract.image_to_string(page_image)
    structured_text = pytesseract.image_to_data(page_image)

    return unstructured_text, structured_text


def mod_brightcontrast(img, brightness, contrast):
    print("Increasing brightness/contrast", brightness, contrast, flush=True)
    img = np.int16(img)
    img = img * (contrast/127+1) - contrast + brightness
    img = np.clip(img, 0, 255)
    img = np.uint8(img)

    return img


def remove_redactions(raw_image, bw_thresh=(85, 255), approx_val=.026):
    blurred_image = cv2.GaussianBlur(raw_image, (5, 5), 0)
    gray_image = cv2.cvtColor(blurred_image, cv2.COLOR_BGR2GRAY)
    (thresh, bw_image) = cv2.threshold(
        gray_image, 85, bw_thresh[1], cv2.THRESH_BINARY)

    contours = cv2.findContours(
        bw_image, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
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
        # draw white boxes on top of redaction boxes. second draw handles edges.
        cv2.drawContours(raw_image, redaction_contours, i,
                         color=(255, 255, 255), thickness=cv2.FILLED)
        cv2.drawContours(raw_image, redaction_contours, i,
                         color=(255, 255, 255), thickness=4)

    return raw_image


def ocr_file(pdf_path, pages=[], brightness=30, contrast=30, no_redactions=True):
    file_basename = os.path.basename(pdf_path)

    print("OCRing", pdf_path, flush=True)

    page_images = convert_from_path(pdf_path)
    page_num = 1
    pdf_name = os.path.basename(pdf_path)
    for page_image in page_images:

        print(pdf_name, flush=True)
        page_image = np.array(page_image)
        if brightness or contrast:
            clean_page_image = mod_brightcontrast(
                page_image, brightness, contrast)

        if no_redactions:
            clean_page_image = remove_redactions(page_image)

        # skip pages if list of pages not provided
        if pages and page_num not in pages:
            page_num += 1
            continue

        pdf_outdir = os.path.join("/out", '%s.%s' %
                                  (file_basename, str(page_num).zfill(3)))
        os.makedirs(pdf_outdir, exist_ok=True)

        structured_text, unstructured_text = ocr_page(clean_page_image)

        out_fp = os.path.join(pdf_outdir, 'page.txt')
        print(out_fp, flush=True)
        with open(out_fp, 'w') as fh:
            fh.write(unstructured_text)

        out_fp = os.path.join(pdf_outdir, 'page.tsv')
        print(out_fp, flush=True)
        with open(out_fp, 'w') as fh:
            fh.write(structured_text)\

        out_fp = os.path.join(pdf_outdir, 'page.png')
        print(out_fp, flush=True)
        cv2.imwrite(out_fp, page_image)

        page_num += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='OCR pdfs from specified repository')
    parser.add_argument('pdfs_repo')
    args = parser.parse_args()
    repo_dir = os.path.join("/in", args.pdf_repo)

    for filename in os.listdir(repo_dir):
        if filename.endswith(".pdf"):
            ocr_file(os.path.join(repo_dir, filename))
