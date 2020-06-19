# cpdp_ocr

## ETL Flow

For the purpose of this document, "token" is a generic word used to describe any whitespace delimited set of characters.

Warning to all who enter: this data is *exceptionally* messy. 

1. There can be many document types within the same document. 
2. There can be repeats of the same document. 
3. All documents are scanned. Assume it was scanned poorly.
4. Redaction is inconsistent and often cuts into other words.
5. Make zero assumptions about the OCR.

### 0. Setup:

This step primes the database for pipeline, downloads files from Dropbox into 0_setup/output, and inserts high level batch and pdf data into cr_batches and cr_pdfs.

a. (clean.sh) Drops "cpdp" database and recreates it as postgres user.
b. (setup_database.sql) Creates tables used throughout project.
c. (setup_batches.py) Downloads batches of pdfs from dropbox into 0_setup/output. Batch paths come from batches.txt. Inserts row per batch into cr_batches. Inserts row per PDF into cr_pdfs with cr_batches id, pdf filename and page_count.

Notes: 
-setup_batches.py can be run more once and will continue wherever it's left off.
-PDFs are not split into pages at this step.
-Because there are no filename colisions in CPD's batches, all files are put into the same directory.

### 1. OCR

a. (ocr_docs.py) Pulls list of PDFs from from cr_pdfs and checks to see if PDF is OCRd and extracted into 1_ocr/output/ completely. 
b. (ocr_docs.py) If a file needs OCRing, each PDF page is extracted as a PIL image into memory for processing.
c. (ocr_docs.py) Page processing:
     0. Each page's OCR'd files are stored as 1_ocr/output/$PDF_NAME/$PDF_NAME.$PAGENO.$TYPE
     i. Page brightness and contrast are increased by 30%.
    ii. Black squares are identified as redaction boxes and filled with white. This improves OCR.
   iii. Page is fed into tesseract twice: once for simple, unstructured text extraction, and another for structured extraction. This results in a .txt (unstructured) and a .tsv (structured) file for each page.
    iv. If enabled (not currently, since it's *very* slow), the color of each OCR'd token and its background is extracted from each document and added to the .tsv data per-token.
     v. Structured data is pushed into cr_ocr_tokens, unstructured into cr_ocr_text, and a page reference into cr_pdf_pages.

Notes: 
- The identification of redaction boxes does not take into account angles and only checks for four sided geometries over a certain size. This leads to occasional odd behavior.
Notes:
- A file is completely extracted if there is a .tsv, .txt, .png and for each page under each PDF's respective output dir.
- The extracted .png files are not used in the workflow anywhere and only exist for convenience.
- All inserts *except* for cr_ocr_text are repeatable. It's also a very, very slow step and needs to be re-worked.

### 2. Classifiers

Runs through a simple set of checks for words or phrases that a specific document might contain. This step needs significant work and varifiability. Because of OCR errors, all checks have some amount of string distance checks. Ideally, this should be done with topic modeling.

There are three general checks. All checks use levenstein distancekk: 
a. If a page starts with X, then the page is likely Y.
b. If a page ends with X, then page is likely Y.
c. If a page's left side starts with X1, then later X2, then later X3, ..., then the page is likely Y.

Theory of how to do this for form documents:
1. Identify groups of font sizes.
2. Run topic modeling against groups of fonts with a high number of topics.
3. From this should come out two types of topics: header info and other text.
4. This should lead into a model that identifies which pages have which headers and if combining pages completes a document type.

### 3. Parsers

Currently only one parser that extracts information from summary sheets. These are the most common documents and contain decently descriptive narratives. Each summary sheet has a series of tables with defined and consistent names.

All of this is done using the structured tsv data from tesseract, and all "lines" are made on the fly.  

a. (summary_logs.py) From all pages in cr_pdf_pages, identify starts of sections eg "Incident Information" or "Accused Finding History". This is effectively the "table name"
b. (summary_logs.py) The start of each section defines boundaries between tables.
c. (summary_logs.py) Starting with the top-right token of the table, generate a series of combined phrases by iterating left-ward + iterating downward. 
d. (summary_logs.py) Iterate through combinations and identify token combo with closest levenstein distance to far right column name. This provides an approximate top-sside boundary for non-header rows as well as high confidence left-side boundary. Section boundaries provide a lower limit. 
e. (summary_logs.py) Repeat c + d, but with the right-side boundary being the previous column's left-side boundary. Iterate until no more columns available.
f. (summary_logs.py) Data is dumped into cr_summary_data.

Notes: 
- "Incident Details" is a section in the summary document with a different structure and is ignored.
- Doc type is ignored, despite being classified prior to this step. This process is run on *all* documents just in case. 
- While table names and column names are hard coded, levenstein distance is still used to account for OCR errors.

### 4. Uploaders
