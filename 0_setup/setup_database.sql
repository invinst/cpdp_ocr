CREATE TABLE cr_batch (
	id SERIAL PRIMARY KEY,
	dropbox_path TEXT,
	date_received TIMESTAMP,
	unique (dropbox_path)
) ;

CREATE TABLE cr_batch_data (
	id SERIAL PRIMARY KEY,
	batch_id INTEGER REFERENCES cr_batch (id),
	control_number TEXT,
	prod_begin TEXT,
	prod_end TEXT,
	prod_begin_attachment TEXT,
	prod_end_attachment TEXT,
	filename TEXT,
	filepath TEXT,
	log_number TEXT,
	placeholder TEXT
) ;

CREATE TABLE cr_pdfs (
	id SERIAL PRIMARY KEY,
	batch_id INTEGER REFERENCES cr_batch (id),
	cr_id INTEGER,
	page_count INT,
	filename TEXT,
	doccloud_id INT,
	doccloud_url TEXT,
	unique (batch_id, filename)
) ;

CREATE TABLE cr_pdf_pages (
	id SERIAL PRIMARY KEY,
	pdf_id INTEGER REFERENCES cr_pdfs (id),
	page_num INTEGER,
	page_classification TEXT,
	unique(pdf_id, page_num)
) ;

CREATE TABLE cr_ocr_tokens (
	id SERIAL PRIMARY KEY,
	pdf_id INTEGER REFERENCES cr_pdfs (id),
        page_id INTEGER REFERENCES cr_pdf_pages (id),
	page_num int,
	lvl int,
	block_num int,
	par_num int,
	line_num int,
	word_num int,
	left_bound int,
	top_bound int,
	width_bound int,
	height_bound int,
	conf int,
	text TEXT,
	background_color text,
	text_color text,
	background_color_name TEXT
) ;

CREATE TABLE cr_ocr_text (
    id SERIAL PRIMARY KEY,
    pdf_id INTEGER REFERENCES cr_pdfs (id),
    page_id INTEGER REFERENCES cr_pdf_pages (id),
    ocr_text TEXT,
    unique(pdf_id, page_id)
) ;


CREATE TABLE cr_summary_data (
    id SERIAL PRIMARY KEY,
    pdf_id INTEGER REFERENCES cr_pdfs (id),
    page_id INTEGER REFERENCES cr_pdf_pages (id),
    section_name TEXT,
    column_name TEXT,
    row_num INTEGER,
    text TEXT
) ;

CREATE TABLE cr_redaction_blocks (
    id SERIAL PRIMARY KEY,
    page_num INTEGER,
    coords TEXT
) ;

CREATE MATERIALIZED VIEW cr_narratives as (SELECT  p.cr_id, p.filename AS pdf_name, p.id as pdf_id, pp.id as page_id, sd.id summary_data_id, pp.page_num, sd.section_name, sd.column_name, sd.text, p.doccloud_url
  FROM cr_summary_data sd, cr_pdfs p, cr_pdf_pages pp
  WHERE sd.page_id = pp.id AND p.id = pp.pdf_id AND sd.text IS NOT NULL
  AND ((section_name = 'Accused Members' AND column_name = 'Initial / Intake Allegation')
    OR (section_name = 'Review Incident' AND 'col_name' = 'Remarks')
    OR (section_name = 'Incident Finding / Overall Case Finding' and column_name = 'Finding')
    OR (section_name = 'Current Allegations' and column_name = 'Allegation'))) ;

