# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdmay29jun1/individual/share/ocr/src/pdf2hocr.R

pacman::p_load(
    argparse,
    dplyr,
    here,
    feather,
    logger,
    magick,
    pdftools,
    purrr,
    readr,
    stringr,
    tesseract,
    tidyr,
    tools,
    xml2
)

parser <- ArgumentParser()
parser$add_argument("--index")
parser$add_argument("--xmldir")
parser$add_argument("--DPI", type="integer")
parser$add_argument("--output")
args <- parser$parse_args()

####

OCR_DPI <- args$DPI
log_info("DPI: ", OCR_DPI)

####

ocr_cached <- function(filename, pageno, engine, DPI=OCR_DPI) {
    xml_fn <- paste0(file_path_sans_ext(basename(filename)),
                     sprintf("-%04i", pageno))
    xml_fn <- paste0(args$xmldir, "/", xml_fn, ".xml")
    if (file.exists(xml_fn)) return(read_xml(xml_fn))
    log_info("OCR for: ", xml_fn)
    img <- pdf_render_page(filename, page = pageno,
                           dpi = DPI, numeric = FALSE) %>%
        image_read
    hocr <- ocr(img, HOCR=TRUE, engine=engine)
    res <- read_xml(hocr)
    write_xml(res, xml_fn)
    res
}

xtract <- function(pg) {
    words <- xml_find_all(pg, "//span[@class='ocrx_word']")
    lines <- map(words, xml_parent)
    paragraphs <- map(lines, xml_parent)
    blocks <- map(paragraphs, xml_parent)
    tibble(
        page_id      = xml_attr(pg, "id"),
        block_id     = map_chr(blocks, xml_attr, "id"),
        paragraph_id = map_chr(paragraphs, xml_attr, "id"),
        line_id      = map_chr(lines, xml_attr, "id"),
        word_id      = map_chr(words, xml_attr, "id"),
        loc_conf     = map_chr(words, xml_attr, "title"),
        text         = xml_text(words)
    )
}

process_pdf <- function(filename) {
    doc <- here::here(filename)
    n_pages <- pdf_length(doc)
    eng <- tesseract(language = "eng",
                     options  = list(tessedit_pageseg_mode = 1),
                     cache    = TRUE)
    foil <- map(seq_len(n_pages), ~ocr_cached(doc, pageno=., engine=eng) )
    pages <- map(foil, xml_find_all, "/div[@class='ocr_page']")
    flat_pages <- map_dfr(pages, xtract, .id = "page_no")
    flat_pages %>%
        mutate(bbox = str_extract(loc_conf, "bbox ([0-9]{1,4}(\\s|;)){4}"),
               bbox = str_replace_all(bbox, "(bbox)|;", "") %>% str_trim,
               conf = str_extract(loc_conf, "[0-9]+$")) %>%
    separate(bbox, into = c("x0", "y0", "x1", "y1"), sep = "\\s+") %>%
    mutate(across(ends_with("_id"), ~str_extract(., "[0-9]+$"))) %>%
    select(-loc_conf) %>%
    mutate_at(vars(-text), as.integer)
}

####

index <- read_delim(args$index, delim="|",
                    col_types = cols(.default=col_character()))
if (!dir.exists(args$xmldir)) dir.create(args$xmldir)

out <- index %>%
    mutate(hocr = map(rpt_location, process_pdf)) %>%
    transmute(rpt_id, rpt_sha1, filename=basename(rpt_location), hocr) %>%
    unnest(hocr) %>%
    mutate(dpi = OCR_DPI)

# consistency
out <- out %>%
    rename(docid=rpt_id, doc_sha1=rpt_sha1, docpg=page_no) %>%
    mutate(pg_type = ifelse(docpg == 1, "front", "continuation"))

write_feather(out, args$output)

# done.

