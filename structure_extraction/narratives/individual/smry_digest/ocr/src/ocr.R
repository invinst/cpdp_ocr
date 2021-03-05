# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# load libs {{{
pacman::p_load(
    argparse,
    arrow,
    digest,
    dplyr,
    here,
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
# }}}

# command line args {{{
parser <- ArgumentParser()
parser$add_argument("--inputs")
parser$add_argument("--xmldir", default="output/xml300")
parser$add_argument("--DPI", type="integer", default=300)
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

# setup etc. {{{
# input_files <- list.files("../download-pdfs/output/subsets", full.names=T)
input_files <- str_split(args$inputs, "\\|")[[1]] %>%
    purrr::keep(~str_length(.) > 0)

OCR_DPI <- args$DPI
log_info("DPI: ", OCR_DPI)
stub <- function(hash) str_sub(hash, 1, 7)
# }}}

# ocr code {{{
ocr_cached <- function(filename, pageno, engine, DPI, xmldir) {
    xml_fn <- sprintf("%04i", pageno)
    xml_fn <- paste0(xmldir, "/", xml_fn, ".xml")
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

process_pdf <- function(doc, xmldir, DPI=OCR_DPI) {
    n_pages <- pdf_length(doc)
    hash <- digest::digest(doc, file=TRUE, algo="sha1")
    fileid <- stub(hash)
    filedir <- paste0(xmldir, "/", hash)
    if (!dir.exists(filedir)) dir.create(filedir, recursive=TRUE)
    eng <- tesseract(language = "eng",
                     options  = list(tessedit_pageseg_mode = 6),
                     cache    = TRUE)
    ocrd <- map(seq_len(n_pages),
                ~ocr_cached(doc, pageno=., engine=eng,
                            xmldir=filedir, DPI=DPI) )
    pages <- map(ocrd, xml_find_all, "/div[@class='ocr_page']")
    flat_pages <- map_dfr(pages, xtract, .id = "page_no")
    flat_pages %>%
        mutate(bbox = str_extract(loc_conf, "bbox ([0-9]{1,4}(\\s|;)){4}"),
               bbox = str_replace_all(bbox, "(bbox)|;", "") %>% str_trim,
               conf = str_extract(loc_conf, "[0-9]+$")) %>%
    separate(bbox, into = c("x0", "y0", "x1", "y1"), sep = "\\s+") %>%
    mutate(across(ends_with("_id"), ~str_extract(., "[0-9]+$"))) %>%
    select(-loc_conf) %>%
    mutate_at(vars(-text), as.integer) %>% select(-page_id) %>%
    mutate(filename=basename(doc), fileid=fileid) %>%
    select(fileid, filename, everything())
}
# }}}

log_info("starting import (using cached xml if available)")
processed <- map_dfr(input_files, process_pdf, xmldir=args$xmldir, DPI=OCR_DPI)
log_info("finished importing documents")

out <- processed %>%
    mutate(x0=x0/OCR_DPI, x1=x1/OCR_DPI, y0=y0/OCR_DPI, y1=y1/OCR_DPI)

write_parquet(out, args$output)

# done.
