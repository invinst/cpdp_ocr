# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# load libs {{{
pacman::p_load(
    argparse,
    arrow,
    digest,
    dplyr,
    magick,
    pdftools,
    purrr,
    stringr,
    tesseract,
    tidyr,
    xml2
)
# }}}

# {{{ command-line args
parser <- ArgumentParser()
parser$add_argument("--files")
parser$add_argument("--xmldir")
parser$add_argument("--DPI", type="integer", default = 300L)
parser$add_argument("--output")
args <- parser$parse_args()
#}}}

# functions for pdf -> bounding box data frame {{{
stub <- function(longid) str_sub(longid, 1, 7)

pdf2xml <- function(filename, engine, dpi=args$DPI, xmldir=args$xmldir) {
    hash <- digest::digest(filename, algo="sha1", file=TRUE)
    fileid <- stub(hash)
    filedir <- paste0(xmldir, "/", hash)
    if (!dir.exists(filedir)) dir.create(filedir, recursive=TRUE)
    n_pages <- pdf_length(filename)
    xmls <- map(seq_len(n_pages),
                ~pg2xml(filename, pg=., engine=engine, dpi=dpi,
                        filedir=filedir))
    map_dfr(xmls, xml2df, .id="pg") %>%
        mutate(fileid=stub(hash), filesha1=hash, dpi=dpi) %>%
        select(fileid, filesha1, everything())
}

process_image <- function(img) {
    # note that tesseract already handles some basic stuff
    # (https://tesseract-ocr.github.io/tessdoc/ImproveQuality.html),
    # this is mainly to remove lines etc. from forms
    img %>%
        image_deskew %>%
        image_convert(type='grayscale') %>%
        image_contrast(sharpen=3) %>%
        image_negate %>%
        image_morphology(method='Thinning', kernel='Rectangle:25x25+0+0') %>%
        image_morphology(method='Thinning', kernel='Rectangle:40x1+0+0<') %>%
        image_negate
}

pg2xml <- function(filename, pg, engine, dpi, filedir) {
    outname <- sprintf("%04i", pg)
    outpath <- paste0(filedir, "/", outname, ".xml")
    if (file.exists(outpath)) return(read_xml(outpath))
    img <- image_read_pdf(filename, pages=pg, density=dpi)
    hocr <- img %>%
        process_image %>%
        ocr(engine=engine, HOCR=TRUE) %>%
        read_xml
    write_xml(hocr, outpath)
    hocr
}

hocr_elements <- function(xml) {
    words <- xml_find_all(xml, "//span[@class='ocrx_word']")
    lines <- map(words, xml_parent)
    paragraphs <- map(lines, xml_parent)
    blocks <- map(paragraphs, xml_parent)
    tibble(
        page_id   = xml_attr(xml, "id"),
        block_id  = map_chr(blocks, xml_attr, "id"),
        par_id    = map_chr(paragraphs, xml_attr, "id"),
        line_id   = map_chr(lines, xml_attr, "id"),
        line_info = map_chr(lines, xml_attr, "title"),
        word_id   = map_chr(words, xml_attr, "id"),
        loc_conf  = map_chr(words, xml_attr, "title"),
        text      = xml_text(words)
    )
}

xtract <- function(strings, elname) {
    pat <- paste0("(^|\\s+)", elname, "([^;]+)(;|$)")
    str_trim(str_match(strings, pat)[,3])
}

xml2df <- function(xml) {
    hocr_elements(xml) %>%
        mutate(bbox = xtract(loc_conf, 'bbox'),
               conf = xtract(loc_conf, 'x_wconf'),
               sz   = xtract(line_info, 'x_size')) %>%
        separate(bbox, into = c("x0", "y0", "x1", "y1"), sep = "\\s+") %>%
        mutate(across(ends_with("_id"), ~str_extract(., "[0-9]+$"))) %>%
        select(-page_id, -loc_conf, -line_info) %>%
        mutate(across(ends_with("_id"), as.integer),
               across(c(x0, y0, x1, y1), as.integer),
               across(c(conf, sz), as.numeric))
}
# }}}

engine <- tesseract('eng', options=list(tessedit_pageseg_mode=6), cache=TRUE)

files <- readLines(args$files)
cpd <- map_dfr(files, pdf2xml, engine=engine,
               dpi=args$DPI, xmldir=args$xmldir)

write_parquet(cpd, args$output)
