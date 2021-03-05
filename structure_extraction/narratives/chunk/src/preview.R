# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# libs {{{
pacman::p_load(
    argparse,
    arrow,
    dplyr,
    magick,
    purrr,
    stringr,
    tidyr
)
# }}}

# args {{{
parser <- ArgumentParser()
parser$add_argument("--input", default = "output/segmented.parquet")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

# fn to add rectangles to page image {{{
add_rects <- function(img, rectangles) {
    ht <- image_info(img)$height
    plot(img)
    if (length(rectangles) <= 0) return(TRUE)
    for (r in rectangles)
        rect(xleft = r$x0,
             xright = r$x1,
             ybottom = ht - r$y0,
             ytop = ht - r$y1,
        col = NA, border='red')
    TRUE
}
# }}}

# load data {{{
cpd <- read_parquet(args$input)
meta <- readr::read_delim("../ocr/output/cpd-info.csv", delim="|",
                          col_names=c("filesha1", "filename") ,col_types='cc')
meta <- meta %>% mutate(doctype = str_split(filename, "/") %>% map_chr(2))
# }}}

samples <- meta %>%
    nest(data=-doctype) %>%
    mutate(data=map(data, sample_n, 2)) %>%
    unnest(data) %>%
    mutate(filename=str_replace(filename, "^input", "../ocr/input"))

imgs <- samples %>% inner_join(cpd, by=c("filesha1")) %>%
    mutate(pg=as.integer(pg)) %>%
    distinct(fileid, filename, doctype, pg) %>%
    mutate(img = map2(filename, pg, image_read_pdf, density=300))

rects <- samples %>% inner_join(cpd, by=c("filesha1")) %>%
    mutate(pg=as.integer(pg)) %>%
    distinct(fileid, doctype, pg, chunkid,
             x0=chunk.x0, x1=chunk.x1,
             y0=chunk.y0, y1=chunk.y1) %>%
    nest(rect=c(-fileid, -pg)) %>%
    mutate(rect = map(rect, ~pmap(., list)))

bloop <- imgs %>% inner_join(rects, by=c("fileid", "pg"))

pdf("output/test.pdf", height = 11, width = 8.5, compress = TRUE)
par(mar = c(0, 0, 0, 0))
success <- map2_lgl(bloop$img, bloop$rect, add_rects)
dev.off()
# }}}

