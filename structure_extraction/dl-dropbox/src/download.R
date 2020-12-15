# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# load libs {{{
pacman::p_load(
    argparse,
    assertr,
    digest,
    dplyr,
    here,
    pdftools,
    purrr,
    rdrop2,
    readr,
    stringr,
    tidyr,
    tools
)
# }}}

# command-line args {{{
parser <- ArgumentParser()
parser$add_argument("--pageclasses")
parser$add_argument("--type")
parser$add_argument("--token")
parser$add_argument("--outdir")
args <- parser$parse_args()
# }}}

# load data {{{
tok <- readRDS(args$token)

pageclasses <- read_delim(args$pageclasses, delim=",",
                          col_types = cols(.default=col_character(),
                                           page_nr=col_integer(),
                                           confidence=col_double()))
# }}}

# dl from dropbox {{{
download <- function(dat) {
    filename <- dat[["filename"]]
    db_hash  <- dat[["db_hash"]]
    output   <- dat[["output"]]
    if (file.exists(output)) return(TRUE)
    search_res <- drop_search(filename, dtoken=tok)
    stopifnot(length(search_res$matches) == 1)
    search_res <- search_res$matches[[1]]
    hash <- search_res$metadata$content_hash
    stopifnot("dbox hash must match reference" = hash == db_hash)
    drop_download(search_res$metadata$path_display, local_path=output)
}

relevant <- pageclasses %>%
    # don't download if you can't verify it's the same file
    filter(!is.na(dbx_hash)) %>%
    arrange(dbx_hash, page_nr) %>%
    group_by(dbx_hash) %>%
    mutate(page_to = lead(page_nr, 1) - 1L) %>%
    ungroup %>%
    filter(prediction==args$type) %>%
    mutate(output_loc = paste0(args$outdir, "/fullpdfs/", dbx_hash, ".pdf")) %>%
    rename(filename=pdf, db_hash=dbx_hash, output=output_loc)

downloaded <- relevant %>%
    distinct(filename, db_hash, output) %>%
    mutate(args = pmap(., list)) %>%
    mutate(result = map(args, safely(download)))

invisible(downloaded %>% verify(map_lgl(result, "result")))
# }}}

# produce subsetted pdfs with just relevant pages {{{
psub <- function(input, pages, output) {
    if (file.exists(output)) return(output)
    pdf_subset(input=input, pages=pages, output=output)
}

subsetter <- relevant %>%
    mutate(last_page = map_int(output, pdf_length)) %>%
    mutate(page_to = coalesce(page_to, last_page))

subsetter <- subsetter %>%
    rename(full = output) %>%
    group_by(filename, db_hash) %>%
    mutate(rpt_num = rank(page_nr)) %>%
    ungroup %>%
    mutate(sub_name = paste0(args$outdir, "/subsets/", file_path_sans_ext(filename),
                             "-", abbreviate(args$type) ,"-",
                             sprintf("%03d", rpt_num), ".pdf")) %>%
    mutate(pagerange = map2(page_nr, page_to, ~seq(from=.x, to=.y, by=1L))) %>%
    select(input=full, pages=pagerange, output=sub_name)

files_written <- pmap_chr(subsetter, psub)
# }}}

taskpath <- str_replace(getwd(), here::here(), "")

dictionary <- relevant %>%
    select(rpt_type = prediction,
           original_filename = filename,
           batch,
           dbx_hash = db_hash,
           confidence,
           local_filename_full=output) %>%
    inner_join(subsetter, by = c(local_filename_full = "input")) %>%
    rename(local_filename_subset = output) %>%
    mutate(page_from=map_int(pages, min), page_to=map_int(pages, max)) %>%
    select(-pages) %>%
    mutate(rpt_sha1 = map_chr(local_filename_subset, digest,
                              algo="sha1", file=TRUE),
           rpt_id   = substr(rpt_sha1, start=1, stop=8)) %>%
    mutate(rpt_location = paste0(taskpath, "/", local_filename_subset))

dictname <- paste0(args$outdir, "/metadata.csv")
write_delim(dictionary, dictname, delim="|")

 # done.
