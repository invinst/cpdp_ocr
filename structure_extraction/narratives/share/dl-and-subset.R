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
parser$add_argument("--pageclasses", default="input/CPD-44.112-A_samples.csv")
parser$add_argument("--category", default="CPD-44.112-A")
parser$add_argument("--token", default="frozen/auth-token.rds")
parser$add_argument("--outdir")
args <- parser$parse_args()
# }}}

# load data {{{
tok <- readRDS(args$token)

pageclasses <- read_delim(args$pageclasses, delim=",",
                          col_types = cols(.default=col_character(),
                                           page=col_integer(),
                                           last_page=col_integer()))
# }}}

# dl from dropbox {{{
download <- function(dat) {
    filename <- dat[["input"]]
    output   <- dat[["output"]]
    if (file.exists(output)) return(TRUE)
    search_res <- drop_search(filename, dtoken=tok)
    stopifnot(length(search_res$matches) == 1)
    search_res <- search_res$matches[[1]]
    drop_download(search_res$metadata$path_display, local_path=output)
}

relevant <- pageclasses %>%
    filter(category==args$category) %>%
    mutate(output_loc = paste0(args$outdir, "/fullpdfs/", filename)) %>%
    rename(input=filename, page_from=page, page_to=last_page, output=output_loc)

downloaded <- relevant %>%
    distinct(input, output) %>%
    mutate(args = pmap(., list)) %>%
    mutate(result = map(args, safely(download)))

downloaded <- downloaded %>%
    filter(map_lgl(result, ~is.null(.$error)))
# }}}

# produce subsetted pdfs with just relevant pages {{{
psub <- function(input, pages, output) {
    if (file.exists(output)) return(output)
    pdf_subset(input=input, pages=pages, output=output)
}

subsetted <- relevant %>%
    rename(filename=input, full=output) %>%
    filter(full %in% downloaded$output) %>%
    group_by(filename) %>%
    mutate(rpt_num = paste(sprintf("%04d", page_from),
                           sprintf("%04d", page_to),
                           sep="-")) %>%
    ungroup %>%
    mutate(sub_name = paste0(args$outdir, "/subsets/", file_path_sans_ext(filename),
                             "-",  rpt_num, ".pdf")) %>%
    mutate(pagerange = map2(page_from, page_to, ~seq(from=.x, to=.y, by=1L))) %>%
    select(input=full, pages=pagerange, output=sub_name) %>%
    mutate(done=pmap(., safely(psub)))
# }}}

dictionary <- subsetted %>%
    filter(map_lgl(done, ~is.null(.$error))) %>%
    select(filename=output) %>%
    mutate(filesha1=map_chr(filename, digest, algo="sha1", file=TRUE))

dictname <- paste0(args$outdir, "/metadata.csv")
write_delim(dictionary, dictname, delim="|")

# done.
