# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/import/src/pull-pages.R

library(pacman)
pacman::p_load(argparse, dplyr, dbplyr, DBI, RPostgreSQL,
               readr, stringr, feather)

#####

parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--output")
args <- parser$parse_args()

####

df2db <- function(data, connection, name) {
    copy_to(dest = connection, df = data,
            name = name, overwrite = TRUE)
    tbl(connection, name)
}

dl_pages <- function(pages_to_parse) {
    con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                          dbname = 'cpdp_struct_parse',
                          user = 'tshah')
    on.exit(dbDisconnect(con))
    to_parse <- df2db(pages_to_parse, connection = con, name = "tmp_parse")
    ocr_tokens <- tbl(con, "cr_ocr_tokens")
    page_dict <- tbl(con, "cr_pdfs")
    page_dict %>%
        rename(pdf_id = id) %>%
        inner_join(to_parse, by = c("cr_id", "filename")) %>%
        inner_join(ocr_tokens, by = c("pdf_id", "page_num")) %>%
        collect
}

####

needs_parsing <- read_csv(args$input,
                        col_types = cols(.default = col_character(),
                                         cr_id    = col_double(),
                                         page_num = col_double()))

needs_parsing <- needs_parsing %>%
    distinct(cr_id, filename, dropbox_path,
             page_num, page_classification)

output <- dl_pages(needs_parsing)

output <- output %>%
    mutate(text = stringr::str_trim(text)) %>%
    filter(text != "")

write_feather(output, args$output)

# done.
