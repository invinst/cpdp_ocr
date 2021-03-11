# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# libs {{{
pacman::p_load(
    argparse,
    assertr,
    arrow,
    DBI,
    dbplyr,
    dplyr,
    logger,
    readr,
    RPostgreSQL,
    stringr
)
# }}}

# args {{{
parser <- ArgumentParser()
parser$add_argument("--faces")
parser$add_argument("--smrydigest")
parser$add_argument("--webcomplaint")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

# read input data {{{
build_xref <- function() {
    con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                          dbname = 'cpdp_struct_parse',
                          user = 'tshah')
    on.exit(dbDisconnect(con))
    ref <- tbl(con, "cr_pdfs")
    collect(ref) %>% distinct(filename, ref_crid=cr_id, doccloud_url)
}


smdig <- read_parquet(args$smrydigest)
wbcmp <- read_parquet(args$webcomplaint)
ref <- build_xref()
faces <- readr::read_csv(args$faces,
                         col_types=cols(.default=col_character(),
                                        page_num=col_integer(),
                                        batch_id=col_integer()))
# }}}

# get narratives from face sheets {{{
log_info("read in ", nrow(faces), " face sheet narratives (all types)")

allegation_cols <- c("Initial / Intake Allegation", "Allegation")
face_allegs <- faces %>%
    distinct(cr_id, section_name, column_name,
             pdf_name, page_num, text) %>%
    filter(column_name %in% allegation_cols)

log_info("allegations are those labeled: ",
         paste0("'", allegation_cols, "'", collapse=", "))
log_info("read in ", nrow(face_allegs), " face sheet allegation narratives")

face_narrs <- face_allegs %>%
    left_join(ref, by=c("pdf_name"="filename")) %>%
    verify(nrow(.) == nrow(face_allegs)) %>%
    #     filter(!is.na(doccloud_url)) %>%
    verify(is.na(ref_crid) | ref_crid == cr_id) %>%
    transmute(cr_id,
              filename=pdf_name,
              rpt_type=paste(section_name, column_name, sep=" "),
              rpt_page_from=page_num, rpt_page_to=page_num, doccloud_url, text)

    # log_info("doccloud urls found for ", nrow(face_narrs),
    #          " face sheet allegation narratives")
# }}}

# log summary digest {{{
# by convention, downloaded/ocr'd pdf filenames = orig_filename-XXXX-XXXX.pdf,
# where the X's are the pagenumbers of the original file
log_info("read in ", nrow(smdig), " log summary digest narratives")

smdig_narrs <- smdig %>%
    mutate(orig_file = str_sub(filename, 1, -15),
           orig_file = paste0(orig_file, ".pdf"),
           page_start = str_sub(filename, -13, -10) %>% as.integer,
           page_end = str_sub(filename, -8, -5) %>% as.integer) %>%
    left_join(ref, by=c(orig_file="filename")) %>%
    verify(nrow(.) == nrow(smdig)) %>%
    #     filter(!is.na(doccloud_url)) %>%
    verify(is.na(ref_crid) | ref_crid == cr_id) %>%
    transmute(cr_id, filename=orig_file,
              rpt_type="log_summary_digest",
              rpt_page_from=page_start, rpt_page_to=page_end,
              doccloud_url, text)

    # log_info("doccloud urls found for ", nrow(smdig_narrs),
    #          " log summary digest narratives")
# }}}

# web complaint detail {{{
# by convention, downloaded/ocr'd pdf filenames = orig_filename-XXXX-XXXX.pdf,
# where the X's are the pagenumbers of the original file
log_info("read in ", nrow(wbcmp), " web complaint detail narratives")

wbcmp_narrs <- wbcmp %>%
    mutate(orig_file = str_sub(filename, 1, -15),
           orig_file = paste0(orig_file, ".pdf"),
           page_start = str_sub(filename, -13, -10) %>% as.integer,
           page_end = str_sub(filename, -8, -5) %>% as.integer) %>%
    left_join(ref, by=c(orig_file="filename")) %>%
    verify(nrow(.) == nrow(wbcmp)) %>%
    #     filter(!is.na(doccloud_url)) %>%
    verify(is.na(ref_crid) | ref_crid == cr_id) %>%
    transmute(cr_id,
              filename=orig_file,
              rpt_type="web_complaint_detail",
              rpt_page_from=page_start, rpt_page_to=page_end,
              doccloud_url, text)

    # log_info("doccloud urls found for ", nrow(wbcmp_narrs),
    #          " web complaint detail narratives")
# }}}

out <- bind_rows(face_narrs, smdig_narrs, wbcmp_narrs)

write_parquet(out, args$output)

# done.
