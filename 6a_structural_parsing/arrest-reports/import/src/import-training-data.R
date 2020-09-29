# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/import/src/import-training-data.R

library(pacman)
pacman::p_load(argparse, dplyr, feather, readr, purrr, stringr)

parser <- ArgumentParser()
parser$add_argument("--inputdir")
parser$add_argument("--output")
args <- parser$parse_args()

####

POSS_LABELS <- c(
    "arrestee_vehicle", "charges", "court_info", "felony_review",
    "footer", "header", "incident", "incident_narrative", "interview_log",
    "lockup_keeper_processing", "longline", "movement_log", "non_offenders",
    "offender", "processing_personnel", "properties", "recovered_narcotics",
    "reporting_personnel", "visitor_log", "warrant", "wc_comments"
)

####

filenames <- list.files(args$inputdir, full.names = TRUE)

column_specification <- readr::cols(
    .default = col_character(),
    pdf_id = col_integer(),
    filename = col_character(),
    docid = col_integer(),
    page_num = col_integer(),
    block_num = col_integer(),
    par_num = col_integer(),
    line_num = col_integer(),
    line_top = col_integer(),
    line_bottom = col_integer(),
    line_left = col_integer(),
    line_right = col_integer(),
    text = col_character(),
    label = col_character()
)

labs <- map(filenames, read_delim, delim = "|", na = "",
            col_types = column_specification) %>%
    map_dfr(select, pdf_id, filename, docid,
            ends_with("_num"), starts_with("line_"),
            text, label) %>%
    # was originally trying to separate sections of header, but no longer
    mutate(label = ifelse(str_detect(label, "^header"), "header", label))

stopifnot(setequal(labs$label, POSS_LABELS))

out <- labs %>%
    mutate(label = factor(label, levels = POSS_LABELS))

write_feather(out, args$output)

# done.
