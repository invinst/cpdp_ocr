# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/import/src/import-training-data.R

library(pacman)
pacman::p_load(argparse, dplyr, feather, readr, purrr, stringr, logger, tidyr)

parser <- ArgumentParser()
parser$add_argument("--info")
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

file_info <- read_delim(args$info, delim = "|",
                        col_types = 'cc')

read_training_data <- function(filename, delimiter) {
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
    td <- read_delim(filename,
                     delim = delimiter, na = "",
                     col_types = column_specification)
    if ("section" %in% names(td)) {
        log_info(paste(rep("=", 50), collapse = ""))
        log_info("performance on training batch ", filename)
        smry <- td %>%
            filter(line_left >= 110) %>%
            mutate(correct = section == label) %>%
            group_by(label) %>%
            summarise(correct = sum(correct),
                      total = n(),
                      .groups = "drop") %>%
            transmute(label, correct = paste(correct, total, sep = "/")) %>%
            mutate_all(as.character)
        walk2(smry$label, smry$correct,
              ~log_info(.x, ": ", .y))
        log_info(paste(rep("=", 50), collapse = ""))
    }
    return(td)
}

log_info("starting import")
labs <- map2(file_info$filename, file_info$delimiter,
             read_training_data) %>%
    map_dfr(select, pdf_id, filename, docid,
            ends_with("_num"), starts_with("line_"),
            text, label) %>%
    # was originally trying to separate sections of header, but no longer
    mutate(label = ifelse(str_detect(label, "^header"), "header", label))

probs <- labs %>%
    group_by(pdf_id, page_num, block_num, par_num, line_num) %>%
    filter(n() > 1)

stopifnot(setequal(labs$label, POSS_LABELS))
stopifnot(nrow(probs) == 0)
log_info("successfully imported")

out <- labs %>%
    mutate(label = factor(label, levels = POSS_LABELS)) %>%
    arrange(pdf_id, docid, page_num) %>%
    group_by(pdf_id, docid) %>%
    mutate(newdoc = page_num > lag(page_num) + 1,
           newdoc = tidyr::replace_na(newdoc, TRUE)) %>%
    ungroup %>%
    mutate(docid = cumsum(newdoc)) %>%
    select(-newdoc)

outsmry <- out %>%
    distinct(docid, label) %>%
    group_by(label) %>%
    summarise(n_docs = n_distinct(docid), .groups = "drop") %>%
    arrange(desc(n_docs))

log_info(nrow(out), " rows imported")
log_info(nrow(distinct(out, filename)), " pdfs")
log_info(nrow(distinct(out, docid)), " documents")
log_info("documents with each label:")
walk2(as.character(outsmry$label),
      outsmry$n_docs,
      ~log_info(str_pad(.x, width = 25, side = "left", pad = " "), ": ", .y))

write_feather(out, args$output)

# done.
