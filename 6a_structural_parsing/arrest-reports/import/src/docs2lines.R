# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/import/src/docs2lines.R

library(pacman)
pacman::p_load(argparse, feather, dplyr, tidyr)

parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--output")
args <- parser$parse_args()

arrest_reports <- read_feather(args$input) %>%
    filter(page_classification == "ARREST Report")

# collapse to one row per line
arr_lines <- arrest_reports %>%
    mutate(top_bound = ifelse(top_bound <= 0, NA, top_bound)) %>%
    arrange(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
    group_by(pdf_id, filename, page_num, block_num, par_num, line_num) %>%
    summarise(text = paste(text, collapse = " "),
              line_top = min(top_bound, na.rm = TRUE),
              line_bottom = max(top_bound + height_bound, na.rm = TRUE),
              line_left = min(left_bound),
              line_right = max(left_bound + width_bound),
              .groups = "drop") %>%
    mutate(line_top = ifelse(line_top < 0, 0, line_top))

# ad hoc: docid to distinguish between documents that are in the same pdf
page2docid <- arr_lines %>%
    distinct(pdf_id, page_num) %>%
    arrange(pdf_id, page_num) %>%
    group_by(pdf_id) %>%
    mutate(last_page = lag(page_num, 1)) %>%
    replace_na(list(last_page = 0)) %>%
    mutate(new_document = case_when(
            last_page == 0 ~ TRUE,
            page_num > last_page + 1 ~ TRUE,
            TRUE ~ FALSE
        )) %>%
    ungroup %>%
    mutate(docid = cumsum(new_document)) %>%
    select(-new_document, -last_page)

out <- arr_lines %>%
    inner_join(page2docid, by = c("pdf_id", "page_num")) %>%
    select(docid, everything())

stopifnot(nrow(out) == nrow(arr_lines))

write_feather(out, args$output)

# done.
