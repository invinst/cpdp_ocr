# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/src/arr-rpt.R

library(pacman)
pacman::p_load(argparse, dplyr, feather, stringr)

parser <- ArgumentParser()
parser$add_argument("--positional",
                    default = "output/arrest-report-position-sections.feather")
parser$add_argument("--contextual",
                    default = "output/arrest-report-content-sections.feather")
parser$add_argument("--docs",
                    default = "output/page-data.feather")
args <- parser$parse_args()

pos <- read_feather(args$positional)
con <- read_feather(args$contextual)

docs <- read_feather(args$docs)

ar_lines <- docs %>%
    filter(page_classification == "ARREST Report") %>%
    filter(top_bound > 0, left_bound > 0, width_bound < 1500) %>%
    mutate(text = str_replace_all(text, "(\\n[0-9]*)|(\\t[0-9]*)", " "),
           text = str_squish(text)) %>%
    arrange(pdf_id, filename, dropbox_path,
            page_num, block_num, par_num, line_num, word_num) %>%
    group_by(pdf_id, filename, dropbox_path, page_num, block_num, par_num, line_num) %>%
    summarise(text = paste(text, collapse = " "),
              line_top = min(top_bound),
              line_bottom = max(top_bound + height_bound),
              line_left = min(left_bound),
              line_right = max(left_bound + width_bound),
              .groups = "drop")

position_matched <- ar_lines %>%
    inner_join(pos, by = c("pdf_id", "page_num")) %>%
    filter(line_top >= top, line_bottom <= bottom) %>%
    select(pdf_id, page_num, block_num, par_num, line_num, section_name)

context_matched <- ar_lines %>%
    inner_join(con, by = c("pdf_id", "page_num")) %>%
    filter(line_top >= top, line_bottom <= bottom) %>%
    select(pdf_id, page_num, block_num, par_num, line_num, section_name)

training <- sample(unique(ar_lines$pdf_id), 15)
docs %>% filter(pdf_id == 20606) %>% distinct(filename, dropbox_path)

ar_lines %>%
    filter(pdf_id %in% training) %>%
    #     filter(pdf_id == 12280) %>%
    select(-filename, -dropbox_path) %>%
    left_join(rename(position_matched, position_section = section_name),
              by = c("pdf_id", "page_num", "block_num",
                     "par_num", "line_num")) %>%
    left_join(rename(context_matched, context_section = section_name),
              by = c("pdf_id", "page_num", "block_num",
                     "par_num", "line_num")) %>%
    arrange(pdf_id, page_num, line_top, block_num, par_num, line_num) %>%
    mutate(position = NA_character_) %>%
    write_excel_csv("hand/arr-rpt-training-data.csv", na = "", delim = "|")
