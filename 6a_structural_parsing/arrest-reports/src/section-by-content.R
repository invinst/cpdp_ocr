# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/src/section-by-content.R

library(pacman)
pacman::p_load(argparse, dplyr, feather, readr, stringr, tidyr, rlang, purrr)

parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--hints")
parser$add_argument("--output")
args <- parser$parse_args()

####

match_hint <- function(words, hint) {
    flags <- map(hint, ~str_detect(words, .))
    reduce(flags, `&`)
}

####

docs <- read_feather(args$input)

hints <- read_delim(args$hints, delim = "|", col_types = 'ccc') %>%
    filter(!is.na(hints)) %>%
    mutate(hints = str_split(hints, ";"))

####

arrest_reports <- filter(docs, page_classification == "ARREST Report") %>%
    select(pdf_id, page_num, contains("_bound"), text,
           block_num, par_num, line_num, word_num)

arrest_reports <- arrest_reports %>%
    arrange(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
    group_by(pdf_id, page_num, block_num, par_num, line_num) %>%
    summarise(text = paste(text, collapse = " "),
              line_top = min(top_bound),
              line_bottom = max(top_bound + height_bound),
              .groups = "drop")

hints_to_match <- set_names(hints$hints, hints$section) %>%
    set_names(~paste0("sec_", .))

section_matches <- map_dfc(hints_to_match,
                           ~match_hint(arrest_reports$text, .))

matched_sections <- arrest_reports %>%
    bind_cols(section_matches) %>%
    pivot_longer(starts_with("sec_"), names_to = "section", values_to = "matched") %>%
    filter(matched) %>%
    select(pdf_id, page_num, block_num, par_num, line_num, section) %>%
    mutate(section = str_replace(section, "^sec_", ""))

write_feather(matched_sections, args$output)

# done.
