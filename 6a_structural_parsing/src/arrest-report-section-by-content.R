# vim: set ts=4 softtabstop=0 expandtab sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/src/content-positions.R

library(pacman)
pacman::p_load(argparse, dplyr, feather, readr, stringr, tidyr, rlang, purrr)

parser <- ArgumentParser()
parser$add_argument("--input",
                    default = "output/page-data.feather")
parser$add_argument("--hints",
                    default = "hand/arrest-report-section-hints.csv")
parser$add_argument("--output", default = "output/arrest-report-content-sections.feather")
args <- parser$parse_args()

####

match_hint <- function(words, hint) {
    flags <- map(hint, ~str_detect(words, .))
    reduce(flags, `&`)
}

sec_runs <- function(data) {
    arrange(data, line_top, block_num, par_num, line_num) %>%
        mutate_at(vars(starts_with("sec")), cumsum)
}

####

docs <- read_feather(args$input)

hints <- read_delim(args$hints, delim = "|", col_types = 'ccc') %>%
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

section_flags <- arrest_reports %>%
    bind_cols(section_matches) %>%
    arrange(pdf_id, page_num, line_top, block_num, par_num, line_num) %>%
    group_by(pdf_id, page_num) %>%
    mutate_at(vars(starts_with("sec")), cumsum) %>%
    ungroup

flag_totals <- section_flags %>%
    select(starts_with("sec")) %>%
    reduce(`+`)

output <- section_flags %>%
    mutate(section_num = flag_totals) %>%
    group_by(pdf_id, page_num, section_num) %>%
    mutate(section_top = min(line_top), section_bottom = max(line_bottom)) %>%
    ungroup %>%
    pivot_longer(starts_with("sec_"), names_to = "sec", values_to = "val") %>%
    filter(val > 0) %>%
    group_by(pdf_id, page_num, sec) %>%
    summarise(section_number = min(section_num),
              section_top = min(section_top),
              section_bottom = min(section_bottom),
              .groups = "drop") %>%
    arrange(pdf_id, page_num, section_number) %>%
    transmute(pdf_id, page_num, section_name = str_replace(sec, "^sec_", ""),
              top = section_top, bottom = section_bottom)

write_feather(output, args$output)

# done.
