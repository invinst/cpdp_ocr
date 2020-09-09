# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/src/section-by-position.R

library(pacman)
pacman::p_load(argparse, feather, dplyr, rlang,
               tidyr, purrr, stringr, stringdist, readr)


parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--sections")
parser$add_argument("--output")
args <- parser$parse_args()

####

docs <- read_feather(args$input)

section_info <- read_delim(args$sections, delim = "|",
                             na = "", col_types = 'ccc') %>%
    filter(!is.na(label))

known_sections <- structure(section_info$label, names = section_info$section)

# finds vertically oriented text on the left margin of the page
is_heading <- function(height, width, left) {
    height > width & left + width < 110
}

bottom_of <- function(top, height) top + height
extent_of <- function(left, width) left + width

cleanup_header <- function(hdr) {
    str_replace_all(hdr, "[^ -~]", "") %>% str_replace_all("[0-9]", "")
}

clarify <- function(raw_data) {
    n_alphas <- str_length(str_replace_all(raw_data, "[^A-Z]", ""))
    is_legible <- n_alphas > 4
    to_match <- raw_data[is_legible]
    result <- unique(cleanup_header(to_match))
    result[str_length(result) <= 50]
}

best_match <- function(observed, expected) {
    candidates <- clarify(observed$header)
    distances <- map(expected,
                     ~stringdist(candidates, ., method = "cosine", q = 4))
    dict <- tibble(observed = candidates,
                   as_tibble(distances)) %>%
        pivot_longer(-observed,
                     names_to = "candidate",
                     values_to = "distance") %>%
        group_by(observed) %>%
        filter(distance <= min(distance),
               distance < .7) %>%
        ungroup %>%
        transmute(observed, section_name = candidate)
    observed %>% mutate(matchable = cleanup_header(header)) %>%
        inner_join(dict, by = c(matchable = "observed")) %>%
        distinct(pdf_id, page_num, section_name, lab_top, lab_bottom)
}

has_overlaps <- function(labs) {
    test <- labs %>%
        arrange(pdf_id, page_num, lab_top) %>%
        group_by(pdf_id, page_num) %>%
        filter(lab_top < lag(lab_bottom, 1))
    nrow(test) > 0
}

combine_overlapping <- function(labs) {
    labs %>%
        arrange(pdf_id, page_num, lab_top) %>%
        group_by(pdf_id, page_num) %>%
        mutate(overlap = lab_top < lag(lab_bottom, 1)) %>%
        replace_na(list(overlap = FALSE)) %>%
        mutate(group_id = cumsum(!overlap)) %>%
        mutate(lab_top = ifelse(overlap, lag(lab_top, 1), lab_top)) %>%
        group_by(pdf_id, page_num, group_id) %>%
    summarise(header  = paste(header, collapse = " "),
              lab_top     = min(lab_top),
              lab_bottom  = max(lab_bottom),
              lab_left    = min(lab_left),
              lab_right   = max(lab_right),
              .groups = "drop")
}

###

arrest_reports <- filter(docs, page_classification == "ARREST Report") %>%
    mutate(bottom = bottom_of(top_bound, height_bound),
           right  = extent_of(left_bound, width_bound)) %>%
    group_by(pdf_id, page_num, block_num, par_num, line_num) %>%
    mutate(line_top = min(top_bound), line_bottom = max(bottom)) %>%
    ungroup %>%
    select(pdf_id, page_num, contains("_bound"), text,
           bottom, right,
           block_num, par_num, line_num, word_num,
           line_top, line_bottom)

observed_labels <- arrest_reports %>%
    arrange(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
    group_by(pdf_id, page_num, block_num, par_num, line_num) %>%
    summarise(header  = paste(text, collapse = " "),
              line_top = min(line_top),
              line_bottom = max(line_bottom),
              line_left    = min(left_bound),
              line_right   = max(right),
              .groups = "drop") %>%
    filter(is_heading(height = line_bottom - line_top,
                      width  = line_right - line_left,
                      left   = line_left)) %>%
    mutate(lab_top = line_top, lab_bottom = line_bottom,
           lab_left = line_left, lab_right = line_right)

while(has_overlaps(observed_labels)) {
    observed_labels <- combine_overlapping(observed_labels)
}

observed_labels <- observed_labels %>%
    filter(lab_bottom - lab_top > 100)

arrest_report_sections <- best_match(observed_labels, known_sections)

check <- arrest_report_sections %>%
    count(pdf_id, page_num, lab_top) %>%
    pluck("n") %>% unique
stopifnot(check == 1L)

out <- arrest_reports %>%
    left_join(arrest_report_sections, by = c("pdf_id", "page_num")) %>%
    filter(line_top >= lab_top, line_bottom <= lab_bottom) %>%
    distinct(pdf_id, page_num,
             block_num, par_num, line_num,
             section = section_name)

exceptions <- out %>%
    group_by(pdf_id, page_num, block_num, par_num, line_num) %>%
    filter(n() > 1)

stopifnot(nrow(exceptions) == 0)

write_feather(out, args$output)

# done.
