# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/features/src/extract-headings.R

library(pacman)
pacman::p_load(argparse, feather, dplyr, rlang,
               tidyr, purrr, stringr, stringdist)

parser <- ArgumentParser()
parser$add_argument("--input", default = "input/arrest-reports-lines.feather")
parser$add_argument("--output")
args <- parser$parse_args()

####

KNOWN_SECTIONS <- c(
    reporting_personnel      = "REPORTING PERSONNEL",
    lockup_keeper_processing = "LOCKUP KEEPER PROCESSING",
    visitor_log              = "VISITOR LOG",
    movement_log             = "MOVEMENT LOG",
    wc_comments              = "WC COMMENTS",
    processing_personnel     = "PROCESSING PERSONNEL",
    recovered_narcotics      = "RECOVERED NARCOTICS",
    warrant                  = "WARRANT",
    offender                 = "OFFENDER",
    non_offenders            = "NON-OFFENDER(S)",
    arrestee_vehicle         = "ARRESTEE VEHICLE",
    properties               = "PROPERTIES",
    incident_narrative       = "INCIDENT NARRATIVE",
    incident                 = "INCIDENT",
    court_info               = "COURT INFO",
    interview_log            = "INTERVIEW LOG",
    charges                  = "CHARGES",
    felony_review            = "FELONY REVIEW"
)

### functions ##

# finds vertically oriented text on the left margin of the page
is_heading <- function(height, width, left) {
    height > width & left + width < 110
}

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
    candidates <- clarify(observed$text)
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
        transmute(observed, heading = candidate)
    observed %>% mutate(matchable = cleanup_header(text)) %>%
        inner_join(dict, by = c(matchable = "observed")) %>%
        distinct(pdf_id, page_num, heading, lab_top, lab_bottom)
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
    summarise(text  = paste(text, collapse = " "),
              lab_top     = min(lab_top),
              lab_bottom  = max(lab_bottom),
              lab_left    = min(lab_left),
              lab_right   = max(lab_right),
              .groups = "drop")
}

###

arrest_reports <- read_feather(args$input)

heading_candidates <- arrest_reports %>%
    filter(is_heading(height = line_bottom - line_top,
                      width  = line_right - line_left,
                      left   = line_left)) %>%
    mutate(lab_top = line_top, lab_bottom = line_bottom,
           lab_left = line_left, lab_right = line_right)

while(has_overlaps(heading_candidates)) {
    heading_candidates <- combine_overlapping(heading_candidates)
}

heading_candidates <- filter(heading_candidates, lab_bottom - lab_top > 100)

arrest_report_sections <- best_match(heading_candidates, KNOWN_SECTIONS)

check <- arrest_report_sections %>%
    count(pdf_id, page_num, lab_top) %>%
    pluck("n") %>% unique
stopifnot(check == 1L)

out <- arrest_reports %>%
    left_join(arrest_report_sections, by = c("pdf_id", "page_num")) %>%
    filter(line_top >= lab_top, line_bottom <= lab_bottom,
           line_top < Inf, line_bottom > -Inf) %>%
    distinct(filename, pdf_id, page_num,
             block_num, par_num, line_num, heading)

exceptions <- out %>%
    group_by(pdf_id, filename, page_num, block_num, par_num, line_num) %>%
    filter(n() > 1)

stopifnot(nrow(exceptions) == 0)

write_feather(out, args$output)

# done.
