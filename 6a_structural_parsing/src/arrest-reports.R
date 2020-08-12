# vim: set ts=4 softtabstop=0 expandtab sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/src/arrest-reports.R

library(pacman)
pacman::p_load(argparse, feather, dplyr, rlang, janitor,
               tidyr, yaml, purrr, stringr, stringdist)


parser <- ArgumentParser()
parser$add_argument("--input", default = "output/page-data.feather")
parser$add_argument("--sections", default = "hand/arrest-report-sections.yaml")
parser$add_argument("--exceptions", default = "output/position-exceptions.feather")
parser$add_argument("--output", default = "output/position-sections.feather")
args <- parser$parse_args()

####

docs <- read_feather(args$input)

known_sections <- read_yaml(args$sections) %>%
    set_names(janitor::make_clean_names)

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
        left_join(dict, by = c(matchable = "observed")) %>%
        mutate(section_name = ifelse(is.na(section_name),
                                     paste0("UNKNOWN (", matchable, ")"),
                                     section_name))
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
    #     mutate(bottom = bottom_of(top_bound, height_bound),
    #            right  = extent_of(left_bound, width_bound)) %>%
    #     select(pdf_id, page_num, contains("_bound"), text,
    #            bottom, right,
    #            block_num, par_num, line_num, word_num) %>%
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


arr_rpt <- arrest_reports %>%
    #     filter(pdf_id == 10437, page_num == 2, block_num == 32) %>%
    select(pdf_id, page_num, contains("bound"), text,
           block_num, par_num, line_num, word_num) %>%
    left_join(arrest_report_sections, by = c("pdf_id", "page_num")) %>%
    mutate(in_range = top_bound > lab_top &
               bottom_of(top_bound, height_bound) < lab_bottom &
               left_bound > lab_right,
           is_header = top_bound >= lab_top &
               bottom_of(top_bound, height_bound) <= lab_bottom &
               left_bound <= lab_right) %>%
    group_by(pdf_id, page_num, block_num, par_num, line_num, matchable) %>%
    mutate(in_range = any(in_range)) %>%
    ungroup %>%
    mutate(section_name = case_when(
               in_range ~ section_name,
               is_header ~ "HEADING",
               TRUE ~ NA_character_
           )) %>%
    group_by(pdf_id, page_num, block_num, par_num, line_num) %>%
    mutate(unmatched = is.na(max(section_name, na.rm = TRUE))) %>%
    #     arrange(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
    #     print(n = Inf)
    filter(in_range | unmatched | is_header) %>%
    ungroup %>%
    select(pdf_id, page_num,
           block_num, par_num, line_num, word_num,
           left_bound, top_bound, width_bound, height_bound,
           text, section_name) %>%
    distinct

ambiguous_section_assignments <- arr_rpt %>%
    group_by(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
    filter(n() > 1) %>%
    ungroup

stopifnot(nrow(ambiguous_section_assignments) == 0)

exceptions <- arrest_reports %>%
    anti_join(arr_rpt, by = c("pdf_id", "page_num", "block_num",
                              "par_num", "line_num", "word_num")) %>%
    select(pdf_id, page_num, block_num, par_num, line_num, word_num,
           left_bound, top_bound, text)

write_feather(exceptions, args$exceptions)
write_feather(arr_rpt, args$output)

    # incident_narrative|start|"facts,probable,cause,substantiate,limited"
    # court_info|start|"^Desired Court Date"
    # lockup_keeper_processing|start|"^Holding Facility"
    # properties|start|"^Confiscated Properties"
    # incident|start|"^Arrest"



# arr_rpt %>%
#     filter(is.na(section_name) | section_name != "HEADING") %>%
#     mutate(has_facts = str_detect(text, "facts"),
#            has_probable = str_detect(text, "probable"),
#            has_cause = str_detect(text, "cause"),
#            has_limited = str_detect(text, "limited"),
#            has_substantiate = str_detect(text, "substantiate")) %>%
#     group_by(pdf_id, page_num, block_num, par_num, line_num) %>%
#     summarise(has_all = max(has_facts) * max(has_probable) * max(has_cause) * max(has_limited) * max(has_substantiate) > 0,
#               section_name = max(section_name, na.rm = TRUE),
#               top = min(top_bound),
#               .groups = "drop") %>%
#     arrange(pdf_id, page_num, block_num, par_num, line_num, top) %>%
#     group_by(pdf_id, page_num) %>%
#     mutate(section_name = case_when(
#                has_all ~ "incident_narrative",
#                is.na(section_name) & lag(has_all, 1) ~ "incident_narrative",
#                TRUE ~ section_name)) %>%
#     fill(section_name, .direction = "down") %>%
#     ungroup
# 
#     filter(page_num == 3, block_num == 2) %>%
#     pluck("text") %>% str_split("\n") %>% pluck(1) %>% str_split("\t") %>%
#     head
# 
#     arrange(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
#     group_by(pdf_id, page_num, section_name, block_num, par_num, line_num, has_all) %>%
#     summarise(text = paste(text, collapse = " "),
#               top = min(top_bound),
#               .groups = "drop") %>%
#     arrange(top) %>%
#     print(n = Inf)
# 
# arr_rpt %>%
#     filter(pdf_id == 348, page_num == 2) %>%
#     mutate(text = str_replace_all(text, "(\t|\n)[0-9]*", " ") %>% str_squish) %>%
#     print(n = Inf)
#     mutate(has_facts = str_detect(text, "facts"),
#            has_probable = str_detect(text, "probable"),
#            has_cause = str_detect(text, "cause"),
#            has_limited = str_detect(text, "limited"),
#            has_substantiate = str_detect(text, "substantiate")) %>%
#     group_by(pdf_id, page_num, block_num, par_num, line_num) %>%
#     mutate(has_all = max(has_facts) * max(has_probable) * max(has_cause) * max(has_limited) * max(has_substantiate) > 0) %>%
#     filter(has_all & !is.na(section_name)) %>% group_by(section_name) %>% summarise(n = n_distinct(pdf_id))
#     filter(section_name == "incident_narrative" | has_all) %>%
#     print(n = 50)
# group_by(section_name) %>%
#     summarise(n = n_distinct(pdf_id)) %>%
#     arrange(desc(n)) %>% print(n = 50)
#     filter(section_name == "UNKNOWN (= = E z)") %>%
#     print(n = 50)
# 
# arr_rpt %>%
#     filter(pdf_id == 21, page_num == 4) %>%
#     arrange(pdf_id, page_num, block_num, par_num,
#             line_num, word_num, top_bound, left_bound) %>%
#     group_by(pdf_id, page_num, section_name, block_num, par_num, line_num) %>%
#     summarise(text = paste(text, collapse = " "),
#               top = min(top_bound),
#               .groups = "drop") %>%
#     arrange(pdf_id, page_num, top) %>%
#     print(n = Inf)
# 
# 
# arr_rpt %>%
#     arrange(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
#     select(pdf_id, page_num,
#            block_num, par_num, line_num, word_num,
#            left_bound, top_bound, width_bound, height_bound,
#            text, section_name) %>%
#     filter(pdf_id == 21, page_num == 4) %>%
#     print(n = Inf)
# 
#     left_join(arr_rpt %>%
#                   select(pdf_id, page_num, section_name, block_num, par_num,
#                          left_bound, top_bound, text),
#               by = c("pdf_id", "page_num")) %>%
#     group_by(pdf_id, page_num) %>%
#     
# 
# arrest_reports %>%
#     select(pdf_id, dropbox_path, filename, page_num,
#            contains("bound"), text,
#            block_num, par_num, line_num, word_num) %>%
#     arrange(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
#     filter(pdf_id == 28, page_num == 2) %>%
#     print(n = Inf)
# 
# 
# arr_rpt %>%
#     arrange(pdf_id, page_num, lvl, block_num, par_num, line_num, word_num) %>%
#     group_by(pdf_id, filename, dropbox_path,
#              page_num, section_name, lvl, block_num, par_num, line_num) %>%
#     summarise(text = paste(text, collapse = " "),
#               top = min(top_bound), left = min(left_bound),
#               bottom = max(bottom), right = max(right),
#               .groups = "drop") %>%
#     arrange(pdf_id, page_num, top)
# 
# ar %>%
#     #     filter(section_name == "incident_narrative", pdf_id == 28, page_num == 2) %>%
#     filter(section_name == "incident_narrative") %>%
#     mutate(text = str_replace_all(text, "(\t|\n)[0-9]*", " "),
#            text = str_squish(text) %>% str_trim) %>%
#     group_by(pdf_id, dropbox_path, filename, page_num, section_name) %>%
#     summarise(text = paste(text, collapse = "\n"),
#               .groups = "drop") %>%
#     #     sample_n(1) %>%
#     #     filter(pdf_id == 28) %>%
#     pluck("text", 1) %>% cat("\n")
# 
# x %>% filter(pdf_id == 19412)
# 
#     filter(is.na(section_name)) %>%
#     group_by()
#     count(header, matchable, sort = TRUE)
# 
# narr_start <- "(The facts for probable cause to arrest AND to substantiate the charges include, but are not limited to, the following)"
# arr_rpt %>%
#     filter(pdf_id == 177, page_num == 2) %>%
#     group_by(block_num, par_num, line_num) %>%
#     summarise(text = paste(text, collapse = " "), .groups = "drop") %>%
#     mutate(bloop = stringdist(text, narr_start, method = "cosine", q = 4)) %>%
#     print(n = 50)
#     #     filter(str_detect(text, ""))
#     filter(pdf_id == 177, page_num == 2) %>%
#     #     print(n = Inf)
#     group_by(block_num, line_num) %>%
#     arrange(pdf_id, page_num, block_num, line_num, word_num) %>%
#     summarise(text = paste(text, collapse = " "), top = min(top_bound),
#               .groups = "drop") %>%
#     arrange(top) %>%
#     print(n = 50)
