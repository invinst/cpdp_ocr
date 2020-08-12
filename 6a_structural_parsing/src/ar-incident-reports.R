# vim: set ts=4 softtabstop=0 expandtab sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/src/ar-incident-reports.R

library(pacman)
pacman::p_load(argparse, dplyr, feather, stringr, assertr, yaml)


parser <- ArgumentParser()
parser$add_argument("--positional",
                    default = "output/arrest-report-position-sections.feather")
parser$add_argument("--contextual",
                    default = "output/arrest-report-content-sections.feather")
parser$add_argument("--docs",
                    default = "output/page-data.feather")
parser$add_argument("--output")
args <- parser$parse_args()

pos <- read_feather(args$positional)
con <- read_feather(args$contextual)

logname <- paste0(tools::file_path_sans_ext(args$output), ".txt")

docs <- read_feather(args$docs)

ar_lines <- docs %>%
    filter(page_classification == "ARREST Report") %>%
    mutate(text = str_replace_all(text, "(\\n[0-9]*)|(\\t[0-9]*)", " "),
           text = str_squish(text)) %>%
    arrange(pdf_id, filename, dropbox_path,
            page_num, block_num, par_num, line_num, word_num, text) %>%
    group_by(pdf_id, filename, dropbox_path, page_num, block_num, par_num, line_num) %>%
    summarise(text = paste(text, collapse = " "),
              line_top = min(top_bound),
              line_bottom = max(top_bound + height_bound),
              .groups = "drop")

incident_narratives <- con %>%
    full_join(pos, by = c("pdf_id", "page_num", "section_name")) %>%
    filter(section_name == "incident_narrative") %>%
    mutate(ok = case_when(
            pdf_id == 19965 & page_num == 15 ~ TRUE,
            pdf_id == 19832 & page_num == 40 ~ TRUE,
            is.na(top.x) | is.na(top.y) ~ TRUE,
            top.x <= top.y ~ TRUE,
            bottom.x >= bottom.y ~ TRUE,
            TRUE ~ FALSE)) %>%
    verify(ok) %>%
    transmute(pdf_id, page_num, section_name,
              section_top    = coalesce(top.x, top.y),
              section_bottom = coalesce(bottom.x, bottom.y),
              context        = !is.na(top.x),
              position       = !is.na(top.y)) %>%
    left_join(ar_lines, by = c("pdf_id", "page_num")) %>%
    filter(section_top <= line_top, section_bottom >= line_bottom) %>%
    arrange(pdf_id, page_num, section_name, line_top) %>%
    group_by(pdf_id, page_num, section_name) %>%
    summarise(narrative = paste(text, collapse = "\n") %>%
                  str_squish %>% str_trim,
              context = max(context),
              position = max(position),
              .groups = "drop")

id_smry <- incident_narratives %>%
    mutate(identified_by = case_when(
            context > 0 & position > 0 ~ "context + header position",
            context > 0 & position <= 0 ~ "context only",
            context <= 0 ~ "header position only",
            TRUE ~ "ERROR")) %>%
    count(identified_by) %>%
    mutate(notes = structure(as.list(n), names = identified_by))

log <- list(
    task = "arrest report: incident narratives",
    output_filename = args$output,
    n_arr_rpt = length(unique(ar_lines$pdf_id)),
    n_incident_reports_scraped = length(unique(incident_narratives$pdf_id)),
    how_identified = id_smry$notes
)

yaml::write_yaml(log, logname)

write_feather(incident_narratives, args$output)

# done.
