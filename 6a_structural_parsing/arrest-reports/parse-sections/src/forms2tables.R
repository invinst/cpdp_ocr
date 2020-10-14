# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/parse-sections/src/forms2tables.R

library(pacman)
pacman::p_load(
    argparse,
    dplyr,
    feather,
    logger,
    purrr,
    stringdist,
    stringr,
    tidyr,
    yaml
)


parser <- ArgumentParser()
parser$add_argument("--input", default = "input/segmented-arrest-reports.feather")
parser$add_argument("--hints", default = "hand/regex-hints.yaml")
args <- parser$parse_args()

##### extraction related functions

sec2lines <- function(section) {
    section %>%
        arrange(document_id, page_num,
                block_num, par_num, line_num, word_num) %>%
        group_by(document_id, filename, page_num,
                 block_num, par_num, line_num) %>%
        summarise(text = paste(text, collapse = " "), .groups = "drop") %>%
        arrange(document_id, page_num, block_num, par_num, line_num)
}

txtract <- function(regexp, index) {
    function(txt) str_match(txt, regexp)[,index]
}

extract_regexes <- function(section, regexes) {
    rx_cols <- names(regexes)
    extractors <- map(regexes, ~do.call(txtract, .))
    out <- bind_cols(section, map_dfc(extractors, ~.(section$text)))
    id_cols <- setdiff(colnames(out), rx_cols)
    structure(out, id_cols = id_cols)
}

remove_dupes <- function(vals) {
    problems <- vals %>%
        group_by(document_id, page_num, field) %>%
        filter(n_distinct(value) > 1) %>%
        ungroup
    if (nrow(problems) > 0) {
        walk(seq_along(problems$value),
        ~log_warn("file: ", problems$filename[.],
                  " (page ", problems$page_num[.], ")",
                  "; field: ", problems$field[.],
                  "; value: '", problems$value[.], "'",
                  "; action: dropped"))
    }
    vals %>%
        group_by(document_id, page_num, field) %>%
        filter(n_distinct(value) == 1) %>%
        ungroup
}

reshape_extracted <- function(extracted) {
    id_cols <- attr(extracted, "id_cols")
    if (is.null(id_cols)) stop("unable to distinguish columns for reshaping")
    rx_cols <- setdiff(colnames(extracted), id_cols)
    extracted %>%
        pivot_longer(all_of(rx_cols), names_to = "field", values_to = "value") %>%
        filter(!is.na(value)) %>%
        mutate(value = str_trim(str_to_lower(value))) %>%
        distinct(document_id, filename, page_num, field, value) %>%
        remove_dupes %>%
        pivot_wider(names_from = field, values_from = value)
}

field_value_candidates <- function(raw_form, fv_pattern, known_fields) {
    collapsed <- sec2lines(raw_form)
    fv_matches <- str_match(collapsed$text, fv_pattern)
    extracted <- collapsed %>%
        mutate(field = fv_matches[,2], value = fv_matches[,3]) %>%
        mutate(across(c(field, value), ~str_trim(str_to_lower(.)))) %>%
        filter(!is.na(field), !is.na(value)) %>%
        distinct(document_id, filename, page_num, field, value)
    dists <- map_dfc(known_fields,
                     ~stringdist(extracted$field, .,
                                 method = "cosine", q = 4))
    bind_cols(extracted, dists) %>%
        pivot_longer(cols = all_of(names(known_fields)),
                     names_to = "candidate", values_to = "distance")
}

select_best <- function(candidates) {
    candidates %>%
        group_by(document_id, page_num, field, value) %>%
        filter(distance == min(distance)) %>% ungroup %>%
        transmute(document_id, filename, page_num,
                  field = candidate, value)
}

###

arr <- read_feather(args$input)
regex_hints <- read_yaml(args$hints)

lkr_lh_fields <- list(
    prints              = "prints taken",
    palmprints          = "palmprints taken",
    injury              = "is there obvious pain or injury",
    appears_irrational  = "appears to be irrational",
    withdrawal          = "signs of alcohol/drug withdrawal",
    despondent          = "appears to be despondent",
    infection           = "is there obvious signs of infection",
    carrying_medication = "carrying medication",
    received            = "received in lockup",
    photo               = "photograph taken",
    facility            = "holding facility",
    released            = "released from lockup",
    under_influence     = "under the influence of alcohol/drugs"
)

lkr_rh_fields <- list(
    treatment         = "are you receiving treatment",
    first_arrest      = "first time ever been arrested",
    suicide_harm      = "attempted suicide/serious harm",
    medical_mental    = "serious medical or mental problems",
    taking_medication = "presently taking medication",
    pregnant          = "(if female)are you pregnant",
    trans_gnc         = "transgender/intersex/gender non-conforming"
)

log_info("loaded ", nrow(arr), " rows of arrest report data")

#######################
# some pdfs include multiple arrest reports, need to split
# this could be done better

arr2doc <- arr %>%
    distinct(pdf_id, filename, page_num) %>%
    arrange(pdf_id, filename, page_num) %>%
    group_by(pdf_id, filename) %>%
    mutate(new_document = page_num != lag(page_num, 1) + 1) %>%
    ungroup %>%
    replace_na(list(new_document = TRUE)) %>%
    mutate(document_id = cumsum(new_document)) %>%
    distinct(pdf_id, filename, page_num, document_id)

log_info(nrow(arr2doc), " distinct arrest reports")

arr <- arr %>%
    inner_join(arr2doc, by = c("pdf_id", "filename", "page_num")) %>%
    select(document_id,
           pdf_id, filename, cr_id, batch_id, page_num,
           ends_with("_num"), ends_with("_bound"),
           section, text, conf) %>%
    group_by(document_id, pdf_id, filename) %>%
    mutate(left_half = left_bound < 800,
           relative_top_bound = top_bound - min(top_bound)) %>%
    ungroup

##################

###### incident narratives ########
inc_nar <- arr %>%
    filter(section == "incident_narrative") %>%
    arrange(document_id, pdf_id, page_num, block_num,
            par_num, line_num, word_num) %>%
    group_by(document_id, filename) %>%
    summarise(page_from = min(page_num), page_to = max(page_num),
              text = paste(text, collapse = " "), .groups = "drop")
log_info("incident_narrative: ", nrow(inc_nar))

########### offender ############

off_lh <- arr %>%
    filter(section == "offender", left_half) %>%
    sec2lines %>%
    extract_regexes(regex_hints$offender_left) %>%
    reshape_extracted

off_rh <- arr %>%
    filter(section == "offender", !left_half) %>%
    sec2lines %>%
    group_by(document_id, page_num) %>%
    mutate(text = ifelse(lead(text, 1) %in% c("Complexion", "Style"),
                         paste(text, lead(text, 1)),
                         text)) %>%
    ungroup %>%
    extract_regexes(regex_hints$offender_right) %>%
    # this is a hack, ideas for improvement?
    mutate(race = ifelse(!is.na(lag(sex, 1)), text, NA)) %>%
    reshape_extracted

offender <- full_join(off_lh, off_rh,
                      by = c("document_id", "filename", "page_num"))
log_info("offender: ", nrow(offender))

#### incident #####

incident_lh <- arr %>%
    filter(section == "incident", left_half) %>%
    sec2lines %>%
    extract_regexes(regex_hints$incident_left) %>%
    reshape_extracted

incident_rh <- arr %>%
    filter(section == "incident", !left_half) %>%
    sec2lines %>%
    extract_regexes(regex_hints$incident_right) %>%
    reshape_extracted

incident <- full_join(incident_lh, incident_rh,
                      by = c("document_id", "filename", "page_num"))
log_info("incident: ", nrow(incident))

#### court info #####

court_info_lh <- arr %>%
    filter(section == "court_info", left_half) %>%
    sec2lines %>%
    extract_regexes(regex_hints$court_info_left) %>%
    reshape_extracted

court_info_rh <- arr %>%
    filter(section == "court_info", !left_half) %>%
    sec2lines %>%
    extract_regexes(regex_hints$court_info_right) %>%
    reshape_extracted

court_info <- full_join(court_info_lh, court_info_rh,
                        by = c("document_id", "filename", "page_num"))
log_info("court info: ", nrow(court_info))

###### lockup keeper processing ####

lkr_lh <- arr %>%
    filter(section == "lockup_keeper_processing", left_half,
           relative_top_bound < 620) %>%
    field_value_candidates(fv_pattern = "^([^?:]+)[?:](.+)$",
                      known_fields = lkr_lh_fields) %>%
    filter(distance < .5) %>%
    mutate(dubious = candidate %in% c("prints", "palmprints") &
           distance > 0) %>%
    mutate(candidate = case_when(
        dubious & value %in% c("yes", "no") ~ "palmprints",
        dubious & str_detect(value, "[0-9]") ~ "prints",
        TRUE ~ candidate)) %>%
    select_best %>%
    remove_dupes %>%
    pivot_wider(names_from = field, values_from = value)

lkr_rh <- arr %>%
    filter(section == "lockup_keeper_processing", !left_half,
           relative_top_bound < 620) %>%
    field_value_candidates(fv_pattern =  "^([^?]+)[?](.+)$",
                           known_fields = lkr_rh_fields) %>%
    filter(distance < .5) %>%
    select_best %>%
    remove_dupes %>%
    pivot_wider(names_from = field, values_from = value)

lkr <- full_join(lkr_lh, lkr_rh,
                 by = c("document_id", "filename", "page_num"))

log_info("lockup keeper processing: ", nrow(lkr))

##### processing personnel ####

arr %>%
    filter(section == "processing_personnel")


#### reporting personnel #####
arr %>%
    filter(section == "reporting_personnel")

