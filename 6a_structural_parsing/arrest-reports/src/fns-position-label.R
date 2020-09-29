has_overlaps <- function(labs) {
    test <- labs %>%
        arrange(filename, pdf_id, page_num, lab_top) %>%
        group_by(filename, pdf_id, page_num) %>%
        filter(lab_top < lag(lab_bottom, 1))
    nrow(test) > 0
}

combine_overlapping <- function(obs) {
    obs %>%
        arrange(filename, pdf_id, page_num, lab_top) %>%
        group_by(filename, pdf_id, page_num) %>%
        mutate(overlap = lab_top < lag(lab_bottom, 1)) %>%
        replace_na(list(overlap = FALSE)) %>%
        mutate(group_id = cumsum(!overlap)) %>%
        mutate(lab_top = ifelse(overlap, lag(lab_top, 1), lab_top)) %>%
        group_by(filename, pdf_id, page_num, group_id) %>%
        summarise(text       = paste(text, collapse = " "),
                  lab_top    = min(lab_top),
                  lab_bottom = max(lab_bottom),
                  lab_left   = min(lab_left),
                  lab_right  = max(lab_right),
                  .groups    = "drop")
}

sideways_labels <- function(arr_lines) {
    obs <- arr_lines %>%
        filter(line_bottom - line_top > line_right - line_left & line_right < 110) %>%
        mutate(lab_top = line_top, lab_bottom = line_bottom,
               lab_left = line_left, lab_right = line_right)
    while(has_overlaps(obs)) {
        obs <- combine_overlapping(obs)
    }
    filter(obs, lab_bottom - lab_top > 100)
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
    distances <- map_dfc(expected,
                         ~stringdist(candidates, ., method = "cosine", q = 4))
    dict <- tibble(observed = candidates, distances) %>%
        pivot_longer(-observed,
                     names_to = "candidate",
                     values_to = "distance") %>%
        group_by(observed) %>%
        filter(distance <= min(distance),
               distance < .7) %>%
        ungroup %>%
        transmute(observed, section_name = candidate)
    observed %>%
        mutate(matchable = cleanup_header(text)) %>%
        inner_join(dict, by = c(matchable = "observed")) %>%
        distinct(filename, pdf_id, page_num, section_name, lab_top, lab_bottom)
}

check <- function(result) {
    ck <- result %>%
        count(filename, pdf_id, page_num, lab_top) %>%
        pluck("n") %>% unique
    stopifnot(ck == 1L)
    TRUE
}

docs2label <- function(docs, expected_labels) {
    known_sections <- c(
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
    observed_labels <- sideways_labels(docs)
    result <- best_match(observed_labels, known_sections)
    check(result)
    out <- docs %>%
        left_join(result, by = c("filename", "pdf_id", "page_num")) %>%
        filter(line_top >= lab_top, line_bottom <= lab_bottom) %>%
        distinct(filename, pdf_id, page_num,
                 block_num, par_num, line_num,
                 section = section_name)
    exceptions <- out %>%
        group_by(pdf_id, page_num, block_num, par_num, line_num) %>%
        filter(n() > 1)
    stopifnot(nrow(exceptions) == 0)
    out
}

