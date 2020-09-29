# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/src/prep-training-data.R

library(pacman)
pacman::p_load(argparse, readr, dplyr, purrr, feather,
               stringr, tidyr, tidytext, stringi)

parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--position")
parser$add_argument("--trainingdir")
parser$add_argument("--train")
parser$add_argument("--test")
args <- parser$parse_args()

###

add_features <- function(docs, feats) {
    feat_names <- feats
    feat_words <- paste0("(^|\\W)", feats, "($|\\W)")
    feat_words <- set_names(feat_words, paste0("w_", feat_names))
    textdata <- str_to_upper(docs$text)
    features <- purrr::map_dfc(feat_words,
                               ~as.integer(str_detect(textdata, .)))
    bind_cols(docs, features)
}

###

test_ids <- c(22108, 22144, 19689, 20461)

poss_labels <- c(
    "arrestee_vehicle", "charges", "court_info", "felony_review",
    "footer", "header", "incident", "incident_narrative", "interview_log",
    "lockup_keeper_processing", "longline", "movement_log", "non_offenders",
    "offender", "processing_personnel", "properties", "recovered_narcotics",
    "reporting_personnel", "visitor_log", "warrant", "wc_comments"
)

position_labs <- read_feather(args$position)

training_labels <- list.files(args$trainingdir, full.names = TRUE) %>%
    map(read_delim, delim = "|", na = "",
        col_types = cols(.default = col_character(),
                         pdf_id = col_integer(),
                         page_num = col_integer(),
                         block_num = col_integer(),
                         par_num = col_integer(),
                         line_num = col_integer())) %>%
    map_dfr(select, pdf_id, page_num, block_num, par_num, line_num, label) %>%
    # was originally trying to separate sections of header, but no longer
    mutate(label = ifelse(str_detect(label, "^header"), "header", label))

stopifnot(length(setdiff(training_labels$label, poss_labels)) == 0)

training_labels <- training_labels %>%
    mutate(label = factor(label, levels = poss_labels))

train_ids <- setdiff(training_labels$pdf_id, test_ids)

arr <- read_feather(args$input) %>%
    filter(page_classification == "ARREST Report") %>%
    select(id, pdf_id, filename, page_num, contains("_bound"), text,
           block_num, par_num, line_num, word_num)

arr_lines <- arr %>%
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

# note: not always clear, when multiple docs are in the same pdf,
#       when one document ends and the next begins. but there is relevant
#       state at the document level for classification -- a section that has
#       already appeared shouldn't appear again. `docid` is an ad-hoc attempt
#       to distinguish
# docid is NOT meant to be used outside of this context to identify records,
#       is not stable. only useful to split up documents.

arr_lines <- arr_lines %>%
    arrange(pdf_id, page_num, line_top, block_num, par_num, line_num) %>%
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

wc <- arr_lines %>%
    mutate(identifier = paste(docid, page_num, block_num, par_num, line_num,
                              sep = "_")) %>%
    unnest_tokens(word, text, token = "words") %>%
    filter(!str_detect(word, "[0-9]")) %>%
    anti_join(stop_words, by = "word") %>%
    count(identifier, word)

wc_dtm <- cast_dtm(wc, identifier, word, n)
system.time(wc_lda <- LDA(wc_dtm, k = 25, control = list(seed = 19481210)))

####

labeled_data <- training_labels %>%
    inner_join(arr_lines, by = c("pdf_id", "page_num", "block_num",
                                 "par_num", "line_num"))

labeled_data %>%
    select(pdf_id, filename, docid,
           page_num, block_num, par_num, line_num,
           line_top, line_bottom, line_left, line_right,
           text, label) %>%
    write_delim("hand/section-labels-training-data/sec-class-training-0.csv", delim = "|", na = "")

#### build features from most common words in each section
# note: only using the training set to generate these features

stopwords <- tidytext::stop_words %>% mutate(word = str_to_upper(word))

feature_words <- labeled_data %>%
    filter(!pdf_id %in% test_ids) %>%
    mutate(word = str_split(text, "\\W+")) %>%
    tidyr::unnest(word) %>%
    mutate(word = str_to_upper(word) %>% str_trim) %>%
    filter(word != "") %>%
    filter(!str_detect(word, "^[0-9]+$")) %>%
    group_by(label, word) %>%
    summarise(n = n_distinct(docid), .groups = "drop") %>%
    filter(n > 3) %>%
    anti_join(stopwords, by = "word") %>%
    arrange(label, desc(n)) %>%
    group_by(label) %>%
    top_n(7, wt = n) %>%
    ungroup %>%
    distinct(word) %>%
    arrange(word) %>%
    pluck("word")

# calculate features for all labeled data (training + test)
labeled_data <- add_features(labeled_data, feature_words) %>%
    mutate(textlen = str_length(stri_enc_tonative(text)))

labeled_data <- labeled_data %>%
    left_join(rename(position_labs, position_label = section),
              by = c("pdf_id", "page_num",
                     "block_num", "par_num", "line_num")) %>%
    replace_na(list(position_label = "NODATA"))

# add "previous label" feature
labeled_data <- labeled_data %>%
    arrange(docid, page_num, line_top, block_num, par_num, line_num) %>%
    group_by(docid, page_num) %>%
    mutate(previous_label_page = lag(label, 1)) %>%
    group_by(docid) %>%
    mutate(previous_label_doc = lag(label, 1)) %>%
    ungroup %>%
    mutate_at(vars(previous_label_page, previous_label_doc),
              ~factor(., levels = c(poss_labels, "PAGE.TOP", "DOC.TOP"))) %>%
    replace_na(list(previous_label_page = "PAGE.TOP",
                    previous_label_doc = "DOC.TOP"))

write_feather(labeled_data %>% filter(pdf_id %in% train_ids), args$train)
write_feather(labeled_data %>% filter(pdf_id %in% test_ids), args$test)

# done.
