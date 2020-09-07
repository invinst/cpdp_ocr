# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/src/ar-train.R

library(pacman)
pacman::p_load(argparse, dplyr, readr, randomForest, stringr, stringi, rlang)


parser <- ArgumentParser()
parser$add_argument("--training", default = "hand/arr-rpt-training-data.tsv")
parser$add_argument("--features", default = "hand/feature-hints.txt")
parser$add_argument("--sec_content", default = "output/arrest-report-content-sections.feather")
parser$add_argument("--sec_pos", default = "output/arrest-report-position-sections.feather")
args <- parser$parse_args()

####

# this is v. inefficient, but at least it works...
pred_doc <- function(doc, fit) {
    pl <- function(string)
        factor(string, levels = c(poss_labels, "PAGE.TOP", "DOC.TOP"))
    ##
    res <- factor(rep(NA_character_, times = nrow(doc)),
                  levels = fit$classes)
    ##
    cur_page <- doc[1,]$page_num
    res[1] <- doc[1,] %>%
        mutate(previous_label_page = pl("PAGE.TOP"),
               previous_label_doc = pl("DOC.TOP")) %>%
        predict(fit, newdata = .)
    if (nrow(doc) < 2) return(res)
    for (r in 2:nrow(doc)) {
        thispage <- doc[r,]$page_num
        newpage <- thispage != cur_page
        res[r] <- doc[r,] %>%
            mutate(previous_label_page = if_else(newpage,
                                                pl(res[r-1]), pl("PAGE.TOP")),
                   previous_label_doc = pl(res[r-1])) %>%
            predict(fit, newdata = .)
        cur_page <- thispage
    }
    res
}

###

poss_labels <- c("arrestee_vehicle", "charges", "court_info",
                 "footer", "header0", "header1", "header2",
                 "incident_narrative", "interview_log",
                 "lockup_keeper_processing", "longline", "movement_log",
                 "non_offenders", "processing_personnel", "properties",
                 "recovered_narcotics", "reporting_personnel", "visitor_log",
                 "warrant", "wc_comments")

arr <- read_delim(args$training, delim = "\t", na = "",
                  col_types = cols(pdf_id = col_integer(),
                                   page_num = col_double(),
                                   block_num = col_double(),
                                   par_num = col_double(),
                                   line_num = col_double(),
                                   text = col_character(),
                                   line_top = col_double(),
                                   line_bottom = col_double(),
                                   line_left = col_double(),
                                   line_right = col_double(),
                                   position_section = col_character(),
                                   context_section = col_character(),
                                   label = col_factor(levels = poss_labels)))

feature_hints <- readLines(args$features)
feature_names <- feature_hints
feature_hints <- paste0("(^|\\W)", feature_hints, "($|\\W)")
feature_hints <- set_names(feature_hints,
                           paste0("w_", feature_names))

features <- map_dfc(feature_hints, ~str_detect(str_to_upper(arr$text), .) %>% as.integer)

arr <- arr %>%
    arrange(pdf_id, page_num, line_top) %>%
    group_by(pdf_id) %>%
    mutate(last_page = lag(page_num, 1)) %>%
    replace_na(list(last_page = 0)) %>%
    mutate(new_document = case_when(
            last_page == 0 ~ TRUE,
            page_num > last_page + 1 ~ TRUE,
            TRUE ~ FALSE
        )) %>%
    ungroup %>%
    mutate(tmpdocid = cumsum(new_document)) %>%
    bind_cols(features) %>%
    arrange(tmpdocid, page_num, line_top, block_num, par_num, line_num) %>%
    group_by(tmpdocid, page_num) %>%
    mutate(previous_label_page = lag(label, 1)) %>%
    group_by(tmpdocid) %>%
    mutate(previous_label_doc = lag(label, 1)) %>%
    ungroup %>%
    mutate_at(vars(previous_label_page, previous_label_doc),
              ~factor(., levels = c(poss_labels, "PAGE.TOP", "DOC.TOP"))) %>%
    replace_na(list(previous_label_page = "PAGE.TOP",
                    previous_label_doc = "DOC.TOP")) %>%
    select(-position_section, -context_section, -new_document, -last_page) %>%
    mutate(textlen = str_length(stri_enc_tonative(text)))

train_ids <- sample(unique(arr$pdf_id), size = 12, replace = FALSE)


train <- arr %>%
    filter(pdf_id %in% train_ids) %>%
    select(line_top, line_bottom, line_left, line_right, textlen,
           starts_with("w_", ), previous_label_page, previous_label_doc,
           label)

modfit <- randomForest(label ~ ., data = train)
modfit2 <- randomForest(label ~ .,
                        data = arr %>%
                            select(line_top, line_bottom,
                                   line_left, line_right, textlen,
                                   starts_with("w_", ),
                                   previous_label_page, previous_label_doc,
                                   label))

predictions <- arr %>%
    select(-previous_label_page, -previous_label_doc) %>%
    arrange(tmpdocid, page_num, line_top, block_num, par_num, line_num) %>%
    group_by(tmpdocid) %>%
    nest %>%
    mutate(predicted_label = map(data, pred_doc, fit = modfit2))

predictions %>%
    unnest(cols = c(data, predicted_label)) %>%
    ungroup %>%
    #     group_by(tmpdocid) %>%
    summarise(m = sum(predicted_label == label) / n(), n = n()) %>%
    print(n = Inf)


alldocs <- feather::read_feather("output/page-data.feather")

alldocs_lines <- alldocs %>%
    filter(page_classification == "ARREST Report") %>%
    mutate(text = str_replace_all(text, "(\\n[0-9]*)|(\\t[0-9]*)", " "),
           text = str_squish(text)) %>%
    arrange(pdf_id, filename, dropbox_path,
            page_num, block_num, par_num, line_num, word_num, text) %>%
    group_by(pdf_id, filename, dropbox_path, page_num, block_num, par_num, line_num) %>%
    summarise(text = paste(text, collapse = " "),
              line_top = min(top_bound),
              line_bottom = max(top_bound + height_bound),
              line_left = min(left_bound),
              line_right = max(left_bound + width_bound),
              .groups = "drop")

alldocs_features <- map_dfc(feature_hints,
                            ~str_detect(str_to_upper(alldocs_lines$text), .) %>%
                                as.integer)

alldocs_topredict <- alldocs_lines %>%
    arrange(pdf_id, page_num, line_top) %>%
    group_by(pdf_id) %>%
    mutate(last_page = lag(page_num, 1)) %>%
    replace_na(list(last_page = 0)) %>%
    mutate(new_document = case_when(
            last_page == 0 ~ TRUE,
            page_num > last_page + 1 ~ TRUE,
            TRUE ~ FALSE
        )) %>%
    ungroup %>%
    mutate(tmpdocid = cumsum(new_document)) %>%
    bind_cols(alldocs_features) %>%
    mutate(textlen = str_length(stri_enc_tonative(text)))


system.time(
    alldocs_predictions <- alldocs_topredict %>%
        arrange(tmpdocid, page_num, line_top, block_num, par_num, line_num) %>%
        group_by(tmpdocid) %>%
        nest %>%
        mutate(predicted_label = map(data, pred_doc, fit = modfit))
)

alldocs_predictions %>%
    ungroup %>%
    unnest(c(data, predicted_label)) %>%
    filter(predicted_label == "incident_narrative") %>%
    select(tmpdocid, page_num, text) %>%
    group_by(tmpdocid, page_num) %>%
    summarise(text = paste(text, collapse = "\\n")) %>%
    ungroup %>%
    sample_n(15)

print("HI")

    # stops <- stop_words %>% mutate(word = str_to_upper(word))
    # 
    # arr %>%
    #     mutate(bloop = str_split(text, pattern = "\\W+")) %>%
    #     unnest(bloop) %>%
    #     mutate(bloop = str_trim(bloop) %>% str_to_upper) %>%
    #     filter(bloop != "") %>%
    #     filter(!str_detect(bloop, "^[0-9]+$")) %>%
    #     group_by(label, bloop) %>%
    #     summarise(n = n_distinct(tmpdocid), .groups = "drop") %>%
    #     filter(n > 3) %>%
    #     anti_join(stops, by = c("bloop" = "word")) %>%
    #     arrange(label, desc(n)) %>%
    #     group_by(label) %>%
    #     top_n(7, wt = n) %>%
    #     ungroup %>%
    #     distinct(bloop) %>%
    #     arrange(bloop) %>%
    #     pluck("bloop") -> feature_hints
    # 
    # writeLines(feature_hints, "hand/feature-hints.txt")
    # 
