# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/features/src/lines2features.R

library(pacman)
pacman::p_load(
    argparse,
    dplyr,
    feather,
    purrr,
    rlang,
    stringr,
    tidyr,
    tidytext,
    topicmodels,
    yaml
)


parser <- ArgumentParser()
parser$add_argument("--input", default = "input/labeled-arrest-report-lines.feather")
parser$add_argument("--topics", default = "output/lda-model.rds")
parser$add_argument("--posheadings", default = "output/training-headings.feather")
parser$add_argument("--regexes", default = "hand/regexes.yaml")
parser$add_argument("--output")
args <- parser$parse_args()

arr <- read_feather(args$input)
topic_mod <- readRDS(args$topics)
heads <- read_feather(args$posheadings)
regexes <- read_yaml(args$regexes)

####

is_legible <- function(txt)
    str_count(txt, "[a-zA-Z0-9 ]") > 1

######
## calculate topic features using pre-trained topic model
arr_dtm <- arr %>%
    unnest_tokens(word, text) %>%
    filter(word %in% topic_mod@terms) %>%
    mutate(identifier = paste(pdf_id, page_num, block_num,
                              par_num, line_num, sep = "_")) %>%
    count(identifier, word) %>%
    cast_dtm(identifier, word, n)

arr_topics <- topicmodels::posterior(topic_mod, newdata = arr_dtm)

arr_topics_df <- as_tibble(arr_topics$topics, rownames = "identifier") %>%
    set_names(~paste0("t_", .)) %>%
    separate(t_identifier,
             into = c("pdf_id", "page_num", "block_num",
                      "par_num", "line_num"), sep = "_") %>%
    mutate_at(vars(pdf_id, contains("_num")), as.integer)

####
# regex-based features

re_cols <- map_dfc(regexes, ~as.integer(str_detect(arr$text, .)))
stopifnot(nrow(re_cols) == nrow(arr))

####
# putting everything together:
feats <- arr %>%
    bind_cols(re_cols) %>%
    left_join(heads,
              by = c("pdf_id", "filename",
                     "page_num", "block_num", "par_num", "line_num")) %>%
    left_join(arr_topics_df,
              by = c("pdf_id", "page_num",
                     "block_num", "par_num", "line_num")) %>%
    mutate_at(vars(starts_with("t_")), ~replace_na(., 0)) %>%
    mutate(heading = replace_na(heading, "NODATA")) %>%
    filter(is_legible(text))

####
# layout-based and page-global features
feats <- feats %>%
    group_by(docid) %>%
    mutate(across(starts_with("re_"), cummax, .names = "cum_{col}"),
           across(starts_with("re_"),    max, .names = "pg_{col}")) %>%
    ungroup %>%
    arrange(docid, page_num, line_top, block_num, par_num, line_num) %>%
    group_by(docid, page_num) %>%
    mutate(line_gap = line_top - lag(line_bottom, 1)) %>%
    replace_na(list(line_gap = 0)) %>%
    ungroup

write_feather(feats, args$output)

# done.
