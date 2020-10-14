# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/model/src/train-rf.R

library(pacman)
pacman::p_load(argparse, feather, dplyr, randomForest, tidyr, logger, purrr)

parser <- ArgumentParser()
parser$add_argument("--input", default = "input/training-features.feather")
parser$add_argument("--output")
args <- parser$parse_args()

set.seed(194821210)
arr <- read_feather(args$input)

log_info("splitting data into train/test")
splits <- arr %>%
    group_by(docid) %>%
    nest %>%
    ungroup %>%
    mutate(train = sample(c(TRUE, FALSE), prob = c(.8, .2),
                          size = nrow(.), replace = TRUE)) %>%
    unnest(data)

train <- splits %>% filter(train) %>% select(-train)
test <- splits %>% filter(!train) %>% select(-train)

log_info("fitting random forest model")
fit <- randomForest(
    y = train$label,
    x = train %>%
        select(line_top, line_bottom, line_left, line_right,
               heading,
               matches("(^|_)re_"), matches("(^|_)t_")) %>%
        mutate(across(matches("(^|_)re_"), ~replace_na(., 0)))
)
log_info("done fitting rf model")

performance <- test %>%
    select(pdf_id, docid, page_num,
           block_num, par_num, line_num,
           line_top, line_bottom, line_left, line_right,
           heading, label,
           matches("(^|_)re_"), matches("(^|_)t_")) %>%
    mutate(across(matches("(^|_)re_"), ~replace_na(., 0))) %>%
    mutate(pred = predict(fit, newdata = .)) %>%
    select(label, pred, pdf_id, docid, page_num, block_num, par_num, line_num) %>%
    mutate(correct = label == pred) %>%
    group_by(label) %>%
    summarise(correct = sum(correct), out_of = n(), .groups = "drop") %>%
    mutate(smry = paste0(correct, "/", out_of))

log_info("performance on held out test data, by category:")
walk2(as.character(performance$label), performance$smry,
      ~log_info(.x, ": ", .y))
log_info("overall performance: ",
         sum(performance$correct), "/", sum(performance$out_of))

saveRDS(fit, args$output)

# done.
