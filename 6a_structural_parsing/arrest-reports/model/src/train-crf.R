# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/model/src/train-crf.R

library(pacman)
pacman::p_load(argparse, feather, dplyr, crfsuite, tidyr, logger, purrr, tools)

parser <- ArgumentParser()
parser$add_argument("--input", default = "input/training-features.feather")
parser$add_argument("--output")
args <- parser$parse_args()

modelfile <- paste0(tools::file_path_sans_ext(args$output), ".crfsuite")


set.seed(194821210)
arr <- read_feather(args$input)

prepped <- arr %>%
    select(docid, page_num, block_num, par_num, line_num,
           label,
           heading,
           matches("(^|_)re_"), contains("topic"),
           pos_top, pos_bot,
           starts_with("cum_"), starts_with("pg_")) %>%
    mutate(heading = ifelse(heading == "NODATA", NA, heading)) %>%
    mutate_at(vars(-docid, -page_num, -block_num,
                   -par_num, -line_num), as.character) %>%
    pivot_longer(cols = c(-docid, -page_num, -block_num,
                          -par_num, -line_num, -label),
                 names_to = "variable", values_to = "value") %>%
    filter(!is.na(value)) %>%
    mutate(value = paste0(variable, "=", value)) %>%
    pivot_wider(names_from = variable, values_from = value)

log_info("splitting data into train/test")
splits <- prepped %>%
    group_by(docid) %>%
    nest %>%
    ungroup %>%
    mutate(train = sample(c(TRUE, FALSE), prob = c(.8, .2),
                          size = nrow(.), replace = TRUE)) %>%
    unnest(data)

train <- splits %>% filter(train) %>% select(-train)
test <- splits %>% filter(!train) %>% select(-train)

log_info("fitting crf model")
fit <- crf(
    x = train %>%
        select(-docid, -page_num, -block_num, -par_num, -line_num, -label),
    y = train$label,
    group = train$docid,
    file = modelfile
)
log_info("done fitting crf model")

performance <- test %>%
    mutate(pred = predict(fit, newdata = ., group = docid)$label) %>%
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
