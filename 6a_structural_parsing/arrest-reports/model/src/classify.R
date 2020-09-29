# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/model/src/classify.R

library(pacman)
pacman::p_load(argparse, randomForest, dplyr, feather, logger)


parser <- ArgumentParser()
parser$add_argument("--input", default = "input/all-features.feather")
parser$add_argument("--classifier", default = "output/rf-model.rds")
parser$add_argument("--output")
args <- parser$parse_args()

feats <- read_feather(args$input)
fit <- readRDS(args$classifier)

feats <- feats %>% filter(line_top < Inf, line_bottom > 0)

log_info("classifying new data")
log_info("there are ", nrow(feats), " rows to classify")
preds <- predict(fit, newdata = feats)
log_info("generated ", length(preds), " labels")

stopifnot(length(preds) == nrow(feats))

out <- feats %>%
    select(filename, pdf_id, page_num, block_num, par_num, line_num) %>%
    mutate(section = preds)

write_feather(out, args$output)

# done.
