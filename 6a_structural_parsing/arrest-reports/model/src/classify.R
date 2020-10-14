# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/model/src/classify.R

library(pacman)
pacman::p_load(argparse, crfsuite, dplyr, feather, logger, tidyr)

parser <- ArgumentParser()
parser$add_argument("--input", default = "input/all-features.feather")
parser$add_argument("--classifier", default = "output/crf-model.rds")
parser$add_argument("--output")
args <- parser$parse_args()

feats <- read_feather(args$input)
fit <- readRDS(args$classifier)

to_predict <- feats %>%
    filter(line_top < Inf, line_bottom > 0) %>%
    select(docid, pdf_id, filename, page_num, block_num, par_num, line_num,
           heading,
           matches("(^|_)re_"), contains("topic"),
           pos_top, pos_bot,
           starts_with("cum_"), starts_with("pg_")) %>%
    mutate(heading = ifelse(heading == "NODATA", NA, heading)) %>%
    mutate_at(vars(-docid, -pdf_id, -filename, -page_num, -block_num,
                   -par_num, -line_num), as.character) %>%
    pivot_longer(cols = c(-docid, -filename, -pdf_id, -page_num, -block_num,
                          -par_num, -line_num),
                 names_to = "variable", values_to = "value") %>%
    filter(!is.na(value)) %>%
    mutate(value = paste0(variable, "=", value)) %>%
    pivot_wider(names_from = variable, values_from = value)

log_info("classifying new data")
log_info("there are ", nrow(to_predict), " rows to classify")
preds <- predict(fit, newdata = to_predict, group = to_predict$docid)
log_info("generated ", nrow(preds), " labels")

stopifnot(nrow(preds) == nrow(to_predict))

out <- to_predict %>%
    mutate(section = preds$label) %>%
    select(filename, pdf_id, page_num, block_num, par_num, line_num, section)

write_feather(out, args$output)

# done.
