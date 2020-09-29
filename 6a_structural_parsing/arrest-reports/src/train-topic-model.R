# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/src/train-topic-model.R

library(pacman)
pacman::p_load(argparse, dplyr, tidytext, tm, topicmodels, readr)

parser <- ArgumentParser()
parser$add_argument("--wordcounts", default = "frozen/doc-word-counts.csv.gz")
parser$add_argument("--output")
args <- parser$parse_args()

wc <- read_delim(args$wordcounts,
                 delim = "|", na = "",
                 col_types = cols(
                     identifier = col_character(),
                     word = col_character(),
                     n = col_integer()))


wtm <- cast_dtm(wc, identifier, word, n)

docs_lda <- LDA(docs_wtm, k = 25, control = list(seed = 19481210))

saveRDS(docs_lda, args$output)

# done.
