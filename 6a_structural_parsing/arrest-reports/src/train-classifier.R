# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/src/train-classifier.R

library(pacman)
pacman::p_load(argparse, randomForest, feather)

parser <- ArgumentParser()
parser$add_argument("--train", default = "output/training-features.feather")
parser$add_argument("--test", default = "output/testing-features.feather")
parser$add_argument("--output")
args <- parser$parse_args()

####


train <- read_feather(args$train)

modfit <- randomForest(label ~ . - pdf_id - page_num - block_num - par_num - line_num - text - docid,
                       data = train)
predict(modfit, newdata = test)



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
