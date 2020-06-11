library(tidyverse)

x <- read_csv("needs_structural_parsing.csv")

db_path <- function(fn) paste0("~/Dropbox", path, "/", fn)

x %>%
    head(1) %>%
    #     transmute(cr_id, filename,
    #               page_num, path = db_path(dropbox_path, filename)) %>%
    glimpse

bloop <- read_csv("~/Dropbox/Green v. CPD FOIA Files/Green 2019.12.02 Production.csv")

library(tesseract)

bloop <- ocr_data("~/Dropbox/Green v. CPD FOIA Files/OCR text/CPD 0017331.pdf/CPD 0017331.pdf.2.png")
