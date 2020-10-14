library(pacman)
pacman::p_load(argparse, dplyr, feather)


parser <- ArgumentParser()
parser$add_argument("--input", default = "input/page-data.feather")
parser$add_argument("--sections", default = "output/all-section-labels.feather")
parser$add_argument("--output")
args <- parser$parse_args()

hocr <- read_feather(args$input) %>%
    filter(page_classification == "ARREST Report")
labs <- read_feather(args$sections)

n_rows <- nrow(hocr)

joined <- hocr %>%
    left_join(labs,
              by = c("filename", "pdf_id", "page_num",
                     "block_num", "par_num", "line_num"))

write_feather(joined, args$output)

# done.
