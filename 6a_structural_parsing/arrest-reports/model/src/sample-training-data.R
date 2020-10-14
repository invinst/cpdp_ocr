library(pacman)
pacman::p_load(argparse, dplyr, feather, tidyr, stringr, readr)


parser <- ArgumentParser()
parser$add_argument("--input", default = "../import/output/all-arr-lines.feather")
parser$add_argument("--labels", default = "output/all-section-labels.feather")
parser$add_argument("--reviewed", default = "input/training-features.feather")
parser$add_argument("--output")
args <- parser$parse_args()

arr <- read_feather(args$input)
labs <- read_feather(args$labels)
already_reviewed <- read_feather(args$reviewed)
already_reviewed <- already_reviewed %>% distinct(pdf_id, filename, page_num)

out <- arr %>%
    select(pdf_id, filename, docid, page_num, block_num, par_num, line_num,
           line_top, line_bottom, line_left, line_right, text) %>%
    anti_join(already_reviewed, by = c("pdf_id", "filename", "page_num")) %>%
    left_join(labs, by = c("pdf_id", "filename", "page_num", "block_num",
                           "par_num", "line_num")) %>%
    group_by(docid) %>% nest %>% ungroup %>%
    sample_n(8) %>% unnest(data) %>%
    arrange(pdf_id, page_num, block_num, par_num, line_num) %>%
    filter(str_count(text, "[A-Za-z0-9 ]") > 1)

write_delim(out, args$output, delim = "|")

# done.
