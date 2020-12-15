library(pacman)
pacman::p_load(argparse, dplyr, feather, stringr)

parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--output")
args <- parser$parse_args()

oc <- read_feather(args$input)

out <- oc %>%
    arrange(docid, docpg, word_id) %>%
    group_by(docid, docpg, line_id) %>%
    summarise(text = paste(text, collapse = " ") %>% str_trim,
              x0=min(x0/dpi), y0=min(y0/dpi),
              x1=max(x1/dpi), y1=max(y1/dpi),
              .groups="drop") %>%
    filter(text != "")

write_feather(out, args$output)

# done.
