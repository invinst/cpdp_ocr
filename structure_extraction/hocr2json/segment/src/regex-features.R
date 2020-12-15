# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:


# setup {{{
library(pacman)
pacman::p_load(argparse, dplyr, feather, yaml, stringr, purrr)

parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--regexes")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

docs <- read_feather(args$input)
regexes <- read_yaml(args$regexes)

regexes <- map_chr(regexes, ~paste0("(\\W|^)", ., "(\\W|$)"))

re_cols <- map_dfc(regexes, ~as.integer(str_detect(docs$text, .)))
stopifnot(nrow(re_cols) == nrow(docs))

out <- bind_cols(docs, re_cols) %>%
    select(docid, docpg, line_id, starts_with("re_"))

write_feather(out, args$output)

# done.
