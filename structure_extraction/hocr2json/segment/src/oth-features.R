# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

library(pacman)
pacman::p_load(argparse, dplyr, feather, stringr)

parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--output")
args <- parser$parse_args()

docs <- read_feather(args$input)

feats <- docs %>%
    mutate(across(c(y0, y1),
                  cut, breaks=c(0,.5,1,1.5,9.5,10,Inf), include.lowest=TRUE,
                  .names = "{col}_bin"),
           x0_bin = cut(x0, breaks=c(0,.5,1,3,5,Inf), include.lowest=TRUE),
           has_qmark = str_detect(text, "\\?"),
           has_colon = str_detect(text, "\\:"),
           has_octo = str_detect(text, "#"),
           caps_pct = str_count(text, "[A-Z]")/str_length(text),
           has_caps = caps_pct > .25,
           word_1 = str_extract(text, "^[^\\W]+(\\W|$)") %>% str_trim,
           word_n = str_extract(text, "[^\\W]+$") %>% str_trim,
           has_digits = cut(str_count(text, "[0-9]"),
                            c(0,1,5,Inf),
                            include.lowest=TRUE)) %>%
    group_by(docid, docpg) %>%
    mutate(has_gap = cut(y0 - lag(y1,1), breaks=c(-Inf,0,.1,.2,.3,Inf))) %>%
    ungroup %>%
    select(docid, docpg, line_id, x1,
           ends_with("_bin"), starts_with("has_"), starts_with("word_"))

write_feather(feats, args$output)

# done.
