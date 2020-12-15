# vim: set ts=4 sts=0 sw=4 si fenc=utf-8:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# setup {{{
pacman::p_load(argparse, dplyr, feather)

parser <- ArgumentParser()
parser$add_argument("--rxfeats")
parser$add_argument("--othfeats")
parser$add_argument("--marginfeats")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

marg <- read_feather(args$marginfeats) %>% rename(margin = section_name)
rx <- read_feather(args$rxfeats)
oth <- read_feather(args$othfeats)

idfields <- c("docid", "docpg", "line_id")

feats <- oth %>%
    left_join(rx, by=c("docid","docpg","line_id")) %>%
    left_join(marg, by=c("docid","docpg","line_id")) %>%
    filter(x1 > .5) %>% select(-x1)

feat_fields <- setdiff(names(feats), idfields)

feats <- feats %>%
    arrange(docid, docpg, line_id) %>%
    group_by(docid, docpg) %>%
    mutate(across(all_of(feat_fields), ~lag(.,1), .names = "prv_{col}"),
           across(all_of(feat_fields), ~lead(.,1), .names = "nxt_{col}")) %>%
    ungroup

out <- feats %>%
    mutate(ln_identifier=paste(docid, docpg, line_id, sep = "."),
           pg_identifier=paste(docid, docpg, sep=".")) %>%
    select(pg_identifier, ln_identifier, everything())

write_feather(out, args$output)
