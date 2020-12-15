# vim: set ts=4 sts=0 sw=4 si fenc=utf-8:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

pacman::p_load(argparse, dplyr, feather)

parser <- ArgumentParser()
parser$add_argument("--records")
parser$add_argument("--classes")
parser$add_argument("--output")
args <- parser$parse_args()

docs <- read_feather(args$records) %>% rename(word_conf = conf)
labs <- read_feather(args$classes) %>% rename(section_conf = conf)

out <- docs %>% left_join(labs, by = c("docid","docpg", "line_id"))

write_feather(out, args$output)

# done.
