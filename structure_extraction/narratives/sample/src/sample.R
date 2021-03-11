# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# load libs {{{
pacman::p_load(
    argparse,
    arrow,
    dplyr,
    purrr,
    tidyr,
    writexl
)
# }}}

# args {{{
parser <- ArgumentParser()
parser$add_argument("--input", default = "../merge/output/narratives.parquet")
parser$add_argument("--sampsize", type="integer", default=35L)
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

narrs <- read_parquet(args$input)

# todo: ability to sample by strata, requires cpdp metadata
out <- narrs %>% 
    nest(data=-rpt_type) %>%
    mutate(data=map(data, sample_n, args$sampsize)) %>%
    unnest(data) %>%
    sample_frac(1)

write_xlsx(out, args$output)

# done.
