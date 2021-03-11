# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

pacman::p_load(
    argparse,
    arrow,
    digest,
    dplyr,
    purrr,
    readr,
    stringr,
    tidyr,
    writexl
)


parser <- ArgumentParser()
parser$add_argument("--input", default="../merge/output/narratives.parquet")
parser$add_argument("--sampsize", type="integer", default=50)
parser$add_argument("--output")
args <- parser$parse_args()

narrs <- read_parquet(args$input)

pats <- c("on[\\-\\s]*going", "harass") %>%
    paste('(', ., ')', sep='', collapse='|')

# pats <- c('\\Wanus\\W', '\\Wgrope', 'vagina',
#   '\\Wbreast', 'rectum', 'cavity',
#   'buttocks', 'scrotum', '\\Wpenis\\W') %>%
#     paste('(', ., ')', sep='', collapse='|')

targeted_samps <- narrs %>% mutate(pattext=str_to_lower(text)) %>%
    filter(str_length(text) < 30000) %>%
    filter(str_detect(pattext, pats)) %>% filter(!is.na(doccloud_url)) %>%
    distinct(cr_id, filename, rpt_type, doccloud_url, text) %>%
    nest(data=-rpt_type) %>%
    mutate(sampsize=pmin(map_int(data, nrow), args$sampsize)) %>%
    mutate(data=map2(data, sampsize, sample_n)) %>% select(-sampsize) %>%
    unnest(data) %>% sample_frac(1) %>%
    select(cr_id, filename, rpt_type, text, doccloud_url)


# samps <- narrs %>%
#     group_by(cr_id, section_name, column_name, pdf_name, text) %>%
#     summarise(page_num = min(page_num), .groups='drop') %>%
#     filter((section_name == "Accused Members" &
#             column_name == "Initial / Intake Allegation")) %>%
#     nest(data=c(-section_name, -column_name)) %>%
#     mutate(data = map(data, sample_n, args$sampsize)) %>%
#     unnest(data) %>%
#     select(narrative_type = column_name, cr_id, pdf_name, page_num, text) %>%
#     mutate(text = str_replace_all(text, "\\n", " ") %>% str_squish)

write_xlsx(targeted_samps, args$output)

# done.
