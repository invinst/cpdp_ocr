# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

pacman::p_load(
    argparse,
    digest,
    dplyr,
    purrr,
    readr,
    stringr,
    tidyr,
    writexl
)


parser <- ArgumentParser()
parser$add_argument("--input", default="input/narratives.csv")
parser$add_argument("--sampsize", type="integer", default=100)
parser$add_argument("--output", default="output/narrative-samples.xlsx")
args <- parser$parse_args()

narrs <- read_csv(args$input,
         col_types = cols(
            cr_id = col_integer(),
            pdf_name = col_character(),
            page_num = col_integer(),
            section_name = col_character(),
            column_name = col_character(),
            text = col_character(),
            batch_id = col_integer(),
            dropbox_path = col_character(),
            doccloud_url = col_character()
    )
)

pats <- c('\\Wanus\\W', '\\Wgrope', 'vagina',
  '\\Wbreast', 'rectum', 'cavity',
  'buttocks', 'scrotum', '\\Wpenis\\W') %>%
    paste('(', ., ')', sep='', collapse='|')

targeted_samps <- narrs %>% mutate(text=str_to_lower(text)) %>%
    filter(str_detect(text, pats)) %>%
    group_by(cr_id, section_name, column_name, pdf_name, text) %>%
    summarise(page_num=min(page_num), .groups='drop') %>%
    select(narrative_type = column_name, cr_id, pdf_name, page_num, text) %>%
    mutate(text = str_replace_all(text, "\\n", " ") %>% str_squish) %>%
    mutate(narr_id = map_chr(text, digest, algo="sha1"),
           narr_id = str_sub(narr_id, 1, 7)) %>%
    select(cr_id, narr_id, everything())

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
