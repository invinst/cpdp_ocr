library(pacman)
pacman::p_load(argparse, tidyverse, dbplyr, DBI, RPostgreSQL,
               janitor, stringdist)


parser <- ArgumentParser()
parser$add_argument("--files", default = "../6_reports/output/needs_structural_parsing.csv")
args <- parser$parse_args()

files <- read_csv(args$files,
                  col_types = cols(.default = col_character(),
                                   cr_id    = col_double(),
                                   page_num = col_double()))

con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), dbname = 'cpdp_struct_parse', user = 'tshah')
dbListTables(con)

ocr_tokens <- tbl(con, "cr_ocr_tokens")
dict <- tbl(con, "cr_pdfs")

needs_parsing <- files %>% distinct(cr_id, filename, dropbox_path,
                                    page_num, page_classification)

db_drop_table(con, "tmp_parse")
copy_to(con, needs_parsing, name = "tmp_parse")
to_parse <- tbl(con, "tmp_parse")

test <- dict %>%
    rename(pdf_id = id) %>%
    inner_join(to_parse, by = c("cr_id", "filename")) %>%
    inner_join(ocr_tokens, by = c("pdf_id", "page_num")) %>%
    collect

arrest_reports <- test %>%
    filter(page_classification == "ARREST Report") %>%
    mutate(text = str_trim(text)) %>%
    filter(text != "")

# sections:
# "offender" section: has left and right half

# first break up report into sections/blocks

file_dict <- arrest_reports %>%
    distinct(pdf_id, dropbox_path, filename)

known_sections <- c("OFFENDER",
                    "INCIDENT",
                    "CHARGES",
                    "FELONY REVIEW",
                    "RECOVERED_NARCOTICS",
                    "WARRANT",
                    "NON-OFFENDER(S)",
                    "ARRESTEE VEHICLE",
                    "PROPERTIES",
                    "INCIDENT NARRATIVE",
                    "COURT INFO",
                    "REPORTING PERSONNEL",
                    "LOCKUP KEEPER PROCESSING",
                    "INTERVIEW LOG",
                    "VISITOR LOG",
                    "MOVEMENT LOG",
                    "WC COMMENTS",
                    "PROCESSING PERSONNEL")

cleanup_header <- function(hdr) {
    str_replace_all(hdr, "[^ -~]", "") %>% str_replace_all("[0-9]", "")
}

known_sections <- set_names(known_sections,
                            janitor::make_clean_names)

section_labels <- arrest_reports %>%
    mutate(section_name = height_bound > width_bound & left_bound + width_bound < 110,
           bottom = top_bound + height_bound) %>%
    filter(section_name) %>%
    select(pdf_id, page_num, left_bound:text, bottom,
           block_num, par_num, line_num, word_num) %>%
    arrange(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
    group_by(pdf_id, page_num, block_num) %>%
    summarise(header = paste(text, collapse = " "),
              top = min(top_bound), bottom = max(bottom)) %>%
    ungroup

section_labels <- section_labels %>%
    arrange(pdf_id, page_num, top) %>%
    group_by(pdf_id, page_num) %>%
    mutate(overlap = top < lag(bottom, 1)) %>%
    replace_na(list(overlap = FALSE)) %>%
    #     filter(pdf_id == 20302, page_num == 24) %>%
    mutate(group_id = cumsum(!overlap)) %>%
    mutate(top = ifelse(overlap, lag(top, 1), top)) %>%
    group_by(pdf_id, page_num, top) %>%
    summarise(header = paste(header, collapse = " "),
              top = min(top), bottom = max(bottom)) %>%
    ungroup

# never going to be able to match the ones that are just like "$*^&(&%@#)&"
observed_labels <- section_labels %>%
    mutate(n_alphas = str_length(str_replace_all(header, "[^A-Z]", ""))) %>%
    filter(n_alphas > 4) %>%
    mutate(to_match = cleanup_header(header)) %>%
    distinct(to_match) %>%
    mutate(len = str_length(to_match)) %>%
    filter(len <= 50) %>%
    pluck("to_match")

dists <- map(known_sections,
             ~stringdist(observed_labels, .,
                         method = "cosine", q = 4))

section_labels_std <- tibble(observed = observed_labels,
                            as_tibble(dists)) %>%
    pivot_longer(-observed,
                 names_to = "candidate",
                 values_to = "distance") %>%
    group_by(observed) %>%
    filter(distance <= min(distance),
           distance < .7) %>%
    ungroup %>%
    transmute(as_observed = observed,
              standard = candidate)

section_labels %>%
    mutate(header = cleanup_header(header)) %>%
    left_join(section_labels_std, by = c(header = "as_observed")) %>%
    transmute(pdf_id, page_num, top, bottom,
              section = coalesce(standard, header),
              standard) %>%
    filter(!is.na(standard)) %>%
    arrange(pdf_id, page_num, top) %>%
    group_by(pdf_id, page_num) %>%
    filter(top < lag(bottom, 1))

section_names %>%
    count(header, sort = TRUE) %>%
    print(n = 25)

    mutate(section_name = height_bound > width_bound & left_bound + width_bound < 110) %>%
    arrange(pdf_id, page_num, block_num, par_num, line_num, word_num) %>%
    group_by(pdf_id, page_num, block_num, par_num) %>%
    summarise(text = paste(text, collapse = " "),
              section_name = max(section_name),
              left_bound = min(left_bound),
              top_bound = min(top_bound)) %>%
    ungroup %>%
    filter(section_name > 0)

arrest_reports %>%
    mutate(poss_section_head = height_bound > width_bound) %>%
    #     filter(str_detect(filename, "1054783"), page_num == 106) %>%
    group_by(pdf_id, page_num) %>%
    arrange(top_bound, left_bound) %>%
    mutate(space_above = top_bound - lag(top_bound)) %>%
    mutate(new_section = space_above > 100 | top_bound == min(top_bound)) %>%
    select(lvl:text, space_above, new_section) %>%
    group_by(pdf_id, page_num, top_bound) %>%
    summarise(text = paste(text, collapse = " "),
              new_section = any(new_section, na.rm = TRUE)) %>%
    I
    #     print(n = Inf)

    select(page_num, left_bound, top_bound, text) %>%
    arrange(page_num, top_bound, left_bound) %>%
    group_by(page_num, top_bound) %>%
    summarise(text = paste(text, collapse = " ")) %>%
    ungroup %>%
    filter(page_num == 106) %>%
    print(n = Inf)
    #     filter(block_num == 10, page_num == 106) %>%
    #     distinct(page_num)
    select(block_num, par_num, line_num, word_num, text, left_bound, top_bound) %>%
    group_by(block_num, par_num, line_num) %>%
    summarise(text = paste(text, collapse = " ")) %>%
    print(n = Inf)

inc <- test %>%
    #     filter(filename == "CPD 0052746.pdf") %>%
    filter(filename == "LOG_1060152.pdf") %>% distinct(page_num)
    filter(str_trim(text) != "") %>%
    #     mutate(inc = str_detect(text, "^\\(The")) %>%
    arrange(pdf_id, filename, page_num, block_num, par_num, line_num, word_num) %>%
    group_by(pdf_id, filename, page_num, block_num, par_num, line_num) %>%
    summarise(text = paste(text, collapse = " ")) %>% ungroup %>% print(n = Inf)

    select(filename)
    select(page_num, id, filename,block_num, par_num,
           line_num, word_num, text, left_bound,
           width_bound, top_bound, height_bound) %>%
    count(page_num, left_bound, width_bound,
          top_bound, height_bound, sort = TRUE)



test %>%
    filter(str_trim(text) != "") %>%
    select(cr_id, pdf_id, filename, page_num, lvl,
           block_num, par_num, line_num, word_num,
           left_bound, top_bound, text) %>%
    filter(filename == "CPD 0007256.pdf") %>%
    arrange(page_num, block_num, par_num, line_num, word_num) %>%
    group_by(page_num, block_num, par_num, line_num) %>%
    summarise(text = paste(text, collapse = " ")) %>%
    print(n = Inf)

ocr_tokens %>%
    filter(pdf_id == 177) %>%
    collect %>%
    filter(str_trim(text) != "") %>%
    arrange(page_num, block_num, par_num, line_num, word_num) %>%
    group_by(page_num, block_num, par_num, line_num) %>%
    summarise(test = paste(text, collapse = " "))

test %>%
    filter(text != "") %>%
    arrange(cr_id, page_num, block_num, par_num, line_num, word_num) %>%
    group_by(cr_id, page_num, block_num, par_num, line_num) %>%
    summarise(text = str_flatten(text, collapse = " "))
