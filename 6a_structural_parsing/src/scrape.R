library(pacman)
pacman::p_load(argparse, tidyverse, dbplyr, DBI, RPostgreSQL)


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

tbl(con, "cr_pdf_pages") %>%
    filter(pdf_id == 177)

needs_parsing <- files %>% distinct(cr_id, filename, dropbox_path,
                                    page_num, page_classification)

db_drop_table(con, "tmp_parse")
copy_to(con, needs_parsing, name = "tmp_parse")
to_parse <- tbl(con, "tmp_parse")

test <- dict %>%
    rename(pdf_id = id) %>%
    inner_join(to_parse, by = c("cr_id", "filename")) %>%
    #     filter(page_classification == "ARREST Report") %>%
    inner_join(ocr_tokens, by = c("pdf_id", "page_num")) %>% collect

test %>%
    filter(page_classification == "Incident Report") %>%
    select()

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
