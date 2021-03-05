# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# load libs {{{
pacman::p_load(
    argparse,
    arrow,
    dplyr,
    logger,
    purrr,
    stringr,
    stringdist,
    tidyr
)
# }}}

# args {{{
parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

docs <- read_parquet(args$input)

cleanup_allegations_box <- function(segmented) {
    s_logtype_affidavit <- 'NOTE: Complaint Log "Type" remains classified as Info — Sworn Affidavit NOT on file'
    s_logtype_a2 <- 'Complaint type classified as "CR" Sworn affidavit on file'
    filter(segmented, flag == "alleg") %>%
        mutate(cleantext = str_replace_all(text, "[^\\w\\s]", "") %>% str_trim) %>%
        filter(
            cleantext != "ALLEGATIONS",
            !str_detect(text, "^CPD [0-9]+$"),
            !str_detect(text, "^CPD.+Page [0-9]+"),
            !str_detect(text, "^\\(Rev\\."),
            !str_detect(cleantext, "Sworn [Aa]ffidavit NOT on file"),
            !str_detect(text, "^CPD\\-44\\.112"),
            !str_detect(text, "IF CPD MEMBER, LIST"),
            !str_detect(text, "LIST RANK, STAR, EMPLOYEE"),
            str_count(text, "[A-Za-z0-9]") > 2,
            stringdist(text, s_logtype_affidavit, method="jaccard", q=8) > .8,
            stringdist(text, s_logtype_a2, method="jaccard", q=8) > .8,
            !str_detect(cleantext, "SEE ATTACHED")
            ) %>%
        select(-cleantext)
} 

format_text <- function(alleg) {
    alleg %>% arrange(fileid, page_no, line_id) %>%
        group_by(fileid, page_no) %>%
        mutate(newpara = y0 > lag(y1) + .1) %>%
        replace_na(list(newpara=TRUE)) %>%
        mutate(pid = cumsum(newpara)) %>%
        group_by(fileid, filename, page_no, pid) %>%
        summarise(text=paste(text, collapse=" ") %>% str_squish,
                  .groups="drop") %>%
        group_by(fileid, filename) %>%
        summarise(text=paste(text, collapse="\n\n"),
                  .groups="drop")
}

remove_heads <- function(lns) {
    lns %>%
        mutate(top5=y0<.5, top1=y0<1.1, hd=case_when(
            top5 ~ TRUE,
            top1 & str_detect(text, "^INSTRUCTIONS") ~ TRUE,
            top1 & str_detect(text, "^SUSTAINED") ~ TRUE,
            top1 & str_detect(text, "^SUMMARY REPORT") ~ TRUE,
            top1 & str_detect(text, "^CHICAGO POLICE") ~ TRUE,
            TRUE ~ FALSE)) %>%
        filter(!hd) %>% select(-top5, -top1, -hd)
}


doclines <- docs %>%
    arrange(fileid, filename, page_no, line_id, word_id) %>%
    group_by(fileid, filename, page_no, line_id) %>%
    filter(conf > 50, str_trim(text) != "") %>%
    summarise(text=paste(text, collapse=" "),
              x0=min(x0), x1=max(x1), y0=min(y0), y1=max(y1),
              .groups="drop")

segmented <- doclines %>%
    arrange(fileid, page_no, line_id) %>%
    mutate(cleantext = str_replace_all(text, "[^\\w\\s]", "") %>%
               str_squish %>% str_trim) %>%
    mutate(flag = case_when(
        cleantext == "ALLEGATIONS" ~ "alleg",
        str_detect(text, "IF CPD MEMBER, LIST") ~ "alleg",
        str_detect(text, "LIST RANK, STAR, EMPLOYEE") ~ "alleg",
        cleantext == "SUMMARY" ~ "summ",
        str_detect(text, "^Briefly summarize the investigation") ~ "summ",
        str_detect(text, "your efforts to prove or disprove the allegation") ~ "summ",
        str_detect(text, "support the allegation\\(s\\)") ~ "summ",
        str_detect(text, "sustained cases ONLY") ~ "summ",
        str_detect(text, "be included as attachments") ~ "summ",
        str_detect(cleantext, "^ATTACHMENTS") ~ "attach",
        str_detect(cleantext, "^FINDINGS") ~ "findings",
        str_detect(cleantext, "^EVIDENCE") ~ "evidence",
        str_detect(cleantext, "^INVESTIGATION") ~ "investigations",
        str_detect(cleantext, "^INVESTIGATION SUMMARY") ~ "investigations",
        str_detect(cleantext, "^SEE ATTACHED") ~ "ignore",
        str_detect(cleantext, "^SEE NARRATIVE") ~ "ignore",
        str_detect(cleantext, "^See Attached") ~ "ignore",
        str_detect(cleantext, "^See Narrative") ~ "ignore",
        str_detect(cleantext, "^SEE PAGE") ~ "ignore",
        TRUE ~ NA_character_)) %>%
    group_by(fileid, page_no) %>% mutate(gap=y0-lag(y0)) %>%
    mutate(flag = ifelse(y0 > 10 &
                         str_detect(cleantext, "(Page)|(CPD\\-44)") &
                         gap > 1.5, "foot", flag)) %>%
    fill(flag, .direction="down") %>% ungroup %>%
    ungroup

log_info(distinct(docs, fileid) %>% nrow, " documents to start")

pass1 <- segmented %>% filter(flag == "alleg") %>%
    group_by(fileid) %>%
    filter(min(page_no) == 1, max(page_no) == 1) %>%
    ungroup %>%
    cleanup_allegations_box %>%
    format_text

log_info(nrow(distinct(pass1, fileid)), " narratives extracted in pass 1")

pass2 <- segmented %>% filter(flag=="alleg") %>%
    anti_join(pass1, by="fileid") %>%
    cleanup_allegations_box %>%
    remove_heads %>%
    group_by(fileid) %>% filter(n_distinct(page_no) == 1) %>% ungroup %>%
    format_text

log_info(nrow(distinct(pass2, fileid)), " narratives extracted in pass 2")

pass3 <- segmented %>%
    filter(flag == "alleg") %>%
    anti_join(pass1, by="fileid") %>% anti_join(pass2, by="fileid") %>%
    cleanup_allegations_box %>%
    remove_heads %>%
    format_text

log_info(nrow(distinct(pass3, fileid)), " narratives extracted in pass 3")

pass4 <- segmented %>%
    anti_join(pass1, by="fileid") %>% anti_join(pass2, by="fileid") %>%
    anti_join(pass3, by="fileid") %>%
    arrange(fileid, page_no, line_id) %>% select(-flag) %>%
    mutate(flag=case_when(
            str_detect(text, "^ALLEGATION\\:") ~ "alleg",
            str_detect(text, "^Allegations\\:") ~ "alleg",
            str_detect(text, "^SUMMARY(\\:)?$") ~ "summary",
            str_detect(text, "SUMMARY/INVESTIGATION") ~ "summary")) %>%
    group_by(fileid) %>% fill(flag, .direction="down") %>%
    filter(flag=="alleg") %>%
    format_text

log_info(nrow(distinct(pass4, fileid)), " narratives extracted in final pass")

log_info("unable to extract from the following:")

docs %>% distinct(fileid, filename) %>%
    anti_join(pass1, by="fileid") %>% anti_join(pass2, by="fileid") %>%
    anti_join(pass3, by="fileid") %>% anti_join(pass4, by="fileid") %>%
    print(n=Inf)

out <- bind_rows(pass1, pass2, pass3, pass4) %>%
    mutate(cr_id = str_match(filename, "^LOG_([0-9]+)\\-")[,2]) %>%
    select(fileid, cr_id, filename, text)

write_parquet(out, args$output)

# done.
