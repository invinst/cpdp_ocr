# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# load libs {{{
pacman::p_load(
    argparse,
    assertr,
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

# functions {{{
pad_num <- function(string) str_pad(string, width=4, side="left", pad="0")

# create a sortable line identifier that can span multiple pages
make_seqid <- function(page_no, y0) {
    paste0(pad_num(page_no), "-", sprintf("%06.2f", y0))
}

identify_margins <- function(narratives) {
    label_based <- narratives %>%
        mutate(inc=stringdist(text, "incident") < 3,
               descr=stringdist(text, "Description") < 3) %>%
        group_by(fileid) %>% filter(sum(x1 < 3.5 & inc) == 1) %>% ungroup %>%
        filter(x1 < 3.5, inc) %>%
        group_by(fileid) %>%
        summarise(left_margin=max(x1), .groups="drop")

    p2 <- narratives %>% anti_join(label_based, by="fileid") %>%
        group_by(fileid, page_no, line_id) %>%
        summarise(text=paste(text, collapse=" "), x0=min(x0),
                  .groups="drop_last") %>%
        filter(x0 > 2, x0 < 4.5) %>%
        summarise(lo=quantile(x0, .1), hi=quantile(x0, .9),
                  .groups="drop") %>%
        filter(hi-lo < .25) %>%
        transmute(fileid, page_no, left_margin=lo)

    out <- narratives %>%
        distinct(fileid, page_no) %>%
        inner_join(label_based, by="fileid") %>%
        bind_rows(p2)

    problems <- distinct(narratives, fileid, page_no) %>%
        anti_join(out, by=c("fileid", "page_no")) %>%
        distinct(fileid)

    if (nrow(problems) > 0) {
        log_info(nrow(problems), " narratives dropped, couldn't format")
        iwalk(seq_len(nrow(problems)), ~log_info(problems$fileid[.]))
    }
    out %>% anti_join(problems, by="fileid")
}
# }}}

# data {{{
docs <- read_parquet(args$input)

doclines <- docs %>%
    arrange(fileid, filename, page_no, line_id, word_id) %>%
    group_by(fileid, filename, page_no, line_id) %>%
    filter(conf > 50, str_trim(text) != "") %>%
    summarise(text=paste(text, collapse=" "),
              x0=min(x0), x1=max(x1), y0=min(y0), y1=max(y1),
              .groups="drop")
# }}}

log_info(nrow(distinct(docs, fileid)), " files imported")

# setup: create informative features, remove headers/footers {{{
feats <- doclines %>%
    group_by(fileid, page_no) %>%
    mutate(info_incident = stringdist(text, "INFORMATION ABOUT THE INCIDENT") < 7,
           location_incident = stringdist(text, "Location of the incident") < 7,
           indented = x0 > 2.5,
           allcaps = text == str_to_upper(text),
           top = y0 < .5, bottom = y0 > max(y1) - 1.1,
           s_wcomp = str_detect(text, "WEB Complaint"),
           s_pg = str_detect(text, "Page [0-9]+ of [0-9]+"),
           s_http = str_detect(text, "http\\://"),
           s_cpd = str_detect(text, "^CPD"),
           s_dt = str_detect(text, "[0-9]+/[0-9]+/[0-9]+")) %>%
    ungroup %>%
    mutate(hdr = top & (s_wcomp | s_pg),
           ftr = bottom & (s_http | s_cpd | s_dt))

headers <- feats %>% filter(hdr) %>%
    group_by(fileid, page_no) %>%
    summarise(hdr_until = max(y1), .groups="drop")

footers <- feats %>% filter(ftr) %>%
    group_by(fileid, page_no) %>%
    summarise(ftr_from = min(y0), .groups="drop")

remove_headers_footers <- function(lns, heads=headers, foots=footers) {
    lns %>%
        left_join(heads, by=c("fileid", "page_no")) %>%
        replace_na(list(hdr_until=0)) %>%
        filter(y0 > hdr_until) %>%
        left_join(foots, by=c("fileid", "page_no")) %>%
        replace_na(list(ftr_from=Inf)) %>%
        filter(y1 < ftr_from)
}
# }}}

# pass1: narratives that can be neatly extracted based on pattern-match
#        of form labels that appear just before and after the narrative
# {{{
pass1 <- feats %>% remove_headers_footers %>%
    arrange(fileid, page_no, line_id) %>%
    mutate(seqid = make_seqid(page_no, y0)) %>%
    group_by(fileid) %>%
    mutate(narr_start = lag(info_incident, default=FALSE),
           narr_end = lead(location_incident, default=FALSE)) %>%
    filter(sum(narr_start) == 1, sum(narr_end) == 1) %>%
    summarise(from = min(ifelse(narr_start, seqid, "zzzz")),
              to = max(ifelse(narr_end, seqid, "00000")),
              .groups="drop") %>%
    filter(to > from)

log_info(nrow(distinct(pass1, fileid)), " candidate narratives in pass 1")

pass1_narr <- docs %>% remove_headers_footers %>%
    inner_join(pass1, by="fileid") %>%
    mutate(seqid=make_seqid(page_no, y0)) %>%
    filter(seqid >= from, seqid <= to)

pass1_narr_margins <- identify_margins(pass1_narr)

pass1_narr_clean <- pass1_narr %>%
    inner_join(pass1_narr_margins, by=c("fileid", "page_no")) %>%
    filter(x0 > left_margin) %>%
    group_by(fileid, filename) %>%
    summarise(text=paste(text, collapse=" "), .groups="drop")

log_info(nrow(distinct(pass1_narr_clean, fileid)),
         " narratives total from pass 1")
# }}}


# pass2: just pattern match the first part,
#        and search for the end
# {{{
pass2_start <- feats %>% anti_join(pass1_narr_clean, by="fileid") %>%
    remove_headers_footers %>%
    mutate(seqid = make_seqid(page_no, y0)) %>%
    group_by(fileid) %>% filter(sum(info_incident) == 1) %>% ungroup %>%
    filter(info_incident) %>% select(fileid, narr_start=seqid)

pass2 <- feats %>% anti_join(pass1_narr_clean, by="fileid") %>%
    remove_headers_footers %>%
    mutate(seqid = make_seqid(page_no, y0)) %>%
    inner_join(pass2_start, by="fileid") %>%
    filter(seqid > narr_start) %>%
    group_by(fileid, page_no) %>%
    mutate(narr_end = !lead(indented) & !lead(allcaps) & lead(y0)-y1 > .3) %>%
    replace_na(list(narr_end=FALSE)) %>%
    group_by(fileid) %>%
    mutate(narr_end = min(ifelse(narr_end, seqid, "zzzzz"))) %>%
    filter(min(narr_end) < "zzzzz") %>%
    filter(seqid > narr_start, seqid <= narr_end) %>%
    group_by(fileid) %>%
    summarise(from=narr_start, to=narr_end, .groups="drop") %>%
    distinct

log_info(nrow(distinct(pass2, fileid)),
         " narrative candidates found in pass 2")

pass2_narr <- docs %>% remove_headers_footers %>%
    inner_join(pass2, by="fileid") %>%
    mutate(seqid=make_seqid(page_no, y0)) %>%
    filter(seqid >= from, seqid <= to)

pass2_narr_margins <- identify_margins(pass2_narr)

pass2_narr_clean <- pass2_narr %>%
    inner_join(pass2_narr_margins, by=c("fileid", "page_no")) %>%
    filter(x0 > left_margin) %>%
    arrange(fileid, page_no, line_id, word_id) %>%
    group_by(fileid, filename) %>%
    summarise(text=paste(text, collapse=" "), .groups="drop")

log_info(nrow(distinct(pass2_narr_clean, fileid)), " narratives from pass2")
# }}}

out <- pass1_narr_clean %>%
    bind_rows(pass2_narr_clean) %>%
    mutate(cr_id = str_match(filename, "^LOG[_ ]([0-9]+)\\-")[,2]) %>%
    verify(!is.na(cr_id)) %>%
    select(fileid, cr_id, filename, text)

log_info(nrow(distinct(out, fileid)), " narratives extracted in all")

write_parquet(out, args$output)

# done.

#c348431
