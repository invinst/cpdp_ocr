# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# load libs {{{
pacman::p_load(
    argparse,
    assertr,
    dplyr,
    feather,
    jsonlite,
    logger,
    purrr,
    readr,
    stringr,
    tidygraph,
    tidyr,
    tools
)
# }}}

# command line args {{{
parser <- ArgumentParser()
parser$add_argument("--input")
parser$add_argument("--dict")
parser$add_argument("--pdfin")
parser$add_argument("--jsonout")
parser$add_argument("--pdfout")
parser$add_argument("--report")
args <- parser$parse_args()
# }}}

# funs {{{
pad_num <- function(number, width=4) {
    str_pad(number, width=width, pad="0", side="left")
}

boxes2edges <- function(boxes, is_edge, key) {
    n_box <- nrow(boxes)
    result <- vector("list", n_box)
    for (index in seq_len(n_box-1)) {
        ind2 <- index+1
        box1 <- boxes[index, ]
        boxk <- boxes[ind2:n_box, ]
        neighbor_ids <- boxk[[key]][is_edge(box1, boxk)]
        if (length(neighbor_ids) > 0)
            result[[index]] <- tibble(from=box1[[key]], to=neighbor_ids)
    }
    bind_rows(purrr::compact(result))
}

calc_groupid <- function(boxes, is_edge, key="id") {
    edges <- boxes2edges(boxes, is_edge, key=key)
    if (nrow(edges) <= 0) return(boxes %>%
                                 mutate(groupid=seq_len(nrow(.))) %>%
                                 select(all_of(key), groupid))
    tbl_graph(edges=edges, nodes=boxes, node_key=key, directed=F) %>%
        mutate(groupid=group_components()) %>%
        as_tibble("nodes") %>%
        select(all_of(key), groupid)
}

distance <- function(a.start, a.end, b.start, b.end) {
    pmax(b.start-a.end, a.start-b.end, 0)
}

is_x_close <- function(box1, box2, threshold) {
    distance(box1$x0, box1$x1, box2$x0, box2$x1) < threshold
}

overlap <- function(a.start, a.end, b.start, b.end) {
    unadjusted <- pmin(a.end, b.end) - pmax(a.start, b.start)
    unadjusted <- pmax(0, unadjusted)
    max_overlap <- pmin(a.end-a.start, b.end-b.start)
    unadjusted/max_overlap
}

# }}}

oc <- read_feather(args$input)

oc <- oc %>%
    distinct(docid, docpg, line_id, word_id, text, x0, y0, x1, y1, word_conf,
             section, section_conf)

dict <- readr::read_delim(args$dict,
                          delim="|",
                          col_types = cols(.default=col_character(),
                                           page_from=col_integer(),
                                           page_to=col_integer())) %>%
    distinct(orig_filename = original_filename,
             orig_pg_start = page_from, orig_pg_end = page_to,
             batch, dbx_hash, docid=rpt_id, doc_sha1=rpt_sha1,
             doc_filename = local_filename_subset) %>%
    mutate(doc_filename = basename(doc_filename))

# narratives {{{
log_info("starting narratives")

narratives <- oc %>% filter(section == "narrative") %>%
    arrange(docid, docpg, line_id, word_id) %>%
    group_by(docid, docpg) %>%
    summarise(text = paste(text, collapse = " "),
              conf = sum(section_conf), n = n(),
              .groups = "drop_last") %>%
    arrange(docid, docpg) %>%
    summarise(narrative = paste(text, collapse = " "),
              conf = sum(conf), n = sum(n),
              .groups="drop") %>%
    transmute(docid, narrative_conf = conf/n, narrative)

log_info("done with narratives")
# }}}

# incidents {{{
log_info("starting incidents")

incident_lines <- oc %>% filter(section == "incident") %>%
    arrange(docid, docpg, y0) %>%
    group_by(docid, docpg) %>%
    mutate(sameline = y0 <= lag((y0+y1)/2, 1)) %>%
    ungroup %>% replace_na(list(sameline=FALSE)) %>%
    mutate(line_id = cumsum(!sameline)) %>%
    arrange(docid, docpg, line_id, x0) %>%
    group_by(docid, docpg, line_id) %>%
    summarise(text = paste(text, collapse = " "),
              .groups="drop")

inc <- incident_lines %>%
    mutate( info_type = case_when(
        str_detect(text, "^IUCR") ~ "iucr",
        str_detect(text, "[0-9]{4} \\-") ~ "iucr",
        str_detect(text, "^Occurrence Date") ~ "occ_date")) %>%
    filter(!is.na(info_type))

inc_docs_review <- inc %>% group_by(docid) %>%
    filter(n_distinct(docpg) > 1) %>%
    summarise(n_dates = sum(info_type == "occ_date"),
              .groups = "drop") %>%
    mutate(suspected_issue = case_when(
        n_dates > 1 ~ "multiple OCIC reports concatenated",
        TRUE ~ "unknown page types"))

incidents <- inc %>%
    group_by(docid) %>% filter(n_distinct(docpg) == 1) %>% ungroup %>%
    group_by(docid) %>%
    mutate(occ_date = max(ifelse(info_type == "occ_date", text, NA_character_),
                          na.rm=T),
           occ_date = str_replace(occ_date, "#.*$", "") %>% str_trim,
           iucr_seqid = cumsum(info_type == "iucr")) %>%
    ungroup %>%
    filter(info_type != "occ_date") %>%
    select(docid, incident_date = occ_date,
           iucr_id = iucr_seqid, iucr=text)

log_info("done with incidents")
# }}}

# personnel {{{
same_column <- function(box1, box2) {
    is_x_close(box1, box2, threshold=40)
}

personnel_columns_canon <- c(
    p_empno = "Emp No",
    p_starno = "Star No",
    p_name = "Name",
    p_user = "User",
    p_unit = "Unit",
    p_date = "Date",
    p_beat = "Beat")

log_info("starting personnel...")

pers_headings <- oc %>% filter(section == "personnel") %>%
    group_by(docid, docpg) %>%
    filter(line_id==min(line_id)) %>% ungroup %>%
    mutate(text=str_trim(text)) %>%
    mutate(column = case_when(
        str_detect(text, "Star(No)?") ~ "star_no",
        str_detect(text, "Emp(No)?") ~ "emp_no",
        str_detect(text, "Name") ~ "name",
        str_detect(text, "User") ~ "user",
        str_detect(text, "Date") ~ "date",
        str_detect(text, "Unit") ~ "unit",
        str_detect(text, "Beat") ~ "beat",
        TRUE ~ NA_character_)) %>%
    arrange(docid, docpg, column, line_id, word_id) %>%
    group_by(docid, docpg, column, line_id) %>%
    summarise(text=paste(text, collapse=' '),
              x0=min(x0), x1=max(x1),
              .groups="drop") %>%
    filter(!is.na(column)) %>% select(-line_id)

pers_cols <- oc %>% filter(section == "personnel") %>%
    mutate(word_id = as.character(word_id)) %>%
    group_by(docid, docpg) %>%
    filter(line_id > min(line_id)) %>%
    nest %>% ungroup %>%
    mutate(data = map(data, calc_groupid, is_edge=same_column, key="word_id")) %>%
    unnest(data) %>% mutate(word_id=as.integer(word_id)) %>%
    rename(column_id=groupid)

pers <- oc %>% inner_join(pers_cols, by=c("docid", "docpg", "word_id")) %>%
    arrange(docid, docpg, line_id, column_id, word_id) %>%
    group_by(docid, docpg, line_id, column_id) %>%
    summarise(text=paste(text, collapse=" "),
              x0=min(x0), x1=max(x1),
              y0=min(y0),
              .groups="drop") %>%
    inner_join(pers_headings, by=c("docid", "docpg"),
               suffix = c(".row", ".head")) %>%
    group_by(docid, docpg, column_id) %>%
    mutate(col1 = x1.row < min(x0.head)) %>%
    ungroup %>%
    mutate(column = ifelse(col1, "role", column)) %>%
    filter(column=="role" | overlap(x0.row, x1.row, x0.head, x1.head) > .5) %>%
    distinct(docid, docpg, line_id, column_id, column, text=text.row) %>%
    group_by(docid, docpg, line_id, column_id) %>%
    filter(n_distinct(column) == 1) %>%
    mutate(text=paste(text,collapse=" ")) %>%
    ungroup

pers_docs_review <-  pers %>% group_by(docid) %>%
    filter(n_distinct(docpg) > 1) %>%
    ungroup %>% distinct(docid) %>%
    mutate(suspected_issue = "unknown page types")

personnel <- pers %>%
    group_by(docid) %>% filter(n_distinct(docpg) == 1) %>% ungroup %>%
    group_by(docid, docpg, column_id) %>%
    mutate(row=rank(line_id)) %>%
    ungroup %>%
    select(docid, docpg, row, column, text) %>%
    group_by(docid, docpg, row, column) %>% filter(n() == 1) %>%
    ungroup %>%
    filter(!column %in% c("emp_no", "user")) %>%
    distinct(docid, row, column, text)

personnel <- pivot_wider(personnel, names_from=column, values_from=text)

log_info("done extracing personnel data")
# }}}

# putting together as jsons {{{

# just high confidence ones to start... need to review/audit
usable_narratives <- narratives %>% filter(narrative_conf >= .5)
usable_incidents <- incidents %>% filter(!is.na(iucr))
usable_personnel <- personnel %>%
    group_by(docid) %>%
    filter(max(!is.na(role)) > 0,
           max(!is.na(star_no)) + max(!is.na(unit)) > 0,
           #            max(!is.na(unit)) > 0,
           max(!is.na(name)) > 0,
           max(str_count(role, "[a-zA-Z]")) > 2,
           max(str_count(name, "[a-zA-Z]")) > 2) %>%
    ungroup

jsons <- usable_incidents %>% nest(incident=c(-docid, -incident_date)) %>%
    inner_join(usable_personnel %>% nest(personnel = -docid), by="docid") %>%
    inner_join(usable_narratives %>% nest(narrative = -docid), by="docid") %>%
    anti_join(inc_docs_review, by = "docid") %>%
    anti_join(pers_docs_review, by = "docid") %>%
    nest(data=-docid) %>%
    mutate(json = map(data, toJSON, dataframe='rows', pretty=TRUE))

issues <- inc_docs_review %>% select(docid, suspected_issue) %>%
    bind_rows(pers_docs_review %>% select(docid, suspected_issue)) %>%
    distinct %>%
    group_by(docid) %>%
    summarise(suspected_issues = paste(sort(suspected_issue), collapse="; "),
              .groups="drop")
# }}}

out <- jsons %>%
    inner_join(dict, by="docid") %>%
    select(docid, batch, dbx_hash, orig_filename,
           orig_pg_start, orig_pg_end, doc_sha1,
           doc_filename, json) %>%
    mutate(jsonout = paste0(args$jsonout, "/",
                            str_replace(doc_filename, "\\.pdf$", ".json"))) %>%
    mutate(written = map2(json, jsonout, writeLines))

out <- out %>%
    transmute(doc_filename, doc_json = basename(jsonout), doc_sha1,
              batch, orig_filename, orig_pg_start, orig_pg_end)

invisible(out %>%
    mutate(pdf_in  = paste0(args$pdfin, "/", doc_filename),
           pdf_out = paste0(args$pdfout, "/", doc_filename)) %>%
    select(pdf_in, pdf_out) %>%
    mutate(copied = map2(pdf_in, pdf_out, file.copy)))

write_csv(out, args$report)

issues_name <- paste0(file_path_sans_ext(args$report), "-problems.csv")
issues <- issues %>%
    inner_join(dict, by="docid") %>%
    select(doc_filename, doc_sha1,
           batch, orig_filename, orig_pg_start, orig_pg_end,
           suspected_issues)

write_csv(issues, issues_name)

# done.
