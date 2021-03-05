# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# libs {{{
pacman::p_load(
    argparse,
    arrow,
    dplyr,
    magick,
    purrr,
    stringr,
    tidygraph,
    tidyr
)
# }}}

# args {{{
parser <- ArgumentParser()
parser$add_argument("--input", default = "../ocr/output/cpd.parquet")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

# load data {{{
cpd <- read_parquet(args$input)

meta <- readr::read_delim("../ocr/output/cpd-info.csv", delim="|",
                          col_names=c("filesha1", "filename") ,col_types='cc')
meta <- meta %>% mutate(doctype = str_split(filename, "/") %>% map_chr(2))
# }}}

# layout fns {{{
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

y_distance <- function(box1, box2) {
    distance(box1$y0, box1$y1, box2$y0, box2$y1)
}

x_distance <- function(box1, box2) {
    distance(box1$x0, box1$x1, box2$x0, box2$x1)
}

overlap <- function(a.start, a.end, b.start, b.end) {
    unadjusted <- pmin(a.end, b.end) - pmax(a.start, b.start)
    unadjusted <- pmax(0, unadjusted)
    max_overlap <- pmax(a.end-a.start, b.end-b.start)
    unadjusted/max_overlap
}

x_overlap = function(box1, box2) {
    overlap(box1$x0, box1$x1, box2$x0, box2$x1)
}

y_overlap = function(box1, box2) {
    overlap(box1$y0, box1$y1, box2$y0, box2$y1)
}
# }}}

# define adjacency {{{
same_group <- function(box1, box2) {
    sz <- pmin(box1$sz, box2$sz)
    x_distance(box1, box2) < sz & y_distance(box1, box2) < sz
}
# }}}

pad_num <- function(number, width=4) {
    str_pad(number, width=width, pad="0", side="left")
}

chunk_boxes <- cpd %>%
    filter(str_count(text, "[A-Za-z0-9]") >= 1) %>%
    #     filter(fileid==doc, pg==fpg) %>%
    mutate(word_id=pad_num(word_id)) %>%
    nest(data=c(-fileid, -pg)) %>%
    mutate(data=map(data, calc_groupid, is_edge=same_group, key="word_id")) %>%
    unnest(data) %>%
    mutate(word_id=as.integer(word_id)) %>%
    inner_join(cpd, by=c("fileid", "pg", "word_id")) %>%
    group_by(fileid, pg, chunkid=groupid) %>%
    summarise(x0=min(x0), x1=max(x1), y0=min(y0), y1=max(y1), .groups="drop")

out <- cpd %>%
    inner_join(chunk_boxes %>%
               rename(chunk.x0=x0, chunk.x1=x1, chunk.y0=y0, chunk.y1=y1) %>%
               mutate(chunk.area = (chunk.x1-chunk.x0) * (chunk.y1-chunk.y0)),
               by=c("fileid", "pg")) %>%
    filter(x0 >= chunk.x0, x1 <= chunk.x1, y0 >= chunk.y0, y1 <= chunk.y1) %>%
    group_by(fileid, pg, word_id) %>%
    filter(chunk.area == max(chunk.area)) %>% ungroup

# should guarantee that every word belongs to at most 1 chunk
ambiguous <- out %>% group_by(fileid, pg, word_id) %>% filter(n() > 1)
stopifnot(nrow(ambiguous) == 0)

write_parquet(out, args$output)

# done.
