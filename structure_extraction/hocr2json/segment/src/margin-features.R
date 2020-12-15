# vim: set ts=4 sts=0 sw=4 si fenc=utf-8:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# frontmatter {{{
library(pacman)
pacman::p_load(argparse, feather, dplyr, yaml, stringdist, tidyr, purrr)

parser <- ArgumentParser()
parser$add_argument("--input", default = "output/doclines.feather")
parser$add_argument("--sections", default = "hand/section-names.yaml")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

docs <- read_feather(args$input)
secs <- read_yaml(args$sections)

candidates <- docs %>%
    filter(x1 < .5) %>%
    distinct(text)

section_xref <- bind_cols(candidates,
          map_dfc(secs,
                  ~stringdist(candidates$text, ., method="cosine", q=4))) %>%
    pivot_longer(-text, names_to="candidate", values_to="distance") %>%
    filter(distance < .7) %>%
    group_by(text) %>%
    filter(distance == min(distance)) %>%
    nest %>% ungroup %>%
    mutate(data=map(data, sample_n, 1)) %>%
    unnest(data) %>%
    distinct(text, section_name = candidate)

sections <- docs %>%
    filter(x1 < .5) %>%
    inner_join(section_xref, by = "text") %>%
    transmute(docid, docpg, section_name, y0, y1)

overlaps <- sections %>% inner_join(sections, by=c("docid", "docpg"),
                        suffix=c(".a", ".b")) %>%
    filter(y0.b >= y0.a, y1.b <= y1.a,
           section_name.a != section_name.b) %>%
    select(docid, docpg, section_name=section_name.a,
           y0=y0.a, y1=y1.a)

sections <- sections %>%
    anti_join(overlaps, by=c("docid", "docpg", "section_name", "y0", "y1"))

matched_sections <- docs %>%
    inner_join(sections, by = c("docid", "docpg"),
               suffix = c("_a", "_b")) %>%
    filter(is.na(y0_b) | (y0_a >= y0_b & y1_a <= y1_b)) %>%
    distinct(docid, docpg, line_id, section_name)

overmatched <- matched_sections %>%
    group_by(docid, docpg, line_id) %>%
    filter(n() > 1)

stopifnot(nrow(overmatched) == 0)

write_feather(matched_sections, args$output)

# done.
