# vim: set ts=4 sts=0 sw=4 si fenc=utf-8:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# setup {{{
pacman::p_load(
    argparse,
    crfsuite,
    dplyr,
    feather,
    logger,
    tidyr
)

parser <- ArgumentParser()
parser$add_argument("--features", default = "output/all-features.feather")
parser$add_argument("--model", default = "frozen/line-classifier.crfsuite")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}


feats <- read_feather(args$features)
model <- as.crf(args$model)

to_predict <- feats %>%
    mutate_at(vars(-docid, -docpg, -line_id), as.character) %>%
    pivot_longer(cols = c(-docid, -docpg, -line_id,
                          -pg_identifier, -ln_identifier),
                 names_to = "variable", values_to = "value") %>%
    filter(!is.na(value)) %>%
    mutate(value = paste0(variable, "=", value)) %>%
    pivot_wider(names_from = variable, values_from = value)

log_info("classifying new data")
log_info("there are ", nrow(to_predict), " rows to classify")
preds <- predict(model, newdata = to_predict, group = to_predict$pg_identifier)
log_info("generated ", nrow(preds), " labels")

stopifnot(nrow(preds) == nrow(to_predict))

out <- to_predict %>%
    mutate(section = preds$label, conf=preds$marginal) %>%
    select(docid, docpg, line_id, section, conf)

write_feather(out, args$output)

# done.
