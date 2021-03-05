# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

pageclasses := input/CPD-44.112-A_samples.csv
token := frozen/auth-token.rds

script := ../../../share/dl-and-subset.R

.PHONY: all clean

all: output/metadata.csv

output/metadata.csv: $(script) $(pageclasses) $(token)
	-mkdir -p output/fullpdfs
	-mkdir -p output/subsets
	Rscript --vanilla $< \
		--pageclasses=$(pageclasses) \
		--category="CPD-44.112-A" \
		--token=$(token) \
		--outdir=output

# done.
