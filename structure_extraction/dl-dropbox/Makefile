# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

pageclasses := frozen/predictions_2020-01_2020-07.csv
token := frozen/auth-token.rds

.PHONY: all clean

all: output/ocir/metadata.csv

clean: 
		-rm -r output/*

output/ocir/metadata.csv: src/download.R $(pageclasses) $(token)
	-mkdir -p output/ocir/fullpdfs
	-mkdir -p output/ocir/subsets
	Rscript --vanilla $< \
		--pageclasses=$(pageclasses) \
		--token=$(token) \
		--type="Original Case Incident Report" \
		--outdir=output/ocir

# done.
