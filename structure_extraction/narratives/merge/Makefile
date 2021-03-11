# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

.PHONY: all clean

smrydigest := ../individual/smry_digest/extract/output/smry-digest-narratives.parquet
facesheet := ../../../6_reports/output/narratives.csv
webcomplaint := ../individual/web_complaint/extract/output/web-complaint-narratives.parquet

all: output/narratives.parquet

clean: 
	-rm -r output/*

output/narratives.parquet: src/combine.R \
	$(facesheet) $(smrydigest) $(webcomplaint)
	-mkdir output
	Rscript --vanilla $< \
		--faces=$(facesheet) \
		--smrydigest=$(smrydigest) \
		--webcomplaint=$(webcomplaint) \
		--output=$@ > output/merge-log.txt

# done.
