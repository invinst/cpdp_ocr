# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

.PHONY: all clean

all: output/smry-digest-narratives.parquet

ocr_txt := ../ocr/output/smry_digest.parquet

clean: 
	-rm -r output/*

output/smry-digest-narratives.parquet: src/extract-smry-digest.R $(ocr_txt)
	-mkdir output
	Rscript --vanilla $< \
		--input=$(ocr_txt) \
		--output=$@ > output/smry-digest-log.txt

# done.
