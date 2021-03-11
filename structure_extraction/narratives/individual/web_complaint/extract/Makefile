# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

.PHONY: all clean

all: output/web-complaint-narratives.parquet

ocr_txt := ../ocr/output/web_complaint.parquet

clean: 
	-rm -r output/*

output/web-complaint-narratives.parquet: src/extract-web-complaint.R $(ocr_txt)
	-mkdir output
	Rscript --vanilla $< \
		--input=$(ocr_txt) \
		--output=$@ > output/web-complaint-log.txt

# done.
