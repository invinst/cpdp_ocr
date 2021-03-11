# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

#pdfs := $(shell find ../download-pdfs/output/subsets -type f -iname '*.pdf')
fileindex := ../download-pdfs/output/metadata.csv
pdfs_delim := $(shell find ../download-pdfs/output/subsets -type f -iname '*.pdf' | tr '\n' '|')


output := output/web_complaint.parquet
cache := output/xml300

.PHONY: all clean

all: $(output)

clean: 
	-rm $(output)

$(output): src/ocr.R $(fileindex)
	-mkdir $(cache)
	Rscript --vanilla $< \
		--inputs="$(pdfs_delim)" \
		--xmldir=$(cache) \
		--DPI=300 \
		--output=$@

# done.
