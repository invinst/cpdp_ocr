# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

script := src/pdf2hocr.R
index := ../dl-dropbox/output/ocir/metadata.csv

DPI := 300

.PHONY: all clean

all: output/ocir.feather

clean: 
	-rm output/ocir-hocr.feather

output/ocir.feather: \
		$(script) \
		$(index)
	-mkdir -p output/xml$(DPI)
	Rscript --vanilla $< \
		--index=$(index) \
		--xmldir=output/xml$(DPI) \
		--DPI=$(DPI) \
		--output=$@ > output/ocr-log.txt

# done.
