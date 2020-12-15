# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

input := ../segment/output/ocir.feather
pdfin := ../../dl-dropbox/output/ocir/subsets
pdfout := output/pdf
jsonout := output/json
dict := ../../dl-dropbox/output/ocir/metadata.csv

.PHONY: all clean

all: output/index.csv

clean: 
	-rm -r output/*

output/index.csv: src/extract.R $(input)
	-mkdir -p $(pdfout)
	-mkdir -p $(jsonout)
	Rscript --vanilla $< \
		--input=$(input) \
		--dict=$(dict) \
		--pdfin=$(pdfin) \
		--jsonout=$(jsonout) \
		--pdfout=$(pdfout) \
		--report=$@ > $@-log.txt


