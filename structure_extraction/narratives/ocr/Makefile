# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

xmldir   := output/xml300
filelist := output/to-import.txt
output   := output/cpd.parquet
meta     := output/cpd-info.csv

.PHONY: all clean

all: $(output) $(meta)

clean: 
	-rm $(filelist)
	-rm $(output)
	-rm $(meta)

$(output): src/import.R $(filelist)
	Rscript --vanilla $< \
		--files=$(filelist) \
		--xmldir=$(xmldir) \
		--DPI=300 \
		--output=$@

$(meta): $(filelist)
	cat $(filelist) | xargs sha1sum | sed 's/\s\+/|/' > $@

$(filelist):
	-mkdir output
	find input -type f -iname '*.pdf' > $@

