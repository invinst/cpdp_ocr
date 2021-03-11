# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

.PHONY: all clean

narratives := ../merge/output/narratives.parquet
outputfile := output/sampled-narratives-20210305.xlsx

# sample this many per report type
samplesize := 35

all: $(outputfile)

clean: 
	-rm -r output/*

$(outputfile): src/sample.R $(narratives)
	-mkdir output
	Rscript --vanilla $< \
		--input=$(narratives) \
		--sampsize=$(samplesize) \
		--output=$@

# done.

