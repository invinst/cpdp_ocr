# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

docs := ../../pdf2hocr/output/ocir.feather

doclines := output/doclines.feather
docmargins := output/docmargins.feather
docregexes := output/docregx.feather
docoth := output/docother.feather

output := output/ocir.feather

.PHONY: all clean

all: $(output)

clean: 
		-rm -r output/*

$(output): \
		src/export.R \
		$(docs) \
		output/classifications.feather
	Rscript --vanilla $< \
		--records=$(docs) \
		--classes=output/classifications.feather \
		--output=$@

output/classifications.feather: \
		src/classify.R \
		output/all-features.feather \
		frozen/line-classifier.crfsuite
	Rscript --vanilla $< \
		--features=output/all-features.feather \
		--model=frozen/line-classifier.crfsuite \
		--output=$@

output/all-features.feather: \
		src/collect-features.R \
		$(docmargins) $(docregexes) $(docoth)
	Rscript --vanilla $< \
		--rxfeats=$(docregexes) \
		--othfeats=$(docoth) \
		--marginfeats=$(docmargins) \
		--output=$@

$(docmargins): \
		src/margin-features.R \
		$(doclines) \
		hand/section-names.yaml
	Rscript --vanilla $< \
		--input=$(doclines) \
		--sections=hand/section-names.yaml \
		--output=$@

$(docoth): \
		src/oth-features.R \
		$(doclines)
	Rscript --vanilla $< \
		--input=$(doclines) \
		--output=$@

$(docregexes): \
		src/regex-features.R \
		$(doclines) \
		hand/regexes.yaml
	Rscript --vanilla $< \
		--input=$(doclines) \
		--regexes=hand/regexes.yaml \
		--output=$@

$(doclines): \
		src/docs2lines.R \
		$(docs)
	-mkdir output
	Rscript --vanilla $< \
		--input=$(docs) \
		--output=$@

# done.
