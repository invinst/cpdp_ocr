# vim: set ts=8 softtabstop=0 sw=8 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/features/Makefile

.PHONY: all clean

all: output/all-features.feather output/training-features.feather

clean: 
		-rm -r output/*

output/%-features.feather: \
		src/lines2features.R \
		output/%-headings.feather \
		output/lda-model.rds \
		hand/regexes.yaml
	Rscript --vanilla $< \
		--input=input/$*-arr-lines.feather \
		--topics=output/lda-model.rds \
		--posheadings=output/$*-headings.feather \
		--regexes=hand/regexes.yaml \
		--output=$@

output/lda-model.rds: \
		src/train-topic-model.R \
		frozen/doc-word-counts.csv.gz
	Rscript --vanilla $< \
		--input=frozen/doc-word-counts.csv.gz \
		--output=$@ > $@-log.txt

output/%-headings.feather: \
		src/extract-headings.R \
		input/%-arr-lines.feather
	Rscript --vanilla $< \
		--input=input/$*-arr-lines.feather \
		--output=$@

# done.
