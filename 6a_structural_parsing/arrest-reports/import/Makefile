# vim: set ts=8 softtabstop=0 sw=8 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/import/Makefile

trainingfiles := $(shell find frozen/section-labels-training-data -type f)

.PHONY: all clean

all: output/all-arr-lines.feather \
	output/training-arr-lines.feather

clean: 
		-rm -r output/*

output/all-arr-lines.feather: \
		src/docs2lines.R \
		input/page-data.feather
	Rscript --vanilla $< \
		--input=input/page-data.feather \
		--output=$@

output/training-arr-lines.feather: \
		src/import-training-data.R \
		frozen/training-data-info.csv \
		$(trainingfiles)
	Rscript --vanilla $< \
		--info=frozen/training-data-info.csv \
		--output=$@ > output/training-data-summary.txt

# done.
