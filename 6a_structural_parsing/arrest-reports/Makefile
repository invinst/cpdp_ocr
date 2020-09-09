# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/Makefile

trainingfiles := $(shell find hand/section-labels-training-data -type f)

.PHONY: all clean

all: output/position-sections.feather \
	output/content-sections.feather \
	output/training-features.feather

clean: 
		-rm -r output/*

output/training-features.feather: \
		src/prep-training-data.R \
		input/page-data.feather \
		output/position-sections.feather \
		$(trainingfiles)
	Rscript --vanilla $< \
			--input=input/page-data.feather \
			--position=output/position-sections.feather \
			--trainingdir=hand/section-labels-training-data \
			--train=output/training-features.feather \
			--test=output/testing-features.feather

output/position-sections.feather: \
		src/section-by-position.R \
		input/page-data.feather \
		hand/section-info.csv
	Rscript --vanilla $< \
			--input=input/page-data.feather \
			--sections=hand/section-info.csv \
			--output=$@

output/content-sections.feather: \
		src/section-by-content.R \
		input/page-data.feather \
		hand/section-info.csv
	Rscript --vanilla $< \
			--input=input/page-data.feather \
			--hints=hand/section-info.csv \
			--output=$@

# done.
