# vim: set ts=8 softtabstop=0 sw=8 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/model/Makefile

.PHONY: all clean

modelfiles := output/crf-model.rds output/crf-model.crfsuite

all: output/segmented-arrest-reports.feather

clean: 
		-rm -r output/*

output/segmented-arrest-reports.feather: \
		src/export.R \
		output/all-section-labels.feather \
		input/page-data.feather
	Rscript --vanilla $< \
		--input=input/page-data.feather \
		--sections=output/all-section-labels.feather \
		--output=$@

output/all-section-labels.feather: \
		src/classify.R \
		input/all-features.feather \
		$(modelfiles)
	Rscript --vanilla $< \
		--input=input/all-features.feather \
		--classifier=output/crf-model.rds \
		--output=$@ > output/crf-classify-log.txt

$(modelfiles): \
		src/train-crf.R \
		input/training-features.feather
	Rscript --vanilla $< \
		--input=input/training-features.feather \
		--output=$@ > output/crf-training-log.txt

output/rf-model.rds: \
		src/train-rf.R \
		input/training-features.feather
	Rscript --vanilla $< \
		--input=input/training-features.feather \
		--output=$@ > output/rf-training-log.txt

# done.
