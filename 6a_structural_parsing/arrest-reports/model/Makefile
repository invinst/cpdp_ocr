# vim: set ts=8 softtabstop=0 sw=8 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/model/Makefile

.PHONY: all clean

all: output/all-section-labels.feather

clean: 
		-rm -r output/*

output/all-section-labels.feather: \
		src/classify.R \
		input/all-features.feather \
		output/rf-model.rds
	Rscript --vanilla $< \
		--input=input/all-features.feather \
		--classifier=output/rf-model.rds \
		--output=$@ > output/rf-classify-log.txt

output/rf-model.rds: \
		src/train-rf.R \
		input/training-features.feather
	Rscript --vanilla $< \
		--input=input/training-features.feather \
		--output=$@ > output/rf-training-log.txt

# done.
