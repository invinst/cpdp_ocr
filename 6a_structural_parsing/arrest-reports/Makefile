# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/arrest-reports/Makefile

trainingfiles := $(shell find hand/section-labels-training-data -type f)
wordcounts := frozen/doc-word-counts.csv.gz

input := input/page-data.feather
topicmodel := output/lda-model.rds
classifier := output/model-classify-section.rds

.PHONY: all clean

all: output/segmented-arrest-reports.feather

clean: 
		-rm -r output/*

output/segmented-arrest-reports.feather: \
		src/classify.R \
		$(input) \
		$(topicmodel) \
		$(classifier)
	Rscript --vanilla $< \
			--input=$(input) \
			--topics=$(topicmodel) \
			--classifier=$(classifier) \
			--output=$@

$(classifier): \
		src/train-classifier.R \
		$(trainingfiles) \
		$(topicmodel)
	Rscript --vanilla $< \
			--trainingdir=hand/section-labels-training-data \
			--topics=$(topicmodel) \
			--output=$@ > output/classifier-training.log

$(topicmodel): \
		src/train-topic-model.R \
		$(wordcounts)
	Rscript --vanilla $< \
			--wordcounts=$(wordcounts) \
			--output=$@

# done.
