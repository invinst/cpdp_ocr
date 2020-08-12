# vim: set ts=4 softtabstop=0 noexpandtab sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/Makefile

.PHONY: all clean

proot := $(shell git rev-parse --show-toplevel)
toparse := $(proot)/6_reports/output/needs_structural_parsing.csv
dbname := cpdp_struct_parse

all: output/arrest-report-incident-narratives.feather

clean: 
		-rm -r output/*

output/arrest-report-incident-narratives.feather: \
		src/ar-incident-reports.R \
		output/arrest-report-content-sections.feather \
		output/arrest-report-position-sections.feather \
		output/page-data.feather
	Rscript --vanilla $< \
			--positional=output/arrest-report-position-sections.feather \
			--contextual=output/arrest-report-content-sections.feather \
			--docs=output/page-data.feather \
			--output=$@

output/arrest-report-position-sections.feather: \
		src/arrest-report-section-by-position.R \
		output/page-data.feather \
		hand/arrest-report-sections.yaml
	Rscript --vanilla $< \
			--input=output/page-data.feather \
			--sections=hand/arrest-report-sections.yaml \
			--output=$@

output/arrest-report-content-sections.feather: \
		src/arrest-report-section-by-content.R \
		output/page-data.feather \
		hand/arrest-report-section-hints.csv
	Rscript --vanilla $< \
			--input=output/page-data.feather \
			--hints=hand/arrest-report-section-hints.csv \
			--output=$@

output/page-data.feather: \
		src/pull-pages.R \
		output/db.log \
		$(toparse)
	Rscript --vanilla $< \
			--input=$(toparse) \
			--output=$@

output/db.log: input/pg_dump.sql.gz
	dropdb --if-exists $(dbname)
	-createuser cpdp
	createdb $(dbname)
	zcat $< | psql $(dbname) > $@ 2>&1

# done.
