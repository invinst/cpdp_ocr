# vim: set ts=4 softtabstop=0 sw=4 si fileencoding=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/import/Makefile

.PHONY: all clean

proot := $(shell git rev-parse --show-toplevel)
toparse := $(proot)/6_reports/output/needs_structural_parsing.csv
dbname := cpdp_struct_parse

all: output/page-data.feather

clean: 
		-rm -r output/*

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
