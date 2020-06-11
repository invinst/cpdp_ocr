# vim: set ts=4 softtabstop=0 expandtab sw=4 si fe=utf-8:
#
# Authors:     TS
# Maintainers: TS
# Copyright:   2020, HRDAG, GPL v2 or later
# =========================================
# cpdp_ocr/6a_structural_parsing/Makefile

.PHONY: all clean

dbname := cpdp_struct_parse
all: output/db.log

clean: 
		-rm -r output/*

output/db.log: input/pg_dump.sql.gz
	dropdb --if-exists $(dbname)
	-createuser cpdp
	createdb $(dbname)
	zcat $< | psql $(dbname) > $@ 2>&1

# done.
