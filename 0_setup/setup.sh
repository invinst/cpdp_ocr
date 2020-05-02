#!/bin/bash

#mkdir -p /dev/shm/cpdp_pdfs/pdfs
#mkdir -p /opt/data/cpdp_pdfs/ocrd

sudo su postgres -c "psql < <(echo 'DROP DATABASE cpdp ; CREATE DATABASE cpdp ;')"

psql -U cpdp -d cpdp < setup_database.sql

python3 setup_batches.py

#should return nothing if no duplicate files exist
#find ./output/ -name \*pdf | xargs -P12 -I{} md5sum {}  | awk '{print $1}' | sort | uniq -c | awk '$1 > 1 {print "Duplicate Files!"}'
