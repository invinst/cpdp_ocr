#!/bin/bash

export OMP_THREAD_LIMIT=1

mkdir 0_setup/output
mkdir 1_ocr/output
mkdir 2_classifiers/output
mkdir 3_parsers/output
mkdir 4_uploaders/output
mkdir 6_setup/output

pushd 0_setup
./clean.sh
./run.sh
popd

pushd 1_ocr
./run.sh
popd

pushd 2_classifiers
.run.sh
popd

pushd 3_parsers
./run.sh
popd

pushd 4_uploaders
psql -U cpdp -d cpdp < post_insert_updates.sql
popd

pushd 6_reports
