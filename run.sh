#!/bin/bash

pushd 0_setup
./setup.sh
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
