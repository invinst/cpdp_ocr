#!/bin/bash
set -ex

# scripts/try.sh INPUT_FOLDER OUTPUT_FOLDER SCRIPT <SCRIPT_ARG...>
# Try a python script in src folder
# e.g. scripts/try.sh sample_pdfs sample_pages ocr.py sample_pdfs

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
IN_DIR=$1
OUT_DIR=$2
SCRIPT=$3
shift
shift
shift

cd $DIR
docker build -t ocr .
mkdir -p $OUT_DIR
docker run --rm -v $DIR/$IN_DIR:/in/$IN_DIR -v $DIR/$OUT_DIR:/out -v $DIR/src:/src ocr /src/$SCRIPT $@