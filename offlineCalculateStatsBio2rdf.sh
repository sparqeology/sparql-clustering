#!/bin/bash

DATASETS_CSV_PATH="./input/datasets.csv";
DB_LOC="/Applications/apache-jena-fuseki-4.6.1/run/databases/lsq2";
BASE_QUERY_PATH="./queries/statsFromBase";
BASE_OUTPUT_PATH="./output/stats";

for QUERY_PATH in $BASE_QUERY_PATH/*.rq; do
    QUERY_NAME=`basename $QUERY_PATH .rq`
    QUERY_OUTPUT_PATH="$BASE_OUTPUT_PATH/$QUERY_NAME"

    mkdir -p $QUERY_OUTPUT_PATH
    BASE_QUERY_COMMAND="tdb2.tdbquery --loc=$DB_LOC --file=$QUERY_PATH --results=CSV --base="

    OUTPUT_FILE="$QUERY_OUTPUT_PATH/bio2rdf.csv"

    echo "Computing $OUTPUT_FILE..."
    DATASET=http://lsq.aksw.org/datasets/bio2rdf
    $BASE_QUERY_COMMAND$DATASET/ >$OUTPUT_FILE
    echo "Done!"

done

