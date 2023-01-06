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

    tail -n +1 "$DATASETS_CSV_PATH" | csvquote | awk '
    BEGIN {
        FS = ",";
    }
    {
        dataset = $1;
        datasetId = $2;
        endpoint = $3;
        prefixesJson = $4;

        outputFile = outputPath "/" datasetId ".csv";

        print "Computing " outputFile "...";
        command = baseCommand dataset "/ >" outputFile;
        system(command);
        print "Done!";
        
    }' "baseCommand=$BASE_QUERY_COMMAND" "outputPath=$QUERY_OUTPUT_PATH"

done

