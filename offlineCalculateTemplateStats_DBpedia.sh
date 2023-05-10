#!/bin/bash

DATASETS_CSV_PATH="./input/datasets.csv";
DB_LOC="/Applications/apache-jena-fuseki-4.6.1/run/databases/lsq2";
BASE_QUERY_PATH="./queries";
BASE_OUTPUT_PATH="./output/stats";

tdb2.tdbquery --loc=$DB_LOC --file=./queries/coOccurring.rq --results=CSV >./output/stats/coOccurence.csv
