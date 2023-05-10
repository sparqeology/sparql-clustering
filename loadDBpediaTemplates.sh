#!/bin/bash

INPUT_DIR=./output/10_execs/rdf
STORE_LOC=/Applications/apache-jena-fuseki-4.6.1/run/databases/lsq2
DATASETS_NS=http://lsq.aksw.org/datasets
OUTPUT_GRAPH_SUFFIX=/templates/10_execs

echo tdb2.tdbloader --loc=$STORE_LOC --graph=$DATASETS_NS/bench-dbpedia.3.5.1$OUTPUT_GRAPH_SUFFIX $INPUT_DIR/bench-dbpedia.3.5.1.log-lsq2.nt.gz

echo tdb2.tdbloader --loc=$STORE_LOC --graph=$DATASETS_NS/dbpedia$OUTPUT_GRAPH_SUFFIX $INPUT_DIR/dbpedia.nt.gz
echo tdb2.tdbloader --loc=$STORE_LOC --graph=$DATASETS_NS/dbpedia$OUTPUT_GRAPH_SUFFIX $INPUT_DIR/dbpedia_1.nt.gz

echo tdb2.tdbloader --loc=$STORE_LOC --graph=$DATASETS_NS/dbpedia-2015$OUTPUT_GRAPH_SUFFIX $INPUT_DIR/dbpedia-2015.nt.gz
echo tdb2.tdbloader --loc=$STORE_LOC --graph=$DATASETS_NS/dbpedia-2015$OUTPUT_GRAPH_SUFFIX $INPUT_DIR/dbpedia-2015_1.nt.gz
