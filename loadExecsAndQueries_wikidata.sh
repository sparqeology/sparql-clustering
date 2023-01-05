#!/bin/bash

for FILE_PATH in $1/*_execs.nt.gz; do
    FILE_NAME=`basename $FILE_PATH _execs.nt.gz`
    echo "Loading execs of... $FILE_NAME"
    tdb2.tdbloader --loc=/Applications/apache-jena-fuseki-4.6.1/run/databases/lsq2 --graph=http://lsq.aksw.org/datasets/wikidata/execs $FILE_PATH
    echo "Done!"
done

for FILE_PATH in $1/*_queries.nt.gz; do
    FILE_NAME=`basename $FILE_PATH _queries.nt.gz`
    echo "Loading queries of... $FILE_NAME"
    tdb2.tdbloader --loc=/Applications/apache-jena-fuseki-4.6.1/run/databases/lsq2 --graph=http://lsq.aksw.org/datasets/wikidata/queries $FILE_PATH
    echo "Done!"
done
