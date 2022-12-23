#!/bin/bash

for FILE_PATH in $1/*.nt.gz; do
    FILE_NAME=`basename $FILE_PATH .nt.gz`
    echo "Loading... $FILE_NAME"
    tdb2.tdbloader --loc=/Applications/apache-jena-fuseki-4.6.1/run/databases/lsq2 --graph=http://lsq.aksw.org/datasets/$FILE_NAME $FILE_PATH
    echo "Done!"
done
