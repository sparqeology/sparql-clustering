#!/bin/bash

for FILE_PATH in $1/*_execs.nt.gz; do
    FILE_NAME=`basename $FILE_PATH _execs.nt.gz`
    echo "Deleting execs and queries of... $FILE_NAME"
    echo "DROP GRAPH <http://lsq.aksw.org/datasets/${FILE_NAME}>;" >./tmp/drop.ru
    cat ./tmp/drop.ru
    tdb2.tdbupdate --loc=/Applications/apache-jena-fuseki-4.6.1/run/databases/lsq2 --update=./tmp/drop.ru
    rm ./tmp/drop.ru
    echo "Done!"
done

