#!/bin/bash

echo "@prefix ds: <http://lsq.aksw.org/datasets/>."
echo "@prefix sd: <http://www.w3.org/ns/sparql-service-description#>."
echo "<http://lsq.aksw.org/datasets> {"
echo ""
echo "  ds: sd:namedGraph"

FIRST=1
for FILE_PATH in $1/*.nt; do
    FILE_NAME=`basename $FILE_PATH .nt`
    if ((FIRST == 0)); then
        echo ","
    fi
    FIRST=0
#    echo ""
    echo -n "    ds:$FILE_NAME"
done
echo "."
echo ""

for FILE_PATH in $1/*.nt; do
    FILE_NAME=`basename $FILE_PATH .nt`
    echo "  ds:$FILE_NAME a sd:NamedGraph."
done
echo ""

echo "}"

