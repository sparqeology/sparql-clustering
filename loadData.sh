#!/bin/bash

for FILE_PATH in $1/*.nt; do
    FILE_NAME=`basename $FILE_PATH .nt`
    echo "Loading... $FILE_NAME"
    s-put http://localhost:3030/lsq2/data http://lsq.aksw.org/datasets/$FILE_NAME $FILE_PATH
    echo "Done!"
done
