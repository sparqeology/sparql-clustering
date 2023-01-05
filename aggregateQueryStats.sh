#!/bin/bash

echo "dataset,numOfQueries,numOfExecutions,begin,end,entropy"

for FILE_PATH in $1/*.csv; do
    DATASET_NAME=`basename $FILE_PATH .csv`
    tail -n +2 "$FILE_PATH" | csvquote | awk '
    BEGIN {
        FS = ",";
        OFS = ",";
        totNumOfExecutions = 0;
        totNumOfQueries = 0;
        begin = '9999';
        end = '0';
        entropyPartB = 0;
    }
    {
        id = $1;
        numOfExecutions = $2;
        timeOfFirstExecution = $3;
        timeOfLastExecution = $4;
        numOfHosts = $5;

        totNumOfExecutions += numOfExecutions;
        totNumOfQueries++;
        entropyPartB += numOfExecutions * log(numOfExecutions)
        if (timeOfFirstExecution < begin) {
            begin = timeOfFirstExecution;
        }
        if (timeOfLastExecution > end) {
            end = timeOfLastExecution;
        }
    }
    END {
        entropy = (log(totNumOfExecutions) - entropyPartB/totNumOfExecutions)/log(2); 
        print dataset,totNumOfQueries,totNumOfExecutions,begin,end,entropy;
    }' "dataset=$DATASET_NAME"
done