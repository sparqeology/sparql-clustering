#!/bin/bash

echo "dataset,numOfHosts,numOfExecutions,begin,end,entropy"

for FILE_PATH in $1/*.csv; do
    DATASET_NAME=`basename $FILE_PATH .csv`
    tail -n +2 "$FILE_PATH" | awk '
    BEGIN {
        FS = ",";
        OFS = ",";
        totNumOfExecutions = 0;
        totNumOfHosts = 0;
        begin = '9999';
        end = '0';
    }
    {
        id = $1;
        numOfExecutions = $2;
        numOfQueries = $3;
        timeOfFirstExecution = $4;
        timeOfLastExecution = substr($5,1,length($5)-1);

        totNumOfExecutions += numOfExecutions;
        totNumOfHosts++;
        entropyPartB += numOfExecutions * log(numOfExecutions)
        if (timeOfFirstExecution < begin) {
            begin = timeOfFirstExecution;
        }
        if (timeOfLastExecution > end) {
            end = timeOfLastExecution;
        }
    }
    END {
        if (totNumOfHosts > 0) {
            entropy = (log(totNumOfExecutions) - entropyPartB/totNumOfExecutions)/log(2); 
        } else {
            entropy = 0;
        }
        print dataset,totNumOfHosts,totNumOfExecutions,begin,end,entropy;
    }' "dataset=$DATASET_NAME"
done