#!/bin/bash

echo "dataset,numOfTemplates,numOfQueries,numOfExecutions,begin,end,entropy,infoGain"

for FILE_PATH in $1/*.csv; do
    DATASET_NAME=`basename $FILE_PATH .csv`
    tail -n +3 "$FILE_PATH" | csvquote | awk '
    BEGIN {
        FS = ",";
        OFS = ",";
        totNumOfExecutions = 0;
        totNumOfTemplates = 0;
        totNumOfQueries = 0;
        begin = '9999';
        end = '0';
        entropyPartB = 0;
        infoGainPart = 0;
    }
    {
        id = $1;
        numOfInstances = $3;
        numOfExecutions = $4;
        timeOfFirstExecution = $5;
        timeOfLastExecution = $6;
        sumOfLogOfNumOfExecutions = $7;
        numOfSpecializations = $8;

        totNumOfExecutions += numOfExecutions;
        totNumOfQueries += numOfInstances;
        totNumOfTemplates++;

        entropyPartB += numOfExecutions * log(numOfExecutions);

        infoGainPart += numOfExecutions * (numOfInstances * numOfExecutions - sumOfLogOfNumOfExecutions);

        if (timeOfFirstExecution < begin) {
           begin = timeOfFirstExecution;
        }
        if (timeOfLastExecution > end) {
           end = timeOfLastExecution;
        }
    }
    END {
        entropy = (log(totNumOfExecutions) - entropyPartB/totNumOfExecutions)/log(2); 
        infoGain = infoGainPart / totNumOfExecutions;
        print dataset,totNumOfTemplates,totNumOfQueries,totNumOfExecutions,begin,end,entropy,infoGain;
    }' "dataset=$DATASET_NAME"
done