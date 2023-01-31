#!/bin/bash

INPUT_DIR="./output/rootTemplates-all/stats"
OUTPUT_FILE="./output/general/stats/templateStats.csv"

echo "dataset,numOfTemplates,numOfQueries,numOfExecutions,begin,end,entropyWithTemplates,eS,eC,eA,eD,qS,qC,qA,qD,tS,tC,tA,tD,eS%,eC%,eA%,eD%,qS%,qC%,qA%,qD%,tS%,tC%,tA%,tD%,maxNumOfOriginalRdfTerms,avgNumOfOriginalRdfTermsByQuery,avgNumOfOriginalRdfTermsByTemplate,maxNumOfParameters,avgNumOfParametersByQuery,avgNumOfParametersByTemplate" >"$OUTPUT_FILE"

for FILE_PATH in $INPUT_DIR/*.csv; do
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
        queryTypes[1] = "S";
        queryTypes[2] = "C";
        queryTypes[3] = "A";
        queryTypes[4] = "D";
        numQueryTypes = 4;
        for (queryTypeIndex = 1; queryTypeIndex <= numQueryTypes; queryTypeIndex++) {
            queryType = queryTypes[queryTypeIndex];
            totNumOfQueriesByType[queryType] = 0;
            totNumOfTemplatesByType[queryType] = 0;
            totNumOfExecutionsByType[queryType] = 0;
        }
        totWeightedNumOfOriginalRdfTerms = 0;
        totNumOfOriginalRdfTerms = 0;
        totWeightedNumOfParameters = 0;
        totNumOfParameters = 0;
        maxNumOfOriginalRdfTerms = 0;
        maxNumOfParameters = 0;
    }
    {
        id = $1;
        queryText = $2;
        numOfInstances = $3;
        numOfExecutions = $4;
        timeOfFirstExecution = $5;
        timeOfLastExecution = $6;
        sumOfLogOfNumOfExecutions = $7;
        numOfSpecializations = $8;
        numOfOriginalRdfTerms = $9;
        numOfParameters = $10;

        totNumOfExecutions += numOfExecutions;
        totNumOfQueries += numOfInstances;
        totNumOfTemplates++;

        totWeightedNumOfOriginalRdfTerms += numOfOriginalRdfTerms * numOfInstances;
        totNumOfOriginalRdfTerms += numOfOriginalRdfTerms;
        totWeightedNumOfParameters += numOfParameters * numOfInstances;
        totNumOfParameters += numOfParameters;

        if (numOfOriginalRdfTerms > maxNumOfOriginalRdfTerms) {
            maxNumOfOriginalRdfTerms = numOfOriginalRdfTerms;
        }
        if (numOfParameters > maxNumOfParameters) {
            maxNumOfParameters = numOfParameters;
        }

        entropyPartB += numOfExecutions * log(numOfExecutions);

        infoGainPart += numOfExecutions * (numOfInstances * log(numOfExecutions)/log(2) - sumOfLogOfNumOfExecutions);

        if (timeOfFirstExecution < begin) {
           begin = timeOfFirstExecution;
        }
        if (timeOfLastExecution > end) {
           end = timeOfLastExecution;
        }

        ucQueryText = toupper(queryText);

        for (queryType in totNumOfQueriesByType) {
            isQueryType[queryType] = 0;
        }
        if (ucQueryText ~ /CONSTRUCT/) {
            isQueryType["C"] = 1;
        } else if (ucQueryText ~ /ASK/) {
            isQueryType["A"] = 1;
        } else if (ucQueryText ~ /DESCRIBE/) {
            isQueryType["D"] = 1;
        } else if (ucQueryText ~ /SELECT/) {
            isQueryType["S"] = 1;
        }
        for (queryType in totNumOfQueriesByType) {
            totNumOfQueriesByType[queryType] += isQueryType[queryType] * numOfInstances;
            totNumOfExecutionsByType[queryType] += isQueryType[queryType] * numOfExecutions;
            totNumOfTemplatesByType[queryType] += isQueryType[queryType];
        }


    }
    END {
        entropy = (log(totNumOfExecutions) - entropyPartB/totNumOfExecutions)/log(2); 
        infoGain = infoGainPart / totNumOfExecutions;

        avgNumOfOriginalRdfTermsByQuery = totWeightedNumOfOriginalRdfTerms / totNumOfQueries;
        avgNumOfOriginalRdfTermsByTemplate = totNumOfOriginalRdfTerms / totNumOfTemplates;
        avgNumOfParametersByQuery = totWeightedNumOfParameters / totNumOfQueries;
        avgNumOfParametersByTemplate = totNumOfParameters / totNumOfTemplates;


        numOfExecutionsByTypeVector = "";
        numOfQueriesByTypeVector = "";
        numOfTemplatesByTypeVector = "";
        percOfExecutionsByTypeVector = "";
        percOfQueriesByTypeVector = "";
        percOfTemplatesByTypeVector = "";
        for (queryTypeIndex = 1; queryTypeIndex <= numQueryTypes; queryTypeIndex++) {
            queryType = queryTypes[queryTypeIndex];
            numOfExecutionsByTypeVector = numOfExecutionsByTypeVector "," totNumOfExecutionsByType[queryType];
            numOfQueriesByTypeVector = numOfQueriesByTypeVector "," totNumOfQueriesByType[queryType];
            numOfTemplatesByTypeVector = numOfTemplatesByTypeVector "," totNumOfTemplatesByType[queryType];
            percOfExecutionsByTypeVector = percOfExecutionsByTypeVector "," (totNumOfExecutionsByType[queryType] * 100 / totNumOfExecutions);
            percOfQueriesByTypeVector = percOfQueriesByTypeVector "," (totNumOfQueriesByType[queryType] * 100 / totNumOfQueries);
            percOfTemplatesByTypeVector = percOfTemplatesByTypeVector "," (totNumOfTemplatesByType[queryType] * 100 / totNumOfTemplates);
        }
        typeVector = substr(numOfExecutionsByTypeVector,2) numOfQueriesByTypeVector numOfTemplatesByTypeVector percOfExecutionsByTypeVector percOfQueriesByTypeVector percOfTemplatesByTypeVector;

        print dataset,totNumOfTemplates,totNumOfQueries,totNumOfExecutions,begin,end,entropy,typeVector,maxNumOfOriginalRdfTerms,avgNumOfOriginalRdfTermsByQuery,avgNumOfOriginalRdfTermsByTemplate,maxNumOfParameters,avgNumOfParametersByQuery,avgNumOfParametersByTemplate;
    }' "dataset=$DATASET_NAME" >>"$OUTPUT_FILE"
done