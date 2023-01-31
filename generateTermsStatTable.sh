#!/bin/bash

STATS_DIR="./output/general/stats"
PAPER_LAYOUT="./input/datasetsForPaper.csv"
CSV_OUTPUT="./output/general/dsTableTerms.csv"

echo "label,numOfQueries,numOfTemplates,maxNumOfOriginalRdfTerms,avgNumOfOriginalRdfTermsByQuery,avgNumOfOriginalRdfTermsByTemplate,maxNumOfParameters,avgNumOfParametersByQuery,avgNumOfParametersByTemplate" >$CSV_OUTPUT

awk 'BEGIN { FS=","; OFS=","; } {print $1,$2,$6}' <(tail -n +2 "$STATS_DIR/hostStats.csv" | sort) \
| join -t "," <(tail -n +2 "$STATS_DIR/queryStats.csv" | sort) - \
| join -t "," - <(awk 'BEGIN { FS=","; OFS=","; } {print $1,$2,$32,$33,$34,$35,$36,$37}' <(tail -n +2 "$STATS_DIR/templateStats.csv" | sort)) \
| join -t "," <(awk 'BEGIN { FS=","; OFS=","; } {print $1,NR,$2,$3}' <(tail -n +2 "$PAPER_LAYOUT") | sort) - \
| sort -t "," -k 2 -n \
| awk 'BEGIN { FS=","; OFS="," } {
    level = $3;
    label = $4;
    numOfQueries = $5;
    numOfExecutions = $6;
    begin = $7;
    end = $8;
    entropyForQueries = 0 + $9;
    numOfHosts = $10;
    entropyForHosts = 0 + $11;
    numOfTemplates = $12;

    maxNumOfOriginalRdfTerms = $13;
    avgNumOfOriginalRdfTermsByQuery = $14;
    avgNumOfOriginalRdfTermsByTemplate = $15;
    maxNumOfParameters = $16;
    avgNumOfParametersByQuery = $17;
    avgNumOfParametersByTemplate = $18;

    if (level == 1) {
        print label,numOfQueries,numOfTemplates,maxNumOfOriginalRdfTerms,avgNumOfOriginalRdfTermsByQuery,avgNumOfOriginalRdfTermsByTemplate,maxNumOfParameters,avgNumOfParametersByQuery,avgNumOfParametersByTemplate;
    }
}' >>$CSV_OUTPUT

