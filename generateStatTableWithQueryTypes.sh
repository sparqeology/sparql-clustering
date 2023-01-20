#!/bin/bash

STATS_DIR="./output/general/stats"
PAPER_LAYOUT="./input/datasetsForPaper.csv"
LATEX_OUTPUT="./output/general/dsTable.tex"
CSV_OUTPUT="./output/general/dsTableWithQueryTypes.csv"

echo "label,numOfQueries,numOfExecutions,begin,end,entropyForQueries,numOfHosts,numOfTemplates,entropyForTemplates,entropyForHosts,eS,eC,eA,eD,qS,qC,qA,qD,tS,tC,tA,tD,eSp,eCp,eAp,eDp,qSp,qCp,qAp,qDp,tSp,tCp,tAp,tDp" >$CSV_OUTPUT

awk 'BEGIN { FS=","; OFS=","; } {print $1,$2, $6}' <(tail -n +2 "$STATS_DIR/hostStats.csv" | sort) \
| join -t "," <(tail -n +2 "$STATS_DIR/queryStats.csv" | sort) - \
| join -t "," - <(awk 'BEGIN { FS=","; OFS=","; } {print $1,$2,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31}' <(tail -n +2 "$STATS_DIR/templateStats.csv" | sort)) \
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
    entropyForTemplates = 0 + $13;

    logQueriesTemplatesRatio = log(numOfQueries / numOfTemplates) / log(2);
    entropyDiff = entropyForQueries - entropyForTemplates;

    print label,numOfQueries,numOfExecutions,begin,end,entropyForQueries,numOfHosts,numOfTemplates,entropyForTemplates,entropyForHosts,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37;
}' >>$CSV_OUTPUT

