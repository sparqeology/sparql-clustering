#!/bin/bash

STATS_DIR="./output/general/stats"
PAPER_LAYOUT="./input/datasetsForPaper.csv"
LATEX_OUTPUT="./output/general/dsTable.tex"
CSV_OUTPUT="./output/general/dsTable.csv"

echo "% Latex generated for the table of datasets according to the following format." >$LATEX_OUTPUT
echo "%" >>$LATEX_OUTPUT
echo "% \primaryDataset{label}{numOfQueries}{numOfExecutions}{entropyForQueries}{numOfHosts}{numOfTemplates}{entropyForTemplates}{informationGain}" >>$LATEX_OUTPUT
echo "% \secondaryDataset{label}{numOfQueries}{numOfExecutions}{entropyForQueries}{numOfHosts}{numOfTemplates}{entropyForTemplates}{informationGain}" >>$LATEX_OUTPUT
echo "" >>$LATEX_OUTPUT

# echo "dsLabel,dsNumOfQueries,dsNumOfExecutions,dsBegin,dsEnd,dsEntropyForQueries,dsNumOfHosts,dsNumOfTemplates,dsEntropyForTemplates,dsInformationGain" >$CSV_OUTPUT

#awk 'BEGIN { FS=","; OFS=","; } {print $1,NR,$2,$3}' <(tail -n +2 "$PAPER_LAYOUT") | sort

awk 'BEGIN { FS=","; OFS=","; } {print $1,$2}' <(tail -n +2 "$STATS_DIR/hostStats.csv" | sort) \
| join -t "," <(tail -n +2 "$STATS_DIR/queryStats.csv" | sort) - \
| join -t "," - <(awk 'BEGIN { FS=","; OFS=","; } {print $1,$2,$7,$8}' <(tail -n +2 "$STATS_DIR/templateStats.csv" | sort)) \
| join -t "," <(awk 'BEGIN { FS=","; OFS=","; } {print $1,NR,$2,$3}' <(tail -n +2 "$PAPER_LAYOUT") | sort) - \
| sort -t "," -k 2 -n \
| awk 'BEGIN { FS=","; OFS=","; CONVFMT="%.2f"; } {
    level = $3;
    label = $4;
    numOfQueries = $5;
    numOfExecutions = $6;
    begin = $7;
    end = $8;
    entropyForQueries = 0 + $9;
    numOfHosts = $10;
    numOfTemplates = $11;
    entropyForTemplates = 0 + $12;
    informationGain = 0 + $13;

    if (NR > 1) {
        print "\\\\";
    }

    row = "{" label "}{" numOfQueries "}{" numOfExecutions "}{" entropyForQueries "}{" numOfHosts  "}{" numOfTemplates "}{" entropyForTemplates "}{" informationGain "}";
    if (level == "1") {
        print "\\primaryDataset" row;
    } else {
        print "\\secondaryDataset" row;
    }

    # if (level == "1") {
    #     print "\\begin{primaryDataset}";
    # } else {
    #     print "\\begin{secondaryDataset}";
    # }

    # print "\\providecommand{\\dsLabel}{" label "}";
    # print "\\providecommand{\\dsNumOfQueries}{" numOfQueries "}";
    # print "\\providecommand{\\dsNumOfExecutions}{" numOfExecutions "}";
    # print "\\providecommand{\\dsBegin}{" begin "}";
    # print "\\providecommand{\\dsEnd}{" end "}";
    # print "\\providecommand{\\dsEntropyForQueries}{" entropyForQueries "}";
    # print "\\providecommand{\\dsNumOfHosts}{" numOfHosts "}";
    # print "\\providecommand{\\dsNumOfTemplates}{" numOfTemplates "}";
    # print "\\providecommand{\\dsEntropyForTemplates}{" entropyForTemplates "}";
    # print "\\providecommand{\\dsInformationGain}{" informationGain "}";

    # if (level == "1") {
    #     print "\\end{primaryDataset}";
    # } else {
    #     print "\\end{secondaryDataset}";
    # }

    # print label,numOfQueries,numOfExecutions,begin,end,entropyForQueries,numOfHosts,numOfTemplates,entropyForTemplates,informationGain;
}' >>$LATEX_OUTPUT

