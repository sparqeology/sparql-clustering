import fs from 'fs';

import { parse } from 'csv-parse';
import generalizeAndAggregate from './generalizeAndAggregate.js';

const queriesInputStream = fs.createReadStream('./input/clustered_queries/clusters.csv');

const parser = parse({
    delimiter: ',',
    columns: ['groupId','query'],
    cast: (value, context) => {
        // if (context.header) return value;
        if (context.column === 0) return Number(value);
        return String(value);
    }
});

async function* filterStreamByGroup(inputStream, groupId) {
    [mainStream, localStream] = inputStream.tee();
    for await (const record of localStream) {
        if (record.groupId === groupId) {
            yield record.query;
        }
    }
}

async function* splitStreamByGroups(inputStream) {
    // [mainStream, localStream] = inputStream.tee();
    const streams = inputStream.tee();
    const mainStream = streams[0];
    const localStream = streams[1];
    const groupMap = {};
    for await (const record of localStream) {
        if (!(record.groupId in groupMap)) {
            groupMap[record.groupId] = true;
            yield {
                groupId: record.groupId,
                queryStream: filterStreamByGroup(mainStream, record.groupId)
            };
        }
    }
}

// const aggregatePromise = generalizeAndAggregate(parser, {
//     maxVars: 3, excludePreamble: true,
//     generalizationTree: true,
//     minNumOfExecutions: 50,
//     minNumOfHosts: 10,
//     includeSimpleQueries: true,
//     countInstances: true,
//     minBindingDivergenceRatio: 0.05,
//     // showBindingDistributions: true
// });
  
// const clusterStream = splitStreamByGroups(queriesInputStream.pipe(parser));
const clusterStream = splitStreamByGroups(queriesInputStream);

async function main() {
    for await (const cluster of clusterStream) {
        console.log('****************************************');
        console.log('**** Group ' + cluster.groupId + ' *****');
        console.log('****************************************');
        console.log('');
        for await (const query of cluster.queryStream) {
            console.log(query);
        }
    }
}

// aggregatePromise.then(result => {
//     // fs.writeFileSync('./output/queryTreeExtended2.json', JSON.stringify(result, null, 2), 'utf8');
//     // fs.writeFileSync('./output/queryTree_10_50.json', JSON.stringify(result, null, 2), 'utf8');
//     fs.writeFileSync('./output/queryTree_10_50_bis.json', JSON.stringify(result, null, 2), 'utf8');
//     // fs.writeFileSync('./output/queryTreeExtended_10_50.json', JSON.stringify(result, null, 2), 'utf8');
// }, err => {
//     console.error(err);
// });

main().then(result => {
    // fs.writeFileSync('./output/queryTreeExtended2.json', JSON.stringify(result, null, 2), 'utf8');
    // fs.writeFileSync('./output/queryTree_10_50.json', JSON.stringify(result, null, 2), 'utf8');
    console.log('OK');
    // fs.writeFileSync('./output/queryTreeExtended_10_50.json', JSON.stringify(result, null, 2), 'utf8');
}, err => {
    console.error(err);
});