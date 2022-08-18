import fs from 'fs';

import { parse } from 'csv-parse';

import generalizeAndAggregate from './generalizeAndAggregate.js';
import tee from './tee.js'

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
    const [localStream, mainStream] = tee(inputStream);
    console.log('group:' + groupId);
    for await (const record of localStream) {
        if (record.groupId === groupId) {
            yield record.query;
        }
    }
}

async function* splitStreamByGroups(inputStream) {
    var [localStream, mainStream] = tee(inputStream);
    const groupMap = {};
    for await (const record of localStream) {
        if (!(record.groupId in groupMap)) {
            groupMap[record.groupId] = true;
            var [mainStream, streamCopy] = tee(mainStream);
            yield {
                groupId: record.groupId,
                queryStream: filterStreamByGroup(streamCopy, record.groupId)
            };
        }
    }
}

async function* map(inputGenerator, fn) {
    for await (const data of inputGenerator) {
        yield fn(data);
    }
}

async function generalizeByCluster(globalStream, options) {
    const clusterStream = splitStreamByGroups(globalStream);
    const paramQueriesByCluster = {};
    for await (const cluster of clusterStream) {
        console.log('****************************************');
        console.log('**** Group ' + cluster.groupId + ' *****');
        console.log('****************************************');
        console.log('');
        const queryObjStream = map(cluster.queryStream, query => ({
            text: query,
            numOfExecutions: 1,
            numOfHosts: 1
        }));
        paramQueriesByCluster['group_' + cluster.groupId] = await generalizeAndAggregate(queryObjStream, options);
        // for await (const query of queryObjStream) {
        //     console.log(query);
        // }
    }
    // console.log(paramQueriesByCluster);
    return paramQueriesByCluster;
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

queriesInputStream.pipe(parser);

async function main() {
    const paramQueriesByCluster = await generalizeByCluster(parser, {
        maxVars: 5,
        excludePreamble: true,
        generalizationTree: true,
        onlyRoots: true,
        includeSimpleQueries: true,
        countInstances: true,
        minBindingDivergenceRatio: 0.05
    });
    fs.writeFileSync('./output/queryRootsByCluster.json', JSON.stringify(paramQueriesByCluster, null, 2), 'utf8');
}

main().then(result => {
    console.log('OK');
}, err => {
    console.error(err);
});