import fs from 'fs';

import { parse } from 'csv-parse';

import generalizeAndAggregate from './generalizeAndAggregate.js';
import tee from './tee.js'
import splitStreamByGroups from './splitStreamByGroups.js';

const queriesInputStream = fs.createReadStream('./input/dbpedia_clusters/clusters.csv');

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
    // console.log('group:' + groupId);
    for await (const record of localStream) {
        if (record.groupId === groupId) {
            yield record.query;
        }
    }
}

// async function* splitStreamByGroups(inputStream) {
//     var [localStream, mainStream] = tee(inputStream);
//     const groupMap = {};
//     for await (const record of localStream) {
//         if (!(record.groupId in groupMap)) {
//             groupMap[record.groupId] = true;
//             var [mainStream, streamCopy] = tee(mainStream);
//             yield {
//                 groupId: record.groupId,
//                 queryStream: filterStreamByGroup(streamCopy, record.groupId)
//             };
//         }
//     }
// }

async function* map(inputGenerator, fn) {
    for await (const data of inputGenerator) {
        yield fn(data);
    }
}

async function* generalizeByCluster(globalStream, options) {
    const clusterStream = splitStreamByGroups(globalStream);
    // const paramQueriesByCluster = {};
    for await (const cluster of clusterStream) {
        if (options.startFrom && cluster.groupId < options.startFrom) {
            console.log('Skipping group ' + cluster.groupId + '...');
            for await (const {} of cluster.queryStream) {}
            continue;
        }
        // console.log('****************************************');
        // console.log('**** Group ' + cluster.groupId + ' *****');
        console.log('Elaborating group ' + cluster.groupId + '...');
        // console.log('****************************************');
        // console.log('');
        // const queryObjStream = map(cluster.queryStream, query => ({
        //     text: query,
        //     numOfExecutions: 1,
        //     numOfHosts: 1
        // }));
        var queryCounter = 0;
        const queryObjStream = map(cluster.queryStream, query => {
            if (queryCounter % 1000 === 0) {
                // console.log('\n' + queryCounter / 1000 + ' K')
                process.stdout.write(queryCounter / 1000 + ' K\r');
            }
            queryCounter++;
            return {
                text: query,
                numOfExecutions: 1,
                numOfHosts: 1
            }
        });
        // paramQueriesByCluster['group_' + cluster.groupId] = await generalizeAndAggregate(queryObjStream, options);
        yield {
            groupId: cluster.groupId,
            result: await generalizeAndAggregate(queryObjStream, options)
        };
        console.log('\nDone!');
        // for await (const query of queryObjStream) {
        //     console.log(query);
        // }
    }
    // console.log(paramQueriesByCluster);
    // return paramQueriesByCluster;
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

const inputRecords = queriesInputStream.pipe(parser);

async function main() {
    const clustersResultStream = generalizeByCluster(inputRecords, {
        maxVars: 2,
        excludePreamble: true,
        generalizationTree: true,
        onlyRoots: true,
        includeSimpleQueries: true,
        countInstances: true,
        minBindingDivergenceRatio: 0.05,
        asArray: true,
        memoized: true,
        startFrom: 5,
        sparqlParameters: true
    });
    // const paramQueriesByCluster = await generalizeByCluster(inputRecords, {
    //     maxVars: 1,
    //     excludePreamble: true,
    //     generalizationTree: true,
    //     onlyRoots: true,
    //     includeSimpleQueries: true,
    //     countInstances: true,
    //     minBindingDivergenceRatio: 0.05,
    //     asArray: true,
    //     memoized: true
    // });
    // fs.writeFileSync('./output/dbpedia/queryRootsByCluster_1varMax.json', JSON.stringify(paramQueriesByCluster, null, 2), 'utf8');
    for await (const {groupId, result} of clustersResultStream) {
        fs.writeFileSync(
            `./output/dbpedia/clusters/queryRoots_2varMax_cluster_${groupId}.json`,
            JSON.stringify(result, null, 2), 'utf8');
    }
}

main().then(result => {
    console.log('OK');
}, err => {
    console.error(err);
});