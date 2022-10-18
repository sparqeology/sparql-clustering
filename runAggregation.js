import fs from 'fs';

import aggregateAndSpecialize from './aggregateAndSpecialize.js';
import ParametricQueriesStorage from './storeForest.js';
import dbpediaPrefixes from './dbpediaPrefixes.json' assert { type: "json" };
import queryEndpoint from './queryEndpoint.js';

const queryText = fs.readFileSync('./queries.rq');

export default async function runAggregation(options) {
    console.time('main');
    const storage = new ParametricQueriesStorage(options)
    const actionId = await storage.recordProcessStart();
    const queries = queryEndpoint(options.inputEndpointURL, options.inputGraphnames, queryText);
    const result = await aggregateAndSpecialize(queries, options);
    console.time('storeResults');
    await storage.storeForest(result, null, actionId)
    await storage.recordProcessCompletion(actionId)
    console.timeEnd('storeResults');
    console.timeEnd('main');
}

async function test() {
    const graphStoreURL = 'http://localhost:3030/lsqDev/data';
    const endpointURL = 'http://localhost:3030/lsqDev/query';
    const updateURL = 'http://localhost:3030/lsqDev/update';
    const inputGraphnames = ['https://dbpedia.org/sparql'];
    const outputGraphname = 'https://dbpedia.org/sparql/result/data';
    const metadataGraphname = 'https://dbpedia.org/sparql/result';

    await runAggregation({
        // maxVars: 3,
        excludePreamble: true,
        generalizationTree: true,
        // onlyRoots: true,
        asArray: true,
        // minNumOfExecutions: 50,
        // minNumOfHosts: 10,
        sparqlParameters: true,
        // includeSimpleQueries: true,
        // countInstances: true,
        // minBindingDivergenceRatio: 0.05,
        asArray: true,
        minNumOfInstances: 2,
        defaultPreamble: {
            prefixes: dbpediaPrefixes
        },
        // showBindingDistributions: true
        inputEndpointURL: endpointURL, 
        outputGraphStoreURL: graphStoreURL, 
        metadataGraphStoreURL: graphStoreURL, 
        metadataUpdateURL: updateURL,
        inputGraphnames, outputGraphname, metadataGraphname,
        resourcesNs: 'http://sparql-clustering.org/',
        defaultPreamble: {
            prefixes: dbpediaPrefixes
        }    
    })
}

test().then(result => {
}, err => {
  console.error(err);
});

