import fs from 'fs';

import aggregateAndSpecialize from './aggregateAndSpecialize.js';
import ParametricQueriesStorage from './storeForest.js';
import dbpediaPrefixes from './dbpediaPrefixes.json' assert { type: "json" };
import queryEndpoint from './queryEndpoint.js';

const queryText = fs.readFileSync('./queries.rq');

/**
 * Run the aggregation process
 * @param  {object} options Options to configure the process
 * @param  {string} options.inputEndpointURL URL of the endpoint from which source data is read
 * @param  {string[]} options.inputGraphnames Optionally, array of IRIs corresponding to the graph names from which the data source is read (if undefined or empty, the default graph is used)
 * @param  {string} options.outputGraphStoreURL URL of the graph store to which the output is written
 * @param  {string} options.outputGraphname Optionally, IRI corresponding to the graph name to which the output is written (if undefined, the default graph is used)
 * @param  {boolean} options.overwriteOutputGraph If true, the target graph for the result is overwritten. Otherwise, the results are added to the current content of the graph.
 * @param  {string} options.metadataGraphStoreURL URL of the graph store to which the metadata about process execution is written
 * @param  {string} options.metadataUpdateURL URL of the update endpoint used to update the metadata about process execution (maust correspond to the same triple store as options.metadataGraphStoreURL)
 * @param  {string} options.metadataGraphname Optionally, IRI corresponding to the graph name to which the metadata about process execution is written (if undefined, the default graph is used)
 * @param  {string} options.resourcesNs Root namespace used to mint resource IRIs
 */
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

