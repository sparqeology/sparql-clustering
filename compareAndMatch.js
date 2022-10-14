import fs from 'fs';

import { parse } from 'csv-parse';
import aggregateAndSpecialize from './aggregateAndSpecialize.js';
import ParametricQueriesStorage from './storeForest.js';
import dbpediaPrefixes from './dbpediaPrefixes.json' assert { type: "json" };

const queriesInputStream = fs.createReadStream('./output/queries_dbpedia_3.5.1_20100430.csv');
// const queriesInputStream = fs.createReadStream('./output/query1.csv');

const graphStoreURL = 'http://localhost:3030/lsqDev/data';
const updateURL = 'http://localhost:3030/lsqDev/update';
const inputGraphname = 'https://dbpedia.org/sparql';
const outputGraphname = 'https://dbpedia.org/sparql/result/data';
const metadataGraphname = 'https://dbpedia.org/sparql/result';

// const dbpediaPrefixes = require('./dbpediaPrefixes.json');

const parser = parse({
    delimiter: ',',
    columns: true,
    cast: (value, context) => {
      if (context.header) return value;
      if (context.column.startsWith('numOf')) return Number(value);
      return String(value);
    }
});

const aggregatePromise = aggregateAndSpecialize(parser, {
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
  }
  // showBindingDistributions: true
});
  
queriesInputStream.pipe(parser);

async function main() {
  console.time('main');
  const storage = new ParametricQueriesStorage({
    inputGraphStoreURL: graphStoreURL, 
    outputGraphStoreURL: graphStoreURL, 
    metadataGraphStoreURL: graphStoreURL, 
    metadataUpdateURL: updateURL,
    inputGraphname, outputGraphname, metadataGraphname,
    resourcesNs: 'http://sparql-clustering.org/',
    defaultPreamble: {
      prefixes: dbpediaPrefixes
    }
  })
  const actionId = await storage.recordProcessStart();
  const result = await aggregatePromise;
  fs.writeFileSync('./output/dbpedia/queryForest_3.5.1_20100430.json', JSON.stringify(result, null, 2), 'utf8');
  await storage.storeForest(result, null, actionId)
  await storage.recordProcessCompletion(actionId)
  console.timeEnd('main');
}

// console.time('main');
main().then(result => {
  // console.timeEnd('main');
  // // fs.writeFileSync('./output/queryTreeExtended2.json', JSON.stringify(result, null, 2), 'utf8');
  // // fs.writeFileSync('./output/queryTree_10_50.json', JSON.stringify(result, null, 2), 'utf8');
  // // fs.writeFileSync('./output/queryRootsExtended_minExecs_50.json', JSON.stringify(result, null, 2), 'utf8');
  // fs.writeFileSync('./output/dbpedia/queryForest_3.5.1.json', JSON.stringify(result, null, 2), 'utf8');
  // // fs.writeFileSync('./output/queryRoots_10_50.json', JSON.stringify(result, null, 2), 'utf8');
  // // fs.writeFileSync('./output/queryRootsAsArray.json', JSON.stringify(result, null, 2), 'utf8');
  // // fs.writeFileSync('./output/queryTreeExtended_10_50.json', JSON.stringify(result, null, 2), 'utf8');

}, err => {
  console.error(err);
});
