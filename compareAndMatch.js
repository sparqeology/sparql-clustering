import fs from 'fs';

import { parse } from 'csv-parse';
import aggregateAndSpecialize from './aggregateAndSpecialize.js';

const queriesInputStream = fs.createReadStream('./output/queries_dbpedia_3.5.1.csv');
// const queriesInputStream = fs.createReadStream('./output/query1.csv');

const graphStoreURI = 'http://localhost:3030/lsq2/data';
const graphname = '';

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
  includeSimpleQueries: true,
  countInstances: true,
  // minBindingDivergenceRatio: 0.05,
  asArray: true
  // showBindingDistributions: true
});
  
queriesInputStream.pipe(parser);

console.time('main');
aggregatePromise.then(result => {
  console.timeEnd('main');
  // fs.writeFileSync('./output/queryTreeExtended2.json', JSON.stringify(result, null, 2), 'utf8');
  // fs.writeFileSync('./output/queryTree_10_50.json', JSON.stringify(result, null, 2), 'utf8');
  // fs.writeFileSync('./output/queryRootsExtended_minExecs_50.json', JSON.stringify(result, null, 2), 'utf8');
  fs.writeFileSync('./output/dbpedia/queryForest_3.5.1.json', JSON.stringify(result, null, 2), 'utf8');
  // fs.writeFileSync('./output/queryRoots_10_50.json', JSON.stringify(result, null, 2), 'utf8');
  // fs.writeFileSync('./output/queryRootsAsArray.json', JSON.stringify(result, null, 2), 'utf8');
  // fs.writeFileSync('./output/queryTreeExtended_10_50.json', JSON.stringify(result, null, 2), 'utf8');
}, err => {
  console.error(err);
});
