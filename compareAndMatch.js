import fs from 'fs';

import { parse } from 'csv-parse';
import generalizeAndAggregate from './generalizeAndAggregate.js';

const queriesInputStream = fs.createReadStream('./output/queriesFromEP.csv');
// const queriesInputStream = fs.createReadStream('./output/query1.csv');

const parser = parse({
    delimiter: ',',
    columns: true,
    cast: (value, context) => {
      if (context.header) return value;
      if (context.column.startsWith('numOf')) return Number(value);
      return String(value);
    }
});

const aggregatePromise = generalizeAndAggregate(parser, {
    maxVars: 3, excludePreamble: true,
  generalizationTree: true,
  minNumOfExecutions: 50,
  minNumOfHosts: 10,
  includeSimpleQueries: true,
  countInstances: true,
  minBindingDivergenceRatio: 0.05,
  // showBindingDistributions: true
});
  
queriesInputStream.pipe(parser);

aggregatePromise.then(result => {
  // fs.writeFileSync('./output/queryTreeExtended2.json', JSON.stringify(result, null, 2), 'utf8');
  // fs.writeFileSync('./output/queryTree_10_50.json', JSON.stringify(result, null, 2), 'utf8');
  fs.writeFileSync('./output/queryTree_10_50_bis.json', JSON.stringify(result, null, 2), 'utf8');
  // fs.writeFileSync('./output/queryTreeExtended_10_50.json', JSON.stringify(result, null, 2), 'utf8');
}, err => {
  console.error(err);
});