import fs from 'fs';

import {pipeline} from 'stream';

import { stringify } from 'csv-stringify';
import { parse } from 'csv-parse';
import generalizeQuery from './generalizeQuery.js';

// const queriesInputStream = fs.createReadStream('./output/queries.csv');
const queriesInputStream = fs.createReadStream('./output/someQueries.csv');

const limit = 10000000000;
var queryCount = 0;

const paramQueryMap = {};

// const queriesOutputStream = fs.createWriteStream('./output/queries.csv');

const parser = parse({
    delimiter: ','
});

parser.on('readable', function(){
    let record;
    while ((limit === undefined || queryCount < limit) && (record = parser.read()) !== null) {
      const id = record[0];
      const query = record[1];
      // console.log(query);
      const paramQueries = generalizeQuery(query, {maxVars: 3});
      paramQueries.shift(); // skip first item, which is the query itself
      // console.log(paramQueries);
      paramQueries.forEach(paramQuery => {
        if (! (paramQuery.query in paramQueryMap)) {
          paramQueryMap[paramQuery.query] = [];
        }
        paramQueryMap[paramQuery.query].push({
          originalQueryId: id,
          originalQuery: query,
          bindings: paramQuery.paramBindings
        });
      });
      queryCount++;
    }
    if (limit !== undefined && queryCount >= limit) {
      queriesInputStream.close();
      parser.emit('end');
      parser.end();
    }
});

parser.on('error', function(err){
    console.error(err.message);
});

parser.on('end', () => {
  const outputParamQueryMap = Object.fromEntries(
      Object.entries(paramQueryMap)
      .filter(([k,v]) => (v.length > 1)));
      // TODO: add filter excluding when one of the bound vars is a constant
  // const outputParamQueryMap = paramQueryMap;
  // console.log(outputParamQueryMap);
  fs.writeFileSync('./output/paramQueries.json', JSON.stringify(outputParamQueryMap, null, 2), 'utf8');
});
  
queriesInputStream.pipe(parser);

// pipeline(queriesInputStream, parser);
