import fs from 'fs';

import { parse } from 'csv-parse';
import generalizeAndAggregate from './generalizeAndAggregate.js';

// const queriesInputStream = fs.createReadStream('./output/queries.csv');
const queriesInputStream = fs.createReadStream('./output/queriesFromEP.csv');
// const queriesInputStream = fs.createReadStream('./output/meshQueries.csv');

const limit = 10000000000;
var queryCount = 0;

const parser = parse({
    delimiter: ',',
    columns: true,
    cast: (value, context) => {
      if (context.header) return value;
      if (context.column.startsWith('numOf')) return Number(value);
      return String(value);
    }
});

const eventListeners = {};

const aggregatePromise = generalizeAndAggregate({
  on: (event, callback) => {
    if (!(event in eventListeners)) {
      eventListeners[event] = [];
    }
    eventListeners[event].push(callback);
  }
}, {maxVars: 3, excludePreamble: true, generalizationTree: true,
  countInstances: true
});

function emitEvent(event, payload) {
  if (event in eventListeners) {
    eventListeners[event].forEach(callback => {
      callback(payload);
    })
  }
}

parser.on('readable', function(){
    let record;
    while ((limit === undefined || queryCount < limit) && (record = parser.read()) !== null) {
      emitEvent('data', record)
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
  emitEvent('end');
});
  
queriesInputStream.pipe(parser);

aggregatePromise.then(result => {
  // fs.writeFileSync('./output/queryTreeExtended2.json', JSON.stringify(result, null, 2), 'utf8');
  fs.writeFileSync('./output/queryTree2.json', JSON.stringify(result, null, 2), 'utf8');
}, err => {
  console.error(err);
});