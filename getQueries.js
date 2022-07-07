import rdfParser from "rdf-parse";
import fs from "fs";

import { stringify } from 'csv-stringify';


const path = './input/input.nt';
const inputStream = fs.createReadStream(path);

const queriesOutputStream = fs.createWriteStream('./output/queries.csv');

const parser = rdfParser.default;

const stringifier = stringify({
    delimiter: ','
});

stringifier.on('readable', function(){
    let row;
    while((row = stringifier.read()) !== null){
        queriesOutputStream.write(row);
    }
});

const prefixLength = 'http://lsq.aksw.org/lsqQuery-'.length;
  
parser.parse(inputStream, {path})
    .on('data', (quad) => {
        // console.log(quad._predicate);
        if (quad._predicate.id === 'http://lsq.aksw.org/vocab#text') {
            // console.log(stringify({
            //     id: quad._subject.id,
            //     query: quad._object
            // }));
            stringifier.write([quad._subject.id.substr(prefixLength), quad._object.value]);
            // console.log(quad);
        }
    })
    .on('error', (error) => console.error(error))
    .on('end', () => {
        stringifier.end();
        console.log('All done!');
    });