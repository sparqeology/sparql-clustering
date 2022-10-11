import {SparqlEndpointFetcher} from "fetch-sparql-endpoint";
import fs from "fs";

const endpointFetcher = new SparqlEndpointFetcher();


import { stringify } from 'csv-stringify';


const queriesOutputStream = fs.createWriteStream('./output/executionsFromEP_20100430.csv');

const stringifier = stringify({
    delimiter: ','
});

stringifier.on('readable', function(){
    let row;
    while((row = stringifier.read()) !== null){
        queriesOutputStream.write(row);
    }
});

const queryText = fs.readFileSync('./executions.rq');

const bindingsStream = await endpointFetcher.fetchBindings('http://localhost:3030/lsqDev', queryText);
bindingsStream
    .on('variables', (variables) => {
        const varnames = variables.map(v => v.value);
        // console.log(varnames);
        stringifier.write(varnames);
        bindingsStream
            .on('data', (bindings) => {
                stringifier.write(varnames.map(varname => bindings[varname].value));
                    // stringifier.write([bindings.id.value, bindings.text.value, bindings.executions.value]);
            })
            .on('error', (error) => console.error(error))
            .on('end', () => {
                stringifier.end();
                console.log('All done!');
            });
});
 