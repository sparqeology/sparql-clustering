import {SparqlEndpointFetcher} from "fetch-sparql-endpoint";
import fs from "fs";

const endpointFetcher = new SparqlEndpointFetcher();


import { stringify } from 'csv-stringify';


const queriesOutputStream = fs.createWriteStream('./output/queries_dbpedia_3.5.1_20100430.csv');

const stringifier = stringify({
    delimiter: ','
});

stringifier.on('readable', function(){
    let row;
    while((row = stringifier.read()) !== null){
        queriesOutputStream.write(row);
    }
});

const queryText = fs.readFileSync('./queries.rq');

function xsd(localTypeName) {
    return 'http://www.w3.org/2001/XMLSchema#' + localTypeName;
}

function castRDFTerm(rdfTerm) {
    if (rdfTerm.termType === "Literal") {
        const datatypeIRI = rdfTerm.datatype.value;
        if (datatypeIRI === xsd('string') || datatypeIRI === xsd('langString')) {
            return rdfTerm.value;
        } else if (datatypeIRI === xsd('integer')) {
            return Number.parseInt(rdfTerm.value);
        } else if (datatypeIRI === xsd('decimal')) {
            return Number.parseFloat(rdfTerm.value);
        } else if (datatypeIRI === xsd('boolean')) {
            return !!rdfTerm.value;
        } else {
            return rdfTerm.value;
        }
    }
    return null;
}

const bindingsStream = await endpointFetcher.fetchBindings('http://localhost:3030/lsqDev', queryText);
bindingsStream
    .on('variables', (variables) => {
        const varnames = variables.map(v => v.value);
        // console.log(varnames);
        stringifier.write(varnames);
        bindingsStream
            .on('data', (bindings) => {
                stringifier.write(varnames.map(varname => castRDFTerm(bindings[varname])));
                    // stringifier.write([bindings.id.value, bindings.text.value, bindings.executions.value]);
            })
            .on('error', (error) => console.error(error))
            .on('end', () => {
                stringifier.end();
                console.log('All done!');
            });
});
 