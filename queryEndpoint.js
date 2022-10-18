import {SparqlEndpointFetcher} from "fetch-sparql-endpoint";
import S2A from 'stream-to-async-iterator';
const StreamToAsyncIterator = S2A.default;

const endpointFetcher = new SparqlEndpointFetcher();

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

export function buildStoreGraphUrl(graphStoreUrl, graphname) {
    return graphStoreUrl + '?' + (graphname ? 'graph=' + encodeURIComponent(graphname) : 'defaultGraph')
}

export function buildEndpointGraphsUrl(graphStoreUrl, graphnames) {
    return graphStoreUrl + '?' +
        (graphnames ? graphnames.map(graphname => 'default-graph-uri=' + encodeURIComponent(graphname)).join('&')  : '');
}


export default async function* queryEndpoint(endpointUrl, graphnames, queryStr) {
    const url = buildEndpointGraphsUrl(endpointUrl, graphnames);
    const bindingsStream = await endpointFetcher.fetchBindings(url, queryStr);
    for await (const bindings of new StreamToAsyncIterator(bindingsStream)) {
        yield Object.fromEntries(Object.entries(bindings).map(([varname, value]) => [varname,castRDFTerm(value)]));
    }
}

// const queryText = `
// PREFIX lsqr: <http://lsq.aksw.org/>
// PREFIX lsqv: <http://lsq.aksw.org/vocab#>
// PREFIX prov: <http://www.w3.org/ns/prov#>

// SELECT ?id ?text (COUNT(?exec) AS ?numOfExecutions) (COUNT(DISTINCT ?host) AS ?numOfHosts)
// WHERE {
//   GRAPH <https://dbpedia.org/sparql> {
//     ?query lsqv:hash ?id;
//           lsqv:text ?text;
//           lsqv:hasRemoteExec ?exec.
//     ?exec lsqv:hostHash ?host.
//   }
// }
// GROUP BY ?id ?text
// LIMIT 25
// `;

// const endpointUrl = 'http://localhost:3030/lsqDev/query';

// async function test() {
//     for await (const bindings of queryEndpoint(endpointUrl, queryText)) {
//         console.log(bindings);
//     }
// }

// test().then(res => {
//     console.log('done!')
// }).catch(err => {
//     console.log(err)
// });