import fetch from 'node-fetch';
import crypto from 'crypto';
import {v4 as uuidv4} from 'uuid';
import { rebaseTerm, escapeLiteral } from './turtleEncoding.js';
import { mergePreambles } from './queryHandling.js';

function md5(str) {
    return crypto.createHash('md5').update(str).digest("hex")
}

function escapeLsqId(lsqId) {
    return lsqId.replaceAll('-', '_')
}

async function httpCall(url, options) {
    const response = await fetch(url, options);

    if (!response.ok) {
        const message = await response.text();
        throw new Error(message, { response });
    }
}

function buildGraphUrl(graphStoreUrl, graphname) {
    return graphStoreUrl + '?' + (graphname ? 'graph=' + encodeURIComponent(graphname) : 'defaultGraph')
}

export default class ParametricQueriesStorage {
    queryCount = 0;

    constructor(options) {
        this.options = options;
        const {resourcesNs, outputGraphStoreURL, outputGraphname = null} = this.options;
        this.preamble = `
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix lsqv: <http://lsq.aksw.org/vocab#>.
        @prefix prov: <http://www.w3.org/ns/prov#> .
        @prefix prv: <http://purl.org/net/provenance/ns#>.
        @prefix prvTypes: <http://purl.org/net/provenance/types#>.
        @prefix wfprov: <http://purl.org/wf4ever/wfprov#>.
        @prefix sh: <http://www.w3.org/ns/shacl#>.

        @prefix lsqQueries: <http://lsq.aksw.org/lsqQuery->.
        @prefix actions: <${resourcesNs}actions/>.
        @prefix templates: <${resourcesNs}templates/>.
        @prefix executions: <${resourcesNs}executions/>.
        @prefix queryGenerations: <${resourcesNs}queryGenerations/>.
        @prefix params: <${resourcesNs}params/>.
        @prefix bindings: <${resourcesNs}bindings/>.
        @prefix paramPaths: <${resourcesNs}paramPaths/>.
        `;
        this.metadataPreamble = `
        @prefix schema: <https://schema.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix sd: <http://www.w3.org/ns/sparql-service-description#>.
        
        @prefix actions: <${resourcesNs}actions/>.
        @prefix graphs: <${resourcesNs}graphs/>.
        @prefix services: <${resourcesNs}services/>.
        @prefix datasets: <${resourcesNs}datasets/>.
        `;

        this.outputUrl = buildGraphUrl(outputGraphStoreURL, outputGraphname);
    }

    async recordProcessStart() {
        // console.log('Begin of recordProcessStart()');
        const {
            inputGraphStoreURL, inputGraphname = null,
            metadataGraphStoreURL, metadataGraphname = null,
            overwriteOutputGraph = true
        } = this.options

        const actionId = uuidv4()
        const inputGraphId = encodeURIComponent(inputGraphStoreURL) + (inputGraphname ? '_' + encodeURIComponent(inputGraphname) : '')
        const inputGraphStoreId = encodeURIComponent(inputGraphStoreURL)

        const initialMetadata = this.metadataPreamble + `
        actions:${actionId} a schema:Action;
            schema:startTime '${new Date().toISOString()}'^^xsd:dateTime;
            schema:actionStatus schema:ActiveActionStatus;
            schema:object graphs:${inputGraphId}.
        
        graphs:${inputGraphId} a ${inputGraphname ? `sd:NamedGraph; sd:name <${inputGraphname}>` : 'sd:Graph'}.

        services:${inputGraphStoreId} a sd:Service;
            sd:endpoint <${inputGraphStoreURL}>;
            sd:defaultDataset datasets:${inputGraphStoreId}.

        datasets:${inputGraphStoreId} a sd:Dataset;
            ${inputGraphname ? 'sd:namedGraph' : 'sd:defaultGraph'} graphs:${inputGraphId}.`

        const metadataUrl = buildGraphUrl(metadataGraphStoreURL, metadataGraphname);
        // console.log(initialMetadata);
        await httpCall(metadataUrl, {
            method: 'POST',
            headers: {'Content-Type': 'text/turtle'},
            body: initialMetadata
        });

        if (overwriteOutputGraph) {
            await httpCall(this.outputUrl, {
                method: 'PUT',
                headers: {'Content-Type': 'text/turtle'},
                body: ''
            });
        }

        // console.log('End of recordProcessStart()');
        return actionId
    }

    async storeForest(forest, parentQueryId, actionId) {

        // console.log('Begin of storeForest()');
        for (const query of forest) {
            const {text, instances, specializations, preamble} = query;

            const queryId = ++this.queryCount;

            // const queryTurtle = `
            //     @prefix pasq: <http://pasq.org/pasq/>. 
            //     @prefix queries: <${queryNs}>.

            //     ` + (parentQueryId === undefined ?
            //         `queries:${queryId} a pasq:TopParametricQuery.` :
            //         `queries:${parentQueryId} pasq:specialization queries:${queryId}.`) `

            //     queries:${queryId}
            //         a pasq:ParametricQuery;
            //         pasq:id ${queryId};
            //         pasq:queryText "${text}".
            // `;

            var queryTurtle = this.preamble +
                (parentQueryId ?
                    `templates:${queryId} prov:specializationOf  templates:${parentQueryId}.` :
                    `templates:${queryId} prov:wasGeneratedBy actions:${actionId}.`) + `
                templates:${queryId}
                    a prvTypes:QueryTemplate, sh:Parametrizable;
                    lsqv:text ${escapeLiteral(text)}.
            `;

            for (const paramIndex of instances[0].bindings.map(({}, paramIndex) => paramIndex)) {
                const paramName = '';
                queryTurtle += `
                    templates:${queryId} sh:parameter params:${queryId}_${paramIndex}.

                    params:${queryId}_${paramIndex}
                        a sh:Parameter;
                        sh:path paramPaths:${paramName};
                        sh:description "${paramName}".
                `;
            }

            //  templates:${queryId} sh:parameter ...

            for (const {id: lsqIdUnesc, bindings} of instances) {
                // const lsqId = escapeIriFragment(lsqIdUnesc)
                const lsqId = escapeLsqId(lsqIdUnesc)
                // const queryGenerationIri = escapeIri(`queryGenerations:${queryId}_${lsqId}`)
                queryTurtle += `
                    queryGenerations:${queryId}_${lsqId}
                        a  prvTypes:DataCreation;
                        prv:usedGuideline templates:${queryId}.

                    lsqQueries:${lsqId}
                        a prvTypes:SPARQLQuery;
                        prv:createdBy queryGenerations:${queryId}_${lsqId}.
                `;

                for (const [paramIndex, bindingValue] of bindings.entries()) {
                    // console.log(bindingValue);
                    let rdfValue;
                    try {
                        rdfValue = rebaseTerm(bindingValue, mergePreambles(this.options.defaultPreamble, preamble));
                    } catch(exception) {}
                    // console.log(rdfValue);
                    // const bindingIri = ParametricQueriesStorage.escapeIri(`bindings:${lsqId}_${paramIndex}`)
                    // const paramIri = ParametricQueriesStorage.escapeIri(`params:${queryId}_${paramIndex}`)
                    queryTurtle += `
                        queryGenerations:${queryId}_${lsqId} prv:usedData bindings:${lsqId}_${paramIndex}.
                        bindings:${lsqId}_${paramIndex}
                            a wfprov:Artifact;
                            wfprov:describedByParameter params:${queryId}_${paramIndex};
                            ${rdfValue ? `rdf:value ${rdfValue};` : ''}
                            lsqv:text ${escapeLiteral(bindingValue)}. 
                    `; // wfprov:usedInput
                }
                    // queryTurtle += `
                //     queryExecutions:${lsqId} prv:usedGuideline queries:${queryId}.

                //     queryExecutions:${lsqId}
                //         a prvTypes:QueryTemplate;
                //         pasq:queryText "${text}".
                // `;
            }

            // console.log(url)
            // console.log(queryTurtle)
            // return queryTurtle;
            
            await httpCall(this.outputUrl, {
                method: 'POST',
                headers: {'Content-Type': 'text/turtle'},
                body: queryTurtle
            });

            await this.storeForest(specializations, queryId);
            // console.log('End of storeForest()');
            
        }


    }

    async recordProcessCompletion(actionId) {
        const {
            outputGraphStoreURL, outputGraphname = null,
            metadataUpdateURL
        } = this.options
    
        const outputGraphId = encodeURIComponent(outputGraphStoreURL) + (outputGraphname ? '_' + encodeURIComponent(outputGraphname) : '')
        const outputGraphStoreId = encodeURIComponent(outputGraphStoreURL)

        const metadataUpdate = this.metadataPreamble + `
        DELETE {
            actions:${actionId} schema:actionStatus schema:ActiveActionStatus
        }
        INSERT {
            actions:${actionId} schema:actionStatus schema:CompletedActionStatus;
                schema:endTime '${new Date().toISOString()}'^^xsd:dateTime;
                schema:result graphs:${outputGraphId}.

            graphs:${outputGraphId} a ${outputGraphname ? `sd:NamedGraph; sd:name <${outputGraphname}>` : 'sd:Graph'}.

            services:${outputGraphStoreId} a sd:Service;
                sd:endpoint <${outputGraphStoreURL}>;
                sd:defaultDataset datasets:${outputGraphStoreId}.
    
            datasets:${outputGraphStoreId} a sd:Dataset;
                ${outputGraphname ? 'sd:namedGraph' : 'sd:defaultGraph'} inputs:${outputGraphId}.
        };`

        await httpCall(metadataUpdateURL, {
            method: 'POST',
            headers: {'Content-Type': 'application/sparql-update'},
            body: metadataUpdate
        });
    }
    
}

// async function test() {
//     const graphStoreURI = 'http://localhost:3030/lsq2/data';
//     const graphname = 'http://example.org/test';
//     const queryNs = 'http://example.org/test/queries/';

//     const id1 = 'ID_001';
//     const id2 = 'ID_002';
//     const queryText = 'SELECT * WHERE {?s ?p ?o}';

//     const queryTurtle = `
//         @prefix pasq: <http://pasq.org/pasq/>. 
//         @prefix queries: <${queryNs}>.

//         queries:${id1}
//             pasq:specialization queries:${id2};
//             pasq:queryText "${queryText}".
//     `;

//     console.log(graphStoreURI + '?graph=' + encodeURIComponent(graphname))
//     // return queryTurtle;
    
//     const response = await fetch(graphStoreURI + '?graph=' + encodeURIComponent(graphname), {
//         method: 'POST',
//         headers: {'Content-Type': 'text/turtle'},
//         body: queryTurtle
//     });

//     if (!response.ok) {
//         const message = await response.text();
//         throw new Error(message, { response });
//     }
//     return response.status
// }

// test().then(result => {
//     console.log(result);
// }, err => {
//     console.error(err);
// });