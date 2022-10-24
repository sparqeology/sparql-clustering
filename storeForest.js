import crypto from 'crypto';
import {v4 as uuidv4} from 'uuid';
import { rebaseTerm, escapeLiteral } from './turtleEncoding.js';
import { mergePreambles } from './queryHandling.js';
import SparqlGraphConnection from './sparqlGraphConnection.js';
import httpCall from './httpCall.js';
import { buildStoreGraphUrl } from './queryEndpoint.js';

function md5(str) {
    return crypto.createHash('md5').update(str).digest("hex")
}

function escapeLsqId(lsqId) {
    return lsqId.replaceAll('-', '_')
}

export default class ParametricQueriesStorage {
    queryCount = 0;

    constructor(options) {
        this.options = options;
        const {resourcesNs, outputGraphStoreURL, outputGraphname = null} = this.options;
        this.preamble = `
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX lsqv: <http://lsq.aksw.org/vocab#>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX prv: <http://purl.org/net/provenance/ns#>
        PREFIX prvTypes: <http://purl.org/net/provenance/types#>
        PREFIX wfprov: <http://purl.org/wf4ever/wfprov#>
        PREFIX sh: <http://www.w3.org/ns/shacl#>

        PREFIX lsqQueries: <http://lsq.aksw.org/lsqQuery->
        PREFIX actions: <${resourcesNs}actions/>
        PREFIX templates: <${resourcesNs}templates/>
        PREFIX executions: <${resourcesNs}executions/>
        PREFIX queryGenerations: <${resourcesNs}queryGenerations/>
        PREFIX params: <${resourcesNs}params/>
        PREFIX bindings: <${resourcesNs}bindings/>
        PREFIX paramPaths: <${resourcesNs}paramPaths/>
        `;
        this.metadataPreamble = `
        PREFIX schema: <https://schema.org/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
        
        PREFIX actions: <${resourcesNs}actions/>
        PREFIX graphs: <${resourcesNs}graphs/>
        PREFIX services: <${resourcesNs}services/>
        PREFIX datasets: <${resourcesNs}datasets/>
        `;

        this.outputUrl = buildStoreGraphUrl(outputGraphStoreURL, outputGraphname);
    }

    async recordProcessStart() {
        const {
            inputEndpointURL, inputGraphnames = null,
            metadataUpdateURL, metadataGraphname = null,
            overwriteOutputGraph = true
        } = this.options

        const actionId = uuidv4()
        const inputGraphStoreId = encodeURIComponent(inputEndpointURL)

        const metadataSetup = this.metadataPreamble + `
        INSERT DATA {
            ${metadataGraphname ? `GRAPH <${metadataGraphname}> {` : ''}
                actions:${actionId} a schema:Action;
                    schema:startTime '${new Date().toISOString()}'^^xsd:dateTime;
                    schema:actionStatus schema:ActiveActionStatus.
            
                services:${inputGraphStoreId} a sd:Service;
                    sd:endpoint <${inputEndpointURL}>;
                    sd:defaultDataset datasets:${inputGraphStoreId}.

                ${(inputGraphnames || [null]).map(inputGraphname => {
                    const inputGraphId = encodeURIComponent(inputEndpointURL) + (inputGraphname ? '_' + encodeURIComponent(inputGraphname) : '')
                    return `
                    actions:${actionId} schema:object graphs:${inputGraphId}.
                    graphs:${inputGraphId} a ${inputGraphname ? `sd:NamedGraph; sd:name <${inputGraphname}>` : 'sd:Graph'}.
                    datasets:${inputGraphStoreId} a sd:Dataset;
                        ${inputGraphname ? 'sd:namedGraph' : 'sd:defaultGraph'} graphs:${inputGraphId}.`;
                }).join('')}
            ${metadataGraphname ? '}' : ''}
        };`

        await httpCall(metadataUpdateURL, {
            method: 'POST',
            headers: {'Content-Type': 'application/sparql-update'},
            body: metadataSetup
        });

        if (overwriteOutputGraph) {
            await httpCall(this.outputUrl, {
                method: 'PUT',
                headers: {'Content-Type': 'text/turtle'},
                body: ''
            });
        }

        this.outputGraphConnection = new SparqlGraphConnection(this.outputUrl, {
            preamble: this.preamble
        })

        return actionId
    }

    async storeForest(forest, parentQueryId, actionId) {

        for (const query of forest) {
            const {text, instances, specializations, preamble} = query;

            const queryId = ++this.queryCount;

            var queryTurtle = (parentQueryId ?
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

            for (const {id: lsqIdUnesc, bindings} of instances) {
                const lsqId = escapeLsqId(lsqIdUnesc)
                queryTurtle += `
                    queryGenerations:${queryId}_${lsqId}
                        a  prvTypes:DataCreation;
                        prv:usedGuideline templates:${queryId}.

                    lsqQueries:${lsqId}
                        a prvTypes:SPARQLQuery;
                        prv:createdBy queryGenerations:${queryId}_${lsqId}.
                `;

                for (const [paramIndex, bindingValue] of bindings.entries()) {
                    let rdfValue;
                    try {
                        rdfValue = rebaseTerm(bindingValue, mergePreambles(this.options.defaultPreamble, preamble));
                    } catch(exception) {}
                    queryTurtle += `
                        queryGenerations:${queryId}_${lsqId} prv:usedData bindings:${lsqId}_${paramIndex}.
                        bindings:${lsqId}_${paramIndex}
                            a wfprov:Artifact;
                            wfprov:describedByParameter params:${queryId}_${paramIndex};
                            ${rdfValue ? `rdf:value ${rdfValue};` : ''}
                            lsqv:text ${escapeLiteral(bindingValue)}. 
                    `; // wfprov:usedInput
                }
            }

            this.outputGraphConnection.post(queryTurtle);
            await this.storeForest(specializations, queryId);
        }


    }

    async recordProcessCompletion(actionId) {
        const {
            outputGraphStoreURL, outputGraphname = null,
            metadataUpdateURL, metadataGraphname = null
        } = this.options
    
        await this.outputGraphConnection.sync();

        const outputGraphId = encodeURIComponent(outputGraphStoreURL) + (outputGraphname ? '_' + encodeURIComponent(outputGraphname) : '')
        const outputGraphStoreId = encodeURIComponent(outputGraphStoreURL)

        const metadataUpdate = this.metadataPreamble + `
        DELETE DATA {
            ${metadataGraphname ? `GRAPH <${metadataGraphname}> {` : ''}
                actions:${actionId} schema:actionStatus schema:ActiveActionStatus
            ${metadataGraphname ? '}' : ''}
        };
        
        INSERT DATA {
            ${metadataGraphname ? `GRAPH <${metadataGraphname}> {` : ''}
                actions:${actionId} schema:actionStatus schema:CompletedActionStatus;
                    schema:endTime '${new Date().toISOString()}'^^xsd:dateTime;
                    schema:result graphs:${outputGraphId}.

                graphs:${outputGraphId} a ${outputGraphname ? `sd:NamedGraph; sd:name <${outputGraphname}>` : 'sd:Graph'}.

                services:${outputGraphStoreId} a sd:Service;
                    sd:endpoint <${outputGraphStoreURL}>;
                    sd:defaultDataset datasets:${outputGraphStoreId}.
        
                datasets:${outputGraphStoreId} a sd:Dataset;
                    ${outputGraphname ? 'sd:namedGraph' : 'sd:defaultGraph'} graphs:${outputGraphId}.
            ${metadataGraphname ? '}' : ''}
        };`

        await httpCall(metadataUpdateURL, {
            method: 'POST',
            headers: {'Content-Type': 'application/sparql-update'},
            body: metadataUpdate
        });
    }
    
}
