import fetch from 'node-fetch';

export default class ParametricQueriesStorage {
    queryCount = 0;

    constructor(options) {
        this.options = options;
        const {resourcesNs} = this.options;
        this.prefix = `
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix lsqv: <http://lsq.aksw.org/vocab#>.
        @prefix prov: <http://www.w3.org/ns/prov#> .
        @prefix prv: <http://purl.org/net/provenance/ns#>.
        @prefix prvTypes: <http://purl.org/net/provenance/types#>.
        @prefix wfprov: <http://purl.org/wf4ever/wfprov#>.

        @prefix lsqQueries: <http://lsq.aksw.org/lsqQuery->.
        @prefix templates: <${resourcesNs}templates/>.
        @prefix executions: <${resourcesNs}executions/>.
        @prefix queryGenerations: <${resourcesNs}queryGenerations/>.
        @prefix params: <${resourcesNs}params/>.
        @prefix bindings: <${resourcesNs}bindings/>.
        `;
    }
    async storeForest(forest, parentQueryId) {
        const {graphStoreURI, graphname} = this.options;
        for (const query of forest) {
            const {text, instances, specializations} = query;

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

            var queryTurtle = this.prefix +
                (parentQueryId === undefined ?
                    `templates:${queryId} prov:wasGeneratedBy <>.` :
                    `templates:${queryId} prov:specializationOf  templates:${parentQueryId}.`) + `
                templates:${queryId}
                    a prvTypes:QueryTemplate, sh:Parametrizable;
                    lsqv:text "${text}".
            `;

            for (const paramIndex of instances[0].bindings.map(({}, paramIndex) => paramIndex)) {
                const paramName = '';
                queryTurtle += `
                    templates:${queryId} sh:parameter params:${queryId}_${paramIndex}.

                    params:${queryId}_${paramIndex}
                        a sh:Parameter;
                        sh:path paramPath:${paramName};
                        sh:description "${paramName}".
                `;
            }

            //  templates:${queryId} sh:parameter ...

            for ({id: lsqId, bindings, numOfExecutions, numOfHosts} of instances) {
                queryTurtle += `
                    queryGeneration:${queryId}_${lsqId}
                        a  prvTypes:DataCreation;
                        prv:usedGuideline templates:${queryId}s.

                    lsqQueries:${lsqId}
                        a prvTypes:SPARQLQuery;
                        prv:createdBy queryGenerations:${lsqId}.
                `;

                for (const [paramIndex, bindingValue] of bindings.entries()) {
                    queryTurtle += `
                        queryGenerations:${queryId}_${lsqId} prv:usedData bindings:${lsqId}_${paramIndex}.
#                        wfprov:usedInput
                        bindings:${lsqId}_${paramIndex}
                            a wfprov:Artifact;
                            wfprov:describedByParameter params:${queryId}_${paramIndex};
                            rdf:value ${bindingValue}.
                    `;
                }
                    // queryTurtle += `
                //     queryExecutions:${lsqId} prv:usedGuideline queries:${queryId}.

                //     queryExecutions:${lsqId}
                //         a prvTypes:QueryTemplate;
                //         pasq:queryText "${text}".
                // `;
            }

            console.log(graphStoreURI + '?graph=' + encodeURIComponent(graphname))
            // return queryTurtle;
            
            const response = await fetch(graphStoreURI + '?graph=' + encodeURIComponent(graphname), {
                method: 'POST',
                headers: {'Content-Type': 'text/turtle'},
                body: queryTurtle
            });

            if (!response.ok) {
                const message = await response.text();
                throw new Error(message, { response });
            }

            await storeForest(specializations, query);
            
        }
    }
}

async function test() {
    const graphStoreURI = 'http://localhost:3030/lsq2/data';
    const graphname = 'http://example.org/test';
    const queryNs = 'http://example.org/test/queries/';

    const id1 = 'ID_001';
    const id2 = 'ID_002';
    const queryText = 'SELECT * WHERE {?s ?p ?o}';

    const queryTurtle = `
        @prefix pasq: <http://pasq.org/pasq/>. 
        @prefix queries: <${queryNs}>.

        queries:${id1}
            pasq:specialization queries:${id2};
            pasq:queryText "${queryText}".
    `;

    console.log(graphStoreURI + '?graph=' + encodeURIComponent(graphname))
    // return queryTurtle;
    
    const response = await fetch(graphStoreURI + '?graph=' + encodeURIComponent(graphname), {
        method: 'POST',
        headers: {'Content-Type': 'text/turtle'},
        body: queryTurtle
    });

    if (!response.ok) {
        const message = await response.text();
        throw new Error(message, { response });
    }
    return response.status
}

test().then(result => {
    console.log(result);
}, err => {
    console.error(err);
});