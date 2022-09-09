import fetch from 'node-fetch';

export default class ParametricQueriesStorage {
    queryCount = 0;

    constructor(options) {
        this.options = options;
    }
    async storeForest(forest, parentQueryId) {
        for (const {text, instances, specializations} of forest) {
            const {graphStoreURI, graphname, queryNs, queryExecutionsNs} = this.options;

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

            var queryTurtle = `
                @prefix prov: <http://www.w3.org/ns/prov#> .
                @prefix prv: <http://purl.org/net/provenance/ns#>.
                @prefix prvTypes: <http://purl.org/net/provenance/types#>.
                @prefix pasq: <http://pasq.org/pasq/>. 
                @prefix templates: <${queryNs}>.
                @prefix executions: <${queryExecutionsNs}>.

                ` + (parentQueryId === undefined ?
                    `queries:${queryId} a pasq:TopParametricQuery.` :
                    `queries:${parentQueryId} pasq:specialization queries:${queryId}.`) `

                templates:${queryId}
                    a prvTypes:QueryTemplate, sh:Parametrizable;
                    lsqv:text "${text}";
                    sh:parameter "${instances[0].bindings.map(({}, index) => `param:${queryId}_${index}`).join(', ')}".
            `;

            for (const paramIndex of instances[0].bindings.map(({}, paramIndex) => paramIndex)) {
                const paramName = '';
                queryTurtle += `
                    templates:${queryId} sh:parameter param:${queryId}_${paramIndex}.

                    param:${queryId}_${paramIndex}
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

                    queries:${lsqId}
                        a prvTypes:SPARQLQuery;
                        prv:createdBy queryGeneration:${lsqId}.
                `;

                for (const [paramIndex, bindingValue] of bindings.entries()) {
                    queryTurtle += `
                        queryGeneration:${queryId}_${lsqId} prv:usedData binding:${lsqId}_${paramIndex}.
                        binding:${lsqId}_${paramIndex}
                            a xxx:Binding;
                            xxx:parameter param:${queryId}_${paramIndex};
                            xxx:value ${bindingValue}.
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