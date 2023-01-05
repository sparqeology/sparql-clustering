import fs from 'node:fs';
import https from 'node:https';
import zlib from 'node:zlib';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import bz2 from 'unbzip2-stream';
import N3 from 'n3';
import N3TollerantStreamParser from './N3TollerantStreamParser.js';
import queryEndpoint from './queryEndpoint.js';

const { DataFactory } = N3;
const { namedNode, literal, defaultGraph, quad } = DataFactory;

const datasetsQuery = fs.readFileSync('./queries/datasets.rq');

const PROV_PREFIX = 'http://www.w3.org/ns/prov#';
const LSQV_PREFIX = 'http://lsq.aksw.org/vocab#';

const REMOTE_EXECUTION_PREFIX = 'http://lsq.aksw.org/re-';
const QUERY_PREFIX = 'http://lsq.aksw.org/lsqQuery-';

const EXECUTION_IRI_PROPERTY_NAME_MAPPING = {
    [PROV_PREFIX + 'atTime']: 'atTime',
    [LSQV_PREFIX + 'hostHash']: 'hostHash'
};
const EXECUTION_PROPERTY_NAME_IRI_MAPPING = Object.fromEntries(
    Object.entries(EXECUTION_IRI_PROPERTY_NAME_MAPPING).map(([iri, name]) => ([name, iri]))
);

const includedProperties = [
    ...['hash', 'hasRemoteExec', 'hostHash'].map(localName => LSQV_PREFIX + localName),
    PROV_PREFIX + 'atTime'
];

async function httpsGet(urlOrOptions) {
    return new Promise((resolve, reject) => {
        https.get(urlOrOptions, result => {
            resolve(result);
        })
    });
}

async function fileExists(filePath) {
    try {
        await fs.promises.access(filePath);
    } catch(err) {
        if (err.code === 'ENOENT') {
            return false;
        }
        throw err;
    }
    return true;
}

class ExecutionExtractor extends Transform {
    currentExecution = null;
    executionCache = {};
    executionToQueryCache = {};

    queryCache = {};
    confirmedQueries = new Set();
    queriesToBeIncluded = new Set();

    constructor() {
        super({objectMode: true});
    }

    _transform(quad, encoding, callback) {
        if (quad._subject.id.startsWith(REMOTE_EXECUTION_PREFIX)) {
            this.addExecutionProperty(quad._subject.id, EXECUTION_IRI_PROPERTY_NAME_MAPPING[quad._predicate.id], quad._object);
        } else if (quad._predicate.id === LSQV_PREFIX + 'hasRemoteExec') {
            this.addQueryExecutionAssociation(quad._subject.id, quad._object.id);
            this.includeQuery(quad._subject.id);
        } else if (quad._predicate.id === LSQV_PREFIX + 'text') {
            this.addQuery(quad._subject.id, quad._object);
        }
        callback();
    }

    getQueryHash(queryId) {
        return queryId.substring(QUERY_PREFIX.length);
    }

    addQuery(queryId, queryText) {
        const queryHash = this.getQueryHash(queryId);
        if (this.confirmedQueries.has(queryHash)) {
            return;
        }
        if (this.queriesToBeIncluded.has(queryHash)) {
            this.confirmedQueries.add(queryHash);
            this.outputQueryInfo(queryId, queryHash, queryText);
            this.queriesToBeIncluded.delete(queryHash);
        } else {
            this.queryCache[queryHash] = queryText;
        }
    }

    includeQuery(queryId) {
        const queryHash = this.getQueryHash(queryId);
        if (this.confirmedQueries.has(queryHash)) {
            return;
        }
        if (queryHash in this.queryCache) {
            this.confirmedQueries.add(queryHash);
            this.outputQueryInfo(queryId, queryHash, this.queryCache[queryHash]);
            delete this.queryCache[queryHash];
        } else { 
            this.queriesToBeIncluded.add(queryHash);
        }
    }

    outputQueryInfo(queryId, queryHash, queryText) {
        this.push({
            type: 'query',
            queryId, queryHash, queryText
        });
    }

    outputExecutionInfo(queryId, executionId, executionInfo) {
        this.push({
            type: 'execution',
            queryId, executionId, executionInfo
        });
    }

    outputExecutionInfoArray(queryId, executionId, executionInfoArray) {
        executionInfoArray.forEach(executionInfo => {
            this.outputExecutionInfo(queryId, executionId, executionInfo);
        });
    }

    addQueryExecutionAssociation(queryId, executionId) {
        if (executionId in this.executionCache) {
            const executionInfoArray = this.executionCache[executionId].shift();
            if (this.executionCache[executionId].length === 0) {
                delete this.executionCache[executionId];
            }
            this.outputExecutionInfoArray(queryId, executionId, executionInfoArray);
            return;
        }
        if (executionId in this.executionToQueryCache) {
            this.executionToQueryCache[executionId].push(queryId);
            return;
        }
        this.executionToQueryCache[executionId] = [queryId];
    }

    addExecutionSet(executionId, executionInfoArray) {
        if (executionId in this.executionToQueryCache) {
            const queryId = this.executionToQueryCache[executionId].shift();
            if (this.executionToQueryCache[executionId].length === 0) {
                delete this.executionToQueryCache[executionId];
            }
            this.outputExecutionInfoArray(queryId, executionId, executionInfoArray);
            return;
        }
        if (executionId in this.executionCache) {
            this.executionCache[executionId].push(executionInfoArray);
            return;
        }
        this.executionCache[executionId] = [executionInfoArray];
    }

    addExecutionProperty(queryExecIri, propertyName, propertyValue) {
        if (!this.currentExecution) {
            this.currentExecution = {
                id: queryExecIri,
                infoArray: [propertyName ? { [propertyName]: propertyValue } : {}]
            };
        } else {
            if (!propertyName) {
                return;
            }
            const lastInfo = this.currentExecution.infoArray.at(-1);
            if (propertyName in lastInfo) {
                this.currentExecution.infoArray.push({ [propertyName]: propertyValue });
                return;
            }
            this.currentExecution.infoArray.forEach(execInfo => {
                if (!(propertyName in execInfo)) {
                    execInfo[propertyName] = propertyValue;
                }
            });
        }
        if (propertyName === 'atTime') {
            this.addExecutionSet(this.currentExecution.id, this.currentExecution.infoArray);
            this.currentExecution = null;
        }
    }
}

const JSONLD_BEGIN = `{"@context":${JSON.stringify({
    'isRemoteExecOf': {'@reverse': {'@id': LSQV_PREFIX + 'hasRemoteExec'}},
    ...Object.fromEntries(
        Object.entries(EXECUTION_IRI_PROPERTY_NAME_MAPPING).map(([iri, name]) => ([name, {'@id': iri}])))
})},"@graph":[`;
const JSONLD_END = ']}';

class ExecutionAsQuadsSerializer extends Transform {
    execCount = 0;
    constructor(dataset) {
        super({objectMode: true});
        this.dataset = dataset;
    }

    _transform({queryId, executionInfo}, encoding, callback) {
        const executionId =
            `${this.dataset}/executions/${++this.execCount}`;
        this.push(quad(
            namedNode(queryId),
            namedNode(LSQV_PREFIX + 'hasRemoteExec'),
            namedNode(executionId)
        ));
        Object.entries(executionInfo).forEach(([propertyName, propertyValue]) => {
            this.push(quad(
                namedNode(executionId),
                namedNode(EXECUTION_PROPERTY_NAME_IRI_MAPPING[propertyName]),
                propertyValue
            ));
        });
        callback();
    }
}

class QueryAsQuadsSerializer extends Transform {
    execCount = 0;
    constructor(dataset) {
        super({objectMode: true});
        this.dataset = dataset;
    }

    _transform({queryId, queryHash, queryText}, encoding, callback) {
        // const queryId = QUERY_PREFIX + queryHash;
        this.push(quad(
            namedNode(queryId),
            namedNode(LSQV_PREFIX + 'hash'),
            literal(queryHash)
        ));
        this.push(quad(
            namedNode(queryId),
            namedNode(LSQV_PREFIX + 'text'),
            queryText
        ));
        callback();
    }
}

class ObjectLogger extends Transform {
    constructor(dataset) {
        super({objectMode: true});
    }
    _transform(data, encoding, callback) {
        console.log(data);
        callback(null, data);
    }
};


class DetourStream extends Transform {
    constructor(filterFn, stream) {
        super({objectMode: true});
        this.filterFn = filterFn;
        this.stream = stream;
    }
    _transform(data, encoding, callback) {
        if (this.filterFn(data)) {
            this.stream.write(data);
            callback();
        } else {
            callback(null, data);
        }
    }
    _flush(callback) {
        this.stream.end();
        callback();
    }
};

async function loadDatasetAndFilter(dataset, datasetId, options) {
    const {inputDirPath, outputDirPath, outputCsv} = options;
    // const remoteFilePath = remoteDirPath + datasetId + '.nt.bz2';
    // const response = await fetch(remoteFilePath);
    // if (!response.ok) {
    //     throw new Error(response.statusText);
    // }

    const inputFilePath = inputDirPath + datasetId + '.nt.bz2';

    const executionExtractor = new ExecutionExtractor();

    if (outputCsv) {
        const outputFilePath = `${outputDirPath}${datasetId}.csv.gz`;

        await pipeline(
            // response.body,
            fs.createReadStream(inputFilePath),
            bz2(),
            new N3TollerantStreamParser({format: 'N-Triples'}),
            new ExecutionExtractor(),
            new ExecutionAsQuadsSerializer(dataset), // TODO: change
            new N3.StreamWriter({format: 'N-Triples'}), // TODO: change
            zlib.createGzip(),
            fs.createWriteStream(outputFilePath)
        );
    } else {
        // const outputFilePath = `${outputDirPath}${datasetId}_execs.nt.gz`;

        function ntGzWriter(filePath) {
            const writer = new N3.StreamWriter({format: 'N-Triples'});
            writer.pipe(zlib.createGzip()).pipe(fs.createWriteStream(filePath));
            return writer;
        }

        const querySink = new QueryAsQuadsSerializer(dataset);
        querySink.pipe(ntGzWriter(`${outputDirPath}${datasetId}_queries.nt.gz`));

        const executionSink = new ExecutionAsQuadsSerializer(dataset);
        executionSink.pipe(ntGzWriter(`${outputDirPath}${datasetId}_execs.nt.gz`));

        await pipeline(
            // response.body,
            fs.createReadStream(inputFilePath),
            bz2(),
            new N3TollerantStreamParser({format: 'N-Triples'}),
            executionExtractor,
            new DetourStream(({type}) => (type === 'query'), querySink),
            executionSink
        );

    }
}

// async function* asyncMap(input, fn) {
//     for await (const item of input) {
//         yield fn(item);
//     }
// }

// async function asyncForEach(input, fn) {
//     const promises = [];
//     // for await (const promise of asyncMap(input, fn)) {
//     //     promises.push(promise);
//     // }
//     for await (const item of input) {
//         promises.push(fn(item));
//     }
//     await Promise.all(promises);
// }

async function asyncForEach(input, fn) {
    for await (const item of input) {
        await fn(item);
    }
}

async function loadDatasetsAndFilter(options) {
    const {inputEndpointURL, datasetsGraphname} = options;
    if (datasetsGraphname) {
        const datasets = queryEndpoint(
            inputEndpointURL, [datasetsGraphname], datasetsQuery);
        await asyncForEach(datasets, async ({dataset, datasetId}) => {
            if (!options.excludeDatasets || !options.excludeDatasets.includes(dataset)) {
                console.log('Starting dataset ' + dataset);
                try {
                    await loadDatasetAndFilter(dataset, datasetId, options);
                    console.log('Done with ' + dataset);
                } catch (e) {
                    console.log('Error in ' + dataset + ':');
                    console.log(e);
                }
            }
        });
    }
}

async function main() {
    await loadDatasetsAndFilter({
        inputEndpointURL: 'http://localhost:3030/lsq2/query', 
        datasetsGraphname: 'http://lsq.aksw.org/datasets',
        remoteDirPath: 'https://hobbitdata.informatik.uni-leipzig.de/lsqv2/dumps/',
        // outputDirPath: './input/datasets/csv/',
        inputDirPath: './input/lsq2-source/',
        outputDirPath: './input/rdf/datasets/',
        // outputCsv: true,
        excludeDatasets: [
            'http://lsq.aksw.org/datasets/bench-affymetrix-lsq2',
            'http://lsq.aksw.org/datasets/bench-biomedels-lsq2',
            'http://lsq.aksw.org/datasets/bench-bioportal-lsq2', 
            'http://lsq.aksw.org/datasets/bench-ctd-lsq2', 
            'http://lsq.aksw.org/datasets/bench-dbpedia-20151025-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbpedia-20151124-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbpedia-20151126-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbpedia-20151213-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbpedia-20151230-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbpedia-20160117-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbpedia-20160212-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbpedia-20160222-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbpedia-20160301-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbpedia-20160303-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20160304-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20160314-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20160411-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia.3.5.1.log-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbsnp-lsq2',
            'http://lsq.aksw.org/datasets/bench-drugbank-lsq2',
            'http://lsq.aksw.org/datasets/bench-genage-lsq2',
            'http://lsq.aksw.org/datasets/bench-gendr-lsq2',
            'http://lsq.aksw.org/datasets/bench-gene-lsq2',
            'http://lsq.aksw.org/datasets/bench-goa-lsq2',
            'http://lsq.aksw.org/datasets/bench-hgnc-lsq2', 
            'http://lsq.aksw.org/datasets/bench-homologene-lsq2',
            'http://lsq.aksw.org/datasets/bench-irefindex-lsq2',
            'http://lsq.aksw.org/datasets/bench-kegg-lsq2',
            'http://lsq.aksw.org/datasets/bench-linkedGeoData-lsq2',
            'http://lsq.aksw.org/datasets/bench-linkedspl-lsq2',
            'http://lsq.aksw.org/datasets/bench-mgi-lsq2',
            'http://lsq.aksw.org/datasets/bench-ncbigene-lsq2',
            'http://lsq.aksw.org/datasets/bench-omim-lsq2',
            'http://lsq.aksw.org/datasets/bench-pharmgkb-lsq2',
            'http://lsq.aksw.org/datasets/bench-sabiork-lsq2',
            'http://lsq.aksw.org/datasets/bench-sgd-lsq2',
            'http://lsq.aksw.org/datasets/bench-sidr-lsq2',
            'http://lsq.aksw.org/datasets/bench-swdf-lsq2',
            'http://lsq.aksw.org/datasets/bench-taxonomy-lsq2',
            'http://lsq.aksw.org/datasets/bench-wikidata-interval1-organic-lsq2',
            'http://lsq.aksw.org/datasets/bench-wikidata-interval2-organic-lsq2',
            'http://lsq.aksw.org/datasets/bench-wikidata-interval3-organic-lsq2',
            'http://lsq.aksw.org/datasets/bench-wikidata-interval4-organic-lsq2',
            'http://lsq.aksw.org/datasets/bench-wikidata-interval5-organic-lsq2',
            'http://lsq.aksw.org/datasets/bench-wikidata-interval6-organic-lsq2',
            'http://lsq.aksw.org/datasets/bench-wikidata-interval7-organic-lsq2',
            'http://lsq.aksw.org/datasets/bench-wormbase-lsq2'
        ]
    });
}

main().then(() => {
    console.log('done!');
}, (reason) => {
    console.error(reason);
});
