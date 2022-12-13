import fs from 'fs';
import { QueryEngine } from '@comunica/query-sparql-file';
import queryEndpoint from './queryEndpoint.js';

const myEngine = new QueryEngine();
const datasetsQuery = fs.readFileSync('./queries/datasets.rq');
const templateStatsQuery = '' + fs.readFileSync('./queries/templateStats.rq');


async function calculateStatsForDataset(dataset, datasetId, options) {
    const {inputDirPath, outputDirPath} = options;
    const inputFilePath = inputDirPath + datasetId + '.nt';
    const result = await myEngine.query(
        templateStatsQuery.replaceAll('$dataset', `<${dataset}>`), {
        sources: [inputFilePath]
    });
    const { data } = await myEngine.resultToString(result, 'text/csv');
    const outputFilePath = outputDirPath + datasetId + '.csv';
    data.pipe(fs.createWriteStream(outputFilePath)); // Print to standard output
}

async function calculateStats(options) {
    const {inputEndpointURL, datasetsGraphname} = options;
    if (datasetsGraphname) {
        const datasets = queryEndpoint(
            inputEndpointURL, [datasetsGraphname], datasetsQuery);
        for await (const {dataset, datasetId} of datasets) {
            if (!options.excludeDatasets || !options.excludeDatasets.includes(dataset)) {
                console.log('Dataset: ' + dataset);
                await calculateStatsForDataset(dataset, datasetId, options);
            }
        }
    }
}

async function main() {
    calculateStats({
        inputEndpointURL: 'http://localhost:3030/lsq2/query', 
        datasetsGraphname: 'http://lsq.aksw.org/datasets',
        inputDirPath: './output-rdf/',
        outputDirPath: './output/stats/topTemplates/',
        excludeDatasets: [
            'http://lsq.aksw.org/datasets/bench-affymetrix-lsq2',
            'http://lsq.aksw.org/datasets/bench-biomedels-lsq2',
            'http://lsq.aksw.org/datasets/bench-bioportal-lsq2',
            'http://lsq.aksw.org/datasets/bench-ctd-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-dbpedia-20151025-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20151124-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20151126-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-dbpedia-20151213-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-dbpedia-20151230-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-dbpedia-20160117-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20160212-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-dbpedia-20160222-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20160301-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20160303-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-dbpedia-20160304-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20160314-lsq2',
            // 'http://lsq.aksw.org/datasets/bench-dbpedia-20160411-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-dbpedia.3.5.1.log-lsq2',
            'http://lsq.aksw.org/datasets/bench-dbsnp-lsq2', // missing
            'http://lsq.aksw.org/datasets/bench-drugbank-lsq2',
            'http://lsq.aksw.org/datasets/bench-genage-lsq2',
            'http://lsq.aksw.org/datasets/bench-gendr-lsq2',
        //     // 'http://lsq.aksw.org/datasets/bench-gene-lsq2',
            'http://lsq.aksw.org/datasets/bench-goa-lsq2',
            'http://lsq.aksw.org/datasets/bench-hgnc-lsq2', // missing
        //     // 'http://lsq.aksw.org/datasets/bench-homologene-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-irefindex-lsq2',
        //     // 'http://lsq.aksw.org/datasets/bench-kegg-lsq2',
        //     // 'http://lsq.aksw.org/datasets/bench-linkedGeoData-lsq2',
        //     // 'http://lsq.aksw.org/datasets/bench-linkedspl-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-mgi-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-ncbigene-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-omim-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-pharmgkb-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-sabiork-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-sgd-lsq2',
        //     'http://lsq.aksw.org/datasets/bench-sidr-lsq2',
        //     // 'http://lsq.aksw.org/datasets/bench-swdf-lsq2',
        //     // 'http://lsq.aksw.org/datasets/bench-taxonomy-lsq2',
        // //     'http://lsq.aksw.org/datasets/bench-wikidata-interval1-organic-lsq2',
        // //     'http://lsq.aksw.org/datasets/bench-wikidata-interval2-organic-lsq2',
        // //     'http://lsq.aksw.org/datasets/bench-wikidata-interval3-organic-lsq2',
        // //     'http://lsq.aksw.org/datasets/bench-wikidata-interval4-organic-lsq2',
        // //     'http://lsq.aksw.org/datasets/bench-wikidata-interval5-organic-lsq2',
        // //     'http://lsq.aksw.org/datasets/bench-wikidata-interval6-organic-lsq2',
        // //     'http://lsq.aksw.org/datasets/bench-wikidata-interval7-organic-lsq2',
        //     // 'http://lsq.aksw.org/datasets/bench-wormbase-lsq2'
        ]
    })
}

main().then(() => {
    console.log('done!');
});