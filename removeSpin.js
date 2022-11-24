import fs from 'fs';
import httpCall from './httpCall.js';
import queryEndpoint from './queryEndpoint.js';

const LIST_DATASETS_QUERY = fs.readFileSync('./queries/listDatasets.rq');
const REMOVE_SPIN_UPDATE = '' + fs.readFileSync('./updates/removeSpinFromLsq.ru');

async function removeSpinFromDataset(updateEndpointUrl, datasetUri) {
    await httpCall(updateEndpointUrl, {
        method: 'POST',
        headers: {'Content-Type': 'application/sparql-update'},
        body: REMOVE_SPIN_UPDATE.replaceAll('?dataset', '<' + datasetUri + '>')
    });
}

export default async function removeSpin({queryEndpointUrl, updateEndpointUrl, datasetsSpecGraphname}) {
    for await (const {dataset} of queryEndpoint(queryEndpointUrl, [datasetsSpecGraphname], LIST_DATASETS_QUERY)) {
        try {
            console.log(`Removing SPIN from dataset ${dataset}...`);
            await removeSpinFromDataset(updateEndpointUrl, dataset);
            console.log('Done!');
        } catch(e) {
            console.log(e);
        }
    }
}

async function main() {
    await removeSpin({
        updateEndpointUrl: 'http://localhost:3030/lsq2/update',
        queryEndpointUrl: 'http://localhost:3030/lsq2/query',
        datasetsSpecGraphname: 'http://lsq.aksw.org/datasets'
    });
}

main().then(result => {
}, err => {
  console.error(err);
});