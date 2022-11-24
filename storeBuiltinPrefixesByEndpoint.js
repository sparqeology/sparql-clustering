import fs from 'fs';
import httpCall from './httpCall.js';
import { escapeLiteral } from './turtleEncoding.js';
import prefixJsonFileByService from './prefixJsonFileByService.json' assert { type: "json" };
const STORE_SERVICE_PREFIXES = '' + fs.readFileSync('./updates/storeServicePrefixes.ru');
const JSON_DATATYPE_URI = 'https://www.iana.org/assignments/media-types/application/json';

async function storeBuiltinPrefixes(updateEndpointUrl, serviceUri, jsonFilePath) {
    const prefixesJson = await fs.promises.readFile(jsonFilePath);
    // console.log({
    //     method: 'POST',
    //     headers: {'Content-Type': 'application/sparql-update'},
    //     body: STORE_SERVICE_PREFIXES
    //         .replaceAll('?service', `<${serviceUri}>`)
    //         .replaceAll('?prefixes', `"${prefixesJson}"^^<${JSON_DATATYPE_URI}>`)
    // });
    console.log(`Storing prefixes for service ${serviceUri}...`);
    await httpCall(updateEndpointUrl, {
        method: 'POST',
        headers: {'Content-Type': 'application/sparql-update'},
        body: STORE_SERVICE_PREFIXES
            .replaceAll('?service', `<${serviceUri}>`)
            .replaceAll('?prefixes', `${escapeLiteral(prefixesJson)}^^<${JSON_DATATYPE_URI}>`)
    });
    console.log('Done!');
}

export default async function storeBuiltinPrefixesByEndpoint(updateEndpointUrl) {
    for (const [serviceUri, jsonFilePath] of Object.entries(prefixJsonFileByService)) {
        await storeBuiltinPrefixes(updateEndpointUrl, serviceUri, jsonFilePath);
    }
}

async function main() {
    await storeBuiltinPrefixesByEndpoint('http://localhost:3030/lsq2/update');
}

main().then(result => {
}, err => {
  console.error(err);
});