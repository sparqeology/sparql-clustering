import fsStream, { Transform } from 'node:stream';
import zlib from 'node:zlib';
import fs from 'node:fs';
import N3 from 'n3';
import { finished } from 'node:stream/promises';

const { DataFactory } = N3;
const { namedNode, literal, defaultGraph, quad } = DataFactory;

class GraphnameAdder extends Transform {

    constructor(graphname) {
        super({objectMode: true});
        this.graphname = graphname;
    }

    _transform(inputQuad, encoding, callback) {
        this.push(quad(
            inputQuad.subject, inputQuad.predicate, inputQuad.object,
            N3.DataFactory.namedNode(this.graphname)
        ));
        callback();
    }

}

export default class RdfFileStreamConnection {

    constructor(filePath, options = {}) {
        this.filePath = filePath;
        this.currFilePath = filePath;
        this.options = options;
        this.fileCounter = 0;
        this.charCounter = 0.0;
    }

    getOutputStream() {
        const format = this.options.format === 'application/n-quads' && this.options.outputGraphname ?
            'application/n-quads' :
            'application/n-triples';
        if (!this.outputStream) {
            const turtleParser = new N3.StreamParser();
            const quadStream = format === 'application/n-quads' ?
                turtleParser.pipe(new GraphnameAdder(this.options.outputGraphname)) :
                turtleParser;
            const ntStream = quadStream
                .pipe(new N3.StreamWriter({format}));
            const byteStream = this.options.compress ?
                ntStream.pipe(zlib.createGzip()) :
                ntStream;
            byteStream
                .pipe(fs.createWriteStream(this.currFilePath, {
                    flags: 'a'
                }));
            this.outputStream = turtleParser;
            if (this.options.preamble) {
                this.outputStream.write(this.options.preamble + '\n');
            }
        }
        return this.outputStream;
    }

    async delete() {
        await fs.promises.writeFile(this.filePath, '');
    }

    async post(turtleStr) {
        if (this.options.maxFileSize) {
            this.charCounter += turtleStr.length;
            if (this.charCounter > this.options.maxFileSize) {
                await this.sync();
                this.fileCounter++;
                const filePathSteps = this.filePath.split('/');
                const fileName = filePathSteps.pop();
                const dirPath = filePathSteps.join('/') + '/';
                const fileNameDotComponents = fileName.split('.');
                const baseFileName = fileNameDotComponents.shift();
                const extension = '.' + fileNameDotComponents.join('.');
                this.currFilePath = dirPath + baseFileName + '_' + this.fileCounter + extension;
                this.charCounter = 0;
            }
        }
        this.getOutputStream().write(turtleStr);
    }

    async sync() {
        if (this.outputStream) {
            this.outputStream.end();
            await finished(this.outputStream);
            this.outputStream = null;
        }
    }

}

// async function test () {
//     const connection = new RdfFileStreamConnection('./output/test.nt.gz', {
//         compress: true,
//         preamble: `
//         PREFIX ex: <http://example.org/>
//         `
//     });
//     await connection.delete();
//     await connection.post(`
//         ex:Pippo
//             a ex:TipoStrano;
//             ex:loves ex:Pluto;
//             ex:hates ex:Paperino.
//         ex:Pluto
//             a ex:TipoStrano;
//             ex:loves ex:Pizza.
//     `);
//     await connection.post(`
//         ex:Paperino
//             a ex:TipoStrano;
//             ex:hates ex:Pippo, ex:Pluto.
//     `);
//     await connection.post(`
//         ex:Pizza a ex:RobaBuona.
//     `);
//     await connection.sync();
// }
 
// test().then(() => {
//     console.log('done!');
// }, (error) => {
//     console.error(error);
// })
