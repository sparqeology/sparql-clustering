import fsStream from 'node:stream';
import fs from 'node:fs';
import N3 from 'n3';

const DEFAULT_BUFFER_SIZE = 10000000;

async function turtleToNT(turtleStr, options = {}) {
    const format = options.format === 'application/n-quads' && options.outputGraphname ?
            'application/n-quads' :
            'application/n-triples';
    const turtleParser = new N3.Parser();
    const quadwriter = new N3.Writer({format});   
    return new Promise((resolve, reject) => {
        turtleParser.parse(turtleStr, (error, quad, prefixes) => {
            if (error) {
                reject(error);
            }
            if (quad) {
                if (format === 'application/n-quads') {
                    quadwriter.addQuad(
                        quad.subject, quad.predicate, quad.object,
                        N3.DataFactory.namedNode(options.outputGraphname));
                } else {
                    quadwriter.addQuad(quad);
                }
            } else {
                quadwriter.end((error, result) => {
                    if (error) {
                        reject(error);
                    } else {
                        resolve(result);
                    }
                });
            }
        })
    });
}


export default class RdfFileConnection {

    constructor(filePath, options = {}) {
        // console.log(this.options);
        this.filePath = filePath;
        this.options = options;
        this.buffer = Buffer.alloc(options.bufferSize || DEFAULT_BUFFER_SIZE);
        this.bufferPosition = 0;
    }

    async delete() {
        fs.promises.writeFile(this.filePath, '');
    }

    async post(turtleStr) {
        const nTriplesStr = await turtleToNT(this.options.preamble + '\n' + turtleStr, this.options);
        await this.addToBuffer(nTriplesStr);
    }

    async addToBuffer(nTriplesStr) {
        const newDataBuffer = Buffer.from(nTriplesStr);
        const writtenBytes = newDataBuffer.copy(this.buffer, this.bufferPosition);
        this.bufferPosition += writtenBytes;
        if (writtenBytes < newDataBuffer.length) {
            await this._flush(newDataBuffer.toString('utf-8', writtenBytes));
        }
    }

    async _flush(lastNTriplesStr) {
        await fs.promises.appendFile(this.filePath, this.bufferPosition >= this.buffer.byteLength ? this.buffer : this.buffer.slice(0, this.bufferPosition));
        this.buffer.fill();
        this.bufferPosition = 0;
        if (lastNTriplesStr) {
            this.addToBuffer(lastNTriplesStr);
        }
    }

    async sync() {
        await this._flush();
    }

}

