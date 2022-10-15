import httpCall from './httpCall.js';

const DEFAULT_MAX_BUFFER_SIZE = 5000000;

export default class SparqlGraphConnection {

    constructor(url, options = {}) {
        this.options = options;
        this.url = url;
        this.buffer = '';
        this.maxBufferSize = options.maxBufferSize || DEFAULT_MAX_BUFFER_SIZE;
    }

    async post(turtleStr) {
        this.buffer += turtleStr;
        if (this.buffer.length >= this.maxBufferSize) {
            await this.flush();
        }
    }

    async flush() {
        const payload = (this.options.preamble || '') + this.buffer;
        this.buffer = '';
        await httpCall(this.url, {
            method: 'POST',
            headers: {'Content-Type': 'text/turtle'},
            body: payload
        });    
    }

}

