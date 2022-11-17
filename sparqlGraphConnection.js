import httpCall from './httpCall.js';
import { Buffer } from 'node:buffer';

const DEFAULT_BUFFER_SIZE = 5000000;
const DEFAULT_MAX_PARALLEL_CALLS = 10;

export default class SparqlGraphConnection {

    constructor(url, options = {}) {
        this.options = options;
        this.url = url;
        this.buffer = Buffer.alloc(options.bufferSize || DEFAULT_BUFFER_SIZE);
        this.bufferPosition = 0;
        this.maxCalls = options.maxCalls || DEFAULT_MAX_PARALLEL_CALLS;
        this.promises = [];
        this.error = null;
    }

    _checkError() {
        if (this.error) {
            throw new Error(this.error);
        }
    }

    async _waitTillWaitingCallsAre(numMaxWaitingCalls) {
        this._checkError();
        while(this.promises.length > numMaxWaitingCalls) {
            await new Promise(resolve => setTimeout(resolve));
            this._checkError();
        }
    }

    async post(turtleStr) {
        this._checkError();
        const newDataBuffer = Buffer.from(turtleStr);
        const writtenBytes = newDataBuffer.copy(this.buffer, this.bufferPosition);
        this.bufferPosition += writtenBytes;
        if (writtenBytes < newDataBuffer.length) {
            await this._flush(newDataBuffer.toString('utf-8', writtenBytes));
        }
    }

    async _flush(lastTurtleStr) {
        const payload = (this.options.preamble || '') +
                this.buffer.toString('utf-8', 0, this.bufferPosition) +
                (lastTurtleStr || '');
        this.buffer.fill();
        this.bufferPosition = 0;
        await this._waitTillWaitingCallsAre(this.maxCalls);
        // console.log('Calling POST to ' + this.url + ' with content: ' + payload);
        const callPromise = httpCall(this.url, {
            method: 'POST',
            headers: {'Content-Type': 'text/turtle'},
            body: payload
        }); 
        this.promises.push(callPromise);
        callPromise.then(() => {
            this.promises.splice(this.promises.indexOf(callPromise), 1);
        }, (error) => {
            this.error = error;
        })
    }

    async sync() {
        // console.log('Flushing connection... ');
        await this._flush();
        // console.log('Done!');
        // console.log('Waiting for responses...');
        await this._waitTillWaitingCallsAre(0);
        // console.log('Done!');
    }

}

