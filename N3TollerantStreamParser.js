// **N3StreamParser** parses a text stream into a quad stream.
import N3 from 'n3';
import { Transform } from 'readable-stream';

// ## Constructor
export default class N3TollerantStreamParser extends Transform {
  constructor(options) {
    super({ decodeStrings: true });
    this._readableState.objectMode = true;

    // Set up parser with dummy stream to obtain `data` and `end` callbacks
    const parser = new N3.Parser(options);
    const innerResolveIRI = parser._resolveIRI;
    parser._resolveIRI = (iri) => {
        const resolvedIRI = innerResolveIRI.call(parser, iri);
        if (resolvedIRI) {
          return resolvedIRI;
        } else {
          console.warn("Invalid IRI replaced: " + iri);
          return parser._base;
        } 
    }
    let onData, onEnd;
    parser.parse({
      on: (event, callback) => {
        switch (event) {
        case 'data': onData = callback; break;
        case 'end':   onEnd = callback; break;
        }
      },
    },
      // Handle quads by pushing them down the pipeline
      (error, quad) => { error && this.emit('error', error) || quad && this.push(quad); },
      // Emit prefixes through the `prefix` event
      (prefix, uri) => { this.emit('prefix', prefix, uri); }
    );

    // Implement Transform methods through parser callbacks
    this._transform = (chunk, encoding, done) => { onData(chunk); done(); };
    this._flush = done => { onEnd(); done(); };
  }

  // ### Parses a stream of strings
  import(stream) {
    stream.on('data',  chunk => { this.write(chunk); });
    stream.on('end',   ()      => { this.end(); });
    stream.on('error', error => { this.emit('error', error); });
    return this;
  }
}