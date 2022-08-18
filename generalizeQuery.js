import {Parser} from 'sparqljs';

import { LookaheadLexer } from './lookaheadLexer.js';

class Tokenizer {
    constructor(queryStr, options = {}) {
        const sparqlParser = new Parser();
        this.QUERY_SYMBOLS = [
            'SELECT',
            'CONSTRUCT',
            'ASK',
            'DESCRIBE'
        ].map(terminal => sparqlParser.symbols_[terminal]);
        this.GENERALIZABLE_SYMBOLS = [
            'PNAME_LN',
            'IRIREF',
            'INTEGER',
            'DECIMAL',
            'DOUBLE',
            'INTEGER_POSITIVE',
            'DECIMAL_POSITIVE',
            'DOUBLE_POSITIVE',
            'INTEGER_NEGATIVE',
            'DECIMAL_NEGATIVE',
            'DOUBLE_NEGATIVE',
            'STRING_LITERAL1',
            'STRING_LITERAL2',
            'STRING_LITERAL_LONG1',
            'STRING_LITERAL_LONG2'
        ].map(terminal => sparqlParser.symbols_[terminal]);
        this.STRING_LITERAL_SYMBOLS = [
            'STRING_LITERAL1',
            'STRING_LITERAL2',
            'STRING_LITERAL_LONG1',
            'STRING_LITERAL_LONG2'
        ].map(terminal => sparqlParser.symbols_[terminal]);
        this.STRING_LITERAL_FOLLOWUP_SYMBOLS = [
            '^^',
            'LANGTAG'
        ].map(terminal => sparqlParser.symbols_[terminal]);
        this.DATATYPE_SYMBOL = sparqlParser.symbols_['^^'];
        this.LANGTAG_SYMBOL = sparqlParser.symbols_['LANGTAG'];
        this.EOF = sparqlParser.lexer.EOF;
        this.options = options;
        const baseLexer = sparqlParser.lexer;
        baseLexer.setInput(queryStr);
        this.lexer = new LookaheadLexer(baseLexer);
        this.afterPreamble = false;
    }

    next() {
        const symbol = this.lexer.next();
        if (symbol === this.EOF) {
            return null;
        }
        if (!this.afterPreamble) {
            if (this.QUERY_SYMBOLS.includes(symbol)) {
                this.afterPreamble = true;
                return {
                    match: this.lexer.match,
                    parameterizable: false
                };
            } else {
                return {
                    match: this.options.excludePreamble ? '' : this.lexer.match,
                    parameterizable: false
                }
            }
        } else if (this.GENERALIZABLE_SYMBOLS.includes(symbol)) {
            if (this.options.sparqlParameters &&
                this.STRING_LITERAL_SYMBOLS.includes(symbol) &&
                this.STRING_LITERAL_FOLLOWUP_SYMBOLS.includes(this.lexer.nextSymbol)) {
                const str = this.lexer.match;
                const nextSymbol = this.lexer.lex();
                if (nextSymbol === this.DATATYPE_SYMBOL) {
                    this.lexer.lex();
                }
                return {
                    match: str + this.lexer.match,
                    parameterizable: true
                }
            } else {
                return {
                    match: this.lexer.match,
                    parameterizable: true
                }
            }
        } else {
            return {
                match: this.lexer.match,
                parameterizable: false
            }
        }
    }
}

function generateParameterLabel(parameterIndex, options) {
    return options.sparqlParameters ?
            '$__PARAM_' + parameterIndex :
            '<PARAM_' + parameterIndex + '>';
}

/**
 * Creates generalized versions of a SPARQL query, replacing each time some of the constants (URIs or literals) with named placeholders (parameters 1,2, ...).
 * All the possible generalized versions are created.
 *
 * @param {string} queryStr SPARQL query string.
 * @param {Object} options
 * @param {boolean} options.excludePreamble exclude the query preamble when generating the generalizations.
 * @param {number} options.maxVars maximum number of parameters (replaced constants).
 * @param {number} options.generalizationTree for each parametric query generated, generates also the array of parametric queries in the set that are more general than the current one.
 * @return {{
 *    query: string,
 *    paramBindings: string [],
 *    moreGeneralQueries?: string []
 * } []} array of parametric queries associated to the corresponding binding (replacemente of parameters) leading bakc to the original query.
 */
 export default function generalizeQuery(queryStr, options = {}) {
    const tokenizer = new Tokenizer(queryStr, options);
    var tokenizerResult;
    var parents = [{query: '', paramBindings: [], setBitmap: 0}];
    var globalIndex = 0;
    var tokenIndex = 0;
    while((!options.maxTokens || tokenIndex < options.maxTokens) && (tokenizerResult = tokenizer.next()) !== null) {
        tokenIndex++;
        if (tokenizerResult.parameterizable) {
            const parentsNoGen = parents.map(parent => ({
                query: parent.query + tokenizerResult.match,
                paramBindings: parent.paramBindings,
                setBitmap: parent.setBitmap
            }));
            const parentsToGen = options.maxVars === undefined ? parents : parents.filter(parent => parent.paramBindings.length < options.maxVars);
            const parentsGen = parentsToGen.map(parent => ({
                query: parent.query + generateParameterLabel(parent.paramBindings.length, options),
                paramBindings: parent.paramBindings.concat([tokenizerResult.match]),
                setBitmap: parent.setBitmap + (2 ** globalIndex)
            }));
            parents = parentsNoGen.concat(parentsGen);
            globalIndex++;
        } else {
            parents = parents.map(parent => ({ query: parent.query + tokenizerResult.match, paramBindings: parent.paramBindings, setBitmap: parent.setBitmap}))
        }
    }
    parents = options.generalizationTree ?
            parents.map(parent => ({
                query: parent.query,
                paramBindings: parent.paramBindings,
                moreGeneralQueries: parents.filter(paramQuery => {
                    if ((paramQuery.setBitmap | parent.setBitmap) === paramQuery.setBitmap) {
                        const diff = paramQuery.setBitmap & ~parent.setBitmap
                        return diff > 0 && !(diff & (diff - 1));
                    } else {
                        return false;
                    }
                }).map(paramQuery => paramQuery.query)
            })) :
            parents.map(parent => ({
                query: parent.query,
                paramBindings: parent.paramBindings
            }))
    return parents;
}


// const inputStr = `
// PREFIX foaf: <http://xmlns.com/foaf/0.1/> 

// SELECT * {
//     ?mickey foaf:name "Mickey Mouse"@en
// #        foaf:knows ?other.
// }


// `;

// PREFIX  bio2rdf: <http://bio2rdf.org/>

// SELECT  ?mesh
// WHERE
//   { ?obo_voc  bio2rdf:obo_vocabulary:x-umls_cui  bio2rdf:umls:C1314417 ;
//               bio2rdf:obo_vocabulary:x-mesh_dui  ?mesh
//     FILTER ( ! isLiteral(bio2rdf:umls:C1314417) )
//   }
// `;

// console.log(generalizeQuery(inputStr, {sparqlParameters: true}));
