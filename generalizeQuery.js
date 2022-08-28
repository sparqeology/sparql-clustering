import {Parser} from 'sparqljs';

import { LookaheadLexer } from './lookaheadLexer.js';
import getNonOverlappingFamilies from './getNonOverlappingSubsets.js';

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

function buildGeneralizedQuery(queryVectorEncoding, {queryPieces, constants}, options) {
    return queryVectorEncoding.map((item, index) => 
            queryPieces[index] +
            (item === -1 ? constants[index] : generateParameterLabel(item, options))
        ).join('') + queryPieces.at(-1);
}

function buildBindings(querySubsetsEncoding, constants, options) {
    const bindings = [];
    return querySubsetsEncoding.map(subset => constants[subset[0]]);
}

function decomposeQuery(queryStr, options = {}) {
    const tokenizer = new Tokenizer(queryStr, options);
    var tokenizerResult;
    var tokenIndex = 0;
    const queryPieces = [''];
    const constants = [];

    while((!options.maxTokens || tokenIndex < options.maxTokens) && (tokenizerResult = tokenizer.next()) !== null) {
        tokenIndex++;
        if (tokenizerResult.parameterizable) {
            constants.push(tokenizerResult.match);
            queryPieces.push('');
        } else {
            queryPieces[queryPieces.length-1] += tokenizerResult.match;
        }
    }
    return {queryPieces, constants};
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
 * } []} array of parametric queries associated to the corresponding binding (replacement of parameters) leading back to the original query.
 */
 export default function generalizeQuery(queryStr, options = {}) {
    const {queryPieces, constants} = decomposeQuery(queryStr, options);
    const constantPositions = {};
    const constantsUnique = [];
    constants.forEach((constant, constantIndex) => {
        if (!(constant in constantPositions)) {
            constantPositions[constant] = [];
            constantsUnique.push(constant);
        }
        constantPositions[constant].push(constantIndex);
    });

    const reduceTree = getNonOverlappingFamilies(
            constantsUnique.map(constant => constantPositions[constant]),
            {maxSubsets: options.maxVars, memoized: true});

    if (options.generalizationTree) {
        const generalizationTree = Object.fromEntries(Object.entries(reduceTree).map(([familyId, {vector, subsets, reductions}]) => ([
            familyId,
            {
                query: buildGeneralizedQuery(vector, {queryPieces, constants}, options),
                paramBindings: buildBindings(subsets, constants),
                reductions
            }
        ])));
        return Object.values(generalizationTree).map(({reductions, ...generalizationData}) => ({
            moreGeneralQueries: reductions.map(reductionId => generalizationTree[reductionId].query),
            ...generalizationData
        }));
    } else {
        return Object.values(reduceTree).map(({vector, subsets}) => ({
            query: buildGeneralizedQuery(vector, {queryPieces, constants}, options),
            paramBindings: buildBindings(subsets, constants)
        }));
    }
}


// const inputStr = `
// PREFIX foaf: <http://xmlns.com/foaf/0.1/> 

// SELECT * {
//     ?mickey foaf:name "Mickey Mouse"@en
// #        foaf:knows ?other.
// }


// `;

// const inputStr = `

// PREFIX  bio2rdf: <http://bio2rdf.org/>

// SELECT  ?mesh
// WHERE
//   { ?obo_voc  bio2rdf:obo_vocabulary:x-umls_cui  bio2rdf:umls:C1314417 ;
//               bio2rdf:obo_vocabulary:x-mesh_dui  ?mesh
//     FILTER ( ! isLiteral(bio2rdf:umls:C1314417) )
//   }

// `;

// console.log(generalizeQuery(inputStr, {
//     sparqlParameters: true,
//     generalizationTree: true
// }));
