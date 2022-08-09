import {Parser} from 'sparqljs';

import lineColumn from 'line-column';

const QUERY_SYMBOLS = [
    'SELECT',
    'CONSTRUCT',
    'ASK',
    'DESCRIBE'
];

const GENERALIZABLE_SYMBOLS = [
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
];

const EOF = 6;



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
    const sparqlParser = new Parser();
    const lexer = sparqlParser.lexer;
    lexer.setInput(queryStr);
    const lcFinder = lineColumn(queryStr);
    var afterPreamble = false;
    var symbol;
    var prevStrEnd = 0;
    var parents = [{query: '', paramBindings: []}];
    var globalIndex = 0;
    while((symbol = lexer.lex()) !== EOF) {
        const loc = lexer.yylloc;
        const strStart = lcFinder.toIndex(loc.first_line, loc.first_column + 1);
        if (strStart > prevStrEnd) {
            parents = parents.map(parent => ({
                query: parent.query + queryStr.substring(prevStrEnd, strStart),
                paramBindings: parent.paramBindings,
                setBitmap: parent.setBitmap
            }))
        }
        if (!afterPreamble && QUERY_SYMBOLS.includes(sparqlParser.terminals_[symbol])) {
            afterPreamble = true;
            parents = parents.map(parent => ({
                query: (options.excludePreamble ? '' : parent.query) + lexer.match,
                paramBindings: parent.paramBindings,
                setBitmap: 0
            }))
        } else if (afterPreamble
                && (options.maxVars === undefined || options.maxVars )
                && GENERALIZABLE_SYMBOLS.includes(sparqlParser.terminals_[symbol])) {
            const parentsNoGen = parents.map(parent => ({
                query: parent.query + lexer.match,
                paramBindings: parent.paramBindings,
                setBitmap: parent.setBitmap
            }));
            const parentsToGen = options.maxVars === undefined ? parents : parents.filter(parent => parent.paramBindings.length < options.maxVars);
            const parentsGen = parentsToGen.map(parent => ({
                query: parent.query + '<PARAM_' + parent.paramBindings.length + '>',
                paramBindings: parent.paramBindings.concat([lexer.match]),
                setBitmap: parent.setBitmap + (2 ** globalIndex)
            }));
            parents = parentsNoGen.concat(parentsGen);
            globalIndex++;
        } else {
            parents = parents.map(parent => ({ query: parent.query + lexer.match, paramBindings: parent.paramBindings, setBitmap: parent.setBitmap}))
        }
        prevStrEnd = lcFinder.toIndex(loc.last_line, loc.last_column + 1);
    }
    parents = parents.map(parent => ({
        query: parent.query + queryStr.substring(prevStrEnd),
        paramBindings: parent.paramBindings,
        setBitmap: parent.setBitmap
    }));
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
// PREFIX  bio2rdf: <http://bio2rdf.org/>

// SELECT  ?mesh
// WHERE
//   { ?obo_voc  bio2rdf:obo_vocabulary:x-umls_cui  bio2rdf:umls:C1314417 ;
//               bio2rdf:obo_vocabulary:x-mesh_dui  ?mesh
//     FILTER ( ! isLiteral(bio2rdf:umls:C1314417) )
//   }
// `;

// PREFIX foaf: <http://xmlns.com/foaf/0.1/> 

// SELECT * {
//     ?mickey foaf:name "Mickey Mouse"@en;
//         foaf:knows ?other.
// }


// `;

// console.log(generalizeQuery(inputStr));

// console.log(generalizeQuery(inputStr, {maxVars: 0}));

// console.log(generalizeQuery(inputStr, {maxVars: 1}));

// console.log(generalizeQuery(inputStr, {maxVars: 2, generalizationTree: true}));
