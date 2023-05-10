// import {Parser} from 'sparqljs';
import Sparqljs from 'sparqljs';

// console.log(Sparqljs);
const {Parser, Generator} = Sparqljs;

export function extractFeaturesFromQueryString(queryStr, options = {}) {
    const parser = new Parser();
    const generator = options.generator || new Generator({}).createGenerator();
    // LIMIT\s*\$__PARAM_[0-9]*
    queryStr = queryStr.replaceAll(/LIMIT\s*\$__PARAM_[0-9]*/g, 'LIMIT 10');
    queryStr = queryStr.replaceAll(/OFFSET\s*\$__PARAM_[0-9]*/g, 'OFFSET 10');
    queryStr = queryStr.replaceAll(
        /VALUES\s*([^{$]*)\s\$__PARAM_[0-9]*/g,
        (match, vars) => `VALUES ${vars} {}`);
    
    console.log(queryStr);
    const queryTree = parser.parse(queryStr);
    console.log(JSON.stringify(queryTree, null, 2));
    // const {clauses, triples} =
    return {
        queryType: queryTree.queryType,
        ...extractFeatures(queryTree.where, {
            generator, ...options
        })
    };
    // return {
    //     queryType: queryTree.queryType,
    //     clauses, triples
    // }
}

function mergeDictsIn(dictBase, ...dicts) {
    dicts.forEach(dict => Object.entries(dict).forEach(([key, newValues]) => {
        if (key in dictBase) {
            if (newValues instanceof Array) {
                newValues.forEach(newValue => {
                    if (!(dictBase[key].includes(newValue))) {
                        dictBase[key].push(newValue);
                    }
                });
            } else {
                mergeDictsIn(dictBase[key], newValues);
            }
        } else {
            dictBase[key] = newValues;
        }
    }));
    return dictBase;
}

function emptyExtract() {
    return {clauses: [], triples: [], termsByType: {}};
}

function extractOpsFromExpression(expr, options = {}) {
    if (expr.termType) {
        const generator = options.generator || new Generator({}).createGenerator();
        return {[generator.toEntity(expr)]: 1};
    }
    if (['operation','functionCall'].includes(expr.type)) {
        expr.args.map(arg => extractOpsFromExpression(arg, options)).reduce((merge, newDict) => ({
            // TODO review!!!!!!!!
        }))
    }
}

export function extractFeatures(queryTree, options = {}) {
    // const generator = options.generator || new Generator({}).createGenerator();
    if (Array.isArray(queryTree)) {
        return queryTree.map(subTree => extractFeatures(subTree, options)).reduce((merge, {clauses, triples, termsByType}) => ({
            clauses: [...merge.clauses, ...clauses],
            triples: [...merge.triples, ...triples],
            termsByType: mergeDictsIn(merge.termsByType, termsByType),
        }), emptyExtract());
    }
    // console.log(queryTree);
    if (queryTree.type === 'bgp') {
        const generator = options.generator || new Generator({}).createGenerator();
        return {
            clauses: ['bgp'],//{bgp: 1},
            triples: queryTree.triples.map(({subject, predicate, object}) => ({
                subject: generator.toEntity(subject),
                predicate: generator.toEntity(predicate),
                object: generator.toEntity(object)
            })),
            termsByType: {
                subjObj: queryTree.triples.flatMap(({subject, predicate, object}) => [
                    {[subject.termType]: [generator.toEntity(subject)]},
                    {[object.termType]: [generator.toEntity(object)]}
                ]).reduce((dict, keyValue) => mergeDictsIn(dict, keyValue), {}),
                pred: queryTree.triples.map(({subject, predicate, object}) => ({
                    [predicate.termType || predicate.type]: [generator.toEntity(predicate)]
                })).reduce((dict, keyValue) => mergeDictsIn(dict, keyValue), {})
            }
        }
    }
    if (queryTree.type === 'bind') {
        return {
            ...emptyExtract(),
            clauses: ['bind'],
            termsByType: {
                bindVariables: [queryTree.variable],
                expressionOps: []
            }
        }
    }
    if (queryTree.patterns) {
        const {clauses, ...extract} = extractFeatures(queryTree.patterns);
        return {
            clauses: [queryTree.type, ...clauses],
            ...extract
        }
    }
    return {
        ...emptyExtract(),
        clauses: [queryTree.type]
    }
    // console.log(parsedQuery);
}

const inputStr = `
PREFIX foaf: <http://xmlns.com/foaf/0.1/> 
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#/> 
PREFIX ex: <http://www.example.com/> 

SELECT * {
    ?mickey foaf:name "Pippo"@en, "23"^^<http://www.w3.org/2001/XMLSchema#int>, 42, "12"^^xsd:int.
    ?mickey foaf:knows/foaf:knows ex:Gigi.
    VALUES ?mickey { <ex:m1> <ex:m2> <ex:m3> }
    OPTIONAL {
        ?mickey foaf:knows ?ciccio. 
        OPTIONAL { ?ciccio foaf:knows ?pablo }
    }
    FILTER(xsd:int(?ciccio) + 3 >= str("wow")).
    VALUES ?pablo  $__PARAM_56.
    ex:Gigi foaf:knows/foaf:knows ex:Gigi.
    VALUES (?cicio ?pasticcio)  $__PARAM_31.
    BIND(2 * 20 + 2 AS ?fortytwo).
#        foaf:knows ?other.
}
LIMIT    $__PARAM_45
OFFSET   $__PARAM_22
`;

console.log(extractFeaturesFromQueryString(inputStr));

// const {generalizedQuery, constants} = createGeneralizedQuery(inputStr, {sparqlParameters: true});

// console.log(generalizedQuery);
// console.log(constants);


// // console.log(toString(generalizedQuery));

// const queryClass = {
//     ...generalizedQuery,
//     instances: [
//         {bindings: constants, numOfExecutions: 3},
//         {bindings: constants, numOfExecutions: 5},
//         {bindings: [
//             'foaf:name',
//             '"Pippo"',
//             // 'foaf:hates'
//         ], numOfExecutions: 8},
//         // {bindings: ['foaf:name', '"Donald Duck"', 'foaf:hates'], numOfExecutions: 4},
//         {bindings: [
//             'foaf:hates', '"Rodolfo"',
//             // 'foaf:hates'
//         ], numOfExecutions: 6},
//         // {bindings: ['foaf:hates', '"Rambo"', 'foaf:hates'], numOfExecutions: 2},
//     ]
// }

// const queryClass2 =    {
//     "queryPieces": [
//       "SELECT  ?s ?o\nWHERE\n  { GRAPH bio2rdf:bioportal_resource:bio2rdf.dataset.bioportal.R3.statistics\n      { ?s  ",
//       "  ?o }\n  }\nOFFSET  ",
//       "\nLIMIT   ",
//       "\n"
//     ],
//     "parameterByPosition": [
//       0,
//       1,
//       2
//     ],
//     "instances": [
//       {
//         "bindings": [
//           "rdf:type",
//           "5000",
//           "5000"
//         ],
//         "id": "14d6836f1070e73ab44fea4fcef086f0f952658aeaf7313da011e4160db93d22",
//         "numOfExecutions": 2,
//         "numOfHosts": 1
//       },
//       {
//         "bindings": [
//           "rdfs:label",
//           "10000",
//           "10000"
//         ],
//         "id": "19dd3bdcef3066c9d30a06c6f713c1f4b7f51de5cad50be1a8be6ead8f3c2498",
//         "numOfExecutions": 7,
//         "numOfHosts": 1
//       },
//       {
//         "bindings": [
//           "rdf:type",
//           "120000",
//           "5000"
//         ],
//         "id": "06f2e8e1506f5aba60c8b4ab6d4dc1652cdba9c332049cdc9155cbf3dfa762f8",
//         "numOfExecutions": 1,
//         "numOfHosts": 1
//       }
//     ]
//   };
  
//   // // console.log(JSON.stringify(simplifyAndGenerateSpecializations(queryClass, 0), null, 2));


// // console.log(JSON.stringify(buildSpecializationTree(queryClass), null, 2));

// console.log(JSON.stringify(simplifyQueryBasic(queryClass2), null, 2));

