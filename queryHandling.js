import Tokenizer from "./Tokenizer.js";
import { parsePreamble } from "./sparqlEncoding.js";

export function decomposeQuery(queryStr, options = {}) {
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
    let preamble;
    try {
        preamble = parsePreamble(tokenizer.preamble);
    } catch(exception) {
        // Not a valid preamble, probably query is sintactically incorrect, it falls back to put the "preamble" back in the query.
        preamble = {};
        queryPieces[0] = tokenizer.preamble + queryPieces[0];
    }
    return {queryPieces, constants, preamble};
}

export function createGeneralizedQuery(queryStr, options = {}) {
    const {queryPieces, constants, preamble} = decomposeQuery(queryStr, options);
    return {
        generalizedQuery: {
            queryPieces,
            parameterByPosition: constants.map((constant, index) => index)
        },
        constants,
        preamble
    };
}

export function generateParameterLabel(parameterIndex) {
    return '$__PARAM_' + parameterIndex;
}


export function toString({queryPieces, parameterByPosition}) {
    var queryStr = queryPieces[0];
    parameterByPosition.forEach((parameter, position) => {
        queryStr += generateParameterLabel(parameter) + queryPieces[position + 1];
    });
    return queryStr;
}

export function queriesExcept(queries, queriesToSubtract) {
    const querySet = new Set(queriesToSubtract.map(toString));
    return queries.filter(query => {
        const queryStr = toString(query);
        if (!querySet.has(queryStr)) {
            querySet.add(queryStr);
            return true;
        } else {
            return false;
        }
    });
}

class QuerySet {
    constructor(queries = []) {
        this.map = new Map(queries.map(query => [toString(query), query]));
    }

    addIfNotExisting(queries) {
        return queries.filter(query => {
            const queryStr = toString(query);
            if (!this.map.has(queryStr)) {
                this.map.set(queryStr, query);
                return true;
            } else {
                return false;
            }
        });
    }

    values() {
        return this.map.values();
    }
 }

export function uniqueQueries(queries) {
    const querySet = new Set();
    return queries.filter(query => {
        const queryStr = toString(query);
        if (!querySet.has(queryStr)) {
            querySet.add(queryStr);
            return true;
        } else {
            return false;
        }
    });
}

export function fixParameter({queryPieces, parameterByPosition, preamble}, parameterToFix, value) {
    const newQuery = {queryPieces: [queryPieces[0]], parameterByPosition: [], preamble};
    parameterByPosition.forEach((parameter, position) => {
        const queryPiece = queryPieces[position + 1];
        if (parameter === parameterToFix) {
            newQuery.queryPieces[newQuery.queryPieces.length - 1] += value + queryPiece;
        } else {
            newQuery.queryPieces.push(queryPiece);
            newQuery.parameterByPosition.push(parameter > parameterToFix ? parameter - 1 : parameter);
        }
    });
    return newQuery;
}

export function pairParameters({queryPieces, parameterByPosition, preamble}, parameter1, parameter2) {
    const newQuery = {queryPieces, parameterByPosition: [], preamble};
    parameterByPosition.forEach(parameter => {
        if (parameter === parameter2) {
            newQuery.parameterByPosition.push(parameter1);
        } else {
            newQuery.parameterByPosition.push(parameter > parameter2 ? parameter - 1 : parameter);
        }
    });
    return newQuery;
}

export function simplifyQueryBasic(parametricQuery, fromParameter = 0) {
    const firstInstancebindings = parametricQuery.instances[0].bindings;
    if (fromParameter >= firstInstancebindings.length) {
        return parametricQuery;
    }
    const firstValue = firstInstancebindings[fromParameter];
    let pairedParameter = fromParameter;
    while ((pairedParameter = firstInstancebindings.indexOf(firstValue, pairedParameter + 1)) !== -1) {
        if (parametricQuery.instances.every(({bindings}) => bindings[pairedParameter] === bindings[fromParameter])) {
            return simplifyQueryBasic(
                selectByPairedParameters(parametricQuery, fromParameter, pairedParameter, true),
                fromParameter);
        }
    }
    if (parametricQuery.instances.every(({bindings}) => bindings[fromParameter] === firstValue)) {
        return simplifyQueryBasic(
            selectByParameterValue(parametricQuery, fromParameter, firstValue, true),
            fromParameter);
    }
    return simplifyQueryBasic(parametricQuery, fromParameter + 1);
}

function selectByParameterValue({queryPieces, parameterByPosition, preamble, instances}, parameter, value, skipFilter = false) {
    return {
        ...fixParameter({queryPieces, parameterByPosition}, parameter, value),
        preamble,
        instances:
                (skipFilter ? instances : instances.filter(({bindings}) => bindings[parameter] === value))
                .map(({bindings, ...instanceData}) => ({
                    bindings: [...bindings.slice(0, parameter), ...bindings.slice(parameter + 1)],
                    ...instanceData
                }))
    }
}

function selectByPairedParameters({queryPieces, parameterByPosition, preamble, instances}, parameter1, parameter2, skipFilter = false) {
    return {
        ...pairParameters({queryPieces, parameterByPosition}, parameter1, parameter2),
        preamble,
        instances:
                (skipFilter ? instances : instances.filter(({bindings}) => bindings[parameter1] === bindings[parameter2]))
                .map(({bindings, ...instanceData}) => ({
                    bindings: [...bindings.slice(0, parameter2), ...bindings.slice(parameter2 + 1)],
                    ...instanceData
                }))
    }
}

function totNumOfExecutions(instances) {
    return instances.reduce((totNumOfExecutions, {numOfExecutions}) => totNumOfExecutions + numOfExecutions);
}

export function generateSpecializations(parametricQuery, fromParameter, options = {}) {
    if (fromParameter >= parametricQuery.instances[0].bindings.length) {
        return [];
    }

    const minNumOfInstances = options.minNumOfInstancesInSubclass || options.minNumOfInstances || 1;
    const minNumOfExecutions = options.minNumOfExecutionsInSubclass || options.minNumOfExecutions || 1;

    const instancesByValue = Object.create(null);
    const instancesByRepeatedParam = Object.create(null);

    parametricQuery.instances.forEach(instance => {
            const value = instance.bindings[fromParameter];
        if (!(value in instancesByValue)) {
            instancesByValue[value] = [instance];
        } else {
            instancesByValue[value].push(instance);
        }
        instance.bindings.forEach((otherValue, parameter) => {
            if (parameter > fromParameter && otherValue === value) {
                if (!(otherValue in instancesByRepeatedParam)) {
                    instancesByRepeatedParam[parameter] = [instance];
                } else {
                    instancesByRepeatedParam[parameter].push(instance);
                }
            }
        });
    });

    var valuesAndInstancesForSpecializations = Object.entries(instancesByValue);
    if (minNumOfInstances > 1) {
        valuesAndInstancesForSpecializations = valuesAndInstancesForSpecializations.filter(
                ([value, instances]) => instances.length >= minNumOfInstances);
    }
    if (minNumOfExecutions > 1) {
        valuesAndInstancesForSpecializations = valuesAndInstancesForSpecializations.filter(
                ([value, instances]) => totNumOfExecutions(instances) >= minNumOfExecutions);
    }
    const fixedValueSpecializations = valuesAndInstancesForSpecializations.map(([value, instances]) =>
            simplifyAndGenerateSpecializations(
                selectByParameterValue({
                    ...parametricQuery,
                    instances
                }, fromParameter, value, true), fromParameter, options));

    var paramAndInstancesForSpecializations = Object.entries(instancesByRepeatedParam);
    if (minNumOfInstances > 1) {
        paramAndInstancesForSpecializations = paramAndInstancesForSpecializations.filter(
                ([value, instances]) => instances.length >= minNumOfInstances);
    }
    if (minNumOfExecutions > 1) {
        paramAndInstancesForSpecializations = paramAndInstancesForSpecializations.filter(
                ([value, instances]) => totNumOfExecutions(instances) >= minNumOfExecutions);
    }
    const repeatedValueSpecializations = paramAndInstancesForSpecializations.map(([pairedParameter, instances]) =>
            simplifyAndGenerateSpecializations(
                selectByPairedParameters({
                    ...parametricQuery,
                    instances
                }, fromParameter, pairedParameter, true), fromParameter + 1, options));   

    const furtherSpecializations = generateSpecializations(parametricQuery, fromParameter + 1, options);
            
    return uniqueQueries([
        ...fixedValueSpecializations,
        ...repeatedValueSpecializations,
        ...furtherSpecializations
    ]);
    
}

export function simplifyAndGenerateSpecializations(parametricQuery, fromParameter, options = {}) {
    const simplifiedQuery = simplifyQueryBasic(parametricQuery);
    if (fromParameter >= parametricQuery.instances[0].bindings.length) {
        return {
            ...simplifiedQuery,
            specializations: []
        };
    }
    return {
        ...simplifiedQuery,
        specializations: generateSpecializations(simplifiedQuery, fromParameter, options)
    }
}

function pruneRedundantQueries({specializations, ...queryData}) {
    var indirectSpecializations = [];
    const directSpecializations = [];
    const querySet = new QuerySet();
    for (const specialization of specializations) {
        const {allSpecializations, ...prunedQuery} = pruneRedundantQueries(specialization);
        indirectSpecializations = indirectSpecializations.concat(querySet.addIfNotExisting(allSpecializations));
        directSpecializations.push(prunedQuery);
    }
    const filteredDirectSpecialization = querySet.addIfNotExisting(directSpecializations);
    return {
        ...queryData,
        specializations: filteredDirectSpecialization,
        allSpecializations: [...querySet.values()]
    }

}

export function buildSpecializationTree(queryClass, options = {}) {
    const {allSpecializations, ...specializationTree} = 
        pruneRedundantQueries(simplifyAndGenerateSpecializations(queryClass, 0, options));
    return specializationTree;
}

export function mergePreambles(preamble1, preamble2) {
    if (!preamble1) {
        return preamble2;
    }
    return {
        base: 'base' in preamble1 ? preamble1.base : preamble2.base,
        prefixes: 'prefixes' in preamble1 ? 'prefixes' in preamble2 ? {...preamble1.prefixes, ...preamble2.prefixes} : preamble1.prefixes : preamble2.prefixes
    };
}

// const inputStr = `
// PREFIX foaf: <http://xmlns.com/foaf/0.1/> 
// PREFIX xsd: <http://www.w3.org/2001/XMLSchema#/> 
// PREFIX ex: <http://www.example.com/> 

// SELECT * {
//     ?mickey foaf:name "Pippo"@en, "23"^^<http://www.w3.org/2001/XMLSchema#int>, 42, "12"^^xsd:int
//     VALUES ?mickey { <ex:m1> <ex:m2> <ex:m3> }
// #        foaf:knows ?other.
// }
// `;

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

