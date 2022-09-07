import Tokenizer from "./Tokenizer.js";

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
    return {queryPieces, constants};
}

export function createGeneralizedQuery(queryStr, options = {}) {
    const {queryPieces, constants} = decomposeQuery(queryStr, options);
    return {
        generalizedQuery: {
            queryPieces,
            parameterByPosition: constants.map((constant, index) => index)
        },
        constants
    };
}

function generateParameterLabel(parameterIndex) {
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

export function fixParameter({queryPieces, parameterByPosition}, parameterToFix, value) {
    const newQuery = {queryPieces: [queryPieces[0]], parameterByPosition: []};
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

export function pairParameters({queryPieces, parameterByPosition}, parameter1, parameter2) {
    const newQuery = {queryPieces, parameterByPosition: []};
    parameterByPosition.forEach(parameter => {
        if (parameter === parameter2) {
            newQuery.parameterByPosition.push(parameter1);
        } else {
            newQuery.parameterByPosition.push(parameter > parameter2 ? parameter - 1 : parameter);
        }
    });
    return newQuery;
}

export function simplifyQueryBasic({queryPieces, parameterByPosition, instances}, fromParameter = 0) {
    const firstInstancebindings = instances[0].bindings;
    // console.log(fromParameter);
    if (fromParameter >= firstInstancebindings.length) {
        return {queryPieces, parameterByPosition, instances};
    }
    const firstValue = firstInstancebindings[fromParameter];
    let pairedParameter = fromParameter;
    while ((pairedParameter = firstInstancebindings.indexOf(firstValue, pairedParameter + 1)) !== -1) {
        // console.log('checking if parameter ' + pairedParameter + ' is paired...');
        if (instances.every(({bindings}) => bindings[pairedParameter] === bindings[fromParameter])) {
            // console.log('yes it is!');
            return simplifyQueryBasic(
                selectByPairedParameters({queryPieces, parameterByPosition, instances}, fromParameter, pairedParameter, true),
                fromParameter);
        }
    }
    // console.log('checking if value ' + firstValue + ' is fixed...');
    if (instances.every(({bindings}) => bindings[fromParameter] === firstValue)) {
        // console.log('yes it is!');
        return simplifyQueryBasic(
            selectByParameterValue({queryPieces, parameterByPosition, instances}, fromParameter, firstValue, true),
            fromParameter);
    }
    return simplifyQueryBasic({queryPieces, parameterByPosition, instances}, fromParameter + 1);
}

function selectByParameterValue({queryPieces, parameterByPosition, instances}, parameter, value, skipFilter = false) {
    return {
        ...fixParameter({queryPieces, parameterByPosition}, parameter, value),
        instances:
                (skipFilter ? instances : instances.filter(({bindings}) => bindings[parameter] === value))
                .map(({bindings, ...instanceData}) => ({
                    bindings: [...bindings.slice(0, parameter), ...bindings.slice(parameter + 1)],
                    ...instanceData
                }))
    }
}

function selectByPairedParameters({queryPieces, parameterByPosition, instances}, parameter1, parameter2, skipFilter = false) {
    return {
        ...pairParameters({queryPieces, parameterByPosition}, parameter1, parameter2),
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

export function generateSpecializations(inputQuery, fromParameter, options = {}) {
    if (fromParameter >= inputQuery.instances[0].bindings.length) {
        return [];
    }

    const {queryPieces, parameterByPosition, instances} = inputQuery;

    const minNumOfInstances = options.minNumOfInstancesInSubclass || options.minNumOfInstances || 1;
    const minNumOfExecutions = options.minNumOfExecutionsInSubclass || options.minNumOfExecutions || 1;

    const instancesByValue = Object.create(null);
    const instancesByRepeatedParam = Object.create(null);

    instances.forEach(instance => {
            const value = instance.bindings[fromParameter];
        if (!(value in instancesByValue)) {
            instancesByValue[value] = [instance];
        } else {
            instancesByValue[value].push(instance);
        }
        instance.bindings.forEach((otherValue, parameter) => {
            if (parameter > fromParameter && otherValue === value) {
                if (!(otherValue in instancesByRepeatedParam)) {
                    instancesByRepeatedParam[otherValue] = [instance];
                } else {
                    instancesByRepeatedParam[otherValue].push(instance);
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
            simplifyAndGenerateSpecializations(selectByParameterValue({
                queryPieces,
                parameterByPosition,
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
            simplifyAndGenerateSpecializations(selectByPairedParameters({
                queryPieces,
                parameterByPosition,
                instances
            }, fromParameter, pairedParameter, true), fromParameter + 1, options));   

    const furtherSpecializations = generateSpecializations(inputQuery, fromParameter + 1, options);
            
    return uniqueQueries([
        ...fixedValueSpecializations,
        ...repeatedValueSpecializations,
        ...furtherSpecializations
    ]);
    
}

export function simplifyAndGenerateSpecializations(inputQuery, fromParameter, options = {}) {
    const simplifiedQuery = simplifyQueryBasic(inputQuery);
    if (fromParameter >= inputQuery.instances[0].bindings.length) {
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


// const inputStr = `
// PREFIX foaf: <http://xmlns.com/foaf/0.1/> 

// SELECT * {
//     ?mickey foaf:name "Mickey Mouse"@en
// #        foaf:knows ?other.
// }


// `;

// const {generalizedQuery, constants} = createGeneralizedQuery(inputStr, {sparqlParameters: true});

// // console.log(generalizedQuery);


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

