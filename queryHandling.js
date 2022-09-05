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
    // const newQueryPieces = [queryPieces[0]];
    // const newParameterByPosition
    const newQuery = {queryPieces: [queryPieces[0]], parameterByPosition: []};
    // var positionsToSkip = 0;
    // const positionsFixed = [];
    parameterByPosition.forEach((parameter, position) => {
        const queryPiece = queryPieces[position + 1];
        if (parameter === parameterToFix) {
            newQuery.queryPieces[newQuery.queryPieces.length - 1] += value + queryPiece;
            // positionsToSkip++;
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
    // console.log('Simplifying' + toString({queryPieces, parameterByPosition}) + 'with instances' + JSON.stringify(instances) + '...');
    const firstInstancebindings = instances[0].bindings;
    if (fromParameter >= firstInstancebindings.length) {
        return {queryPieces, parameterByPosition, instances};
    }
    const firstValue = firstInstancebindings[fromParameter];
    let pairedParameter;
    while ((pairedParameter = firstInstancebindings.indexOf(firstValue, fromParameter + 1)) !== -1) {
        if (instances.every(({bindings}) => bindings[pairedParameter] === bindings[fromParameter])) {
            return simplifyQueryBasic(
                selectByPairedParameters({queryPieces, parameterByPosition, instances}, fromParameter, pairedParameter, true),
                // {
                //     ...pairParameters({queryPieces, parameterByPosition}, fromParameter, pairedParameter),
                //     instances: instances.map(({bindings, ...instanceData}) => ({
                //         bindings: [...bindings.slice(0, pairedParameter), ...bindings.slice(pairedParameter + 1)],
                //         ...instanceData
                //     }))
                // },
                fromParameter);
        }
    }
    if (instances.every(({bindings}) => bindings[fromParameter] === firstValue)) {
        return simplifyQueryBasic(
            selectByParameterValue({queryPieces, parameterByPosition, instances}, fromParameter, firstValue, true),
            // {
            //     ...fixParameter({queryPieces, parameterByPosition}, fromParameter, firstValue),
            //     instances: instances.map(({bindings, ...instanceData}) => ({
            //         bindings: [...bindings.slice(0, fromParameter), ...bindings.slice(fromParameter + 1)],
            //         ...instanceData
            //     }))
            // },
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

var callCount = 0;

export function generateSpecializations(inputQuery, fromParameter, options = {}) {
    if (fromParameter >= inputQuery.instances[0].bindings.length) {
        return [];
    }
    // let {queryPieces, parameterByPosition, instances} =
    //         simplifyQueryBasic(inputQuery, fromParameter, options);

    const {queryPieces, parameterByPosition, instances} = inputQuery;

    const minNumOfInstances = options.minNumOfInstancesInSubclass || options.minNumOfInstances || 1;
    const minNumOfExecutions = options.minNumOfExecutionsInSubclass || options.minNumOfExecutions || 1;

    // const bindingDistrByInstances = Object.create(null);
    // const bindingDistrByExecutions = Object.create(null);
    const instancesByValue = Object.create(null);
    const instancesByRepeatedParam = Object.create(null);

    // const bindingDistr = Object.create(null);
    // instances.forEach(({bindings, numOfExecutions}) => {
    instances.forEach(instance => {
            const value = instance.bindings[fromParameter];
        if (!(value in instancesByValue)) {
            instancesByValue[value] = [instance];
            // bindingDistr[value] = {numOfInstances: 1, numOfExecutions};
            // bindingDistrByInstances[value] = 1;
            // bindingDistrByExecutions[value] = numOfExecutions;
        } else {
            instancesByValue[value].push(instance);
            // const prevBindingDistr = bindingDistr[value];
            // bindingDistr[value] = {
            //     numOfInstances: prevBindingDistr.numOfInstances + 1,
            //     numOfExecutions: prevBindingDistr.numOfExecutions + numOfExecutions
            // }
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
            
    const callNum = callCount++;
    // console.log(`Call n. ${callNum}.
    // fromParameter: ${fromParameter}
    // numOfInstances: ${instances.length}
    // The following specializations have been generated:
    // Fixed Value (call n. ${callNum}): ${fixedValueSpecializations.map(toString)}
    // Repeated Value (call n. ${callNum}): ${repeatedValueSpecializations.map(toString)}
    // Further (call n. ${callNum}): ${furtherSpecializations.map(toString)}
    // Total (call n. ${callNum}): ${uniqueQueries([
    //     ...fixedValueSpecializations,
    //     ...repeatedValueSpecializations,
    //     ...furtherSpecializations
    // ]).map(toString)}`);

    // const directSpecializations = queriesExcept([
    //     ...fixedValueSpecializations,
    //     ...repeatedValueSpecializations,
    //     ...furtherSpecializations
    // ], allSpecializations);

    // return {
    //     directSpecializations,
    //     allSpecializations: [ ...allSpecializations, ...directSpecializations]
    // };
    return uniqueQueries([
        ...fixedValueSpecializations,
        ...repeatedValueSpecializations,
        ...furtherSpecializations
    ]);
    // return {
    //     queryPieces, parameterByPosition, instances,
    //     specializations: uniqueQueries([
    //         ...fixedValueSpecializations,
    //         ...repeatedValueSpecializations,
    //         ...furtherSpecializations
    //     ])
    // }
    
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
    const indirectSpecializations = [];
    const directSpecializations = [];
    // console.log(rootQuery);
    specializations.forEach(specialization => {
        const {allSpecializations, ...prunedQuery} = pruneRedundantQueries(specialization);
        indirectSpecializations.concat(allSpecializations);
        directSpecializations.push(prunedQuery);
    });
    const querySet = new QuerySet(indirectSpecializations);
    const filteredDirectSpecialization = querySet.addIfNotExisting(directSpecializations);
    return {
        ...queryData,
        specializations: filteredDirectSpecialization,
        // directSpecializations: filteredDirectSpecialization,
        allSpecializations: querySet.values()
    }

}

// export function simplifyQuery({queryPieces, parameterByPosition, instances}, fromParameter, maxEqualBindings) {
//     const bindingDistr = Object.create(null);
//     instances.forEach(({bindings}) => {
//         const value = bindings[fromParameter];
//         if (!(value in bindingDistr)) {
//             bindingDistr[value] = 1;
//         } else {
//             bindingDistr[value] += 1;
//         }
//     });
//     for (value in bindingDistr) {
//         if (value > maxEqualBindings) {
//             return [
//                 ...simplifyQuery({
//                     ...fixParameter({queryPieces, parameterByPosition}, fromParameter, value),
//                     instances:
//                             instances
//                             .filter(({bindings}) => bindings[fromParameter] === value)
//                             .map(({bindings, ...instanceData}) => ({
//                                 bindings: [...bindings.slice(0, fromParameter), ...bindings.slice(fromParameter + 1)],
//                                 ...instanceData
//                             }))
//                 }, fromParameter + 1, maxEqualBindings),
//                 ...simplifyQuery({
//                     queryPieces, parameterByPosition,
//                     instances: instances.filter(({bindings}) => bindings[fromParameter] !== value)
//                 }, fromParameter + 1, maxEqualBindings)
//             ];
//         }
//     }
// }


const inputStr = `
PREFIX foaf: <http://xmlns.com/foaf/0.1/> 

SELECT * {
    ?mickey foaf:name "Mickey Mouse"@en
#        foaf:knows ?other.
}


`;

const {generalizedQuery, constants} = createGeneralizedQuery(inputStr, {sparqlParameters: true});

console.log(generalizedQuery);


console.log(toString(generalizedQuery));

const queryClass = {
    ...generalizedQuery,
    instances: [
        {bindings: constants, numOfExecutions: 3},
        {bindings: constants, numOfExecutions: 5},
        {bindings: [
            'foaf:name',
            '"Pippo"',
            // 'foaf:hates'
        ], numOfExecutions: 8},
        // {bindings: ['foaf:name', '"Donald Duck"', 'foaf:hates'], numOfExecutions: 4},
        {bindings: [
            'foaf:hates', '"Rodolfo"',
            // 'foaf:hates'
        ], numOfExecutions: 6},
        // {bindings: ['foaf:hates', '"Rambo"', 'foaf:hates'], numOfExecutions: 2},
    ]
}

console.log(JSON.stringify(simplifyAndGenerateSpecializations(queryClass, 0), null, 2));

console.log(JSON.stringify(pruneRedundantQueries(simplifyAndGenerateSpecializations(queryClass, 0)), null, 2));

