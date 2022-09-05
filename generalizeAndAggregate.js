import generalizeQuery from './generalizeQuery.js';

function buildGeneralizationForestFromQuery(queryText, queryDict, mappedQueries, queryForest) {
    if (queryText in queryDict) {
        if (!(queryText in mappedQueries)) {
            const {generalizations: generalizations, ...queryObj} = { ...queryDict[queryText], specializations: {}};
            const generalizationObjs = generalizations
                    .map(generalization => buildGeneralizationForestFromQuery(generalization, queryDict, mappedQueries, queryForest))
                    .filter(generalization => generalization !== null);
            if (generalizationObjs.length > 0) {
                generalizationObjs.forEach(generalizationObj => {
                    generalizationObj.specializations[queryText] = queryObj;
                })
            } else {
                queryForest[queryText] = queryObj;
            }
            mappedQueries[queryText] = queryObj;
        }
        return mappedQueries[queryText];
    } else {
        return null;
    }
}

function buildGeneralizationForest(queryDict) {
    const mappedQueries = {};
    const queryForest = {};
    Object.keys(queryDict).forEach(queryText => {
        buildGeneralizationForestFromQuery(queryText, queryDict, mappedQueries, queryForest);
    });
    return queryForest;
}

function queryDictionariesAsArrays(queryDictionary) {
    if (queryDictionary === undefined) {
        return undefined;
    }
    return Object.entries(queryDictionary).map(([queryText, queryData]) => {
        return {
            queryText,
            specializations: queryDictionariesAsArrays(queryData.specializations),
            ...queryData
        };
    })
}

// const level = require('level')
// const db = level('./db', { valueEncoding: 'json' })

class MyMap {
    constructor() {
        this.data = {}
    }

    has(key) {
        return (key in this.data);
    }
    get(key) {
        return this.data[key];
    }
    async set(key, value) {
        this.data[key] = value;
    }
    entries() {
        return Object.entries(this.data);
    }
}

// class MyMap {
//     constructor() {
//         this.data = {}
//     }

//     has(key) {
//         return (key in this.data);
//     }
//     get(key) {
//         return this.data[key];
//     }
//     set(key, value) {
//         this.data[key] = value;
//     }
//     entries() {
//         return Object.entries(this.data);
//     }
// }

export default async function generalizeAndAggregate(queryStream, options = {}) {
    // const paramQueryMap = {};
    const paramQueryMap = new MyMap();
    console.time('create generalization dictionary');
    for await (const query of queryStream) {
        const paramQueries = generalizeQuery(query.text, options);
        if (!options.includeSimpleQueries) {
            paramQueries.shift(); // skip first item, which is the query itself
        }
        paramQueries.forEach(paramQuery => {
            // const paramQueryData = paramQueryMap.has(paramQuery.query) ?
            //         paramQueryMap.get(paramQuery.query) :
            //         (options.generalizationTree ?
            //             {
            //                 instances: [],
            //                 generalizations: paramQuery.moreGeneralQueries 
            //             } :
            //             {
            //                 instances: []
            //             });
            var paramQueryData;
            if (paramQueryMap.has(paramQuery.query)) {
                paramQueryData = paramQueryMap.get(paramQuery.query);
            } else {
                paramQueryData = options.generalizationTree ?
                    {
                        instances: [],
                        generalizations: paramQuery.moreGeneralQueries 
                    } :
                    {
                        instances: []
                    };
                paramQueryMap.set(paramQuery.query, paramQueryData);
            }
            // var paramQueryData = paramQueryMap.get(paramQuery.query);
            // if (paramQueryData === undefined) {
            //     paramQueryData = options.generalizationTree ?
            //         {
            //             instances: [],
            //             generalizations: paramQuery.moreGeneralQueries 
            //         } :
            //         {
            //             instances: []
            //         };
            // }
            paramQueryData.instances.push({
                originalQueryId: query.id,
                // originalQuery: query.text,
                bindings: paramQuery.paramBindings,
                numOfExecutions: query.numOfExecutions,
                numOfHosts: query.numOfHosts
            });
        //   if (! (paramQuery.query in paramQueryMap)) {
        //     paramQueryMap[paramQuery.query] =
        //             options.generalizationTree ?
        //                 {
        //                     instances: [],
        //                     generalizations: paramQuery.moreGeneralQueries 
        //                 } :
        //                 {
        //                     instances: []
        //                 };
        //   }
        //   paramQueryMap[paramQuery.query].instances.push({
        //     originalQueryId: query.id,
        //     // originalQuery: query.text,
        //     bindings: paramQuery.paramBindings,
        //     numOfExecutions: query.numOfExecutions,
        //     numOfHosts: query.numOfHosts
        //   });
        });
    }
    console.timeEnd('create generalization dictionary');
    console.time('filtering dictionary');
    var outputParamQueryMap = Object.fromEntries(
        // Object.entries(paramQueryMap)
        [...paramQueryMap.entries()]
                .filter(([k,v]) => {
                    const bindingsArray = v.instances.map(query => query.bindings);
                    const numOfBindings = bindingsArray.length;
                    const maxEqualBindings = options.minBindingDivergenceRatio ? (1.0 - options.minBindingDivergenceRatio) * numOfBindings : numOfBindings - 1;
                    return (bindingsArray
                            .reduce(
                                    (bindingsDistribution, currBindings) => bindingsDistribution.map((bindingDistr, index) => {
                                        bindingDistr[currBindings[index]] = (currBindings[index] in bindingDistr ? bindingDistr[currBindings[index]] : 0) + 1;
                                        return bindingDistr;
                                    }),
                                    numOfBindings > 0 ? bindingsArray[0].map(binding => ({})) : []
                            )
                            .every(bindingDistr => Object.values(bindingDistr).every(bindingCount => bindingCount <= maxEqualBindings)));
                })
    );
    if (options.showBindingDistributions) {
        outputParamQueryMap = Object.fromEntries(
            Object.entries(outputParamQueryMap)
                    .map(([k,v]) => {
                        const bindingsArray = v.instances.map(query => query.bindings);
                        const numOfBindings = bindingsArray.length;
                        const maxEqualBindings = options.minBindingDivergenceRatio ? (1.0 - options.minBindingDivergenceRatio) * numOfBindings : numOfBindings - 1;
                        return [
                            k,
                            {
                                ...v,
                                maxEqualBindings,
                                bindingDistribution:
                                        bindingsArray
                                                .reduce(
                                                        (bindingsDistribution, currBindings) => bindingsDistribution.map((bindingDistr, index) => {
                                                            bindingDistr[currBindings[index]] = (currBindings[index] in bindingDistr ? bindingDistr[currBindings[index]] : 0) + 1;
                                                            return bindingDistr;
                                                        }),
                                                        numOfBindings > 0 ? bindingsArray[0].map(binding => ({})) : []),
                            },
                        ];
                    })
        );   
    }
    if (options.minNumOfInstances) {
        outputParamQueryMap = Object.fromEntries(
            Object.entries(outputParamQueryMap)
                    .filter(([k,v]) => (v.instances.length >= options.minNumOfInstances))
        );    
    }
    if (options.minNumOfExecutions) {
        outputParamQueryMap = Object.fromEntries(
            Object.entries(outputParamQueryMap)
                    .filter(([k,v]) => (v.instances.reduce((sum, instance) => sum + instance.numOfExecutions, 0) >= options.minNumOfExecutions))
        );    
    }
    // if (options.minNumOfHosts) {
    //     outputParamQueryMap = Object.fromEntries(
    //         Object.entries(outputParamQueryMap)
    //                 .filter(([k,v]) => (v.instances.reduce((sum, instance) => sum + instance.numOfHosts, 0) >= options.minNumOfHosts))
    //     );    
    // }
    console.timeEnd('filtering dictionary');
    if (options.countInstances) {
        console.time('aggregating single instances');
        outputParamQueryMap = Object.fromEntries(
            Object.entries(outputParamQueryMap)
                    .map(([query,queryDataWithInstances]) => {
                        const {instances, ...queryData} = queryDataWithInstances;
                        return ([query, {
                            ...queryData,
                            numOfInstances: instances.length,
                            numOfExecutions: instances.reduce((sum, instance) => sum + instance.numOfExecutions, 0),
                            // numOfHosts: v.instances.reduce((sum, instance) => sum + instance.numOfHosts, 0)
                        }]);
                    })
        );
        console.timeEnd('aggregating single instances');
    }
    if (options.generalizationTree) {
        console.time('building generalization forest');
        outputParamQueryMap = buildGeneralizationForest(outputParamQueryMap);
        if (options.onlyRoots) {
            outputParamQueryMap = Object.fromEntries(
                Object.entries(outputParamQueryMap)
                        .map(([paramQuery, paramQueryDataAndSpecializations]) => {
                            const {specializations, ...paramQueryData} = paramQueryDataAndSpecializations;
                            return [paramQuery, paramQueryData];
                        })
            );
        }
        console.timeEnd('building generalization forest');
    }
    if (options.asArray) {
        console.time('converting to array');
        outputParamQueryMap = queryDictionariesAsArrays(outputParamQueryMap);
        console.timeEnd('converting to array');
    }
    return outputParamQueryMap;
}

