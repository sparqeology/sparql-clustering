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

export default async function generalizeAndAggregate(queryStream, options = {}) {
    const paramQueryMap = {};
    for await (const query of queryStream) {
        const paramQueries = generalizeQuery(query.text, options);
        if (!options.includeSimpleQueries) {
            paramQueries.shift(); // skip first item, which is the query itself
        }
        paramQueries.forEach(paramQuery => {
          if (! (paramQuery.query in paramQueryMap)) {
            paramQueryMap[paramQuery.query] =
                    options.generalizationTree ?
                        {
                            instances: [],
                            generalizations: paramQuery.moreGeneralQueries 
                        } :
                        {
                            instances: []
                        };
          }
          paramQueryMap[paramQuery.query].instances.push({
            originalQueryId: query.id,
            // originalQuery: query.text,
            bindings: paramQuery.paramBindings,
            numOfExecutions: query.numOfExecutions,
            numOfHosts: query.numOfHosts
          });
        });
    }
    var outputParamQueryMap = Object.fromEntries(
        Object.entries(paramQueryMap)
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
    if (options.minNumOfHosts) {
        outputParamQueryMap = Object.fromEntries(
            Object.entries(outputParamQueryMap)
                    .filter(([k,v]) => (v.instances.reduce((sum, instance) => sum + instance.numOfHosts, 0) >= options.minNumOfHosts))
        );    
    }
    if (options.countInstances) {
        outputParamQueryMap = Object.fromEntries(
            Object.entries(outputParamQueryMap)
                    .map(([k,v]) => ([k, {
                        ...v,
                        instances: v.instances.length,
                        numOfExecutions: v.instances.reduce((sum, instance) => sum + instance.numOfExecutions, 0),
                        numOfHosts: v.instances.reduce((sum, instance) => sum + instance.numOfHosts, 0)
                    }]))
        );
    }
    if (options.generalizationTree) {
        outputParamQueryMap = buildGeneralizationForest(outputParamQueryMap);
    }
    return outputParamQueryMap;
}

