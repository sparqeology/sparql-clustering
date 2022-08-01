import generalizeQuery from './generalizeQuery.js';

function buildGeneralizationForestFromQuery(queryText, queryDict, mappedQueries, queryForest) {
    if (queryText in queryDict) {
        if (!(queryText in mappedQueries)) {
            // const queryGeneralizations = queryDict[queryText].generalizations;
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

export default async function generalizeAndAggregate(source, options = {}) {
    const paramQueryMap = {};
    return new Promise((resolve, reject) => {
        source.on('data', query => {
            const paramQueries = generalizeQuery(query.text, options);
            paramQueries.shift(); // skip first item, which is the query itself
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
        });
        source.on('end', () => {
            var outputParamQueryMap = Object.fromEntries(
                Object.entries(paramQueryMap)
                        .filter(([k,v]) => (v.instances.length > 1))
                        .filter(([k,v]) => (
                                v.instances.map(paramQuery => paramQuery.bindings)
                                .reduce((fixedBindings, currBindings) => fixedBindings.map((fixed, index) => currBindings[index] === fixed ? fixed : null))
                                .every(value => value === null)
                        ))
            );
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
            resolve(outputParamQueryMap);
        });
        source.on('error', err => {
            reject(err);
        });
    });
}

