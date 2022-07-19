import generalizeQuery from './generalizeQuery.js';

function buildGeneralizationForestFromQuery(queryText, queryDict, mappedQueries, queryForest) {
    if (queryText in queryDict) {
        if (!(queryText in mappedQueries)) {
            // const queryGeneralizations = queryDict[queryText].generalizations;
            const {generalizations: generalizations, ...queryObj} = { ...queryDict[queryText], specializations: {}};
            if (generalizations.length > 0) {
                generalizations.forEach(generalization => {
                    const generalizationObj = buildGeneralizationForestFromQuery(generalization, queryDict, mappedQueries, queryForest);
                    if (generalizationObj) {
                        generalizationObj.specializations[queryText] = queryObj;
                    }
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
            // console.log(paramQueries);
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
                bindings: paramQuery.paramBindings
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
                                ...v, instances: v.instances.length
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

