import {buildSpecializationTree, createGeneralizedQuery, toString, mergePreambles} from './queryHandling.js';
import {preambleToString} from './sparqlEncoding.js'

function aggregateInstances({instances, specializations, ...queryData}) {
    return {
        ...queryData,
        instances,
        numOfInstances: instances.length,
        numOfExecutions: instances.reduce((sum, instance) => sum + instance.numOfExecutions, 0),
        timeOfFirstExecution: instances
            .map(instance => instance.timeOfFirstExecution)
            .reduce((minTime, time) =>  minTime < time ? minTime : time),
        timeOfLastExecution: instances
            .map(instance => instance.timeOfLastExecution)
            .reduce((maxTime, time) =>  maxTime > time ? maxTime : time),
        specializations: specializations.map(aggregateInstances),
        sumOfLogOfNumOfExecutions:
            instances.reduce((sum, instance) => sum + Math.log2(instance.numOfExecutions), 0)
    }
}

function textualForm({queryPieces, parameterByPosition, preamble, specializations, ...queryData}) {
    return {
        text: preambleToString(preamble) + '\n' + toString({queryPieces, parameterByPosition}),
        preamble,
        ...queryData,
        specializations: specializations.map(textualForm)
    }
}

function sortByNumOfExecutions(queryArray) {
    queryArray.sort((a, b) => b.numOfExecutions - a.numOfExecutions);
    for (const {specializations} of queryArray) {
        sortByNumOfExecutions(specializations);
    }
}

export default async function aggregateAndSpecialize(queryStream, options = {}) {
    const paramQueryMap = new Map();
    console.time('create generalization dictionary');
    var queryCounter = 0;
    var totalExecutions = 0;
    var timeOfFirstExecution = null, timeOfLastExecution = null;
    for await (const {text: queryText, ...queryData} of queryStream) {
        if (queryCounter % 1000 === 0) {
            process.stdout.write(('  ' + queryCounter / 1000).padStart(8, ' ') + ' K\r');
        }
        const {generalizedQuery, constants, preamble} = createGeneralizedQuery(queryText, options);
        const queryStr = toString(generalizedQuery);
        const instance = {
            bindings: constants,
            ...queryData
        };
        if (paramQueryMap.has(queryStr)) {
            const queryObj = paramQueryMap.get(queryStr);
            queryObj.instances.push(instance);
            queryObj.preamble = mergePreambles(queryObj.preamble, preamble);
        } else {
            paramQueryMap.set(queryStr, {
                ...generalizedQuery,
                instances: [instance],
                preamble
            });
        }
        queryCounter++;
        totalExecutions += queryData.numOfExecutions || 0;
        timeOfFirstExecution = queryData.timeOfFirstExecution &&
            (!timeOfFirstExecution || queryData.timeOfFirstExecution < timeOfFirstExecution) ?
                    queryData.timeOfFirstExecution :
                    timeOfFirstExecution
        timeOfLastExecution = queryData.timeOfLastExecution &&
            (!timeOfLastExecution || queryData.timeOfLastExecution > timeOfLastExecution) ?
                    queryData.timeOfLastExecution :
                    timeOfLastExecution
    }
    console.timeEnd('create generalization dictionary');
    console.log(queryCounter + ' queries managed');

    const totalQueries = queryCounter;

    console.time('build specialization forest');
    var queryForest = [];
    const nonClusterizedQueryIds = options.includeSimpleQueries ? [] : null;
    queryCounter = 0;
    for (const [queryStr, queryData] of paramQueryMap) {
        if (queryCounter % 1000 === 0) {
            process.stdout.write(('  ' + queryCounter / 1000).padStart(8, ' ') + ' K\r');
        }
        if ((!options.minNumOfInstances ||
                queryData.instances.length >= options.minNumOfInstances)
            && (!options.minNumOfExecutions ||
                queryData.instances.reduce((sum, instance) => sum + instance.numOfExecutions, 0) >= options.minNumOfExecutions)) {
            queryForest.push(buildSpecializationTree(queryData, options));
        } else if (options.includeSimpleQueries
            && queryData.instances.length == 1
            && (!options.minNumOfExecutionsForSimpleQueries
                || queryData.instances[0].numOfExecutions >= options.minNumOfExecutionsForSimpleQueries)) {
            nonClusterizedQueryIds.push(queryData.instances[0].id);
        }
        queryCounter++;
    }
    console.timeEnd('build specialization forest');
    console.log(queryCounter + ' specialization trees built');

    if (options.countInstances) {
        console.time('aggregating instances');
        queryForest = queryForest.map(aggregateInstances);
        console.timeEnd('aggregating instances');
    }

    console.time('queries as text');
    queryForest = queryForest.map(textualForm);
    console.timeEnd('queries as text');

    if (options.sortResults) {
        console.time('sort queries');
        sortByNumOfExecutions(queryForest);
        console.timeEnd('sort queries');
    }

    return {
        queryForest, nonClusterizedQueryIds,
        totalQueries, totalExecutions,
        timeOfFirstExecution, timeOfLastExecution
    };
}

