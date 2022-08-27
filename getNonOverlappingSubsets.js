function compareSets(setA, setB) {
    if (setA.length === 0) {
        return setB.length === 0 ? 0 : 1;
    } else if (setB.length === 0) {
        return -1;
    } else {
        const [itemA, ...restA] = setA;
        const [itemB, ...restB] = setB;
        const sign = Math.sign(itemA - itemB);
        return (sign === 0) ?
            compareSets(restA, restB) :
            sign;
    }
}

function getSubsets(items, minItems = 0, maxItems = Infinity) {
    return items.reduce((subsets, item) => [
        ...subsets,
        ...subsets.filter(subset => subset.length < maxItems).map(subset => [
            ...subset,
            item
        ])
    ], [[]]).filter(subset => subset.length >= minItems);
}

function lookup(sets, newSet, start, end) {
    if (start === undefined) {
        start = 0;
    }
    if (end === undefined) {
        end = sets.length;
    }
    if (start === end) {
        return start;
    } else {
        const middle = Math.floor((end - 1 - start) / 2) + start;
        return (compareSets(newSet, sets[middle]) < 0) ?
            lookup(sets, newSet, start, middle) :
            lookup(sets, newSet, middle + 1, end);
    }
}

function insertInOrder(sets, newSet) {
    if (newSet.length === 0) {
        return sets;
    }
    const index = lookup(sets, newSet);
    return [
        ...sets.slice(0, index),
        newSet,
        ...sets.slice(index)
    ]
}

function order(unorderedSets) {
    return unorderedSets.reduce(insertInOrder, []);
}

function minus(setA, setB) {
    return setA.filter(item => !setB.includes(item))
}

function reduce(family) {
    return [
        ...family.unassignedClusters.flatMap((cluster, index) => 
            getSubsets(cluster, 1).map(clusterSubset => ({
                unassignedClusters: insertInOrder([
                    ...family.unassignedClusters.slice(0, index),
                    ...family.unassignedClusters.slice(index + 1)
                ], minus(cluster, clusterSubset)),
                subsets: insertInOrder(family.subsets, clusterSubset)
            }))),
        ...family.subsets.flatMap((set, setIndex) => {
            const [setHead, ...setTail] = set;
            return getSubsets(setTail, 1).map(subset => ({
                    unassignedClusters: family.unassignedClusters,
                    subsets: [
                        ...family.subsets.slice(0, setIndex),
                        minus(set,subset),
                        ...insertInOrder(family.subsets.slice(setIndex + 1), subset)
                    ]
            }));
        })
    ];
} 

function reduceTree(family, tree = {}, options = {}) {
    const familyStr = JSON.stringify(family);
    if (!(familyStr in tree)) {
        const reductions = ('maxSubsets' in options) && family.subsets.length === options.maxSubsets ? [] : reduce(family);
        tree[familyStr] = {
            ...family,
            reductions: reductions.map(JSON.stringify)
        }
        reductions.forEach(reduction => reduceTree(reduction, tree, options));
    }
    return tree;
}

export default function getNonOverlappingFamilies(clusters, options = {}) {
    const emptyFamily = {
        unassignedClusters: clusters,
        subsets: []
    };
    var reduceTreeEntries = Object.entries(reduceTree(emptyFamily, {}, options));
    if (options.excludeEmptyFamily) {
        reduceTreeEntries = reduceTreeEntries.filter(([id, ...family]) => id !== emptyFamily);
    }
    const numItems = clusters.flat().length;
    return Object.fromEntries(
            reduceTreeEntries.map(([id, {reductions, subsets}]) => [id, {
                reductions,
                subsets,
                vector: [...Array(numItems).keys()].map(item => subsets.findIndex(subset => subset.includes(item)))
            }]));
}

// function test() {
//     // console.log(subsets([10,20,30], 2, 2));
//     // const result = reduce( {
//     //     unassignedClusters: [
//     //         [1, 5, 6],
//     //         [2],
//     //         [3, 7],
//     //         [4],
//     //         [8]
//     //     ],
//     //     subsets: []
//     // });
//     // console.log(JSON.stringify(result, null, 4));
//     // const result2 = result.map(reduce);
//     // console.log('result2');
//     // console.log(JSON.stringify(result2, null, 4));

//     const reductionTree = getNonOverlappingFamilies([
//             [0],
//             [1, 3],
//             [2]
//     ], {
//         // maxSubsets: 2
//     });

//     console.log(reductionTree);

// }

// test();