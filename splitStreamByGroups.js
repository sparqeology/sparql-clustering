import lookaheadGenerator from "./lookaheadGenerator.js";

async function* filterStreamByGroup(laStream) {
    // console.log('filterStreamByGroup()');
    for await (const record of laStream) {
        // console.log(record);
        yield record.query;
        // console.log('instance of group: ' + record.groupId);
        // process.stdout.write(record.groupId + laStream.next.groupId + '-');
        if (laStream.lookAhead === undefined || laStream.lookAhead.groupId !== record.groupId) {
            break;
        }
    }
}

export default async function* splitStreamByGroups(inputStream) {
    // console.log('splitStreamByGroups()');
    var laStream = await lookaheadGenerator(inputStream);
    // console.log(laStream.hasNext);
    // console.log(laStream.lookAhead);
    while(laStream.hasNext) {
        const currGroupId = laStream.lookAhead.groupId;
        // console.log('beginning of group: ' + currGroupId);
        yield {
            groupId: currGroupId,
            queryStream: filterStreamByGroup(laStream)
        };
        // console.log(laStream.hasNext);
        // console.log(laStream.lookAhead);
        while(laStream.hasNext && laStream.lookAhead.groupId === currGroupId) {
            await new Promise(resolve => setImmediate(resolve));
        }
    }
}


