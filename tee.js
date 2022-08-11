export default function tee(inputGenerator) {
    const buffers = [[], []];
    function* yieldBuffer(buffer) {
        for (var bufferIndex = 0; bufferIndex < buffer.length; bufferIndex++) {
            yield buffer[bufferIndex];
        };
        buffer.length = 0;
    }
    return [0, 1].map(generatorIndex => {
        const thisBuffer = buffers[generatorIndex];
        const otherBuffer = buffers[1 - generatorIndex];
        return (async function*() {
            for await (const data of inputGenerator) {
                yield* yieldBuffer(thisBuffer);
                otherBuffer.push(data);
                yield data;
            }
            yield* yieldBuffer(thisBuffer);
        })();
    });
}