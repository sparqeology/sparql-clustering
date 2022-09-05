// export default class LookaheadGenerator {
//     constructor(baseGenerator) {
//         this.baseGenerator = baseGenerator;
//         const {value, done} = await baseGenerator.next();
//         this.next = value;
//         this.hasNext = !done;
//         this.generator = function*() {
//             for await (const data of this.baseGenerator) {
//                 yield this.next;
//                 this.next = data;
//             }
//             this.next = undefined;
//             this.hasNext = false;
//         }();
//     }
// }

export default async function lookaheadGenerator(baseGenerator) {
    // console.log(baseGenerator);
    const baseIterator = ('next' in baseGenerator) ? baseGenerator : baseGenerator[Symbol.asyncIterator]();
    // console.log(baseIterator);
    var lookAhead;
    lookAhead = (await baseIterator.next()).value;

    return {
        baseGenerator,
        get lookAhead() {return lookAhead},
        get hasNext() {return lookAhead !== undefined;},
        [Symbol.asyncIterator]: async function*() {
            // console.log('creating generator...');
            while (true) {
                const {value, done} = await baseIterator.next();
                if (done) break;
                // console.log('next record:')
                // console.log(value);
                // console.log('record:')
                // console.log(lookAhead);
                const result = lookAhead;
                lookAhead = value;
                yield result;
            }
            // for await (const data of baseIterator) {
            //     console.log('next record:')
            //     console.log(data);
            //     console.log('record:')
            //     console.log(lookAhead);
            //     const result = lookAhead;
            //     lookAhead = data;
            //     yield result;
            // }
            if (lookAhead !== undefined) {
                const result = lookAhead;
                lookAhead = undefined;
                yield result;
            }
        }
        // generator: async function*() {
        //     // console.log('creating generator...');
        //     for await (const data of baseIterator) {
        //         console.log('next record:')
        //         console.log(data);
        //         console.log('record:')
        //         console.log(next);
        //         const result = next;
        //         next = data;
        //         yield result;
        //     }
        //     next = undefined;
        // }()
        // async * generatorFunction() {
        //     for await (const data of baseGenerator) {
        //         yield next;
        //         next = data;
        //     }
        //     next = undefined;
        //     hasNext = false;
        // }(),
        // generator: 
        // generator: async function*() {
        //     for await (const data of this.baseGenerator) {
        //         yield this.next;
        //         this.next = data;
        //     }
        //     this.next = undefined;
        //     this.hasNext = false;
        // }()
    }
    // this.baseGenerator = baseGenerator;
    // this.next = value;
    // this.hasNext = !done;
    // this.generator = function*() {
    //     for await (const data of this.baseGenerator) {
    //         yield this.next;
    //         this.next = data;
    //     }
    //     this.next = undefined;
    //     this.hasNext = false;
    // }();
}