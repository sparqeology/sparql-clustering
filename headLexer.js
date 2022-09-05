export class HeadLexer {
    constructor(baseLexer, numOfTokens) {
        this.baseLexer = baseLexer;
        this.EOF = baseLexer.EOF;
        this.numOfRemainingTokens = numOfTokens;
    }

    next() {
        if (this.numOfRemainingTokens > 0) {
            const symbol = this.baseLexer.next();
            this.numOfRemainingTokens--;
            this.match = this.baseLexer.match;
            return symbol;
        } else {
            return this.EOF;
        }
    }

    lex() {
        const r = this.next();
        if (r) {
            return r;
        } else {
            return this.lex();
        }
    }
}
