export class LookaheadLexer {
    lookForNextToken() {
        while (!(this.nextSymbol = this.baseLexer.next())) {
            this.noTokenMatch += this.baseLexer.match;
        }
        this.nextMatch = this.baseLexer.match;
    }

    constructor(baseLexer) {
        this.baseLexer = baseLexer;
        this.EOF = baseLexer.EOF;
        this.noTokenMatch = '';
        this.lookForNextToken();
    }

    next() {
        if (this.noTokenMatch.length > 0) {
            this.match = this.noTokenMatch;
            this.noTokenMatch = '';
            return false;
        } else {
            const currSymbol = this.nextSymbol;
            this.match = this.nextMatch;
            if (currSymbol !== this.baseLexer.EOF) {
                this.lookForNextToken();
            }
            return currSymbol;
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
