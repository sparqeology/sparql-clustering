import {Parser} from 'sparqljs';

import { LookaheadLexer } from './lookaheadLexer.js';

export default class Tokenizer {
    constructor(queryStr, options = {}) {
        const sparqlParser = new Parser();
        this.QUERY_SYMBOLS = [
            'SELECT',
            'CONSTRUCT',
            'ASK',
            'DESCRIBE'
        ].map(terminal => sparqlParser.symbols_[terminal]);
        this.GENERALIZABLE_SYMBOLS = [
            'PNAME_LN',
            'IRIREF',
            'INTEGER',
            'DECIMAL',
            'DOUBLE',
            'INTEGER_POSITIVE',
            'DECIMAL_POSITIVE',
            'DOUBLE_POSITIVE',
            'INTEGER_NEGATIVE',
            'DECIMAL_NEGATIVE',
            'DOUBLE_NEGATIVE',
            'STRING_LITERAL1',
            'STRING_LITERAL2',
            'STRING_LITERAL_LONG1',
            'STRING_LITERAL_LONG2'
        ].map(terminal => sparqlParser.symbols_[terminal]);
        this.STRING_LITERAL_SYMBOLS = [
            'STRING_LITERAL1',
            'STRING_LITERAL2',
            'STRING_LITERAL_LONG1',
            'STRING_LITERAL_LONG2'
        ].map(terminal => sparqlParser.symbols_[terminal]);
        this.STRING_LITERAL_FOLLOWUP_SYMBOLS = [
            '^^',
            'LANGTAG'
        ].map(terminal => sparqlParser.symbols_[terminal]);
        this.DATATYPE_SYMBOL = sparqlParser.symbols_['^^'];
        this.LANGTAG_SYMBOL = sparqlParser.symbols_['LANGTAG'];
        this.EOF = sparqlParser.lexer.EOF;
        this.options = options;
        const baseLexer = sparqlParser.lexer;
        baseLexer.setInput(queryStr);
        this.lexer = new LookaheadLexer(baseLexer);
        this.afterPreamble = false;
        this.preamble = '';
    }

    next() {
        const symbol = this.lexer.next();
        if (symbol === this.EOF) {
            return null;
        }
        if (!this.afterPreamble) {
            if (this.QUERY_SYMBOLS.includes(symbol)) {
                this.afterPreamble = true;
                return {
                    match: this.lexer.match,
                    parameterizable: false
                };
            } else {
                // if (this.options.excludePreamble) {
                    this.preamble += this.lexer.match;
                // }
                return {
                    match: this.options.excludePreamble ? '' : this.lexer.match,
                    parameterizable: false
                }
            }
        } else if (this.GENERALIZABLE_SYMBOLS.includes(symbol)) {
            if (this.options.sparqlParameters &&
                this.STRING_LITERAL_SYMBOLS.includes(symbol) &&
                this.STRING_LITERAL_FOLLOWUP_SYMBOLS.includes(this.lexer.nextSymbol)) {
                const str = this.lexer.match;
                const nextSymbol = this.lexer.lex();
                if (nextSymbol === this.DATATYPE_SYMBOL) {
                    this.lexer.lex();
                }
                return {
                    match: str + this.lexer.match,
                    parameterizable: true
                }
            } else {
                return {
                    match: this.lexer.match,
                    parameterizable: true
                }
            }
        } else {
            return {
                match: this.lexer.match,
                parameterizable: false
            }
        }
    }
}

