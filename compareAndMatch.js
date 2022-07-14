import fs from "fs";

import { stringify } from 'csv-stringify';
import { parse } from 'csv-parse';
// import JisonLex from 'jison-lex';
import {Parser} from 'sparqljs';

import lineColumn from 'line-column';

const queriesInputStream = fs.createReadStream('./output/queries.csv');

const sparqlParser = new Parser();
var parsedQuery = sparqlParser.parse(
  'PREFIX foaf: <http://xmlns.com/foaf/0.1/> ' +
  'SELECT * { ?mickey foaf:name "Mickey Mouse"@en; foaf:knows ?other. }');

// const grammar = fs.readFileSync('./parser/parser.jison', 'utf8');

// // generate source
// // const lexerSource = JisonLex.generate(grammar);

// // create a parser in memory
// var lexer = new JisonLex(grammar);

// // generate source
// var lexerSource = JisonLex.generate(grammar);
// console.log(lexerSource);

const inputStr = `


SELECT * WHERE {
  
  ?s ?p
  ?o
}


`;

const lcFinder = lineColumn(inputStr);

const lexer = sparqlParser.lexer;
lexer.setInput(inputStr);
console.log(lexer.lex());
// console.log(lexer.next());
console.log(lexer.showPosition());
console.log(lexer.match);
console.log(lexer.yylineno);
const loc = lexer.yylloc;
console.log(loc);
console.log(lcFinder.toIndex(loc.first_line, loc.first_column + 1));
console.log(lcFinder.toIndex(loc.last_line, loc.last_column + 1));
console.log(lexer.yytext);
console.log(lexer.yyleng);

console.log(lexer.lex());
console.log(lexer.showPosition());
console.log(lexer.match);
console.log(lexer.yylineno);
const loc2 = lexer.yylloc;
console.log(loc2);
console.log(lcFinder.toIndex(loc2.first_line, loc2.first_column + 1));
console.log(lcFinder.toIndex(loc2.last_line, loc2.last_column + 1));
console.log(lexer.yytext);
console.log(lexer.yyleng);

// console.log(sparqlParser.terminals_);
console.log(sparqlParser.yy);

console.log(sparqlParser.trace())

// const queriesOutputStream = fs.createWriteStream('./output/queries.csv');

const parser = parse({
    delimiter: ','
});

parser.on('readable', function(){
    let record;
    while ((record = parser.read()) !== null) {
        console.log(record);
    }
  });

parser.on('error', function(err){
    console.error(err.message);
});
  
// queriesInputStream.pipe(parser);