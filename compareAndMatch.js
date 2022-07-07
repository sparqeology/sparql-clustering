import fs from "fs";

import { stringify } from 'csv-stringify';
import { parse } from 'csv-parse';
// import JisonLex from 'jison-lex';
import {Parser} from 'sparqljs';



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

const lexer = sparqlParser.lexer;
lexer.setInput('   SELECT * WHERE {?s ?p ?o}');
console.log(lexer.lex());
console.log(lexer.lex());


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