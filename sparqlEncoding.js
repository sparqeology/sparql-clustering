import SparqlJs from "sparqljs"

const { Generator, Parser } = SparqlJs;

const generator = new Generator({allPrefixes: true});
const parser = Parser();

export function parsePreamble(preambleStr) {
  return parser.parse(preambleStr)
}

export function prefixesToString(prefixes) {
  return generator.stringify({prefixes});
}

export function preambleToString(preamble) {
    return generator.stringify(preamble);
}
