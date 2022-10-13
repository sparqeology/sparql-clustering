import {Writer, DataFactory, Parser} from 'n3';

export function escapeLiteral(literalStr) {
  return n3Writer._encodeLiteral(DataFactory.literal(literalStr))
}

export function parsePreamble(preambleStr) {
  const parser = new Parser();
  parser.parse(preambleStr);
  return {
    baseIRI: parser._base,
    prefixes: parser._prefixes
  };
}

export function prefixesToString(prefixes) {
  const writer = new Writer({prefixes});
  let preambleStr;
  writer.end((error, result) => {
    if (error) {
      throw error;
    }
    preambleStr = result;
  });
  return preambleStr;
}

export function preambleToString(preamble) {
  return ('baseIRI' in preamble ? `@base <${preamble.baseIRI}>.\n` : '') +
      ('prefixes' in preamble ? prefixesToString(preamble.prefixes) : '');
}

const DUMMY_PREDICATE_OBJECT = ' <a:> <a:> .\n';

export function rebaseTerm(term, sourcePreamble, destinationPreamble = {}) {
  const parser = new Parser({baseIRI: sourcePreamble.baseIRI});
  const sourcePreambleStr = 'prefixes' in sourcePreamble ? prefixesToString(sourcePreamble.prefixes) : '';
  const quads = parser.parse(sourcePreambleStr + term + DUMMY_PREDICATE_OBJECT);
  const writer = new Writer(destinationPreamble);
  const quadStr = writer.quadsToString(quads);
  return quadStr.substring(0, quadStr.length - DUMMY_PREDICATE_OBJECT.length);
}
