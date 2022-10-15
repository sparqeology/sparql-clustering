import {Writer, DataFactory, Parser} from 'n3';
import { prefixesToString } from './sparqlEncoding.js';

const defaultWriter = new Writer()

export function escapeLiteral(literalStr) {
  return defaultWriter._encodeLiteral(DataFactory.literal(literalStr))
}

export function escapeIriFragment(iriStr) {
  const completeIri = defaultWriter._encodeIriOrBlank(DataFactory.namedNode(iriStr))
  return completeIri.substring(1, completeIri.length - 2)
}

const ENVELOPE_BEGIN = '<a:> <a:> ';
const ENVELOPE_END = '.\n';

export function rebaseTerm(term, sourcePreamble, destinationPreamble = {}) {
  const parser = new Parser({baseIRI: sourcePreamble.base});
  const sourcePreambleStr = 'prefixes' in sourcePreamble ? prefixesToString(sourcePreamble.prefixes) : '';
  const quads = parser.parse(sourcePreambleStr + ENVELOPE_BEGIN + term + ENVELOPE_END);
  const writer = new Writer(destinationPreamble);
  const quadStr = writer.quadsToString(quads);
  return quadStr.substring(ENVELOPE_BEGIN.length, quadStr.length - ENVELOPE_END.length);
}
