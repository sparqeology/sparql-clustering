PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
PREFIX lsqv: <http://lsq.aksw.org/vocab#>
PREFIX dcterms: <http://purl.org/dc/terms/>

INSERT {
  GRAPH <http://lsq.aksw.org/datasets> {
    ?endpoint dcterms:isPartOf <http://sparql-clustering.org/services/bio2rdf.org>
  }
} 
WHERE {
  GRAPH <http://lsq.aksw.org/datasets> {
    ?dataset lsqv:endpoint ?endpoint.
    FILTER(CONTAINS(STR(?endpoint), 'bio2rdf.org'))
  }
}