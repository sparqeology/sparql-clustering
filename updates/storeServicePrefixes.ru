PREFIX lsqv: <http://lsq.aksw.org/vocab#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX sh: <http://www.w3.org/ns/shacl#>

INSERT {
  GRAPH <http://lsq.aksw.org/datasets> {
    ?endpoint sh:declare ?prefixes.
  }
} 
WHERE {
  GRAPH <http://lsq.aksw.org/datasets> {
    ?query lsqv:endpoint ?endpoint.
    ?endpoint dcterms:isPartOf* ?service.
  }
}