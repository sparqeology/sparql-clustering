PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
PREFIX lsqv: <http://lsq.aksw.org/vocab#>

INSERT {
  GRAPH <http://lsq.aksw.org/datasets> {
    ?dataset lsqv:endpoint ?endpoint
  }
} 
WHERE {
  GRAPH ?dataset {
    ?query lsqv:endpoint ?endpoint
  }
  GRAPH <http://lsq.aksw.org/datasets> {
    ?dataset a sd:NamedGraph
  }
}