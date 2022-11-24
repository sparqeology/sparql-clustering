PREFIX lsqv: <http://lsq.aksw.org/vocab#>

DELETE {
    GRAPH ?dataset {
        ?query lsqv:hasSpin ?spinRoot.
        ?spinS ?spinP ?spinO
    }
}
WHERE {
    GRAPH ?dataset {
        ?query lsqv:hasSpin ?spinRoot.
        ?spinRoot !<>* ?spinS.
        ?spinS ?spinP ?spinO.
    }
}
