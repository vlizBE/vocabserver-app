import os
from string import Template
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")

def unify_from_node_shape(node_shape, source_dataset, metadata_graph, source_graph, target_graph):
    query_template = Template("""
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dct: <http://purl.org/dc/terms/>

INSERT {
    GRAPH $target_graph {
        ?s
            a ?destClass ;
            ?destPath ?sourceValue .
        ?s dct:source $source_dataset .
    }
}
WHERE {
    GRAPH $metadata_graph {
        $node_shape
            a sh:NodeShape ;
            sh:targetClass ?sourceClass ;
            sh:property ?propertyShape .
        ?propertyShape
            a sh:PropertyShape ;
            sh:path ?pathString .
        BIND(URI(?pathString) AS ?sourcePath)
        BIND(skos:prefLabel as ?destPath)
        BIND(skos:Concept as ?destClass)
    }
    GRAPH $source_graph {
        ?s
            a ?sourceClass ;
            ?sourcePath ?sourceValue .
    }
}""")
    query_string = query_template.substitute(
        target_graph=sparql_escape_uri(target_graph),
        source_dataset=sparql_escape_uri(source_dataset),
        metadata_graph=sparql_escape_uri(metadata_graph),
        source_graph=sparql_escape_uri(source_graph),
        node_shape=sparql_escape_uri(node_shape)
    )
    return query_string

