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

def get_property_paths(node_shape, metadata_graph):
    query_template = Template("""
PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?sourceClass ?sourcePathString ?destClass ?destPath
WHERE {
    GRAPH $metadata_graph {
        $node_shape
            a sh:NodeShape ;
            sh:targetClass ?sourceClass ;
            sh:property ?propertyShape .
        ?propertyShape
            a sh:PropertyShape ;
            sh:path ?sourcePathString .
        BIND(skos:prefLabel as ?destPath)
        BIND(skos:Concept as ?destClass)
    }
}
""")
    query_string = query_template.substitute(
        metadata_graph=sparql_escape_uri(metadata_graph),
        node_shape=sparql_escape_uri(node_shape),
    )
    return query_string

def get_ununified_batch(dest_class,
                        dest_predicate,
                        source_dataset,
                        source_class,
                        source_path_string,
                        source_graph,
                        target_graph,
                        batch_size):
    query_template = Template("""
PREFIX dct: <http://purl.org/dc/terms/>

CONSTRUCT {
    ?s a $dest_class .
    ?s $dest_predicate ?sourceValue .
    ?s dct:source $source_dataset .
}
WHERE {
    FILTER NOT EXISTS {
        GRAPH $target_graph {
            ?s
                a $dest_class ;
                $dest_predicate ?sourceValue .
        }
    }
    GRAPH $source_graph {
        ?s
            a $source_class ;
            $source_path_string ?sourceValue .
    }
}
LIMIT $batch_size
""")
    query_string = query_template.substitute(
        dest_class=sparql_escape_uri(dest_class),
        dest_predicate=sparql_escape_uri(dest_predicate),
        source_dataset=sparql_escape_uri(source_dataset),
        source_class=sparql_escape_uri(source_class),
        source_path_string=source_path_string, # !
        source_graph=sparql_escape_uri(source_graph),
        target_graph=sparql_escape_uri(target_graph),
        batch_size=batch_size
    )
    return query_string

def delete_from_graph(subjects, graph):
    query_template = Template("""
WITH $graph
DELETE {
    ?s ?p ?o .
}
WHERE {
    ?s ?p ?o .
    VALUES ?s {
        $subjects
    }
}
""")
    query_string = query_template.substitute(
        subjects="\n        ".join([sparql_escape_uri(s) for s in subjects]),
        graph=sparql_escape_uri(graph),
    )
    return query_string


