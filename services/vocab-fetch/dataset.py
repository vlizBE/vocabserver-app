import os
from string import Template
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")

def get_dataset(dataset, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT DISTINCT ?format ?download_url ?data_dump ?type ?dataset_graph ?vocab ?creation_date
WHERE {
    GRAPH $graph {
        $dataset
            a void:Dataset ;
            foaf:page ?download_url ;
            dc:type ?type .
        ?vocab ext:sourceDataset $dataset.
        OPTIONAL { $dataset void:feature ?format . }
        OPTIONAL {
            $dataset void:dataDump ?data_dump .
            ?data_dump dct:created ?creation_date .
        }
        OPTIONAL { $dataset ext:datasetGraph ?dataset_graph . }
    }
}
ORDER BY DESC(?creation_date)""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        dataset=sparql_escape_uri(dataset),
    )
    return query_string

def get_dataset_by_uuid(uuid, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX void: <http://rdfs.org/ns/void#>

SELECT ?dataset
WHERE {
    GRAPH $graph {
        ?dataset a void:Dataset ;
            mu:uuid $uuid .
    }
}""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        uuid=sparql_escape_string(uuid),
    )
    return query_string

def update_dataset_download(dataset, new_file, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX void: <http://rdfs.org/ns/void#>

WITH $graph
INSERT {
    ?dataset void:dataDump $new_file .
}
WHERE {
    BIND($dataset as ?dataset)
    ?dataset a void:Dataset .
    OPTIONAL { ?dataset void:dataDump ?old_file }
}""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        dataset=sparql_escape_uri(dataset),
        new_file=sparql_escape_uri(new_file),
    )
    return query_string

