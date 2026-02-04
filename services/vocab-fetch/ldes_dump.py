import os
from string import Template
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string
from query_triplestore import query
from sparql_util import binding_results

TASKS_GRAPH = "http://mu.semte.ch/graphs/public"
VOID_DATASET_GRAPH = "http://mu.semte.ch/graphs/public"
VOCAB_DOWNLOAD_JOB = "http://lblod.data.gift/id/jobs/concept/JobOperation/vocab-download"
LDES_TYPE = "http://vocabsearch.data.gift/dataset-types/LDES"
STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled'

def query_outdated_dump_ldes_datasets():
    query_string = Template("""
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT DISTINCT ?dataset
WHERE
{
    {
        SELECT DISTINCT
            ?dataset
            ?dataset_graph
            (MAX(?dump_modification_date) AS ?latest_dump_modification_date)
        WHERE {
            GRAPH $void_dataset_graph {
                ?dataset
                    a void:Dataset ;
                    ext:datasetGraph ?dataset_graph ;
                    dct:type $ldes_type .
                ?vocab ext:sourceDataset ?dataset.
                OPTIONAL {
                    ?dataset void:dataDump / dct:created ?creation_date .
                }
            }
            # fallback in case no dump exists yet
            BIND(IF(BOUND(?creation_date), ?creation_date, "2000-01-01T00:00:00Z"^^xsd:dateTime ) AS ?dump_modification_date  ) .
        }
        GROUP BY ?dataset ?dataset_graph
        ORDER BY ?dataset
    }
    GRAPH ?dataset_graph {
        # hardcoded timestamp predicates here.
        # BODC ldes use dct:date, MarineRegions dct:modified
        ?elem ( dct:modified | dct:date ) ?element_modification_date .
        # work around invalid bodc xsd:dateTime's ("2024-01-08 18:29:18.0"^^xsd:dateTime for example)
        BIND(STRDT(REPLACE(STR(?element_modification_date), " ", "T"), xsd:dateTime) AS ?sanitized_element_modification_date)
    }
    FILTER (?sanitized_element_modification_date > ?latest_dump_modification_date)
}
    """).substitute(
        void_dataset_graph=sparql_escape_uri(VOID_DATASET_GRAPH),
        ldes_type=sparql_escape_uri(LDES_TYPE)
    )
    query_res = query(query_string)
    datasets = binding_results(query_res, "dataset")
    return datasets

def query_all_ldes_datasets():
    query_string = Template("""
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT DISTINCT ?dataset
WHERE {
    GRAPH $dataset_graph {
      ?dataset a void:Dataset ;
      dct:type $ldes_type .
      ?vocab ext:sourceDataset ?dataset.
    }
}
    """).substitute(
        ldes_type=sparql_escape_uri(LDES_TYPE),
        dataset_graph=sparql_escape_uri(VOID_DATASET_GRAPH)
    )
    query_res = query(query_string)
    datasets = binding_results(query_res, "dataset")
    return datasets