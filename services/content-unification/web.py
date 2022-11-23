import os
from string import Template

from rdflib import Graph, URIRef

from flask import request

from escape_helpers import sparql_escape, sparql_escape_uri
from helpers import generate_uuid, logger
from helpers import query as sparql_query
from helpers import update as sparql_update
from sudo_query import query_sudo, update_sudo

from sparql_util import serialize_graph_to_sparql

from job import run_job
from file import construct_insert_file_query, construct_get_file_query, shared_uri_to_path
from vocabulary import get_vocabulary
from dataset import get_dataset

from unification import unify_from_node_shape

# Maybe make these configurable
FILE_RESOURCE_BASE = 'http://example-resource.com/'
JOBS_GRAPH = "http://mu.semte.ch/graphs/public"
TEMP_GRAPH_BASE = 'http://example-resource.com/graph/'
VOCAB_GRAPH = "http://mu.semte.ch/graphs/public"
UNIFICATION_TARGET_GRAPH = "http://mu.semte.ch/graphs/public"
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")

CONT_UN_JOB_TYPE = "http://mu.semte.ch/vocabularies/ext/ContentUnificationJob"

def load_vocab_file(uri: str, graph: str = MU_APPLICATION_GRAPH):
    query_string = construct_get_file_query(uri, graph)
    file_result = query_sudo(query_string)['results']['bindings'][0]

    g = Graph()
    g.parse(shared_uri_to_path(file_result['physicalFile']['value']))

    return g

def get_job_uri(job_uuid: str, job_type: str, graph: str = MU_APPLICATION_GRAPH):
    query_template = Template('''
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT DISTINCT ?job_uri WHERE {
    GRAPH $graph {
        ?job_uri a $job_type ;
             mu:uuid $job_uuid .
    }
}
''')

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        job_type=sparql_escape_uri(job_type),
        job_uuid=sparql_escape(job_uuid),
    )
    query_res = sparql_query(query_string)
    return query_res['results']['bindings'][0]['job_uri']['value']

def run_vocab_unification(vocab_uri):
    vocab = query_sudo(get_vocabulary(vocab_uri, VOCAB_GRAPH))['results']['bindings'][0]
    dataset = query_sudo(get_dataset(vocab['sourceDataset']['value'], VOCAB_GRAPH))['results']['bindings'][0]
    g = load_vocab_file(dataset['data_dump']['value'], VOCAB_GRAPH)
    temp_named_graph = TEMP_GRAPH_BASE + generate_uuid()
    for query_string in serialize_graph_to_sparql(g, temp_named_graph):
        update_sudo(query_string)
    # We might want to dump intermediary unified content to file before committing to store
    unification_query_string = unify_from_node_shape(vocab['mappingShape']['value'], VOCAB_GRAPH, temp_named_graph, VOCAB_GRAPH)
    update_sudo(unification_query_string)

@app.route('/<job_uuid>', methods=['POST'])
def run_vocab_unification_req(job_uuid: str):
    try:
        job_uri = get_job_uri(job_uuid, CONT_UN_JOB_TYPE)
    except Exception:
        logger.info(f"No job found by uuid {job_uuid}")
        return
    
    run_job(
        job_uri,
        JOBS_GRAPH,
        lambda sources: run_vocab_unification(sources[0]),
        query_sudo,
        update_sudo
    )

    return ''

@app.route('/delta', methods=['POST'])
def process_delta():
    inserts = request.json[0]['inserts']
    job_triple = next(filter(
        lambda x: x['predicate']['value'] == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
        inserts
    ))
    job_uri = job_triple['subject']['value']
    
    run_job(
        job_uri,
        JOBS_GRAPH,
        lambda sources: run_vocab_unification(sources[0]),
        query_sudo,
        update_sudo
    )
    return '', 200
