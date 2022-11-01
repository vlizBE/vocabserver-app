import os
from string import Template

from rdflib import Graph

from escape_helpers import sparql_escape, sparql_escape_uri
from helpers import generate_uuid, logger
from helpers import query as sparql_query
from helpers import update as sparql_update
from sudo_query import query_sudo, update_sudo

from file import construct_insert_file_query, construct_get_file_query, shared_uri_to_path
from job import run_job

from sparql_util import serialize_graph_to_sparql

# Maybe make these configurable
FILE_RESOURCE_BASE = 'http://example-resource.com/'
JOBS_GRAPH = "http://mu.semte.ch/graphs/public"
UNIFICATION_TARGET_GRAPH = "http://mu.semte.ch/graphs/public"
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")

def load_vocab_file(uri: str, graph: str = MU_APPLICATION_GRAPH):
    query_string = construct_get_file_query(uri, graph)
    file_result = query_sudo(query_string)['results']['bindings'][0]

    g = Graph()
    g.parse(shared_uri_to_path(file_result['physicalFile']['value']))

    return g

def get_job_uri(job_uuid: str, graph: str = MU_APPLICATION_GRAPH):
    query_template = Template('''
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT DISTINCT ?job_uri WHERE {
    GRAPH $graph {
        ?job_uri a ext:ContentUnificationJob ;
             mu:uuid $job_uuid .
    }
}
''')

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        job_uuid=sparql_escape(job_uuid),
    )
    query_res = sparql_query(query_string)
    return query_res['results']['bindings'][0]['job_uri']['value']

def run_vocab_unification(file_uri: str, src_file_graph: str, target_graph: str):
    # Note that for now "unification" means no more than "dump to graph"
    # We might want to dump intermediary "unified" content to file before comitting to store
    g = load_vocab_file(file_uri, src_file_graph)
    query_string = serialize_graph_to_sparql(g, target_graph)
    update_sudo(query_string)

@app.route('/<job_uuid>', methods=['POST'])
def run_vocab_unification_req(job_uuid: str):
    try:
        job_uri = get_job_uri(job_uuid)
    except Exception:
        logger.info(f"No job found by uuid ${job_uuid}")
    
    run_job(
        job_uri,
        JOBS_GRAPH,
        lambda sources: run_vocab_unification(sources[0], JOBS_GRAPH, UNIFICATION_TARGET_GRAPH),
        query_sudo,
        update_sudo
    )

    return ''
