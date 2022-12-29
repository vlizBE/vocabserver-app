import os
from string import Template

from rdflib import Graph, URIRef
import requests
from more_itertools import batched

from flask import request

from escape_helpers import sparql_escape, sparql_escape_uri
from helpers import generate_uuid, logger
from helpers import query as sparql_query
from helpers import update as sparql_update
from sudo_query import query_sudo, auth_update_sudo as update_sudo

from sparql_util import serialize_graph_to_sparql, sparql_construct_res_to_graph, load_file_to_db, drop_graph, diff_graphs

from job import run_job
from vocabulary import get_vocabulary
from dataset import get_dataset

from unification import unify_from_node_shape, get_property_paths, get_ununified_batch, delete_from_graph

# Maybe make these configurable
FILE_RESOURCE_BASE = 'http://example-resource.com/'
JOBS_GRAPH = "http://mu.semte.ch/graphs/public"
TEMP_GRAPH_BASE = 'http://example-resource.com/graph/'
VOCAB_GRAPH = "http://mu.semte.ch/graphs/public"
UNIFICATION_TARGET_GRAPH = "http://mu.semte.ch/graphs/public"
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")

CONT_UN_JOB_TYPE = "http://mu.semte.ch/vocabularies/ext/ContentUnificationJob"

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
    vocab_sources = query_sudo(get_vocabulary(vocab_uri, VOCAB_GRAPH))['results']['bindings']
    for vocab_source in vocab_sources:
        dataset_versions = query_sudo(get_dataset(vocab_source['sourceDataset']['value'], VOCAB_GRAPH))['results']['bindings']
        new_temp_named_graph = load_file_to_db(dataset_versions[0]['data_dump']['value'], VOCAB_GRAPH)
        if len(dataset_versions) > 1: # previous dumps exist
            old_temp_named_graph = load_file_to_db(dataset_versions[1]['data_dump']['value'], VOCAB_GRAPH)
            diff_subjects = diff_graphs(old_temp_named_graph, new_temp_named_graph)
            for diff_subjects_batch in batched(diff_subjects, 10):
                query_sudo(delete_from_graph(diff_subjects_batch, VOCAB_GRAPH))
            drop_graph(old_temp_named_graph)
        prop_paths_qs = get_property_paths(vocab_source['mappingShape']['value'], VOCAB_GRAPH)
        prop_paths_res = query_sudo(prop_paths_qs)
        for path_props in prop_paths_res['results']['bindings']:
            while True:
                get_batch_qs = get_ununified_batch(path_props['destClass']['value'],
                                                   path_props['destPath']['value'],
                                                   vocab_source['sourceDataset']['value'],
                                                   path_props['sourceClass']['value'],
                                                   path_props['sourcePathString']['value'], # !
                                                   new_temp_named_graph, VOCAB_GRAPH, 10)
                # We might want to dump intermediary unified content to file before committing to store
                batch_res = query_sudo(get_batch_qs)
                if not batch_res['results']['bindings']:
                    logger.info("Finished unification")
                    break
                else:
                    logger.info("Running unification batch")
                g = sparql_construct_res_to_graph(batch_res)
                for query_string in serialize_graph_to_sparql(g, VOCAB_GRAPH):
                    update_sudo(query_string)
        drop_graph(new_temp_named_graph)


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
