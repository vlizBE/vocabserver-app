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

from sparql_util import serialize_graph_to_sparql, sparql_construct_res_to_graph, load_file_to_db, drop_graph, \
    diff_graphs, copy_graph_to_temp

from task import run_task, find_actionable_task
from vocabulary import get_vocabulary
from dataset import get_dataset

from unification import unify_from_node_shape, get_property_paths, get_ununified_batch, delete_from_graph
from remove_vocab import remove_files, select_vocab_concepts_batch, remove_vocab_data_dumps, remove_vocab_source_datasets, remove_vocab_meta, remove_vocab_vocab_fetch_jobs, remove_vocab_vocab_unification_jobs, remove_vocab_partitions, remove_vocab_mapping_shape

# Maybe make these configurable
FILE_RESOURCE_BASE = 'http://example-resource.com/'
TASKS_GRAPH = "http://mu.semte.ch/graphs/public"
TEMP_GRAPH_BASE = 'http://example-resource.com/graph/'
VOCAB_GRAPH = "http://mu.semte.ch/graphs/public"
UNIFICATION_TARGET_GRAPH = "http://mu.semte.ch/graphs/public"
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
TEMP_GRAPH_BASE = 'http://example-resource.com/graph/'

CONT_UN_OPERATION = "http://mu.semte.ch/vocabularies/ext/ContentUnificationJob"

def run_vocab_unification(vocab_uri):
    vocab_sources = query_sudo(get_vocabulary(vocab_uri, VOCAB_GRAPH))['results']['bindings']
    temp_named_graph = TEMP_GRAPH_BASE + generate_uuid()
    for vocab_source in vocab_sources:
        dataset_versions = query_sudo(get_dataset(vocab_source['sourceDataset']['value'], VOCAB_GRAPH))['results']['bindings']
        print(dataset_versions)
        # TODO: LDES check
        if 'data_dump' in dataset_versions[0].keys():
            new_temp_named_graph = load_file_to_db(dataset_versions[0]['data_dump']['value'], VOCAB_GRAPH, temp_named_graph)
            if len(dataset_versions) > 1: # previous dumps exist
                old_temp_named_graph = load_file_to_db(dataset_versions[1]['data_dump']['value'], VOCAB_GRAPH)
                diff_subjects = diff_graphs(old_temp_named_graph, new_temp_named_graph)
                for diff_subjects_batch in batched(diff_subjects, 10):
                    query_sudo(delete_from_graph(diff_subjects_batch, VOCAB_GRAPH))
                drop_graph(old_temp_named_graph)
        else:
            copy_graph_to_temp(dataset_versions[0]['dataset_graph']['value'], temp_named_graph)
    prop_paths_qs = get_property_paths(vocab_sources[0]['mappingShape']['value'], VOCAB_GRAPH)
    prop_paths_res = query_sudo(prop_paths_qs)

    for path_props in prop_paths_res['results']['bindings']:
        while True:
            get_batch_qs = get_ununified_batch(path_props['destClass']['value'],
                                               path_props['destPath']['value'],
                                               [vocab_source['sourceDataset']['value'] for vocab_source in vocab_sources],
                                               path_props['sourceClass']['value'],
                                               path_props['sourcePathString']['value'], # !
                                               temp_named_graph, VOCAB_GRAPH, 10)
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

    drop_graph(temp_named_graph)

@app.route('/delete-vocabulary/<vocab_uuid>', methods=('DELETE',))
def delete_vocabulary(vocab_uuid: str):
    remove_files(vocab_uuid, VOCAB_GRAPH)
    update_sudo(remove_vocab_data_dumps(vocab_uuid, VOCAB_GRAPH))

    # concepts may return too many results in mu-auth internal construct. Batch it here.
    while True:
        batch = query_sudo(select_vocab_concepts_batch(vocab_uuid, VOCAB_GRAPH))
        bindings = batch['results']['bindings']
        if bindings:
            g = sparql_construct_res_to_graph(batch)
            for query_string in serialize_graph_to_sparql(g, VOCAB_GRAPH, "DELETE"):
                update_sudo(query_string)
        else:
            break
    update_sudo(remove_vocab_vocab_fetch_jobs(vocab_uuid, VOCAB_GRAPH))

    update_sudo(remove_vocab_vocab_unification_jobs(vocab_uuid, VOCAB_GRAPH))
    update_sudo(remove_vocab_partitions(vocab_uuid, VOCAB_GRAPH))
    update_sudo(remove_vocab_source_datasets(vocab_uuid, VOCAB_GRAPH))
    update_sudo(remove_vocab_mapping_shape(vocab_uuid, VOCAB_GRAPH))
    update_sudo(remove_vocab_meta(vocab_uuid, VOCAB_GRAPH))
    return '', 200


@app.route('/delta', methods=['POST'])
def process_delta():
    inserts = request.json[0]['inserts']
    try:
        task_triple = next(filter(
            lambda x: x['predicate']['value'] == 'http://www.w3.org/ns/adms#status' and x['object']['value'] == 'http://redpencil.data.gift/id/concept/JobStatus/scheduled',
            inserts
        ))
    except StopIteration:
        return "Can't do anything with this delta. Skipping.", 500
    task_uri = task_triple['subject']['value']

    task_q = find_actionable_task(task_uri, TASKS_GRAPH)
    task_res = query_sudo(task_q)
    if task_res["results"]["bindings"]:
        task_operation = [binding["operation"]['value'] for binding in task_res["results"]["bindings"] if "operation" in binding][0]
    else:
        return "Don't know how to handle task without operation type", 500

    if task_operation == CONT_UN_OPERATION:
        logger.debug(f"Running task {task_uri}, operation {task_operation}")
        run_task(
            task_uri,
            TASKS_GRAPH,
            lambda sources: [run_vocab_unification(sources[0])],
            query_sudo,
            update_sudo
        )
        return '', 200
    else:
        return "Don't know how to handle task with operation type " + task_operation, 500
