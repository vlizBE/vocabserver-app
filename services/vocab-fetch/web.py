import os
from datetime import datetime
from string import Template
import threading

import requests
from escape_helpers import sparql_escape, sparql_escape_uri
from flask import request
from helpers import generate_uuid, logger
from helpers import query, update
from sudo_query import query_sudo, update_sudo

from rdflib import Graph, URIRef, Literal
from void import generateVoID, deleteVoID

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from file import file_to_shared_uri, shared_uri_to_path
from file import construct_get_file_query, construct_insert_file_query
from task import find_actionable_task_of_type, run_task, find_actionable_task, create_download_task
from dataset import get_dataset, update_dataset_download, get_dataset_by_uuid
from ldes_dump import query_outdated_dump_ldes_datasets
from sparql_util import binding_results, serialize_graph_to_sparql, graph_to_file
from format_to_mime import FORMAT_TO_MIME_EXT

# Maybe make these configurable
TASKS_GRAPH = "http://mu.semte.ch/graphs/public"
FILES_GRAPH = "http://mu.semte.ch/graphs/public"
VOID_DATASET_GRAPH = "http://mu.semte.ch/graphs/public"

FILE_RESOURCE_BASE = "http://example-resource.com/"
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
VOID_DATASET_RESOURCE_BASE = "http://example-resource.com/void-dataset/"

VOCAB_DOWNLOAD_JOB = "http://lblod.data.gift/id/jobs/concept/JobOperation/vocab-download"
LDES_TYPE = "http://vocabsearch.data.gift/dataset-types/LDES"

UPDATE_DATASET_DUMP_CRON_PATTERN = os.environ.get("UPDATE_DATASET_DUMP_CRON_PATTERN")


def load_vocab_file(uri: str, graph: str = MU_APPLICATION_GRAPH):
    query_string = construct_get_file_query(uri, graph)
    file_result = query_sudo(query_string)["results"]["bindings"][0]

    g = Graph()
    g.parse(shared_uri_to_path(file_result["physicalFile"]["value"]))

    return g


def download_vocab_file(url: str, format: str, graph: str = MU_APPLICATION_GRAPH):
    mime_type, file_extension = FORMAT_TO_MIME_EXT[format]
    accept_string = ", ".join(
        value[0] + (";q=1.0" if key == format else ";q=0.1")
        for key, value in FORMAT_TO_MIME_EXT.items()
    )
    upload_resource_uuid = generate_uuid()
    upload_resource_uri = f"{FILE_RESOURCE_BASE}{upload_resource_uuid}"
    file_resource_uuid = generate_uuid()
    file_resource_name = f"{file_resource_uuid}.{file_extension}"

    file_resource_uri = file_to_shared_uri(file_resource_name)

    headers = {"Accept": accept_string}

    with requests.get(url, headers=headers, stream=True) as res:
        if res.url != url:
            logger.info("You've been redirected. Probably want to replace url in db.")

        assert res.ok

        # TODO: better handling + negociating
        logger.info(f'Content-Type: {res.headers["Content-Type"]}')
        logger.info(f"MIME-Type: {mime_type}")

        with open(shared_uri_to_path(file_resource_uri), "wb") as f:
            for chunk in res.iter_content(chunk_size=None):
                f.write(chunk)

            f.seek(0, 2)
            file_size = f.tell()

    file = {
        "uri": upload_resource_uri,
        "uuid": upload_resource_uuid,
        "name": file_resource_name,
        "mimetype": "text/plain",
        "created": datetime.now(),
        "size": file_size,
        "extension": file_extension,
    }
    physical_file = {
        "uri": file_resource_uri,
        "uuid": file_resource_uuid,
        "name": file_resource_name,
    }

    query_string = construct_insert_file_query(file, physical_file, graph)

    # TODO Check query result before writing file to disk
    update_sudo(query_string)

    return upload_resource_uri


def escape(binding):
    if binding["type"] == "uri":
        return URIRef(binding["value"])
    elif binding["type"] == "typed-literal":
        return Literal(binding["value"], datatype=binding["datatype"])
    else:
        return Literal(binding["value"])


def load_vocab_graph(graph: str):
    query_template = Template("""
SELECT ?s ?p ?o WHERE {
    GRAPH $graph {
        ?s ?p ?o .
    }
}
""")
    query_string = query_template.substitute(graph=sparql_escape_uri(graph))
    results = query_sudo(query_string)["results"]["bindings"]
    g = Graph()
    for triple in results:
        g.add(tuple(map(escape, (triple["s"], triple["p"], triple["o"]))))
    return g


def get_task_uri(task_uuid: str, graph: str = MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT DISTINCT ?task_uri WHERE {
    GRAPH $graph {
        ?task_uri task:operation <http://mu.semte.ch/vocabularies/ext/dataset-download-task>;
             mu:uuid $task_uuid .
    }
}
""")

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        task_uuid=sparql_escape(task_uuid),
    )
    query_res = query(query_string)
    return query_res["results"]["bindings"][0]["task_uri"]["value"]

def redownload_dataset(dataset_uri):
    dataset_result = query_sudo(get_dataset(dataset_uri, VOID_DATASET_GRAPH))['results']['bindings'][0]
    _type = dataset_result['type']['value']
    if _type == LDES_TYPE:
        graph = dataset_result['dataset_graph']['value']
        file_uri = graph_to_file(graph, FILES_GRAPH)
    else:
        download_link = dataset_result['download_url']['value']
        file_format = dataset_result['format']['value']
        file_uri = download_vocab_file(download_link, file_format, FILES_GRAPH)
    update_sudo(update_dataset_download(dataset_uri, file_uri, VOID_DATASET_GRAPH))
    return dataset_uri


@app.route("/dataset-download-task/<task_uuid>/run", methods=["POST"])
def run_dataset_download_route(task_uuid: str):
    try:
        task_uri = get_task_uri(task_uuid)
    except Exception:
        logger.info(f"No job found by uuid ${task_uuid}")
        return

    run_task(
        task_uri,
        TASKS_GRAPH,
        lambda sources: [redownload_dataset(sources[0])],
        query_sudo,
        update_sudo,
    )

    return ""



def generate_dataset_structural_metadata(dataset_uri, should_return_vocab):
    dataset_res = query_sudo(get_dataset(dataset_uri, VOID_DATASET_GRAPH))["results"][
        "bindings"
    ][0]
    # We update dataset metadata by purging old and re-loadind new data (instead of applying the diff)
    # This has some unexpected consequences, since ldes consumer-manager triggers on addition and
    # removal of void:Datasets. Since our actual intention isn't to remove the whole object (including type), but
    # rather to update more detailed properties, we exclude the `rdf:type` predicate from the removal process.
    deleteVoID(dataset_uri, VOID_DATASET_GRAPH)
    generateVoID(dataset_res["dataset_graph"]["value"], dataset_uri, VOID_DATASET_GRAPH)
    if should_return_vocab:
      return dataset_res["vocab"]["value"]
    else:
      return dataset_uri    


VOCAB_DOWNLOAD_OPERATION = "http://mu.semte.ch/vocabularies/ext/VocabDownloadJob"
METADATA_EXTRACTION_OPERATION = (
    "http://mu.semte.ch/vocabularies/ext/MetadataExtractionJob"
)

running_tasks_lock = threading.Lock()


def run_tasks():
    # run this function only once at a time to avoid overloading the service
    acquired = running_tasks_lock.acquire(blocking=False)

    if not acquired:
        logger.debug("Already running `run_tasks`")
        return

    try:
        while True:
            task_q = find_actionable_task_of_type(
                [VOCAB_DOWNLOAD_OPERATION, METADATA_EXTRACTION_OPERATION], TASKS_GRAPH
            )
            task_res = query_sudo(task_q)
            if task_res["results"]["bindings"]:
                (task_uri, task_operation, job_operation) = binding_results(
                    task_res, ("uri", "operation", "job_operation")
                )[0]
            else:
                logger.debug("No more tasks found")
                return
            try:
                if task_operation == VOCAB_DOWNLOAD_OPERATION:
                    logger.debug(f"Running task {task_uri}, operation {task_operation}")
                    run_task(
                        task_uri,
                        TASKS_GRAPH,
                        lambda sources: [redownload_dataset(sources[0])],
                        query_sudo,
                        update_sudo,
                    )

                elif task_operation == METADATA_EXTRACTION_OPERATION:
                    logger.debug(f"Running task {task_uri}, operation {task_operation}")
                    # this task is part of a job that will run unification afterwards
                    # which needs vocab uri as the output of previous job
                    # Maybe better to create an extra task "metadata_extraction_before_unification"
                    # which can be used in the VOCAB_DOWNLOAD job
                    should_return_vocab = (job_operation == VOCAB_DOWNLOAD_JOB)
                    run_task(
                        task_uri,
                        TASKS_GRAPH,
                        lambda sources: [
                            generate_dataset_structural_metadata(sources[0], should_return_vocab)
                        ],
                        query_sudo,
                        update_sudo,
                    )
            except:
                logger.warn(
                    f"Problem while running task {task_uri}, operation {task_operation}"
                )
    finally:
        running_tasks_lock.release()

def update_ldes_dataset_dump():
    logger.info("querying for LDES datasets with updates (requiring an update of their file-dump)")
    datasets = query_outdated_dump_ldes_datasets()
    if datasets:
        logger.info("LDES datasets needing dump update: " + str(datasets))
    for dataset in datasets:
        logger.info(f"Creating dump task for dataset {dataset}")
        qs = create_download_task(dataset, TASKS_GRAPH)
        update_sudo(qs)



if UPDATE_DATASET_DUMP_CRON_PATTERN:
    # work around running the cron job twice in flask dev mode
    # context: https://stackoverflow.com/questions/25504149/why-does-running-the-flask-dev-server-run-itself-twice
    from werkzeug.serving import is_running_from_reloader
    if is_running_from_reloader():
        scheduler = BackgroundScheduler()
        logger.info(f"Configuring dataset dump update cron interval {UPDATE_DATASET_DUMP_CRON_PATTERN}")
        scheduler.add_job(update_ldes_dataset_dump, CronTrigger.from_crontab(UPDATE_DATASET_DUMP_CRON_PATTERN))
        scheduler.start()

@app.route("/delta", methods=["POST"])
def process_delta():
    inserts = request.json[0]["inserts"]
    task_triples = [
        t
        for t in inserts
        if t["predicate"]["value"] == "http://www.w3.org/ns/adms#status"
        and t["object"]["value"]
        == "http://redpencil.data.gift/id/concept/JobStatus/scheduled"
    ]
    if not task_triples:
        return "Can't do anything with this delta. Skipping.", 500

    thread = threading.Thread(target=run_tasks)
    thread.start()

    return "", 200

