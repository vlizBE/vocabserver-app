import os
from datetime import datetime
from string import Template

import requests
from escape_helpers import sparql_escape, sparql_escape_uri
from flask import request
from helpers import generate_uuid, logger
from helpers import query as sparql_query
from helpers import update as sparql_update
from sudo_query import query_sudo, update_sudo

from file import (construct_insert_file_query, file_to_shared_uri,
                  shared_uri_to_path)
from job import run_job

# Maybe make these configurable
JOBS_GRAPH = "http://mu.semte.ch/graphs/public"
FILES_GRAPH = "http://mu.semte.ch/graphs/public"

FILE_RESOURCE_BASE = 'http://example-resource.com/'
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")


def download_vocab_file(uri: str, graph: str = MU_APPLICATION_GRAPH):
    headers = {"Accept": "text/turtle"}
    r = requests.get(uri, headers=headers)
    # TODO: better handling + negociating
    assert r.headers["Content-Type"] == "text/turtle"

    upload_resource_uuid = generate_uuid()
    upload_resource_uri = f'{FILE_RESOURCE_BASE}{upload_resource_uuid}'
    file_extension = "ttl"  # TODO
    file_resource_uuid = generate_uuid()
    file_resource_name = f'{file_resource_uuid}.{file_extension}'

    file_resource_uri = file_to_shared_uri(file_resource_name)

    file = {
        'uri': upload_resource_uri,
        'uuid': upload_resource_uuid,
        'name': file_resource_name,
        'mimetype': 'text/plain',
        'created': datetime.now(),
        'size': r.headers["Content-Length"],
        'extension': file_extension,
    }
    physical_file = {
        'uri': file_resource_uri,
        'uuid': file_resource_uuid,
        'name': file_resource_name,
    }

    with open(shared_uri_to_path(file_resource_uri), 'wb') as f:
        f.write(r.content)

    query_string = construct_insert_file_query(file, physical_file, graph)

    # TODO Check query result before writing file to disk
    update_sudo(query_string)

    return upload_resource_uri


def get_job_uri(job_uuid: str, graph: str = MU_APPLICATION_GRAPH):
    query_template = Template('''
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT DISTINCT ?job_uri WHERE {
    GRAPH $graph {
        ?job_uri a ext:VocabDownloadJob ;
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


@app.route('/<job_uuid>', methods=['POST'])
def run_vocab_download(job_uuid: str):
    try:
        job_uri = get_job_uri(job_uuid)
    except Exception:
        logger.info(f"No job found by uuid ${job_uuid}")

    run_job(
        job_uri,
        JOBS_GRAPH,
        lambda sources: [download_vocab_file(
            sources[0], FILES_GRAPH)],
        query_sudo,
        update_sudo
    )

    return ''


@app.route('/delta', methods=['POST'])
def process_delta():
    inserts = request.json[0]['inserts']
    job_uuid = next(filter(
        lambda x: x['predicate']['value'] == 'http://mu.semte.ch/vocabularies/core/uuid',
        inserts
    ))['object']['value']
    run_vocab_download(job_uuid)
    return '', 200
