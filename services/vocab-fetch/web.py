import os
from datetime import datetime
from string import Template

import requests
from escape_helpers import sparql_escape, sparql_escape_uri
from flask import request
from helpers import generate_uuid, logger
from helpers import query, update
from sudo_query import query_sudo, update_sudo

from rdflib import Graph, URIRef, Literal
from rdflib.void import generateVoID

from file import file_to_shared_uri, shared_uri_to_path
from file import construct_get_file_query, construct_insert_file_query
from job import run_job
from dataset import get_dataset, update_dataset_download, get_dataset_by_uuid
from sparql_util import serialize_graph_to_sparql
from format_to_mime import FORMAT_TO_MIME_EXT

# Maybe make these configurable
JOBS_GRAPH = "http://mu.semte.ch/graphs/public"
FILES_GRAPH = "http://mu.semte.ch/graphs/public"
VOID_DATASET_GRAPH = "http://mu.semte.ch/graphs/public"

FILE_RESOURCE_BASE = 'http://example-resource.com/'
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
VOID_DATASET_RESOURCE_BASE = 'http://example-resource.com/void-dataset/'

def load_vocab_file(uri: str, graph: str = MU_APPLICATION_GRAPH):
    query_string = construct_get_file_query(uri, graph)
    file_result = query_sudo(query_string)['results']['bindings'][0]

    g = Graph()
    g.parse(shared_uri_to_path(file_result['physicalFile']['value']))

    return g

def download_vocab_file(url: str, format: str, graph: str = MU_APPLICATION_GRAPH):
    mime_type, file_extension = FORMAT_TO_MIME_EXT[format]
    accept_string = ', '.join(
        value[0] + (';q=1.0' if key == format else ';q=0.1') for key, value in FORMAT_TO_MIME_EXT.items())
    upload_resource_uuid = generate_uuid()
    upload_resource_uri = f'{FILE_RESOURCE_BASE}{upload_resource_uuid}'
    file_resource_uuid = generate_uuid()
    file_resource_name = f'{file_resource_uuid}.{file_extension}'

    file_resource_uri = file_to_shared_uri(file_resource_name)

    headers = {
        "Accept": accept_string,
        "Accept-Charset": "UTF-8;q=1.0, *;q=0.9"
    }

    with requests.get(url, headers=headers, stream=True) as res:
        if res.url != url:
            logger.info(
                "You've been redirected. Probably want to replace url in db.")

        assert res.ok

        # TODO: better handling + negociating
        logger.info(f'Content-Type: {res.headers["Content-Type"]}')
        logger.info(f'MIME-Type: {mime_type}')

        with open(shared_uri_to_path(file_resource_uri), 'wb') as f:
            for chunk in res.iter_content(chunk_size=None):
                f.write(chunk)

            f.seek(0, 2)
            file_size = f.tell()

    file = {
        'uri': upload_resource_uri,
        'uuid': upload_resource_uuid,
        'name': file_resource_name,
        'mimetype': 'text/plain',
        'created': datetime.now(),
        'size': file_size,
        'extension': file_extension,
    }
    physical_file = {
        'uri': file_resource_uri,
        'uuid': file_resource_uuid,
        'name': file_resource_name,
    }

    query_string = construct_insert_file_query(file, physical_file, graph)

    # TODO Check query result before writing file to disk
    update_sudo(query_string)

    return upload_resource_uri

def escape(binding):
    if binding['type'] == 'uri':
        return URIRef(binding['value'])
    elif binding['type'] == 'typed-literal':
        return Literal(binding['value'], datatype=binding['datatype'])
    else:
        return Literal(binding['value'])

def load_vocab_graph(graph: str):
    query_template = Template("""
SELECT ?s ?p ?o WHERE {
    GRAPH $graph {
        ?s ?p ?o .
    }
}
""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph)
    )
    results = query_sudo(query_string)['results']['bindings']
    g = Graph()
    for triple in results:
        g.add(tuple(map(escape, (triple['s'], triple['p'], triple['o']))))
    return g

def get_job_uri(job_uuid: str, graph: str = MU_APPLICATION_GRAPH):
    query_template = Template('''
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT DISTINCT ?job_uri WHERE {
    GRAPH $graph {
        ?job_uri a ext:DatasetDownloadJob ;
             mu:uuid $job_uuid .
    }
}
''')

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        job_uuid=sparql_escape(job_uuid),
    )
    query_res = query(query_string)
    return query_res['results']['bindings'][0]['job_uri']['value']

def redownload_dataset(dataset_uri):
    dataset_result = query_sudo(get_dataset(dataset_uri, VOID_DATASET_GRAPH))['results']['bindings'][0]
    download_link = dataset_result['download_url']['value']
    file_format = dataset_result['format']['value']
    file_uri = download_vocab_file(download_link, file_format, FILES_GRAPH)
    update_sudo(update_dataset_download(dataset_uri, file_uri, VOID_DATASET_GRAPH))
    return dataset_uri

@app.route('/dataset-download-job/<job_uuid>/run', methods=['POST'])
def run_dataset_download_route(job_uuid: str):
    try:
        job_uri = get_job_uri(job_uuid)
    except Exception:
        logger.info(f"No job found by uuid ${job_uuid}")
        return

    run_job(
        job_uri,
        JOBS_GRAPH,
        lambda sources: [redownload_dataset(sources[0])],
        query_sudo,
        update_sudo
    )

    return ''

def remove_old_metadata_from_graph(g, graph_name):
    for (s,p,_) in g.triples((None, None, None)):
        deletequery = "\n".join([f"PREFIX {prefix}: {ns.n3()}" for prefix, ns in g.namespaces()])
        deletequery += f"\nDELETE WHERE {{\n\tGRAPH {sparql_escape_uri(graph_name)} {{\n"
        deletequery += f" \t\t{s.n3()} {p.n3()} ?o ."
        deletequery += f" \n\t }}\n}}\n"
        update_sudo(deletequery)


def generate_dataset_structural_metadata(dataset_uri):
    dataset_res = query_sudo(get_dataset(dataset_uri, VOID_DATASET_GRAPH))['results']['bindings'][0]
    if 'data_dump' in dataset_res.keys():
        dataset_contents_g = load_vocab_file(dataset_res['data_dump']['value'], FILES_GRAPH)
    else:
        dataset_contents_g = load_vocab_graph(dataset_res['dataset_graph']['value'])
    dataset_meta_g, dataset = generateVoID(dataset_contents_g, dataset=URIRef(dataset_uri))
    remove_old_metadata_from_graph(dataset_meta_g, VOID_DATASET_GRAPH)
    for query_string in serialize_graph_to_sparql(dataset_meta_g, VOID_DATASET_GRAPH):
        update_sudo(query_string)
    return dataset_uri

VOCAB_DOWNLOAD_JOB = "http://mu.semte.ch/vocabularies/ext/VocabDownloadJob"
METADATA_EXTRACTION_JOB = "http://mu.semte.ch/vocabularies/ext/MetadataExtractionJob"

@app.route('/delta', methods=['POST'])
def process_delta():
    inserts = request.json[0]['inserts']
    job_triple = next(filter(
        lambda x: x['predicate']['value'] == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
        inserts
    ))
    job_uri = job_triple['subject']['value']
    job_type = job_triple['object']['value']
    
    if job_type == VOCAB_DOWNLOAD_JOB:
        run_job(
            job_uri,
            JOBS_GRAPH,
            lambda sources: [redownload_dataset(sources[0])],
            query_sudo,
            update_sudo
        )
        return '', 200
    elif job_type == METADATA_EXTRACTION_JOB:
        run_job(
            job_uri,
            JOBS_GRAPH,
            lambda sources: [generate_dataset_structural_metadata(sources[0])],
            query_sudo,
            update_sudo
        )
        return '', 200
    else:
        return "Don't know how to handle job of type " + job_type, 500