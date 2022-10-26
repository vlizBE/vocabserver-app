import os
import sys
from datetime import datetime
from io import TextIOWrapper
from string import Template
from typing import List

import requests
from escape_helpers import sparql_escape, sparql_escape_uri
from helpers import generate_uuid, logger
from helpers import query as sparql_query
from helpers import update as sparql_update

from file import construct_insert_file_query
from job import attach_job_sources, create_job, run_job


def make_prefix(prefix: str, uri: str):
    return f'PREFIX {prefix}: <{uri}>'


class Prefixes:
    MU = make_prefix('mu', 'http://mu.semte.ch/vocabularies/core/')
    DCAT = make_prefix('dcat', 'http://www.w3.org/ns/dcat#')
    VANN = make_prefix('vann', 'http://purl.org/vocab/vann/')
    NFO = make_prefix(
        'nfo', 'http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#')
    NIE = make_prefix(
        'nie', 'http://www.semanticdesktop.org/ontologies/2007/01/19/nie#')
    DC = make_prefix('dc', 'http://purl.org/dc/terms/')
    DBPEDIA = make_prefix('dbpedia', 'http://dbpedia.org/ontology/')
    EXT = make_prefix('ext', 'http://mu.semte.ch/vocabularies/ext/')
    RDFS = make_prefix('rdfs', 'http://www.w3.org/2000/01/rdf-schema#')
    COGS = make_prefix('cogs', 'http://vocab.deri.ie/cogs#')
    PROV = make_prefix('prov', 'http://www.w3.org/ns/prov#')


def print(s: str):
    app.logger.info(s)


# Maybe make these configurable
FILE_RESOURCE_BASE = 'http://example-resource.com/'
STORAGE_PATH = '/data'
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")


def file_to_shared_uri(file_name):
    return f'share://{file_name}'


def create_file(
    resource_name: str,
    content: bytes,
    graph: str = MU_APPLICATION_GRAPH
):
    upload_resource_uuid = generate_uuid()
    file_extension = 'ttl'
    file_resource_uuid = generate_uuid()
    file_resource_name = f'{file_resource_uuid}.{file_extension}'

    fh = open(f'{STORAGE_PATH}/{file_resource_name}', 'wb')
    now = datetime.now()

    file = {
        'uri': f'{FILE_RESOURCE_BASE}{upload_resource_uuid}',
        'uuid': upload_resource_uuid,
        'name': resource_name,
        'mimetype': 'text/plain',
        'created': datetime.now(),
        'size': len(content),
        'extension': file_extension,
    }

    physical_file = {
        'uri': file_to_shared_uri(file_resource_name),
        'uuid': file_resource_uuid,
        'name': file_resource_name,
    }

    query_string = construct_insert_file_query(file, physical_file)

    # TODO Check query result before writing file to disk
    sparql_update(query_string)

    fh.write(content)
    fh.close()


def download_vocab_file(name, sources: List[str], graph: str = MU_APPLICATION_GRAPH):
    r = requests.get(sources[0])
    create_file(name, r.content)


def get_job_query(job_uuid: str, graph=MU_APPLICATION_GRAPH):
    query_template = Template('''
$prefixes

SELECT DISTINCT ?jobUri ?vocabName WHERE {
    GRAPH $graph {
        ?jobUri a cogs:Job ;
             mu:uuid $job_uuid ;
             prov:used ?vocabUrl .
        ?vocabUrl rdfs:label ?vocabName .
    }
}
''')

    query_string = query_template.substitute(
        prefixes='\n'.join([
            Prefixes.MU,
            Prefixes.COGS,
            Prefixes.RDFS,
            Prefixes.PROV,
        ]),
        graph=sparql_escape_uri(graph),
        job_uuid=sparql_escape(job_uuid),
    )

    return query_string


@app.route('/<job_uuid>', methods=('POST',))
def fetch_vocab_route(job_uuid: str):
    job_query = get_job_query(job_uuid)
    job_res = sparql_query(job_query)
    binding = job_res['results']['bindings'][0]
    run_job(
        binding['jobUri']['value'],
        MU_APPLICATION_GRAPH,
        lambda sources: download_vocab_file(
            binding['vocabName']['value'], sources),
        sparql_query,
        sparql_update
    )

    return ''
