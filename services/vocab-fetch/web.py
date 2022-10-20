import os
import sys
from datetime import datetime
from io import TextIOWrapper
from string import Template
from typing import List

import requests
from escape_helpers import sparql_escape, sparql_escape_uri
from flask import request
from helpers import generate_uuid, logger
from helpers import query as sparql_query
from helpers import update as sparql_update

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
    file_format = 'some_format'  # TODO: Fill this in
    file_size = len(content)
    file_extension = 'ttl'

    upload_resource_uuid = generate_uuid()
    upload_resource_name = resource_name
    upload_resource_uri = f'{FILE_RESOURCE_BASE}{upload_resource_uuid}'

    file_resource_uuid = generate_uuid()
    file_resource_name = f'{file_resource_uuid}.{file_extension}'
    file_resource_uri = file_to_shared_uri(file_resource_name)

    fh = open(f'{STORAGE_PATH}/{file_resource_name}', 'wb')

    now = datetime.now()

    query_template = Template('''
$prefixes

INSERT DATA {
    GRAPH <$graph> {
        $upload_resource_uri a nfo:FileDataObject ;
            nfo:fileName $upload_resource_name ;
            mu:uuid $upload_resource_uuid ;
            dc:format $file_format ;
            nfo:fileSize $file_size ;
            dbpedia:fileExtension $file_extension ;
            dc:created $now ;
            dc:modified $now .
        
        $file_resource_uri a nfo:FileDataObject ;
            nie:dataSource $upload_resource_uri ;
            nfo:fileName $file_resource_name ;
            mu:uuid $file_resource_uuid ;
            dc:format $file_format ;
            nfo:fileSize $file_size ;
            dbpedia:fileExtension $file_extension ;
            dc:created $now ;
            dc:modified $now .
    }
}
''')

    query_string = query_template.substitute(
        prefixes='\n'.join([
            Prefixes.NFO,
            Prefixes.MU,
            Prefixes.DC,
            Prefixes.DBPEDIA,
            Prefixes.NIE,
        ]),

        graph=graph,

        upload_resource_uuid=sparql_escape(upload_resource_uuid),
        upload_resource_name=sparql_escape(resource_name),
        upload_resource_uri=sparql_escape_uri(upload_resource_uri),

        file_format=sparql_escape(file_format),
        file_size=sparql_escape(file_size),
        file_extension=sparql_escape(file_extension),

        file_resource_uuid=sparql_escape(file_resource_uuid),
        file_resource_name=sparql_escape(file_resource_name),
        file_resource_uri=sparql_escape_uri(file_resource_uri),

        now=sparql_escape(now),
    )

    sparql_update(query_string)

    fh.write(content)
    fh.close()


def fetch_vocab(sources: List[str], graph: str = MU_APPLICATION_GRAPH):

    query_template = Template('''
$prefixes

SELECT ?url WHERE {
    GRAPH $graph {
        $vocab vann:preferredNamespaceUri ?url .
    }
}
''')
    query_string = query_template.substitute(
        prefixes='\n'.join([
            Prefixes.MU,
            Prefixes.VANN,
            Prefixes.DCAT,
        ]),
        graph=sparql_escape_uri(graph) if graph else '?g',
        vocab=sparql_escape_uri(sources[0])
    )

    res = sparql_query(query_string)
    if not res['results']['bindings']:
        return []

    url = res['results']['bindings'][0]['url']['value']

    r = requests.get(url)
    create_file(sources[1], r.content)

    return [url]


@app.route('/fetch-vocab', methods=('POST',))
def fetch_vocab_route():
    #
    if not request.json:
        return "Missing data specifier", 400
    query_string, job = create_job(
        'http://mu.semte.ch/vocabularies/ext/VocabFetchJob',
        'http://somejob-resource-base/'
    )
    sparql_update(query_string)
    sparql_update(
        attach_job_sources(
            job['uri'],
            [
                request.json['data']['url'],
                request.json['data']['name']
            ]
        )
    )
    run_job(
        job['uri'],
        MU_APPLICATION_GRAPH,
        fetch_vocab,
        sparql_query,
        sparql_update
    )

    return ''


@app.route('/hello')
def hello():
    return 'Hellooo'
