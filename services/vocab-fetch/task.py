import os
import datetime
from string import Template
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string
from helpers import generate_uuid, logger
import traceback

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")

STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy'
STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled'
STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success'
STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed'

def create_download_task(dataset, graph=MU_APPLICATION_GRAPH):
    job_uri_prefix = 'http://redpencil.data.gift/id/job/'
    job_uuid = generate_uuid()
    job_uri = job_uri_prefix + job_uuid
    container_uri_prefix = 'http://redpencil.data.gift/id/container/'
    container_uuid = generate_uuid()
    container_uri = container_uri_prefix + container_uuid
    task_uri_prefix = 'http://redpencil.data.gift/id/task/'
    task_uuid = generate_uuid()
    task_uri = task_uri_prefix + task_uuid
    created = datetime.datetime.now()

    query_template = Template("""
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX adms: <http://www.w3.org/ns/adms#>

INSERT {
    GRAPH $graph {
        $job a cogs:Job;
            dct:creator "empty";
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled>;
            dct:created $created ;
            dct:modified $created ;
            task:operation <http://lblod.data.gift/id/jobs/concept/JobOperation/vocab-download> .
        $task_uri a task:Task ;
            mu:uuid $task_uuid ;
            dct:created $created ;
            dct:modified $created ;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> ;
            task:operation <http://mu.semte.ch/vocabularies/ext/VocabDownloadJob> ;
            task:index "0" ;
            dct:isPartOf $job ;
            task:inputContainer $container .
        $container a nfo:DataContainer ;
            mu:uuid $container_uuid ;
            ext:content $dataset .
    }
}
WHERE {
    GRAPH $graph {
        ?task a task:Task .
        FILTER NOT EXISTS {
            ?task
                adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> ;
                task:operation <http://mu.semte.ch/vocabularies/ext/VocabDownloadJob> ;
                task:inputContainer / ext:content $dataset .
        }
    }
}""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        job=sparql_escape_uri(job_uri),
        task_uri=sparql_escape_uri(task_uri),
        task_uuid=sparql_escape_string(task_uuid),
        created=sparql_escape_datetime(created),
        container_uuid=sparql_escape_string(container_uuid),
        container=sparql_escape_uri(container_uri),
        dataset=sparql_escape_uri(dataset)
    )
    return query_string

def attach_task_results_container(task, results, graph=MU_APPLICATION_GRAPH):
    CONTAINER_URI_PREFIX = 'http://redpencil.data.gift/id/container/'
    container_uuid = generate_uuid()
    container_uri = CONTAINER_URI_PREFIX + container_uuid

    container_query_template = Template("""
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

INSERT {
    GRAPH $graph {
        $task task:resultsContainer $container .
        $container a nfo:DataContainer ;
            mu:uuid $container_uuid ;
            ext:content $results .
    }
}
WHERE {
    GRAPH $graph {
        $task a task:Task .
    }
}""")
    container_query_string = container_query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        task=sparql_escape_uri(task),
        container_uuid=sparql_escape_string(container_uuid),
        container=sparql_escape_uri(container_uri),
        results=", ".join([sparql_escape_uri(result) for result in results])
    )
    return container_query_string

def update_task_status(task, status, graph=MU_APPLICATION_GRAPH):
    time = datetime.datetime.now()

    query_template = Template("""
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX dct: <http://purl.org/dc/terms/>

DELETE {
    GRAPH $graph {
        $task adms:status ?old_status ;
            dct:modified ?old_modified .
    }
}
INSERT {
    GRAPH $graph {
        $task adms:status $new_status ;
            dct:modified $modified .
    }
}
WHERE {
  GRAPH $graph {
      $task a task:Task ;
            adms:status ?old_status .
      OPTIONAL { $task dct:modified ?old_modified }
  }
}""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        task=sparql_escape_uri(task),
        new_status=sparql_escape_uri(status),
        modified=sparql_escape_datetime(time)
    )
    return query_string


def find_actionable_task(task_uri, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT (?uuid as ?id) ?status ?created ?used ?operation WHERE {
    GRAPH $graph {
        $task_uri a task:Task ;
            dct:created ?created ;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> ;
            task:operation ?operation ;
            mu:uuid ?uuid .
        OPTIONAL { $task_uri task:inputContainer/ext:content ?used }
    }
}
ORDER BY ASC(?created)
LIMIT 1
""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        task_uri=sparql_escape_uri(task_uri),
    )
    return query_string


def find_actionable_task_of_type(types, graph):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT (?task as ?uri) (?uuid as ?id) ?created ?used ?operation ?job_operation WHERE {
    GRAPH $graph {
        ?task a task:Task ;
            dct:created ?created ;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> ;
            task:operation ?operation ;
            mu:uuid ?uuid .
        OPTIONAL { ?task task:inputContainer/ext:content ?used }
        OPTIONAL {?task dct:isPartOf/task:operation ?job_operation}
        VALUES ?operation {$task_types}
    }
} LIMIT 1
""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        task_types = " ".join([sparql_escape_uri(uri) for uri in types])
        )
    return query_string

def run_task(task_uri, graph, runner_func, sparql_query, sparql_update):
    # start_time = time.time()

    task_q = find_actionable_task(task_uri, graph)
    task_res = sparql_query(task_q)
    if task_res["results"]["bindings"]:
        used = [binding["used"]["value"] for binding in task_res["results"]["bindings"] if "used" in binding]
    else:
        raise Exception(f"Didn't find actionable task for <{task_uri}>")

    logger.info(f"Started running task {task_uri}")

    sparql_update(update_task_status(task_uri, STATUS_BUSY, graph))
    try:
        generated = runner_func(used)
        if generated:
            logger.info(f"Running task <{task_uri}> with source <{used[0]}> generated <{generated[0]}>")
            sparql_update(attach_task_results_container(task_uri, generated, graph))
        sparql_update(update_task_status(task_uri, STATUS_SUCCESS, graph))
        return generated
    except Exception as e:
        logger.error(f"Error running task {task_uri}: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sparql_update(update_task_status(task_uri, STATUS_FAILED, graph))

    # end_time = time.time()
    # logger.info(
    # f"Finished running job at {start_time}, took {end_time - start_time} seconds"
    # )

