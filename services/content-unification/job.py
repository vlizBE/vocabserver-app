import os
import datetime
from string import Template
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string
from helpers import generate_uuid, logger

############################################################
# TODO: keep this generic and extract into packaged module later
############################################################

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
FINAL_STATUSES = (
    "http://vocab.deri.ie/cogs#Success",
    "http://redpencil.data.gift/id/concept/JobStatus/success", # compat (unpublished)
    "http://vocab.deri.ie/cogs#Fail",
    "http://redpencil.data.gift/id/concept/JobStatus/failed", # compat (unpublished)
)
STARTED_STATUSES = (
    "http://vocab.deri.ie/cogs#Running",
    "http://redpencil.data.gift/id/concept/JobStatus/busy" # compat (unpublished)
)

STATUS_STARTED = "http://vocab.deri.ie/cogs#Running"
STATUS_SUCCESS = "http://vocab.deri.ie/cogs#Success"
STATUS_FAILED = "http://vocab.deri.ie/cogs#Fail"

def create_job(extra_rdf_type, resource_base, graph=MU_APPLICATION_GRAPH):
    uuid = generate_uuid()
    job = {
        "uri": resource_base.rstrip("/") + f"/jobs/{uuid}",
        "id": uuid,
        "created": datetime.datetime.now()
    }
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>

INSERT DATA {
    GRAPH $graph {
        $job a cogs:Job , $extra_rdf_type ;
        mu:uuid $uuid ;
        dct:created $created .
    }
}""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        job=sparql_escape_uri(job["uri"]),
        extra_rdf_type=sparql_escape_uri(extra_rdf_type),
        uuid=sparql_escape_string(job["id"]),
        created=sparql_escape_datetime(job["created"])
    )
    return query_string, job


def attach_job_sources (job, sources, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX prov: <http://www.w3.org/ns/prov#>
INSERT {
    GRAPH $graph {
        $job prov:used $sources .
    }
}
WHERE {
    GRAPH $graph {
        $job a cogs:Job .
    }
}""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        job=sparql_escape_uri(job),
        sources=", ".join([sparql_escape_uri(source) for source in sources])
    )
    return query_string


def attach_job_results (job, results, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX prov: <http://www.w3.org/ns/prov#>
INSERT {
    GRAPH $graph {
        $job prov:generated $results .
    }
}
WHERE {
    GRAPH $graph {
        $job a cogs:Job .
    }
}""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        job=sparql_escape_uri(job),
        results=", ".join([sparql_escape_uri(result) for result in results])
    )
    return query_string


def update_job_status (job, status, graph=MU_APPLICATION_GRAPH):
    time = datetime.datetime.now()

    if status in FINAL_STATUSES:
        time_pred = 'http://www.w3.org/ns/prov#endedAtTime'
    elif status in STARTED_STATUSES:
        time_pred = 'http://www.w3.org/ns/prov#startedAtTime'
    else:
        time_pred = None

    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>

DELETE {
    GRAPH $graph {
        $job ext:status ?old_status .
    }
}
INSERT {
    GRAPH $graph {
        $job ext:status $new_status .
        $time_triple
    }
}
WHERE {
  GRAPH $graph {
      $job a cogs:Job .
      OPTIONAL { $job ext:status ?old_status }
  }
}""")
    if time_pred:
        time_triple = f"{sparql_escape_uri(job)} {sparql_escape_uri(time_pred)} {sparql_escape_datetime(time)}."
    else:
        time_triple = ""
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        job=sparql_escape_uri(job),
        new_status=sparql_escape_uri(status),
        time_triple=time_triple
    )
    return query_string


def find_next_job(extra_rdf_type, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>

SELECT ?job (?uuid as ?id) ?status ?created WHERE {
    GRAPH $graph {
        ?job a cogs:Job , $extra_rdf_type ;
            dct:created ?created ;
            mu:uuid ?uuid .
            
        OPTIONAL { ?job ext:status ?status . }
        FILTER (?status NOT IN (
                $already_started_statuses
            )
        )
    }
}
ORDER BY ASC(?created)
LIMIT 1
""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        extra_rdf_type=sparql_escape_uri(extra_rdf_type),
        already_started_statuses=",\n                ".join(
            [sparql_escape_uri(stat) for stat in FINAL_STATUSES + STARTED_STATUSES])
    )
    return query_string


def find_actionable_job(job, graph=MU_APPLICATION_GRAPH):
    """ Query for making sure a job is still available just before running it.
        Also fetches the entities the job uses
    """
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX cogs: <http://vocab.deri.ie/cogs#>

SELECT (?uuid as ?id) ?status ?used WHERE {
    GRAPH $graph {
        $job a cogs:Job ;
            mu:uuid ?uuid .
            
        OPTIONAL { $job prov:used ?used . }
        OPTIONAL { $job ext:status ?status . }
        FILTER (?status NOT IN (
                $already_started_statuses
            )
        )
    }
}
""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        job=sparql_escape_uri(job) if graph else "?g",
        already_started_statuses=",\n                ".join(
            [sparql_escape_uri(stat) for stat in FINAL_STATUSES + STARTED_STATUSES])
    )
    return query_string


def run_job(job_uri, graph, runner_func, sparql_query, sparql_update):
    # start_time = time.time()

    job_q = find_actionable_job(job_uri, graph)
    job_res = sparql_query(job_q)
    if job_res["results"]["bindings"]:
        used = [binding["used"]["value"] for binding in job_res["results"]["bindings"] if "used" in binding]
    else:
        raise Exception(f"Didn't find actionable job for <{job_uri}>")

    # logger.info(f"Started running job {job_uri}")

    sparql_update(update_job_status(job_uri, STATUS_STARTED, graph))
    try:
        generated = runner_func(used)
        if generated:
            logger.info(f"Running job <{job_uri}> with source <{used[0]}> generated <{generated[0]}>")
            sparql_update(attach_job_results(job_uri, generated, graph))
        sparql_update(update_job_status(job_uri, STATUS_SUCCESS, graph))
        return generated
    except Exception as e:
        logger.error(e)
        sparql_update(update_job_status(job_uri, STATUS_FAILED, graph))

    # end_time = time.time()
    # logger.info(
    # f"Finished running job at {start_time}, took {end_time - start_time} seconds"
    # )

