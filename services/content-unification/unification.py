import os
import datetime
from string import Template
import re
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string, sparql_escape_int, sparql_escape_bool
from helpers import generate_uuid

from constants import (
    MU_APPLICATION_GRAPH,
    JOB_URI_PREFIX,
    TASK_URI_PREFIX,
    CONTAINER_URI_PREFIX,
    FILTER_COUNT_INPUT_URI_PREFIX,
    FILTER_COUNT_OUTPUT_URI_PREFIX,
)
NEW_SUBJECT_BASE = "http://example-resource.com/dataset-subject/"

def get_property_paths(node_shape, metadata_graph):
    query_template = Template("""
PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT DISTINCT ?sourceClass ?sourcePathString ?sourceFilter ?destClass ?destPath
WHERE {
    GRAPH $metadata_graph {
        $node_shape
            a sh:NodeShape ;
            sh:targetClass ?sourceClass ;
            sh:property ?propertyShape .
        OPTIONAL {
            $node_shape ext:filter ?sourceFilter .
        }
        ?propertyShape
            a sh:PropertyShape ;
            sh:description ?destPath ;
            sh:path ?sourcePathString .
        BIND(skos:Concept as ?destClass)
    }
}
""")
    query_string = query_template.substitute(
        metadata_graph=sparql_escape_uri(metadata_graph),
        node_shape=sparql_escape_uri(node_shape),
    )
    return query_string


# Unification works as follows:
# the Subject from the source is taken based on the provided class (Pivot Type)
# and predicate path (Label/Tag path) given by the user
# this source is added to every provided dataset with a constructed URI (?internalSubject)
# that is unique per vocab (?vocabUri) and dataset subject (?sourceSubject)
# prov:wasDerivedFrom connects the original dataset's URI
# Note that this constructed URI is only needed for internal use:
# this avoids reusing URIs for the same dataset subjects over multiple vocabs (which might use the same dataset source)
# however, the user is only interested in the actual dataset uri (via prov:wasDerivedFrom)

# A unified entity is connected to a vocab via one (or more) datasets, but in reality a unified entity
# is part of a vocabulary (a vocab has one "unified dataset"), not part of "multiple" datasets. 
# As long as a concept is connected to one dataset of the vocab, the search will find it back.
UNUNIFIED_BATCH_TEMPLATE = Template("""
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

CONSTRUCT {
      ?internalSubject
          prov:wasDerivedFrom ?sourceSubject ;
          a $dest_class ;
          $dest_predicate ?sourceValue ;
          dct:source ?sourceDataset .
}
WHERE {
    VALUES ?sourceDataset {
        $source_datasets
    }
    ?vocabUri ext:sourceDataset ?sourceDataset .
    
    FILTER NOT EXISTS {
        GRAPH $target_graph {
            ?internalSubject 
                prov:wasDerivedFrom ?sourceSubject ;
                a $dest_class ;
                $dest_predicate ?sourceValue .
        }
    }
    VALUES ?source_graph { $source_graphs }
    GRAPH ?source_graph {
        ?sourceSubject
            a $source_class ;
            $source_path_string ?sourceValue .
        BIND(?sourceSubject as ?entity)
        $source_filter
    }
    BIND(IRI(CONCAT($new_subject_uri_base, MD5(CONCAT(str(?vocabUri), str(?sourceSubject))))) as ?internalSubject)
}
LIMIT $batch_size
""")

def get_ununified_batch(dest_class,
                        dest_predicate,
                        source_datasets,
                        source_class,
                        source_path_string,
                        source_filter,
                        source_graphs,
                        target_graph,
                        batch_size):
    query_string = UNUNIFIED_BATCH_TEMPLATE.substitute(
        dest_class=sparql_escape_uri(dest_class),
        dest_predicate=sparql_escape_uri(dest_predicate),
        source_datasets="\n         ".join([sparql_escape_uri(source_dataset) for source_dataset in source_datasets]),
        source_class=sparql_escape_uri(source_class),
        source_path_string=source_path_string,  # !this is already formatted as a sparql predicate path by the frontend. 
        source_graphs=" ".join([sparql_escape_uri(source_graph) for source_graph in source_graphs]),
        target_graph=sparql_escape_uri(target_graph),
        batch_size=batch_size,
        new_subject_uri_base=sparql_escape_string(NEW_SUBJECT_BASE),
        source_filter=source_filter
    )
    return query_string

UNUNIFIED_BATCH_TEMPLATE_VARS = set(re.compile(r'\?\w+').findall(UNUNIFIED_BATCH_TEMPLATE.template))
UNUNIFIED_BATCH_TEMPLATE_VARS.remove("?entity")

def count_ununified(source_class, source_path_string, source_filter, source_graphs):
    query_template = Template("""
SELECT (COUNT(DISTINCT ?entity) AS ?count) {
    VALUES ?source_graph { $source_graphs }
    GRAPH ?source_graph {
        ?sourceSubject
            a $source_class ;
            $source_path_string ?sourceValue .
        BIND(?sourceSubject as ?entity)
        $source_filter
    }
}
""")
    query_string = query_template.substitute(
        source_class=sparql_escape_uri(source_class),
        source_path_string=source_path_string,
        source_filter=source_filter,
        source_graphs=" ".join([sparql_escape_uri(source_graph) for source_graph in source_graphs]),
    )
    return query_string

def start_filter_count_task(dataset_uri, source_class, source_path_string, source_filter, graph):
    import datetime

    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

INSERT DATA {
    GRAPH $graph {
        $input_uri a ext:FilterCountInput ;
            mu:uuid $input_uuid ;
            dct:created $created ;
            dct:modified $created ;
            ext:dataset $dataset_uri ;
            ext:sourceClass $source_class ;
            ext:sourcePathString $source_path_string ;
            ext:sourceFilter $source_filter .
        $container_uri a nfo:DataContainer ;
            mu:uuid $container_uuid ;
            ext:content $input_uri .
        $job_uri a cogs:Job ;
            mu:uuid $job_uuid ;
            dct:created $created ;
            dct:modified $created ;
            dct:creator "empty" ;
            task:operation <http://lblod.data.gift/id/jobs/concept/JobOperation/filter-count> ;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> .
        $task_uri a task:Task ;
            mu:uuid $task_uuid ;
            dct:created $created ;
            dct:modified $created ;
            task:index "0";
            dct:isPartOf $job_uri;
            task:inputContainer $container_uri;
            task:operation <http://mu.semte.ch/vocabularies/ext/FilterCountJob> ;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> .
    }
}
      """)
    container_uuid = generate_uuid()
    container_uri = CONTAINER_URI_PREFIX + container_uuid
    job_uuid = generate_uuid()
    job_uri = JOB_URI_PREFIX + job_uuid
    task_uuid = generate_uuid()
    task_uri = TASK_URI_PREFIX + task_uuid
    created = datetime.datetime.now()

    input_uuid = generate_uuid()
    input_uri = FILTER_COUNT_INPUT_URI_PREFIX + input_uuid

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        container_uri=sparql_escape_uri(container_uri),
        job_uri=sparql_escape_uri(job_uri),
        task_uri=sparql_escape_uri(task_uri),
        container_uuid=sparql_escape_string(container_uuid),
        job_uuid=sparql_escape_string(job_uuid),
        task_uuid=sparql_escape_string(task_uuid),
        created=sparql_escape_datetime(created),

        dataset_uri=sparql_escape_uri(dataset_uri),
        input_uuid=sparql_escape_string(input_uuid),
        input_uri=sparql_escape_uri(input_uri),
        source_class=sparql_escape_uri(source_class),
        source_path_string=sparql_escape_string(source_path_string),
        source_filter=sparql_escape_string(source_filter),
    )
    return task_uuid, query_string

def get_filter_count_input(input_uri):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT ?dataset ?sourceClass ?sourcePathString ?sourceFilter {
    $input_uri a ext:FilterCountInput ;
        ext:dataset ?dataset ;
        ext:sourceClass ?sourceClass ;
        ext:sourcePathString ?sourcePathString ;
        ext:sourceFilter ?sourceFilter .
}
    """)
    query_string = query_template.substitute(
        input_uri = sparql_escape_uri(input_uri)
    )
    return query_string

def remove_filter_count_input(input_uri, graph):
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

WITH $graph
DELETE {
    $input_uri a ext:FilterCountInput ;
        ?p ?o .
} WHERE {
    $input_uri a ext:FilterCountInput ;
        ?p ?o .
}
    """)
    query_string = query_template.substitute(
        input_uri=sparql_escape_uri(input_uri),
        graph=sparql_escape_uri(graph)
    )
    return query_string

def write_filter_count_output(graph, dataset_uri, query, valid, count=None, warning=None, error=None):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

INSERT DATA {
    GRAPH $graph {
        $output_uri a ext:FilterCountOutput ;
            mu:uuid $output_uuid ;
            ext:dataset $dataset_uri ;
            dct:created $created ;
            dct:modified $created ;
            $attributes .
    }
}
        """)

    print(f"{query=}")
    attributes = [
        f"ext:query {sparql_escape_string(query)}",
        f"ext:valid {sparql_escape_bool(valid)}"
    ]
    if count or count == 0: attributes.append(f"ext:count {sparql_escape_int(count)}")
    if warning: attributes.append(f"ext:warning {sparql_escape_string(warning)}")
    if error: attributes.append(f"ext:error {sparql_escape_string(error)}")

    attributes_string = " ;\n            ".join(attributes)
    output_uuid = generate_uuid()
    output_uri = FILTER_COUNT_OUTPUT_URI_PREFIX + output_uuid

    created = datetime.datetime.now()

    query_string = query_template.substitute(
        attributes=attributes_string,
        dataset_uri=sparql_escape_uri(dataset_uri),
        output_uuid=sparql_escape_string(output_uuid),
        output_uri=sparql_escape_uri(output_uri),
        created=sparql_escape_datetime(created),
        graph=sparql_escape_uri(graph)
    )
    return output_uri, query_string

def get_delete_subjects_batch(
    dest_class,
    source_datasets,
    source_class,
    source_path_string,
    source_filter,
    source_graphs,
    target_graph,
    batch_size
) -> str:
    query_template = Template("""
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX prov: <http://www.w3.org/ns/prov#>

SELECT ?targetSubject {
    VALUES ?sourceDataset {
        $source_datasets
    }
    GRAPH $target_graph {
        ?targetSubject prov:wasDerivedFrom ?sourceSubject ;
            a $dest_class ;
            dct:source ?sourceDataset .
    }
    FILTER NOT EXISTS {
        VALUES ?source_graph { $source_graphs }
        GRAPH ?source_graph {
            ?sourceSubject
                a $source_class ;
                $source_path_string ?sourceValue .
            BIND (?sourceSubject as ?entity)
            $source_filter
        }
    }
}
LIMIT $batch_size
    """)
    query_string = query_template.substitute(
        dest_class=sparql_escape_uri(dest_class),
        source_datasets="\n        ".join([sparql_escape_uri(source_dataset) for source_dataset in source_datasets]),
        source_class=sparql_escape_uri(source_class),
        source_path_string=source_path_string,
        source_filter=source_filter,
        source_graphs=" ".join([sparql_escape_uri(source_graph) for source_graph in source_graphs]),
        target_graph=sparql_escape_uri(target_graph),
        batch_size=batch_size
    )
    return query_string

def delete_subjects(target_subjects, target_graph):
    query_template = Template("""
DELETE {
    GRAPH $target_graph {
        ?targetSubject ?p ?o .
    }
} WHERE {
    VALUES ?targetSubject {
        $target_subjects
    }
    ?targetSubject ?p ?o .
}
    """)
    query_string = query_template.substitute(
        target_subjects="\n        ".join([sparql_escape_uri(subject) for subject in target_subjects]),
        target_graph=sparql_escape_uri(target_graph)
    )
    return query_string

# delete the subjects provided that are part of a dataset
# note that these are related to the subject uris in our internal app via prov:wasDerivedFrom,
# see `get_ununified_batch` function for details
def delete_dataset_subjects_from_graph(subjects, graph):
    query_template = Template("""
PREFIX prov: <http://www.w3.org/ns/prov#>
WITH $graph
DELETE {
    ?internalSubject prov:wasDerivedFrom ?datasetSubject .
    ?internalSubject ?p ?o .
}
WHERE {
    ?internalSubject prov:wasDerivedFrom ?datasetSubject .
    ?internalSubject ?p ?o .
    VALUES ?datasetSubject {
        $subjects
    }
}
""")
    query_string = query_template.substitute(
        subjects="\n        ".join([sparql_escape_uri(s) for s in subjects]),
        graph=sparql_escape_uri(graph),
    )
    return query_string
