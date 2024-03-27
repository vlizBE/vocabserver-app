from escape_helpers import sparql_escape_uri
from more_itertools import batched

BATCH_SIZE = 100


# adapted from https://github.com/RDFLib/rdflib/issues/1704
def serialize_graph_to_sparql(g, graph_name: str):
    for triples_batch in batched(g.triples((None, None, None)), BATCH_SIZE):
        updatequery = "\n".join(
            [f"PREFIX {prefix}: {ns.n3()}" for prefix, ns in g.namespaces()]
        )
        updatequery += f"\nINSERT DATA {{\n\tGRAPH {sparql_escape_uri(graph_name)} {{\n"
        updatequery += " .\n".join(
            [f"\t\t{s.n3()} {p.n3()} {o.n3()}" for (s, p, o) in triples_batch]
        )
        updatequery += f" . \n\t }}\n}}\n"
        yield updatequery


def binding_results(json_result, binded_values):
    bindings = []
    for binding in json_result["results"]["bindings"]:
        if isinstance(binded_values, tuple):
            values = tuple(
                (binding[key]["value"] if key in binding else None)
                for key in binded_values
            )
        else:
            values = binding[binded_values]["value"]
        bindings.append(values)
    return bindings
