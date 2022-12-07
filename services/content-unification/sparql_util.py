from escape_helpers import sparql_escape_uri
from more_itertools import batched
from rdflib.graph import Graph
from rdflib.term import URIRef, Literal
import tempfile

BATCH_SIZE = 100

# adapted from https://github.com/RDFLib/rdflib/issues/1704
def serialize_graph_to_sparql(g, graph_name: str):
    for triples_batch in batched(g.triples((None, None, None)), BATCH_SIZE):
        updatequery = "\n".join([f"PREFIX {prefix}: {ns.n3()}" for prefix, ns in g.namespaces()])
        updatequery += f"\nINSERT DATA {{\n\tGRAPH {sparql_escape_uri(graph_name)} {{\n"
        updatequery += " .\n".join([f"\t\t{s.n3()} {p.n3()} {o.n3()}" for (s, p, o) in triples_batch])
        updatequery += f" . \n\t }}\n}}\n"
        yield updatequery

def json_to_term(json_term):
    if json_term['type'] == 'uri':
        return URIRef(json_term['value'])
    else:
        lang = json_term['lang'] if 'lang' in json_term else None
        return Literal(json_term['value'], lang=lang)

def sparql_construct_res_to_graph(res):
    """ Turn results of a sparql construct query into an rdflib graph """
    g = Graph()
    for binding in res['results']['bindings']:
        s = URIRef(binding['s']['value'])
        p = URIRef(binding['p']['value'])
        o = json_to_term(binding['o'])
        g.add((s, p, o))
    return g
