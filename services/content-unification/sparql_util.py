from escape_helpers import sparql_escape_uri

# https://github.com/RDFLib/rdflib/issues/1704
def serialize_graph_to_sparql(g, graph_name: str):
    updatequery = "\n".join([f"PREFIX {prefix}: {ns.n3()}" for prefix, ns in g.namespaces()])
    updatequery += f"\nINSERT DATA {{\n\tGRAPH {sparql_escape_uri(graph_name)} {{\n"
    updatequery += " .\n".join([f"\t\t{s.n3()} {p.n3()} {o.n3()}" for (s, p, o) in g.triples((None, None, None))])
    updatequery += f" . \n\t }}\n}}\n"
    return updatequery
