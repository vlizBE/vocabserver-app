import datetime
import os
import time

from SPARQLWrapper import SPARQLWrapper, JSON
from helpers import logger

sparqlQuery = SPARQLWrapper(os.environ.get("MU_VIRTUOSO_ENDPOINT"), returnFormat=JSON)

def query(the_query):
    """Execute the given SPARQL query (select/ask/construct)on the triple store and returns
    the results in the given returnFormat (JSON by default)."""
    logger.debug("execute query: \n" + the_query)
    sparqlQuery.setQuery(the_query)
    return sparqlQuery.query().convert()
