import datetime
import os
import time

from SPARQLWrapper import SPARQLWrapper, JSON
from helpers import logger

sparqlQuery = SPARQLWrapper(os.environ.get("MU_SPARQL_ENDPOINT"), returnFormat=JSON)
sparqlQuery.addCustomHttpHeader("mu-auth-sudo", "true")
sparqlUpdate = SPARQLWrapper(os.environ.get("MU_SPARQL_UPDATEPOINT"), returnFormat=JSON)
sparqlUpdate.method = "POST"
sparqlUpdate.addCustomHttpHeader("mu-auth-sudo", "true")

authSparqlUpdate = SPARQLWrapper(os.environ.get("MU_AUTH_ENDPOINT"), returnFormat=JSON)
authSparqlUpdate.method = "POST"
authSparqlUpdate.addCustomHttpHeader("mu-auth-sudo", "true")


def query_sudo(the_query):
    """Execute the given SPARQL query (select/ask/construct)on the triple store and returns
    the results in the given returnFormat (JSON by default)."""
    logger.debug("execute query: \n" + the_query)
    sparqlQuery.setQuery(the_query)
    try:
        return sparqlQuery.query().convert()
    except Exception as e:
        logger.error("Query failed: \n" + the_query)
        logger.error(f"Query error: {str(e)}")
        raise


def update_sudo(the_query, attempt=0, max_retries=5):
    """Execute the given update SPARQL query on the triple store,
    if the given query is no update query, nothing happens."""
    sparqlUpdate.setQuery(the_query)
    if sparqlUpdate.isSparqlUpdateRequest():
        try:
            start = time.time()
            logger.debug("execute query: \n" + the_query)

            sparqlUpdate.query()

            logger.debug(f"Query took {round((time.time() - start) * 10**3)} ms")
        except Exception as e:
            logger.error("Update query failed: \n" + the_query)
            logger.error(f"Update query error: {str(e)}")
            if attempt <= max_retries:
                wait_time = 0.6 * attempt + 30
                logger.warn(f"Retrying after {wait_time} seconds [{attempt}/{max_retries}]")
                time.sleep(wait_time)

                update_sudo(the_query, attempt + 1, max_retries)
            else:
                logger.error("Max attempts reached for query. Skipping.")
                raise e


def auth_update_sudo(the_query):
    """Execute the given update SPARQL query on the triple store,
    if the given query is no update query, nothing happens."""
    authSparqlUpdate.setQuery(the_query)
    if authSparqlUpdate.isSparqlUpdateRequest():
        try:
            start = time.time()
            authSparqlUpdate.query()
            logger.debug(f"Query took {round((time.time() - start) * 10**3)} ms")
        except Exception as e:
            logger.error("Auth update query failed: \n" + the_query)
            logger.error(f"Auth update query error: {str(e)}")
            raise
