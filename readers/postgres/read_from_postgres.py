import logging
from services.postgres.get_employee_details import query_postgres_data
from queries.postgres.employee_details import get_employee_details_query
from pprint import pformat

logger = logging.getLogger(__name__)


def read_from_postgres(conn):
    """
    Reads data from Postgres using the given connection and formats the output.
    """
    logger.info("Querying Postgres for employee data")

    result = query_postgres_data(conn, get_employee_details_query)

    if result:
        logger.info("Successfully retrieved %d records.",
                    len(result))
        logger.info("Query result:\n%s", pformat(
            result))
    else:
        logger.warning("No data found")
        result = []

    return result
