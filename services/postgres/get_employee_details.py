import logging

logger = logging.getLogger(__name__)


def query_postgres_data(conn, query):
    """
    Executes a query on the Postgres database and returns the results.

    :param conn: Database connection object
    :param query: SQL query string
    :return: List of tuples containing the query results
    """
    if not conn:
        logger.error("Failed to connect to Postgres database.")
        return None

    try:
        cursor = conn.cursor()
        logger.info("Executing Postgres query to fetch employee details")
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.info(
            "Query executed successfully. Retrieved %d rows.", len(rows))
        return rows

    except Exception as e:
        logger.exception("Error executing Postgres query.")
        return None

    finally:
        if conn:
            conn.close()
            logger.info("Postgres database connection closed.")
