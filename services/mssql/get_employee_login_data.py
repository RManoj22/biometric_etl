import logging

logger = logging.getLogger(__name__)


def query_mssql_data(conn, query, date):
    """
    Executes a query on the MSSQL database and returns the results.

    :param conn: Database connection object
    :param query: SQL query string
    :param date: Parameter to be used in the query
    :return: List of tuples containing the query results
    """
    if not conn:
        logger.error("Failed to connect to MSSQL database.")
        return None

    try:
        cursor = conn.cursor()
        logger.info("Executing MSSQL query for date: %s", date)
        cursor.execute(query, (date,))
        rows = cursor.fetchall()
        logger.info(
            "Query executed successfully. Retrieved %d rows.", len(rows))
        return rows

    except Exception as e:
        logger.exception("Error executing MSSQL query.")
        return None

    finally:
        if conn:
            conn.close()
            logger.info("MSSQL database connection closed.")
