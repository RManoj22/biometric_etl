import logging

logger = logging.getLogger(__name__)


def query_mssql_data(conn, query, params):
    """
    Executes a query on the MSSQL database and returns the results.

    :param conn: Database connection object
    :param query: SQL query string
    :param params: List of parameters (Employee IDs + Date)
    :return: List of tuples containing the query results
    """
    if not conn:
        logger.error("Failed to connect to MSSQL database.")
        return None

    try:
        cursor = conn.cursor()
        logger.info("Executing MSSQL query with parameters: %s", params)

        cursor.execute(query, params)  # Pass list of parameters safely
        rows = cursor.fetchall()

        logger.info(
            "Query executed successfully. Retrieved %d rows.", len(rows))
        return rows

    except Exception as e:
        logger.exception("Error executing MSSQL query.")
        return None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("MSSQL database connection closed.")
