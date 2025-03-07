import logging
from queries.postgres.match_employee_ids import match_employee_id_query

logger = logging.getLogger(__name__)


def match_employee_with_id(mssql_data, postgres_conn):
    """
    Maps MSSQL employee IDs to Postgres employee IDs using the biometric_empidmapping table.

    :param mssql_data: List of dictionaries containing MSSQL employee data.
    :param postgres_conn: Active Postgres database connection.
    :return: List of dictionaries with matched employee data.
    """
    mapped_results = []

    if not mssql_data:
        logger.warning("No MSSQL data provided for mapping.")
        return mapped_results

    if not postgres_conn or postgres_conn.closed:
        logger.error("Postgres connection is not available or already closed.")
        return mapped_results

    try:
        with postgres_conn.cursor() as cursor:
            for emp in mssql_data:
                emp_id_mssql = emp["EmpId"]
                logger.info(
                    f"Looking up Postgres EmpId for MSSQL EmpId: {emp_id_mssql}")

                cursor.execute(match_employee_id_query, (emp_id_mssql,))
                result = cursor.fetchone()

                if result:
                    emp_id_pg = result[0]
                    logger.info(
                        f"Found Postgres EmpId: {emp_id_pg} for MSSQL EmpId: {emp_id_mssql}")
                    emp["MappedPgEmpId"] = emp_id_pg
                else:
                    logger.warning(
                        f"No Postgres EmpId found for MSSQL EmpId: {emp_id_mssql}")
                    emp["MappedPgEmpId"] = None

                mapped_results.append(emp)

    except Exception as e:
        logger.exception("Error occurred while matching employee IDs.")

    return mapped_results
