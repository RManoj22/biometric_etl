from datetime import datetime
from decimal import Decimal
import logging
from services.mssql.get_employee_login_data import query_mssql_data
from queries.mssql.employee_login_hours import get_employee_login_hours
from pprint import pformat

logger = logging.getLogger(__name__)


def format_result(result):
    """
    Converts query results (list of tuples) into a structured list of dictionaries.
    """
    formatted_result = [
        {
            "EmpIdN": int(row[0]),  # Employee ID
            # Convert datetime to string
            "AttdDateD": row[1].strftime("%Y-%m-%d"),
            "NWHN": float(row[2])  # Convert Decimal to float
        }
        for row in result
    ]
    return formatted_result


def read_from_mssql(conn, emp_mssql_ids, date):
    """
    Reads data from MSSQL using the given connection and formats the output.
    """
    if not emp_mssql_ids:
        logger.warning("No MSSQL Employee IDs provided.")
        return []

    logger.info(
        f"Querying MSSQL for {len(emp_mssql_ids)} employee(s) on {date}.")

    # Dynamically format the query to handle multiple employee IDs
    placeholders = ','.join(['%s'] * len(emp_mssql_ids))
    formatted_query = get_employee_login_hours.format(placeholders)

    # Execute query with employee IDs and date as parameters
    result = query_mssql_data(conn, formatted_query, emp_mssql_ids + [date])

    if result:
        logger.info(
            f"Successfully retrieved {len(result)} records from MSSQL.")

        # Convert result into a structured format with cleaned data types
        formatted_result = format_result(result)

        logger.info("Formatted Query result:\n%s", pformat(formatted_result))
        return formatted_result
    else:
        logger.warning(f"No data found for {date}.")
        return []
