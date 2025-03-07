import logging
from services.mssql.get_employee_login_data import query_mssql_data
from queries.mssql.employee_login_hours import get_employee_login_hours
from pprint import pformat

logger = logging.getLogger(__name__)


def read_from_mssql(conn, date):
    """
    Reads data from MSSQL using the given connection and formats the output.
    """
    logger.info("Querying MSSQL for employee login data on %s", date)

    result = query_mssql_data(conn, get_employee_login_hours, date)

    if result:
        formatted_result = [
            {
                "EmpId": row[0],
                "Name": row[1],
                "Date": row[2].strftime("%Y-%m-%d"),
                "HoursWorked": float(row[3])
            }
            for row in result
        ]

        retrieved_dates = {row[2].strftime("%Y-%m-%d") for row in result}
        logger.info("Successfully retrieved %d records for dates: %s",
                    len(formatted_result), ", ".join(retrieved_dates))
        logger.info("Query result:\n%s", pformat(formatted_result))
    else:
        logger.warning("No data found for %s", date)
        formatted_result = []

    return formatted_result
