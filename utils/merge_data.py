import logging

logger = logging.getLogger(__name__)


def merge_employee_data(postgres_data, mssql_data):
    """
    Merges Postgres and MSSQL data based on employee IDs.

    :param postgres_data: List of Postgres employee data.
    :param mssql_data: List of MSSQL attendance data.
    :return: List of dictionaries containing merged employee details.
    """
    logger.info("Starting to merge Postgres and MSSQL data.")

    # Create a mapping of MSSQL ID to Postgres details for quick lookup
    emp_mapping = {row[0]["EmpIdMapping_MSSQLEmpID"]: row[0]
                   for row in postgres_data}
    logger.info(f"Created employee mapping with {len(emp_mapping)} records.")

    merged_data = []

    for record in mssql_data:
        mssql_id = record["EmpIdN"]
        login_date = record["AttdDateD"]
        login_hours = record["NWHN"]

        if mssql_id in emp_mapping:
            # Get the corresponding Postgres details
            emp_details = emp_mapping[mssql_id]

            merged_entry = {
                "employee_name": emp_details["employee_name"],
                # Postgres ID
                "postgres_id": emp_details["EmpIdMapping_PGEmpID"],
                "mssql_id": mssql_id,  # MSSQL ID
                "date": login_date,  # Attendance Date
                "login_hours": login_hours  # Login Hours
            }

            merged_data.append(merged_entry)
            logger.info(f"Merged data for MSSQL ID {mssql_id}: {merged_entry}")
        else:
            logger.warning(
                f"No Postgres mapping found for MSSQL ID {mssql_id}")

    logger.info(f"Finished merging. Total records merged: {len(merged_data)}")
    return merged_data
