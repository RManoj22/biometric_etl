from datetime import date
from utils.logger import logger
from readers.mssql.read_from_mssql import read_from_mssql
from readers.postgres.read_from_postgres import read_from_postgres
from db.db_connection import connect_mssql, connect_postgres
from utils.merge_data import merge_employee_data
from utils.get_date_info import get_week_details
from utils.update_or_insert_login_hours import update_or_insert_biometric_schedule

# Fetch data for today's date
# date_to_be_fetched = date.today().strftime("%Y-%m-%d")
date_to_be_fetched = '2024-08-08'

if __name__ == "__main__":
    logger.info(f"Application started. Fetching data for {date_to_be_fetched}")

    # Get week details (day, start date, end date)
    week_details = get_week_details(date_to_be_fetched)

    if week_details:
        logger.info(f"Week details retrieved: {week_details}")
    else:
        logger.error("Failed to retrieve week details.")
        exit(1)  # Exit if we can't process the date information

    postgres_conn = connect_postgres()
    mssql_conn = connect_mssql()

    try:
        if not postgres_conn:
            logger.error("Failed to establish a connection to Postgres.")
        if not mssql_conn:
            logger.error("Failed to establish a connection to MSSQL.")

        # Fetch data from Postgres
        postgres_data = read_from_postgres(
            postgres_conn) if postgres_conn else []

        if not postgres_data:
            logger.warning("No data retrieved from Postgres.")
        else:
            # Extract "EmpIdMapping_MSSQLEmpID" from each row
            emp_mssql_ids = [row[0]["EmpIdMapping_MSSQLEmpID"]
                             for row in postgres_data]

            # Fetch data from MSSQL using extracted IDs and date
            mssql_data = read_from_mssql(
                mssql_conn, emp_mssql_ids, date_to_be_fetched) if mssql_conn else []

            if not mssql_data:
                logger.warning("No data retrieved from MSSQL.")
            else:
                logger.info(
                    f"Successfully retrieved {len(mssql_data)} records from MSSQL.")

        # Merge the data only if both sources retrieved records
        if postgres_data and mssql_data:
            merged_data = merge_employee_data(postgres_data, mssql_data)

            if merged_data:
                logger.info(f"Merged data: {merged_data}")

                # Ensure the connection is open before using it
                if postgres_conn.closed:
                    postgres_conn = connect_postgres()

                # Update or insert biometric work schedule
                update_or_insert_biometric_schedule(
                    postgres_conn, merged_data, week_details)
            else:
                logger.warning("No matching records found for merging.")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)

    finally:
        if mssql_conn:
            mssql_conn.close()
            logger.info("MSSQL connection closed.")
        if postgres_conn and not postgres_conn.closed:
            postgres_conn.close()
            logger.info("Postgres connection closed.")
