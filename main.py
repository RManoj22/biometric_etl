from datetime import date
from utils.logger import logger
from readers.mssql.read_from_mssql import read_from_mssql
from readers.postgres.read_from_postgres import read_from_postgres
from db.db_connection import connect_mssql, connect_postgres
from utils.match_employees import match_employee_with_id

date_to_be_fetched = date.today()

if __name__ == "__main__":
    logger.info(f"Application started. Fetching data for {date_to_be_fetched}")

    mssql_conn = connect_mssql()
    postgres_conn = connect_postgres()

    try:
        if not postgres_conn:
            logger.error("Failed to establish a connection to Postgres.")
        if not mssql_conn:
            logger.error("Failed to establish a connection to MSSQL.")

        mssql_data = read_from_mssql(
            mssql_conn, date_to_be_fetched) if mssql_conn else []
        postgres_data = read_from_postgres(
            postgres_conn) if postgres_conn else []

        if not mssql_data:
            logger.warning("No data retrieved from MSSQL.")
        if not postgres_data:
            logger.warning("No data retrieved from Postgres.")

        # Ensure Postgres connection is open before calling match_employee_with_id
        if mssql_data and postgres_conn:
            if postgres_conn.closed:
                logger.warning("Reopening closed Postgres connection.")
                postgres_conn = connect_postgres()  # Reconnect if needed

            matched_results = match_employee_with_id(mssql_data, postgres_conn)
            logger.info(f"Total matched records: {len(matched_results)}")
        else:
            logger.warning(
                "Comparison skipped due to missing data or connection issues.")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)

    finally:
        if mssql_conn:
            mssql_conn.close()
            logger.info("MSSQL connection closed.")
        if postgres_conn and not postgres_conn.closed:
            postgres_conn.close()
            logger.info("Postgres connection closed.")
