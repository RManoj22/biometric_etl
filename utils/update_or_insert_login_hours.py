import psycopg2
from utils.logger import logger

def update_or_insert_biometric_schedule(postgres_conn, merged_data, week_details):
    """
    Updates or inserts biometric work schedule records with detailed logging and transaction handling.

    :param postgres_conn: Connection object to Postgres.
    :param merged_data: List of employee records containing login hours.
    :param week_details: Dictionary with keys: 'day', 'start_date', 'end_date'.
    """

    if not postgres_conn:
        logger.error("No Postgres connection available.")
        return

    try:
        with postgres_conn.cursor() as cursor:
            # Begin transaction
            cursor.execute("BEGIN;")
            
            for record in merged_data:
                employee_id = record["postgres_id"]
                employee_name = record["employee_name"]
                login_hours = int(record["login_hours"])  # Convert float to integer
                work_day = week_details["day"].lower()  # e.g., 'friday'
                start_date = week_details["start_date"]
                end_date = week_details["end_date"]
                login_hour_column = f"loginhour_{work_day}"

                # Log the details before processing
                logger.info(
                    f"Processing employee: {employee_name} (ID: {employee_id}), "
                    f"Date: {start_date} to {end_date}, Day: {work_day}, "
                    f"Login Hours: {login_hours}"
                )

                # Check if the record already exists for the employee and date range
                cursor.execute(
                    """
                    SELECT id FROM biometric_workschedule
                    WHERE employee_id = %s AND start_date = %s AND end_date = %s
                    """,
                    (employee_id, start_date, end_date)
                )
                existing_row = cursor.fetchone()

                if existing_row:
                    logger.info(
                        f"Row already exists for {employee_name} (ID: {employee_id}), updating {work_day} login hours to {login_hours}."
                    )
                    cursor.execute(
                        f"""
                        UPDATE biometric_workschedule
                        SET {login_hour_column} = %s
                        WHERE employee_id = %s AND start_date = %s AND end_date = %s
                        """,
                        (login_hours, employee_id, start_date, end_date)
                    )
                else:
                    logger.info(
                        f"Creating new biometric schedule for {employee_name} (ID: {employee_id}). "
                        f"Setting {work_day} login hours to {login_hours}."
                    )
                    cursor.execute(
                        """
                        INSERT INTO biometric_workschedule (
                            start_date, end_date, loginhour_monday, loginhour_tuesday, loginhour_wednesday, 
                            loginhour_thursday, loginhour_friday, loginhour_saturday, loginhour_sunday, 
                            total_login_hour, employee_id
                        ) VALUES (%s, %s, 0, 0, 0, 0, 0, 0, 0, 0, %s)
                        RETURNING id
                        """,
                        (start_date, end_date, employee_id)
                    )

                    # Update the day's login hours
                    cursor.execute(
                        f"""
                        UPDATE biometric_workschedule
                        SET {login_hour_column} = %s
                        WHERE employee_id = %s AND start_date = %s AND end_date = %s
                        """,
                        (login_hours, employee_id, start_date, end_date)
                    )

            # If everything is successful, commit the transaction
            postgres_conn.commit()
            logger.info("Biometric work schedule update successfully committed.")

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}", exc_info=True)
        postgres_conn.rollback()  # Rollback if any error occurs
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        postgres_conn.rollback()  # Rollback on unexpected errors
