import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def get_week_details(input_date):
    """
    Given a date, returns the day of the week, the start date (Monday), 
    and the end date (Sunday) of the same week.

    :param input_date: A string representing the date (YYYY-MM-DD).
    :return: Dictionary containing 'day', 'start_date', and 'end_date'.
    """
    try:
        # Convert input string to datetime object
        date_obj = datetime.strptime(input_date, "%Y-%m-%d")
        logger.info(f"Processing date: {date_obj.strftime('%Y-%m-%d')}")

        # Get the day of the week
        day_name = date_obj.strftime("%A").lower()  # Example: "friday"
        logger.info(f"Day of the week: {day_name}")

        # Calculate the start of the week (Monday)
        start_of_week = date_obj - timedelta(days=date_obj.weekday())
        # Calculate the end of the week (Sunday)
        end_of_week = start_of_week + timedelta(days=6)

        week_details = {
            "day": day_name,
            "start_date": start_of_week.strftime("%Y-%m-%d"),
            "end_date": end_of_week.strftime("%Y-%m-%d"),
        }

        logger.info(f"Week details: {week_details}")
        return week_details

    except Exception as e:
        logger.error(f"Error processing date {input_date}: {e}", exc_info=True)
        return None
