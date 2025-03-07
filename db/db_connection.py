import os
import pymssql
import psycopg2
import logging
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)


def connect_mssql():
    """Connects to MSSQL using pymssql and returns the connection object."""
    try:
        conn = pymssql.connect(
            server=os.getenv("MSSQL_SERVER"),
            user=os.getenv("MSSQL_USERNAME"),
            password=os.getenv("MSSQL_PASSWORD"),
            database=os.getenv("MSSQL_DATABASE"),
        )
        logger.info("Connected to MSSQL successfully.")
        return conn
    except Exception as e:
        logger.error("Error connecting to MSSQL: %s", e, exc_info=True)
        return None


def connect_postgres():
    """Connects to PostgreSQL and returns the connection object."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DATABASE"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )
        logger.info("Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        logger.error("Error connecting to PostgreSQL: %s", e, exc_info=True)
        return None
