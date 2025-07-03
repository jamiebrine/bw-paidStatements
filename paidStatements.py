import pyodbc
import csv
import smtplib
from email.message import EmailMessage
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import sys

def getDateStrings():
    """
    Gets the date for which the query should be run,
    by calculating the previous working day.

    Returns:
        yesterdayString (str): Last working day in YYYY/MM/DD format
    """

    # Gets the day that it is currently (Monday = 0, Sunday = 6)
    today = datetime.today()
    weekday = today.weekday()

    if weekday == 0:
        # Monday → go back to Friday
        lastWorkingDay = today - timedelta(days=3)
    else:
        # Tuesday–Friday → go back one day
        lastWorkingDay = today - timedelta(days=1)

    # Return dates in the format that SQL expects
    yesterdayString = lastWorkingDay.strftime('%Y/%m/%d')

    return yesterdayString


def getData(query, yesterdayString):
    """
    Executes SQL queries to retrieve payment data for the previous working day.

    Args:
        generalQuery (str): SQL query for non-bank-transfer payments.
        bankTransferQuery (str): SQL query for bank transfer payments.
        yesterdayString (str): Last working day in 'YYYY/MM/DD' format.
        todayString (str): Current day in 'YYYY/MM/DD' format.

    Returns:
        list: Combined list of rows from both queries.
              Each row is a tuple containing (str amount, str payment type).
    """

    # Initialise DB connection, ensuring all necessary credentials exist
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    uid = os.getenv("SQL_UID")
    pwd = os.getenv("SQL_PWD")

    if not all([server, database, uid, pwd]):
        logErrorAndExit(ValueError("Missing one or more SQL connection environment variables"))

    # Define connection string
    connStr = (
        f"DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER=tcp:{server};"
        f"DATABASE={database};"
        f"UID={uid};"
        f"PWD={pwd};"
    )

    # Connect to database and execute queries, combining their results
    try:
        with pyodbc.connect(connStr) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, yesterdayString)

    # Log any errors
    except Exception as e:
        logErrorAndExit(e)

    return rows