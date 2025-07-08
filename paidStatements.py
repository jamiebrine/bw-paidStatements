import pyodbc
import csv
import smtplib
from email.message import EmailMessage
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import sys

def get6MonthsAgo():
    """
    Gets a string representing the date 6 months ago.

    Returns:
        dateString (str): date in YYYY/MM/DD format
    """

    sixMonthsAgo = datetime.today() - timedelta(days=180)
    dateString = sixMonthsAgo.strftime('%Y/%m/%d')
    return dateString


def getData(query, dateString):
    """
    Executes SQL queries to retrieve payments made within the last 6 months.
    Outputs the results to a CSV file to be processed.

    Args:
        query (str): Parameterised query to retrieve payment data
        dateString (str): Lower bound for statement dates

    Returns:
        rows (list of str): Data returned by SQL
        headers (list of str): Data headers
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
                cursor.execute(query, dateString)
                headers = [column[0] for column in cursor.description]
                rows = cursor.fetchall()

    # Log any errors
    except Exception as e:
        logErrorAndExit(e)

    return rows, headers


def dumpToCSV(rows, headers):
    """
    Creates or overwrites a CSV file called 'new.csv' in the current working directory.
    It writes the provided headers as the first row, followed by the data rows.

    Args:
        rows (list of list): A list of data rows, where each row is a list of values.
        headers (list of str): A list of column headers to be written as the first row.

    """
    with open("new.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


def getNewEntries():
    """
    Compares 'old.csv' and 'new.csv' to find new entries.

    Reads both CSV files and determines which rows are new by subtracting
    the set of old rows from the set of new rows.

    Returns:
        newEntries (list of list): A list of rows of data
    """
    try:
        # Find new data by comparing new csv to old one
        with open('old.csv', newline='', encoding='utf-8') as csvfile:
            oldRows = set(tuple(row) for row in csv.reader(csvfile))
        with open('new.csv', newline='', encoding='utf-8') as csvfile:
            newRows = set(tuple(row) for row in csv.reader(csvfile))
    
    except Exception as e:
        logErrorAndExit(e)

    return newRows - oldRows


def createAttachment(rows, headers):
    """
    Creates the CSV attachment that will be send as an email attachment

    Args:
        rows (list of list): list of yesterday's paid statements
        headers (list of str): column headers for the data

    Returns:
        BytesIO: CSV content stored in memory
    """
    # Initialise bytes object to be sent as email attachment
    content = io.BytesIO()
    textWrapper = io.TextIOWrapper(content, encoding='utf-8', newline='')
    writer = csv.writer(textWrapper)

    # CREATE SUBTOTAL ROW
    
    # Write to file
    writer.writerow(headers)
    writer.writerows(rows)

    # Flush and rewind to the beginning
    textWrapper.flush()
    textWrapper.detach()
    content.seek(0)

    # Return CSV content
    return content


def sendEmail(content):
    """
    Sends an email with the CSV report attached via SMTP.

    Args:
        content (io.BytesIO): In-memory buffer containing CSV data.

    Raises:
        Logs and exits the script if email sending fails.
    """
    # SMTP configuration
    smtpServer = os.getenv('SMTP_SERVER')
    smtpPort = int(os.getenv('SMTP_PORT'))
    username = os.getenv('SMTP_USERNAME')
    password = os.getenv('SMTP_PASSWORD')

    # Create the email message
    msg = EmailMessage()
    msg['Subject'] = f'Yesterday\'s paid statements'
    msg['From'] = username
    msg['To'] = os.getenv('SMTP_RECIPIENT')

    # Set message content
    msg.set_content('Yesterday\'s paid statements')

    # Attach CSV
    msg.add_attachment(
        content.read(),
        maintype='text',
        subtype='csv',
        filename='paidStatements.csv'
    )

    # Send the email
    try:
        with smtplib.SMTP(smtpServer, smtpPort) as server:
            server.starttls()
            server.login(username, password)
            server.send_message(msg)

    except Exception as e: logErrorAndExit(e)


def renameFiles():
    """
    Removes old data and replaces it with updated data
    """
    try:
        if os.path.exists('old.csv'):
            os.remove('old.csv')

        if os.path.exists('new.csv'):
            os.rename('new.csv', 'old.csv')
    except Exception as e:
        logErrorAndExit(e)


def logErrorAndExit(e):
    """
    Logs an error message and exits the script.

    Args:
        e (Exception): The exception or error message to log.
    """

    with open("logs.txt", "a") as logs:
        logs.write(f'{e}\n')
    sys.exit(1)


def main():
    """
    Executes the end-to-end data processing pipeline:
        - Loads environment variables and SQL query.
        - Retrieves data from the past 6 months.
        - Saves results to CSV and identifies new entries.
        - Generates an email attachment and sends the email.
        - Renames output files and logs the run status.
    """    
    # Load credentials and query
    load_dotenv()
    with open('query.sql', 'r') as file:
        query = file.read()

    # Main logical flow
    dateString = get6MonthsAgo()
    rows, headers = getData(query, dateString)
    dumpToCSV(rows, headers)
    newEntries = getNewEntries()
    content = createAttachment(newEntries, headers)
    sendEmail(content)
    renameFiles()

    # Log success
    with open("logs.txt", "a") as logs:
        logs.write(f'Successful run: {datetime.now()}\n')


# Run program
if __name__ == '__main__':
    main()