import pyodbc
import csv
import smtplib
from email.message import EmailMessage
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import sys
from typing import Optional
import traceback

# TODO - get this fully from Glenn
recipientsDict = {
    'CV' : ['jamie.brine@brightwells.com'],
    'VT' : ['jamie.brine@brightwells.com'],
    'CC' : ['jamie.brine@brightwells.com'],
    'PM' : ['jamie.brine@brightwells.com'],
    'master': ['jamie.brine@brightwells.com']
}

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
        raise ValueError("Missing one or more SQL connection environment variables")

    # Define connection string
    connStr = (
        f"DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER=tcp:{server};"
        f"DATABASE={database};"
        f"UID={uid};"
        f"PWD={pwd};"
    )

    # Connect to database and execute queries, combining their results
    with pyodbc.connect(connStr) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, dateString)
            headers = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

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
    the set of old rows (as tuples) from the new rows (as tuples),
    but returns the new entries as mutable lists.

    Returns:
        newEntries (list of list): A list of new rows as lists.
    """

    with open('old.csv', newline='', encoding='utf-8') as csvfile:
        oldRows = set(tuple(row) for row in csv.reader(csvfile))
    with open('new.csv', newline='', encoding='utf-8') as csvfile:
        newReader = csv.reader(csvfile)
        newRows = [row for row in newReader]
    
    # Compare using tuple versions
    newEntries = [row for row in newRows if tuple(row) not in oldRows]

    # Convert numerical values to number format
    for entry in newEntries:
        for i in [7, 8, 10, 12]:
            entry[i] = float(entry[i].replace(',',''))

    return newEntries


def splitBySaleType(rows):
    """
    Creates a dictionary of sale types and their new entries

    Args:
        rows (list of list): List of each new entry

    Returns:
        dict: Created dictionary
    """    
    dict = {
    }

    for row in rows:

        # Gets first 2 letters of sale number to determine type
        saleType = row[0][:2]

        # If a sale of this type is already in the dictionary, add to its list 
        if saleType in dict:
            dict[saleType].append(row)
        # Else, create a new entry in the dictionary
        else:
            dict[saleType] = [row]
    
    return dict


def addSubtotals(rowDict):
    """
    Subtotals the values for each sale, inserting subtotal rows into the correct positions

    Args:
        rowDict (dict): Dictionary of (sale type : sale rows) pairs

    Returns:
        dict: Updated dictionary with subtotal rows added
    """
    for saleType in rowDict:

        # Get data for current sale type, and how many rows of data it has
        rows = rowDict[saleType]
        numRows = len(rows)

        # Track where subtotal rows have been added, and the most recent sale number
        subtotalRows = []
        currentSaleNo = rows[0][0]

        # Loop through each row of data, adding a subtotal row after each sale
        i = 0
        j = 0
        
        while i < numRows:

            # Tracks index of actual data rows, ignoring subtotal and buffer rows
            j =  i + 2 * len(subtotalRows)
            if rows[j][0] != currentSaleNo:

                # Subtotal from first row if there are currently none, or previous subtotal row otherwise
                subtotalRow = ['Subtotal:','','','','','','','7','8','','10','','12']
                if subtotalRows == []:
                    kMin = 0
                else:
                    kMin = subtotalRows[-1] + 1
                
                # Calculate subtotals for numerical values, adding them in the correct position
                subtotalRow[7] = sum(rows[k][7] for k in range(kMin, j))
                subtotalRow[8] = sum(rows[k][8] for k in range(kMin, j))
                subtotalRow[10] = sum(rows[k][10] for k in range(kMin, j))
                subtotalRow[12] = sum(rows[k][12] for k in range(kMin, j))

                # Insert subtotal and buffer row and update subtotal tracker
                rows.insert(j, subtotalRow)
                rows.insert(j + 1, ['-'] * 13)
                subtotalRows.append(j + 1)
                currentSaleNo = rows[j + 2][0]

            i += 1

        # Add final subtotal row
        subtotalRow = ['Subtotal:','','','','','','','7','8','','10','','12']
        if subtotalRows == []:
            kMin = 0
        else:
            kMin = subtotalRows[-1] + 1
        kMax = numRows + 2 * len(subtotalRows)

        subtotalRow[7] = sum(rows[k][7] for k in range(kMin, kMax))
        subtotalRow[8] = sum(rows[k][8] for k in range(kMin, kMax))
        subtotalRow[10] = sum(rows[k][10] for k in range(kMin, kMax))
        subtotalRow[12] = sum(rows[k][12] for k in range(kMin, kMax))
        rows.append(subtotalRow)

        # Update row dictionary with added rows 
        rowDict[saleType] = rows
    return rowDict
        

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


    # Write to file
    writer.writerow(headers)
    writer.writerows(rows)

    # Flush and rewind to the beginning
    textWrapper.flush()
    textWrapper.detach()
    content.seek(0)

    # Return CSV content
    return content


def sendEmail(content, recipients, subject):
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
    msg['Subject'] = subject
    msg['From'] = username

    # Process list of recipients as a comma seperated string for SMTP
    msg['To'] = ', '.join(recipients)

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
    with smtplib.SMTP(smtpServer, smtpPort) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(msg)


def renameFiles():
    """
    Removes old data and replaces it with updated data
    """
    if os.path.exists('old.csv'):
        os.remove('old.csv')

    if os.path.exists('new.csv'):
        os.rename('new.csv', 'old.csv')


def logAndExit(exception: Optional[Exception] = None, logFile: str = "logs.txt"):
    """
    Logs a success or error message to the top of the log file and exits the script.

    If an exception is provided, logs it as an error with a traceback.
    Otherwise, logs a default success message.

    Args:
        exception (Exception, optional): The exception to log. If provided, logs as an error.
        logFile (str): Path to the log file. Defaults to 'logs.txt'.
    """
    exitCode = 1 if exception else 0

    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if exception:
            header = f"[{timestamp}] ERROR:\n"
            body = traceback.format_exc()
        else:
            header = f"[{timestamp}] SUCCESS: Script completed successfully.\n"
            body = ""

        fullEntry = header + body + ("-" * 80) + "\n"

        oldContent = ""
        if os.path.exists(logFile):
            with open(logFile, 'r') as file:
                oldContent = file.read()

        with open(logFile, 'w') as file:
            file.write(fullEntry + oldContent)

    # Fallback
    except Exception as e:
        print('Failed to log due to error in logging function:')
        print(e)

    finally:
        sys.exit(exitCode)


def main():
    """
    Executes the end-to-end data processing pipeline:
        - Loads environment variables and SQL query.
        - Retrieves data from the past 6 months.
        - Saves results to CSV and identifies new entries.
        - Generates an email attachment and sends the email.
        - Renames output files and logs the run status.
    """
    try:
        # Load credentials and query
        load_dotenv()
        with open('query.sql', 'r') as file:
            query = file.read()

        # Main logical flow
        dateString = get6MonthsAgo()
        rows, headers = getData(query, dateString)
        dumpToCSV(rows, headers)
        newEntries = getNewEntries()

        rowDict = splitBySaleType(newEntries)
        rowDictWithSubtotals = addSubtotals(rowDict)
        masterCSVRows = []

        for key in rowDictWithSubtotals:
            
            # Sends an email to the correct department for each sale typ
            content = createAttachment(rowDictWithSubtotals[key], headers)
            recipients = recipientsDict[key]
            subject = f'{key} Payments Raised Yesterday'
            sendEmail(content, recipients, subject)

            # Add rows of CSV to master document
            masterCSVRows += rowDictWithSubtotals[key]
            masterCSVRows += [['~~~~~~'] * 14]

        # Send master document
        content = createAttachment(masterCSVRows, headers)
        recipients = recipientsDict['master']
        subject = 'All Payments Raised Yesterday'
        sendEmail(content, recipients, subject)

        # Delete old data and replace it with updated data
        renameFiles()
        logAndExit()
    
    except Exception as e:
        logAndExit(e)


# Run program
if __name__ == '__main__':
    main()