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
    

def sendEmail(content):
    """
    Sends an email with the CSV report attached via SMTP.

    Args:
        content (io.BytesIO): In-memory buffer containing CSV data.

    Raises:
        Logs and exits the script if email sending fails.
    """
    # SMTP configuration (gets credentials from .env file so as not to hard code them in the script)
    smtpServer = os.getenv('SMTP_SERVER')
    smtpPort = int(os.getenv('SMTP_PORT'))
    username = os.getenv('SMTP_USERNAME')
    password = os.getenv('SMTP_PASSWORD')

    # Create the email message
    msg = EmailMessage()
    msg['Subject'] = f'Yesterday\'s numbers (you choose the subject)'
    msg['From'] = username
    msg['To'] = os.getenv('SMTP_RECIPIENT')

    # Set message content
    msg.set_content('test')

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

            with open("logs.txt", "a") as logs:
                logs.write(f'Successful run: {datetime.now()}\n')

    except Exception as e: logErrorAndExit(e)


def logErrorAndExit(e):
    """
    Logs an error message and exits the script.

    Args:
        e (Exception): The exception or error message to log.
    """

    with open("logs.txt", "a") as logs:
        logs.write(f'{e}\n')
    sys.exit(1)


load_dotenv()
query = '''
SELECT 
	tblsaledetails.SaleNo					    AS [Sale Number], 
    CONVERT(
		nvarchar(20), 
		tblsaledetails.actual_date, 
		106
	)										    AS [Sale Date], 
    tblstatement.VendorNumber AS [Vendor Ref], 
    CASE 
		WHEN LTRIM(RTRIM(isnull(tblclient_database.company_name, ''))) = '' THEN
			LTRIM(RTRIM(tblclient_database.title + ' ' + tblclient_database.firstname + ' ' + tblclient_database.surname)) 
		ELSE
			LTRIM(RTRIM(isnull(tblclient_database.company_name, ''))) 
	END										    AS [Account Name], 
    tblclient_database.account_name			    AS [Payee], 
    tblstatement.Statementnumber				AS [Statement No.], 
    CONVERT(
		nvarchar(20), 
		PARSE(tblstatement.statementdate AS date USING 'en-GB'), 
		106
    )										    AS [Statement Date], 
    FORMAT(tblstatement.Total, 'N2')			AS [Amount], 
    FORMAT(tblstatement.LeftToPay, 'N2')		AS [Left to Pay], 
    Replace(
		Replace(
			tblstatement.statementnotes, 
			CHAR(10), 
			''
		), 
		CHAR(13), 
		' '
    )											AS [Statement Notes], 
    FORMAT(
		lotDetail.goods + lotDetail.goodsVAT - (
			lotDetail.commission_VATseparated + lotDetail.commissionVAT_VATseparated + tblstatement.vendorcharges + tblstatement.vatvendorcharges
		), 
		'N2'
    )											AS [Total], 
	CONVERT(
		nvarchar(20), 
		payments.paydate
    )											AS [Payment Date], 
    FORMAT(payments.otherTotal, 'N2')			AS [Bank Transfer] 

FROM 
    tblstatement 
    LEFT JOIN tblsaledetails ON tblsaledetails.SaleID = tblstatement.SaleID 
    LEFT JOIN tblclient_database ON tblclient_database.client_ref = tblstatement.VendorNumber 
    LEFT JOIN (
		SELECT 
		    statementnumber, 
		    paydate, 
		    SUM(
				CASE WHEN type = 'cheque' THEN amount ELSE 0 END
			) AS chqTotal, 
		    SUM(
				CASE WHEN type = 'contra' OR type = 'xko' THEN amount ELSE 0 END
			) AS contraTotal, 
			SUM(
				CASE WHEN type != 'cheque' AND type != 'contra' AND type != 'xko' THEN amount ELSE 0 END
			) AS otherTotal 

		FROM 
			tblstatement_payments 

		GROUP BY 
			statementnumber, 
			paydate

	) AS payments ON payments.Statementnumber = tblstatement.Statementnumber 
	LEFT JOIN (
		SELECT 
			tblstatement_lines.statementnumber, 
			SUM(tbllot_details.hammer_excVAT) AS goods, 
			SUM(tbllot_details.hammer_vat)    AS goodsVAT, 
			SUM(
				CASE WHEN tbllot_details.hammer_vat != 0 THEN tbllot_details.hammer_excVAT END
			)								  AS hammerLiable, 
			SUM(
				CASE WHEN tbllot_details.hammer_vat = 0 THEN tbllot_details.hammer_excVAT END
			)								  AS hammerNotLiable, 
			SUM(
			   CASE WHEN tblsaledetails_vatrates.directChVATshownSeperately != 0 THEN commission_excVAT ELSE 0 END
		    )								  AS commission_VATseparated, 
			SUM(
			   CASE WHEN tblsaledetails_vatrates.directChVATshownSeperately != 0 THEN commission_VAT ELSE 0 END
		    )								  AS commissionVAT_VATseparated, 
			SUM(
				CASE WHEN tblsaledetails_vatrates.directChVATshownSeperately = 0 THEN commission_excVAT ELSE 0 END
			)								  AS commission_VATnotSeparated, 
			SUM(
				CASE WHEN tblsaledetails_vatrates.directChVATshownSeperately = 0 THEN commission_VAT ELSE 0 END
			)								  AS commissionVAT_VATnotSeparated

		FROM 
			tblstatement_lines 
			LEFT JOIN tblstatement ON tblstatement.Statementnumber = tblstatement_lines.statementnumber 
			LEFT JOIN tbllot_details ON tbllot_details.SaleID = tblstatement.saleid 
				AND tbllot_details.lotno = tblstatement_lines.lotno 
				AND tblstatement_lines.hammer > 0.005 
			LEFT JOIN tblsaledetails_vatrates ON tblsaledetails_vatrates.vatid = tbllot_details.VAT_rate 
		
		GROUP BY 
			tblstatement_lines.statementnumber
	) AS lotDetail ON lotDetail.statementnumber = tblstatement.Statementnumber 

WHERE 
	tblstatement.Total > 0 
	AND (
		tblsaledetails.actual_date <= GETDATE() 
		OR (
			tblsaledetails.SaleNo = 'TO010100' 
			OR tblsaledetails.SaleNo = 'PM281299'
		)	
	) 
	AND ISNULL(PARSE(tblstatement.statementdate AS date USING 'en-GB'), '') > ?
	AND ISNULL(payments.otherTotal, 0) != 0
  
ORDER BY 
	[Statement Date] DESC, 
    [Vendor Ref] ASC;
'''
dateString = get6MonthsAgo()
rows, columns = getData(query, dateString)
dumpToCSV(rows, columns)
print(getNewEntries())