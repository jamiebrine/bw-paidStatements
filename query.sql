SELECT 
    tblsaledetails.SaleNo					    AS [Sale Number], 
    tblsaledetails.actual_date, 			    AS [Sale Date], 
    tblstatement.VendorNumber                   AS [Vendor Ref], 
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

