/* ++++++++++++++++++++++++++++++++++++++++++++++++++ *
 * +                                                + *
 * +        TPC-DI  Automated Audit Script          + *
 * +        Version 1.1.0                           + *
 * +                                                + *
 * ++++++++++++++++++++++++++++++++++++++++++++++++++ *
 *                                                    *
 *       ====== Portability Substitutions ======      *
 *     ---        [FROM DUMMY_TABLE]    ------        *
 * DB2            from sysibm.sysdummy1               *
 * ORACLE         from dual                           *
 * SQLSERVER       <blank>                            *
 * -------------------------------------------------- *
 *     ------  [||] (String concatenation) ------     *    
 * SQLSERVER      +                                   *
 * -------------------------------------------------- *
 */

select * from (

--  
--  Checks against the Audit table.  If there is a problem with the Audit table, then other tests are suspect...
--  

SELECT 
    'Audit table batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3
        THEN 'OK' 
        ELSE 'Not 3 batches' 
    END AS Result,
    'There must be audit data for 3 batches' AS Description
FROM Audit

union

SELECT 
    'Audit table sources' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT DataSet) = 13
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'There must be audit data for all data sets' AS Description
FROM Audit
WHERE DataSet IN (   
    'Batch',
    'DimAccount',
    'DimBroker',
    'DimCompany',
    'DimCustomer',
    'DimSecurity',
    'DimTrade',
    'FactHoldings',     
    'FactMarketHistory',
    'FactWatches',
    'Financial',
    'Generator',
    'Prospect'
)


--  
-- Checks against the DImessages table.
--  

union
SELECT 
    'DImessages validation reports' AS Test,
    BatchID,
    CASE 
        WHEN COUNT(*) = 24 THEN 'OK'
        ELSE 'Validation checks not fully reported' 
    END AS Result,
    'Every batch must have a full set of validation reports' AS Description
FROM DImessages
WHERE MessageType = 'Validation'
GROUP BY BatchID


union

SELECT 
    'DImessages batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 4 AND MAX(BatchID) = 3
        THEN 'OK' 
        ELSE 'Not 3 batches plus batch 0' 
    END AS Result,
    'Must have 3 distinct batches reported in DImessages' AS Description
FROM DImessages


union

SELECT 
    'DImessages Phase complete records' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 4 AND MAX(BatchID) = 3
        THEN 'OK' 
        ELSE 'Not 4 Phase Complete Records' 
    END AS Result,
    'Must have 4 Phase Complete records' AS Description
FROM DImessages
WHERE MessageType = 'PCR'


union

SELECT 
    'DImessages sources' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT MessageSource) = 17
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Messages must be present for all tables/transforms' AS Description
FROM DImessages
WHERE MessageType = 'Validation'
  AND MessageSource IN (
    'DimAccount', 'DimBroker', 'DimCustomer', 'DimDate', 'DimSecurity', 'DimTime', 
    'DimTrade', 'FactCashBalances', 'FactHoldings', 'FactMarketHistory', 'FactWatches', 
    'Financial', 'Industry', 'Prospect', 'StatusType', 'TaxRate', 'TradeType'
)


union

SELECT 
    'DImessages initial condition' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK' 
        ELSE 'Non-empty table in before Batch1' 
    END AS Result,
    'All DW tables must be empty before Batch1' AS Description
FROM DImessages
WHERE BatchID = 0
  AND MessageType = 'Validation'
  AND MessageData <> '0'





--  
-- Checks against the DimBroker table.
--  
union

SELECT 
    'DimBroker row count' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(d.*) = a.Value 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Actual row count matches Audit table' AS Description
FROM 
    DimBroker d
JOIN 
    Audit a 
    ON a.DataSet = 'DimBroker' 
    AND a.Attribute = 'HR_BROKERS'
GROUP BY 'DimBroker row count', NULL

union

SELECT 
    'DimBroker distinct keys' AS Test,
    NULL AS Batch,
	a.Value 
    CASE 
        WHEN COUNT(DISTINCT d.SK_BrokerID) = a.Value 
        THEN 'OK' 
        ELSE 'Not unique' 
    END AS Result,
    'All SKs are distinct' AS Description
FROM 
    DimBroker d
JOIN 
    Audit a 
    ON a.DataSet = 'DimBroker' 
    AND a.Attribute = 'HR_BROKERS'
GROUP BY 
    a.Value



union

SELECT 
    'DimBroker BatchID' AS Test,
    1 AS Batch,
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK' 
        ELSE 'Not batch 1' 
    END AS Result,
    'All rows report BatchID = 1' AS Description
FROM DimBroker
WHERE BatchID <> 1


union

SELECT 
    'DimBroker IsCurrent' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK' 
        ELSE 'Not current' 
    END AS Result,
    'All rows have IsCurrent = 1' AS Description
FROM DimBroker
WHERE IsCurrent <> 1


union

SELECT 
    'DimBroker EffectiveDate' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK' 
        ELSE 'Wrong date' 
    END AS Result,
    'All rows have Batch1 BatchDate as EffectiveDate' AS Description
FROM DimBroker
WHERE EffectiveDate <> '1950-01-01'


union

SELECT 
    'DimBroker EndDate' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK' 
        ELSE 'Wrong date' 
    END AS Result,
    'All rows have end of time as EndDate' AS Description
FROM DimBroker
WHERE EndDate <> '9999-12-31'



--  
-- Checks against the DimAccount table.
--  
union

SELECT 
    'DimAccount row count' AS Test,
    1 AS Batch,
    CASE 
        WHEN COUNT(*) >= (
            SELECT 
                COALESCE(SUM(CASE WHEN Attribute = 'C_NEW' THEN Value ELSE 0 END), 0) + 
                COALESCE(SUM(CASE WHEN Attribute = 'CA_ADDACCT' THEN Value ELSE 0 END), 0) + 
                COALESCE(SUM(CASE WHEN Attribute = 'CA_CLOSEACCT' THEN Value ELSE 0 END), 0) + 
                COALESCE(SUM(CASE WHEN Attribute = 'CA_UPDACCT' THEN Value ELSE 0 END), 0) + 
                COALESCE(SUM(CASE WHEN Attribute = 'C_UPDCUST' THEN Value ELSE 0 END), 0) + 
                COALESCE(SUM(CASE WHEN Attribute = 'C_INACT' THEN Value ELSE 0 END), 0) - 
                COALESCE(SUM(CASE WHEN Attribute = 'C_ID_HIST' THEN Value ELSE 0 END), 0) - 
                COALESCE(SUM(CASE WHEN Attribute = 'CA_ID_HIST' THEN Value ELSE 0 END), 0)
            FROM Audit
            WHERE BatchID = 1 AND DataSet IN ('DimCustomer', 'DimAccount')
        )
        THEN 'OK' 
        ELSE 'Too few rows' 
    END AS Result,
    'Actual row count matches or exceeds Audit table minimum' AS Description
FROM DimAccount
WHERE BatchID = 1



union

SELECT 
    'DimAccount row count' AS Test,
    d.BatchID,
    CASE 
        WHEN COUNT(DISTINCT d.BatchID) >= (
            COALESCE(SUM(CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_ADDACCT' THEN Value ELSE 0 END), 0) +
            COALESCE(SUM(CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_CLOSEACCT' THEN Value ELSE 0 END), 0) +
            COALESCE(SUM(CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_UPDACCT' THEN Value ELSE 0 END), 0) -
            COALESCE(SUM(CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_ID_HIST' THEN Value ELSE 0 END), 0)
        )
        THEN 'OK' 
        ELSE 'Too few rows' 
    END AS Result,
    'Actual row count matches or exceeds Audit table minimum' AS Description
FROM DimAccount d
JOIN Audit a ON d.BatchID = a.BatchID
WHERE a.BatchID IN (2, 3)
GROUP BY d.BatchID


union

SELECT 
    'DimAccount distinct keys' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT SK_AccountID) = COUNT(*) 
        THEN 'OK' 
        ELSE 'Not unique' 
    END AS Result,
    'All SKs are distinct' AS Description
FROM DimAccount


-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimAccount EndDate' checks that effective and end dates line up
--   'DimAccount Overlap' checks that there are not records that overlap in time
--   'DimAccount End of Time' checks that every company has a final record that goes to 9999-12-31

union

SELECT 
    'DimAccount EndDate' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(d.*) = COALESCE(joined_count, 0) + COALESCE(end_date_count, 0)
        THEN 'OK' 
        ELSE 'Dates not aligned' 
    END AS Result,
    'EndDate of one record matches EffectiveDate of another, or the end of time' AS Description
FROM DimAccount d
LEFT JOIN (
    SELECT 
        a.AccountID
    FROM 
        DimAccount a
    JOIN 
        DimAccount b 
    ON 
        a.AccountID = b.AccountID 
        AND a.EndDate = b.EffectiveDate
    GROUP BY a.AccountID
) joined_data ON d.AccountID = joined_data.AccountID
LEFT JOIN (
    SELECT 
        1 AS end_date_count 
    FROM 
        DimAccount 
    WHERE EndDate = '9999-12-31' 
    LIMIT 1
) end_date_data ON TRUE



union

SELECT 
    'DimAccount Overlap' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Dates overlap' 
    END AS Result,
    'Date ranges do not overlap for a given Account' AS Description
FROM DimAccount a
JOIN DimAccount b 
    ON a.AccountID = b.AccountID 
    AND a.SK_AccountID <> b.SK_AccountID 
    AND a.EffectiveDate >= b.EffectiveDate 
    AND a.EffectiveDate < b.EndDate

union

SELECT 
    'DimAccount End of Time' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT a.AccountID) = COUNT(DISTINCT CASE WHEN a.EndDate = '9999-12-31' THEN a.AccountID END)
        THEN 'OK' 
        ELSE 'End of time not reached' 
    END AS Result,
    'Every Account has one record with a date range reaching the end of time' AS Description
FROM DimAccount a



union

SELECT 
    'DimAccount consolidation' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Not consolidated' 
    END AS Result,
    'No records become effective and end on the same day' AS Description
FROM DimAccount
WHERE EffectiveDate = EndDate


union

SELECT 
    'DimAccount batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM DimAccount


union

SELECT 
    'DimAccount EffectiveDate' AS Test,
    b.BatchID,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Data out of range - see ticket #71' 
    END AS Result,
    'All records from a batch have an EffectiveDate in the batch time window' AS Description
FROM DimAccount a
JOIN Audit b ON a.BatchID = b.BatchID
WHERE b.DataSet = 'Batch'
  AND b.Attribute IN ('FirstDay', 'LastDay')
  AND b.BatchID IN (1, 2, 3)
  AND (
    (b.Attribute = 'FirstDay' AND a.EffectiveDate >= b.Date) 
    OR 
    (b.Attribute = 'LastDay' AND a.EffectiveDate <= b.Date)
  )
GROUP BY b.BatchID


union

SELECT 
    'DimAccount IsCurrent' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 
             SUM(CASE WHEN EndDate = '9999-12-31' AND IsCurrent = 1 THEN 1 ELSE 0 END) + 
             SUM(CASE WHEN EndDate < '9999-12-31' AND IsCurrent = 0 THEN 1 ELSE 0 END)
        THEN 'OK' 
        ELSE 'Not current' 
    END AS Result,
    'IsCurrent is 1 if EndDate is the end of time, else IsCurrent is 0' AS Description
FROM DimAccount



union

SELECT 
    'DimAccount Status' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All Status values are valid' AS Description
FROM DimAccount
WHERE Status NOT IN ('Active', 'Inactive')


union

SELECT 
    'DimAccount TaxStatus' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All TaxStatus values are valid' AS Description
FROM DimAccount
WHERE BatchID = 1 
  AND TaxStatus NOT IN (0, 1, 2)


union

SELECT 
    'DimAccount SK_CustomerID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_CustomerIDs match a DimCustomer record with a valid date range' AS Description
FROM DimAccount a
JOIN DimCustomer c 
    ON a.SK_CustomerID = c.SK_CustomerID 
    AND c.EffectiveDate <= a.EffectiveDate 
    AND a.EndDate <= c.EndDate

union

SELECT 
    'DimAccount SK_BrokerID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad join - spec problem with DimBroker EffectiveDate values' 
    END AS Result,
    'All SK_BrokerIDs match a broker record with a valid date range' AS Description
FROM DimAccount a
JOIN DimBroker c 
    ON a.SK_BrokerID = c.SK_BrokerID 
    AND c.EffectiveDate <= a.EffectiveDate 
    AND a.EndDate <= c.EndDate


union

SELECT 
    'DimAccount inactive customers' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'If a customer is inactive, the corresponding accounts must also have been inactive' AS Description
FROM DimCustomer c
LEFT JOIN DimAccount a 
    ON a.SK_CustomerID = c.SK_CustomerID 
    AND a.Status = 'Inactive'
WHERE c.Status = 'Inactive'
GROUP BY c.SK_CustomerID
HAVING COUNT(a.SK_CustomerID) > 0


--  
-- Checks against the DimCustomer table.
--  
union

SELECT 
    'DimCustomer row count' AS Test,
    a.BatchID,
    CASE 
        WHEN COUNT(*) >= (
            COALESCE(c_new.Value, 0) + COALESCE(c_inact.Value, 0) + COALESCE(c_updcust.Value, 0) - COALESCE(c_id_hist.Value, 0)
        ) 
        THEN 'OK' 
        ELSE 'Too few rows' 
    END AS Result,
    'Actual row count matches or exceeds Audit table minimum' AS Description
FROM DimCustomer a
LEFT JOIN Audit c_new 
    ON c_new.DataSet = 'DimCustomer' 
    AND c_new.Attribute = 'C_NEW' 
    AND c_new.BatchID = a.BatchID
LEFT JOIN Audit c_inact 
    ON c_inact.DataSet = 'DimCustomer' 
    AND c_inact.Attribute = 'C_INACT' 
    AND c_inact.BatchID = a.BatchID
LEFT JOIN Audit c_updcust 
    ON c_updcust.DataSet = 'DimCustomer' 
    AND c_updcust.Attribute = 'C_UPDCUST' 
    AND c_updcust.BatchID = a.BatchID
LEFT JOIN Audit c_id_hist 
    ON c_id_hist.DataSet = 'DimCustomer' 
    AND c_id_hist.Attribute = 'C_ID_HIST' 
    AND c_id_hist.BatchID = a.BatchID
WHERE a.BatchID IN (1, 2, 3)
GROUP BY a.BatchID



union

SELECT 
    'DimCustomer distinct keys' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT SK_CustomerID) = COUNT(*) 
        THEN 'OK' 
        ELSE 'Not unique' 
    END AS Result,
    'All SKs are distinct' AS Description
FROM DimCustomer


-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimCustomer EndDate' checks that effective and end dates line up
--   'DimCustomer Overlap' checks that there are not records that overlap in time
--   'DimCustomer End of Time' checks that every company has a final record that goes to 9999-12-31

union

SELECT 
    'DimCustomer EndDate' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = (
            COUNT(CASE WHEN a.EndDate = b.EffectiveDate THEN 1 END) 
            + COUNT(CASE WHEN a.EndDate = '9999-12-31' THEN 1 END)
        ) 
        THEN 'OK' 
        ELSE 'Dates not aligned' 
    END AS Result,
    'EndDate of one record matches EffectiveDate of another, or the end of time' AS Description
FROM DimCustomer a
LEFT JOIN DimCustomer b
    ON a.CustomerID = b.CustomerID 
    AND a.EndDate = b.EffectiveDate


union

SELECT 
    'DimCustomer Overlap' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Dates overlap' 
    END AS Result,
    'Date ranges do not overlap for a given Customer' AS Description
FROM DimCustomer a
JOIN DimCustomer b 
    ON a.CustomerID = b.CustomerID 
    AND a.SK_CustomerID <> b.SK_CustomerID 
    AND a.EffectiveDate >= b.EffectiveDate 
    AND a.EffectiveDate < b.EndDate


union

SELECT 
    'DimCustomer End of Time' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT CustomerID) = COUNT(CASE WHEN EndDate = '9999-12-31' THEN 1 END) 
        THEN 'OK' 
        ELSE 'End of time not reached' 
    END AS Result,
    'Every Customer has one record with a date range reaching the end of time' AS Description
FROM DimCustomer


union

SELECT 
    'DimCustomer consolidation' AS Test,
    NULL AS Batch,
    CASE 
        WHEN (
            SELECT COUNT(*) 
            FROM DimCustomer 
            WHERE EffectiveDate = EndDate
        ) = 0 
        THEN 'OK' 
        ELSE 'Not consolidated' 
    END AS Result,
    'No records become effective and end on the same day' AS Description
FROM DimCustomer

union

SELECT 
    'DimCustomer batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 
             AND MAX(BatchID) = 3 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM DimCustomer


union

SELECT 
    'DimCustomer IsCurrent' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = total_enddate_9999 + total_enddate_less_9999
        THEN 'OK' 
        ELSE 'Not current' 
    END AS Result,
    'IsCurrent is 1 if EndDate is the end of time, else IsCurrent is 0' AS Description
FROM DimCustomer,
     (SELECT COUNT(*) AS total_enddate_9999
      FROM DimCustomer 
      WHERE EndDate = '9999-12-31' AND IsCurrent = 1) AS t1,
     (SELECT COUNT(*) AS total_enddate_less_9999
      FROM DimCustomer 
      WHERE EndDate < '9999-12-31' AND IsCurrent = 0) AS t2


union

SELECT 
    'DimCustomer EffectiveDate' AS Test,
    o.BatchID,
    o.Result,
    'All records from a batch have an EffectiveDate in the batch time window' AS Description
FROM (
    SELECT 
        a.BatchID,
        CASE 
            WHEN COUNT(*) = 0 THEN 'OK' 
            ELSE 'Data out of range' 
        END AS Result
    FROM 
        DimCustomer dc
    JOIN 
        Audit a 
        ON dc.BatchID = a.BatchID
    WHERE 
        a.DataSet = 'Batch' 
        AND a.Attribute IN ('FirstDay', 'LastDay')
        AND dc.EffectiveDate < (SELECT Date FROM Audit WHERE DataSet = 'Batch' AND Attribute = 'FirstDay' AND BatchID = dc.BatchID)
        OR dc.EffectiveDate > (SELECT Date FROM Audit WHERE DataSet = 'Batch' AND Attribute = 'LastDay' AND BatchID = dc.BatchID)
    GROUP BY 
        a.BatchID
) o


union

SELECT 
    'DimCustomer Status' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(CASE WHEN Status NOT IN ('Active', 'Inactive') THEN 1 END) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All Status values are valid' AS Description
FROM DimCustomer


union

SELECT 
    'DimCustomer inactive customers' AS Test,
    a.BatchID,
    CASE 
        WHEN COALESCE(m.MessageData, '') = COALESCE(SUM(au.Value), '') 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Inactive customer count matches Audit table' AS Description
FROM 
    Audit a
LEFT JOIN 
    DImessages m 
    ON m.BatchID = a.BatchID 
    AND m.MessageType = 'Validation' 
    AND m.MessageSource = 'DimCustomer' 
    AND m.MessageText = 'Inactive customers'
LEFT JOIN 
    Audit au 
    ON au.BatchID <= a.BatchID 
    AND au.DataSet = 'DimCustomer' 
    AND au.Attribute = 'C_INACT'
WHERE 
    a.BatchID IN (1, 2, 3)
GROUP BY 
    a.BatchID


union

SELECT 
    'DimCustomer Gender' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(CASE WHEN Gender NOT IN ('M', 'F', 'U') THEN 1 END) = 0
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All Gender values are valid' AS Description
FROM DimCustomer


union

SELECT 
    'DimCustomer age range alerts' AS Test,
    dm.BatchID,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN dm.MessageText = 'DOB out of range' THEN 1 END) = 
            (SELECT COALESCE(SUM(Value), 0) 
             FROM Audit a 
             WHERE a.DataSet = 'DimCustomer' 
             AND a.BatchID = dm.BatchID 
             AND a.Attribute IN ('C_DOB_TO', 'C_DOB_TY')) 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Count of age range alerts matches audit table' AS Description
FROM 
    DImessages dm
JOIN 
    Audit a ON a.BatchID = dm.BatchID
WHERE 
    dm.MessageType = 'Alert' 
    AND dm.MessageText = 'DOB out of range' 
    AND dm.BatchID IN (1, 2, 3)
GROUP BY 
    dm.BatchID



union

SELECT 
    'DimCustomer customer tier alerts' AS Test,
    a.BatchID,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN dm.MessageText = 'Invalid customer tier' THEN 1 END) = 
            COALESCE(SUM(aud.Value), 0) 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Count of customer tier alerts matches audit table' AS Description
FROM 
    Audit a
LEFT JOIN 
    DImessages dm ON a.BatchID = dm.BatchID 
    AND dm.MessageType = 'Alert' 
    AND dm.MessageText = 'Invalid customer tier'
LEFT JOIN 
    Audit aud ON aud.DataSet = 'DimCustomer' 
    AND aud.BatchID = a.BatchID 
    AND aud.Attribute = 'C_TIER_INV'
WHERE 
    a.BatchID IN (1, 2, 3)
GROUP BY 
    a.BatchID


union    

SELECT 
    'DimCustomer TaxID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'TaxID values are properly formatted' AS Description
FROM DimCustomer
WHERE TaxID NOT LIKE '___-__-____'


union  

SELECT 
    'DimCustomer Phone1' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Phone1 values are properly formatted' AS Description
FROM DimCustomer
WHERE Phone1 NOT LIKE '+1 (___) ___-____%'
  AND Phone1 NOT LIKE '(___) ___-____%'
  AND Phone1 NOT LIKE '___-____%'
  AND Phone1 <> ''
  AND Phone1 IS NOT NULL


union     

SELECT 
    'DimCustomer Phone2' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Phone2 values are properly formatted' AS Description
FROM DimCustomer
WHERE Phone2 NOT LIKE '+1 (___) ___-____%'
  AND Phone2 NOT LIKE '(___) ___-____%'
  AND Phone2 NOT LIKE '___-____%'
  AND Phone2 <> ''
  AND Phone2 IS NOT NULL


union     

SELECT 
    'DimCustomer Phone3' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Phone3 values are properly formatted' AS Description
FROM DimCustomer
WHERE Phone3 NOT LIKE '+1 (___) ___-____%'
  AND Phone3 NOT LIKE '(___) ___-____%'
  AND Phone3 NOT LIKE '___-____%'
  AND Phone3 <> ''
  AND Phone3 IS NOT NULL


union    

SELECT 
    'DimCustomer Email1' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Email1 values are properly formatted' AS Description
FROM DimCustomer
WHERE Email1 NOT LIKE '_%.%@%.%'
  AND Email1 IS NOT NULL


union

SELECT 
    'DimCustomer Email2' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Email2 values are properly formatted' AS Description
FROM DimCustomer
WHERE Email2 NOT LIKE '_%.%@%.%' 
  AND Email2 <> '' 
  AND Email2 IS NOT NULL



union      

SELECT 
    'DimCustomer LocalTaxRate' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(c.LocalTaxRateDesc) = COUNT(t.TX_NAME) 
            AND COUNT(DISTINCT c.LocalTaxRateDesc) > 300
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'LocalTaxRateDesc and LocalTaxRate values are from TaxRate table' AS Description
FROM DimCustomer c
JOIN TaxRate t
    ON c.LocalTaxRateDesc = t.TX_NAME
    AND c.LocalTaxRate = t.TX_RATE


union        

SELECT 
    'DimCustomer NationalTaxRate' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(c.NationalTaxRateDesc) = COUNT(t.TX_NAME) 
            AND COUNT(DISTINCT c.NationalTaxRateDesc) >= 9
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'NationalTaxRateDesc and NationalTaxRate values are from TaxRate table' AS Description
FROM DimCustomer c
JOIN TaxRate t
    ON c.NationalTaxRateDesc = t.TX_NAME
    AND c.NationalTaxRate = t.TX_RATE


union    

SELECT 
    'DimCustomer demographic fields' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT c.CustomerID) = COUNT(DISTINCT c2.CustomerID)
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'For current customer records that match Prospect records, the demographic fields also match' AS Description
FROM DimCustomer c
JOIN Prospect p 
    ON UPPER(c.FirstName || c.LastName || c.AddressLine1 || COALESCE(c.AddressLine2, '') || c.PostalCode) = 
       UPPER(p.FirstName || p.LastName || p.AddressLine1 || COALESCE(p.AddressLine2, '') || p.PostalCode)
    AND COALESCE(c.CreditRating, 0) = COALESCE(p.CreditRating, 0)
    AND COALESCE(c.NetWorth, 0) = COALESCE(p.NetWorth, 0)
    AND COALESCE(c.MarketingNameplate, '') = COALESCE(p.MarketingNameplate, '')
    AND c.IsCurrent = 1
JOIN DimCustomer c2
    ON c2.AgencyID IS NOT NULL
    AND c2.IsCurrent = 1

--  
-- Checks against the DimSecurity table.
--  

union

SELECT 
    'DimSecurity row count' AS Test,
    a.BatchID,
    CASE 
        WHEN CAST(d.MessageData AS bigint) >= COALESCE(SUM(b.Value), 0) THEN 'OK'
        ELSE 'Too few rows'
    END AS Result,
    'Actual row count matches or exceeds Audit table minimum' AS Description
FROM Audit a
JOIN DImessages d
    ON d.MessageType = 'Validation'
    AND d.BatchID = a.BatchID
    AND d.MessageSource = 'DimSecurity'
    AND d.MessageText = 'Row count'
LEFT JOIN Audit b
    ON b.DataSet = 'DimSecurity'
    AND b.Attribute = 'FW_SEC'
    AND b.BatchID <= a.BatchID
WHERE a.BatchID IN (1)
GROUP BY a.BatchID, d.MessageData


union 

SELECT 
    'DimSecurity distinct keys' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT SK_SecurityID) = COUNT(*) THEN 'OK'
        ELSE 'Not unique'
    END AS Result,
    'All SKs are distinct' AS Description
FROM DimSecurity


-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimSecurity EndDate' checks that effective and end dates line up
--   'DimSecurity Overlap' checks that there are not records that overlap in time
--   'DimSecurity End of Time' checks that every company has a final record that goes to 9999-12-31

union

select 
    'DimSecurity EndDate' as Description,
    NULL as Placeholder,
    case 
        when d.total_count = (d.joined_count + d.end_of_time_count) 
        then 'OK' 
        else 'Dates not aligned' 
    end as Status,
    'EndDate of one record matches EffectiveDate of another, or the end of time' as Explanation
from (
    select 
        count(*) as total_count,
        sum(case when a.EndDate = b.EffectiveDate then 1 else 0 end) as joined_count,
        sum(case when a.EndDate = '9999-12-31' then 1 else 0 end) as end_of_time_count
    from DimSecurity a
    left join DimSecurity b
    on a.Symbol = b.Symbol
) d

union

select 
    'DimSecurity Overlap' as Description,
    NULL as Placeholder,
    case 
        when overlap_count = 0 
        then 'OK' 
        else 'Dates overlap' 
    end as Status,
    'Date ranges do not overlap for a given company' as Explanation
from (
    select 
        count(*) as overlap_count
    from DimSecurity a
    join DimSecurity b 
        on a.Symbol = b.Symbol 
        and a.SK_SecurityID <> b.SK_SecurityID 
        and a.EffectiveDate >= b.EffectiveDate 
        and a.EffectiveDate < b.EndDate
) d

union

select 
    'DimSecurity End of Time' as Description,
    NULL as Placeholder,
    case 
        when total_symbols = end_of_time_count 
        then 'OK' 
        else 'End of time not reached' 
    end as Status,
    'Every company has one record with a date range reaching the end of time' as Explanation
from (
    select 
        count(distinct Symbol) as total_symbols,
        count(case when EndDate = '9999-12-31' then 1 else null end) as end_of_time_count
    from DimSecurity
) d

union

select 
    'DimSecurity consolidation' as Description,
    NULL as Placeholder,
    case 
        when consolidated_count = 0 
        then 'OK' 
        else 'Not consolidated' 
    end as Status,
    'No records become effective and end on the same day' as Explanation
from (
    select 
        count(*) as consolidated_count
    from DimSecurity 
    where EffectiveDate = EndDate
) d


union

select 
    'DimSecurity batches' as Description,
    NULL as Placeholder,
    case 
        when distinct_batches = 1 and max_batch_id = 1 
        then 'OK' 
        else 'Mismatch' 
    end as Status,
    'BatchID values must match Audit table' as Explanation
from (
    select 
        count(distinct BatchID) as distinct_batches,
        max(BatchID) as max_batch_id
    from DimSecurity
) d

union

select 
    'DimSecurity IsCurrent' as Description,
    NULL as Placeholder,
    case 
        when total_count = current_count + non_current_count 
        then 'OK' 
        else 'Not current' 
    end as Status,
    'IsCurrent is 1 if EndDate is the end of time, else IsCurrent is 0' as Explanation
from (
    select 
        count(*) as total_count,
        sum(case when EndDate = '9999-12-31' and IsCurrent = 1 then 1 else 0 end) as current_count,
        sum(case when EndDate < '9999-12-31' and IsCurrent = 0 then 1 else 0 end) as non_current_count
    from DimSecurity
) d


union

select 
    'DimSecurity EffectiveDate' as Description,
    a.BatchID,
    case 
        when count(case 
                   when ds.EffectiveDate < first_day.Date or ds.EffectiveDate > last_day.Date 
                   then 1 
                   else null 
               end) = 0 
        then 'OK' 
        else 'Data out of range' 
    end as Result,
    'All records from a batch have an EffectiveDate in the batch time window' as Explanation
from 
    Audit a
left join DimSecurity ds 
    on a.BatchID = ds.BatchID
left join Audit first_day 
    on first_day.DataSet = 'Batch' and first_day.Attribute = 'FirstDay' and first_day.BatchID = a.BatchID
left join Audit last_day 
    on last_day.DataSet = 'Batch' and last_day.Attribute = 'LastDay' and last_day.BatchID = a.BatchID
where a.BatchID in (1, 2, 3)
group by a.BatchID


union

select 
    'DimSecurity Status' as Description,
    NULL as Placeholder,
    case 
        when invalid_status_count = 0 
        then 'OK' 
        else 'Bad value' 
    end as Status,
    'All Status values are valid' as Explanation
from (
    select 
        count(*) as invalid_status_count
    from DimSecurity 
    where Status not in ('Active', 'Inactive')
) d

union

select 
    'DimSecurity SK_CompanyID' as Description,
    NULL as Placeholder,
    case 
        when total_count = valid_join_count 
        then 'OK' 
        else 'Bad join' 
    end as Status,
    'All SK_CompanyIDs match a DimCompany record with a valid date range' as Explanation
from (
    select 
        count(*) as total_count,
        count(*) as valid_join_count
    from DimSecurity a
    join DimCompany c 
        on a.SK_CompanyID = c.SK_CompanyID 
        and c.EffectiveDate <= a.EffectiveDate 
        and a.EndDate <= c.EndDate
) d

union

select 
    'DimSecurity ExchangeID' as Description,
    NULL as Placeholder,
    case 
        when invalid_exchange_count = 0 
        then 'OK' 
        else 'Bad value - see ticket #65' 
    end as Status,
    'All ExchangeID values are valid' as Explanation
from (
    select 
        count(*) as invalid_exchange_count
    from DimSecurity 
    where ExchangeID not in ('NYSE', 'NASDAQ', 'AMEX', 'PCX')
) d

union

select 
    'DimSecurity Issue' as Description,
    NULL as Placeholder,
    case 
        when invalid_issue_count = 0 
        then 'OK' 
        else 'Bad value - see ticket #65' 
    end as Status,
    'All Issue values are valid' as Explanation
from (
    select 
        count(*) as invalid_issue_count
    from DimSecurity 
    where Issue not in ('COMMON', 'PREF_A', 'PREF_B', 'PREF_C', 'PREF_D')
) d


--  
-- Checks against the DimCompany table.
--  
union

select 
    'DimCompany row count' as Description,
    a.BatchID,
    case 
        when cast(dm.MessageData as bigint) <= sum(aud.Value)
        then 'OK' 
        else 'Too few rows' 
    end as Result,
    'Actual row count matches or exceeds Audit table minimum' as Explanation
from 
    Audit a
left join DImessages dm 
    on dm.BatchID = a.BatchID 
    and dm.MessageType = 'Validation' 
    and dm.MessageSource = 'DimCompany' 
    and dm.MessageText = 'Row count'
left join Audit aud 
    on aud.DataSet = 'DimCompany' 
    and aud.Attribute = 'FW_CMP' 
    and aud.BatchID <= a.BatchID
where a.BatchID in (1, 2, 3)
group by a.BatchID, dm.MessageData


union

select 
    'DimCompany distinct keys' as Description,
    NULL as Placeholder,
    case 
        when distinct_count = total_count 
        then 'OK' 
        else 'Not unique' 
    end as Status,
    'All SKs are distinct' as Explanation
from (
    select 
        count(distinct SK_CompanyID) as distinct_count,
        count(*) as total_count
    from DimCompany
) d



-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimCompany EndDate' checks that effective and end dates line up
--   'DimCompany Overlap' checks that there are not records that overlap in time
--   'DimCompany End of Time' checks that every company has a final record that goes to 9999-12-31

union

select 
    'DimCompany EndDate' as Description,
    NULL as Placeholder,
    case 
        when total_count = (valid_join_count + end_of_time_count) 
        then 'OK' 
        else 'Dates not aligned' 
    end as Status,
    'EndDate of one record matches EffectiveDate of another, or the end of time' as Explanation
from (
    select 
        count(*) as total_count,
        count(*) as valid_join_count,
        count(*) as end_of_time_count
    from DimCompany a
    left join DimCompany b 
        on a.CompanyID = b.CompanyID 
        and a.EndDate = b.EffectiveDate
    where a.EndDate = '9999-12-31' or b.CompanyID is not null
) d


union

select 
    'DimCompany Overlap' as Description,
    NULL as Placeholder,
    case 
        when overlap_count = 0 
        then 'OK' 
        else 'Dates overlap' 
    end as Status,
    'Date ranges do not overlap for a given company' as Explanation
from (
    select 
        count(*) as overlap_count
    from DimCompany a
    join DimCompany b 
        on a.CompanyID = b.CompanyID 
        and a.SK_CompanyID <> b.SK_CompanyID 
        and a.EffectiveDate >= b.EffectiveDate 
        and a.EffectiveDate < b.EndDate
) d


union

select 
    'DimCompany End of Time' as Description,
    NULL as Placeholder,
    case 
        when distinct_company_count = end_of_time_count 
        then 'OK' 
        else 'End of time not reached' 
    end as Status,
    'Every company has one record with a date range reaching the end of time' as Explanation
from (
    select 
        count(distinct CompanyID) as distinct_company_count,
        count(*) as end_of_time_count
    from DimCompany
    where EndDate = '9999-12-31'
) d


union

select 
    'DimCompany consolidation' as Description,
    NULL as Placeholder,
    case 
        when consolidation_count = 0 
        then 'OK' 
        else 'Not consolidated' 
    end as Status,
    'No records become effective and end on the same day' as Explanation
from (
    select 
        count(*) as consolidation_count
    from DimCompany
    where EffectiveDate = EndDate
) d


union

select 
    'DimCompany batches' as Description,
    NULL as Placeholder,
    case 
        when distinct_batch_count = 1 and max_batch_id = 1
        then 'OK' 
        else 'Mismatch' 
    end as Status,
    'BatchID values must match Audit table' as Explanation
from (
    select 
        count(distinct BatchID) as distinct_batch_count,
        max(BatchID) as max_batch_id
    from DimCompany
) d

union

select 
    'DimCompany EffectiveDate' as Description,
    a.BatchID,
    case 
        when count(*) = 0 
        then 'OK' 
        else 'Data out of range - see ticket #71' 
    end as Result,
    'All records from a batch have an EffectiveDate in the batch time window' as Explanation
from DimCompany d
join Audit a 
    on d.BatchID = a.BatchID
    and a.DataSet = 'Batch' 
    and a.Attribute in ('FirstDay', 'LastDay')
left join (
    select BatchID, 
           max(case when Attribute = 'FirstDay' then Date end) as FirstDay,
           max(case when Attribute = 'LastDay' then Date end) as LastDay
    from Audit
    where BatchID in (1, 2, 3)
    group by BatchID
) audit_dates on d.BatchID = audit_dates.BatchID
where (d.EffectiveDate < audit_dates.FirstDay or d.EffectiveDate > audit_dates.LastDay)
group by a.BatchID


union

select 
    'DimCompany Status' as Description,
    NULL as Placeholder,
    case 
        when invalid_status_count = 0 
        then 'OK' 
        else 'Bad value' 
    end as Status,
    'All Status values are valid' as Explanation
from (
    select 
        count(*) as invalid_status_count
    from DimCompany
    where Status not in ('Active', 'Inactive')
) d


union

select 
    'DimCompany distinct names' as Description,
    NULL as Placeholder,
    case 
        when duplicate_names_count = 0 
        then 'OK' 
        else 'Mismatch' 
    end as Status,
    'Every company has a unique name' as Explanation
from (
    select 
        count(*) as duplicate_names_count
    from DimCompany a
    join DimCompany b 
        on a.Name = b.Name 
        and a.CompanyID <> b.CompanyID
) d


union				-- Curious, there are duplicate industry names in Industry table.  Should there be?  That's why the distinct stuff...

select 
    'DimCompany Industry' as Description,
    NULL as Placeholder,
    case 
        when count(*) = 0 
        then 'OK' 
        else 'Bad value' 
    end as Status,
    'Industry values are from the Industry table' as Explanation
from DimCompany d
left join Industry i 
    on d.Industry = i.IN_NAME
where i.IN_NAME is null


union

select 
    'DimCompany SPrating' as Description,
    NULL as Placeholder,
    case 
        when count(*) = 0 
        then 'OK' 
        else 'Bad value' 
    end as Status,
    'All SPrating values are valid' as Explanation
from DimCompany
where SPrating not in ('AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+','AA-','A-','BBB-','BB-','B-','CCC-')
  and SPrating is not null


union			-- Right now we have blank (but not null) country names.  Should there be?

select 
    'DimCompany Country' as Description,
    NULL as Placeholder,
    case 
        when count(*) = 0 
        then 'OK' 
        else 'Bad value' 
    end as Status,
    'All Country values are valid' as Explanation
from DimCompany
where Country not in ('Canada', 'United States of America', '')
  and Country is not null

--  
-- Checks against the Prospect table.
--  
union

select 
    'Prospect SK_UpdateDateID' as Description,
    NULL as Placeholder,
    case 
        when count(*) = 0 
        then 'OK' 
        else 'Mismatch' 
    end as Status,
    'SK_RecordDateID must be newer or same as SK_UpdateDateID' as Explanation
from Prospect
where SK_RecordDateID < SK_UpdateDateID


union

select 
    'Prospect SK_RecordDateID' as Description,
    BatchID,
    Result,
    'All records from batch have SK_RecordDateID in or after the batch time window' as Explanation
from (
    select distinct 
        a.BatchID,
        case 
            when count(*) = 0 
            then 'OK' 
            else 'Mismatch' 
        end as Result
    from Audit a
    left join Prospect p on p.BatchID = a.BatchID
    left join DimDate d on d.SK_DateID = p.SK_RecordDateID
    where a.BatchID in (1, 2, 3)
      and d.DateValue < (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
    group by a.BatchID
) o


union

select 
    'Prospect batches' as Description,
    NULL as Placeholder,
    case 
        when count(distinct BatchID) = 3 and max(BatchID) = 3 
        then 'OK' 
        else 'Mismatch' 
    end as Status,
    'BatchID values must match Audit table' as Explanation
from Prospect


 union

select 
    'Prospect Country' as Description,
    NULL as Placeholder,
    case 
        when count(*) = 0 
        then 'OK' 
        else 'Bad value' 
    end as Status,
    'All Country values are valid' as Explanation
from Prospect
where Country not in ('Canada', 'United States of America')
  and Country is not null


union		

select 
    'Prospect MarketingNameplate' as Description,
    NULL as Placeholder,
    case 
        when sum(case when (COALESCE(NetWorth, 0) > 1000000 or COALESCE(Income, 0) > 200000) and MarketingNameplate not like '%HighValue%' then 1 else 0 end) +
             sum(case when (COALESCE(NumberChildren, 0) > 3 or COALESCE(NumberCreditCards, 0) > 5) and MarketingNameplate not like '%Expenses%' then 1 else 0 end) +
             sum(case when (COALESCE(Age, 0) > 45) and MarketingNameplate not like '%Boomer%' then 1 else 0 end) +
             sum(case when (COALESCE(Income, 50000) < 50000 or COALESCE(CreditRating, 600) < 600 or COALESCE(NetWorth, 100000) < 100000) and MarketingNameplate not like '%MoneyAlert%' then 1 else 0 end) +
             sum(case when (COALESCE(NumberCars, 0) > 3 or COALESCE(NumberCreditCards, 0) > 7) and MarketingNameplate not like '%Spender%' then 1 else 0 end) +
             sum(case when (COALESCE(Age, 25) < 25 and COALESCE(NetWorth, 0) > 1000000) and MarketingNameplate not like '%Inherited%' then 1 else 0 end) +
             sum(case when COALESCE(MarketingNameplate, '') not in (
                 '','HighValue','Expenses','HighValue+Expenses','Boomer','HighValue+Boomer','Expenses+Boomer','HighValue+Expenses+Boomer','MoneyAlert','HighValue+MoneyAlert',
                 'Expenses+MoneyAlert','HighValue+Expenses+MoneyAlert','Boomer+MoneyAlert','HighValue+Boomer+MoneyAlert','Expenses+Boomer+MoneyAlert','HighValue+Expenses+Boomer+MoneyAlert',
                 'Spender','HighValue+Spender','Expenses+Spender','HighValue+Expenses+Spender','Boomer+Spender','HighValue+Boomer+Spender','Expenses+Boomer+Spender',
                 'HighValue+Expenses+Boomer+Spender','MoneyAlert+Spender','HighValue+MoneyAlert+Spender','Expenses+MoneyAlert+Spender','HighValue+Expenses+MoneyAlert+Spender',
                 'Boomer+MoneyAlert+Spender','HighValue+Boomer+MoneyAlert+Spender','Expenses+Boomer+MoneyAlert+Spender','HighValue+Expenses+Boomer+MoneyAlert+Spender','Inherited',
                 'HighValue+Inherited','Expenses+Inherited','HighValue+Expenses+Inherited','Boomer+Inherited','HighValue+Boomer+Inherited','Expenses+Boomer+Inherited',
                 'HighValue+Expenses+Boomer+Inherited','MoneyAlert+Inherited','HighValue+MoneyAlert+Inherited','Expenses+MoneyAlert+Inherited','HighValue+Expenses+MoneyAlert+Inherited',
                 'Boomer+MoneyAlert+Inherited','HighValue+Boomer+MoneyAlert+Inherited','Expenses+Boomer+MoneyAlert+Inherited','HighValue+Expenses+Boomer+MoneyAlert+Inherited',
                 'Spender+Inherited','HighValue+Spender+Inherited','Expenses+Spender+Inherited','HighValue+Expenses+Spender+Inherited','Boomer+Spender+Inherited',
                 'HighValue+Boomer+Spender+Inherited','Expenses+Boomer+Spender+Inherited','HighValue+Expenses+Boomer+Spender+Inherited','MoneyAlert+Spender+Inherited',
                 'HighValue+MoneyAlert+Spender+Inherited','Expenses+MoneyAlert+Spender+Inherited','HighValue+Expenses+MoneyAlert+Spender+Inherited','Boomer+MoneyAlert+Spender+Inherited',
                 'HighValue+Boomer+MoneyAlert+Spender+Inherited','Expenses+Boomer+MoneyAlert+Spender+Inherited','HighValue+Expenses+Boomer+MoneyAlert+Spender+Inherited'
             ) then 1 else 0 end) = 0
        then 'OK' 
        else 'Bad value' 
    end as Status,
    'All MarketingNameplate values match the data' as Explanation
from Prospect




--
-- Checks against the FactWatches table.
--

union

select 
    'FactWatches row count' as Description,
    BatchID, 
    Result, 
    'Actual row count matches Audit table' as Explanation
from (
    select distinct BatchID, (
        case when 
            cast((select MessageData from DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) as int) -
            cast((select MessageData from DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID-1) as int) =
            (select Value from Audit where DataSet = 'FactWatches' and Attribute = 'WH_ACTIVE' and BatchID = a.BatchID)
        then 'OK' else 'Mismatch' end
    ) as Result
    from Audit a 
    where BatchID in (1, 2, 3)
) o


union


select 
    'FactWatches batches' as Description,
    NULL as Placeholder,
    case 
        when count(distinct BatchID) = 3 and max(BatchID) = 3 then 'OK' 
        else 'Mismatch' 
    end as Result,
    'BatchID values must match Audit table' as Explanation
from FactWatches


union

select 
    'FactWatches active watches' as Description,
    a.BatchID,
    case 
        when dm1.RowCount + dm2.InactiveWatches = aud.WatchRecords then 'OK' 
        else 'Mismatch' 
    end as Result,
    'Actual total matches Audit table' as Explanation
from Audit a
left join (
    select BatchID, cast(MessageData as bigint) as RowCount
    from DImessages
    where MessageSource = 'FactWatches' 
      and MessageType = 'Validation' 
      and MessageText = 'Row count'
) dm1 on dm1.BatchID = a.BatchID
left join (
    select BatchID, cast(MessageData as bigint) as InactiveWatches
    from DImessages
    where MessageSource = 'FactWatches' 
      and MessageType = 'Validation' 
      and MessageText = 'Inactive watches'
) dm2 on dm2.BatchID = a.BatchID
left join (
    select BatchID, sum(Value) as WatchRecords
    from Audit
    where DataSet = 'FactWatches' 
      and Attribute = 'WH_RECORDS'
    group by BatchID
) aud on aud.BatchID = a.BatchID
where a.BatchID in (1, 2, 3)

 
union

select 
    'FactWatches SK_CustomerID' as Description,
    NULL as BatchID,
    case 
        when fw_count = join_count then 'OK' 
        else 'Bad join' 
    end as Result,
    'All SK_CustomerIDs match a DimCustomer record with a valid date range' as Explanation
from (
    select count(*) as fw_count 
    from FactWatches
) fw, (
    select count(*) as join_count
    from FactWatches a
    join DimCustomer c 
        on a.SK_CustomerID = c.SK_CustomerID
        and c.EffectiveDate <= (
            select DateValue 
            from DimDate 
            where SK_DateID = a.SK_DateID_DatePlaced
        )
        and (
            select DateValue 
            from DimDate 
            where SK_DateID = a.SK_DateID_DatePlaced
        ) <= c.EndDate
) j


union

select 
    'FactWatches SK_SecurityID' as Description, 
    NULL as BatchID,
    case when 
        (select count(*) 
         from FactWatches a) = 
        (select count(*) 
         from FactWatches a
         join DimSecurity c on a.SK_SecurityID = c.SK_SecurityID
         join DimDate d on d.SK_DateID = a.SK_DateID_DatePlaced
         where c.EffectiveDate <= d.DateValue
         and d.DateValue <= c.EndDate)
    then 'OK' 
    else 'Bad join' 
    end as Result, 
    'All SK_SecurityIDs match a DimSecurity record with a valid date range' as Explanation


union		

select 
    'FactWatches date check' as Description, 
    a.BatchID, 
    case when count(w.SK_DateID_DatePlaced) = 0 then 'OK' else 'Mismatch' end as Result,
    'All SK_DateID_ values are in the correct batch time window' as Explanation
from Audit a
join FactWatches w on w.BatchID = a.BatchID
join DimDate dPlaced on dPlaced.SK_DateID = w.SK_DateID_DatePlaced
left join DimDate dRemoved on dRemoved.SK_DateID = w.SK_DateID_DateRemoved
where a.BatchID in (1, 2, 3)
  and (
      (w.SK_DateID_DateRemoved is null and 
          (dPlaced.DateValue > (select Date from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) 
          or dPlaced.DateValue < (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)))
      or 
      (w.SK_DateID_DateRemoved is not null and 
          (dRemoved.DateValue > (select Date from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) 
          or dRemoved.DateValue < (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) 
          or dPlaced.DateValue > dRemoved.DateValue))
  )
group by a.BatchID


--  
-- Checks against the DimTrade table.
--  

union

select 
    'DimTrade row count' as Description, 
    a.BatchID, 
    case 
        when m.MessageData = s.total_value then 'OK' 
        else 'Mismatch' 
    end as Result,
    'Actual total matches Audit table' as Explanation
from Audit a
join DImessages m on m.BatchID = a.BatchID 
    and m.MessageSource = 'DimTrade' 
    and m.MessageType = 'Validation' 
    and m.MessageText = 'Row count'
left join (
    select BatchID, sum(Value) as total_value
    from Audit
    where DataSet = 'DimTrade' and Attribute = 'T_NEW'
    group by BatchID
) s on s.BatchID = a.BatchID
where a.BatchID in (1, 2, 3)


union 

select 
    'DimTrade canceled trades' as Description,
    case 
        when canceled_trades.count_canceled = audit.sum_value then 'OK' 
        else 'Mismatch' 
    end as Result,
    'Actual row counts match Audit table' as Explanation
from (
    select count(*) as count_canceled
    from DimTrade
    where Status = 'Canceled'
) canceled_trades
join (
    select sum(Value) as sum_value
    from Audit
    where DataSet = 'DimTrade' and Attribute = 'T_CanceledTrades'
) audit on 1=1


union 

select 
    'DimTrade commission alerts' as Description,
    case 
        when alert_count.count_invalid_trade_commission = audit.sum_value then 'OK' 
        else 'Mismatch' 
    end as Result,
    'Actual row counts match Audit table' as Explanation
from (
    select count(*) as count_invalid_trade_commission
    from DImessages
    where MessageType = 'Alert' and MessageText = 'Invalid trade commission'
) alert_count
join (
    select sum(Value) as sum_value
    from Audit
    where DataSet = 'DimTrade' and Attribute = 'T_InvalidCommision'
) audit on 1=1


union 

select 
    'DimTrade charge alerts' as Description,
    case 
        when charge_alert_count.count_invalid_trade_fee = audit.sum_value then 'OK' 
        else 'Mismatch' 
    end as Result,
    'Actual row counts match Audit table' as Explanation
from (
    select count(*) as count_invalid_trade_fee
    from DImessages
    where MessageType = 'Alert' and MessageText = 'Invalid trade fee'
) charge_alert_count
join (
    select sum(Value) as sum_value
    from Audit
    where DataSet = 'DimTrade' and Attribute = 'T_InvalidCharge'
) audit on 1=1


union

select 
    'DimTrade batches' as Description,
    case 
        when batch_count.distinct_batch_count = 3 and max_batch.max_batch_id = 3 then 'OK'
        else 'Mismatch' 
    end as Result,
    'BatchID values must match Audit table' as Explanation
from (
    select count(distinct BatchID) as distinct_batch_count
    from DimTrade
) batch_count
join (
    select max(BatchID) as max_batch_id
    from DimTrade
) max_batch on 1=1


union

select 
    'DimTrade distinct keys' as Description,
    case 
        when distinct_trade_count.distinct_trade_id_count = total_trade_count.total_trade_count then 'OK'
        else 'Not unique' 
    end as Result,
    'All keys are distinct' as Explanation
from (
    select count(distinct TradeID) as distinct_trade_id_count
    from DimTrade
) distinct_trade_count
join (
    select count(*) as total_trade_count
    from DimTrade
) total_trade_count on 1=1


union

select 
    'DimTrade SK_BrokerID' as Description,
    case 
        when count_all.count_all = count_joined.count_joined then 'OK'
        else 'Bad join' 
    end as Result,
    'All SK_BrokerIDs match a DimBroker record with a valid date range' as Explanation
from 
    (select count(*) as count_all from DimTrade) count_all
join 
    (select count(*) as count_joined
     from DimTrade a
     join DimBroker c on a.SK_BrokerID = c.SK_BrokerID
     join DimDate d on d.SK_DateID = a.SK_CreateDateID
     where c.EffectiveDate <= d.DateValue
     and d.DateValue <= c.EndDate) count_joined 
on 1 = 1


union

select 
    'DimTrade SK_CompanyID' as Description,
    case 
        when count_all.count_all = count_joined.count_joined then 'OK'
        else 'Bad join' 
    end as Result,
    'All SK_CompanyIDs match a DimCompany record with a valid date range' as Explanation
from 
    (select count(*) as count_all from DimTrade) count_all
join 
    (select count(*) as count_joined
     from DimTrade a
     join DimCompany c on a.SK_CompanyID = c.SK_CompanyID
     join DimDate d on d.SK_DateID = a.SK_CreateDateID
     where c.EffectiveDate <= d.DateValue
     and d.DateValue <= c.EndDate) count_joined 
on 1 = 1


union

select 
    'DimTrade SK_SecurityID' as Description,
    case 
        when count_all.count_all = count_joined.count_joined then 'OK'
        else 'Bad join' 
    end as Result,
    'All SK_SecurityIDs match a DimSecurity record with a valid date range' as Explanation
from 
    (select count(*) as count_all from DimTrade) count_all
join 
    (select count(*) as count_joined
     from DimTrade a
     join DimSecurity c on a.SK_SecurityID = c.SK_SecurityID
     join DimDate d on d.SK_DateID = a.SK_CreateDateID
     where c.EffectiveDate <= d.DateValue
     and d.DateValue <= c.EndDate) count_joined 
on 1 = 1


union

select 
    'DimTrade SK_CustomerID' as Description,
    case 
        when count_all.count_all = count_joined.count_joined then 'OK'
        else 'Bad join' 
    end as Result,
    'All SK_CustomerIDs match a DimCustomer record with a valid date range' as Explanation
from 
    (select count(*) as count_all from DimTrade) count_all
join 
    (select count(*) as count_joined
     from DimTrade a
     join DimCustomer c on a.SK_CustomerID = c.SK_CustomerID
     join DimDate d on d.SK_DateID = a.SK_CreateDateID
     where c.EffectiveDate <= d.DateValue
     and d.DateValue <= c.EndDate) count_joined 
on 1 = 1


union

select 
    'DimTrade SK_AccountID' as Description,
    case 
        when count_all.count_all = count_joined.count_joined then 'OK'
        else 'Bad join' 
    end as Result,
    'All SK_AccountIDs match a DimAccount record with a valid date range' as Explanation
from 
    (select count(*) as count_all from DimTrade) count_all
join 
    (select count(*) as count_joined
     from DimTrade a
     join DimAccount c on a.SK_AccountID = c.SK_AccountID
     join DimDate d on d.SK_DateID = a.SK_CreateDateID
     where c.EffectiveDate <= d.DateValue
     and d.DateValue <= c.EndDate) count_joined 
on 1 = 1


union

select 
    'DimTrade date check' as Description,
    a.BatchID,
    case 
        when count(*) = 0 then 'OK'
        else 'Mismatch'
    end as Result,
    'All SK_DateID values are in the correct batch time window' as Explanation
from 
    Audit a
join 
    DimTrade w on w.BatchID = a.BatchID
join 
    DimDate d_create on d_create.SK_DateID = w.SK_CreateDateID
left join 
    DimDate d_close on d_close.SK_DateID = w.SK_CloseDateID
join 
    Audit b on b.DataSet = 'Batch' and b.Attribute = 'LastDay' and b.BatchID = a.BatchID
join 
    Audit c on c.DataSet = 'Batch' and c.Attribute = 'FirstDay' and c.BatchID = a.BatchID
left join 
    Audit d on d.DataSet = 'Batch' and d.Attribute = 'FirstDay' and d.BatchID = 1
where 
    a.BatchID in (1, 2, 3)
    and (
        (w.SK_CloseDateID is null and (
            d_create.DateValue > b.Date or 
            d_create.DateValue < 
            case when w.Type like 'Limit%' then d.Date else c.Date end
        )) or 
        (w.SK_CloseDateID is not null and (
            d_close.DateValue > b.Date or 
            d_close.DateValue < c.Date or
            w.SK_CloseDateID < w.SK_CreateDateID
        ))
    )
group by a.BatchID


union

select 
    'DimTrade Status' as Description,
    NULL as Placeholder,
    case when count(*) = 0 then 'OK' else 'Bad value' end as Result,
    'All Trade Status values are valid' as Explanation
from 
    DimTrade
where 
    Status not in ('Canceled', 'Pending', 'Submitted', 'Active', 'Completed')


union

select 
    'DimTrade Type' as Description,
    NULL as Placeholder,
    case when count(*) = 0 then 'OK' else 'Bad value' end as Result,
    'All Trade Type values are valid' as Explanation
from 
    DimTrade
where 
    Type not in ('Market Buy', 'Market Sell', 'Stop Loss', 'Limit Sell', 'Limit Buy')



--  
-- Checks against the Financial table.
--  

union 

select 
    'Financial row count' as Description,
    NULL as Placeholder,
    case 
        when m.MessageData = a.FW_FIN then 'OK' 
        else 'Mismatch' 
    end as Result,
    'Actual row count matches Audit table' as Explanation
from 
    (select MessageData from DImessages where MessageSource = 'Financial' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = 1) m,
    (select sum(Value) as FW_FIN from Audit where DataSet = 'Financial' and Attribute = 'FW_FIN') a


union

select 
    'Financial SK_CompanyID' as Description,
    NULL as Placeholder,
    case 
        when count(f.SK_CompanyID) = count(c.SK_CompanyID) then 'OK' 
        else 'Bad join' 
    end as Result,
    'All SK_CompanyIDs match a DimCompany record' as Explanation
from 
    Financial f
join 
    DimCompany c 
    on f.SK_CompanyID = c.SK_CompanyID


union

select 
    'Financial FI_YEAR' as Description,
    NULL as Placeholder,
    case 
        when count(f.FI_YEAR) = 0 then 'OK'
        else 'Bad Year' 
    end as Result,
    'All Years are within Batch1 range' as Explanation
from 
    Financial f
join 
    Audit a 
    on f.FI_YEAR < year(a.Date) 
    and a.DataSet = 'Batch' 
    and a.BatchID = 1 
    and a.Attribute = 'FirstDay'
where 
    f.FI_YEAR > year((select Date from Audit where DataSet = 'Batch' and BatchID = 1 and Attribute = 'LastDay'))


union

select 
    'Financial FI_QTR' as Description,
    NULL as Placeholder,
    case 
        when count(f.FI_QTR) = 0 then 'OK'
        else 'Bad Qtr' 
    end as Result,
    'All quarters are in (1, 2, 3, 4)' as Explanation
from 
    Financial f
where 
    f.FI_QTR not in (1, 2, 3, 4)


union

select 
    'Financial FI_QTR_START_DATE' as Description,
    NULL as Placeholder,
    case 
        when count(f.FI_QTR_START_DATE) = 0 then 'OK'
        else 'Bad date' 
    end as Result,
    'All quarters start on correct date' as Explanation
from 
    Financial f
where 
    f.FI_YEAR <> year(f.FI_QTR_START_DATE)
    or month(f.FI_QTR_START_DATE) <> (f.FI_QTR - 1) * 3 + 1
    or day(f.FI_QTR_START_DATE) <> 1


union	

select 
    'Financial EPS' as Description,
    NULL as Placeholder,
    case 
        when count(f.FI_NET_EARN) = 0 then 'OK'
        else 'Bad EPS' 
    end as Result,
    'Earnings calculations are valid' as Explanation
from 
    Financial f
where 
    (Round(f.FI_NET_EARN/f.FI_OUT_BASIC, 2) - f.FI_BASIC_EPS) not between -0.4 and 0.4
    or (Round(f.FI_NET_EARN/f.FI_OUT_DILUT, 2) - f.FI_DILUT_EPS) not between -0.4 and 0.4
    or (Round(f.FI_NET_EARN/f.FI_REVENUE, 2) - f.FI_MARGIN) not between -0.4 and 0.4



--  
-- Checks against the FactMarketHistory table.
--  

union

select 
    'FactMarketHistory row count' as Description,
    a.BatchID,
    case 
        when cast(dm1.MessageData as int) - cast(dm2.MessageData as int) = am.Value 
        then 'OK' 
        else 'Mismatch' 
    end as Result,
    'Actual row count matches Audit table' as Explanation
from 
    Audit a
    join DImessages dm1 on dm1.MessageSource = 'FactMarketHistory' 
        and dm1.MessageType = 'Validation' 
        and dm1.MessageText = 'Row count' 
        and dm1.BatchID = a.BatchID
    join DImessages dm2 on dm2.MessageSource = 'FactMarketHistory' 
        and dm2.MessageType = 'Validation' 
        and dm2.MessageText = 'Row count' 
        and dm2.BatchID = a.BatchID - 1
    join Audit am on am.DataSet = 'FactMarketHistory' 
        and am.Attribute = 'DM_RECORDS' 
        and am.BatchID = a.BatchID
where 
    a.BatchID in (1, 2, 3)


union

select 
    'FactMarketHistory batches' as Description,
    case 
        when count(distinct f.BatchID) = 3 and max(f.BatchID) = 3 
        then 'OK' 
        else 'Mismatch' 
    end as Result,
    'BatchID values must match Audit table' as Explanation
from 
    FactMarketHistory f


union

select 
    'FactMarketHistory SK_CompanyID' as Description,
    case 
        when count(f.SK_CompanyID) = count(c.SK_CompanyID) 
        then 'OK' 
        else 'Bad join' 
    end as Result,
    'All SK_CompanyIDs match a DimCompany record with a valid date range' as Explanation
from 
    FactMarketHistory f
join 
    DimCompany c on f.SK_CompanyID = c.SK_CompanyID
join 
    DimDate d on d.SK_DateID = f.SK_DateID
where 
    c.EffectiveDate <= d.DateValue
    and d.DateValue <= c.EndDate


union

SELECT 
    'FactMarketHistory SK_SecurityID', 
    NULL, 
    CASE 
        WHEN COUNT(FMH.SK_SecurityID) = 
            COUNT(DISTINCT CASE 
                            WHEN DS.SK_SecurityID IS NOT NULL 
                            AND DS.EffectiveDate <= DD.DateValue
                            AND DD.DateValue <= DS.EndDate
                            THEN FMH.SK_SecurityID
                            END) 
        THEN 'OK' 
        ELSE 'Bad join' 
    END, 
    'All SK_SecurityIDs match a DimSecurity record with a valid date range'
FROM FactMarketHistory FMH
LEFT JOIN DimSecurity DS
    ON FMH.SK_SecurityID = DS.SK_SecurityID
LEFT JOIN DimDate DD
    ON DD.SK_DateID = FMH.SK_DateID
GROUP BY FMH.SK_SecurityID


union	

SELECT 
    'FactMarketHistory SK_DateID', 
    BatchID, 
    Result, 
    'All dates are within batch date range' 
FROM (
    SELECT DISTINCT 
        a.BatchID, 
        CASE 
            WHEN (
                COUNT(CASE 
                        WHEN DD.DateValue < (SELECT Date - 1 DAY FROM Audit WHERE DataSet = 'Batch' AND BatchID = a.BatchID AND Attribute = 'FirstDay')
                        THEN 1 
                    END) 
                + 
                COUNT(CASE 
                        WHEN DD.DateValue >= (SELECT Date FROM Audit WHERE DataSet = 'Batch' AND BatchID = a.BatchID AND Attribute = 'LastDay') 
                        THEN 1 
                    END)
            ) = 0 
            THEN 'OK' 
            ELSE 'Bad Date' 
        END AS Result
    FROM Audit a
    LEFT JOIN FactMarketHistory FMH ON FMH.BatchID = a.BatchID
    LEFT JOIN DimDate DD ON DD.SK_DateID = FMH.SK_DateID
    WHERE a.BatchID IN (1, 2, 3)
    GROUP BY a.BatchID
) o


union

SELECT 
    'FactMarketHistory relative dates', 
    NULL, 
    CASE 
        WHEN COUNT(CASE 
                    WHEN FiftyTwoWeekLow > DayLow 
                         OR DayLow > ClosePrice 
                         OR ClosePrice > DayHigh 
                         OR DayHigh > FiftyTwoWeekHigh 
                    THEN 1 
                END) = 0 
        THEN 'OK' 
        ELSE 'Bad Date' 
    END, 
    '52-week-low <= day_low <= close_price <= day_high <= 52-week-high'
FROM FactMarketHistory


--  
-- Checks against the FactHoldings table.
--  
union

SELECT 
    'FactHoldings row count' AS Description,
    a.BatchID,
    CASE 
        WHEN CAST(dm1.MessageData AS INT) - CAST(dm2.MessageData AS INT) = aud.Value THEN 'OK'
        ELSE 'Mismatch'
    END AS Result,
    'Actual row count matches Audit table' AS Status
FROM 
    Audit a
JOIN 
    DImessages dm1 ON dm1.MessageSource = 'FactHoldings' 
                    AND dm1.MessageType = 'Validation' 
                    AND dm1.MessageText = 'Row count'
                    AND dm1.BatchID = a.BatchID
JOIN 
    DImessages dm2 ON dm2.MessageSource = 'FactHoldings' 
                    AND dm2.MessageType = 'Validation' 
                    AND dm2.MessageText = 'Row count'
                    AND dm2.BatchID = a.BatchID - 1
JOIN 
    Audit aud ON aud.DataSet = 'FactHoldings' 
              AND aud.Attribute = 'HH_RECORDS' 
              AND aud.BatchID = a.BatchID
WHERE 
    a.BatchID IN (1, 2, 3)

  
union

SELECT 
    'FactHoldings batches' AS Description,
    NULL AS Placeholder,
    CASE 
        WHEN fh_count.BatchCount = 3 AND fh_max.BatchID = 3 THEN 'OK'
        ELSE 'Mismatch'
    END AS Result,
    'BatchID values must match Audit table' AS Status
FROM 
    (SELECT COUNT(DISTINCT BatchID) AS BatchCount FROM FactHoldings) fh_count,
    (SELECT MAX(BatchID) AS BatchID FROM FactHoldings) fh_max


union
/* It is possible that the dimension record has changed between orgination of the trade and the completion of the trade. *
 * So, we can check that the Effective Date of the dimension record is older than the the completion date, but the end date could be earlier or later than the completion date
 */

SELECT 
    'FactHoldings SK_CustomerID' AS Description,
    NULL AS Placeholder,
    CASE 
        WHEN fh_count.TotalCount = dim_count.TotalCount THEN 'OK'
        ELSE 'Bad join'
    END AS Result,
    'All SK_CustomerIDs match a DimCustomer record with a valid date range' AS Status
FROM 
    (SELECT COUNT(*) AS TotalCount FROM FactHoldings) fh_count,
    (SELECT COUNT(*) AS TotalCount 
     FROM FactHoldings a 
     JOIN DimCustomer c ON a.SK_CustomerID = c.SK_CustomerID 
     JOIN DimDate d ON c.EffectiveDate <= d.DateValue 
     AND d.SK_DateID = a.SK_DateID) dim_count


union

SELECT 
    'FactHoldings SK_AccountID' AS Description,
    NULL AS Placeholder,
    CASE 
        WHEN fh_count.TotalCount = dim_count.TotalCount THEN 'OK'
        ELSE 'Bad join'
    END AS Result,
    'All SK_AccountIDs match a DimAccount record with a valid date range' AS Status
FROM 
    (SELECT COUNT(*) AS TotalCount FROM FactHoldings) fh_count,
    (SELECT COUNT(*) AS TotalCount 
     FROM FactHoldings a 
     JOIN DimAccount c ON a.SK_AccountID = c.SK_AccountID 
     JOIN DimDate d ON c.EffectiveDate <= d.DateValue 
     AND d.SK_DateID = a.SK_DateID) dim_count


union

SELECT 'FactHoldings SK_CompanyID', NULL, 
       CASE 
           WHEN FactHoldingsCount = FactHoldingsWithValidJoinCount THEN 'OK' 
           ELSE 'Bad join' 
       END, 
       'All SK_CompanyIDs match a DimCompany record with a valid date range'
FROM (
    SELECT 
        (SELECT count(*) FROM FactHoldings) AS FactHoldingsCount,
        (SELECT count(*) 
         FROM FactHoldings a 
         JOIN DimCompany c 
             ON a.SK_CompanyID = c.SK_CompanyID 
            AND c.EffectiveDate <= (SELECT DateValue FROM DimDate WHERE SK_DateID = a.SK_DateID)
        ) AS FactHoldingsWithValidJoinCount
) AS counts


union

SELECT 'FactHoldings SK_SecurityID', NULL, 
       CASE 
           WHEN FactHoldingsCount = FactHoldingsWithValidJoinCount THEN 'OK' 
           ELSE 'Bad join' 
       END, 
       'All SK_SecurityIDs match a DimSecurity record with a valid date range'
FROM (
    SELECT 
        (SELECT count(*) FROM FactHoldings) AS FactHoldingsCount,
        (SELECT count(*) 
         FROM FactHoldings a 
         JOIN DimSecurity c 
             ON a.SK_SecurityID = c.SK_SecurityID 
            AND c.EffectiveDate <= (SELECT DateValue FROM DimDate WHERE SK_DateID = a.SK_DateID)
        ) AS FactHoldingsWithValidJoinCount
) AS counts


union

SELECT 'FactHoldings CurrentTradeID', NULL, 
       CASE 
           WHEN FactHoldingsCount = FactHoldingsWithValidJoinCount THEN 'OK' 
           ELSE 'Failed' 
       END, 
       'CurrentTradeID matches a DimTrade record with Close Date and Time are values are used as the holdings date and time'
FROM (
    SELECT 
        (SELECT count(*) FROM FactHoldings) AS FactHoldingsCount,
        (SELECT count(*) 
         FROM FactHoldings a 
         JOIN DimTrade t 
             ON a.CurrentTradeID = t.TradeID 
            AND a.SK_DateID = t.SK_CloseDateID 
            AND a.SK_TimeID = t.SK_CloseTimeID
        ) AS FactHoldingsWithValidJoinCount
) AS counts


union

SELECT 'FactHoldings SK_DateID', BatchID, Result, 'All dates are within batch date range'
FROM (
    SELECT DISTINCT a.BatchID,
           CASE 
               WHEN COUNT(CASE 
                           WHEN dh.DateValue < a1.Date THEN 1
                           ELSE NULL
                       END) +
                    COUNT(CASE 
                           WHEN dh.DateValue > a2.Date THEN 1
                           ELSE NULL
                       END) = 0 
               THEN 'OK' 
               ELSE 'Bad Date' 
           END AS Result
    FROM Audit a
    JOIN FactHoldings m ON a.BatchID = m.BatchID
    JOIN DimDate dh ON dh.SK_DateID = m.SK_DateID
    LEFT JOIN Audit a1 ON a1.BatchID = a.BatchID AND a1.DataSet = 'Batch' AND a1.Attribute = 'FirstDay'
    LEFT JOIN Audit a2 ON a2.BatchID = a.BatchID AND a2.DataSet = 'Batch' AND a2.Attribute = 'LastDay'
    WHERE a.BatchID IN (1, 2, 3)
    GROUP BY a.BatchID
) o


--  
-- Checks against the FactCashBalances table.
--  
union

SELECT 'FactCashBalances batches', NULL, 
       CASE 
           WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3 THEN 'OK' 
           ELSE 'Mismatch' 
       END, 
       'BatchID values must match Audit table'
FROM FactCashBalances
HAVING COUNT(DISTINCT BatchID) = 3 OR MAX(BatchID) = 3


union

SELECT 'FactCashBalances SK_CustomerID', NULL, 
       CASE 
           WHEN COUNT(*) = COUNT(DISTINCT a.SK_CustomerID) 
           THEN 'OK' 
           ELSE 'Bad join' 
       END, 
       'All SK_CustomerIDs match a DimCustomer record with a valid date range'
FROM FactCashBalances a
JOIN DimCustomer c ON a.SK_CustomerID = c.SK_CustomerID
JOIN DimDate d ON d.SK_DateID = a.SK_DateID
WHERE c.EffectiveDate <= d.DateValue
  AND d.DateValue <= c.EndDate


union
SELECT 'FactCashBalances SK_AccountID', NULL, 
       CASE 
           WHEN COUNT(*) = COUNT(DISTINCT a.SK_AccountID) 
           THEN 'OK' 
           ELSE 'Bad join' 
       END, 
       'All SK_AccountIDs match a DimAccount record with a valid date range'
FROM FactCashBalances a
JOIN DimAccount c ON a.SK_AccountID = c.SK_AccountID
JOIN DimDate d ON d.SK_DateID = a.SK_DateID
WHERE c.EffectiveDate <= d.DateValue
  AND d.DateValue <= c.EndDate

union
SELECT 'FactCashBalances SK_DateID', 
       BatchID, 
       Result, 
       'All dates are within batch date range'
FROM (
    SELECT DISTINCT a.BatchID, 
           CASE 
               WHEN ( 
                   COUNT(CASE WHEN d.DateValue < ad.FirstDay THEN 1 END) +
                   COUNT(CASE WHEN d.DateValue > ad.LastDay THEN 1 END)
               ) = 0
               THEN 'OK' 
               ELSE 'Bad Date' 
           END AS Result
    FROM Audit a
    JOIN DimDate d ON EXISTS (
        SELECT 1 
        FROM FactCashBalances m
        WHERE m.BatchID = a.BatchID 
        AND m.SK_DateID = d.SK_DateID
    )
    LEFT JOIN (
        SELECT BatchID, 
               MAX(CASE WHEN Attribute = 'FirstDay' THEN Date END) AS FirstDay,
               MAX(CASE WHEN Attribute = 'LastDay' THEN Date END) AS LastDay
        FROM Audit
        WHERE DataSet = 'Batch'
        GROUP BY BatchID
    ) ad ON ad.BatchID = a.BatchID
    WHERE a.BatchID IN (1, 2, 3)
    GROUP BY a.BatchID
) o


/*
 *  Checks against the Batch Validation Query row counts
 */

union
select 'Batch row count: ' || MessageSource, BatchID,
	case when RowsLastBatch > RowsThisBatch then 'Row count decreased' else 'OK' end,
	'Row counts do not decrease between successive batches'
from (
	select distinct(a.BatchID), m.MessageSource, cast(m1.MessageData as bigint) as RowsThisBatch, cast(m2.MessageData as bigint) as RowsLastBatch
	from Audit a 
	full join DImessages m on m.BatchID = 0 and m.MessageText = 'Row count' and m.MessageType = 'Validation'
	join DImessages m1 on m1.BatchID = a.BatchID   and m1.MessageSource = m.MessageSource and m1.MessageText = 'Row count' and m1.MessageType = 'Validation'
	join DImessages m2 on m2.BatchID = a.BatchID-1 and m2.MessageSource = m.MessageSource and m2.MessageText = 'Row count' and m2.MessageType = 'Validation'
	where a.BatchID in (1, 2, 3)
) o

union
select 'Batch joined row count: ' || MessageSource, BatchID,
	case when RowsJoined = RowsUnjoined then 'OK' else 'No match' end,
	'Row counts match when joined to dimensions'
from (
	select distinct(a.BatchID), m.MessageSource, cast(m1.MessageData as bigint) as RowsUnjoined, cast(m2.MessageData as bigint) as RowsJoined
	from Audit a 
	full join DImessages m on m.BatchID = 0 and m.MessageText = 'Row count' and m.MessageType = 'Validation'
	join DImessages m1 on m1.BatchID = a.BatchID and m1.MessageSource = m.MessageSource and m1.MessageText = 'Row count' and m1.MessageType = 'Validation'
	join DImessages m2 on m2.BatchID = a.BatchID and m2.MessageSource = m.MessageSource and m2.MessageText = 'Row count joined' and m2.MessageType = 'Validation'
	where a.BatchID in (1, 2, 3)
) o


/*
 *  Checks against the Data Visibility Query row counts
 */

union
select 'Data visibility row counts: ' || MessageSource , NULL as BatchID,
	case when regressions = 0 then 'OK' else 'Row count decreased' end,
	'Row counts must be non-decreasing over time'
from (
	select m1.MessageSource, sum( case when cast(m1.MessageData as bigint) > cast(m2.MessageData as bigint) then 1 else 0 end ) as regressions
	from DImessages m1 
	join DImessages m2 on 
		m2.MessageType IN ('Visibility_1', 'Visibility_2') and 
		m2.MessageText = 'Row count' and
		m2.MessageSource = m1.MessageSource and
		m2.MessageDateAndTime > m1.MessageDateAndTime
	where m1.MessageType IN ('Visibility_1', 'Visibility_2') and m1.MessageText = 'Row count'
	group by m1.MessageSource
) o

union
select 'Data visibility joined row counts: ' || MessageSource , NULL as BatchID,
	case when regressions = 0 then 'OK' else 'No match' end,
	'Row counts match when joined to dimensions'
from (
	select m1.MessageSource, sum( case when cast(m1.MessageData as bigint) > cast(m2.MessageData as bigint) then 1 else 0 end ) as regressions
	from DImessages m1 
	join DImessages m2 on 
		m2.MessageType = 'Visibility_1' and 
		m2.MessageText = 'Row count joined' and
		m2.MessageSource = m1.MessageSource and
		m2.MessageDateAndTime = m1.MessageDateAndTime
	where m1.MessageType = 'Visibility_1' and m1.MessageText = 'Row count'
	group by m1.MessageSource
) o

/* close the outer query */
) q
order by Test, Batch;

