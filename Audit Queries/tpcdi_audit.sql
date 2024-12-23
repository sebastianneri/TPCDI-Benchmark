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



--  
--  Checks against the Audit table.  If there is a problem with the Audit table, then other tests are suspect...
--  

-- Query 1
SELECT 
	'1' AS Query,
    'Audit table batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3
        THEN 'OK' 
        ELSE 'Not 3 batches' 
    END AS Result,
    'There must be audit data for 3 batches' AS Description
FROM Audit
GROUP BY 1, 2, 3, 5

union 

-- Query 2
SELECT 
	'2' AS Query,
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
GROUP BY 1, 2, 3, 5

--  
-- Checks against the DImessages table.
--  

union

-- Query 3
SELECT 
	'3' AS Query,
    'DImessages validation reports' AS Test,
    BatchID AS Batch,
    CASE 
        WHEN COUNT(*) = 24 THEN 'OK'
        ELSE 'Validation checks not fully reported' 
    END AS Result,
    'Every batch must have a full set of validation reports' AS Description
FROM DImessages
WHERE MessageType = 'Validation'
GROUP BY '3', 'DImessages validation reports', BatchID, 'Every batch must have a full set of validation reports'

union

-- Query 4
SELECT 
	'4' AS Query,
    'DImessages batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 4 AND MAX(BatchID) = 3
        THEN 'OK' 
        ELSE 'Not 3 batches plus batch 0' 
    END AS Result,
    'Must have 3 distinct batches reported in DImessages' AS Description
FROM DImessages
GROUP BY 1, 2, 3, 5

union

-- Query 5
SELECT 
	'5' AS Query,
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
GROUP BY '5', 'DImessages Phase complete records', NULL, 'Must have 4 Phase Complete records'

union

-- Query 6
SELECT 
	'6' AS Query,
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
  AND MessageSource IN ('DimAccount','DimBroker','DimCustomer','DimDate','DimSecurity','DimTime','DimTrade','FactCashBalances','FactHoldings',
	'FactMarketHistory','FactWatches','Financial','Industry','Prospect','StatusType','TaxRate','TradeType')
GROUP BY 1, 2, 3, 5

union

-- Query 7
SELECT 
	'7' AS Query,
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
GROUP BY '7', 'DImessages initial condition', NULL, 'All DW tables must be empty before Batch1'


--  
-- Checks against the DimBroker table.
--  
union

-- Query 8
SELECT 
	'8' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
	SELECT 
		'DimBroker row count' AS Test,
		NULL AS Batch,
		a.Value,
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
	GROUP BY 'DimBroker row count', NULL, a.Value, 'Actual row count matches Audit table'
)

union

-- Query 9
SELECT 
	'9' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM 
(
	SELECT 
		'DimBroker distinct keys' AS Test,
		NULL AS Batch,
		a.Value, 
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
	GROUP BY 'DimBroker distinct keys', NULL, a.Value, 'All SKs are distinct'
)

union

-- Query 10
SELECT 
	'10' AS Query,
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
GROUP BY '10', 'DimBroker BatchID', 1, 'All rows report BatchID = 1'

union

-- Query 11
SELECT 
	'11' AS Query,
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
GROUP BY '11', 'DimBroker IsCurrent', NULL, 'All rows have IsCurrent = 1'

union

-- Query 12
SELECT 
	'12' AS Query,
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
GROUP BY '12', 'DimBroker EffectiveDate', NULL, 'All rows have Batch1 BatchDate as EffectiveDate'

union

-- Query 13
SELECT 
	'13' AS Query,
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
GROUP BY '13', 'DimBroker EndDate', NULL, 'All rows have end of time as EndDate'
--  
-- Checks against the DimAccount table.
--  
union

-- Query 14
SELECT 
	'14' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
	SELECT 
		'DimAccount row count' AS Test,
		1 AS Batch,
		o.total_count,
		CASE 
			WHEN COUNT(*) > o.total_count
			THEN 'OK' 
			ELSE 'Too few rows' 
		END AS Result,
		'Actual row count matches or exceeds Audit table minimum' AS Description
	FROM DimAccount
	INNER JOIN (SELECT 
						COALESCE(SUM(CASE WHEN DataSet = 'DimCustomer' AND Attribute = 'C_NEW' THEN Value ELSE 0 END + 
						CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_ADDACCT' THEN Value ELSE 0 END + 
						CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_CLOSEACCT' THEN Value ELSE 0 END + 
						CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_UPDACCT' THEN Value ELSE 0 END + 
						CASE WHEN DataSet = 'DimCustomer' AND Attribute = 'C_UPDCUST' THEN Value ELSE 0 END + 
						CASE WHEN DataSet = 'DimCustomer' AND Attribute = 'C_INACT' THEN Value ELSE 0 END - 
						CASE WHEN DataSet = 'DimCustomer' AND Attribute = 'C_ID_HIST' THEN Value ELSE 0 END - 
						CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_ID_HIST' THEN Value ELSE 0 END), 0) AS total_count
				FROM Audit
				WHERE BatchID = 1) o ON 1=1
	WHERE BatchID = 1
	GROUP BY 'DimAccount row count', 1, o.total_count, 'Actual row count matches or exceeds Audit table minimum'
)
union

-- Query 15
SELECT 
	'15' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimAccount row count' AS Test,
			d.BatchID AS Batch,
			o.total_count,
			CASE 
				WHEN COUNT(d.*) >= o.total_count
				THEN 'OK' 
				ELSE 'Too few rows' 
			END AS Result,
			'Actual row count matches or exceeds Audit table minimum' AS Description
		FROM DimAccount d
		JOIN (
				SELECT 
						a.BatchID,
						COALESCE(SUM(
								CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_ADDACCT' THEN Value ELSE 0 END +
								CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_CLOSEACCT' THEN Value ELSE 0 END +
								CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_UPDACCT' THEN Value ELSE 0 END -
								CASE WHEN DataSet = 'DimAccount' AND Attribute = 'CA_ID_HIST' THEN Value ELSE 0 END), 0) AS total_count
				FROM Audit a
				WHERE a.BatchID IN (2, 3)
				GROUP BY a.BatchID
				) o ON o.BatchID = d.BatchID
		WHERE d.BatchID IN (2, 3)
		GROUP BY 'DimAccount row count', d.BatchID, o.total_count, 'Actual row count matches or exceeds Audit table minimum'
)

union

-- Query 16
SELECT 
	'16' AS Query,
    'DimAccount distinct keys' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT SK_AccountID) = COUNT(*) 
        THEN 'OK' 
        ELSE 'Not unique' 
    END AS Result,
    'All SKs are distinct' AS Description
FROM DimAccount
GROUP BY '16', 'DimAccount distinct keys', NULL, 'All SKs are distinct'


-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimAccount EndDate' checks that effective and end dates line up
--   'DimAccount Overlap' checks that there are not records that overlap in time
--   'DimAccount End of Time' checks that every company has a final record that goes to 9999-12-31

union

-- Query 17
SELECT 
	'17' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimAccount EndDate' AS Test,
			NULL AS Batch,
			f.effective_date_count,
			e.end_date_count,
			CASE 
				WHEN COUNT(d.*) = COALESCE(f.effective_date_count, 0) + COALESCE(e.end_date_count, 0)
				THEN 'OK' 
				ELSE 'Dates not aligned' 
			END AS Result,
			'EndDate of one record matches EffectiveDate of another, or the end of time' AS Description
		FROM DimAccount d
		LEFT JOIN (
			SELECT COUNT(*) AS effective_date_count
			FROM DimAccount a
			JOIN DimAccount b ON a.AccountID = b.AccountID AND a.EndDate = b.EffectiveDate
		) f ON TRUE
		LEFT JOIN (
			SELECT COUNT(*) AS end_date_count 
			FROM DimAccount 
			WHERE EndDate = '9999-12-31' 
		) e ON TRUE
		GROUP BY 'DimAccount EndDate', NULL, effective_date_count, end_date_count, 'EndDate of one record matches EffectiveDate of another, or the end of time'
)

union

-- Query 18
SELECT 
	'18' AS Query,
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
GROUP BY '18', 'DimAccount Overlap', NULL, 'Date ranges do not overlap for a given Account'

union

-- Query 19
SELECT 
	'19' AS Query,
    'DimAccount End of Time' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT a.AccountID) = COUNT(CASE WHEN a.EndDate = '9999-12-31' THEN a.AccountID END)
        THEN 'OK' 
        ELSE 'End of time not reached' 
    END AS Result,
    'Every Account has one record with a date range reaching the end of time' AS Description
FROM DimAccount a
GROUP BY '19', 'DimAccount End of Time', NULL, 'Every Account has one record with a date range reaching the end of time'

union

-- Query 20
SELECT 
	'20' AS Query,
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
GROUP BY '20', 'DimAccount consolidation', NULL, 'No records become effective and end on the same day'

union

-- Query 21
SELECT 
	'21' AS Query,
    'DimAccount batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM DimAccount
GROUP BY '21', 'DimAccount batches', NULL, 'BatchID values must match Audit table'

union

-- Query 22
SELECT 
	'22' AS Query,
    'DimAccount EffectiveDate' AS Test,
    d.BatchID AS Batch,
    CASE 
        WHEN  COUNT(a.*) = 0 
        THEN 'OK' 
        ELSE 'Data out of range - see ticket #71' 
    END AS Result,
    'All records from a batch have an EffectiveDate in the batch time window' AS Description
FROM DimAccount d
JOIN (
	SELECT 
			BatchID,
			CASE WHEN Attribute = 'FirstDay' THEN Date END AS First_Day_Date,
			CASE WHEN Attribute = 'LastDay' THEN Date END AS Last_Day_Date
	FROM Audit 
	WHERE DataSet = 'Batch'
	GROUP BY 1, 2, 3
) a ON d.BatchID = a.BatchID 
	AND (d.EffectiveDate < a.First_Day_Date 
		OR d.EffectiveDate > a.Last_Day_Date)
GROUP BY '22', 'DimAccount EffectiveDate', d.BatchID,  'All records from a batch have an EffectiveDate in the batch time window'

union

-- Query 23
SELECT 
	'23' AS Query,
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
GROUP BY '23', 'DimAccount IsCurrent', NULL, 'IsCurrent is 1 if EndDate is the end of time, else IsCurrent is 0'

union

-- Query 24
SELECT 
	'24' AS Query,
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
GROUP BY '24', 'DimAccount Status', NULL,  'All Status values are valid'

union

-- Query 25
SELECT 
	'25' AS Query,
    'DimAccount TaxStatus' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All TaxStatus values are valid' AS Description
FROM DimAccount
WHERE BatchID = 1 AND TaxStatus NOT IN (0, 1, 2)
GROUP BY '25', 'DimAccount TaxStatus', NULL, 'All TaxStatus values are valid'

union

-- Query 26
SELECT 
	'26' AS Query,
    'DimAccount SK_CustomerID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.*) = COUNT(c.*)
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_CustomerIDs match a DimCustomer record with a valid date range' AS Description
FROM DimAccount a
JOIN DimCustomer c 
    ON a.SK_CustomerID = c.SK_CustomerID 
    AND c.EffectiveDate <= a.EffectiveDate 
    AND a.EndDate <= c.EndDate
GROUP BY '26', 'DimAccount SK_CustomerID', NULL, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'


union

-- Query 27
SELECT 
	'27' AS Query,
    'DimAccount SK_BrokerID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.*) = COUNT(c.*)
        THEN 'OK' 
        ELSE 'Bad join - spec problem with DimBroker EffectiveDate values' 
    END AS Result,
    'All SK_BrokerIDs match a broker record with a valid date range' AS Description
FROM DimAccount a
JOIN DimBroker c 
    ON a.SK_BrokerID = c.SK_BrokerID 
    AND c.EffectiveDate <= a.EffectiveDate 
    AND a.EndDate <= c.EndDate
GROUP BY '27', 'DimAccount SK_BrokerID', NULL,  'All SK_BrokerIDs match a broker record with a valid date range'

union

-- Query 28
SELECT 
	'28' AS Query,
	'DimAccount inactive customers' AS Test,
	NULL AS Batch,
	CASE 
        WHEN COUNT(c.*) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'If a customer is inactive, the corresponding accounts must also have been inactive' AS Description
FROM DimCustomer c
LEFT JOIN DimAccount a on a.SK_CustomerID = c.SK_CustomerID 
WHERE a.Status = 'Inactive' AND c.Status='Inactive'
GROUP BY '28', 'DimAccount inactive customers', NULL, 'If a customer is inactive, the corresponding accounts must also have been inactive'

--  
-- Checks against the DimCustomer table.
--  
union

-- Query 29
SELECT 
	'29' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
	SELECT
			'DimCustomer row count' AS Test,
			d.BatchID AS Batch,
			o.total_count,
			CASE WHEN COUNT(d.*) >= o.total_count 
			THEN 'OK' 
			ELSE 'Too few rows' 
			END AS Result,
			'Actual row count matches or exceeds Audit table minimum' AS Description
	FROM DimCustomer d
	JOIN (
			SELECT 
					BatchID,
					COALESCE(SUM(
								(CASE WHEN DataSet = 'DimCustomer' and Attribute = 'C_NEW' THEN Value END) + 
								(CASE WHEN DataSet = 'DimCustomer' and Attribute = 'C_INACT' THEN Value END) +
								(CASE WHEN DataSet = 'DimCustomer' and Attribute = 'C_UPDCUST' THEN Value END) -
								(CASE WHEN DataSet = 'DimCustomer' and Attribute = 'C_ID_HIST' THEN Value END)), 0) AS total_count
			FROM Audit a
			WHERE a.BatchID in (1, 2, 3)
			GROUP BY BatchID) o ON d.BatchID = o.BatchID
	WHERE d.BatchID in (1, 2, 3)
	GROUP BY 'DimCustomer row count', d.BatchID, o.total_count, 'Actual row count matches or exceeds Audit table minimum'
)

union

-- Query 30
SELECT 
	'30' AS Query,
    'DimCustomer distinct keys' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT SK_CustomerID) = COUNT(*) 
        THEN 'OK' 
        ELSE 'Not unique' 
    END AS Result,
    'All SKs are distinct' AS Description
FROM DimCustomer
GROUP BY '30', 'DimCustomer distinct keys', NULL, 'All SKs are distinct'


-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimCustomer EndDate' checks that effective and end dates line up
--   'DimCustomer Overlap' checks that there are not records that overlap in time
--   'DimCustomer End of Time' checks that every company has a final record that goes to 9999-12-31

union

-- Query 31
SELECT 
	'31' AS Query,
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
GROUP BY '31', 'DimCustomer EndDate', NULL, 'EndDate of one record matches EffectiveDate of another, or the end of time'

union

-- Query 32
SELECT 
	'32' AS Query,
    'DimCustomer Overlap' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.*) = 0 
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
GROUP BY '32', 'DimCustomer Overlap', NULL, 'Date ranges do not overlap for a given Customer'


union

-- Query 33
SELECT 
	'33' AS Query,
    'DimCustomer End of Time' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT CustomerID) = COUNT(CASE WHEN EndDate = '9999-12-31' THEN 1 END) 
        THEN 'OK' 
        ELSE 'End of time not reached' 
    END AS Result,
    'Every Customer has one record with a date range reaching the end of time' AS Description
FROM DimCustomer
GROUP BY '33', 'DimCustomer End of Time', NULL, 'Every Customer has one record with a date range reaching the end of time'

union

-- Query 34
SELECT 
	'34' AS Query,
    'DimCustomer consolidation' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Not consolidated' 
    END AS Result,
    'No records become effective and end on the same day' AS Description
FROM DimCustomer
WHERE EffectiveDate = EndDate
GROUP BY '34', 'DimCustomer consolidation', NULL, 'No records become effective and end on the same day'

union

-- Query 35
SELECT 
	'35' AS Query,
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
GROUP BY '35', 'DimCustomer batches', NULL,  'BatchID values must match Audit table'

union

-- Query 36
SELECT 
	'36' AS Query,
    'DimCustomer IsCurrent' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 
						COUNT(CASE WHEN EndDate = '9999-12-31' and IsCurrent = 1 THEN 1 END) +
						COUNT(CASE WHEN EndDate < '9999-12-31' and IsCurrent = 0 THEN 1 END)
        THEN 'OK' 
        ELSE 'Not current' 
    END AS Result,
    'IsCurrent is 1 if EndDate is the end of time, else IsCurrent is 0' AS Description
FROM DimCustomer
GROUP BY '36', 'DimCustomer IsCurrent', NULL, 'IsCurrent is 1 if EndDate is the end of time, else IsCurrent is 0'


union

-- Query 37
SELECT 
	'37' AS Query,
    'DimCustomer EffectiveDate' AS Test,
    d.BatchID AS Batch,
    CASE 
        WHEN  COUNT(a.*) = 0 
        THEN 'OK' 
        ELSE 'Data out of range' 
    END AS Result,
    'All records from a batch have an EffectiveDate in the batch time window' AS Description
FROM DimCustomer d
JOIN (
	SELECT 
			BatchID,
			CASE WHEN Attribute = 'FirstDay' THEN Date END AS First_Day_Date,
			CASE WHEN Attribute = 'LastDay' THEN Date END AS Last_Day_Date
	FROM Audit 
	WHERE DataSet = 'Batch'
	GROUP BY 1, 2, 3
) a ON d.BatchID = a.BatchID 
	AND (d.EffectiveDate < a.First_Day_Date 
		OR d.EffectiveDate > a.Last_Day_Date)
GROUP BY '37', 'DimCustomer EffectiveDate', d.BatchID,  'All records from a batch have an EffectiveDate in the batch time window'

union

-- Query 38
SELECT 
	'38' AS Query,
    'DimCustomer Status' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(CASE WHEN Status NOT IN ('Active', 'Inactive') THEN 1 END) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All Status values are valid' AS Description
FROM DimCustomer
GROUP BY '38', 'DimCustomer Status', NULL, 'All Status values are valid'
union

-- Query 39
SELECT 
	'39' AS Query,
    'DimCustomer inactive customers' AS Test,
    a.BatchID AS Batch,
    CASE 
        WHEN SUM(CAST(m.MessageData AS bigint)) = SUM(au.Value)
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
WHERE a.BatchID IN (1, 2, 3)
GROUP BY '39', 'DimCustomer inactive customers', a.BatchID, 'Inactive customer count matches Audit table'

union

-- Query 40
SELECT 
	'40' AS Query,
    'DimCustomer Gender' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(CASE WHEN Gender NOT IN ('M', 'F', 'U') THEN 1 END) = 0
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All Gender values are valid' AS Description
FROM DimCustomer
GROUP BY '40', 'DimCustomer Gender', NULL, 'All Gender values are valid'

union

-- Query 41
SELECT 
	'41' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
	SELECT 
		'DimCustomer age range alerts' AS Test,
		dm.BatchID AS Batch,
		a.total_count,
		CASE 
			WHEN COUNT(DISTINCT CASE WHEN dm.MessageText = 'DOB out of range' THEN 1 END) = a.total_count
			THEN 'OK' 
			ELSE 'Mismatch' 
		END AS Result,
		'Count of age range alerts matches audit table' AS Description
	FROM DImessages dm
	JOIN (
			SELECT 
					BatchID, 
					SUM(CASE WHEN Attribute = 'C_DOB_TO' THEN Value END + CASE WHEN Attribute = 'C_DOB_TY' THEN Value END) AS total_count
				FROM Audit a 
				WHERE a.DataSet = 'DimCustomer' 
				AND BatchID IN (1, 2, 3)
				GROUP BY 1) a ON a.BatchID = dm.BatchID 
	WHERE 
		dm.MessageType = 'Alert' 
		AND dm.MessageText = 'DOB out of range' 
		AND dm.BatchID IN (1, 2, 3)
	GROUP BY 'DimCustomer age range alerts', dm.BatchID, a.total_count, 'Count of age range alerts matches audit table'
)

union

-- Query 42

SELECT 
	'42' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimCustomer customer tier alerts' AS Test,
			dm.BatchID AS Batch,
			a.Value,
			CASE 
				WHEN COUNT(*) = a.Value
				THEN 'OK' 
				ELSE 'Mismatch' 
			END AS Result,
			'Count of customer tier alerts matches audit table' AS Description
		FROM DImessages dm 
		LEFT JOIN Audit a ON a.DataSet = 'DimCustomer' 
							AND dm.BatchID = a.BatchID 
							AND a.Attribute = 'C_TIER_INV'
							AND a.BatchID IN (1, 2, 3)
		WHERE dm.MessageType = 'Alert' 
				AND dm.MessageText = 'Invalid customer tier'
		GROUP BY 'DimCustomer customer tier alerts',  dm.BatchID, a.Value, 'Count of customer tier alerts matches audit table'
)

union      

-- Query 43
SELECT 
	'43' AS Query,
    'DimCustomer TaxID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'TaxID values are properly formatted' AS Description
FROM DimCustomer
WHERE TaxID NOT LIKE '___-__-____'
GROUP BY '43', 'DimCustomer TaxID', NULL, 'TaxID values are properly formatted' 

union   

-- Query 44
SELECT 
	'44' AS Query,
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
GROUP BY '44', 'DimCustomer Phone1', NULL, 'Phone1 values are properly formatted'

union    

-- Query 45                 
SELECT 
	'45' AS Query,
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
GROUP BY '45', 'DimCustomer Phone2', NULL,  'Phone2 values are properly formatted'

union      

-- Query 46                  
SELECT 
	'46' AS Query,
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
GROUP BY '46', 'DimCustomer Phone3', NULL, 'Phone3 values are properly formatted'

union 

-- Query 47                   
SELECT 
	'47' AS Query,
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
GROUP BY '47', 'DimCustomer Email1', NULL, 'Email1 values are properly formatted'

union  

-- Query 48                  
SELECT 
	'48' AS Query,
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
GROUP BY '48', 'DimCustomer Email2', NULL, 'Email2 values are properly formatted' 

union  

-- Query 49                    
SELECT 
	'49' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimCustomer LocalTaxRate' AS Test,
			NULL AS Batch,
			t.tax_rate_count,
			CASE 
				WHEN COUNT(c.*) = t.tax_rate_count
					AND COUNT(DISTINCT c.LocalTaxRateDesc) > 300
				THEN 'OK' 
				ELSE 'Mismatch' 
			END AS Result,
			'LocalTaxRateDesc and LocalTaxRate values are from TaxRate table' AS Description
		FROM DimCustomer c
		JOIN (
			SELECT COUNT(t.*) AS tax_rate_count
			FROM DimCustomer c
			JOIN TaxRate t
				ON c.LocalTaxRateDesc = t.TX_NAME
				AND c.LocalTaxRate = t.TX_RATE
		) t ON TRUE
		GROUP BY 'DimCustomer LocalTaxRate', NULL, t.tax_rate_count, 'LocalTaxRateDesc and LocalTaxRate values are from TaxRate table'
)

union                 

-- Query 50
SELECT 
	'50' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimCustomer NationalTaxRate' AS Test,
			NULL AS Batch,
			t.tax_rate_count,
			CASE 
				WHEN COUNT(c.*) = t.tax_rate_count
					AND COUNT(DISTINCT c.NationalTaxRateDesc) >= 9
				THEN 'OK' 
				ELSE 'Mismatch' 
			END AS Result,
			'NationalTaxRateDesc and NationalTaxRate values are from TaxRate table' AS Description
		FROM DimCustomer c
		JOIN (
			SELECT COUNT(*) AS tax_rate_count FROM DimCustomer c 
			JOIN TaxRate t ON c.NationalTaxRateDesc = t.TX_NAME AND c.NationalTaxRate = t.TX_RATE
		) t ON TRUE
		GROUP BY 'DimCustomer NationalTaxRate', NULL, t.tax_rate_count, 'NationalTaxRateDesc and NationalTaxRate values are from TaxRate table'
)

union      

-- Query 51

SELECT 
	'51' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimCustomer demographic fields' AS Test,
			NULL AS Batch,
			prospect_total,
			CASE 
				WHEN COUNT(c.*) = prospect_total
				THEN 'OK' 
				ELSE 'Mismatch' 
			END AS Result,
			'For current customer records that match Prospect records, the demographic fields also match' AS Description
		FROM DimCustomer c
		JOIN (
				SELECT COUNT(*) prospect_total FROM DimCustomer c 
				JOIN Prospect p ON upper(c.FirstName || c.LastName || c.AddressLine1 || COALESCE(c.AddressLine2,'') || c.PostalCode)
								= upper(p.FirstName || p.LastName || p.AddressLine1 || COALESCE(p.AddressLine2,'') || p.PostalCode)
								AND COALESCE(c.CreditRating,0) = COALESCE(p.CreditRating,0) AND COALESCE(c.NetWorth,0) = COALESCE(p.NetWorth,0) AND COALESCE(c.MarketingNameplate, '') = COALESCE(p.MarketingNameplate,'')
								AND c.IsCurrent = 1
		) p ON TRUE
		WHERE c.AgencyID IS NOT NULL AND c.IsCurrent = 1
		GROUP BY 'DimCustomer demographic fields', NULL, prospect_total, 'For current customer records that match Prospect records, the demographic fields also match'
)
---------------------------------------------------------------
--  
-- Checks against the DimSecurity table.
--  

union

-- Query 52
SELECT 
	'52' AS Query,
    'DimSecurity row count' AS Test,
    a.BatchID AS Batch,
    CASE 
        WHEN SUM(CAST(d.MessageData AS bigint)) >= COALESCE(SUM(b.Value), 0) THEN 'OK'
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
GROUP BY '52', 'DimSecurity row count', a.BatchID, 'Actual row count matches or exceeds Audit table minimum'

union

-- Query 53
SELECT 
	'53' AS Query,
    'DimSecurity distinct keys' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT SK_SecurityID) = COUNT(*) THEN 'OK'
        ELSE 'Not unique'
    END AS Result,
    'All SKs are distinct' AS Description
FROM DimSecurity
GROUP BY '53', 'DimSecurity distinct keys', NULL, 'All SKs are distinct'

-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimSecurity EndDate' checks that effective and end dates line up
--   'DimSecurity Overlap' checks that there are not records that overlap in time
--   'DimSecurity End of Time' checks that every company has a final record that goes to 9999-12-31

union

-- Query 54
SELECT 
	'54' AS Query,
	'DimSecurity EndDate' as Test,
	NULL as Batch,
	CASE 
		WHEN 
			COUNT(a.*) =
			SUM(CASE WHEN a.EndDate = b.EffectiveDate THEN 1 ELSE 0 END) + 
			SUM(CASE WHEN a.EndDate = '9999-12-31' THEN 1 ELSE 0 END) 
		THEN 'OK' 
		ELSE 'Dates not aligned' 
	END AS Result, 
	'EndDate of one record matches EffectiveDate of another, or the end of time' AS Description
FROM DimSecurity a
LEFT JOIN DimSecurity b on a.Symbol = b.Symbol
GROUP BY '54', 'DimSecurity EndDate', NULL, 'EndDate of one record matches EffectiveDate of another, or the end of time'

union

-- Query 55
SELECT 
	'55' AS Query,
    'DimSecurity Overlap' AS Test, 
    NULL AS Batch, 
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK' 
        ELSE 'Dates overlap' 
    END AS Result, 
    'Date ranges do not overlap for a given company' AS Description
FROM DimSecurity a
JOIN DimSecurity b 
	ON a.Symbol = b.Symbol 
	AND a.SK_SecurityID <> b.SK_SecurityID 
	AND a.EffectiveDate >= b.EffectiveDate 
	AND a.EffectiveDate < b.EndDate
GROUP BY '55', 'DimSecurity Overlap', NULL,  'Date ranges do not overlap for a given company'

union

-- Query 56
SELECT 
	'56' AS Query,
    'DimSecurity End of Time' AS Test, 
    NULL AS Batch, 
    CASE 
        WHEN COUNT(DISTINCT Symbol) =
        	COUNT(CASE WHEN EndDate = '9999-12-31' THEN 1 END)  
		THEN 'OK' 
        ELSE 'End of time not reached' 
    END AS Result, 
    'Every company has one record with a date range reaching the end of time' AS Description
FROM DimSecurity
GROUP BY '56', 'DimSecurity End of Time', NULL,  'Every company has one record with a date range reaching the end of time'

union

-- Query 57
SELECT 
	'57' AS Query,
    'DimSecurity consolidation' AS Test, 
    NULL AS Batch, 
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK' 
        ELSE 'Not consolidated' 
    END AS Result, 
    'No records become effective and end on the same day' AS Description
FROM DimSecurity
WHERE EffectiveDate = EndDate
GROUP BY '57', 'DimSecurity consolidation', NULL, 'No records become effective and end on the same day'


union

-- Query 58
SELECT 
	'58' AS Query,
    'DimSecurity batches' AS Test, 
    NULL AS Batch, 
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 1 AND MAX(BatchID) = 1 THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result, 
    'BatchID values must match Audit table' AS Description
FROM DimSecurity
GROUP BY '58', 'DimSecurity batches', NULL, 'BatchID values must match Audit table' 


union

-- Query 59
SELECT 
	'59' AS Query,
    'DimSecurity IsCurrent' AS Test, 
    NULL AS Batch, 
    CASE 
        WHEN COUNT(*) = 
             SUM(CASE 
                     WHEN EndDate = '9999-12-31' AND IsCurrent = 1 THEN 1 
                     WHEN EndDate < '9999-12-31' AND IsCurrent = 0 THEN 1 
                     ELSE 0 
                 END) 
        THEN 'OK' 
        ELSE 'Not current' 
    END AS Result, 
    'IsCurrent is 1 if EndDate is the end of time, else IsCurrent is 0' AS Description
FROM DimSecurity
GROUP BY '59', 'DimSecurity IsCurrent', NULL, 'IsCurrent is 1 if EndDate is the end of time, else IsCurrent is 0'


union

-- Query 60
SELECT 
	'60' AS Query,
    'DimSecurity EffectiveDate' AS Test,
    d.BatchID AS Batch,
    CASE 
        WHEN  COUNT(a.*) = 0 
        THEN 'OK' 
        ELSE 'Data out of range' 
    END AS Result,
    'All records from a batch have an EffectiveDate in the batch time window' AS Description
FROM DimSecurity d
JOIN (
	SELECT 
			BatchID,
			CASE WHEN Attribute = 'FirstDay' THEN Date END AS First_Day_Date,
			CASE WHEN Attribute = 'LastDay' THEN Date END AS Last_Day_Date
	FROM Audit 
	WHERE DataSet = 'Batch'
	GROUP BY 1, 2, 3
) a ON d.BatchID = a.BatchID 
	AND (d.EffectiveDate < a.First_Day_Date 
		OR d.EffectiveDate > a.Last_Day_Date)
GROUP BY '60', 'DimSecurity EffectiveDate', d.BatchID,  'All records from a batch have an EffectiveDate in the batch time window'

union

-- Query 61

 SELECT 
 	'61' AS Query,
    'DimSecurity Status' AS Test, 
    NULL AS Batch, 
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result, 
    'All Status values are valid' AS Description
FROM DimSecurity
WHERE Status not in ('Active', 'Inactive')
GROUP BY '61', 'DimSecurity Status', NULL,  'All Status values are valid'


union

-- Query 62
SELECT 
 	'62' AS Query,
	'DimSecurity SK_CompanyID' AS Test, 
	NULL AS Batch,
	CASE 
        WHEN COUNT(c1.*) = COUNT(c2.SK_CompanyID)
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result, 
	'All SK_CompanyIDs match a DimCompany record with a valid date range' AS Description
FROM DimCompany c1
LEFT JOIN DimCompany c2 ON c1.SK_CompanyID = c2.SK_CompanyID and c2.EffectiveDate <= c1.EffectiveDate and c1.EndDate <= c2.EndDate
GROUP BY '62', 'DimSecurity SK_CompanyID', NULL, 'All SK_CompanyIDs match a DimCompany record with a valid date range'


union

-- Query 63
SELECT 
 	'63' AS Query,
    'DimSecurity ExchangeID' AS Test, 
    NULL AS Batch, 
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad value - see ticket #65' 
    END AS Result, 
    'All ExchangeID values are valid' AS Description
FROM DimSecurity
WHERE ExchangeID NOT IN ('NYSE', 'NASDAQ', 'AMEX', 'PCX')
GROUP BY '63', 'DimSecurity ExchangeID', NULL,  'All ExchangeID values are valid'

union

-- Query 64
 SELECT 
  	'64' AS Query,
    'DimSecurity Issue' AS Test, 
    NULL AS Batch, 
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad value - see ticket #65' 
    END AS Result, 
    'All Issue values are valid' AS Description
FROM DimSecurity
WHERE Issue NOT IN ('COMMON', 'PREF_A', 'PREF_B', 'PREF_C', 'PREF_D')
GROUP BY '64', 'DimSecurity Issue', NULL, 'All Issue values are valid'


--  
-- Checks against the DimCompany table.
--  

union

-- Query 65
SELECT 
 	'65' AS Query,
    'DimCompany row count' AS Test,
    a.BatchID AS Batch,	
    CASE 
        WHEN SUM(CAST(d.MessageData AS bigint)) <= COALESCE(SUM(b.Value), 0) THEN 'OK'
        ELSE 'Too few rows'
    END AS Result,
    'Actual row count matches or exceeds Audit table minimum' AS Description
FROM Audit a
JOIN DImessages d
    ON d.MessageType = 'Validation'
    AND d.BatchID = a.BatchID
    AND d.MessageSource = 'DimCompany'
    AND d.MessageText = 'Row count'
LEFT JOIN Audit b
    ON b.DataSet = 'DimCompany'
    AND b.Attribute = 'FW_CMP'
    AND b.BatchID <= a.BatchID
WHERE a.BatchID IN (1, 2, 3)
GROUP BY '65', 'DimCompany row count', a.BatchID, 'Actual row count matches or exceeds Audit table minimum'

 
union

-- Query 66
SELECT 
 	'66' AS Query,
    'DimCompany distinct keys' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT SK_CompanyID) = COUNT(*) 
        THEN 'OK' 
        ELSE 'Not unique' 
    END AS Result,
    'All SKs are distinct' AS Description
FROM DimCompany
GROUP BY '66', 'DimCompany distinct keys', NULL,  'All SKs are distinct'


-- Three tests together check for validity of the EffectiveDate and EndDate handling:
--   'DimCompany EndDate' checks that effective and end dates line up
--   'DimCompany Overlap' checks that there are not records that overlap in time
--   'DimCompany End of Time' checks that every company has a final record that goes to 9999-12-31

union

-- Query 67
SELECT 
 	'67' AS Query,
	'DimCompany EndDate' as Test,
	NULL as Batch,
	CASE 
		WHEN 
			COUNT(a.*) =
			SUM(CASE WHEN a.EndDate = b.EffectiveDate THEN 1 ELSE 0 END) + 
			SUM(CASE WHEN a.EndDate = '9999-12-31' THEN 1 ELSE 0 END) 
		THEN 'OK' 
		ELSE 'Dates not aligned' 
	END AS Result, 
	'EndDate of one record matches EffectiveDate of another, or the end of time' AS Description
FROM DimCompany a
LEFT JOIN DimCompany b on a.CompanyID = b.CompanyID
GROUP BY '67', 'DimCompany EndDate', NULL, 'EndDate of one record matches EffectiveDate of another, or the end of time'

union

-- Query 68
SELECT 
 	'68' AS Query,
    'DimCompany Overlap' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK'
        ELSE 'Dates overlap'
    END AS Result,
    'Date ranges do not overlap for a given company' AS Description
FROM 
    DimCompany a
JOIN 
    DimCompany b 
    ON a.CompanyID = b.CompanyID 
    AND a.SK_CompanyID <> b.SK_CompanyID 
    AND a.EffectiveDate >= b.EffectiveDate 
    AND a.EffectiveDate < b.EndDate
GROUP BY '68', 'DimCompany Overlap', NULL,  'Date ranges do not overlap for a given company'

union

-- Query 69
SELECT 
 	'69' AS Query,
    'DimCompany End of Time' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT CompanyID) = COUNT(CASE WHEN EndDate = '9999-12-31' THEN 1 END) 
        THEN 'OK'
        ELSE 'End of time not reached' 
    END AS Result,
    'Every company has one record with a date range reaching the end of time' AS Description
FROM DimCompany
GROUP BY '69', 'DimCompany End of Time', NULL, 'Every company has one record with a date range reaching the end of time'

union

-- Query 70
SELECT 
 	'70' AS Query,
    'DimCompany consolidation' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(CASE WHEN EffectiveDate = EndDate THEN 1 END) = 0 
        THEN 'OK' 
        ELSE 'Not consolidated' 
    END AS Result,
    'No records become effective and end on the same day' AS Description
FROM DimCompany
GROUP BY '70', 'DimCompany consolidation', NULL, 'No records become effective and end on the same day'

union

-- Query 71
SELECT 
 	'71' AS Query,
    'DimCompany batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 1 AND MAX(BatchID) = 1 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM DimCompany
GROUP BY '71', 'DimCompany batches', NULL, 'BatchID values must match Audit table'

union

-- Query 72
SELECT 
 	'72' AS Query,
    'DimCompany EffectiveDate' AS Test,
    d.BatchID AS Batch,
    CASE 
        WHEN  COUNT(a.*) = 0 
        THEN 'OK' 
        ELSE 'Data out of range' 
    END AS Result,
    'All records from a batch have an EffectiveDate in the batch time window' AS Description
FROM DimCompany d
JOIN (
	SELECT 
			BatchID,
			CASE WHEN Attribute = 'FirstDay' THEN Date END AS First_Day_Date,
			CASE WHEN Attribute = 'LastDay' THEN Date END AS Last_Day_Date
	FROM Audit 
	WHERE DataSet = 'Batch'
	GROUP BY 1, 2, 3
) a ON d.BatchID = a.BatchID 
	AND (d.EffectiveDate < a.First_Day_Date 
		OR d.EffectiveDate > a.Last_Day_Date)
GROUP BY '72', 'DimCompany EffectiveDate', d.BatchID,  'All records from a batch have an EffectiveDate in the batch time window'


union

-- Query 73
 SELECT 
  	'73' AS Query,
    'DimCompany Status' AS Test, 
    NULL AS Batch, 
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result, 
    'All Status values are valid' AS Description
FROM DimCompany
WHERE Status not in ('Active', 'Inactive')
GROUP BY '73', 'DimCompany Status', NULL,  'All Status values are valid'


union

-- Query 74
SELECT 
 	'74' AS Query,
    'DimCompany distinct names' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Every company has a unique name' AS Description
FROM 
    DimCompany a
JOIN DimCompany b ON a.Name = b.Name AND a.CompanyID <> b.CompanyID
GROUP BY '74', 'DimCompany distinct names', NULL, 'Every company has a unique name'


union				
-- Query 75
-- Curious, there are duplicate industry names in Industry table.  Should there be?  That's why the distinct stuff...

SELECT 
 	'75' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
	SELECT 
		'DimCompany Industry' AS Test,
		NULL AS Batch,
		o.total_count,
		CASE 
			WHEN COUNT(d.*) = o.total_count 
			THEN 'OK' 
			ELSE 'Bad value' 
		END AS Result,
		'Industry values are from the Industry table' AS Description
	FROM DimCompany d
	JOIN (
			SELECT 
				COUNT(*) as total_count
			FROM DimCompany d
			JOIN Industry i ON d.Industry = i.IN_NAME) o ON TRUE
	GROUP BY 'DimCompany Industry', NULL, o.total_count, 'Industry values are from the Industry table'
)

union

-- Query 76
SELECT 
 	'76' AS Query,
    'DimCompany SPrating' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All SPrating values are valid' AS Description
FROM DimCompany
WHERE SPrating NOT IN ('AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC', 'CC', 'C', 'D', 'AA+', 'A+', 'BBB+', 'BB+', 'B+', 'CCC+', 'AA-', 'A-', 'BBB-', 'BB-', 'B-','CCC-') 
		AND SPrating IS NOT NULL 
GROUP BY '76', 'DimCompany SPrating', NULL, 'All SPrating values are valid'



union			-- Right now we have blank (but not null) country names.  Should there be?

-- Query 77
SELECT 
 	'77' AS Query,
    'DimCompany Country' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All Country values are valid' AS Description
FROM DimCompany
WHERE Country NOT IN ('Canada', 'United States of America', '') AND Country IS NOT NULL
GROUP BY '77', 'DimCompany Country', NULL, 'All Country values are valid'


--  
-- Checks against the Prospect table.
--  
union

-- Query 78
SELECT 
 	'78' AS Query,
    'Prospect SK_UpdateDateID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'SK_RecordDateID must be newer or same as SK_UpdateDateID' AS Description
FROM Prospect
WHERE SK_RecordDateID < SK_UpdateDateID 
GROUP BY '78', 'Prospect SK_UpdateDateID' , NULL, 'SK_RecordDateID must be newer or same as SK_UpdateDateID'

union

-- Query 79
SELECT 
 	'79' AS Query,
	'Prospect SK_RecordDateID' AS Test,
	p.BatchID AS Batch, 
	CASE 
		WHEN COUNT(*) = 0
		THEN 'OK'
		ELSE 'Mismatch'
	END AS Result,
	'All records from batch have SK_RecordDateID in or after the batch time window' AS Description
FROM Prospect p
JOIN Audit a ON p.BatchID = a.BatchID AND a.DataSet = 'Batch' AND a.Attribute = 'FirstDay' 
JOIN DimDate d ON d.SK_DateID = p.SK_RecordDateID
WHERE d.DateValue < a.Date AND a.BatchID in (1, 2, 3)
GROUP BY '79', 'Prospect SK_RecordDateID', p.BatchID, 'All records from batch have SK_RecordDateID in or after the batch time window'

union

-- Query 80
SELECT 
 	'80' AS Query,
    'Prospect batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM Prospect
GROUP BY '80', 'Prospect batches', NULL, 'BatchID values must match Audit table'

 union

-- Query 81
SELECT 
 	'81' AS Query,
    'Prospect Country' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 
        THEN 'OK' 
        ELSE 'Bad value' 
    END AS Result,
    'All Country values are valid' AS Description
FROM Prospect
WHERE Country NOT IN ('Canada', 'United States of America', '') AND Country IS NOT NULL
GROUP BY '81', 'Prospect Country', NULL, 'All Country values are valid'


union	

-- Query 82
SELECT 
 	'82' AS Query,
	'Prospect MarketingNameplate' AS Test, 
	NULL AS Batch, 
	CASE 
		WHEN (
				sum(case when (COALESCE(NetWorth,0) > 1000000 or COALESCE(Income,0) > 200000) and MarketingNameplate not like '%HighValue%' then 1 else 0 end) +
				sum(case when (COALESCE(NumberChildren,0) > 3 or COALESCE(NumberCreditCards,0) > 5) and MarketingNameplate not like '%Expenses%' then 1 else 0 end) +
				sum(case when (COALESCE(Age,0) > 45) and MarketingNameplate not like '%Boomer%' then 1 else 0 end) +
				sum(case when (COALESCE(Income,50000) < 50000 or COALESCE(CreditRating,600) < 600 or COALESCE(NetWorth,100000) < 100000) and MarketingNameplate not like '%MoneyAlert%' then 1 else 0 end) +
				sum(case when (COALESCE(NumberCars,0) > 3 or COALESCE(NumberCreditCards,0) > 7) and MarketingNameplate not like '%Spender%' then 1 else 0 end) +
				sum(case when (COALESCE(Age,25) < 25 and COALESCE(NetWorth,0) > 1000000) and MarketingNameplate not like '%Inherited%' then 1 else 0 end) +
				sum(case when COALESCE(MarketingNameplate, '') not in (   -- Technically, a few of these combinations cannot really happen
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
					) then 1 else 0  end)
		) = 0
		THEN 'OK' 
		ELSE 'Bad value' END AS Result, 
		'All MarketingNameplate values match the data' AS Description
FROM Prospect
GROUP BY '82', 'Prospect MarketingNameplate', NULL, 'All MarketingNameplate values match the data'

--
-- Checks against the FactWatches table.
--

union

-- Query 83
SELECT
	'83' AS Query,
	'FactWatches row count' AS Test,
	a.BatchID AS Batch,
	CASE 
		WHEN m.total = a.Value 
		THEN 'OK'
		ELSE 'Mismatch'
	END AS Result,
	'Actual row count matches Audit table' AS Description
FROM Audit a
JOIN (
		SELECT
				d.BatchID,
				CAST(CASE WHEN d.BatchID = a.BatchID THEN MessageData END AS INT) - 
				CAST(CASE WHEN d.BatchID = pa.BatchID THEN MessageData END AS INT) AS total
		FROM DImessages d
		JOIN Audit a ON d.BatchID = a.BatchID
		JOIN Audit pa ON d.BatchID = pa.BatchID - 1
		WHERE 
				MessageSource = 'FactWatches' 
				AND MessageType = 'Validation' 
				AND MessageText = 'Row count'
		GROUP BY 1, 2
) m ON m.BatchID = a.BatchID
WHERE a.BatchID in (1, 2, 3) AND a.DataSet = 'FactWatches' and a.Attribute = 'WH_ACTIVE'


union

-- Query 84
SELECT 
	'84' AS Query,
    'FactWatches batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM FactWatches
GROUP BY '84', 'FactWatches batches', NULL, 'BatchID values must match Audit table'


union

-- Query 85

SELECT 
	'85' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT
				'FactWatches active watches' AS Test,
				a.BatchID AS Batch,
				m.total,
				CASE 
					WHEN m.total = SUM(a2.Value)
					THEN 'OK'
					ELSE 'Mismatch'
				END AS Result,
				'Actual total matches Audit table' AS Description
		FROM Audit a
		JOIN (
				SELECT
						BatchID,
						CAST(CASE WHEN MessageText = 'Row count' THEN MessageData END AS INT) +
						CAST(CASE WHEN MessageText = 'Inactive watches' THEN MessageData END AS INT) AS total
				FROM DImessages d
				WHERE 
						MessageSource = 'FactWatches' 
						AND MessageType = 'Validation' 
				GROUP BY 1, 2
		) m ON m.BatchID = a.BatchID
		JOIN Audit a2 ON a2.BatchID <= a.BatchID AND a2.DataSet = 'FactWatches' and a2.Attribute = 'WH_RECORDS'
		WHERE a.BatchID in (1, 2, 3) 
		GROUP BY 'FactWatches active watches', a.BatchID, m.total, 'Actual total matches Audit table'
)
 
union

-- Query 86
SELECT 
	'86' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'FactWatches SK_CustomerID' AS Test,
			NULL AS Batch,
			o.total,
			CASE 
				WHEN COUNT(a.*) = o.total
				THEN 'OK' 
				ELSE 'Bad join' 
			END AS Result,
			'All SK_CustomerIDs match a DimCustomer record with a valid date range' AS Description
		FROM FactWatches a
		JOIN (
			SELECT COUNT(*) AS total
			FROM FactWatches a
			JOIN DimDate d ON d.SK_DateID = a.SK_DateID_DatePlaced
			JOIN DimCustomer c ON a.SK_CustomerID = c.SK_CustomerID 
					AND c.EffectiveDate <= d.DateValue 
					AND d.DateValue <= c.EndDate
		) o ON TRUE
		GROUP BY 'FactWatches SK_CustomerID', NULL, o.total, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
)
union

-- Query 87
SELECT 
	'87' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'FactWatches SK_SecurityID' AS Test,
			NULL AS Batch,
			o.total,
			CASE 
				WHEN COUNT(a.*) = o.total
				THEN 'OK' 
				ELSE 'Bad join' 
			END AS Result,
			'All SK_SecurityIDs match a DimSecurity record with a valid date range' AS Description
		FROM FactWatches a
		JOIN (
			SELECT COUNT(*) AS total
			FROM FactWatches a
			JOIN DimDate d ON d.SK_DateID = a.SK_DateID_DatePlaced
			JOIN DimSecurity c ON a.SK_SecurityID = c.SK_SecurityID 
					AND c.EffectiveDate <= d.DateValue 
					AND d.DateValue <= c.EndDate
		) o ON TRUE
		GROUP BY 'FactWatches SK_SecurityID', NULL, o.total, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
)

union	

-- Query 88
SELECT 
	'88' AS Query,
	'FactWatches date check' AS Test, 
	w.BatchID AS Batch,
	CASE 
		WHEN COUNT(*) = 0
		THEN 'OK'
		ELSE 'Mismatch'
	END AS Result,
	'All SK_DateID_ values are in the correct batch time window' AS Description
FROM FactWatches w 
JOIN DimDate dp ON dp.SK_DateID = w.SK_DateID_DatePlaced
JOIN DimDate dr ON dr.SK_DateID = w.SK_DateID_DateRemoved
JOIN Audit a ON w.BatchID = a.BatchID AND a.BatchID in (1, 2, 3)
JOIN Audit la ON la.DataSet = 'Batch' AND la.Attribute = 'LastDay' AND w.BatchID = la.BatchID
JOIN Audit fa ON fa.DataSet = 'Batch' AND fa.Attribute = 'FirstDay' AND w.BatchID = fa.BatchID
WHERE w.SK_DateID_DateRemoved IS NULL AND (dp.DateValue > la.Date 
											OR dp.DateValue < fa.Date)
		OR w.SK_DateID_DateRemoved IS NOT NULL AND (dr.DateValue > la.Date 
													OR dr.DateValue < fa.Date 
													OR SK_DateID_DatePlaced > SK_DateID_DateRemoved)
GROUP BY '88', 'FactWatches date check', w.BatchID, 'All SK_DateID_ values are in the correct batch time window'
--  
-- Checks against the DimTrade table.
--  

union


-- Query 89
SELECT 
	'89' AS Query,
    'DimTrade row count' AS Test,
    o.BatchID AS Batch,
    CASE 
        WHEN o.MessageData = o.SumValue 
		THEN 'OK'
        ELSE 'Mismatch' 
    END AS Result,
    'Actual total matches Audit table' AS Description
FROM (

    SELECT DISTINCT 
        a.BatchID,
        dm.MessageData,
        SUM(ab.Value) AS SumValue
    FROM Audit a
    JOIN DImessages dm ON dm.BatchID = a.BatchID 
        AND dm.MessageSource = 'DimTrade' 
        AND dm.MessageType = 'Validation' 
        AND dm.MessageText = 'Row count'
    LEFT JOIN 
        Audit ab ON ab.DataSet = 'DimTrade' 
        AND ab.Attribute = 'T_NEW' 
        AND ab.BatchID <= a.BatchID
    WHERE a.BatchID IN (1, 2, 3)
    GROUP BY a.BatchID, dm.MessageData
) o


union 

-- Query 90
SELECT 
	'90' AS Query,
    'DimTrade canceled trades' AS Test,
	NULL AS Batch,
    CASE 
        WHEN COUNT(dt.*) = SUM(a.Value) THEN 'OK'
        ELSE 'Mismatch'
    END AS Result,
    'Actual row counts matches Audit table' AS Description
FROM DimTrade dt
JOIN Audit a ON a.DataSet = 'DimTrade' AND a.Attribute = 'T_CanceledTrades'
WHERE Status = 'Canceled'
GROUP BY '90', 'DimTrade canceled trades', NULL, 'Actual row counts matches Audit table'

union 

-- Query 91
SELECT 
	'91' AS Query,
    'DimTrade commission alerts' AS Test,
	NULL AS Batch,
    CASE 
        WHEN COUNT(dm.*) = SUM(a.Value) THEN 'OK'
        ELSE 'Mismatch'
    END AS Result,
    'Actual row counts matches Audit table' AS Description
FROM DImessages dm
JOIN Audit a ON a.DataSet = 'DimTrade' 
            AND a.Attribute = 'T_InvalidCommision'
            AND dm.MessageType = 'Alert'
            AND dm.MessageText = 'Invalid trade commission'
GROUP BY '91', 'DimTrade commission alerts', NULL, 'Actual row counts matches Audit table' 


union 

-- Query 92
SELECT 
	'92' AS Query,
    'DimTrade charge alerts' AS Test,
	NULL AS Batch,
    CASE 
        WHEN COUNT(dm.*) = SUM(a.Value) THEN 'OK'
        ELSE 'Mismatch'
    END AS Result,
    'Actual row counts matches Audit table' AS Description
FROM DImessages dm
JOIN Audit a ON a.DataSet = 'DimTrade' 
            AND a.Attribute = 'T_InvalidCharge'
            AND dm.MessageType = 'Alert'
            AND dm.MessageText = 'Invalid trade fee'
GROUP BY '92', 'DimTrade charge alerts', NULL, 'Actual row counts matches Audit table'


union

-- Query 93
SELECT 
	'93' AS Query,
    'DimTrade batches' AS Test,
	NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3 THEN 'OK'
        ELSE 'Mismatch'
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM DimTrade
GROUP BY '93', 'DimTrade batches', NULL, 'BatchID values must match Audit table'

union

-- Query 94
SELECT
	'94' AS Query, 
    'DimTrade distinct keys' AS Test,
	NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT TradeID) = COUNT(*) THEN 'OK'
        ELSE 'Not unique'
    END AS Result,
    'All keys are distinct' AS Description
FROM DimTrade
GROUP BY '94', 'DimTrade distinct keys', NULL, 'All keys are distinct'

union

-- Query 95
SELECT 
	'95' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
	SELECT 
		'DimTrade SK_BrokerID' AS Test,
		NULL AS Batch,
		o.total,
		CASE 
			WHEN COUNT(*) = o.total
			THEN 'OK'
			ELSE 'Bad join'
		END AS Result,
		'All SK_BrokerIDs match a DimBroker record with a valid date range' AS Description
	FROM DimTrade
	JOIN (
			SELECT COUNT(*) AS total
			FROM DimTrade a
			JOIN DimBroker c ON a.SK_BrokerID = c.SK_BrokerID
			JOIN DimDate ON SK_DateID = a.SK_CreateDateID
			WHERE c.EffectiveDate <= DateValue
					AND DateValue <= c.EndDate
	) o ON TRUE
	GROUP BY 'DimTrade SK_BrokerID', NULL, o.total, 'All SK_BrokerIDs match a DimBroker record with a valid date range'
)

union

-- Query 96
SELECT 
	'96' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimTrade SK_CompanyID' AS Test,
			NULL AS Batch,
			o.total,
			CASE 
				WHEN COUNT(*) = o.total
				THEN 'OK'
				ELSE 'Bad join'
			END AS Result,
			'All SK_CompanyIDs match a DimCompany record with a valid date range' AS Description
		FROM DimTrade
		JOIN (
				SELECT COUNT(*) AS total
				FROM DimTrade a
				JOIN DimCompany c ON a.SK_CompanyID = c.SK_CompanyID
				JOIN DimDate ON SK_DateID = a.SK_CreateDateID
				WHERE c.EffectiveDate <= DateValue
						AND DateValue <= c.EndDate
		) o ON TRUE
		GROUP BY 'DimTrade SK_CompanyID', NULL, o.total, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
)

union

-- Query 97
SELECT 
	'97' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimTrade SK_SecurityID' AS Test,
			NULL AS Batch,
			o.total,
			CASE 
				WHEN COUNT(*) = o.total
				THEN 'OK'
				ELSE 'Bad join'
			END AS Result,
			'All SK_SecurityIDs match a DimSecurity record with a valid date range' AS Description
		FROM DimTrade
		JOIN (
				SELECT COUNT(*) AS total
				FROM DimTrade a
				JOIN DimSecurity c ON a.SK_SecurityID = c.SK_SecurityID
				JOIN DimDate ON SK_DateID = a.SK_CreateDateID
				WHERE c.EffectiveDate <= DateValue
						AND DateValue <= c.EndDate
		) o ON TRUE
		GROUP BY 'DimTrade SK_SecurityID', NULL, o.total, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
)
union

-- Query 98
SELECT 
	'98' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimTrade SK_CustomerID' AS Test,
			NULL AS Batch,
			o.total,
			CASE 
				WHEN COUNT(*) = o.total
				THEN 'OK'
				ELSE 'Bad join'
			END AS Result,
			'All SK_CustomerIDs match a DimCustomer record with a valid date range' AS Description
		FROM DimTrade
		JOIN (
				SELECT COUNT(*) AS total
				FROM DimTrade a
				JOIN DimCustomer c ON a.SK_CustomerID = c.SK_CustomerID
				JOIN DimDate ON SK_DateID = a.SK_CreateDateID
				WHERE c.EffectiveDate <= DateValue
						AND DateValue <= c.EndDate
		) o ON TRUE
		GROUP BY 'DimTrade SK_CustomerID', NULL, o.total, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
)


union
-- Query 99
SELECT 
	'99' AS Query,
	Test,
	Batch,
	Result,
	Description
FROM (
		SELECT 
			'DimTrade SK_AccountID' AS Test,
			NULL AS Batch,
			CASE 
				WHEN COUNT(*) = o.total
				THEN 'OK'
				ELSE 'Bad join'
			END AS Result,
			'All SK_AccountIDs match a DimAccount record with a valid date range' AS Description
		FROM DimTrade
		JOIN (
				SELECT COUNT(*) AS total
				FROM DimTrade a
				JOIN DimAccount c ON a.SK_AccountID = c.SK_AccountID
				JOIN DimDate ON SK_DateID = a.SK_CreateDateID
				WHERE c.EffectiveDate <= DateValue
						AND DateValue <= c.EndDate
		) o ON TRUE
		GROUP BY 'DimTrade SK_AccountID', NULL, o.total, 'All SK_AccountIDs match a DimAccount record with a valid date range'
)

union

-- Query 100
SELECT 
	'100' AS Query,
	'DimTrade date check' AS Test,
	a.BatchID AS Batch,
	CASE 
		WHEN COUNT(*) = 0 THEN 'OK'
		ELSE 'Mismatch'
	END AS Result,
	'All SK_DateID values are in the correct batch time window' AS Description
FROM DimTrade w
JOIN Audit a ON w.BatchID = a.BatchID
LEFT JOIN DimDate cd ON w.SK_CreateDateID = cd.SK_DateID
LEFT JOIN DimDate cld ON w.SK_CloseDateID = cld.SK_DateID
LEFT JOIN Audit ad_last ON a.BatchID = ad_last.BatchID AND ad_last.DataSet = 'Batch' AND ad_last.Attribute = 'LastDay'
LEFT JOIN Audit ad_first ON a.BatchID = ad_first.BatchID AND ad_first.DataSet = 'Batch' AND ad_first.Attribute = 'FirstDay'
LEFT JOIN Audit ad_first_batch1 ON ad_first_batch1.BatchID = 1 AND ad_first_batch1.DataSet = 'Batch' AND ad_first_batch1.Attribute = 'FirstDay'
WHERE 
	(w.SK_CloseDateID IS NULL AND (
		cd.DateValue > ad_last.Date OR
		cd.DateValue < CASE 
			WHEN w.Type LIKE 'Limit%' THEN ad_first_batch1.Date 
			ELSE ad_first.Date 
		END
	)) OR 
	(w.SK_CloseDateID IS NOT NULL AND (
		cld.DateValue > ad_last.Date OR
		cld.DateValue < ad_first.Date OR
		w.SK_CloseDateID < w.SK_CreateDateID
	))
GROUP BY '100', 'DimTrade date check', a.BatchID, 'All SK_DateID values are in the correct batch time window'

union

-- Query 101
SELECT 
	'101' AS Query,
    'DimTrade Status' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK'
        ELSE 'Bad value'
    END AS Result,
    'All Trade Status values are valid' AS Description
FROM DimTrade
WHERE Status NOT IN ('Canceled', 'Pending', 'Submitted', 'Active', 'Completed')
GROUP BY '101', 'DimTrade Status', NULL, 'All Trade Status values are valid'

union

-- Query 102
SELECT 
	'102' AS Query,
    'DimTrade Type' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0 THEN 'OK'
        ELSE 'Bad value'
    END AS Result,
    'All Trade Type values are valid' AS Description
FROM DimTrade
WHERE Type NOT IN ('Market Buy', 'Market Sell', 'Stop Loss', 'Limit Sell', 'Limit Buy')
GROUP BY '102', 'DimTrade Type', NULL, 'All Trade Type values are valid'

--  
-- Checks against the Financial table.
--  

union 

-- Query 103
SELECT 
	'103' AS Query,
    'Financial row count' AS Test,
    NULL AS Batch,
    CASE 
        WHEN dm.MessageData = a.TotalValue THEN 'OK'
        ELSE 'Mismatch'
    END AS Result,
    'Actual row count matches Audit table' AS Description
FROM DImessages dm
JOIN (SELECT SUM(Value) AS TotalValue FROM Audit WHERE DataSet = 'Financial' AND Attribute = 'FW_FIN') a ON TRUE
WHERE MessageSource = 'Financial' 
		AND MessageType = 'Validation' 
		AND MessageText = 'Row count' 
		AND BatchID = 1
---------------------------------------------------------------------------------------------------------------------------

UNION 

-- Query 104
SELECT 
	'104' AS Query,
    'Financial SK_CompanyID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.*) = COUNT(c.SK_CompanyID) 
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_CompanyIDs match a DimCompany record' AS Description
FROM 
    Financial a
LEFT JOIN 
    DimCompany c
    ON a.SK_CompanyID = c.SK_CompanyID
GROUP BY '104', 'Financial SK_CompanyID', NULL, 'All SK_CompanyIDs match a DimCompany record'

union

-- Query 105
SELECT 
	'105' AS Query,
    'Financial FI_YEAR' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK'
        ELSE 'Bad Year'
    END AS Result,
    'All Years are within Batch1 range' AS Description
FROM 
    Financial f
JOIN 
    Audit a 
    ON a.DataSet = 'Batch' 
    AND a.BatchID = 1 
    AND a.Attribute IN ('FirstDay', 'LastDay')
WHERE 
    (f.FI_YEAR < YEAR(a.Date) AND a.Attribute = 'FirstDay')
    OR (f.FI_YEAR > YEAR(a.Date) AND a.Attribute = 'LastDay')
GROUP BY '105', 'Financial FI_QTR', NULL, 'All Years are within Batch1 range'

union

-- Query 106
SELECT 
	'106' AS Query,
    'Financial FI_QTR' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK'
        ELSE 'Bad Qtr'
    END AS Result,
    'All quarters are in ( 1, 2, 3, 4 )' AS Description
FROM 
    Financial
WHERE 
    FI_QTR not in ( 1, 2, 3, 4 )
GROUP BY '106' 'Financial FI_QTR', NULL, 'All quarters are in ( 1, 2, 3, 4 )'

union

-- Query 107
SELECT 
	'107' AS Query,
    'Financial FI_QTR' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK'
        ELSE 'Bad date'
    END AS Result,
    'All quarters start on correct date' AS Description
FROM 
    Financial
WHERE 
    FI_YEAR <> year(FI_QTR_START_DATE)
	   or month(FI_QTR_START_DATE) <> (FI_QTR-1)*3+1
	   or day(FI_QTR_START_DATE) <> 1
GROUP BY '107', 'Financial FI_QTR', NULL, 'All quarters start on correct date'

union											

-- Query 108
SELECT 
	'108' AS Query,
    'Financial EPS' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(*) = 0
        THEN 'OK'
        ELSE 'Bad EPS'
    END AS Result,
    'Earnings calculations are valid' AS Description
FROM 
    Financial f
WHERE 
    ROUND(f.FI_NET_EARN / f.FI_OUT_BASIC, 2) - f.FI_BASIC_EPS NOT BETWEEN -0.4 AND 0.4
    OR ROUND(f.FI_NET_EARN / f.FI_OUT_DILUT, 2) - f.FI_DILUT_EPS NOT BETWEEN -0.4 AND 0.4
    OR ROUND(f.FI_NET_EARN / f.FI_REVENUE, 2) - f.FI_MARGIN NOT BETWEEN -0.4 AND 0.4
GROUP BY '108', 'Financial EPS', NULL, 'Earnings calculations are valid'

--  
-- Checks against the FactMarketHistory table.
--  

union

-- Query 109
SELECT 
	'109' AS Query,
    'FactMarketHistory row count' AS Test,
    a.BatchID AS Batch,
    CASE 
        WHEN CAST(dm1.MessageData AS INT) - CAST(dm2.MessageData AS INT) = CAST(a2.Value AS INT)
        THEN 'OK'
        ELSE 'Mismatch'
    END AS Result,
    'Actual row count matches Audit table' AS Description
FROM 
    Audit a
JOIN 
    DImessages dm1
	ON dm1.MessageSource = 'FactMarketHistory' 
    AND dm1.MessageType = 'Validation' 
    AND dm1.MessageText = 'Row count' 
    AND dm1.BatchID = a.BatchID
JOIN 
    DImessages dm2 
	ON dm2.MessageSource = 'FactMarketHistory' 
    AND dm2.MessageType = 'Validation' 
    AND dm2.MessageText = 'Row count' 
    AND dm2.BatchID = a.BatchID - 1
JOIN 
    Audit a2
	ON a2.DataSet = 'FactMarketHistory' 
	AND a2.Attribute = 'DM_RECORDS' 
    AND a2.BatchID = a.BatchID
WHERE 
    a.BatchID IN (1, 2, 3)


union

-- Query 110
SELECT 
	'110' AS Query,
    'FactMarketHistory batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3
		THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM 
    FactMarketHistory
GROUP BY '110', 'FactMarketHistory batches', NULL, 'BatchID values must match Audit table'

union

-- Query 111
SELECT 
	'111' AS Query,
    'FactMarketHistory SK_CompanyID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.SK_CompanyID) = COUNT(c.SK_CompanyID)
		THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_CompanyIDs match a DimCompany record with a valid date range' AS Description
FROM 
    FactMarketHistory a
JOIN 
    DimCompany c
	ON a.SK_CompanyID = c.SK_CompanyID
JOIN 
    DimDate d
	ON a.SK_DateID = d.SK_DateID
WHERE 
    c.EffectiveDate <= d.DateValue 
    AND d.DateValue <= c.EndDate
GROUP BY '111', 'FactMarketHistory SK_CompanyID', NULL, 'All SK_CompanyIDs match a DimCompany record with a valid date range'

union

-- Query 112
SELECT 
	'112' AS Query,
    'FactMarketHistory SK_SecurityID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.SK_SecurityID) = COUNT(c.SK_SecurityID)
		THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_SecurityIDs match a DimSecurity record with a valid date range' AS Description
FROM 
    FactMarketHistory a
JOIN 
    DimSecurity c
	ON a.SK_SecurityID = c.SK_SecurityID
JOIN 
    DimDate d
	ON a.SK_DateID = d.SK_DateID
WHERE 
    c.EffectiveDate <= d.DateValue 
    AND d.DateValue <= c.EndDate
GROUP BY '112', 'FactMarketHistory SK_SecurityID', NULL, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'


union															

-- Query 113
SELECT 
	'113' AS Query,
    'FactMarketHistory SK_DateID' AS Test,
    a.BatchID AS Batch,
    CASE 
        WHEN COUNT(CASE WHEN d.DateValue < a2.FirstDay OR d.DateValue >= a2.LastDay THEN 1 ELSE 0 END) = 0
        THEN 'OK'
        ELSE 'Bad Date'
    END AS Result,
    'All dates are within batch date range' AS Description
FROM Audit a
JOINFactMarketHistory m ON m.BatchID = a.BatchID
JOIN DimDate dON d.SK_DateID = m.SK_DateID
LEFT JOIN (
    SELECT 
        BatchID, 
        (CASE WHEN Attribute = 'FirstDay' THEN Date END) AS FirstDay,
        (CASE WHEN Attribute = 'LastDay' THEN Date END) AS LastDay
    FROM Audit
    WHERE DataSet = 'Batch'
    GROUP BY 1, 2, 3
) a2 ON a.BatchID = a2.BatchID
WHERE a.BatchID IN (1, 2, 3)
GROUP BY 1, 2, 3, 5

union

-- Query 114
SELECT 
	'114' AS Query,
    'FactMarketHistory relative dates' AS Test,
    NULL AS Batch,
    CASE 
		WHEN COUNT(CASE WHEN FiftyTwoWeekLow > DayLow 
			OR DayLow > ClosePrice 
			OR ClosePrice > DayHigh 
			OR DayHigh > FiftyTwoWeekHigh 
        	THEN 1 ELSE NULL END) = 0 
    	THEN 'OK' 
        ELSE 'Bad Date' 
    END AS Result,
    '52-week-low <= day_low <= close_price <= day_high <= 52-week-high' AS Description
FROM FactMarketHistory
GROUP BY 1, 2, 3, 5

--  
-- Checks against the FactHoldings table.
--  
union

-- Query 115
SELECT 
	'115' AS Query,
    'FactHoldings row count' AS Test,
    a.BatchID AS Batch,
    CASE 
        WHEN CAST(dm1.MessageData AS INT) - CAST(dm2.MessageData AS INT) = CAST(a2.Value AS INT) 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'Actual row count matches Audit table' AS Description
FROM 
    Audit a
JOIN 
    DImessages dm1 
    ON dm1.MessageSource = 'FactHoldings' 
    AND dm1.MessageType = 'Validation' 
    AND dm1.MessageText = 'Row count' 
    AND dm1.BatchID = a.BatchID
JOIN 
    DImessages dm2 
    ON dm2.MessageSource = 'FactHoldings' 
    AND dm2.MessageType = 'Validation' 
    AND dm2.MessageText = 'Row count' 
    AND dm2.BatchID = a.BatchID - 1
JOIN 
    Audit a2
    ON a2.DataSet = 'FactHoldings' 
    AND a2.Attribute = 'HH_RECORDS' 
    AND a2.BatchID = a.BatchID
WHERE 
    a.BatchID IN (1, 2, 3)

union

-- Query 116
SELECT 
	'116' AS Query,
    'FactHoldings batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchID) = 3 AND MAX(BatchID) = 3 
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM 
    FactHoldings
GROUP BY 1, 2, 3, 5


union
/* It is possible that the dimension record has changed between orgination of the trade and the completion of the trade. *
 * So, we can check that the Effective Date of the dimension record is older than the the completion date, but the end date could be earlier or later than the completion date
 */

-- Query 117
SELECT 
	'117' AS Query,
    'FactHoldings SK_CustomerID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.SK_CustomerID) = COUNT(c.SK_CustomerID) 
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_CustomerIDs match a DimCustomer record with a valid date range' AS Description
FROM 
    FactHoldings a
JOIN 
    DimDate d
	ON d.SK_DateID = a.SK_DateID
JOIN 
    DimCustomer c
	ON a.SK_CustomerID = c.SK_CustomerID 
    AND c.EffectiveDate <= d.DateValue
GROUP BY 1, 2, 3, 5

union

-- Query 118
SELECT 
	'118' AS Query,
    'FactHoldings SK_AccountID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.SK_AccountID) = COUNT(c.SK_AccountID) 
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_AccountIDs match a DimAccount record with a valid date range' AS Description
FROM 
    FactHoldings a
JOIN 
    DimDate d
	ON d.SK_DateID = a.SK_DateID
JOIN 
    DimAccount c
	ON a.SK_AccountID = c.SK_AccountID 
    AND c.EffectiveDate <= d.DateValue
GROUP BY 1, 2, 3, 5

union

-- Query 119
SELECT 
	'119' AS Query,
    'FactHoldings SK_CompanyID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.SK_CompanyID) = COUNT(c.SK_CompanyID) 
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_CompanyIDs match a DimCompany record with a valid date range' AS Description
FROM 
    FactHoldings a
JOIN 
    DimDate d
	ON d.SK_DateID = a.SK_DateID
JOIN 
    DimCompany c
	ON a.SK_CompanyID = c.SK_CompanyID 
    AND c.EffectiveDate <= d.DateValue
GROUP BY 1, 2, 3, 5

union

-- Query 120
SELECT 
	'120' AS Query,
    'FactHoldings SK_SecurityID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.SK_SecurityID) = COUNT(c.SK_SecurityID) 
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_SecurityIDs match a DimSecurity record with a valid date range' AS Description
FROM 
    FactHoldings a
JOIN 
    DimDate d
	ON d.SK_DateID = a.SK_DateID
JOIN 
    DimSecurity c
	ON a.SK_SecurityID = c.SK_SecurityID 
    AND c.EffectiveDate <= d.DateValue
GROUP BY 1, 2, 3, 5


union

-- Query 121
SELECT 
	'121' AS Query,
    'FactHoldings CurrentTradeID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.CurrentTradeID) = COUNT(t.TradeID) 
        THEN 'OK' 
        ELSE 'Failed' 
    END AS Result,
    'CurrentTradeID matches a DimTrade record with Close Date and Time values used as the holdings date and time' AS Description
FROM 
    FactHoldings a
JOIN 
    DimTrade t
	ON a.CurrentTradeID = t.TradeID 
    AND a.SK_DateID = t.SK_CloseDateID 
    AND a.SK_TimeID = t.SK_CloseTimeID
GROUP BY 1, 2, 3, 5


union

-- Query 122
SELECT 
	'122' AS Query,
    'FactHoldings SK_DateID' AS Test,
    a.BatchID AS Batch,
    CASE 
        WHEN COUNT(CASE WHEN d.DateValue < a2.FirstDay OR d.DateValue > a2.LastDay THEN 1 ELSE 0 END) = 0
        THEN 'OK'
        ELSE 'Bad Date'
    END AS Result,
    'All dates are within batch date range' AS Description
FROM 
    Audit a
JOIN
    FactHoldings m
    ON m.BatchID = a.BatchID
JOIN 
    DimDate d
    ON d.SK_DateID = m.SK_DateID
LEFT JOIN (
    SELECT 
        BatchID, 
        (CASE WHEN Attribute = 'FirstDay' THEN Date END) AS FirstDay,
        (CASE WHEN Attribute = 'LastDay' THEN Date END) AS LastDay
    FROM Audit
    WHERE DataSet = 'Batch'
    GROUP BY 1, 2, 3
) a2 ON a.BatchID = a2.BatchID
WHERE 
    a.BatchID IN (1, 2, 3)
GROUP BY 1, 2, 3, 5

--  
-- Checks against the FactCashBalances table.
--  
union

-- Query 123
SELECT 
	'123' AS Query,
    'FactCashBalances batches' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(DISTINCT BatchId) = 3 AND MAX(BatchId) = 3
        THEN 'OK' 
        ELSE 'Mismatch' 
    END AS Result,
    'BatchID values must match Audit table' AS Description
FROM
	FactCashBalances
GROUP BY '123', 'FactCashBalances batches', NULL, 'BatchID values must match Audit table'

union

-- Query 124
SELECT 
	'124' AS Query,
    'FactCashBalances SK_CustomerID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.SK_CustomerID) = COUNT(c.SK_CustomerID) 
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_CustomerIDs match a DimCustomer record with a valid date range' AS Description
FROM
	FactCashBalances a
LEFT JOIN 
    DimDate d
    ON d.SK_DateID = a.SK_DateID
LEFT JOIN 
    DimCustomer c
    ON a.SK_CustomerID = c.SK_CustomerID 
    AND c.EffectiveDate <= d.DateValue
    AND d.DateValue <= c.EndDate
GROUP BY '124', 'FactCashBalances SK_CustomerID', NULL, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'

union

-- Query 125
SELECT 
	'125' AS Query,
    'FactCashBalances SK_AccountID' AS Test,
    NULL AS Batch,
    CASE 
        WHEN COUNT(a.SK_AccountID) = COUNT(c.SK_AccountID) 
        THEN 'OK' 
        ELSE 'Bad join' 
    END AS Result,
    'All SK_AccountIDs match a DimAccount record with a valid date range' AS Description
FROM 
    FactCashBalances a
LEFT JOIN 
    DimDate d ON d.SK_DateID = a.SK_DateID
LEFT JOIN 
    DimAccount c ON a.SK_AccountID = c.SK_AccountID 
    AND c.EffectiveDate <= d.DateValue
    AND d.DateValue <= c.EndDate
GROUP BY '125', 'FactCashBalances SK_AccountID', NULL, 'All SK_AccountIDs match a DimAccount record with a valid date range'


union

-- Query 126
SELECT 
	'126' AS Query,
    'FactCashBalances SK_DateID' AS Test,
    a.BatchID AS Batch,
    CASE 
        WHEN COUNT(CASE WHEN d.DateValue < a2.FirstDay OR d.DateValue > a2.LastDay THEN 1 ELSE 0 END) = 0 
        THEN 'OK' 
        ELSE 'Bad Date' 
    END AS Result,
    'All dates are within batch date range' AS Description
FROM 
    Audit a
JOIN 
    FactCashBalances m
    ON a.BatchID = m.BatchID
JOIN 
    DimDate d
    ON d.SK_DateID = m.SK_DateID
LEFT JOIN (
    SELECT 
        BatchID, 
        (CASE WHEN Attribute = 'FirstDay' THEN Date END) AS FirstDay,
         (CASE WHEN Attribute = 'LastDay' THEN Date END) AS LastDay
    FROM Audit
    WHERE DataSet = 'Batch'
    GROUP BY 1, 2, 3
) a2 ON a.BatchID = a2.BatchID
WHERE 
    a.BatchID IN (1, 2, 3)
GROUP BY 1, 2, 3, 5


/*
 *  Checks against the Batch Validation Query row counts
 */

union

-- Query 127

SELECT 
	'127' AS Query,
	'Batch row count: ' || m.MessageSource AS Test,
	a.BatchID AS Batch,
	CASE 
		WHEN CAST(m2.MessageData AS BIGINT) > CAST(m1.MessageData AS BIGINT) 
		THEN 'Row count decreased' 
		ELSE 'OK' 
	END AS Result,
	'Row counts do not decrease between successive batches' AS Description
FROM 
	Audit a
FULL JOIN 
	DImessages m ON m.BatchID = 0 
	AND m.MessageText = 'Row count' 
	AND m.MessageType = 'Validation'
JOIN 
	DImessages m1 ON m1.BatchID = a.BatchID 
	AND m1.MessageSource = m.MessageSource 
	AND m1.MessageText = 'Row count' 
	AND m1.MessageType = 'Validation'
JOIN 
	DImessages m2 ON m2.BatchID = a.BatchID - 1 
	AND m2.MessageSource = m.MessageSource 
	AND m2.MessageText = 'Row count' 
	AND m2.MessageType = 'Validation'
WHERE 
	a.BatchID IN (1, 2, 3)


union

-- Query 128
SELECT 
	'128' AS Query,
	'Batch joined row count: ' || m.MessageSource AS Test,
	a.BatchID AS Batch,
	CASE 
		WHEN CAST(m1.MessageData AS BIGINT) = CAST(m2.MessageData AS BIGINT) 
		THEN 'OK' 
		ELSE 'No match' 
	END AS Result,
	'Row counts match when joined to dimensions' AS Description
FROM 
	Audit a
FULL JOIN 
	DImessages m ON m.BatchID = 0 
	AND m.MessageText = 'Row count' 
	AND m.MessageType = 'Validation'
JOIN 
	DImessages m1 ON m1.BatchID = a.BatchID 
	AND m1.MessageSource = m.MessageSource 
	AND m1.MessageText = 'Row count' 
	AND m1.MessageType = 'Validation'
JOIN 
	DImessages m2 ON m2.BatchID = a.BatchID 
	AND m2.MessageSource = m.MessageSource 
	AND m2.MessageText = 'Row count joined' 
	AND m2.MessageType = 'Validation'
WHERE 
	a.BatchID IN (1, 2, 3)


/*
 *  Checks against the Data Visibility Query row counts
 */

union

-- Query 129
SELECT 
	Query,
	Test,
	BatchID AS Batch,
	Result,
	Description
FROM (
	SELECT 
		'129' AS Query,
		m1.MessageSource,
		'Data visibility row counts: ' || m1.MessageSource AS Test,
		NULL AS Batch,
		m1.MessageData,
		m2.MessageData,
		CASE 
			WHEN SUM(CASE WHEN CAST(m1.MessageData AS BIGINT) > CAST(m2.MessageData AS BIGINT) THEN 1 ELSE 0 END) = 0 
			THEN 'OK' 
			ELSE 'Row count decreased' 
		END AS Result,
		'Row counts must be non-decreasing over time' AS Description
	FROM 
		DImessages m1
	JOIN 
		DImessages m2 
		ON m2.MessageType IN ('Visibility_1', 'Visibility_2') 
		AND m2.MessageText = 'Row count' 
		AND m2.MessageSource = m1.MessageSource 
		AND m2.MessageDateAndTime > m1.MessageDateAndTime
	WHERE 
		m1.MessageType IN ('Visibility_1', 'Visibility_2') 
		AND m1.MessageText = 'Row count'
	GROUP BY '129', m1.MessageSource, 'Data visibility row counts: ' || m1.MessageSource, NULL, m1.MessageData, m2.MessageData, 'Row counts must be non-decreasing over time'
)

union

-- Query 130
SELECT 
	Query,
	Test,
	BatchID AS Batch,
	Result,
	Description
FROM (
	SELECT 
		'130' AS Query,
		m1.MessageSource,
		'Data visibility joined row counts: ' || m1.MessageSource AS Test,
		NULL AS Batch,
		m1.MessageData,
		m2.MessageData,
		CASE 
			WHEN SUM(CASE WHEN CAST(m1.MessageData AS BIGINT) > CAST(m2.MessageData AS BIGINT) THEN 1 ELSE 0 END) = 0 
			THEN 'OK' 
			ELSE 'No match' 
		END AS Result,
		'Row counts match when joined to dimensions' AS Description
	FROM 
		DImessages m1
	JOIN 
		DImessages m2 
		ON m2.MessageType = 'Visibility_1' 
		AND m2.MessageText = 'Row count joined' 
		AND m2.MessageSource = m1.MessageSource 
		AND m2.MessageDateAndTime = m1.MessageDateAndTime
	WHERE 
		m1.MessageType = 'Visibility_1' 
		AND m1.MessageText = 'Row count'
	GROUP BY '130', m1.MessageSource, 'Data visibility row counts: ' || m1.MessageSource, NULL, m1.MessageData, m2.MessageData, 'Row counts match when joined to dimensions'
)

