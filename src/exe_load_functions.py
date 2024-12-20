import os
import findspark
findspark.init()
from pyspark.sql.functions import to_date, date_format,collect_set, expr, udf, struct, col
# from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType, DateType, TimestampType
from pyspark.sql.types import StringType, StructField, StructType
from datetime import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from src.aux_functions import columnarize_finwire_data_cmp, columnarize_finwire_data_fin, columnarize_finwire_data_sec, extract_finwire_type
from src.aux_functions2 import get_marketingnameplate, cast_to_target_schema
from src.exe_create_functions import create_dim_company, create_dim_security, create_prospect_table
from src.parser_functions import add_account_parser, customer_parser, inactive_parser, update_account_parser, update_customer_parser

#########################################################################
#                                                                       #
#                         LOAD                                          #        
#                                                                       #    
#########################################################################


def load_dim_date(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    schema = """
            `SK_DateID` INTEGER,
            `DateValue` DATE,
            `DateDesc` STRING,
            `CalendarYearID` INTEGER,
            `CalendarYearDesc` STRING,
            `CalendarQtrID` INTEGER,
            `CalendarQtrDesc` STRING,
            `CalendarMonthID` INTEGER,
            `CalendarMonthDesc` STRING,
            `CalendarWeekID` INTEGER,
            `CalendarWeekDesc` STRING,
            `DayOfWeekNum` INTEGER,
            `DayOfWeekDesc` STRING,
            `FiscalYearID` INTEGER,
            `FiscalYearDesc` STRING,
            `FiscalQtrID` INTEGER,
            `FiscalQtrDesc` STRING,
            `HolidayFlag` BOOLEAN
    """
    dates = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder}/Date.txt")
    )
    dates.write.option("overwriteSchema", "true").saveAsTable(
        "DimDate", mode="overwrite"
    )
    return dates


def load_dim_time(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    schema = """
            `SK_TimeID` INTEGER,
            `TimeValue` TIMESTAMP,
            `HourID` INTEGER,
            `HourDesc` STRING,
            `MinuteID` INTEGER,
            `MinuteDesc` STRING,
            `SecondID` INTEGER,
            `SecondDesc` STRING,
            `MarketHoursFlag` BOOLEAN,
            `OfficeHoursFlag` BOOLEAN
    """
    times = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder}/Time.txt")
    )
    times.write.option("overwriteSchema", "true").saveAsTable(
        "DimTime", mode="overwrite"
    )
    return times


def load_tax_rate(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    schema = """
        `TX_ID` String,
        `TX_NAME` String,
        `TX_RATE` Float

    """
    Tax_Rate = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder}/TaxRate.txt")
    )
    Tax_Rate.createOrReplaceTempView("TaxRateView")
    
    spark.sql(
    """
        INSERT INTO TaxRate(TX_ID, TX_NAME,TX_RATE)
        SELECT TX_ID, 
               TX_NAME as NationalTaxRateDesc, 
               TX_RATE as NationalTaxRate 
        FROM TaxRateView
    """
    )
    return Tax_Rate



def load_staging_hr_file(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    schema = """
            `EmployeeID` INTEGER,
            `ManagerID`  INTEGER,
            `EmployeeFirstName` STRING,
            `EmployeeLastName` STRING,
            `EmployeeMI` STRING,
            `EmployeeJobCode` INTEGER,
            `EmployeeBranch` STRING,
            `EmployeeOffice` STRING,
            `EmployeePhone` STRING
            
    """
    brokers = (
        spark.read.format("csv")
        .option("delimiter", ",")
        .schema(schema)
        .load(f"{staging_area_folder}/HR.csv")
        .where("EmployeeJobCode = 314")
    )
    # Save staging data into temp view
    brokers.createOrReplaceTempView("hr")
    # Copy data into warehouse table
    
    spark.sql("""
            SELECT 
                monotonically_increasing_id() AS SK_BrokerID,
                EmployeeID AS BrokerID,
                ManagerID,
                EmployeeFirstName AS FirstName,
                EmployeeLastName AS LastName,
                EmployeeMI AS MiddleInitial,
                EmployeeBranch AS Branch,
                EmployeeOffice AS Office,
                EmployeePhone AS Phone,
                (SELECT TRUE) AS IsCurrent,
                (SELECT 1) AS BatchID,
                (SELECT MIN(DateValue) FROM DimDate) AS EffectiveDate,
                TO_DATE('9999-12-31') AS EndDate
            FROM hr
    """).createOrReplaceTempView("DimBrokerNoUpdate")
    
    spark.sql("""
        SELECT 
            *, 
            MAX(SK_BrokerID) OVER (PARTITION BY BrokerID) AS LAST_SK, 
            LAG(EffectiveDate, -1) IGNORE NULLS OVER (PARTITION BY BrokerID ORDER BY EffectiveDate) 
                AS NewEndDate
        FROM DimBrokerNoUpdate
    """) \
    .withColumn("IsCurrent", expr("CASE WHEN LAST_SK != SK_BrokerID THEN False ELSE TRUE END")) \
    .withColumn(
        "EndDate", 
        expr("CASE WHEN LAST_SK != SK_BrokerID THEN NewEndDate ELSE EndDate END")) \
    .drop("LAST_SK", "NewEndDate") \
    .createOrReplaceTempView("DimBrokerUpdated")
    
    spark.sql(
        """
        INSERT INTO DimBroker(
            SK_BrokerID, BrokerID, ManagerID, FirstName, LastName, MiddleInitial, Branch, Office, Phone, IsCurrent, BatchID, EffectiveDate, EndDate)
        SELECT * FROM DimBrokerUpdated
    """
    )
    return spark.sql("SELECT * FROM DimBroker")

#########################################################################
#                                                                       #
#                         LOAD                       #        
#                                                                       #    
#########################################################################


def load_finwire_file(spark,finwire_file_path, dbname, extract_type='CMP'):
    spark.sql(f"USE {dbname}")
    finwire = spark.read.format("text").load(finwire_file_path)\
        .withColumn("RecType", extract_finwire_type(col("value")))

    finwire_cmp = finwire.where(f"RecType == 'CMP'") \
        .withColumn("columnarized", columnarize_finwire_data_cmp("value")) \
        .select("columnarized.*")
    finwire_sec = finwire.where(f"RecType == 'SEC'") \
        .withColumn("columnarized", columnarize_finwire_data_sec("value")) \
        .select("columnarized.*")
    finwire_fin = finwire.where(f"RecType == 'FIN'") \
        .withColumn("columnarized", columnarize_finwire_data_fin("value")) \
        .select("columnarized.*")
    return finwire_cmp, finwire_sec, finwire_fin



def load_finwire_files(spark,dbname, scale_factor):
    spark.sql(f"USE {dbname}")
    spark.sql(f"DROP TABLE DimCompany")
    create_dim_company(spark, dbname)
    
    files_path = f"{os.getcwd()}/data/{scale_factor}/Batch1/"
    files = os.listdir(files_path)
    finwire_files = [file for file in files if "FINWIRE" in file and "_audit" not in file]
    # First load the Finwire Data into dataframe
    cmp = None
    sec = None
    fin = None
    for i, finwire_file in enumerate(sorted(finwire_files)):
        if i == 0:
            finwire_file_path = files_path + finwire_file
            cmp, sec, fin = load_finwire_file(spark,finwire_file_path, "test")
        else:
            finwire_file_path = files_path + finwire_file
            newcmp, newsec, newfin = load_finwire_file(spark,finwire_file_path, "test")
            cmp = cmp.union(newcmp)
            sec = sec.union(newsec)
            fin = fin.union(newfin)
    cmp.orderBy(['CIK', 'PTS']).createOrReplaceTempView("finwire_cmp")
    sec.orderBy(['Symbol', 'PTS']).createOrReplaceTempView("finwire_sec")
    fin.createOrReplaceTempView("finwire_fin")



def load_finwires_into_dim_company(spark,dbname, scale_factor):
    spark.sql(f"USE {dbname}")

    # Now load industry.txt file
#     industry_schema = "`IN_ID` string, `IN_NAME` string, `IN_SC_ID` string"
#     industry = spark.read.format("csv") \
#         .option("delimiter", "|") \
#         .schema(industry_schema) \
#         .load(f"gs://{bucket_name}/TPCDI_Data/TPCDI_Data/{scale_factor}/Batch1/Industry.txt")
#     industry.createOrReplaceTempView("industry")
    
    # Now load status file
    status_schema = "`ST_ID` string, `ST_NAME` string"
    status = spark.read.format("csv") \
        .option("delimiter", "|") \
        .schema(status_schema) \
        .load(f"{os.getcwd()}/data/{scale_factor}/Batch1/StatusType.txt")
    status.createOrReplaceTempView("status")
    
    spark.sql("""
    SELECT
           monotonically_increasing_id() AS SK_CompanyID,
           CIK AS CompanyID,
           ST_NAME AS Status,
           CompanyName AS Name,
           IN_NAME AS Industry,
           SPrating AS SPrating,
           (SELECT CASE 
                WHEN SUBSTRING(SPrating, 1, 1) == 'A' OR SUBSTRING(SPrating, 1, 3) == 'BBB' THEN FALSE
                ELSE TRUE
            END) AS isLowGrade,
           CEOname AS CEO,
           AddrLine1 AS AddressLine1,
           AddrLine2 AS AddressLine2,
           PostalCode,
           City,
           StateProvince AS StateProv,
           Country,
           Description,
           TO_DATE(FoundingDate, 'yyyyMMdd') AS FoundingDate,
           (SELECT TRUE) AS IsCurrent,
           (SELECT 1) AS BatchID,
           (SELECT 
                 CASE WHEN TO_DATE(PTS, 'yyyyMMdd-kkmmss') IS NOT NULL 
                     THEN TO_DATE(PTS, 'yyyyMMdd-kkmmss')
                 ELSE current_date()
            END) AS EffectiveDate,
           TO_DATE('9999-12-31') AS EndDate
       FROM finwire_cmp, industry, status
       WHERE
           finwire_cmp.Status = status.ST_ID AND
           finwire_cmp.IndustryID = industry.IN_ID
    """).createOrReplaceTempView("DimCompanyNoUpdate")
    
    spark.sql("""
        SELECT 
            *, 
            MAX(SK_CompanyID) OVER (PARTITION BY CompanyID) AS LAST_SK, 
            LAG(EffectiveDate, -1) IGNORE NULLS OVER (PARTITION BY CompanyID ORDER BY EffectiveDate) 
                AS NewEndDate
        FROM DimCompanyNoUpdate
    """) \
    .withColumn("IsCurrent", expr("CASE WHEN LAST_SK != SK_CompanyID THEN False ELSE TRUE END")) \
    .withColumn(
        "EndDate", 
        expr("CASE WHEN LAST_SK != SK_CompanyID THEN NewEndDate ELSE EndDate END")) \
    .drop("LAST_SK", "NewEndDate") \
    .createOrReplaceTempView("DimCompanyUpdated")
    
    cast_to_target_schema(spark, "DimCompanyUpdated", "DimCompany").createOrReplaceTempView("DimCompanyUpdated")


    # Insert data into dimension table
    spark.sql("""
       INSERT INTO DimCompany(SK_CompanyID, CompanyID, Status, Name, Industry, SPrating, isLowGrade,
                               CEO,
                               AddressLine1, AddressLine2, PostalCode, City, StateProv, Country, 
                               Description, FoundingDate, IsCurrent, BatchID, EffectiveDate, EndDate)
       SELECT * FROM DimCompanyUpdated
    """)
    return spark.sql("SELECT * FROM DimCompany")


def load_finwires_into_dim_security(spark,dbname):

    spark.sql(f"USE {dbname}")
    spark.sql(f"DROP TABLE DimSecurity")
    create_dim_security(spark,dbname)
    
    spark.sql("""
            SELECT
            monotonically_increasing_id() AS SK_SecurityID,
            Symbol,
            IssueType AS Issue,
            ST_Name AS Status,
            f.Name,
            ExId as ExchangeID,
            SK_CompanyID,
            ShOut AS SharesOutstanding,
            TO_DATE(FirstTradeDate, 'yyyyMMdd') AS FirstTrade,
            TO_DATE(FirstTradeExchg, 'yyyyMMdd')AS FirstTradeOnExchange,
            Dividend,
            (SELECT TRUE) AS IsCurrent,
            1 AS BatchID,
            (SELECT 
                 CASE WHEN TO_DATE(PTS, 'yyyyMMdd-kkmmss') IS NOT NULL 
                     THEN TO_DATE(PTS, 'yyyyMMdd-kkmmss')
                 ELSE current_date()
            END) AS EffectiveDate,
            TO_DATE('9999-12-31') AS EndDate
        FROM finwire_sec f JOIN status s ON
            (f.Status = s.ST_ID) JOIN DimCompany c ON
            (c.CompanyID = f.CoNameOrCIK OR c.Name=f.CoNameOrCIK)
    """).createOrReplaceTempView("DimSecurityNoUpdate")
    
    spark.sql("""
        SELECT 
            *, 
            MAX(SK_SecurityID) OVER (PARTITION BY Symbol) AS LAST_SK, 
            LAG(EffectiveDate, -1) IGNORE NULLS OVER (PARTITION BY Symbol ORDER BY EffectiveDate) 
                AS NewEndDate
        FROM DimSecurityNoUpdate
    """) \
    .withColumn("IsCurrent", expr("CASE WHEN LAST_SK != SK_SecurityID THEN False ELSE TRUE END")) \
    .withColumn(
        "EndDate", 
        expr("CASE WHEN LAST_SK != SK_SecurityID THEN NewEndDate ELSE EndDate END")) \
    .drop("LAST_SK", "NewEndDate") \
    .createOrReplaceTempView("DimSecurityUpdated")
    
    cast_to_target_schema(spark, "DimSecurityUpdated", "DimSecurity").createOrReplaceTempView("DimSecurityUpdated")

    # Now insert values into dimSecurity
    spark.sql("""
        INSERT INTO DimSecurity(SK_SecurityID, Symbol, Issue, Status, Name, ExchangeID, SK_CompanyID,
                                SharesOutstanding, FirstTrade, FirstTradeOnExchange, Dividend,
                                IsCurrent, BatchID, EffectiveDate, EndDate)
        SELECT * FROM DimSecurityUpdated
           """)
    return spark.sql("SELECT * FROM DimSecurity")



def load_finwires_into_financial_table(spark,dbname):
    spark.sql(f"USE {dbname}")
    
    cast_to_target_schema(spark,"finwire_fin", "Financial").createOrReplaceTempView("finwire_fin")

    spark.sql("""
        INSERT INTO Financial(SK_CompanyID, FI_YEAR, FI_QTR, FI_QTR_START_DATE, FI_REVENUE,
                              FI_NET_EARN, FI_BASIC_EPS, FI_DILUT_EPS, FI_MARGIN, FI_INVENTORY,
                              FI_ASSETS, FI_LIABILITY, FI_OUT_BASIC, FI_OUT_DILUT)
          SELECT DISTINCT
              SK_CompanyID,
              CAST(Year AS INT) AS FI_YEAR,
              CAST(Quarter AS INT) AS FI_QTR,
              TO_DATE(QtrStartDate, 'yyyyMMdd') AS FI_QTR_START_DATE,
              CAST(Revenue AS FLOAT) AS FI_REVENUE,
              CAST(Earnings AS FLOAT) AS FI_NET_EARN,
              CAST(EPS AS FLOAT) AS FI_BASIC_EPS,
              CAST(DilutedEPS AS FLOAT) AS FI_DILUT_EPS,
              CAST(Margin AS FLOAT) AS FI_MARGIN,
              CAST(Inventory AS FLOAT) AS FI_INVENTORY,
              CAST(Assets AS FLOAT) AS FI_ASSETS,
              cast(Liabilities AS FLOAT) AS FI_LIABILITY,
              cast(ShOut AS FLOAT) AS FI_OUT_BASIC,
              cast(DilutedSHOut AS FLOAT) AS FI_OUT_DILUT
          FROM finwire_fin f 
          JOIN DimCompany c ON (c.CompanyID = f.CoNameOrCIK OR c.Name = f.CoNameOrCIK)
    """)
    return spark.sql("SELECT * FROM Financial")


def load_status_type(spark,dbname, staging_area_folder):
    #spark.sql(f"USE {dbname}")
    schema = """
        `ST_ID` String,
        `ST_NAME` String
    """
    status_type = spark.read.format("csv").option("delimiter", "|").schema(schema).load(f"{staging_area_folder}/StatusType.txt")
    
    status_type.createOrReplaceTempView("status_type")
    
    spark.sql(
    """
        INSERT INTO StatusType(ST_ID, ST_NAME)
        SELECT ST_ID, ST_NAME FROM status_type
    """)
    return status_type


def load_trade_type(spark,dbname, staging_area_folder):
    #spark.sql(f"USE {dbname}")
    schema = """
        `TT_ID` String,
        `TT_NAME` String,
        `TT_IS_SELL` INTEGER,
        `TT_IS_MRKT` INTEGER
    """
    trade_type = spark.read.format("csv").option("delimiter", "|").schema(schema).load(f"{staging_area_folder}/TradeType.txt")
    
    trade_type.createOrReplaceTempView("trade_type")
    
    spark.sql(
    """
        INSERT INTO TradeType(TT_ID, TT_NAME, TT_IS_SELL, TT_IS_MRKT)
        SELECT TT_ID, TT_NAME, TT_IS_SELL, TT_IS_MRKT FROM trade_type
    """)
    return trade_type



def load_trade_view(spark,dbname, staging_area_folder):
    #spark.sql(f"USE {dbname}")
    schema = """
            `T_ID` INTEGER,
            `T_DTS` TIMESTAMP,
            `T_ST_ID` String,
            `T_TT_ID` String,
            `T_IS_CASH`  Boolean,
            `T_S_SYMB` String,
            `T_QTY` Float,
            `T_BID_PRICE` Float,
            `T_CA_ID` String,
            `T_EXEC_NAME` String,
            `T_TRADE_PRICE` Float,
            `T_CHRG` Float,
            `T_COMM` Float,
            `T_TAX` Float   
    """
    
    trade = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder}/Trade.txt")
        
    )
    # Save staging data into temp view
    trade.createOrReplaceTempView("trade")
    
    return trade
    
def load_tradehistory_view(spark,dbname, staging_area_folder):
    #spark.sql(f"USE {dbname}")
    schema = """
            `TH_T_ID` INTEGER,
            `TH_DTS` TIMESTAMP,
            `TH_ST_ID` String
    """
    
    trade_history = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder}/TradeHistory.txt")
        
    )
    # Save staging data into temp view
    trade_history.createOrReplaceTempView("tradeHistory")
    
    return trade_history


def load_staging_FactMarketStory(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    schema = """
        `DM_DATE` DATE,
        `DM_S_SYMB` STRING,
        `DM_CLOSE` FLOAT,
        `DM_HIGH` FLOAT,
        `DM_LOW` FLOAT,
        `DM_VOL` INTEGER
    """
    DailyMarket_ = spark.read.format("csv").option("delimiter", "|").schema(schema).load(f"{staging_area_folder}/DailyMarket.txt")
    DailyMarket_.createOrReplaceTempView("dailymarket")

    # TODO: DI Message
    DailyMarket_ = spark.sql(
        """
        WITH DailyMarket AS (
            SELECT DM.*, MIN(dm2.DM_DATE) as FiftyTwoWeekHighDate, MIN(dm3.DM_DATE) as FiftyTwoWeekLowDate
            FROM (
             SELECT dm.DM_CLOSE,
                dm.DM_S_SYMB,
                dm.DM_HIGH,
                dm.DM_LOW,
                dm.DM_VOL,
                dm.DM_DATE,
                max(dm.DM_HIGH) OVER (
                    PARTITION BY dm.DM_S_SYMB
                    ORDER BY CAST(dm.DM_DATE AS timestamp)
                    RANGE BETWEEN INTERVAL 364 DAYS PRECEDING AND CURRENT ROW
                 ) AS FiftyTwoWeekHigh,
                 min(dm.DM_LOW) OVER (
                    PARTITION BY dm.DM_S_SYMB
                    ORDER BY CAST(dm.DM_DATE AS timestamp)
                    RANGE BETWEEN INTERVAL 364 DAYS PRECEDING AND CURRENT ROW
                 ) AS FiftyTwoWeekLow
                 FROM dailymarket dm
            ) DM INNER JOIN dailymarket dm2 ON DM.FiftyTwoWeekHigh = dm2.DM_HIGH AND dm2.DM_DATE BETWEEN date_sub(DM.DM_DATE, 364) AND DM.DM_DATE
         INNER JOIN dailymarket dm3 ON DM.FiftyTwoWeekLow = dm3.DM_LOW AND dm3.DM_DATE BETWEEN date_sub(DM.DM_DATE, 364) AND dm.DM_DATE
            GROUP BY DM.DM_DATE, DM.DM_CLOSE, DM.DM_HIGH, DM.DM_LOW, DM.DM_VOL, DM.FiftyTwoWeekHigh, DM.FiftyTwoWeekLow, dm.DM_S_SYMB
        ), FinData AS (
            SELECT
            SK_CompanyID,
            SUM(FI_BASIC_EPS) OVER (
                PARTITION BY FI_QTR
                ORDER BY FI_YEAR, FI_QTR
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) as Eps
            From Financial
        ), CompEarning AS (
            SELECT dc.CompanyID, fd.Eps
            FROM DimCompany dc
            INNER JOIN FinData fd ON dc.CompanyID = fd.SK_CompanyID
        )
        SELECT  cast(dm.DM_CLOSE as float) as ClosePrice,
                cast(dm.DM_HIGH as float) as DayHigh,
                cast(dm.DM_LOW as float) as DayLow,
                cast(dm.DM_VOL as int) as Volume,
                cast(ds.SK_SecurityID as int) as SK_SecurityID,
                cast(ds.SK_CompanyID as int) as SK_CompanyID,
                cast(dd1.SK_DateID as int) as SK_DateID,
                cast(dd2.SK_DateID as int) as SK_FiftyTwoWeekHighDate,
                cast(dd3.SK_DateID as int) as SK_FiftyTwoWeekLowDate,
                cast(dm.FiftyTwoWeekHigh as float) as FiftyTwoWeekHigh,
                cast(dm.FiftyTwoWeekLow as float) as FiftyTwoWeekLow,
                cast(((ds.dividend / dm.DM_CLOSE) * 100.0) as float) as Yield,
                CASE 
                    WHEN ISNULL(ce.Eps) or ce.Eps = 0 THEN NULL 
                    ELSE cast((dm.DM_CLOSE / ce.Eps) as float)
                END as PERatio,
                cast(1 as int) as BatchID
        FROM DailyMarket dm
        INNER JOIN DimSecurity ds ON ds.Symbol = dm.DM_S_SYMB AND dm.DM_DATE BETWEEN ds.EffectiveDate AND ds.EndDate
        INNER JOIN DimDate dd1 ON dd1.DateValue = dm.DM_DATE
        INNER JOIN DimDate dd2 ON dd2.DateValue = dm.FiftyTwoWeekHighDate
        INNER JOIN DimDate dd3 ON dd3.DateValue = dm.FiftyTwoWeekLowDate
        LEFT JOIN CompEarning ce ON ds.SK_CompanyID = ce.CompanyID
         """)
    
    DailyMarket_.createOrReplaceTempView("dailymarket_insert")

    spark.sql("""
               INSERT INTO FactMarketHistory(ClosePrice, DayHigh, DayLow, Volume, SK_SecurityID, SK_CompanyID, 
                                            SK_DateID, SK_FiftyTwoWeekHighDate, SK_FiftyTwoWeekLowDate,  FiftyTwoWeekHigh, 
                                            FiftyTwoWeekLow, Yield, PERatio, BatchID)
       SELECT * FROM dailymarket_insert
    """)
    
    return spark.sql("""
       Select * from FactMarketHistory
    """)





def load_staging_Prospect(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    spark.sql(f"DROP TABLE Prospect")
    create_prospect_table(spark, dbname)

    schema = """
        `AgencyID` String,
        `LastName` String,
        `FirstName` String,
        `MiddleInitial` String,
        `Gender` String,
        `AddressLine1` String,
        `AddressLine2` String,
        `PostalCode` String,
        `City` String,
        `State` String,
        `Country` String,
        `Phone` String,
        `Income` Integer,
        `NumberCars` Integer,
        `NumberChildren` Integer,
        `MaritalStatus` String,
        `Age` Integer,
        `CreditRating` Integer,
        `OwnOrRentFlag` String,
        `Employer` String,
        `NumberCreditCards` Integer,
        `NetWorth` Integer
    """
    Prospect_ = spark.read.format("csv").option("delimiter", ",").schema(schema).load(f"{staging_area_folder}/Prospect.csv")
    print(f"Path prospect.csv:{staging_area_folder}/Prospect.csv")
    # Get the directory of the script file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print("Script Directory:", script_dir)
    print("Show prospect 1")
    Prospect_.show(5)
    ################################################
    print("EXECUTING UDF MARKETING")
    ################################################
    # udf_marketing = udf(lambda row: get_marketingnameplate(row), StringType())
    # Register as a UDF
    udf_marketing = udf(get_marketingnameplate, StringType())
     ################################################
    print("EXECUTING UDF MARKETING READY")
    ################################################
    # Prospect_ = Prospect_.withColumn('MarketingNameplate', udf_marketing(struct([Prospect_[x] for x in Prospect_.columns])))
    # Add the 'MarketingNameplate' column
    Prospect_ = Prospect_.withColumn(
        'MarketingNameplate',
        udf_marketing(
            struct(
                col('NetWorth'),
                col('Income'),
                col('NumberChildren'),
                col('NumberCreditCards'),
                col('Age'),
                col('CreditRating'),
                col('NumberCars')
            )
        )
    )
    print("Show prospect 2")
    Prospect_.show(5)
    ################################################
    print("PROSPECT__ READY")
    ################################################
    now = datetime.utcnow()
    

    DimDate = spark.sql("""
    SELECT SK_DateID FROM DimDate WHERE SK_DateID = 20201231
    """)
    ######################
    print("Dim Date:")
    DimDate.show()
    print("Show prospect")
    Prospect_.show()
    ######################
    Prospect_ = Prospect_.crossJoin(DimDate)
    ######################
    print("PROSPECT JOIN WITH DIM READY")
    ######################
    Prospect_.createOrReplaceTempView("Prospect_")
    ######################
    print("Temp VIEW WITH prospect")
    ######################
    # print("Show prospect")
    # Prospect_.show()
    ######################
    

    if not spark:
        raise RuntimeError("SparkSession is not initialized!")
    ######################
    print("Inerting in prospect")
    ######################
    spark.sql(
    """
        INSERT INTO Prospect (
               AgencyID, 
               BatchID,
               IsCustomer,
               SK_RecordDateID,
               SK_UpdateDateID,
               LastName,
               FirstName,
               MiddleInitial,
               Gender,
               AddressLine1,
               AddressLine2,
               PostalCode,
               City,
               State,
               Country,
               Phone,
               Income,
               NumberCars,
               NumberChildren,
               MaritalStatus,
               Age,
               CreditRating,
               OwnOrRentFlag,
               Employer,
               NumberCreditCards,
               NetWorth,
               MarketingNameplate)
        SELECT p.AgencyID, 
               1, 
               CASE
                   WHEN dc.Status = 'ACTIVE' THEN True ELSE False
               END,
               p.SK_DateID,
               p.SK_DateID,
               p.LastName,
               p.FirstName,
               p.MiddleInitial,
               p.Gender,
               p.AddressLine1,
               p.AddressLine2,
               p.PostalCode,
               p.City,
               p.State,
               p.Country,
               p.Phone,
               p.Income,
               p.NumberCars,
               p.NumberChildren,
               p.MaritalStatus,
               p.Age,
               p.CreditRating,
               p.OwnOrRentFlag,
               p.Employer,
               p.NumberCreditCards,
               p.NetWorth,
               p.MarketingNameplate
        FROM Prospect_ p
        LEFT JOIN DimCustomer dc ON 
        upper(p.FirstName) = upper(dc.FirstName) AND upper(p.LastName) = upper(dc.LastName)
        AND upper(p.AddressLine1) = upper(dc.AddressLine1) AND upper(p.AddressLine2) = upper(dc.AddressLine2)
        AND upper(p.PostalCode) = upper(dc.PostalCode)
    """)
    return Prospect_

def load_staging_Industry(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    schema = """
        `IN_ID` String,
        `IN_NAME` String,
        `IN_SC_ID` String
    """
    Industry_ = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
.load(f"{staging_area_folder}/Industry.txt")       
    )
    Industry_.createOrReplaceTempView("industry")
    
#     spark.sql(
#     """
#         INSERT INTO Industry(IN_ID, IN_NAME, IN_SC_ID)
#         SELECT i.IN_ID, i.IN_NAME, i.IN_SC_ID FROM industry i
#     """)
    return Industry_




def load_customers(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    file_rdd = spark.read.text(f"{staging_area_folder}/CustomerMgmt.xml", wholetext=True).rdd
    new_customer_records_rdd = file_rdd.flatMap(customer_parser)
    new_customer_schema = StructType([
        StructField("C_ID", StringType(), False),
        StructField("C_TAX_ID", StringType(), False),
        StructField("C_GNDR", StringType(), True),
        StructField("C_TIER", StringType(), True),
        StructField("C_DOB", StringType(), False),
        StructField("C_L_NAME", StringType(), False),
        StructField("C_F_NAME", StringType(), False),
        StructField("C_M_NAME", StringType(), True),
        StructField("C_ADLINE1", StringType(), False),
        StructField("C_ADLINE2", StringType(), True),
        StructField("C_ZIPCODE", StringType(), False),
        StructField("C_CITY", StringType(), False),
        StructField("C_STATE_PROV", StringType(), False),
        StructField("C_CTRY", StringType(), False),
        StructField("C_PRIM_EMAIL", StringType(), False),
        StructField("C_ALT_EMAIL", StringType(), True),
        StructField("C_PHONE_1", StringType(), True),
        StructField("C_PHONE_2", StringType(), True),
        StructField("C_PHONE_3", StringType(), True),
        StructField("C_LCL_TX_ID", StringType(), False),
        StructField("C_NAT_TX_ID", StringType(), False),
        StructField("CA_ID", StringType(), False),
        StructField("CA_TAX_ST", StringType(), False),
        StructField("CA_B_ID", StringType(), False),
        StructField("CA_NAME", StringType(), True)])
    customer_schema = StructType([
        StructField("C_ID", StringType(), True),
        StructField("C_TAX_ID", StringType(), True),
        StructField("C_GNDR", StringType(), True),
        StructField("C_TIER", StringType(), True),
        StructField("C_DOB", StringType(), True),
        StructField("C_L_NAME", StringType(), True),
        StructField("C_F_NAME", StringType(), True),
        StructField("C_M_NAME", StringType(), True),
        StructField("C_ADLINE1", StringType(), True),
        StructField("C_ADLINE2", StringType(), True),
        StructField("C_ZIPCODE", StringType(), True),
        StructField("C_CITY", StringType(), True),
        StructField("C_STATE_PROV", StringType(), True),
        StructField("C_CTRY", StringType(), True),
        StructField("C_PRIM_EMAIL", StringType(), True),
        StructField("C_ALT_EMAIL", StringType(), True),
        StructField("C_PHONE_1", StringType(), True),
        StructField("C_PHONE_2", StringType(), True),
        StructField("C_PHONE_3", StringType(), True),
        StructField("C_LCL_TX_ID", StringType(), True),
        StructField("C_NAT_TX_ID", StringType(), True)])
    new_customer_df = new_customer_records_rdd.toDF(new_customer_schema).select("C_ID", "C_TAX_ID", "C_GNDR", "C_TIER", "C_DOB", "C_L_NAME", "C_F_NAME", "C_M_NAME", "C_ADLINE1", "C_ADLINE2", "C_ZIPCODE", "C_CITY", "C_STATE_PROV", "C_CTRY", "C_PRIM_EMAIL", "C_ALT_EMAIL", "C_PHONE_1", "C_PHONE_2", "C_PHONE_3", "C_LCL_TX_ID", "C_NAT_TX_ID")
    update_customer_rdd = file_rdd.flatMap(update_customer_parser)
    update_customer_df = update_customer_rdd.toDF(customer_schema)
    customers_not_updated = new_customer_df.join(update_customer_df, on=['C_ID'], how='left_anti')
    customers_updated = new_customer_df.join(update_customer_df, on=['C_ID'], how='inner')
    columns = []
    for index, column in enumerate(customers_updated.columns):
        if index <= 20:
            columns.append(column)
        else:
            columns.append(column+'_update')
    # Function for update customers
    def customer_updater(row):
        new_row= [row.C_ID]
        for column in columns:
            if column != 'C_ID' and (not '_update' in column):
                if not getattr(row,column+'_update') is None:
                    new_row.append(getattr(row,column+'_update'))
                else:
                    new_row.append(getattr(row,column))
        return new_row

    customers_updated = customers_updated.toDF(*columns).rdd
    customers_updated = customers_updated.map(customer_updater).toDF(customer_schema)
    Customers = customers_not_updated.union(customers_updated)
    Customers.createOrReplaceTempView("customers")

    dimCustomer = spark.sql("""
                       Select 
                       monotonically_increasing_id() AS SK_CustomerID,
                       c.C_ID as CustomerID,
                       C_TAX_ID as TaxID,
                       C_L_NAME as LastName,
                       C_F_NAME as FirstName,
                       C_M_NAME as MiddleInitial,
                       C_GNDR as Gender,
                       C_TIER as Tier,
                       C_DOB as DOB,
                       C_ADLINE1 as AddressLine1,
                       C_ADLINE2 as AddressLine2,
                       C_ZIPCODE as PostalCode,
                       C_CITY as City,
                       C_STATE_PROV as StateProv,
                       C_CTRY as Country,
                       C_PHONE_1 as Phone1,
                       C_PHONE_2 as Phone2,
                       C_PHONE_3 as Phone3,
                       C_PRIM_EMAIL as Email1,
                       C_ALT_EMAIL as Email2,
                       NAT.TX_NAME as NationalTaxRateDesc,
                       NAT.TX_RATE as NationalTaxRate,
                       LCL.TX_NAME as LocalTaxRateDesc,
                       LCL.TX_RATE as LocalTaxRate,
                       AgencyID as AgencyID,
                       CreditRating as CreditRating,
                       NetWorth as NetWorth,
                        COALESCE(CASE 
                            WHEN NetWorth > 1000000 THEN 'HighValue+' 
                            ELSE NULL 
                        END,
                       CASE 
                            WHEN NumberChildren > 3 THEN 'Expenses+' 
                            WHEN NumberCreditCards > 5 THEN 'Expenses+'
                            ELSE NULL 
                        END,
                       CASE 
                            WHEN Age > 45 THEN 'Boomer+' 
                            ELSE NULL 
                        END,
                       CASE 
                            WHEN Income < 50000 THEN 'MoneyAlert+' 
                            WHEN CreditRating < 600 THEN 'MoneyAlert+' 
                            WHEN NetWorth < 100000 THEN 'MoneyAlert+' 
                            ELSE Null 
                        END,
                       CASE 
                            WHEN NumberCars > 3 THEN 'Spender+' 
                            WHEN NumberCreditCards > 7 THEN 'Spender+' 
                            ELSE Null 
                        END,
                       CASE 
                            WHEN Age < 25 THEN 'Inherited' 
                            WHEN NetWorth > 100000 THEN 'Inherited'  
                            ELSE Null  
                        END) as MarketingNameplate, 
                       CAST('True' as BOOLEAN) as IsCurrent, 
                       CAST('1' as INT) as BatchID, 
                       to_date('2015-01-01', 'yyyy-MM-dd') as EffectiveDate, 
                       to_date('9999-12-31', 'yyyy-MM-dd') as EndDate                       
                       From customers as c 
                       left join TaxRate as NAT on c.C_NAT_TX_ID = NAT.TX_ID 
                       left join TaxRate as LCL on c.C_LCL_TX_ID = LCL.TX_ID 
                       left join Prospect as p on (c.C_L_NAME = p.LastName and c.C_F_NAME = p.FirstName 
                            and c.C_ADLINE1 = p.AddressLine1 and c.C_ADLINE2 =  p.AddressLine2 and c.C_ZIPCODE = p.PostalCode)""")

    inactive_accounts = file_rdd.flatMap(inactive_parser)
    inact_list = inactive_accounts.collect()
    inact_func = udf(lambda x: 'Inactive' if str(x) in inact_list else 'Active')

    dimCustomer = dimCustomer.withColumn('Status', inact_func(dimCustomer.CustomerID))
    dimCustomer.createOrReplaceTempView("tbldimCustomer")
    
    spark.sql(f"DROP TABLE if exists DimCustomer")
    
    dimCustomer.write.mode('overwrite').saveAsTable("DimCustomer", mode="overwrite")
    return dimCustomer

# COMMAND ----------

def load_account(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    file_rdd = spark.read.text(f"{staging_area_folder}/CustomerMgmt.xml", wholetext=True).rdd
    new_customer_records_rdd = file_rdd.flatMap(customer_parser)
    new_customer_schema = StructType([
        StructField("C_ID", StringType(), False),
        StructField("C_TAX_ID", StringType(), False),
        StructField("C_GNDR", StringType(), True),
        StructField("C_TIER", StringType(), True),
        StructField("C_DOB", StringType(), False),
        StructField("C_L_NAME", StringType(), False),
        StructField("C_F_NAME", StringType(), False),
        StructField("C_M_NAME", StringType(), True),
        StructField("C_ADLINE1", StringType(), False),
        StructField("C_ADLINE2", StringType(), True),
        StructField("C_ZIPCODE", StringType(), False),
        StructField("C_CITY", StringType(), False),
        StructField("C_STATE_PROV", StringType(), False),
        StructField("C_CTRY", StringType(), False),
        StructField("C_PRIM_EMAIL", StringType(), False),
        StructField("C_ALT_EMAIL", StringType(), True),
        StructField("C_PHONE_1", StringType(), True),
        StructField("C_PHONE_2", StringType(), True),
        StructField("C_PHONE_3", StringType(), True),
        StructField("C_LCL_TX_ID", StringType(), False),
        StructField("C_NAT_TX_ID", StringType(), False),
        StructField("CA_ID", StringType(), False),
        StructField("CA_TAX_ST", StringType(), False),
        StructField("CA_B_ID", StringType(), False),
        StructField("CA_NAME", StringType(), True)])
    account_schema = StructType([
        StructField("C_ID", StringType(), True),
        StructField("CA_ID", StringType(), True),
        StructField("CA_TAX_ST", StringType(), True),
        StructField("CA_B_ID", StringType(), True),
        StructField("CA_NAME", StringType(), True)])

    new_account_df = new_customer_records_rdd.toDF(new_customer_schema).select("C_ID", "CA_ID", "CA_TAX_ST", "CA_B_ID", "CA_NAME")
    add_account_records_rdd = file_rdd.flatMap(add_account_parser)
    add_account_df = add_account_records_rdd.toDF(account_schema)
    updated_account_rdd = file_rdd.flatMap(update_account_parser)
    updated_account_df = updated_account_rdd.toDF(account_schema)
    
    Accounts = new_account_df.union(add_account_df).join(updated_account_df, on=['C_ID','CA_ID'], how='left_anti').union(updated_account_df)
    inactive_accounts = file_rdd.flatMap(inactive_parser)
    inact_list = inactive_accounts.collect()
    inact_func = udf(lambda x: 'INAC' if str(x) in inact_list else 'ACTV')

    Accounts = Accounts.withColumn('CA_ST_ID', inact_func(Accounts.C_ID))
    
    Accounts.createOrReplaceTempView("accounts")

    dimAccount = spark.sql(""" Select monotonically_increasing_id() AS SK_AccountID,
                           CA_ID as AccountID,
                           C_ID as SK_CustomerID,
                           CA_B_ID as SK_BrokerID,
                           ST_NAME as Status,
                           CA_NAME as AccountDesc,
                           CA_TAX_ST as TaxStatus,
                           CAST('True' as BOOLEAN) as IsCurrent,
                           CAST('1' as INT) as BatchID,
                           to_date('2015-01-01', 'yyyy-MM-dd') as EffectiveDate, 
                           to_date('9999-12-31', 'yyyy-MM-dd') as EndDate 
                           From accounts join StatusType on accounts.CA_ST_ID = StatusType.ST_ID """)

    dimAccount.createOrReplaceTempView("dimAccount_tbl")
    spark.sql(f"DROP TABLE if exists DimAccount")
    dimAccount.write.mode('overwrite').saveAsTable( "DimAccount", mode="overwrite")
    return dimAccount


def load_staging_dim_trade(spark,dbname, staging_area_folder):
    trade_view = load_trade_view(spark, dbname, staging_area_folder)
    tradehistory_view = load_tradehistory_view(spark, dbname, staging_area_folder)
    
    trade = spark.sql("""
            SELECT T.T_ID,
                CASE WHEN (TH.TH_ST_ID = 'SBMT' AND T.T_TT_ID in ('TMS', 'TMB')) OR TH.TH_ST_ID = 'PNDG' THEN TH.TH_DTS ELSE NULL END as create_date,
                CASE WHEN (TH.TH_ST_ID = 'SBMT' AND T.T_TT_ID in ('TMS', 'TMB')) OR TH.TH_ST_ID = 'PNDG' THEN TH.TH_DTS ELSE NULL END as create_time,
                CASE WHEN TH.TH_ST_ID in ('CMPT', 'CNCL') THEN TH.TH_DTS ELSE NULL END as close_date,
                CASE WHEN TH.TH_ST_ID in ('CMPT', 'CNCL') THEN TH.TH_DTS ELSE NULL END as close_time,
             ST.ST_NAME,
             TT.TT_NAME,
             T.T_IS_CASH,
             T.T_QTY,
             T.T_BID_PRICE,
             T.T_EXEC_NAME,
             T.T_TRADE_PRICE,
             T.T_CA_ID,
             T.T_S_SYMB,
             TH.TH_DTS,
             T.T_CHRG,
             T.T_COMM,
             T.T_TAX
             FROM trade T
             INNER JOIN tradeHistory TH ON T.T_ID = TH.TH_T_ID
             INNER JOIN StatusType ST ON T.T_ST_ID = ST.ST_ID
             INNER JOIN TradeType TT ON T.T_TT_ID = TT.TT_ID
    """)
    create_date_dim = spark.sql("""
        SELECT SK_DateID as SK_CreateDateID, DateValue FROM DimDate
    """)
    create_time_dim = spark.sql("""
        SELECT SK_TimeID as SK_CreateTimeID, TimeValue FROM DimTime
    """)
    close_date_dim = spark.sql("""
        SELECT SK_DateID as SK_CloseDateID, DateValue FROM DimDate
    """)
    close_time_dim = spark.sql("""
        SELECT SK_TimeID as SK_CloseTimeID, TimeValue FROM DimTime
    """)

#     trade.show()
    
    trade = (
    trade
    .groupBy('T_ID', 'ST_NAME', 'TT_NAME', 'T_IS_CASH','T_CA_ID','T_S_SYMB','TH_DTS', 'T_QTY', 'T_BID_PRICE', 'T_EXEC_NAME', 'T_TRADE_PRICE', 'T_CHRG', 'T_COMM', 'T_TAX')
    .agg(
        collect_set(col('create_date')).alias('create_date'), 
        collect_set(col('create_time')).alias('create_time'),
        collect_set(col('close_date')).alias('close_date'),
        collect_set(col('close_time')).alias('close_time')
    )
    .select(
        expr('filter(create_date, element -> element is not null)')[0].alias('create_date'),
        expr('filter(create_time, element -> element is not null)')[0].alias('create_time'),
        expr('filter(close_date, element -> element is not null)')[0].alias('close_date'),
        expr('filter(close_time, element -> element is not null)')[0].alias('close_time'),
'T_ID', 'ST_NAME', 'TT_NAME', 'T_IS_CASH','T_CA_ID','T_S_SYMB','TH_DTS', 'T_QTY', 'T_BID_PRICE', 'T_EXEC_NAME', 'T_TRADE_PRICE', 'T_CHRG', 'T_COMM', 'T_TAX'
    )
)

    # Join with date
    trade = trade.join(create_date_dim, to_date(create_date_dim.DateValue) == to_date(trade.create_date), "left").join(create_time_dim, date_format(create_time_dim.TimeValue, "HH:mm:ss") == date_format(trade.create_time, "HH:mm:ss"), "left").join(close_date_dim, to_date(close_date_dim.DateValue) == to_date(trade.create_date), "left").join(close_time_dim, date_format(close_time_dim.TimeValue, "HH:mm:ss") == date_format(trade.create_time, "HH:mm:ss"), "left")

    # Create new view
    trade.createOrReplaceTempView("trade_insert")
    #trade.printSchema()
    
    trade_final=spark.sql("""
        SELECT  T_ID as TradeID,
         da.SK_BrokerID  as SK_BrokerID,
         SK_CreateDateID as SK_CreateDateID,
         SK_CreateTimeID as SK_CreateTimeID,
         SK_CloseDateID as SK_CloseDateID,
         SK_CloseTimeID as SK_CloseTimeID,
         ST_NAME as Status,
         TT_NAME as Type,
         T_IS_CASH as CashFlag,
         ds.SK_SecurityID as SK_SecurityID,
         ds.SK_CompanyID as SK_CompanyID,
         T_QTY as Quantity,
         T_BID_PRICE as BidPrice,
         da.SK_CustomerID as SK_CustomerID,
         da.SK_AccountID as SK_AccountID,
         T_EXEC_NAME as ExecutedBy,
         T_TRADE_PRICE as TradePrice,
         T_CHRG as Fee,
         T_COMM as Comission,
         T_TAX as Tax,
         1 as BatchID
        FROM trade_insert inner join DimAccount as da on trade_insert.T_CA_ID = da.SK_AccountID 
        AND trade_insert.TH_DTS BETWEEN da.EffectiveDate AND da.EndDate inner join DimSecurity
        as ds on (trade_insert.T_S_SYMB = ds.Symbol AND trade_insert.TH_DTS BETWEEN ds.EffectiveDate AND ds.EndDate)
    """)
  
    trade_final.createOrReplaceTempView("trade_insert")
    
    trade_final.write.option("overwriteSchema", "true").saveAsTable("DimTrade", mode="overwrite")
    return spark.sql("""SELECT * FROM DimTrade""")



def load_fact_cash_balances(spark,dbname, staging_area_folder):
    #spark.sql(f"USE {dbname}")
    schema = """
            `CT_CA_ID` INTEGER,
            `CT_DTS` TIMESTAMP,
            `CT_AMT` FLOAT,
            `CT_NAME` STRING
    """
    cash = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder}/CashTransaction.txt")
    )
    
    cash.createOrReplaceTempView("cashTrans")
    factCashBalances = spark.sql(""" 
                       Select SK_CustomerID, 
                           AccountID AS SK_AccountID, 
                           SK_DateID, 
                           sum(CT_AMT) as Cash, 
                           CAST('1' as INT) as BatchID 
                       From cashTrans join DimAccount as ac on (CT_CA_ID =ac.AccountID) 
                       join DimDate as dt on dt.DateValue = Date(CT_DTS) 
                       Group by AccountID, SK_CustomerID, SK_DateID""")
    
    
    
    factCashBalances.write.option("overwriteSchema", "true").saveAsTable("FactCashBalances", mode="overwrite")
    factCashBalances.show(3)
    return factCashBalances



def load_fact_holdings(spark,dbname, staging_area_folder):
    #spark.sql(f"USE {dbname}")
    schema = """
            `HH_H_T_ID` INTEGER,
            `HH_T_ID` INTEGER,
            `HH_BEFORE_QTY` FLOAT,
            `HH_AFTER_QTY` FLOAT
    """
    holding = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder}/HoldingHistory.txt")
    )
    holding.createOrReplaceTempView("holdings")
    factHoldings = spark.sql(""" 
                       Select 
                       SK_CustomerID, 
                       SK_AccountID, 
                       SK_SecurityID, 
                       SK_CompanyID,
                       TradePrice as CurrentPrice,
                       SK_CloseDateID as SK_DateID ,
                       SK_CloseTimeID as SK_TimeID,
                       HH_H_T_ID as TradeId,
                       HH_T_ID as CurrentTradeID,
                       HH_AFTER_QTY as CurrentHolding,
                       CAST('1' as INT) as BatchID 
                       From holdings join DimTrade as ac on (HH_T_ID =ac.TradeID)""")
    
    
    factHoldings.write.option("overwriteSchema", "true").saveAsTable("FactHoldings", mode="overwrite")
    factHoldings.show(2)
    
    return factHoldings


def load_fact_watches(spark,dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    # Customer ID, Ticker symbol, Datetime, activate or cancel watch
    schema = """
            `W_C_ID` BIGINT, 
            `W_S_SYMB` STRING,
            `W_DTS` DATE,
            `W_ACTION` STRING
    """
    
    spark.read.format("csv") \
        .option("delimiter", "|") \
        .schema(schema) \
        .load(f"{staging_area_folder}/WatchHistory.txt") \
    .createOrReplaceTempView("watches")
    
    actv_watches = spark.sql("SELECT * FROM watches").where(col("W_ACTION") == 'ACTV')
    actv_watches.createOrReplaceTempView("actv_watches")
    cncl_watches = spark.sql("SELECT * FROM watches").where(col("W_ACTION") == 'CNCL')
    cncl_watches.createOrReplaceTempView("cncl_watches")

    spark.sql("""
        SELECT w1.W_C_ID, w1.W_S_SYMB, w1.W_DTS AS DatePlaced, w2.W_DTS AS DateRemoved FROM actv_watches w1 LEFT JOIN cncl_watches w2 ON w1.W_C_ID = w2.W_C_ID AND w1.W_S_SYMB = w2.W_S_SYMB
    """).createOrReplaceTempView("watches")
    
    spark.sql("""
        INSERT INTO FactWatches(SK_CustomerID, SK_SecurityID, SK_DateID_DatePlaced, 
                                SK_DateID_DateRemoved, BatchID)
            SELECT 
                c.SK_CustomerID,
                s.SK_SecurityID,
                d1.SK_DateID AS SK_DateID_DatePlaced, 
                d2.SK_DateID AS SK_DateID_DateRemoved,
                1 AS BatchID
            FROM watches w LEFT JOIN 
                DimDate d1 ON DatePlaced = d1.DateValue LEFT JOIN 
                DimDate d2 ON DateRemoved = d2.DateValue LEFT JOIN
                DimSecurity s ON (
                    W_S_SYMB = s.Symbol AND 
                    DatePlaced >= s.EffectiveDate AND
                    DatePlaced < s.EndDate
                ) LEFT JOIN 
                DimCustomer c ON (
                    W_C_ID = c.CustomerID
                )
    """)
    
    return spark.sql("SELECT * FROM FactWatches")


def load_dimen_customer(spark,dbname, staging_area_folder_upl):
    #spark.sql(f"USE {dbname}")
    schema = """
            `CDC_FLAG` STRING,
            `CDC_DSN` LONG,
            `C_ID` STRING,
            `C_TAX_ID` STRING,
            `C_ST_ID` STRING,
            `C_L_NAME` STRING,
            `C_F_NAME` STRING,
            `C_M_NAME` STRING,
            `C_GNDR` STRING,
            `C_TIER` STRING,
            `C_DOB` STRING,
            `C_ADLINE1` STRING,
            `C_ADLINE2` STRING,
            `C_ZIPCODE` STRING,
            `C_CITY` STRING,
            `C_STATE_PROV` STRING,
            `C_CTRY` STRING,
            `C_CTRY_1` STRING,
            `C_AREA_1` STRING,
            `C_LOCAL_1` STRING,
            `C_EXT_1` STRING,
            `C_CTRY_2` STRING,
            `C_AREA_2` STRING,
            `C_LOCAL_2` STRING,
            `C_EXT_2` STRING,
            `C_CTRY_3` STRING,
            `C_AREA_3` STRING,
            `C_LOCAL_3` STRING,
            `C_EXT_3` STRING,
            `C_EMAIL_1` STRING,
            `C_EMAIL_2` STRING,
            `C_LCL_TX_ID` STRING,
            `C_NAT_TX_ID` STRING
    """
    customer_base = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder_upl}/Customer.txt")
    )
    customer_base.createOrReplaceTempView("customer_base_batch1")    
    new_customer_df= spark.sql(""" Select *
                           From customer_base_batch1 as a where a.CDC_FLAG = 'I'  """)

    update_customer_df=spark.sql(""" Select *
                           From customer_base_batch1 as a where a.CDC_FLAG = 'U'  """)
    
    Customers = new_customer_df.join(update_customer_df, on=['C_ID'], how='left_anti')
    
    Customers.createOrReplaceTempView("customers")
    
    #### Added on line 2540 "ST_NAME as Status, "
    dimCustomer = spark.sql("""
                       Select 
                       CDC_DSN AS SK_CustomerID,
                       c.C_ID as CustomerID,
                       C_TAX_ID as TaxID,
                        ST_NAME as Status, 
                       C_L_NAME as LastName,
                       C_F_NAME as FirstName,
                       C_M_NAME as MiddleInitial,
                       (CASE WHEN (C_GNDR = 'F' OR C_GNDR='M') THEN C_GNDR ELSE 'U' END) as Gender,
                       C_TIER as Tier,
                       C_DOB as DOB,
                       C_ADLINE1 as AddressLine1,
                       C_ADLINE2 as AddressLine2,
                       C_ZIPCODE as PostalCode,
                       C_CITY as City,
                       C_STATE_PROV as StateProv,
                       C_CTRY as Country,
                       (
                           CASE 
                           WHEN (C_CTRY_1 IS NOT NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NULL) 
                           THEN CONCAT('+' , C_CTRY_1 , ' (' , C_AREA_1 , ') ' , C_LOCAL_1)
                           WHEN (C_CTRY_1 IS NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NULL) 
                           THEN CONCAT(' (' , C_AREA_1 , ') ' , C_LOCAL_1)
                           WHEN (C_CTRY_1 IS NULL AND C_AREA_1 IS NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NULL) 
                           THEN C_LOCAL_1
                           
                           WHEN (C_CTRY_1 IS NOT NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NOT NULL) 
                           THEN CONCAT('+' , C_CTRY_1 , ' (' , C_AREA_1 , ') ' , C_LOCAL_1, C_EXT_1)
                           WHEN (C_CTRY_1 IS NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NOT NULL) 
                           THEN CONCAT(' (' , C_AREA_1 , ') ' , C_LOCAL_1, C_EXT_1)
                           WHEN (C_CTRY_1 IS NULL AND C_AREA_1 IS NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NOT NULL) 
                           THEN CONCAT(C_LOCAL_1, C_EXT_1)
                           
                           ELSE NULL
                           END
                       ) as Phone1,
                       (
                           CASE 
                           WHEN (C_CTRY_2 IS NOT NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NULL) 
                           THEN CONCAT('+' , C_CTRY_2 , ' (' , C_AREA_2 , ') ' , C_LOCAL_2)
                           WHEN (C_CTRY_2 IS NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NULL) 
                           THEN CONCAT(' (' , C_AREA_2 , ') ' , C_LOCAL_2)
                           WHEN (C_CTRY_2 IS NULL AND C_AREA_2 IS NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NULL) 
                           THEN C_LOCAL_2
                           
                           WHEN (C_CTRY_2 IS NOT NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NOT NULL) 
                           THEN CONCAT('+' , C_CTRY_2 , ' (' , C_AREA_2 , ') ' , C_LOCAL_2, C_EXT_2)
                           WHEN (C_CTRY_2 IS NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NOT NULL) 
                           THEN CONCAT(' (' , C_AREA_2 , ') ' , C_LOCAL_2, C_EXT_2)
                           WHEN (C_CTRY_2 IS NULL AND C_AREA_2 IS NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NOT NULL) 
                           THEN CONCAT(C_LOCAL_2, C_EXT_2)
                           
                           ELSE NULL
                           END
                       ) as Phone2,
                       (
                           CASE 
                           WHEN (C_CTRY_3 IS NOT NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NULL) 
                           THEN CONCAT('+' , C_CTRY_3 , ' (' , C_AREA_3 , ') ' , C_LOCAL_3)
                           WHEN (C_CTRY_3 IS NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NULL) 
                           THEN CONCAT(' (' , C_AREA_3 , ') ' , C_LOCAL_3)
                           WHEN (C_CTRY_3 IS NULL AND C_AREA_3 IS NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NULL) 
                           THEN C_LOCAL_3
                           
                           WHEN (C_CTRY_3 IS NOT NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NOT NULL) 
                           THEN CONCAT('+' , C_CTRY_3 , ' (' , C_AREA_3 , ') ' , C_LOCAL_3, C_EXT_3)
                           WHEN (C_CTRY_3 IS NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NOT NULL) 
                           THEN CONCAT(' (' , C_AREA_3 , ') ' , C_LOCAL_3, C_EXT_3)
                           WHEN (C_CTRY_3 IS NULL AND C_AREA_3 IS NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NOT NULL) 
                           THEN CONCAT(C_LOCAL_3, C_EXT_3)
                           
                           ELSE NULL
                           END
                       ) as Phone3,
                       C_EMAIL_1 as Email1,
                       C_EMAIL_2 as Email2,
                       NAT.TX_NAME as NationalTaxRateDesc,
                       NAT.TX_RATE as NationalTaxRate,
                       LCL.TX_NAME as LocalTaxRateDesc,
                       LCL.TX_RATE as LocalTaxRate,
                       AgencyID as AgencyID,
                       CreditRating as CreditRating,
                       NetWorth as NetWorth,
                        COALESCE(CASE 
                            WHEN NetWorth > 1000000 THEN 'HighValue+' 
                            ELSE NULL 
                        END,
                       CASE 
                            WHEN NumberChildren > 3 THEN 'Expenses+' 
                            WHEN NumberCreditCards > 5 THEN 'Expenses+'
                            ELSE NULL 
                        END,
                       CASE 
                            WHEN Age > 45 THEN 'Boomer+' 
                            ELSE NULL 
                        END,
                       CASE 
                            WHEN Income < 50000 THEN 'MoneyAlert+' 
                            WHEN CreditRating < 600 THEN 'MoneyAlert+' 
                            WHEN NetWorth < 100000 THEN 'MoneyAlert+' 
                            ELSE Null 
                        END,
                       CASE 
                            WHEN NumberCars > 3 THEN 'Spender+' 
                            WHEN NumberCreditCards > 7 THEN 'Spender+' 
                            ELSE Null 
                        END,
                       CASE 
                            WHEN Age < 25 THEN 'Inherited' 
                            WHEN NetWorth > 100000 THEN 'Inherited'  
                            ELSE Null  
                        END) as MarketingNameplate, 
                       CAST('True' as BOOLEAN) as IsCurrent, 
                       CAST('2' as INT) as BatchID, 
                       to_date('2015-01-01', 'yyyy-MM-dd') as EffectiveDate, 
                       to_date('9999-12-31', 'yyyy-MM-dd') as EndDate
                       From customers as c 
                       left join TaxRate as NAT on c.C_NAT_TX_ID = NAT.TX_ID 
                       left join TaxRate as LCL on c.C_LCL_TX_ID = LCL.TX_ID 
                       left join Prospect as p on (c.C_L_NAME = p.LastName and c.C_F_NAME = p.FirstName 
                            and c.C_ADLINE1 = p.AddressLine1 and c.C_ADLINE2 =  p.AddressLine2 and c.C_ZIPCODE = p.PostalCode)
                        left join StatusType on StatusType.ST_ID = c.C_ST_ID """)
    
    dimCustomer.createOrReplaceTempView("dimCustomer_stream")

    dimCustomer = cast_to_target_schema(spark,"dimCustomer_stream", "DimCustomer")
     
    dimCustomer.write.mode("append").saveAsTable( "DimCustomer", mode="append")
    
    return dimCustomer



def load_dimen_account(spark,dbname, staging_area_folder_upl):
    #spark.sql(f"USE {dbname}")
    schema = """
            `CDC_FLAG` STRING,
            `CDC_DSN` INTEGER,
            `CA_ID` LONG,
            `CA_B_ID` STRING,
            `CA_C_ID` STRING,
            `CA_NAME` STRING,
            `CA_TAX_ST` STRING,
            `CA_ST_ID` STRING
    """
    account_base = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder_upl}/Account.txt")
    )
    account_base.createOrReplaceTempView("account_base_batch1")
    
    account_base.show(2)
    
    add_account_df= spark.sql(""" Select *
                           From account_base_batch1 as a where a.CDC_FLAG = 'I'  """)

    updated_account_df=spark.sql(""" Select *
                           From account_base_batch1 as a where a.CDC_FLAG = 'U'  """)
    
    #new_account_df = new_customer_records_rdd.toDF(new_customer_schema).select("C_ID", "CA_ID", "CA_TAX_ST", "CA_B_ID", "CA_NAME")

    Accounts = account_base.union(add_account_df).join(updated_account_df, on=['CA_ID'], how='left_anti').union(updated_account_df)
    Accounts.createOrReplaceTempView("accounts")
    
    dimAccount = spark.sql(""" Select CDC_DSN AS SK_AccountID,
                           CA_ID as AccountID,
                           CA_C_ID as SK_CustomerID,
                           CA_B_ID as SK_BrokerID,
                           ST_NAME as Status,
                           CA_NAME as AccountDesc,
                           CA_TAX_ST as TaxStatus,
                           CAST('True' as BOOLEAN) as IsCurrent,
                           CAST('2' as INT) as BatchID,
                           to_date('2015-01-01', 'yyyy-MM-dd') as EffectiveDate, 
                           to_date('9999-12-31', 'yyyy-MM-dd') as EndDate 
                           From accounts join StatusType on accounts.CA_ST_ID = StatusType.ST_ID """)

    #dimAccount.printSchema()
    
    dimAccount.createOrReplaceTempView("dimAccount_stream")
    dimAccount = cast_to_target_schema(spark,"dimAccount_stream", "DimAccount")

    dimAccount.write.mode("append").saveAsTable( "DimAccount", mode="append")
    
    return dimAccount



# It is almost the same function that load_dimen_customer 
def load_dimen_customer2(spark,dbname, staging_area_folder_up2):
    #spark.sql(f"USE {dbname}")
    schema = """
            `CDC_FLAG` STRING,
            `CDC_DSN` LONG,
            `C_ID` STRING,
            `C_TAX_ID` STRING,
            `C_ST_ID` STRING,
            `C_L_NAME` STRING,
            `C_F_NAME` STRING,
            `C_M_NAME` STRING,
            `C_GNDR` STRING,
            `C_TIER` STRING,
            `C_DOB` STRING,
            `C_ADLINE1` STRING,
            `C_ADLINE2` STRING,
            `C_ZIPCODE` STRING,
            `C_CITY` STRING,
            `C_STATE_PROV` STRING,
            `C_CTRY` STRING,
            `C_CTRY_1` STRING,
            `C_AREA_1` STRING,
            `C_LOCAL_1` STRING,
            `C_EXT_1` STRING,
            `C_CTRY_2` STRING,
            `C_AREA_2` STRING,
            `C_LOCAL_2` STRING,
            `C_EXT_2` STRING,
            `C_CTRY_3` STRING,
            `C_AREA_3` STRING,
            `C_LOCAL_3` STRING,
            `C_EXT_3` STRING,
            `C_EMAIL_1` STRING,
            `C_EMAIL_2` STRING,
            `C_LCL_TX_ID` STRING,
            `C_NAT_TX_ID` STRING
    """
    customer_base = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder_up2}/Customer.txt")
    )
    customer_base.createOrReplaceTempView("customer_base_batch1")    
    new_customer_df= spark.sql(""" Select *
                           From customer_base_batch1 as a where a.CDC_FLAG = 'I'  """)

    update_customer_df=spark.sql(""" Select *
                           From customer_base_batch1 as a where a.CDC_FLAG = 'U'  """)
    
    Customers = new_customer_df.join(update_customer_df, on=['C_ID'], how='left_anti')
    
    Customers.createOrReplaceTempView("customers")
        
    dimCustomer = spark.sql("""
                       Select 
                       CDC_DSN AS SK_CustomerID,
                       c.C_ID as CustomerID,
                       C_TAX_ID as TaxID,
                       C_L_NAME as LastName,
                       C_F_NAME as FirstName,
                       C_M_NAME as MiddleInitial,
                       (CASE WHEN (C_GNDR = 'F' OR C_GNDR='M') THEN C_GNDR ELSE 'U' END) as Gender,
                       C_TIER as Tier,
                       C_DOB as DOB,
                       C_ADLINE1 as AddressLine1,
                       C_ADLINE2 as AddressLine2,
                       C_ZIPCODE as PostalCode,
                       C_CITY as City,
                       C_STATE_PROV as StateProv,
                       C_CTRY as Country,
                       (
                           CASE 
                           WHEN (C_CTRY_1 IS NOT NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NULL) 
                           THEN CONCAT('+' , C_CTRY_1 , ' (' , C_AREA_1 , ') ' , C_LOCAL_1)
                           WHEN (C_CTRY_1 IS NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NULL) 
                           THEN CONCAT(' (' , C_AREA_1 , ') ' , C_LOCAL_1)
                           WHEN (C_CTRY_1 IS NULL AND C_AREA_1 IS NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NULL) 
                           THEN C_LOCAL_1
                           
                           WHEN (C_CTRY_1 IS NOT NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NOT NULL) 
                           THEN CONCAT('+' , C_CTRY_1 , ' (' , C_AREA_1 , ') ' , C_LOCAL_1, C_EXT_1)
                           WHEN (C_CTRY_1 IS NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NOT NULL) 
                           THEN CONCAT(' (' , C_AREA_1 , ') ' , C_LOCAL_1, C_EXT_1)
                           WHEN (C_CTRY_1 IS NULL AND C_AREA_1 IS NULL AND C_LOCAL_1 IS NOT NULL AND C_EXT_1 IS NOT NULL) 
                           THEN CONCAT(C_LOCAL_1, C_EXT_1)
                           
                           ELSE NULL
                           END
                       ) as Phone1,
                       (
                           CASE 
                           WHEN (C_CTRY_2 IS NOT NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NULL) 
                           THEN CONCAT('+' , C_CTRY_2 , ' (' , C_AREA_2 , ') ' , C_LOCAL_2)
                           WHEN (C_CTRY_2 IS NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NULL) 
                           THEN CONCAT(' (' , C_AREA_2 , ') ' , C_LOCAL_2)
                           WHEN (C_CTRY_2 IS NULL AND C_AREA_2 IS NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NULL) 
                           THEN C_LOCAL_2
                           
                           WHEN (C_CTRY_2 IS NOT NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NOT NULL) 
                           THEN CONCAT('+' , C_CTRY_2 , ' (' , C_AREA_2 , ') ' , C_LOCAL_2, C_EXT_2)
                           WHEN (C_CTRY_2 IS NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NOT NULL) 
                           THEN CONCAT(' (' , C_AREA_2 , ') ' , C_LOCAL_2, C_EXT_2)
                           WHEN (C_CTRY_2 IS NULL AND C_AREA_2 IS NULL AND C_LOCAL_2 IS NOT NULL AND C_EXT_2 IS NOT NULL) 
                           THEN CONCAT(C_LOCAL_2, C_EXT_2)
                           
                           ELSE NULL
                           END
                       ) as Phone2,
                       (
                           CASE 
                           WHEN (C_CTRY_3 IS NOT NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NULL) 
                           THEN CONCAT('+' , C_CTRY_3 , ' (' , C_AREA_3 , ') ' , C_LOCAL_3)
                           WHEN (C_CTRY_3 IS NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NULL) 
                           THEN CONCAT(' (' , C_AREA_3 , ') ' , C_LOCAL_3)
                           WHEN (C_CTRY_3 IS NULL AND C_AREA_3 IS NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NULL) 
                           THEN C_LOCAL_3
                           
                           WHEN (C_CTRY_3 IS NOT NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NOT NULL) 
                           THEN CONCAT('+' , C_CTRY_3 , ' (' , C_AREA_3 , ') ' , C_LOCAL_3, C_EXT_3)
                           WHEN (C_CTRY_3 IS NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NOT NULL) 
                           THEN CONCAT(' (' , C_AREA_3 , ') ' , C_LOCAL_3, C_EXT_3)
                           WHEN (C_CTRY_3 IS NULL AND C_AREA_3 IS NULL AND C_LOCAL_3 IS NOT NULL AND C_EXT_3 IS NOT NULL) 
                           THEN CONCAT(C_LOCAL_3, C_EXT_3)
                           
                           ELSE NULL
                           END
                       ) as Phone3,
                       C_EMAIL_1 as Email1,
                       C_EMAIL_2 as Email2,
                       NAT.TX_NAME as NationalTaxRateDesc,
                       NAT.TX_RATE as NationalTaxRate,
                       LCL.TX_NAME as LocalTaxRateDesc,
                       LCL.TX_RATE as LocalTaxRate,
                       AgencyID as AgencyID,
                       CreditRating as CreditRating,
                       NetWorth as NetWorth,
                        COALESCE(CASE 
                            WHEN NetWorth > 1000000 THEN 'HighValue+' 
                            ELSE NULL 
                        END,
                       CASE 
                            WHEN NumberChildren > 3 THEN 'Expenses+' 
                            WHEN NumberCreditCards > 5 THEN 'Expenses+'
                            ELSE NULL 
                        END,
                       CASE 
                            WHEN Age > 45 THEN 'Boomer+' 
                            ELSE NULL 
                        END,
                       CASE 
                            WHEN Income < 50000 THEN 'MoneyAlert+' 
                            WHEN CreditRating < 600 THEN 'MoneyAlert+' 
                            WHEN NetWorth < 100000 THEN 'MoneyAlert+' 
                            ELSE Null 
                        END,
                       CASE 
                            WHEN NumberCars > 3 THEN 'Spender+' 
                            WHEN NumberCreditCards > 7 THEN 'Spender+' 
                            ELSE Null 
                        END,
                       CASE 
                            WHEN Age < 25 THEN 'Inherited' 
                            WHEN NetWorth > 100000 THEN 'Inherited'  
                            ELSE Null  
                        END) as MarketingNameplate, 
                       CAST('True' as BOOLEAN) as IsCurrent, 
                       CAST('3' as INT) as BatchID, 
                       to_date('2015-01-01', 'yyyy-MM-dd') as EffectiveDate, 
                       to_date('9999-12-31', 'yyyy-MM-dd') as EndDate
                       From customers as c 
                       left join TaxRate as NAT on c.C_NAT_TX_ID = NAT.TX_ID 
                       left join TaxRate as LCL on c.C_LCL_TX_ID = LCL.TX_ID 
                       left join Prospect as p on (c.C_L_NAME = p.LastName and c.C_F_NAME = p.FirstName 
                            and c.C_ADLINE1 = p.AddressLine1 and c.C_ADLINE2 =  p.AddressLine2 and c.C_ZIPCODE = p.PostalCode)""")



    
    #dimCustomer.printSchema()
    
    dimCustomer.write.mode("append").saveAsTable( "DimCustomer", mode="append")
    
    return dimCustomer


def load_dimen_account2(spark,dbname, staging_area_folder_up2):
    #spark.sql(f"USE {dbname}")
    schema = """
            `CDC_FLAG` STRING,
            `CDC_DSN` INTEGER,
            `CA_ID` LONG,
            `CA_B_ID` INTEGER,
            `CA_C_ID` STRING,
            `CA_NAME` STRING,
            `CA_TAX_ST` STRING,
            `CA_ST_ID` STRING
    """
    account_base = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder_up2}/Account.txt")
    )
    account_base.createOrReplaceTempView("account_base_batch1")
    
    
    add_account_df= spark.sql(""" Select *
                           From account_base_batch1 as a where a.CDC_FLAG = 'I'  """)

    updated_account_df=spark.sql(""" Select *
                           From account_base_batch1 as a where a.CDC_FLAG = 'U'  """)
    
    #new_account_df = new_customer_records_rdd.toDF(new_customer_schema).select("C_ID", "CA_ID", "CA_TAX_ST", "CA_B_ID", "CA_NAME")

    Accounts = account_base.union(add_account_df).join(updated_account_df, on=['CA_ID'], how='left_anti').union(updated_account_df)
    Accounts.createOrReplaceTempView("accounts")
    
    dimAccount = spark.sql(""" Select CDC_DSN AS SK_AccountID,
                           CA_ID as AccountID,
                           C_ID as SK_CustomerID,
                           CA_B_ID as SK_BrokerID,
                           ST_NAME as Status,
                           CA_NAME as AccountDesc,
                           CA_TAX_ST as TaxStatus,
                           CAST('True' as BOOLEAN) as IsCurrent,
                           CAST('3' as INT) as BatchID,
                           to_date('2015-01-01', 'yyyy-MM-dd') as EffectiveDate, 
                           to_date('9999-12-31', 'yyyy-MM-dd') as EndDate 
                           From accounts join StatusType on accounts.CA_ST_ID = StatusType.ST_ID """)

    #dimAccount.printSchema()
    
    dimAccount.write.mode("append").saveAsTable( "DimAccount", mode="append")
    
    return dimAccount