
from datetime import datetime
import findspark
import pandas as pd

from src.aux_functions2 import get_marketingnameplate
findspark.init()
from pyspark.sql.functions import expr, udf, struct
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType, DateType, TimestampType
#from src.aux_functions import get_marketingnameplate
from src.exe_create_functions import create_fact_market_history, create_prospect_table

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
from src.aux_functions2 import get_marketingnameplate, cast_to_target_schema, get_max
from src.exe_create_functions import create_dim_company, create_dim_security, create_prospect_table
from src.parser_functions import add_account_parser, customer_parser, inactive_parser, update_account_parser, update_customer_parser



def load_update_dimen_trade(spark, dbname,staging_area_folder_up1):
    #spark.sql(f"USE {dbname}")
    schema = """
            `CDC_FLAG` String,
            `CDC_DSN` String,
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
    trade_base = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder_up1}/Trade.txt")
    )
    trade_base.createOrReplaceTempView("trade_base_batch1")
        
    add_trade_df= spark.sql(""" Select *
                           From trade_base_batch1 as a where a.CDC_FLAG = 'I'  """)

    updated_trade_df=spark.sql(""" Select *
                           From trade_base_batch1 as a where a.CDC_FLAG = 'U'  """)
    

    trades = trade_base.union(add_trade_df).join(updated_trade_df, on=['T_ID'], how='left_anti').union(updated_trade_df)
    trades.createOrReplaceTempView("trade_view")
    
    
    trade = spark.sql("""
            SELECT T.T_ID,
                CASE WHEN T.CDC_FLAG = 'I' then TH.TH_DTS ELSE NULL END as create_date,
                CASE WHEN T.CDC_FLAG = 'I' then TH.TH_DTS ELSE NULL END as create_time,
                CASE 
                WHEN T.CDC_FLAG = 'I' then NULL 
                WHEN TH.TH_ST_ID in ('CMPT', 'CNCL') THEN TH.TH_DTS ELSE NULL END as close_date,
                CASE WHEN T.CDC_FLAG = 'I' then NULL 
                WHEN TH.TH_ST_ID in ('CMPT', 'CNCL') THEN TH.TH_DTS ELSE NULL END as close_time,
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
             FROM trade_view T
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
    .groupBy( 'T_ID', 'ST_NAME', 'TT_NAME', 'T_IS_CASH','T_CA_ID','T_S_SYMB','TH_DTS', 'T_QTY', 'T_BID_PRICE', 'T_EXEC_NAME', 'T_TRADE_PRICE', 'T_CHRG', 'T_COMM', 'T_TAX')
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
    
    dimTrade=spark.sql("""
        SELECT  INT(T_ID) as TradeID,
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
         2 as BatchID
        FROM trade_insert inner join DimAccount as da on trade_insert.T_CA_ID = da.SK_AccountID 
        AND trade_insert.TH_DTS BETWEEN da.EffectiveDate AND da.EndDate inner join DimSecurity
        as ds on (trade_insert.T_S_SYMB = ds.Symbol AND trade_insert.TH_DTS BETWEEN ds.EffectiveDate AND ds.EndDate)
    """)
  
    #dimAccount.printSchema()
    
    dimTrade.write.mode("append").saveAsTable( "DimTrade", mode="append")
    
    return dimTrade



def load_update_fact_cash_balances(spark,dbname, staging_area_folder_upl):
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
        .load(f"{staging_area_folder_upl}/CashTransaction.txt")
    )
    
    cash.createOrReplaceTempView("cashTrans")
    factCashBalances = spark.sql(""" 
                       Select SK_CustomerID, 
                           AccountID AS SK_AccountID, 
                           SK_DateID, 
                           sum(CT_AMT) as Cash, 
                           CAST('2' as INT) as BatchID 
                       From cashTrans join DimAccount as ac on (CT_CA_ID =ac.AccountID) 
                       join DimDate as dt on dt.DateValue = Date(CT_DTS) 
                       Group by AccountID, SK_CustomerID, SK_DateID""")
    
    
    
    factCashBalances.write.option("append", "true").saveAsTable("FactCashBalances", mode="append")
#     factCashBalances.show(3)
    return factCashBalances


def load_update_fact_holdings(spark,dbname, staging_area_folder_upl):
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
        .load(f"{staging_area_folder_upl}/HoldingHistory.txt")
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
                       CAST('2' as INT) as BatchID 
                       From holdings join DimTrade as ac on (HH_T_ID =ac.TradeID)""")
    
    
    factHoldings.write.option("append", "true").saveAsTable("FactHoldings", mode="append")
    factHoldings.show(2)
    
    return factHoldings



def load_update_fact_watches(spark, dbname, staging_area_folder_upl):
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
        .load(f"{staging_area_folder_upl}/WatchHistory.txt") \
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
                2 AS BatchID
            FROM watches w LEFT JOIN 
                DimDate d1 ON DatePlaced = d1.DateValue LEFT JOIN 
                DimDate d2 ON DateRemoved = d2.DateValue LEFT JOIN
                DimSecurity s ON (
                    W_S_SYMB = s.Symbol AND 
                    s.isCurrent = True
                ) LEFT JOIN 
                DimCustomer c ON (
                    W_C_ID = c.CustomerID AND
                    c.IsCurrent = True
                )
    """)
    
    return spark.sql("SELECT * FROM FactWatches WHERE BatchID=2")



def load_update_staging_FactMarketStory(spark, dbname, staging_area_folder_upl):
    
    spark.sql(f"USE {dbname}")
    spark.sql("""DROP TABLE FactMarketHistory""")

    create_fact_market_history(spark,dbname)

    schema = """
        `DM_DATE` DATE,
        `DM_S_SYMB` STRING,
        `DM_CLOSE` FLOAT,
        `DM_HIGH` FLOAT,
        `DM_LOW` FLOAT,
        `DM_VOL` INTEGER
    """

    DailyMarket_ = spark.read.format("csv").option("delimiter", "|").schema(schema).load(f"{staging_area_folder_upl}/DailyMarket.txt")
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
                cast(2 as int) as BatchID
        FROM DailyMarket dm
        INNER JOIN DimSecurity ds ON ds.Symbol = dm.DM_S_SYMB AND ds.IsCurrent = 1
        INNER JOIN DimDate dd1 ON dd1.DateValue = dm.DM_DATE
        INNER JOIN DimDate dd2 ON dd2.DateValue = dm.FiftyTwoWeekHighDate
        INNER JOIN DimDate dd3 ON dd3.DateValue = dm.FiftyTwoWeekLowDate
        LEFT JOIN CompEarning ce ON ds.SK_CompanyID = ce.CompanyID
         """)
    
    DailyMarket_.createOrReplaceTempView("dailymarket_insert")
    spark.sql("""
               INSERT INTO FactMarketHistory(ClosePrice, DayHigh, DayLow, Volume, SK_SecurityID, SK_CompanyID, SK_DateID, SK_FiftyTwoWeekHighDate, SK_FiftyTwoWeekLowDate,  FiftyTwoWeekHigh, FiftyTwoWeekLow, Yield, PERatio, BatchID)
       SELECT * FROM dailymarket_insert
    """)
    
    return spark.sql("""
        SELECT * FROM FactMarketHistory WHERE BatchID = 2
    """)


def load_update_staging_Prospect(spark,dbname, staging_area_folder_upl):
    spark.sql(f"USE {dbname}")
    spark.sql("""DROP TABLE Prospect """)

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
    Prospect_ = spark.read.format("csv").option("delimiter", ",").schema(schema).load(f"{staging_area_folder_upl}/Prospect.csv")
    
    udf_marketing = udf(lambda row: get_marketingnameplate(row), StringType())
    Prospect_ = Prospect_.withColumn('MarketingNameplate', udf_marketing(struct([Prospect_[x] for x in Prospect_.columns])))
    
    now = datetime.utcnow()
    
    DimDate = spark.sql("""
        SELECT SK_DateID FROM DimDate WHERE SK_DateID = 20201231
    """)
    Prospect_ = Prospect_.crossJoin(DimDate)
    Prospect_.createOrReplaceTempView("Prospect_")
    
    Prospect_ = spark.sql(
    """
        SELECT p.AgencyID as AgencyID, 
               2 as BatchID, 
               CASE
                   WHEN dc.Status = 'ACTIVE' THEN True ELSE False
               END as IsCustomer,
               p.SK_DateID as SK_RecordDateID,
               p.SK_DateID as SK_UpdateDateID,
               p.LastName as LastName,
               p.FirstName as FirstName,
               p.MiddleInitial as MiddleInitial,
               p.Gender as Gender,
               p.AddressLine1 as AddressLine1,
               p.AddressLine2 as AddressLine2,
               p.PostalCode as PostalCode,
               p.City as City,
               p.State as State,
               p.Country as Country,
               p.Phone as Phone,
               p.Income as Income,
               p.NumberCars as NumberCars,
               p.NumberChildren as NumberChildren,
               p.MaritalStatus as MaritalStatus,
               p.Age as Age,
               p.CreditRating as CreditRating,
               p.OwnOrRentFlag as OwnOrRentFlag,
               p.Employer as Employer,
               p.NumberCreditCards as NumberCreditCards,
               p.NetWorth as NetWorth,
               p.MarketingNameplate as MarketingNameplate
        FROM Prospect_ p
        LEFT JOIN DimCustomer dc ON 
        upper(p.FirstName) = upper(dc.FirstName) AND upper(p.LastName) = upper(dc.LastName)
        AND upper(p.AddressLine1) = upper(dc.AddressLine1) AND upper(p.AddressLine2) = upper(dc.AddressLine2)
        AND upper(p.PostalCode) = upper(dc.PostalCode)
    """)
    Prospect_.createOrReplaceTempView("Prospect_")

    combined_prospect = spark.sql("""
                CREATE TABLE CombinedProspect AS
                SELECT
                    COALESCE(p.AgencyID, np.AgencyID) AS AgencyID,
                    CAST(COALESCE(p.SK_RecordDateID, np.SK_RecordDateID) AS INT) AS SK_RecordDateID,
                    CAST(COALESCE(p.SK_UpdateDateID, np.SK_UpdateDateID) AS INT) AS SK_UpdateDateID,
                    COALESCE(p.BatchID, np.BatchID) AS BatchID,
                    COALESCE(p.IsCustomer, np.IsCustomer) AS IsCustomer,
                    COALESCE(np.LastName, p.LastName) AS LastName,
                    COALESCE(np.FirstName, p.FirstName) AS FirstName,
                    COALESCE(np.MiddleInitial, p.MiddleInitial) AS MiddleInitial,
                    COALESCE(np.Gender, p.Gender) AS Gender,
                    COALESCE(np.AddressLine1, p.AddressLine1) AS AddressLine1,
                    COALESCE(np.AddressLine2, p.AddressLine2) AS AddressLine2,
                    COALESCE(np.PostalCode, p.PostalCode) AS PostalCode,
                    COALESCE(np.City, p.City) AS City,
                    COALESCE(np.State, p.State) AS State,
                    COALESCE(np.Country, p.Country) AS Country,
                    COALESCE(np.Phone, p.Phone) AS Phone,
                    COALESCE(np.Income, p.Income) AS Income,
                    COALESCE(np.NumberCars, p.NumberCars) AS NumberCars,
                    COALESCE(np.NumberChildren, p.NumberChildren) AS NumberChildren,
                    COALESCE(np.MaritalStatus, p.MaritalStatus) AS MaritalStatus,
                    COALESCE(np.Age, p.Age) AS Age,
                    COALESCE(np.CreditRating, p.CreditRating) AS CreditRating,
                    COALESCE(np.OwnOrRentFlag, p.OwnOrRentFlag) AS OwnOrRentFlag,
                    COALESCE(np.Employer, p.Employer) AS Employer,
                    COALESCE(np.NumberCreditCards, p.NumberCreditCards) AS NumberCreditCards,
                    COALESCE(np.NetWorth, p.NetWorth) AS NetWorth,
                    COALESCE(np.MarketingNameplate, p.MarketingNameplate) AS MarketingNameplate
                FROM
                    Prospect p
                FULL OUTER JOIN 
                    Prospect_ np
                ON 
                    p.AgencyID = np.AgencyID;
    """)
    
    spark.sql("""
            INSERT OVERWRITE TABLE Prospect
            SELECT * FROM CombinedProspect
              """)

    spark.sql("""DROP TABLE CombinedProspect""") 

    return spark.sql("""
        SELECT * FROM Prospect WHERE BatchID = 2
    """)


def load_dimen_customer_2(spark,dbname, staging_area_folder_up2):
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

    # Customers.show(2)

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
                       CAST('3' as INT) as BatchID, 
                       to_date('2015-01-01', 'yyyy-MM-dd') as EffectiveDate, 
                       to_date('9999-12-31', 'yyyy-MM-dd') as EndDate
                       From customers as c 
                       left join TaxRate as NAT on c.C_NAT_TX_ID = NAT.TX_ID 
                       left join TaxRate as LCL on c.C_LCL_TX_ID = LCL.TX_ID 
                       left join Prospect as p on (c.C_L_NAME = p.LastName and c.C_F_NAME = p.FirstName 
                            and c.C_ADLINE1 = p.AddressLine1 and c.C_ADLINE2 =  p.AddressLine2 and c.C_ZIPCODE = p.PostalCode)
                            left join StatusType on StatusType.ST_ID = c.C_ST_ID """)



    
    #dimCustomer.printSchema()
    
    dimCustomer.write.mode("append").saveAsTable( "DimCustomer", mode="append")
    
    return dimCustomer


def load_dimen_account_2(spark,dbname, staging_area_folder_up2):
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
    
    # Accounts.show(2)

    dimAccount = spark.sql(""" Select CDC_DSN AS SK_AccountID,
                           CA_ID as AccountID,
                           CA_C_ID as SK_CustomerID,
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

    dimAccount.createOrReplaceTempView("dimAccount_stream")
    dimAccount = cast_to_target_schema(spark, "dimAccount_stream", "DimAccount")
    
    dimAccount.write.mode("append").saveAsTable( "DimAccount", mode="append")
    
    return dimAccount
    
#load_dimen_account("test")

def load_update_dimen_trade_2(spark,dbname,staging_area_folder_up1):
    #spark.sql(f"USE {dbname}")
    schema = """
            `CDC_FLAG` String,
            `CDC_DSN` String,
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
    trade_base = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder_up1}/Trade.txt")
    )
    trade_base.createOrReplaceTempView("trade_base_batch1")
        
    add_trade_df= spark.sql(""" Select *
                           From trade_base_batch1 as a where a.CDC_FLAG = 'I'  """)

    updated_trade_df=spark.sql(""" Select *
                           From trade_base_batch1 as a where a.CDC_FLAG = 'U'  """)
    

    trades = trade_base.union(add_trade_df).join(updated_trade_df, on=['T_ID'], how='left_anti').union(updated_trade_df)
    trades.createOrReplaceTempView("trade_view")
    
    
    trade = spark.sql("""
            SELECT T.T_ID,
                CASE WHEN T.CDC_FLAG = 'I' then TH.TH_DTS ELSE NULL END as create_date,
                CASE WHEN T.CDC_FLAG = 'I' then TH.TH_DTS ELSE NULL END as create_time,
                CASE 
                WHEN T.CDC_FLAG = 'I' then NULL 
                WHEN TH.TH_ST_ID in ('CMPT', 'CNCL') THEN TH.TH_DTS ELSE NULL END as close_date,
                CASE WHEN T.CDC_FLAG = 'I' then NULL 
                WHEN TH.TH_ST_ID in ('CMPT', 'CNCL') THEN TH.TH_DTS ELSE NULL END as close_time,
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
             FROM trade_view T
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
    .groupBy( 'T_ID', 'ST_NAME', 'TT_NAME', 'T_IS_CASH','T_CA_ID','T_S_SYMB','TH_DTS', 'T_QTY', 'T_BID_PRICE', 'T_EXEC_NAME', 'T_TRADE_PRICE', 'T_CHRG', 'T_COMM', 'T_TAX')
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
    
    dimTrade=spark.sql("""
        SELECT  INT(T_ID) as TradeID,
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
         3 as BatchID
        FROM trade_insert inner join DimAccount as da on trade_insert.T_CA_ID = da.SK_AccountID 
        AND trade_insert.TH_DTS BETWEEN da.EffectiveDate AND da.EndDate inner join DimSecurity
        as ds on (trade_insert.T_S_SYMB = ds.Symbol AND trade_insert.TH_DTS BETWEEN ds.EffectiveDate AND ds.EndDate)
    """)
  
    #dimAccount.printSchema()
    
    dimTrade.write.mode("append").saveAsTable( "DimTrade", mode="append")
    
    return dimTrade


def load_update_fact_cash_balances_2(spark,dbname, staging_area_folder_upl):
    #spark.sql(f"USE {dbname}")
    schema = """
            `CDC_FLAG` String,
            `CDC_DSN` String,
            `CT_CA_ID` INTEGER,
            `CT_DTS` TIMESTAMP,
            `CT_AMT` FLOAT,
            `CT_NAME` STRING
    """
    cash = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder_upl}/CashTransaction.txt")
    )
    
    cash.createOrReplaceTempView("cashTrans")
    # cash.show(2)
    factCashBalances = spark.sql(""" 
                       Select SK_CustomerID, 
                           AccountID AS SK_AccountID, 
                           SK_DateID, 
                           sum(CT_AMT) as Cash, 
                           CAST('3' as INT) as BatchID 
                       From cashTrans join DimAccount as ac on (CT_CA_ID =ac.AccountID) 
                       join DimDate as dt on dt.DateValue = Date(CT_DTS) 
                       Group by AccountID, SK_CustomerID, SK_DateID""")
    
    # factCashBalances.show(2)
    
    factCashBalances.write.option("append", "true").saveAsTable("FactCashBalances", mode="append")
#     factCashBalances.show(3)
    return factCashBalances


def load_update_fact_holdings_2(spark,dbname, staging_area_folder_upl):
    #spark.sql(f"USE {dbname}")
    schema = """
            `CDC_FLAG` String,
            `CDC_DSN` String,
            `HH_H_T_ID` INTEGER,
            `HH_T_ID` INTEGER,
            `HH_BEFORE_QTY` FLOAT,
            `HH_AFTER_QTY` FLOAT
    """
    holding = (
        spark.read.format("csv")
        .option("delimiter", "|")
        .schema(schema)
        .load(f"{staging_area_folder_upl}/HoldingHistory.txt")
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
                       CAST('3' as INT) as BatchID 
                       From holdings join DimTrade as ac on (HH_T_ID =ac.TradeID)""")
    
    
    factHoldings.write.option("append", "true").saveAsTable("FactHoldings", mode="append")
    factHoldings.show(2)
    
    return factHoldings



def load_update_fact_watches_2(spark,dbname, staging_area_folder_upl):
    spark.sql(f"USE {dbname}")
    # Customer ID, Ticker symbol, Datetime, activate or cancel watch
    schema = """
            `CDC_FLAG` String,
            `CDC_DSN` String,
            `W_C_ID` BIGINT, 
            `W_S_SYMB` STRING,
            `W_DTS` DATE,
            `W_ACTION` STRING
    """
    
    spark.read.format("csv") \
        .option("delimiter", "|") \
        .schema(schema) \
        .load(f"{staging_area_folder_upl}/WatchHistory.txt") \
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
                3 AS BatchID
            FROM watches w LEFT JOIN 
                DimDate d1 ON DatePlaced = d1.DateValue LEFT JOIN 
                DimDate d2 ON DateRemoved = d2.DateValue LEFT JOIN
                DimSecurity s ON (
                    W_S_SYMB = s.Symbol AND 
                    s.isCurrent = True
                ) LEFT JOIN 
                DimCustomer c ON (
                    W_C_ID = c.CustomerID AND
                    c.IsCurrent = True
                )
    """)
    
    return spark.sql("SELECT * FROM FactWatches WHERE BatchID=3")


def load_update_staging_FactMarketStory_2(spark,dbname, staging_area_folder_upl):
    
    spark.sql(f"USE {dbname}")
    # spark.sql("""DROP TABLE FactMarketHistory""")

    # create_fact_market_history(dbname)

    schema = """
        `CDC_FLAG` String,
        `CDC_DSN` String,
        `DM_DATE` DATE,
        `DM_S_SYMB` STRING,
        `DM_CLOSE` FLOAT,
        `DM_HIGH` FLOAT,
        `DM_LOW` FLOAT,
        `DM_VOL` INTEGER
    """

    DailyMarket_ = spark.read.format("csv").option("delimiter", "|").schema(schema).load(f"{staging_area_folder_upl}/DailyMarket.txt")
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
                cast('3' as int) as BatchID
        FROM DailyMarket dm
        INNER JOIN DimSecurity ds ON ds.Symbol = dm.DM_S_SYMB AND ds.IsCurrent = 1
        INNER JOIN DimDate dd1 ON dd1.DateValue = dm.DM_DATE
        INNER JOIN DimDate dd2 ON dd2.DateValue = dm.FiftyTwoWeekHighDate
        INNER JOIN DimDate dd3 ON dd3.DateValue = dm.FiftyTwoWeekLowDate
        LEFT JOIN CompEarning ce ON ds.SK_CompanyID = ce.CompanyID
         """)
    
    DailyMarket_.createOrReplaceTempView("dailymarket_insert")
    spark.sql("""
               INSERT INTO FactMarketHistory(ClosePrice, DayHigh, DayLow, Volume, SK_SecurityID, SK_CompanyID, SK_DateID, SK_FiftyTwoWeekHighDate, SK_FiftyTwoWeekLowDate,  FiftyTwoWeekHigh, FiftyTwoWeekLow, Yield, PERatio, BatchID)
       SELECT * FROM dailymarket_insert
    """)
    
    return spark.sql("""
        SELECT * FROM FactMarketHistory WHERE BatchID = 3
    """)

def load_update_staging_Prospect_2(spark,dbname, staging_area_folder_upl):
    spark.sql(f"USE {dbname}")
    # spark.sql("""DROP TABLE Prospect """)

    # create_prospect_table(dbname)

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
    Prospect_ = spark.read.format("csv").option("delimiter", ",").schema(schema).load(f"{staging_area_folder_upl}/Prospect.csv")
    
    udf_marketing = udf(lambda row: get_marketingnameplate(row), StringType())
    Prospect_ = Prospect_.withColumn('MarketingNameplate', udf_marketing(struct([Prospect_[x] for x in Prospect_.columns])))
    
    now = datetime.utcnow()
    
    DimDate = spark.sql("""
        SELECT SK_DateID FROM DimDate WHERE SK_DateID = 20201231
    """)
    Prospect_ = Prospect_.crossJoin(DimDate)
    Prospect_.createOrReplaceTempView("Prospect_")
    
    Prospect_ = spark.sql(
    """
        SELECT p.AgencyID as AgencyID, 
               3 as BatchID, 
               CASE
                   WHEN dc.Status = 'ACTIVE' THEN True ELSE False
               END as IsCustomer,
               p.SK_DateID as SK_RecordDateID,
               p.SK_DateID as SK_UpdateDateID,
               p.LastName as LastName,
               p.FirstName as FirstName,
               p.MiddleInitial as MiddleInitial,
               p.Gender as Gender,
               p.AddressLine1 as AddressLine1,
               p.AddressLine2 as AddressLine2,
               p.PostalCode as PostalCode,
               p.City as City,
               p.State as State,
               p.Country as Country,
               p.Phone as Phone,
               p.Income as Income,
               p.NumberCars as NumberCars,
               p.NumberChildren as NumberChildren,
               p.MaritalStatus as MaritalStatus,
               p.Age as Age,
               p.CreditRating as CreditRating,
               p.OwnOrRentFlag as OwnOrRentFlag,
               p.Employer as Employer,
               p.NumberCreditCards as NumberCreditCards,
               p.NetWorth as NetWorth,
               p.MarketingNameplate as MarketingNameplate
        FROM Prospect_ p
        LEFT JOIN DimCustomer dc ON 
        upper(p.FirstName) = upper(dc.FirstName) AND upper(p.LastName) = upper(dc.LastName)
        AND upper(p.AddressLine1) = upper(dc.AddressLine1) AND upper(p.AddressLine2) = upper(dc.AddressLine2)
        AND upper(p.PostalCode) = upper(dc.PostalCode)
    """)
    Prospect_.createOrReplaceTempView("Prospect_")

    combined_prospect = spark.sql("""
                CREATE TABLE CombinedProspect AS
                SELECT
                    COALESCE(p.AgencyID, np.AgencyID) AS AgencyID,
                    CAST(COALESCE(p.SK_RecordDateID, np.SK_RecordDateID) AS INT) AS SK_RecordDateID,
                    CAST(COALESCE(p.SK_UpdateDateID, np.SK_UpdateDateID) AS INT) AS SK_UpdateDateID,
                    COALESCE(p.BatchID, np.BatchID) AS BatchID,
                    COALESCE(p.IsCustomer, np.IsCustomer) AS IsCustomer,
                    COALESCE(np.LastName, p.LastName) AS LastName,
                    COALESCE(np.FirstName, p.FirstName) AS FirstName,
                    COALESCE(np.MiddleInitial, p.MiddleInitial) AS MiddleInitial,
                    COALESCE(np.Gender, p.Gender) AS Gender,
                    COALESCE(np.AddressLine1, p.AddressLine1) AS AddressLine1,
                    COALESCE(np.AddressLine2, p.AddressLine2) AS AddressLine2,
                    COALESCE(np.PostalCode, p.PostalCode) AS PostalCode,
                    COALESCE(np.City, p.City) AS City,
                    COALESCE(np.State, p.State) AS State,
                    COALESCE(np.Country, p.Country) AS Country,
                    COALESCE(np.Phone, p.Phone) AS Phone,
                    COALESCE(np.Income, p.Income) AS Income,
                    COALESCE(np.NumberCars, p.NumberCars) AS NumberCars,
                    COALESCE(np.NumberChildren, p.NumberChildren) AS NumberChildren,
                    COALESCE(np.MaritalStatus, p.MaritalStatus) AS MaritalStatus,
                    COALESCE(np.Age, p.Age) AS Age,
                    COALESCE(np.CreditRating, p.CreditRating) AS CreditRating,
                    COALESCE(np.OwnOrRentFlag, p.OwnOrRentFlag) AS OwnOrRentFlag,
                    COALESCE(np.Employer, p.Employer) AS Employer,
                    COALESCE(np.NumberCreditCards, p.NumberCreditCards) AS NumberCreditCards,
                    COALESCE(np.NetWorth, p.NetWorth) AS NetWorth,
                    COALESCE(np.MarketingNameplate, p.MarketingNameplate) AS MarketingNameplate
                FROM
                    Prospect p
                FULL OUTER JOIN 
                    Prospect_ np
                ON 
                    p.AgencyID = np.AgencyID;
    """)
    
    spark.sql("""
            INSERT OVERWRITE TABLE Prospect
            SELECT * FROM CombinedProspect
              """)

    spark.sql("""DROP TABLE CombinedProspect""") 

    return spark.sql("""
        SELECT * FROM Prospect WHERE BatchID = 3
    """)

def run_incremental_load_2(spark,dbname, scale_factor, file_id):
    metrics = {}


    staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/Batch3"

    # Run incremental update
    start = time.time()
    customer = load_dimen_customer_2(spark,dbname, staging_area_folder)
    account = load_dimen_account_2(spark,dbname, staging_area_folder)
    dimtrade = load_update_dimen_trade_2(spark,dbname, staging_area_folder)

    factcashbalance = load_update_fact_cash_balances_2(spark,dbname, staging_area_folder)
    holding = load_update_fact_holdings_2(spark,dbname, staging_area_folder)
    watch = load_update_fact_watches_2(spark,dbname, staging_area_folder)

    factmarkethistory =load_update_staging_FactMarketStory_2(spark,dbname, staging_area_folder)
    prospect = load_update_staging_Prospect_2(spark,dbname, staging_area_folder)
    end = time.time() - start

    metrics["et"] = end


    factmarkethistory_count = factmarkethistory.count()
    prospect_count = prospect.count()
    dimtrade_count = dimtrade.count()
    factcashbalance_count = factcashbalance.count()
    holding_count = holding.count()
    watch_count = watch.count()
    customer_count = customer.count()
    account_count = account.count()

    # Sum the individual counts
    rows = factmarkethistory_count + prospect_count + dimtrade_count + factcashbalance_count + holding_count + watch_count + customer_count + account_count

    metrics["rows"] = rows
    metrics["throughput"] = (rows / get_max(end,1800))

    metrics_df = pd.DataFrame(metrics, index=[0])
    metrics_df.to_csv(f"{os.getcwd()}/results/data/incremental_load_2_{scale_factor}_{file_id}.csv", index=False)

    return metrics_df


