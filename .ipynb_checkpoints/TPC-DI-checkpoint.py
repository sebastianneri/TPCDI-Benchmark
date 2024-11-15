# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #TPCDI
# MAGIC <hr/>
# MAGIC 
# MAGIC ## Step 1: Initialization
# MAGIC 
# MAGIC ### In this step:
# MAGIC 
# MAGIC - data is cleaned from previous runs
# MAGIC - data warehouse is created (schema)
# MAGIC - helper functions to process data can be created

# COMMAND ----------

## CELL 2
import findspark
findspark.init()
import pyspark
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().set("spark.sql.legacy.createHiveTableByDefault", "false").setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

scale_factor = "Scale3" # Options "Scale3", "Scale6", "Scale9", "Scale12"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Stack

# COMMAND ----------

def clean_warehouse(dbname="test"):
    spark.sql(f"DROP DATABASE IF EXISTS {dbname} CASCADE")
    print(f"Warehouse {dbname} deleted.")

# COMMAND ----------

# MAGIC %pip install google-cloud-storage
# MAGIC %pip install fsspec
# MAGIC %pip install gcsfs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Create Data Warehouse
# MAGIC ### Create Dims

# COMMAND ----------

def create_dim_account(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS DimAccount(
            SK_AccountID INTEGER,
            AccountID BIGINT,
            SK_BrokerID BIGINT,
            SK_CustomerID BIGINT,
            Status CHAR(10),
            AccountDesc CHAR(50),
            TaxStatus INTEGER,
            IsCurrent BOOLEAN,
            BatchID CHAR(14),
            EffectiveDate DATE,
            EndDate DATE
        )
    """
    )
    print("Created dim account.")


def create_dim_broker(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS DimBroker(
            SK_BrokerID INTEGER,
            BrokerID BIGINT,
            ManagerID BIGINT,
            FirstName CHAR(50),
            LastName CHAR(50),
            MiddleInitial CHAR(1),
            Branch CHAR(50),
            Office CHAR(50),
            Phone CHAR(14),
            IsCurrent BOOLEAN,
            BatchID INTEGER,
            EffectiveDate DATE,
            EndDate DATE
        )
    """
    )
    print("Created dim broker.")


def create_dim_company(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS DimCompany(
            SK_CompanyID BIGINT,
            CompanyID BIGINT,
            Status CHAR(10),
            Name CHAR(60),
            Industry CHAR(50),
            SPrating CHAR(4),
            isLowGrade BOOLEAN,
            CEO CHAR(100),
            AddressLine1 CHAR(80),
            AddressLine2 CHAR(80),
            PostalCode CHAR(12),
            City CHAR(25),
            StateProv CHAR(20),
            Country CHAR(24),
            Description CHAR(150),
            FoundingDate DATE,
            IsCurrent BOOLEAN,
            BatchID INTEGER,
            EffectiveDate DATE,
            EndDate DATE
        )
    """
    )
    print("Created dim company.")


def create_dim_customer(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS DimCustomer(
            SK_CustomerID BIGINT,
            CustomerID BIGINT,
            TaxID CHAR(20),
            Status CHAR(10),
            LastName CHAR(30),
            FirstName CHAR(30),
            MiddleInitial CHAR(1),
            Gender CHAR(1),
            Tier INTEGER,
            DOB DATE,
            AddressLine1 CHAR(80),
            AddressLine2 CHAR(84),
            PostalCode CHAR(12),
            City CHAR(25),
            StateProv CHAR(20),
            Country CHAR(24),
            Phone1 CHAR(30),
            Phone2 CHAR(30),
            Phone3 CHAR(30),
            Email1 CHAR(50),
            Email2 CHAR(50),
            NationalTaxRateDesc CHAR(50),
            NationalTaxRate INTEGER,
            LocalTaxRateDesc CHAR(50),
            LocalTaxRate INTEGER,
            AgencyID CHAR(30),
            CreditRating INTEGER,
            NetWorth FLOAT,
            MarketingNamePlate CHAR(100),
            IsCurrent BOOLEAN,
            BatchID INTEGER,
            EffectiveDate DATE,
            EndDate DATE
        )
    """
    )
    print("Created dim customer.")


def create_dim_date(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS DimDate(
            SK_DateID INTEGER,
            DateValue DATE,
            DateDesc CHAR(20),
            CalendarYearID INTEGER,
            CalendarYearDesc CHAR(20),
            CalendarQtrID INTEGER,
            CalendarQtrDesc CHAR(20),
            CalendarMonthID INTEGER,
            CalendarMonthDesc CHAR(20),
            CalendarWeekID INTEGER,
            CalendarWeekDesc CHAR(20),
            DayOfWeekNum TINYINT,
            DayOfWeekDesc CHAR(10),
            FiscalYearID INTEGER,
            FiscalYearDesc CHAR(20),
            FiscalQtrID INTEGER,
            FiscalQtrDesc CHAR(20),
            HolidayFlag BOOLEAN
        )
    """
    )
    print("Created dim date.")


def create_dim_security(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS DimSecurity(
            SK_SecurityID BIGINT,
            Symbol CHAR(15),
            Issue CHAR(6),
            Status CHAR(10),
            Name CHAR(70),
            ExchangeID CHAR(6),
            SK_CompanyID BIGINT,
            SharesOutstanding BIGINT,
            FirstTrade DATE,
            FirstTradeOnExchange DATE,
            Dividend FLOAT,
            IsCurrent BOOLEAN,
            BatchID INTEGER,
            EffectiveDate DATE,
            EndDate DATE
        )
    """
    )
    print("Created dim security.")


def create_dim_time(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS DimTime(
            SK_TimeID INTEGER,
            TimeValue TIMESTAMP,
            HourID INTEGER,
            HourDesc CHAR(20),
            MinuteID INTEGER,
            MinuteDesc CHAR(20),
            SecondID INTEGER,
            SecondDesc CHAR(20),
            MarketHoursFlag BOOLEAN,
            OfficeHoursFlag BOOLEAN
        )
    """
    )
    print("Created dim time.")


def create_dim_trade(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS DimTrade(
            TradeID INTEGER,
            SK_BrokerID INTEGER,
            SK_CreateDateID INTEGER,
            SK_CreateTimeID INTEGER,
            SK_CloseDateID INTEGER,
            SK_CloseTimeID INTEGER,
            Status CHAR(10),
            Type CHAR(12),
            CashFlag BOOLEAN,
            SK_SecurityID INTEGER,
            SK_CompanyID INTEGER,
            Quantity FLOAT,
            BidPrice FLOAT,
            SK_CustomerID INTEGER,
            SK_AccountID INTEGER,
            ExecutedBy CHAR(64),
            TradePrice FLOAT,
            Fee FLOAT,
            Comission FLOAT,
            Tax FLOAT,
            BatchID INTEGER
        )
    """
    )
    print("Created dim trades.")


def create_dimessages_table(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS DIMessages(
            MessageDateAndTime TIMESTAMP,
            BatchID INTEGER,
            MessageSource CHAR(30),
            MessageText CHAR(50),
            MessageType CHAR(12),
            MessageData CHAR(100)
        )
    """
    )
    print("Created dim messages.")


def create_dims(dbname):
    create_dim_account(dbname)
    create_dim_broker(dbname)
    create_dim_company(dbname)
    create_dim_customer(dbname)
    create_dim_date(dbname)
    create_dim_security(dbname)
    create_dim_time(dbname)
    create_dim_trade(dbname)
    create_dimessages_table(dbname)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Facts

# COMMAND ----------

def create_fact_cash_balances(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS FactCashBalances(
            SK_CustomerID INTEGER,
            SK_AccountID INTEGER,
            SK_DateID INTEGER,
            Cash FLOAT,
            BatchID INTEGER
        )
    """)
    print("Created fact cash balances")


def create_fact_holdings(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS FactHoldings(
            TradeID INTEGER,
            CurrentTradeID INTEGER,
            SK_CustomerID INTEGER,
            SK_AccountID INTEGER,
            SK_SecurityID INTEGER,
            SK_CompanyID INTEGER,
            SK_DateID INTEGER,
            SK_TimeID INTEGER,
            CurrentPrice FLOAT,
            CurrentHolding INTEGER,
            BatchID INTEGER
        )
    """)
    print("Created fact holdings")


def create_fact_market_history(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS FactMarketHistory(
            SK_SecurityID INTEGER,
            SK_CompanyID INTEGER,
            SK_DateID INTEGER,
            PERatio FLOAT,
            Yield FLOAT,
            FiftyTwoWeekHigh FLOAT,
            SK_FiftyTwoWeekHighDate INTEGER,
            FiftyTwoWeekLow FLOAT,
            SK_FiftyTwoWeekLowDate INTEGER,
            ClosePrice FLOAT,
            DayHigh FLOAT,
            DayLow FLOAT,
            Volume INTEGER,
            BatchID INTEGER
        )
    """)
    print("Created fact market history")


def create_fact_watches(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS FactWatches(
            SK_CustomerID BIGINT,
            SK_SecurityID BIGINT,
            SK_DateID_DatePlaced BIGINT,
            SK_DateID_DateRemoved BIGINT,
            BatchID INTEGER
        )
    """)
    print("Created fact watches")


def create_facts(dbname):
    create_fact_cash_balances(dbname)
    create_fact_holdings(dbname)
    create_fact_market_history(dbname)
    create_fact_watches(dbname)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Other Tables

# COMMAND ----------

def create_industry_table(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS Industry( 
            IN_ID CHAR(2),
            IN_NAME CHAR(50),
            IN_SC_ID CHAR(4)
        )
    """
    )
    print("Created table industry.")

def create_financial_table(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS Financial( 
           SK_CompanyID BIGINT,
           FI_YEAR Integer,
           FI_QTR Integer,
           FI_QTR_START_DATE DATE,
           FI_REVENUE Float,
           FI_NET_EARN Float,
           FI_BASIC_EPS Float,
           FI_DILUT_EPS  Float,
           FI_MARGIN Float,
           FI_INVENTORY Float,
           FI_ASSETS Float,
           FI_LIABILITY Float,
           FI_OUT_BASIC Float,
           FI_OUT_DILUT Float             
        )
    """
    )
    print("Created table Finacial.")

def create_prospect_table(dbname):
    spark.sql(f"USE {dbname}")
#     spark.sql("""
#         DROP TABLE Prospect
#     """)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS Prospect( 
           AgencyID char(30),
           SK_RecordDateID Integer,
           SK_UpdateDateID Integer,
           BatchID Integer,
           IsCustomer Boolean,
           LastName Char(30),
           FirstName Char(30),
           MiddleInitial Char(1),
           Gender Char(1),
           AddressLine1 Char(80),
           AddressLine2 Char(80),
           PostalCode Char(12),
           City Char(25),
           State Char(20),
           Country Char(24),
           Phone Char(30), 
           Income Char(9),
           NumberCars Integer,
           NumberChildren Integer,
           MaritalStatus Char(1),
           Age Integer,
           CreditRating Integer,
           OwnOrRentFlag Char(1),  
           Employer Char(30),
           NumberCreditCards Integer,
           NetWorth Integer, 
           MarketingNameplate Char(100)
                       
        )
    """
    )
    print("Created table Prospect.")
# create_prospect_table("test")


def create_status_type_table(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS StatusType( 
           ST_ID CHAR(4),
           ST_NAME CHAR(10)
        )
    """
    )
    print("Created table StatusType.")
    


def create_taxrate_table(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS TaxRate( 
           TX_ID CHAR(4),
           TX_NAME CHAR(50),
           TX_RATE Float
        )
    """
    )
    print("Created table TaxRate.")
    


def create_tradetype_table(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS TradeType( 
           TT_ID CHAR(3),
           TT_NAME CHAR(12),
           TT_IS_SELL Integer,
           TT_IS_MRKT Integer
        )
    """
    )
    print("Created table TradeType.")
    


def create_audit_table(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS Audit( 
           DataSet CHAR(20),
           BatchID Integer,
           DATE Date,
           Attribute CHAR(50),
           Value float,
           DValue float 
        )
    """
    )
    print("Created table Audit.")



def create_other_tables(dbname):
    create_industry_table(dbname)
    create_financial_table(dbname)
    create_prospect_table(dbname)
    create_status_type_table(dbname)
    create_taxrate_table(dbname)
    create_tradetype_table(dbname)
    create_audit_table(dbname)

# COMMAND ----------

def create_warehouse(dbname="test"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    create_dims(dbname)
    create_facts(dbname)
    create_other_tables(dbname)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Execute Step 1

# COMMAND ----------

def clean_warehouse(dbname="test"):
    spark.sql(f"DROP DATABASE IF EXISTS {dbname} CASCADE")
    print(f"Warehouse {dbname} deleted.")

# COMMAND ----------

clean_warehouse("test")

# COMMAND ----------

create_warehouse()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Historical Load
# MAGIC 
# MAGIC ### This step:
# MAGIC 
# MAGIC - populates the data warehouse
# MAGIC - applies transformations described in clause 4.5 of the tpc-di manual (batch1 folder on the generated data)
# MAGIC - performs validations based on clause 7.4
# MAGIC - upon completion of the validation stage, a phase completion record is written into DIMessages table
# MAGIC - this step must be timed
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Load Dim Date and Dim Time

# COMMAND ----------

# General variables setup
# staging_area_folder = f"gs://tpcdi-with-spark-bdma/TPCDI_Data/TPCDI_Data/{scale_factor}/Batch1/"

#from google.cloud import storage
#from google.oauth2 import service_account

def create_gcs_client():
    credentials_dict = {
      "type": "service_account",
      "project_id": "bdma-371020",
      "private_key_id": "",
      "private_key": "",
      "client_email": "tpcdi-databricks-bdma@bdma-371020.iam.gserviceaccount.com",
      "client_id": "103179657336858348005",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/tpcdi-databricks-bdma%40bdma-371020.iam.gserviceaccount.com"
    }
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    client = storage.Client(credentials=credentials)
    return client

# COMMAND ----------

# General variables setup
def load_dim_date(dbname, staging_area_folder):
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
# dates = load_dim_date("test")
# dates.limit(3).toPandas()

# COMMAND ----------

def load_dim_time(dbname, staging_area_folder):
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
# times = load_dim_time("test")
# times.limit(3).toPandas()

# COMMAND ----------

#Load tax rate

def load_tax_rate(dbname, staging_area_folder):
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


# Tax_Rate=load_tax_rate("test")
# Tax_Rate.limit(3).toPandas()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### HR File (DimBroker)

# COMMAND ----------

from pyspark.sql.functions import expr
"""
Records where EmployeeJobCode is not 314 are not broker records, and are ignored. The remaining steps are for records where the job code is 314.
- BrokerID, ManagerID, FirstName, LastName, MiddleInitial, Branch, Office and Phone are obtained from these fields of the HR.csv file: EmployeeID, ManagerID, EmployeeFirstName, EmployeeLastName, EmployeeMI, EmployeeBranch, EmployeeOffice and EmployeePhone.
- SK_BrokerID is set appropriately for new records as described in section 4.4.1.3.
- IsCurrent is set to true
- EffectiveDate is set to the earliest date in the DimDate table and EndDate is set to 9999-12-31.
- BatchID is set as described in section 4.4.2
            SK_BrokerID INTEGER GENERATED ALWAYS AS IDENTITY,
            BrokerID BIGINT,
            ManagerID BIGINT,
            FirstName CHAR(50),
            LastName CHAR(50),
            MiddleInitial CHAR(1),
            Branch CHAR(50),
            Office CHAR(50),
            Phone CHAR(14),
            IsCurrent BOOLEAN,
            BatchID INTEGER,
            EffectiveDate DATE,
            EndDate DATE
"""


def load_staging_hr_file(dbname, staging_area_folder):
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

# spark.sql("DELETE FROM DimBroker")
# brokers = load_staging_hr_file("test")
# brokers.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finwire Files 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Columnarize UDF Function

# COMMAND ----------

from pyspark.sql.functions import col, udf, explode, map_keys, array, pandas_udf
from pyspark.sql.types import StringType, Row
import pandas as pd

@pandas_udf(StringType())
def extract_finwire_type(finwire_str):
    finwire_type = finwire_str.str[15:18]
    return finwire_type

@pandas_udf("""
            `PTS` string, `RecType` string, `CompanyName` string, `CIK` string, 
            `Status` string, `IndustryID` string , `SPrating` string, `FoundingDate` string,
            `AddrLine1` string, `AddrLine2` string, `PostalCode` string, `City` string,
            `StateProvince` string, `Country` string, `CEOname` string, `Description` string
        """)
def columnarize_finwire_data_cmp(finwire_str):
    row = pd.DataFrame(columns=['PTS', 'RecType', 'CompanyName', 'CIK', 'Status',
            'IndustryID', 'SPrating', 'FoundingDate',
            'AddrLine1', 'AddrLine2', 'PostalCode', 'City',
            'StateProvince', 'Country', 'CEOname', 'Description'])
    row['PTS'] = finwire_str.str[0:15]
    row['RecType'] = finwire_str.str[15:18]
    row['CompanyName'] = finwire_str.str[18:78]
    row['CIK'] = finwire_str.str[78:88]
    row['Status'] = finwire_str.str[88:92]
    row['IndustryID'] = finwire_str.str[92:94]
    row['SDPrating'] = finwire_str.str[94:98]
    row['FoundingDate'] = finwire_str.str[98:106]
    row['AddrLine1'] = finwire_str.str[106:186]
    row['AddrLine2'] = finwire_str.str[186:266]
    row['PostalCode'] = finwire_str.str[266:278]
    row['City'] = finwire_str.str[278:303]
    row['StateProvince'] = finwire_str.str[303:323]
    row['Country'] = finwire_str.str[323:347]
    row['CEOname'] = finwire_str.str[347:393]
    row['Description'] = finwire_str.str[393:]
    return row


@pandas_udf("""
            `PTS` string, `RecType` string, `Symbol` string, `IssueType` string, `Status` string, 
            `Name` string, `ExID` string, `ShOut` string, `FirstTradeDate` string, 
            `FirstTradeExchg` string, `Dividend` string, `CoNameOrCIK` string
""")
def columnarize_finwire_data_sec(finwire_str):
    row = pd.DataFrame(columns=['PTS', 'RecType', 'Symbol', 'IssueType', 'Status', 'Name', 'ExID',
                                'ShOut', 'FirstTradeDate', 'FirstTradeExchg', 'Dividend',
                                'CoNameOrCIK'])
    row['PTS'] = finwire_str.str[0:15]
    row['RecType'] = finwire_str.str[15:18]
    row['Symbol'] = finwire_str.str[18:33]
    row['IssueType'] = finwire_str.str[33:39]
    row['Status'] = finwire_str.str[39:43]
    row['Name'] = finwire_str.str[43:113]
    row['ExID'] = finwire_str.str[113:119]
    row['ShOut'] = finwire_str.str[119:132]
    row['FirstTradeDate'] = finwire_str.str[132:140]
    row['FirstTradeExchg'] = finwire_str.str[140:148]
    row['Dividend'] = finwire_str.str[148:160]
    row['CoNameOrCIK'] = finwire_str.str[160:]
    return row

@pandas_udf("""
            `PTS` string, `RecType` string , `Year` string , `Quarter` string, `QtrStartDate` string,
            `PostingDate` string, 
            `Revenue` string, `Earnings` string, `EPS` string , `DilutedEPS` string, `Margin` string,
            `Inventory` string, `Assets` string,
            `Liabilities` string, `ShOut` string, `DilutedShOut` string, `CoNameOrCIK` string
""")
def columnarize_finwire_data_fin(finwire_str):
    row = pd.DataFrame(columns=['PTS', 'RecType', 'Year', 'Quarter', 'QtrStartDate', 'PostingDate', 
                                'Revenue', 'Earnings', 'EPS', 'DilutedEPS', 'Margin', 'Inventory', 
                                'Assets', 'Liabilities', 'ShOut', 'DilutedShOut', 'CoNameOrCIK'])
    row['PTS'] = finwire_str.str[0:15]
    row['RecType'] = finwire_str.str[15:18]
    row['Year'] = finwire_str.str[18:22]
    row['Quarter'] = finwire_str.str[22:23]
    row['QtrStartDate'] = finwire_str.str[23:31]
    row['PostingDate'] = finwire_str.str[31:39]
    row['Revenue'] = finwire_str.str[39:56]
    row['Earnings'] = finwire_str.str[56:73]
    row['EPS'] = finwire_str.str[73:85]
    row['DilutedEPS'] = finwire_str.str[85:97]
    row['Margin'] = finwire_str.str[97:109]
    row['Inventory'] = finwire_str.str[109:126]
    row['Assets'] = finwire_str.str[126:143]
    row['Liabilities'] = finwire_str.str[143:160]
    row['ShOut'] = finwire_str.str[160:173]
    row['DilutedShOut'] = finwire_str.str[173:186]
    row['CoNameOrCIK'] = finwire_str.str[186:]
    return row

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load CMP Files (DimCompany)

# COMMAND ----------


def load_finwire_file(finwire_file_path, dbname, extract_type='CMP'):
    print("Processing", finwire_file_path)
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


#from google.cloud import storage

def load_finwire_files(dbname, scale_factor):
    spark.sql(f"USE {dbname}")
    spark.sql(f"DELETE FROM DimCompany")
    storage_client = create_gcs_client()
    bucket_name = "tpcdi-with-spark-bdma" a
    bucket = storage.Bucket(storage_client, bucket_name)

    blobs = bucket.list_blobs(prefix=f"TPCDI_Data/TPCDI_Data/{scale_factor}/Batch1/")
    finwire_files = [blob.name 
                     for blob in blobs if "FINWIRE" in blob.name and "_audit" not in blob.name]
    # First load the Finwire Data into dataframe
    cmp = None
    sec = None
    fin = None
    for i, finwire in enumerate(sorted(finwire_files)):
        if i == 0:
            cmp, sec, fin = load_finwire_file(finwire_file_path, "test")
        else:
            newcmp, newsec, newfin = load_finwire_file(f"gs://{bucket_name}/{finwire}", "test")
            cmp = cmp.union(newcmp)
            sec = sec.union(newsec)
            fin = fin.union(newfin)
    cmp.orderBy(['CIK', 'PTS']).createOrReplaceTempView("finwire_cmp")
    sec.orderBy(['Symbol', 'PTS']).createOrReplaceTempView("finwire_sec")
    fin.createOrReplaceTempView("finwire_fin")

# load_finwire_files("test", scale_Factor)

# COMMAND ----------

def load_finwires_into_dim_company(dbname, scale_factor):
    spark.sql(f"USE {dbname}")
    bucket_name = "tpcdi-with-spark-bdma"
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
        .load(f"gs://{bucket_name}/TPCDI_Data/TPCDI_Data/{scale_factor}/Batch1/StatusType.txt")
    status.createOrReplaceTempView("status")
    
    spark.sql("""
    SELECT
           monotonically_increasing_id() AS SK_CompanyID,
           CIK AS CompanyId,
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
    
    # Insert data into dimension table
    spark.sql("""
       INSERT INTO DimCompany(SK_CompanyID, CompanyID, Status, Name, Industry, SPrating, isLowGrade,
                               CEO,
                               AddressLine1, AddressLine2, PostalCode, City, StateProv, Country, 
                               Description, FoundingDate, IsCurrent, BatchID, EffectiveDate, EndDate)
       SELECT * FROM DimCompanyUpdated
    """)
    return spark.sql("SELECT * FROM DimCompany")

# spark.sql("DELETE FROM DimCompany")
# finwires = load_finwires_into_dim_company("test")
# finwires.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load SEC Files (DimSecurity)

# COMMAND ----------

"""         
            SK_SecurityID INTEGER,
            Symbol CHAR(15),
            Issue CHAR(6),
            Status CHAR(10),
            Name CHAR(70),
            ExchangeID CHAR(6),
            SK_CompanyID BIGINT,
            SharesOutstanding BIGINT,
            FirstTrade DATE,
            FirstTradeOnExchange DATE,
            Dividend FLOAT,
            IsCurrent BOOLEAN,
            BatchID INTEGER,
            EffectiveDate DATE,
            EndDate DATE
            
            `PTS` string, `RecType` string, `Symbol` string, `IssueType` string, `Status` string, 
            `Name` string, `ExID` string, `ShOut` string, `FirstTradeDate` string, 
            `FirstTradeExchg` string, `Dividend` string, `CoNameOrCIK` string
"""
def load_finwires_into_dim_security(dbname):

    spark.sql(f"USE {dbname}")
    spark.sql(f"DELETE FROM DimSecurity")
    
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
            TO_DATE(FirstTradeDate, 'yyyyMMdd') AS FirstTradeDate,
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
    
    # Now insert values into dimSecurity
    spark.sql("""
        INSERT INTO DimSecurity(SK_SecurityID, Symbol, Issue, Status, Name, ExchangeID, SK_CompanyID,
                                SharesOutstanding, FirstTrade, FirstTradeOnExchange, Dividend,
                                IsCurrent, BatchID, EffectiveDate, EndDate)
        SELECT * FROM DimSecurityUpdated
           """)
    return spark.sql("SELECT * FROM DimSecurity")

# spark.sql("DELETE FROM DimSecurity")
# finwire_sec = load_finwires_into_dim_security("test")
# finwire_sec.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load FIN files (Financial table)

# COMMAND ----------

"""
            `PTS` string, `RecType` string , `Year` string , `Quarter` string, `QtrStartDate` string,
            `PostingDate` string, 
            `Revenue` string, `Earnings` string, `EPS` string , `DilutedEPS` string, `Margin` string,
            `Inventory` string, `Assets` string,
            `Liabilities` string, `ShOut` string, `DilutedShOut` string, `CoNameOrCIK` string
        """
def load_finwires_into_financial_table(dbname):
    spark.sql(f"USE {dbname}")
    spark.sql("""
        INSERT INTO Financial(SK_CompanyID, FI_YEAR, FI_QTR, FI_QTR_START_DATE, FI_REVENUE,
                              FI_NET_EARN, FI_BASIC_EPS, FI_DILUT_EPS, FI_MARGIN, FI_INVENTORY,
                              FI_ASSETS, FI_LIABILITY, FI_OUT_BASIC, FI_OUT_DILUT)
          SELECT DISTINCT
              SK_CompanyID,
              Year AS FI_YEAR,
              Quarter AS FI_QTR,
              TO_DATE(QtrStartDate, 'yyyyMMdd') AS FI_QTR_START_DATE,
              Revenue AS FI_REVENUE,
              Earnings AS FI_NET_EARN,
              EPS AS FI_BASIC_EPS,
              DilutedEPS AS FI_DILUT_EPS,
              Margin AS FI_MARGIN,
              Inventory AS FI_INVENTORY,
              Assets AS FI_ASSETS,
              Liabilities AS FI_LIABILITY,
              ShOut AS FI_OUT_BASIC,
              DilutedSHOut AS FI_OUT_DILUT
          FROM finwire_fin f JOIN DimCompany c ON
              (c.CompanyID=f.CoNameOrCIK OR c.Name= f.CoNameOrCIK)
    """)
    return spark.sql("SELECT * FROM Financial")

# spark.sql("DELETE FROM Financial")
# financial = load_finwires_into_financial_table("test")
# financial.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dim Trade

# COMMAND ----------

"""
    Loading status type and trade type into staging database
"""

def load_status_type(dbname, staging_area_folder):
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


def load_trade_type(dbname, staging_area_folder):
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

# trade_type = load_trade_type("test")
# trade_type.limit(3).toPandas()

# status_type = load_status_type("test")
# status_type.limit(3).toPandas()

# COMMAND ----------

#TradeHistory.txt
#The TradeHistory.txt file is a plain-text file with variable length fields separated by a vertical
#bar (“|”). Records have a terminator character appropriate for the System Under Test. This
#file is used only in the Historical Load.


def load_trade_view(dbname, staging_area_folder):
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
    
def load_tradehistory_view(dbname, staging_area_folder):
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## FactMarketHistory

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

def load_staging_FactMarketStory(dbname, staging_area_folder):
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
               INSERT INTO FactMarketHistory(ClosePrice, DayHigh, DayLow, Volume, SK_SecurityID, SK_CompanyID, SK_DateID, SK_FiftyTwoWeekHighDate, SK_FiftyTwoWeekLowDate,  FiftyTwoWeekHigh, FiftyTwoWeekLow, Yield, PERatio, BatchID)
       SELECT * FROM dailymarket_insert
    """)
    
    return spark.sql("""
       Select * from FactMarketHistory
    """)

# spark.sql("""DELETE FROM FactMarketHistory""")
# load_staging_FactMarketStory("test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prospect

# COMMAND ----------

#Create prospect
from pyspark.sql.functions import udf, struct
from datetime import datetime

def get_marketingnameplate(row):
    result = []
    
    if (row.NetWorth and row.NetWorth > 1000000) or (row.Income and row.Income > 200000):
        result.append("HighValue")
    if (row.NumberChildren and row.NumberChildren > 3) or ( row.NumberCreditCards and row.NumberCreditCards > 5):
        result.append("Expenses")
    if (row.Age and row.Age > 45):
        result.append("Boomer")
    if (row.Income and row.Income < 50000) or (row.CreditRating and row.CreditRating < 600) or (row.NetWorth and row.NetWorth < 100000):
        result.append("MoneyAlert")
    if (row.NumberCars and row.NumberCars > 3) or (row.NumberCreditCards and row.NumberCreditCards > 7):
        result.append("Spender")
    if (row.Age and row.Age < 25) and (row.NetWorth and row.NetWorth > 1000000):
        result.append("Inherited")
    
    return "+".join(result) if result else None


def load_staging_Prospect(dbname, staging_area_folder):
    spark.sql(f"USE {dbname}")
    spark.sql("""DELETE FROM Prospect """)
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
    
    udf_marketing = udf(lambda row: get_marketingnameplate(row), StringType())
    Prospect_ = Prospect_.withColumn('MarketingNameplate', udf_marketing(struct([Prospect_[x] for x in Prospect_.columns])))
    
    now = datetime.utcnow()
    
    DimDate = spark.sql("""
        SELECT SK_DateID FROM DimDate WHERE SK_DateID = 20201231
    """)
    Prospect_ = Prospect_.crossJoin(DimDate)
    Prospect_.createOrReplaceTempView("Prospect_")
    
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

#Prospect_ = load_staging_Prospect("test")
#Prospect_.limit(3).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Industry

# COMMAND ----------

#Load industry 

def load_staging_Industry(dbname, staging_area_folder):
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

# spark.sql("USE test")
# spark.sql("DROP TABLE Industry")
# create_industry_table("test")
# Industry_ = load_staging_Industry("test", f"gs://tpcdi-with-spark-bdma/TPCDI_Data/TPCDI_Data/Scale3/Batch1")
# Industry_.limit(3).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### XML Customer 

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType, DateType, TimestampType
import xml.etree.ElementTree as ET
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime


def customer_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'NEW':
                record = []
                list_of_attributes = ['C_ID', 'C_TAX_ID', 'C_GNDR', 'C_TIER', 'C_DOB']
                for attribute in list_of_attributes:
                    try:
                        record.append(Customer.attrib[attribute])
                    except:
                        record.append(None)
                for Element in Customer:
                    if Element.tag == 'ContactInfo':
                        for Subelement in Element:
                            if Subelement.tag[:-1] == 'C_PHONE_':
                                phone_number = ''
                                for Subsubelement in Subelement:
                                    if isinstance(Subsubelement.text, str):                                
                                        phone_number += Subsubelement.text + " "
                                if len(phone_number)>1:
                                    phone_number = phone_number[:-1]
                                else:
                                    phone_number = None
                                record.append(phone_number)
                            else:
                                record.append(Subelement.text)
                    elif Element.tag == 'Account':
                        for attribute in Element.attrib.values():
                            record.append(attribute)
                        for Subelement in Element:
                            record.append(Subelement.text)
                    else:
                        for Subelement in Element:
                            record.append(Subelement.text)
                records.append(record)
    return records

def add_account_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'ADDACCT':
                record = []
                record.append(Customer.attrib['C_ID'])
                for Element in Customer:
                    if Element.tag == 'Account':
                        for attribute in Element.attrib.values():
                            record.append(attribute)
                        for Subelement in Element:
                            record.append(Subelement.text)
                records.append(record)
    return records

# COMMAND ----------

#Update

def update_customer_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'UPDCUST':
                record = []
                list_of_attributes = ['C_ID', 'C_TAX_ID', 'C_GNDR', 'C_TIER', 'C_DOB']
                for attribute in list_of_attributes:
                    try:
                        record.append(Customer.attrib[attribute])
                    except:
                        record.append(None)
                for Element in Customer:
                    dict={
                    "C_L_NAME":None,
                    "C_F_NAME":None,
                    "C_M_NAME":None,
                    'C_ADLINE1':None,
                    'C_ADLINE2':None,
                    'C_ZIPCODE':None,
                    'C_CITY':None,
                    'C_STATE_PROV':None,
                    'C_CTRY':None,
                    'C_PRIM_EMAIL':None,
                    'C_ALT_EMAIL':None,
                    'C_PHONE_1':None,
                    'C_PHONE_2':None,
                    'C_PHONE_3':None,
                    "C_LCL_TX_ID":None,
                    "C_NAT_TX_ID":None
                    }
                    if Element.tag == 'ContactInfo':
                        for Subelement in Element:
                            if Subelement.tag[:-1] == 'C_PHONE_':
                                phone_number = ''
                                for Subsubelement in Subelement:
                                    if isinstance(Subsubelement.text, str):                                
                                        phone_number += Subsubelement.text + " "
                                if len(phone_number)>1:
                                    phone_number = phone_number[:-1]
                                else:
                                    phone_number = None
                                dict[Subelement.tag] = phone_number
                            else:
                                dict[Subelement.tag] = Subelement.text
                    elif Element.tag == 'Account':
                        continue
                    else:
                        for Subelement in Element:
                            dict[Subelement.tag] = Subelement.text
                records.append(record+list(dict.values()))
    return records

def update_account_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'UPDACCT':
                record = []
                record.append(Customer.attrib['C_ID'])
                dict = {
                        "CA_B_ID":None,
                        "CA_NAME":None}
                
                for Account in Customer:
                    record.append(Account.attrib['CA_ID'])
                    try:
                        record.append(Account.attrib['CA_TAX_ST'])
                    except:
                        record.append(None)
                        dict = {
                        "CA_B_ID":None,
                        "CA_NAME":None}
                    for element in Account:
                        dict[element.tag] = element.text
                records.append(record+list(dict.values()))
    return records

# COMMAND ----------

#inactive

def inactive_parser(rdd):
    root = ET.fromstring(rdd[0])
    records= []
    for Action in root:
        for Customer in Action:
            ActionType = Action.attrib['ActionType']
            if ActionType == 'INACT' or ActionType == 'CLOSEACCT':
                records.append(Customer.attrib['C_ID'])
    return records

# COMMAND ----------

def load_customers(dbname, staging_area_folder):
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

    customers_updated = customers_updated.toDF(*columns).rdd
    def customer_updater(row):
        new_row= [row.C_ID]
        for column in columns:
            if column != 'C_ID' and (not '_update' in column):
                if not getattr(row,column+'_update') is None:
                    new_row.append(getattr(row,column+'_update'))
                else:
                    new_row.append(getattr(row,column))
        return new_row
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

def load_account(dbname, staging_area_folder):
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continue DimTrade

# COMMAND ----------

spark.sql(f"DROP TABLE if exists DimTrade")
create_dim_trade("test")


# COMMAND ----------

from pyspark.sql.functions import *

def load_staging_dim_trade(dbname, staging_area_folder):
    trade_view = load_trade_view(dbname, staging_area_folder)
    tradehistory_view = load_tradehistory_view(dbname, staging_area_folder)
    
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
    trade.printSchema()
    
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
    
# load_staging_dim_trade("test")

# COMMAND ----------

dim_trade = spark.sql("""
        SELECT * FROM DimTrade
""")
#dim_trade.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Cash Balances

# COMMAND ----------

def load_fact_cash_balances(dbname, staging_area_folder):
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
                           AccountID, 
                           SK_DateID, 
                           sum(CT_AMT) as Cash, 
                           CAST('1' as INT) as BatchID 
                       From cashTrans join DimAccount as ac on (CT_CA_ID =ac.AccountID) 
                       join DimDate as dt on dt.DateValue = Date(CT_DTS) 
                       Group by AccountID, SK_CustomerID, SK_DateID""")
    
    
    
    factCashBalances.write.option("overwriteSchema", "true").saveAsTable("FactCashBalances", mode="overwrite")
    factCashBalances.show(3)
    return factCashBalances

# load_fact_cash_balances("FactCashBalances")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Holdings

# COMMAND ----------

def load_fact_holdings(dbname, staging_area_folder):
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

# load_fact_holdings("FactHoldings")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Fact Watches

# COMMAND ----------

"""
            SK_CustomerID BIGINT,
            SK_SecurityID BIGINT,
            SK_DateID_DatePlaced BIGINT,
            SK_DateID_DateRemoved BIGINT,
            BatchID INTEGER
            
            
"""

def load_fact_watches(dbname, staging_area_folder):
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

# spark.sql("USE test")
# spark.sql("DELETE FROM FactWatches")
# watches= load_fact_watches("test")
# watches.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Historical Load

# COMMAND ----------

import time
import pandas as pd

def run_historical_load(scale_factors=["Scale3"]):
    dbname = "test"
     # Options "Scale3", "Scale6", "Scale9", "Scale12"
    for scale_factor in scale_factors:
        metrics = {}
        # Init DB
        start = time.time()
        clean_warehouse(dbname)
        create_warehouse(dbname)
        end = time.time() - start
        
        metrics["create_db_time"] = end
        
        staging_area_folder = f"/Users/sebastianneri/Documents/BDMA/ULB/INFO-H419 - Data warehouses/Projects/TPC-DI/data/{scale_factor}/Batch1"
        
        # Run historical load
        start = time.time()
        dimdate = load_dim_date(dbname, staging_area_folder)
        dimtime = load_dim_time(dbname, staging_area_folder)
        taxrate = load_tax_rate(dbname, staging_area_folder)
        staginghr = load_staging_hr_file(dbname, staging_area_folder)
        industry = load_staging_Industry(dbname, staging_area_folder)

        
        load_finwire_files(dbname, scale_factor)
        dimcompany = load_finwires_into_dim_company(dbname, scale_factor)
        dimsecurity = load_finwires_into_dim_security(dbname)
        fintable = load_finwires_into_financial_table(dbname)
        
        statustype = load_status_type(dbname, staging_area_folder)
        tradetype = load_trade_type(dbname, staging_area_folder)
        
        factmarkethistory =load_staging_FactMarketStory(dbname, staging_area_folder)
        prospect = load_staging_Prospect(dbname, staging_area_folder)
        
        customer = load_customers(dbname, staging_area_folder)
        account = load_account(dbname, staging_area_folder)
        
        dimtrade = load_staging_dim_trade(dbname, staging_area_folder)
        factcashbalance = load_fact_cash_balances(dbname, staging_area_folder)
        holding = load_fact_holdings(dbname, staging_area_folder)
        watch = load_fact_watches(dbname, staging_area_folder)
        end = time.time() - start
        
        metrics["et"] = end
        
        rows = dimdate.count() + dimtime.count() + taxrate.count() + staginghr.count() + dimcompany.count() + dimsecurity.count() + fintable.count() + statustype.count() + tradetype.count() + factmarkethistory.count() + prospect.count() + industry.count() + dimtrade.count() + factcashbalance.count() + holding.count() + watch.count() + customer.count() + account.count()
        metrics["rows"] = rows
        metrics["throughput"] = (rows / end)
        
        metrics_df = pd.DataFrame(metrics, index=[0])
        dbutils.fs.put(f"/FileStore/historical_load_{scale_factor}.csv", metrics_df.to_csv())

run_historical_load()

# COMMAND ----------

# import pandas as pd
# metrics = {'create_db_time': -66.36713218688965, 'et': -1484.7563452720642, 'rows': 22806424, 'throughput': -15360.381568748904}
# metrics_df = pd.DataFrame(metrics, index=[0])
# scale_factor = "Scale3"
# client = create_gcs_client()
# dbutils.fs.put("/FileStore/test.csv", metrics_df.to_csv())
# # metrics_df.to_csv(f"/dbfs/Filestore/historical_load.csv")

# COMMAND ----------

spark.sql("SELECT * FROM Industry").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Incremental Update 1
# MAGIC 
# MAGIC ### This step:
# MAGIC - populates the data warehouse
# MAGIC - applies transformations described in clause 4.6 of the tpc-di manual (batch2 folder on the generated data)
# MAGIC - performs validations based on clause 7.4
# MAGIC - upon completion of the validation stage, a phase completion record is written into DIMessages table
# MAGIC - this step must be timed

# COMMAND ----------

staging_area_folder_up1 = f"gs://tpcdi-with-spark-bdma/TPCDI_Data/TPCDI_Data/{scale_factor}/Batch2/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transformation Details for Incremental Updates

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DimenCustomer

# COMMAND ----------

from datetime import datetime

def load_dimen_customer(dbname, staging_area_folder_upl):
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
                       CAST('2' as INT) as BatchID, 
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
    
# load_dimen_customer("test")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DimenAccount

# COMMAND ----------

from datetime import datetime

def load_dimen_account(dbname, staging_area_folder_upl):
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
    
    dimAccount.write.mode("append").saveAsTable( "DimAccount", mode="append")
    
    return dimAccount
    
# load_dimen_account("test")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrade

# COMMAND ----------

def load_update_dimen_trade(dbname,staging_area_folder_up1):
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
    trade.printSchema()
    
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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts
# MAGIC ### Fact Cash Balances

# COMMAND ----------

def load_update_fact_cash_balances(dbname, staging_area_folder_upl):
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
                           AccountID, 
                           SK_DateID, 
                           sum(CT_AMT) as Cash, 
                           CAST('2' as INT) as BatchID 
                       From cashTrans join DimAccount as ac on (CT_CA_ID =ac.AccountID) 
                       join DimDate as dt on dt.DateValue = Date(CT_DTS) 
                       Group by AccountID, SK_CustomerID, SK_DateID""")
    
    
    
    factCashBalances.write.option("append", "true").saveAsTable("FactCashBalances", mode="append")
#     factCashBalances.show(3)
    return factCashBalances

# load_fact_cash_balances("FactCashBalances")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Holdings

# COMMAND ----------

def load_update_fact_holdings(dbname, staging_area_folder_upl):
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

# load_fact_holdings("FactHoldings")

# COMMAND ----------

# MAGIC %md
# MAGIC #### FactWatches

# COMMAND ----------

"""
            SK_CustomerID BIGINT,
            SK_SecurityID BIGINT,
            SK_DateID_DatePlaced BIGINT,
            SK_DateID_DateRemoved BIGINT,
            BatchID INTEGER
            
            
"""

def load_update_fact_watches(dbname, staging_area_folder_upl):
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

# spark.sql("USE test")
# spark.sql("DELETE FROM FactWatches WHERE BatchID=2")
# watches= load_fact_watches("test")
# watches.where("BatchID = 2").limit(10).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## FactMarketistory

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

def load_update_staging_FactMarketStory(dbname, staging_area_folder_upl):
#     spark.sql(f"USE {dbname}")
#     spark.sql("""DELETE FROM FactMarketHistory""")
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

# marketh = load_staging_FactMarketStory("test", "gs://tpcdi-with-spark-bdma/TPCDI_Data/TPCDI_Data/Scale3/Batch2/")
# marketh.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prospect

# COMMAND ----------

#Create prospect
from pyspark.sql.functions import udf, struct
from datetime import datetime

def get_marketingnameplate(row):
    result = []
    
    if (row.NetWorth and row.NetWorth > 1000000) or (row.Income and row.Income > 200000):
        result.append("HighValue")
    if (row.NumberChildren and row.NumberChildren > 3) or ( row.NumberCreditCards and row.NumberCreditCards > 5):
        result.append("Expenses")
    if (row.Age and row.Age > 45):
        result.append("Boomer")
    if (row.Income and row.Income < 50000) or (row.CreditRating and row.CreditRating < 600) or (row.NetWorth and row.NetWorth < 100000):
        result.append("MoneyAlert")
    if (row.NumberCars and row.NumberCars > 3) or (row.NumberCreditCards and row.NumberCreditCards > 7):
        result.append("Spender")
    if (row.Age and row.Age < 25) and (row.NetWorth and row.NetWorth > 1000000):
        result.append("Inherited")
    
    return "+".join(result) if result else None


def load_update_staging_Prospect(dbname, staging_area_folder_upl):
    spark.sql(f"USE {dbname}")
    spark.sql("""DELETE FROM Prospect """)
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
    
    spark.sql("""
        MERGE INTO Prospect
        USING (SELECT * FROM Prospect_) AS NP
        ON Prospect.AgencyID = NP.AgencyID
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)
    
    return spark.sql("""
        SELECT * FROM Prospect WHERE BatchID = 2
    """)
# Prospect_ = load_staging_Prospect("test", "gs://tpcdi-with-spark-bdma/TPCDI_Data/TPCDI_Data/Scale3/Batch2/")
# Prospect_.limit(3).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Incremental Update

# COMMAND ----------

import time
import pandas as pd


import string
import random

from statistics import geometric_mean

def get_max(num1, num2):
    if num1 > num2:
        return num1
    return num2

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def run_historical_load(dbname, scale_factor, file_id):
    metrics = {}
    # Init DB
    start = time.time()
    clean_warehouse(dbname)
    create_warehouse(dbname)
    end = time.time() - start

    metrics["create_db_time"] = end

    staging_area_folder = f"gs://tpcdi-with-spark-bdma/TPCDI_Data/TPCDI_Data/{scale_factor}/Batch1"

    # Run historical load
    start = time.time()
    dimdate = load_dim_date(dbname, staging_area_folder)
    dimtime = load_dim_time(dbname, staging_area_folder)
    taxrate = load_tax_rate(dbname, staging_area_folder)
    staginghr = load_staging_hr_file(dbname, staging_area_folder)
    industry = load_staging_Industry(dbname, staging_area_folder)


    load_finwire_files(dbname, scale_factor)
    dimcompany = load_finwires_into_dim_company(dbname, scale_factor)
    dimsecurity = load_finwires_into_dim_security(dbname)
    fintable = load_finwires_into_financial_table(dbname)

    statustype = load_status_type(dbname, staging_area_folder)
    tradetype = load_trade_type(dbname, staging_area_folder)

    factmarkethistory =load_staging_FactMarketStory(dbname, staging_area_folder)
    prospect = load_staging_Prospect(dbname, staging_area_folder)

    customer = load_customers(dbname, staging_area_folder)
    account = load_account(dbname, staging_area_folder)

    dimtrade = load_staging_dim_trade(dbname, staging_area_folder)
    factcashbalance = load_fact_cash_balances(dbname, staging_area_folder)
    holding = load_fact_holdings(dbname, staging_area_folder)
    watch = load_fact_watches(dbname, staging_area_folder)
    end = time.time() - start

    metrics["et"] = end

    rows = dimdate.count() + dimtime.count() + taxrate.count() + staginghr.count() + dimcompany.count() + dimsecurity.count() + fintable.count() + statustype.count() + tradetype.count() + factmarkethistory.count() + prospect.count() + industry.count() + dimtrade.count() + factcashbalance.count() + holding.count() + watch.count() + customer.count() + account.count()
    metrics["rows"] = rows
    metrics["throughput"] = (rows / end)

    metrics_df = pd.DataFrame(metrics, index=[0])
    dbutils.fs.put(f"/FileStore/historical_load_{scale_factor}_{file_id}.csv", metrics_df.to_csv())
    print(f"/FileStore/historical_load_{scale_factor}_{file_id}.csv")
    return metrics_df

def run_incremental_load(dbname, scale_factor, file_id):
    metrics = {}

    staging_area_folder = f"gs://tpcdi-with-spark-bdma/TPCDI_Data/TPCDI_Data/{scale_factor}/Batch2"

    # Run incremental update
    start = time.time()
    customer = load_dimen_customer(dbname, staging_area_folder)
    account = load_dimen_account(dbname, staging_area_folder)
    dimtrade = load_update_dimen_trade(dbname, staging_area_folder)

    factcashbalance = load_update_fact_cash_balances(dbname, staging_area_folder)
    holding = load_update_fact_holdings(dbname, staging_area_folder)
    watch = load_update_fact_watches(dbname, staging_area_folder)

    factmarkethistory =load_update_staging_FactMarketStory(dbname, staging_area_folder)
    prospect = load_update_staging_Prospect(dbname, staging_area_folder)
    end = time.time() - start

    metrics["et"] = end

    rows = factmarkethistory.count() + prospect.count() + dimtrade.count() + factcashbalance.count() + holding.count() + watch.count() + customer.count() + account.count()

    metrics["rows"] = rows
    metrics["throughput"] = (rows / get_max(end,1800))

    metrics_df = pd.DataFrame(metrics, index=[0])
    dbutils.fs.put(f"/FileStore/incremental_load_{scale_factor}_{file_id}.csv", metrics_df.to_csv())
    print(f"/FileStore/incremental_load_{scale_factor}_{file_id}.csv")
    return metrics_df

def run(scale_factors=["Scale12"]):
  #"Scale3", "Scale6", "Scale9"]): 
    dbname = "test"
     # Options "Scale3", "Scale6", "Scale9", "Scale12"
    file_id = id_generator()
    print(file_id)
    for scale_factor in scale_factors:
        metrics = {}
        hist_res = run_historical_load(dbname, scale_factor, file_id)
        hist_incr = run_incremental_load(dbname, scale_factor, file_id)
        
        metrics["TPC_DI_RPS"] = int(geometric_mean([hist_res["throughput"], hist_incr["throughput"]]))
        metrics_df = pd.DataFrame(metrics, index=[0])
        dbutils.fs.put(f"/FileStore/overall_stats_{scale_factor}_{file_id}.csv", metrics_df.to_csv())
        print(f"/FileStore/overall_stats_{scale_factor}_{file_id}.csv")
    
        print(hist_res, hist_incr, metrics_df)
run()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Incremental Update 2
# MAGIC This step:
# MAGIC - populates the data warehouse
# MAGIC - applies transformations described in clause 4.6 of the tpc-di manual (batch3 folder on the generated data)
# MAGIC - performs validations based on clause 7.4
# MAGIC - upon completion of the validation stage, a phase completion record is written into DIMessages table
# MAGIC - this step must be timed

# COMMAND ----------

get_max(5,10)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Transformation Details for Incremental Updates

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DimenCustomer

# COMMAND ----------

from datetime import datetime

def load_dimen_customer(dbname):
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
    
load_dimen_customer("test")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DimenAccount

# COMMAND ----------

from datetime import datetime

def load_dimen_account(dbname):
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
    
load_dimen_account("test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Automated Audit Phase
# MAGIC This step:
# MAGIC - audit data is loaded into the audit table following these rules:
# MAGIC   - The first row in every audit data file contains only the field names, not audit data. This record may be used to aid in the load process, but must not be loaded into the Audit table.
# MAGIC   - Each field in the audit data must be loaded into the cooresponding column (the column of the same name) of the Audit table.
# MAGIC - it's valid to create helper functions to aid performance of automated audit
# MAGIC - contents of data warehouse must not be modified on this stage. 
# MAGIC - at the beginning of this step, data visibility query 1 (appendix C from the manual) must be executed.
# MAGIC - audit query must be executed (appendix A).
# MAGIC - a valid benchmark run must report "OK" for every test.

# COMMAND ----------

