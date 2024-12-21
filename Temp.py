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
import shutil
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


warehouse_path = os.getcwd()+'/warehouse/'
shutil.rmtree(warehouse_path)
os.makedirs(warehouse_path)

conf = pyspark.SparkConf().set("spark.sql.legacy.createHiveTableByDefault", "false").set("spark.sql.autoBroadcastJoinThreshold", -1).set("spark.sql.warehouse.dir", warehouse_path).setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
spark_ui_url = sc.uiWebUrl
print(f"Spark UI is available at {spark_ui_url}")


scale_factor = "Scale3" # Options "Scale3"]):#, "Scale4", "Scale5", "Scale6"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Stack

# COMMAND ----------


def clean_warehouse(dbname="test"):
    pass



def cast_to_target_schema(source_table: str, target_table: str):
    pass


def create_dim_account(dbname):
    pass


def create_dim_broker(dbname):
    pass


def create_dim_company(dbname):
    pass


def create_dim_customer(dbname):
    pass


def create_dim_date(dbname):
    pass


def create_dim_security(dbname):
    pass


def create_dim_time(dbname):
    pass


def create_dim_trade(dbname):
    pass


def create_dimessages_table(dbname):
    pass


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


def create_fact_cash_balances(dbname):
    pass


def create_fact_holdings(dbname):
    pass


def create_fact_market_history(dbname):
    pass


def create_fact_watches(dbname):
    pass


def create_facts(dbname):
    create_fact_cash_balances(dbname)
    create_fact_holdings(dbname)
    create_fact_market_history(dbname)
    create_fact_watches(dbname)


def create_industry_table(dbname):
    pass

def create_financial_table(dbname):
    pass

def create_prospect_table(dbname):
    pass

def create_status_type_table(dbname):
    pass
    


def create_taxrate_table(dbname):
    pass
    


def create_tradetype_table(dbname):
    pass
    

def create_audit_table(dbname):
    pass



def create_other_tables(dbname):
    create_industry_table(dbname)
    create_financial_table(dbname)
    create_prospect_table(dbname)
    create_status_type_table(dbname)
    create_taxrate_table(dbname)
    create_tradetype_table(dbname)
    create_audit_table(dbname)



def create_warehouse(dbname="test"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    create_dims(dbname)
    create_facts(dbname)
    create_other_tables(dbname)


def clean_warehouse(dbname="test"):
    pass

clean_warehouse("test")



create_warehouse()



def create_gcs_client():
    pass


def load_dim_date(dbname, staging_area_folder):
    pass



def load_dim_time(dbname, staging_area_folder):
    pass


def load_tax_rate(dbname, staging_area_folder):
    pass

from pyspark.sql.functions import expr


def load_staging_hr_file(dbname, staging_area_folder):
    pass



from pyspark.sql.functions import col, udf, explode, map_keys, array, pandas_udf
from pyspark.sql.types import StringType, Row
import pandas as pd

@pandas_udf(StringType())
def extract_finwire_type(finwire_str):
    pass

@pandas_udf("""
            `PTS` string, `RecType` string, `CompanyName` string, `CIK` string, 
            `Status` string, `IndustryID` string , `SPrating` string, `FoundingDate` string,
            `AddrLine1` string, `AddrLine2` string, `PostalCode` string, `City` string,
            `StateProvince` string, `Country` string, `CEOname` string, `Description` string
        """)
def columnarize_finwire_data_cmp(finwire_str):
    pass


@pandas_udf("""
            `PTS` string, `RecType` string, `Symbol` string, `IssueType` string, `Status` string, 
            `Name` string, `ExID` string, `ShOut` string, `FirstTradeDate` string, 
            `FirstTradeExchg` string, `Dividend` string, `CoNameOrCIK` string
""")
def columnarize_finwire_data_sec(finwire_str):
    pass

@pandas_udf("""
            `PTS` string, `RecType` string , `Year` string , `Quarter` string, `QtrStartDate` string,
            `PostingDate` string, 
            `Revenue` string, `Earnings` string, `EPS` string , `DilutedEPS` string, `Margin` string,
            `Inventory` string, `Assets` string,
            `Liabilities` string, `ShOut` string, `DilutedShOut` string, `CoNameOrCIK` string
""")
def columnarize_finwire_data_fin(finwire_str):
    pass




def load_finwire_file(finwire_file_path, dbname, extract_type='CMP'):
    pass




def load_finwire_files(dbname, scale_factor):
    pass



def load_finwires_into_dim_company(dbname, scale_factor):
    pass

def load_finwires_into_dim_security(dbname):
    pass

def load_finwires_into_financial_table(dbname):
    pass



def load_status_type(dbname, staging_area_folder):
   pass


def load_trade_type(dbname, staging_area_folder):
    pass




def load_trade_view(dbname, staging_area_folder):
    pass
    
def load_tradehistory_view(dbname, staging_area_folder):
    pass


from pyspark.sql.window import Window
from pyspark.sql.functions import *

def load_staging_FactMarketStory(dbname, staging_area_folder):
    pass


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
    spark.sql(f"DROP TABLE Prospect")
    create_prospect_table(dbname)

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

def run_historical_load(scale_factors=["Scale3"]):#, "Scale4", "Scale5", "Scale6"]):
    dbname = "test"
     # Options "Scale3"]):#, "Scale4", "Scale5", "Scale6"
    for scale_factor in scale_factors:
        metrics = {}
        # Init DB
        start = time.time()
        clean_warehouse(dbname)
        create_warehouse(dbname)
        end = time.time() - start
        
        metrics["create_db_time"] = end
        
        staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/Batch1"
        
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
        
        dimdate_count = dimdate.count()
        dimtime_count = dimtime.count()
        taxrate_count = taxrate.count()
        staginghr_count = staginghr.count()
        dimcompany_count = dimcompany.count()
        dimsecurity_count = dimsecurity.count()
        fintable_count = fintable.count()
        statustype_count = statustype.count()
        tradetype_count = tradetype.count()
        factmarkethistory_count = factmarkethistory.count()
        prospect_count = prospect.count()
        industry_count = industry.count()
        dimtrade_count = dimtrade.count()
        factcashbalance_count = factcashbalance.count()
        holding_count = holding.count()
        watch_count = watch.count()
        customer_count = customer.count()
        account_count = account.count()

        # Sum the individual counts
        rows = dimdate_count + dimtime_count + taxrate_count + staginghr_count + dimcompany_count + dimsecurity_count + fintable_count + statustype_count + tradetype_count + factmarkethistory_count + prospect_count + industry_count + dimtrade_count + factcashbalance_count + holding_count + watch_count + customer_count + account_count

        metrics["rows"] = rows
        metrics["throughput"] = (rows / end)
        
        metrics_df = pd.DataFrame(metrics, index=[0])

        metrics_df.to_csv(f"{os.getcwd()}/results/data/historical_load_{scale_factor}.csv", index=False)

run_historical_load()

# COMMAND ----------

# import pandas as pd
# metrics = {'create_db_time': -66.36713218688965, 'et': -1484.7563452720642, 'rows': 22806424, 'throughput': -15360.381568748904}
# metrics_df = pd.DataFrame(metrics, index=[0])
# scale_factor = "Scale3"
# client = create_gcs_client()
# dbutils.fs.put("/FileStore/test.csv", metrics_df.to_csv())
# # metrics_df.to_csv(f"/results/data/historical_load.csv")

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

staging_area_folder_up1 = f"{os.getcwd()}/data/{scale_factor}/Batch2/"

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

    dimCustomer = cast_to_target_schema("dimCustomer_stream", "DimCustomer")
     
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
    
    dimAccount.createOrReplaceTempView("dimAccount_stream")
    dimAccount = cast_to_target_schema("dimAccount_stream", "DimAccount")

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
    
    spark.sql(f"USE {dbname}")
    spark.sql("""DROP TABLE FactMarketHistory""")

    create_fact_market_history(dbname)

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
    spark.sql("""DROP TABLE Prospect """)

    create_prospect_table(dbname)

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


def load_audit_data(staging_area_folder):
    files = os.listdir(staging_area_folder)
    columns = ["DataSet", "BatchID", "Date", "Attribute", "Value", "DValue"]
    for file in files:
        if "audit" in file:
            audit_data = spark.read.csv(f"{staging_area_folder}/{file}", header=True, sep=",")
            audit_data = audit_data.toDF(*columns)
            audit_data.createOrReplaceTempView("audit_temp")
            audit_data = cast_to_target_schema("audit_temp", "Audit")
            audit_data.write.mode("append").saveAsTable("Audit", mode="append")

def load_queries(file_path):
    with open(file_path) as file:
        query = " ".join(file.readlines())
        return query

def run_audit(dbname, scale_factor):
    create_audit_table(dbname)
    batches = ["Batch1", "Batch2", "Batch3"]
    file_path = './Audit Queries/tpcdi_audit.sql'
    spark.sql(load_queries(file_path))
    for batch in batches:
        staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/{batch}"
        load_audit_data(staging_area_folder)

def run_historical_load(dbname, scale_factor, file_id):
    metrics = {}
    # Init DB
    start = time.time()
    clean_warehouse(dbname)
    create_warehouse(dbname)
    end = time.time() - start

    metrics["create_db_time"] = end

    staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/Batch1/"

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

    factmarkethistory = load_staging_FactMarketStory(dbname, staging_area_folder)
    prospect = load_staging_Prospect(dbname, staging_area_folder)

    customer = load_customers(dbname, staging_area_folder)
    account = load_account(dbname, staging_area_folder)

    dimtrade = load_staging_dim_trade(dbname, staging_area_folder)
    factcashbalance = load_fact_cash_balances(dbname, staging_area_folder)
    holding = load_fact_holdings(dbname, staging_area_folder)
    watch = load_fact_watches(dbname, staging_area_folder)
    end = time.time() - start

    metrics["et"] = end
    dimdate_count = dimdate.count()
    dimtime_count = dimtime.count()
    taxrate_count = taxrate.count()
    staginghr_count = staginghr.count()
    dimcompany_count = dimcompany.count()
    dimsecurity_count = dimsecurity.count()
    fintable_count = fintable.count()
    statustype_count = statustype.count()
    tradetype_count = tradetype.count()
    factmarkethistory_count = factmarkethistory.count()
    prospect_count = prospect.count()
    industry_count = industry.count()
    dimtrade_count = dimtrade.count()
    factcashbalance_count = factcashbalance.count()
    holding_count = holding.count()
    watch_count = watch.count()
    customer_count = customer.count()
    account_count = account.count()

    # Sum the individual counts
    rows = dimdate_count + dimtime_count + taxrate_count + staginghr_count + dimcompany_count + dimsecurity_count + fintable_count + statustype_count + tradetype_count + factmarkethistory_count + prospect_count + industry_count + dimtrade_count + factcashbalance_count + holding_count + watch_count + customer_count + account_count

    metrics["rows"] = rows
    metrics["throughput"] = (rows / end)

    metrics_df = pd.DataFrame(metrics, index=[0])
    
    metrics_df.to_csv(f"{os.getcwd()}/results/data/historical_load_{scale_factor}_{file_id}.csv", index=False)
    return metrics_df

def run_incremental_load(dbname, scale_factor, file_id):
    metrics = {}

    clean_warehouse(dbname)
    create_warehouse(dbname)

    staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/Batch2"

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
    metrics_df.to_csv(f"{os.getcwd()}/results/data/incremental_load_{scale_factor}_{file_id}.csv", index=False)

    return metrics_df

def run(scale_factors=["Scale3"]):#, "Scale4", "Scale5", "Scale6"]): 
    dbname = "test"
     # Options "Scale3"]):#, "Scale4", "Scale5", "Scale6"
    file_id = id_generator()

    for scale_factor in scale_factors:
        metrics = {}
        hist_res = run_historical_load(dbname, scale_factor, file_id)
        hist_incr = run_incremental_load(dbname, scale_factor, file_id)
        run_audit(dbname, scale_factor)

        metrics["TPC_DI_RPS"] = int(geometric_mean([hist_res["throughput"], hist_incr["throughput"]]))
        metrics_df = pd.DataFrame(metrics, index=[0])
        metrics_df.to_csv(f"{os.getcwd()}/results/data/overall_stats_{scale_factor}_{file_id}.csv", index=False)
    
        print(hist_res, hist_incr, metrics_df)
try:
    run() 
except Exception as e:
    print(e)
    input()


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

def load_dimen_customer(dbname, staging_area_folder_up2):
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
    
#load_dimen_customer("test")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DimenAccount

# COMMAND ----------

from datetime import datetime

def load_dimen_account(dbname, staging_area_folder_up2):
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
    
#load_dimen_account("test")



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
