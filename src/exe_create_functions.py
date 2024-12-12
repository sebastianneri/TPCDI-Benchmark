import shutil
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

#########################################################################
#                                                                       #
#                       CREATE DIM TABLES QUERIES                       #        
#                                                                       #    
#########################################################################

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



#########################################################################
#                                                                       #
#                       CREATE FACT TABLES QUERIES                      #        
#                                                                       #    
#########################################################################


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


#########################################################################
#                                                                       #
#                     CREATE FACT TABLES OTHER TABLES                   #        
#                                                                       #    
#########################################################################


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


#########################################################################
#                                                                       #
#                       CREATE/CLEAN DATA WAREHOUSE                     #        
#                                                                       #    
#########################################################################


def create_warehouse(dbname="test"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    create_dims(dbname)
    create_facts(dbname)
    create_other_tables(dbname)


def clean_warehouse(dbname="test"):
    spark.sql(f"DROP DATABASE IF EXISTS {dbname} CASCADE")
    warehouse_path = os.getcwd()+'/warehouse/'
    shutil.rmtree(warehouse_path)
    os.makedirs(warehouse_path)
    print(f"Warehouse {dbname} deleted.")

