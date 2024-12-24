import findspark
findspark.init()
from pyspark.sql.functions import col, udf, explode, map_keys, array, pandas_udf, struct
from pyspark.sql.types import StringType, Row
import pandas as pd
import time
import string
import random
import pyspark
from pyspark.sql import SparkSession
import os
import shutil




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




