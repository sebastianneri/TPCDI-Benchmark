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

