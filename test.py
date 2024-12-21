import findspark
findspark.init()
import pyspark
findspark.find()
import shutil
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, map_keys, array, pandas_udf

warehouse_path = os.getcwd()+'/warehouse/'
shutil.rmtree(warehouse_path)
os.makedirs(warehouse_path)

conf = pyspark.SparkConf().set("spark.sql.legacy.createHiveTableByDefault", "false").set("spark.sql.autoBroadcastJoinThreshold", -1).set("spark.sql.warehouse.dir", warehouse_path).setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

def cast_to_target_schema(source_table: str, target_table: str):
    # Load source and target DataFrames
    source_df = spark.table(source_table)
    target_df = spark.table(target_table)

    # Get the schema of the target table
    target_schema = target_df.schema

    # Create a list to hold columns, casting only the matching columns
    casted_columns = []

    for column in source_df.columns:
        if column in target_df.columns:
            # Get the target data type for matching columns
            target_dtype = target_schema[column].dataType
            # Cast to target data type and add to the list
            casted_columns.append(col(column).cast(target_dtype).alias(column))
        else:
            # Keep the original column if it does not exist in target schema
            casted_columns.append(col(column))

    # Select all columns from the source with necessary casts applied
    casted_df = source_df.select(*casted_columns)

    return casted_df

def create_audit_table(dbname):
    spark.sql(f"USE {dbname}")
    #spark.sql(f"DROP TABLE Audit")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS Audit ( 
           DataSet CHAR(20),
           BatchID INTEGER,
           Date DATE,
           Attribute CHAR(50),
           Value FLOAT,
           DValue FLOAT 
        )
    """
    )
    print("Created table Audit.")

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

def run_audit(dbname, staging_area_folder, scale_factor):
    staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/Batch1"
    create_audit_table(dbname)
    load_audit_data(staging_area_folder)
    file_path = './Audit Queries/tpcdi_audit.sql'
    spark.sql(load_queries(file_path))


dbname = 'test'
scale_factor = 'Scale3'
staging_area_folder = f"{os.getcwd()}/data/{scale_factor}/Batch1"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {"test"}")
run_audit(dbname, staging_area_folder, scale_factor)
