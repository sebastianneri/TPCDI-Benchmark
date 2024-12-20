import findspark
findspark.init()
import pandas as pd
import time
import string
import random
import pyspark
from pyspark.sql import SparkSession
import os
import shutil


warehouse_path = os.getcwd()+'/warehouse/'
shutil.rmtree(warehouse_path)
os.makedirs(warehouse_path)

# Configure Spark
conf = (
    pyspark.SparkConf()
    .set("spark.sql.legacy.createHiveTableByDefault", "false")
    .set("spark.sql.autoBroadcastJoinThreshold", -1)
    .set("spark.sql.debug.maxToStringFields", "1000")
    .set("spark.sql.warehouse.dir", warehouse_path)
    .setAppName("appName")
    .setMaster("local")
)

# Initialize SparkContext
sc = pyspark.SparkContext(conf=conf)

# Create SparkSession
spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()


# Get Spark UI URL
#spark_ui_url = sc.uiWebUrl
#print(f"Spark UI is available at {spark_ui_url}")
