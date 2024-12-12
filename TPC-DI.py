# import sys
import os
# os.chdir("..")  # Change to the parent directory
#  print("Current working directory:", os.getcwd())  # Verify the change
# Add the `src` directory to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

import findspark
findspark.init()
from src.aux_functions import get_max
from src.run_functions import run, run_historical_load
from src.exe_create_functions import clean_warehouse, create_dim_trade, create_warehouse
import pyspark
# findspark.find()
import shutil
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
#from pyspark.sql.functions import expr
from pyspark.sql.window import Window
from pyspark.sql.functions import *
print("All libraries loaded!!")



#Create prospect
# from pyspark.sql.functions import udf, struct

warehouse_path = os.getcwd()+'/warehouse/'
shutil.rmtree(warehouse_path)
os.makedirs(warehouse_path)

conf = pyspark.SparkConf().set("spark.sql.legacy.createHiveTableByDefault", "false").set("spark.sql.autoBroadcastJoinThreshold", -1).set("spark.sql.warehouse.dir", warehouse_path).setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
spark_ui_url = sc.uiWebUrl
print(f"Spark UI is available at {spark_ui_url}")


scale_factor = "Scale3" 

# Clean data warehouse
print("Clean data warehouse")
clean_warehouse("test")
print("Clean data warehouse ready!!")
# COMMAND ----------
# Create datawarehouse
print("Create data warehouse")
create_warehouse()
print("Create data warehouse ready!!")


# Create Dimtrade table
spark.sql(f"DROP TABLE if exists DimTrade")
create_dim_trade("test")


# try query to dim_trade
dim_trade = spark.sql("""
        SELECT * FROM DimTrade
""")

# Run historical load 
print("Running historical load data")
run_historical_load()
print("Historical load data ready!!")
# COMMAND ----------


# COMMAND ----------

spark.sql("SELECT * FROM Industry").toPandas()

# COMMAND ----------

staging_area_folder_up1 = f"{os.getcwd()}/data/{scale_factor}/Batch2/"

run()


get_max(5,10)




