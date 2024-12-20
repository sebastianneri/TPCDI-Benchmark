# Import the queries
import findspark
findspark.init()
from src.visibilities_queries import tpcdi_visibility_q1, tpcdi_visibility_q2
from src.validation_query import tpcdi_validation_query
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession



#########################################################################
#                                                                       #
#                         VISIBILITY QUERIES                            #        
#                                                                       #    
#########################################################################

# Function to verify visibility of the data written to the Data Warehouse
# The data collected by this query is stored in the DImessages

# Execution function visibility query 1
def execute_visibility_query_1(spark,visibility_query1=tpcdi_visibility_q1):
    spark.sql(visibility_query1)


# Execution function visibility query 2
def execute_visibility_query_2(spark,visibility_query2=tpcdi_visibility_q2):
    spark.sql(visibility_query2)


#########################################################################
#                                                                       #
#                         VALIDITY QUERIES                              #        
#                                                                       #    
#########################################################################


 # The batch validation query writes results into the DImessages table defined in Clause 3.2.8,
 # which are used in the automated audit phase to validate the transformed data in the Data
 # Warehouse.

# Execution function validity query
def execute_validity_query(spark,validity_query=tpcdi_validation_query):
    spark.sql(validity_query)


