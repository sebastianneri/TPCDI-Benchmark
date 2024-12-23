# Import the queries
import findspark
findspark.init()
from src.visibilities_queries import tpcdi_visibility_q1, tpcdi_visibility_q2
from src.validation_query import tpcdi_validation_query_1, tpcdi_validation_query_2
import findspark
import threading
import time
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
def execute_visibility_query_1(spark,data_load_start_event,visibility_query1=tpcdi_visibility_q1):
    # Wait until the data load starts
    data_load_start_event.wait()
    print("Visibility queries will start 5 minutes after data load begins.")
    # Wait for 5 minutes after data loading begins
    time_sleep = 1 #300 seconds (5 minutes)
    time.sleep(time_sleep)  # 5 minutes = 300 seconds
    start_time = time.time()
    print("Starting visibility queries...")
    while True:
        # Simulate executing the query
        print(f"Executing visibility query at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
        result=spark.sql(visibility_query1)
        result.show()
        # Sleep for the remaining time in the 5-minute interval
        elapsed = time.time() - start_time
        if elapsed >= 60:  # Stop after 10 minutes
            print("Stopping visibility queries.")
            break
        time.sleep(max(time_sleep - (time.time() - start_time) % time_sleep, 0))  # Maintain 5-minute intervals


# Execution function visibility query 2
def execute_visibility_query_2(spark,visibility_query2=tpcdi_visibility_q2):
    result=spark.sql(visibility_query2)
    result.show()

#########################################################################
#                                                                       #
#                         VALIDITY QUERIES                              #        
#                                                                       #    
#########################################################################


 # The batch validation query writes results into the DImessages table defined in Clause 3.2.8,
 # which are used in the automated audit phase to validate the transformed data in the Data
 # Warehouse.

# Execution function validity query
def execute_validity_query1(spark,validity_query=tpcdi_validation_query_1):
    result=spark.sql(validity_query)
    result.show()

# Execution function validity query
def execute_validity_query2(spark,validity_query=tpcdi_validation_query_2):
    result=spark.sql(validity_query)
    result.show()


# Run the data load and visibility queries concurrently
def run_data_load_with_visibility_queries(incremental_load_func, visibility_query_func):
    # Create threads for data load and visibility queries
    data_incremental_load_thread = threading.Thread(target=incremental_load_func)
    visibility_thread = threading.Thread(target=visibility_query_func)

    # Start both threads
    data_incremental_load_thread.start()
    visibility_thread.start()

    # Wait for both threads to finish
    data_incremental_load_thread.join()
    visibility_thread.join()

