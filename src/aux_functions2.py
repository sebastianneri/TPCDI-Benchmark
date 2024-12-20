import findspark
findspark.init()
from pyspark.sql.functions import col
import string
import random
from pyspark.sql import SparkSession



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

def cast_to_target_schema(spark, source_table: str, target_table: str):
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



def get_max(num1, num2):
    if num1 > num2:
        return num1
    return num2

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))