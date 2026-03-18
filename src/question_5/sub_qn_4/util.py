import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql.functions import col


def add_bonus(employee_df):
    """Add bonus column = salary * 2."""
    return employee_df.withColumn("bonus", col("salary") * 2)