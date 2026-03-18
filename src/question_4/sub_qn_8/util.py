import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql.functions import col, year, month, dayofmonth

def add_year_month_day(df):
    """Add year, month, day columns from load_date column."""
    return df \
        .withColumn("year",  year(col("load_date"))) \
        .withColumn("month", month(col("load_date"))) \
        .withColumn("day",   dayofmonth(col("load_date")))