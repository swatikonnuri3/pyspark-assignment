import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql.functions import col

def filter_by_id(df, id_value="0001"):
    """Filter records where id equals the given value."""
    return df.filter(col("id") == id_value)