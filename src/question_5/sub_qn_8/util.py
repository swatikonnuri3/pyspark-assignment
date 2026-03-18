import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql.functions import current_date


def lowercase_columns_and_load_date(df):
    """
    Dynamically convert all column names to lowercase
    and add load_date column with current date.
    """
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())

    df = df.withColumn("load_date", current_date())

    return df