import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

def write_partitioned_table(df, spark, database="employee", table="employee_details"):
    """
    Write DataFrame as JSON partitioned table with year, month, day.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    row = df.select("year", "month", "day").first()
    y, m, d = row["year"], row["month"], row["day"]

    df.write \
        .format("json") \
        .mode("overwrite") \
        .option("replaceWhere", f"year = {y} AND month = {m} AND day = {d}") \
        .partitionBy("year", "month", "day") \
        .saveAsTable(f"{database}.{table}")

    print(f"Table {database}.{table} written with partitions year={y}, month={m}, day={d}")