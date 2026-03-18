import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

def write_csv_options(df, base_path="output/q3"):
    """
    Write DataFrame as CSV with different write options.
    """
    df.write.mode("overwrite") \
        .option("header", True) \
        .csv(f"{base_path}/csv_overwrite")
    df.write.mode("append") \
        .option("header", True) \
        .csv(f"{base_path}/csv_append")
    df.write.mode("overwrite") \
        .option("header", True) \
        .option("delimiter", "|") \
        .csv(f"{base_path}/csv_pipe")
    df.write.mode("overwrite") \
        .option("header",     True) \
        .option("nullValue",  "N/A") \
        .option("dateFormat", "yyyy-MM-dd") \
        .csv(f"{base_path}/csv_nulldate")
    print(f"All CSV files written to {base_path}")