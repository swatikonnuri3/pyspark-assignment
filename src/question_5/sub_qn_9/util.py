import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config


def write_external_tables(df, spark,
                          database="employee_ext",
                          base_path="spark-warehouse/employee_ext"):
    """
    Create 2 external tables:
    - Table 1: CSV format    → employee_csv
    - Table 2: Parquet format → employee_parquet
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    csv_path     = f"{base_path}/employee_csv"
    parquet_path = f"{base_path}/employee_parquet"

    # External table 1 - CSV
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.employee_csv (
            employee_id   INT,
            employee_name STRING,
            department    STRING,
            state         STRING,
            salary        INT,
            age           INT,
            load_date     DATE
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{os.path.abspath(csv_path)}'
    """)

    df.write \
      .mode("overwrite") \
      .option("header", True) \
      .csv(csv_path)

    print(f"External CSV table {database}.employee_csv created.")

    # External table 2 - Parquet
    df.write \
      .mode("overwrite") \
      .parquet(parquet_path)

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.employee_parquet (
            employee_id   INT,
            employee_name STRING,
            department    STRING,
            state         STRING,
            salary        INT,
            age           INT,
            load_date     DATE
        )
        STORED AS PARQUET
        LOCATION '{os.path.abspath(parquet_path)}'
    """)

    print(f"External Parquet table {database}.employee_parquet created.")