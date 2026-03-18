import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config
import shutil

def write_managed_table(df, spark):
    """
    Write DataFrame as a managed table.
    Database: user
    Table:    login_details
    Mode:     overwrite
    """
    # Remove existing location if it exists to avoid LOCATION_ALREADY_EXISTS error
    warehouse = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    table_path = os.path.join(warehouse, "user.db", "login_details")
    if os.path.exists(table_path):
        shutil.rmtree(table_path)

    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    df.write.mode("overwrite") \
        .saveAsTable("user.login_details")
    print("Table user.login_details created successfully.")