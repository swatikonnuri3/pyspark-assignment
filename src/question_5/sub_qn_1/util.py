import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def get_spark():
    return SparkSession.builder.appName("Q5").master("local").getOrCreate()


def create_employee_df(spark):
    schema = StructType([
        StructField("employee_id",   IntegerType(), True),
        StructField("employee_name", StringType(),  True),
        StructField("department",    StringType(),  True),
        StructField("state",         StringType(),  True),
        StructField("salary",        IntegerType(), True),
        StructField("age",           IntegerType(), True),
    ])
    data = [
        (11, "james",  "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott",  "D103", "ca", 8000, 36),
        (15, "jen",    "D102", "ny", 9500, 38),
        (16, "jeff",   "D103", "uk", 9100, 35),
        (17, "maria",  "D101", "ny", 7900, 40),
    ]
    return spark.createDataFrame(data, schema)


def create_department_df(spark):
    schema = StructType([
        StructField("dept_id",   StringType(), True),
        StructField("dept_name", StringType(), True),
    ])
    data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support"),
    ]
    return spark.createDataFrame(data, schema)


def create_country_df(spark):
    schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True),
    ])
    data = [
        ("ny", "newyork"),
        ("ca", "california"),
        ("uk", "russia"),
    ]
    return spark.createDataFrame(data, schema)