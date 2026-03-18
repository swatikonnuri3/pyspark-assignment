import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def get_spark():
    return SparkSession.builder.appName("Q1_SubQn1").master("local").getOrCreate()


def create_purchase_data_df(spark):
    schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])
    data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
    ]
    return spark.createDataFrame(data, schema)


def create_product_data_df(spark):
    schema = StructType([
        StructField("product_model", StringType(), True)
    ])
    data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]
    return spark.createDataFrame(data, schema)