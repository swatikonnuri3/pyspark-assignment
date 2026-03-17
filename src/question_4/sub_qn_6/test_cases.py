import pytest
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from util import camel_to_snake, rename_camel_to_snake


def test_camel_to_snake_basic():
    assert camel_to_snake("employeeId")   == "employee_id"
    assert camel_to_snake("firstName")    == "first_name"
    assert camel_to_snake("projectName")  == "project_name"

def test_camel_to_snake_already_snake():
    assert camel_to_snake("employee_id") == "employee_id"

def test_camel_to_snake_single_word():
    assert camel_to_snake("name") == "name"

def test_rename_columns(spark_session):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Test_Q4_6").master("local").getOrCreate()
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("employeeId", StringType()),
        StructField("firstName",  StringType()),
    ])
    df     = spark.createDataFrame([("1", "James")], schema)
    result = rename_camel_to_snake(df)
    assert "employee_id" in result.columns
    assert "first_name"  in result.columns