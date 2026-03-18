import pytest
import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

sys.modules.pop('util', None)
sys.path.insert(0, os.path.dirname(__file__))
from util import add_load_date

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q4_7").master("local").getOrCreate()


def test_load_date_column_added(spark):
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("id", StringType())])
    df = spark.createDataFrame([("0001",)], schema)
    assert "load_date" in add_load_date(df).columns

def test_load_date_is_date_type(spark):
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("id", StringType())])
    df = spark.createDataFrame([("0001",)], schema)
    assert isinstance(add_load_date(df).schema["load_date"].dataType, DateType)

def test_load_date_not_null(spark):
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import col
    schema = StructType([StructField("id", StringType())])
    df = spark.createDataFrame([("0001",)], schema)
    assert add_load_date(df).filter(col("load_date").isNull()).count() == 0