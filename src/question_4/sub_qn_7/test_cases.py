import pytest
import sys
import os
import json
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from util import add_load_date


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q4_7").master("local").getOrCreate()


def test_load_date_column_added(spark):
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("id", StringType())])
    df     = spark.createDataFrame([("0001",)], schema)
    result = add_load_date(df)
    assert "load_date" in result.columns

def test_load_date_is_date_type(spark):
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("id", StringType())])
    df     = spark.createDataFrame([("0001",)], schema)
    result = add_load_date(df)
    assert isinstance(result.schema["load_date"].dataType, DateType)

def test_load_date_not_null(spark):
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import col
    schema = StructType([StructField("id", StringType())])
    df     = spark.createDataFrame([("0001",)], schema)
    result = add_load_date(df)
    assert result.filter(col("load_date").isNull()).count() == 0