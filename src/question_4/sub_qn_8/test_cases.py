import pytest
import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

sys.modules.pop('util', None)
sys.path.insert(0, os.path.dirname(__file__))
from util import add_year_month_day

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q4_8").master("local").getOrCreate()

@pytest.fixture(scope="session")
def df_with_date(spark):
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import current_date
    schema = StructType([StructField("id", StringType())])
    df = spark.createDataFrame([("0001",)], schema)
    return df.withColumn("load_date", current_date())


def test_year_column_exists(spark, df_with_date):
    assert "year" in add_year_month_day(df_with_date).columns

def test_month_column_exists(spark, df_with_date):
    assert "month" in add_year_month_day(df_with_date).columns

def test_day_column_exists(spark, df_with_date):
    assert "day" in add_year_month_day(df_with_date).columns

def test_month_in_valid_range(spark, df_with_date):
    row = add_year_month_day(df_with_date).collect()[0]
    assert 1 <= row["month"] <= 12

def test_day_in_valid_range(spark, df_with_date):
    row = add_year_month_day(df_with_date).collect()[0]
    assert 1 <= row["day"] <= 31