import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
import util as sub1
from util import lowercase_columns_and_load_date


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_8").master("local").getOrCreate()

@pytest.fixture(scope="session")
def result(spark):
    df = sub1.create_employee_df(spark)
    return lowercase_columns_and_load_date(df)


def test_all_columns_lowercase(spark, result):
    for col_name in result.columns:
        assert col_name == col_name.lower()

def test_load_date_exists(spark, result):
    assert "load_date" in result.columns

def test_load_date_is_date_type(spark, result):
    assert isinstance(result.schema["load_date"].dataType, DateType)

def test_row_count_unchanged(spark, result):
    assert result.count() == 7

def test_load_date_not_null(spark, result):
    from pyspark.sql.functions import col
    assert result.filter(col("load_date").isNull()).count() == 0