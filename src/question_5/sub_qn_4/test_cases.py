import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import add_bonus


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_4").master("local").getOrCreate()

@pytest.fixture(scope="session")
def result(spark):
    return add_bonus(sub1.create_employee_df(spark))


def test_bonus_column_exists(spark, result):
    assert "bonus" in result.columns

def test_bonus_is_double_salary(spark, result):
    for row in result.collect():
        assert row["bonus"] == row["salary"] * 2

def test_row_count_unchanged(spark, result):
    assert result.count() == 7

def test_james_bonus(spark, result):
    from pyspark.sql.functions import col
    row = result.filter(col("employee_name") == "james").collect()[0]
    assert row["bonus"] == 18000