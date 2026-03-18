import pytest
import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

sys.modules.pop('util', None)
sys.path.insert(0, os.path.dirname(__file__))
from util import get_final_output

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q2_6").master("local").getOrCreate()


def test_output_columns(spark):
    df = get_final_output(spark)
    assert df.columns == ["card_number", "masked_card_number"]

def test_output_row_count(spark):
    assert get_final_output(spark).count() == 5

def test_only_two_columns(spark):
    assert len(get_final_output(spark).columns) == 2

def test_no_nulls_in_masked(spark):
    from pyspark.sql.functions import col
    df = get_final_output(spark)
    assert df.filter(col("masked_card_number").isNull()).count() == 0