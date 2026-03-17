import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import replace_state_with_country


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_7").master("local").getOrCreate()

@pytest.fixture(scope="session")
def result(spark):
    return replace_state_with_country(
        sub1.create_employee_df(spark),
        sub1.create_country_df(spark)
    )


def test_row_count_unchanged(spark, result):
    assert result.count() == 7

def test_state_has_country_names(spark, result):
    states = [r.state for r in result.collect()]
    assert "newyork"    in states
    assert "california" in states
    assert "russia"     in states

def test_no_state_codes(spark, result):
    states = [r.state for r in result.collect()]
    assert "ny" not in states
    assert "ca" not in states
    assert "uk" not in states

def test_james_is_newyork(spark, result):
    from pyspark.sql.functions import col
    row = result.filter(col("employee_name") == "james").collect()[0]
    assert row["state"] == "newyork"