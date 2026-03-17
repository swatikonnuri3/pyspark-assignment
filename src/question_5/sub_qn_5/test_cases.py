import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import reorder_columns


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_5").master("local").getOrCreate()

@pytest.fixture(scope="session")
def result(spark):
    return reorder_columns(sub1.create_employee_df(spark))


def test_column_order(spark, result):
    assert result.columns == ["employee_id", "employee_name",
                               "salary", "state", "age", "department"]

def test_row_count_unchanged(spark, result):
    assert result.count() == 7

def test_all_columns_present(spark, result):
    for col in ["employee_id", "employee_name", "salary",
                "state", "age", "department"]:
        assert col in result.columns