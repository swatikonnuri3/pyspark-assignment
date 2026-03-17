import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import perform_joins


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_6").master("local").getOrCreate()

@pytest.fixture(scope="session")
def results(spark):
    return perform_joins(
        sub1.create_employee_df(spark),
        sub1.create_department_df(spark)
    )


def test_all_join_types_returned(spark, results):
    assert "inner" in results
    assert "left"  in results
    assert "right" in results

def test_inner_join_count(spark, results):
    # All 7 employees have matching dept
    assert results["inner"].count() == 7

def test_left_join_count(spark, results):
    # All 7 employees kept even if no dept match
    assert results["left"].count() == 7

def test_right_join_count(spark, results):
    # 5 depts, only D101/D102/D103 have employees → D104/D105 get null rows
    assert results["right"].count() == 9

def test_inner_join_columns(spark, results):
    cols = results["inner"].columns
    assert "employee_name" in cols
    assert "dept_name"     in cols