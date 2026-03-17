import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import emp_dept_name_starts_with_m


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_3").master("local").getOrCreate()

@pytest.fixture(scope="session")
def result(spark):
    return emp_dept_name_starts_with_m(
        sub1.create_employee_df(spark),
        sub1.create_department_df(spark)
    )


def test_result_count(spark, result):
    assert result.count() == 2

def test_columns(spark, result):
    assert result.columns == ["employee_name", "dept_name"]

def test_michel_present(spark, result):
    names = [r.employee_name for r in result.collect()]
    assert "michel" in names

def test_maria_present(spark, result):
    names = [r.employee_name for r in result.collect()]
    assert "maria" in names

def test_no_non_m_names(spark, result):
    for row in result.collect():
        assert row.employee_name.startswith("m")