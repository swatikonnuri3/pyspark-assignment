import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import avg_salary_by_dept


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_2").master("local").getOrCreate()

@pytest.fixture(scope="session")
def result(spark):
    return avg_salary_by_dept(
        sub1.create_employee_df(spark),
        sub1.create_department_df(spark)
    )


def test_result_has_3_depts(spark, result):
    assert result.count() == 3

def test_columns_exist(spark, result):
    assert "dept_name"  in result.columns
    assert "avg_salary" in result.columns

def test_d101_avg_salary(spark, result):
    from pyspark.sql.functions import col
    row = result.filter(col("dept_id") == "D101").collect()[0]
    assert row["avg_salary"] == 8600.0

def test_d102_avg_salary(spark, result):
    from pyspark.sql.functions import col
    row = result.filter(col("dept_id") == "D102").collect()[0]
    assert row["avg_salary"] == 8700.0

def test_d103_avg_salary(spark, result):
    from pyspark.sql.functions import col
    row = result.filter(col("dept_id") == "D103").collect()[0]
    assert row["avg_salary"] == 8550.0