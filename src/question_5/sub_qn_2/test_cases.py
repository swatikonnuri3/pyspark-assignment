import pytest
import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

import importlib.util as ilu
def load(path, name):
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

base = os.path.join(os.path.dirname(__file__), '..')
sub1 = load(os.path.join(base, 'sub_qn_1', 'util.py'), 'sub1')

sys.modules.pop('util', None)
sys.path = [p for p in sys.path if 'question_' not in p and 'sub_qn_' not in p]
sys.path.insert(0, os.path.dirname(__file__))
from util import avg_salary_by_dept

from pyspark.sql import SparkSession


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