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
from util import emp_dept_name_starts_with_m

from pyspark.sql import SparkSession


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