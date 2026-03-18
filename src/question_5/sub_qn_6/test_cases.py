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
from util import perform_joins

from pyspark.sql import SparkSession


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
    assert results["inner"].count() == 7

def test_left_join_count(spark, results):
    assert results["left"].count() == 7

def test_right_join_count(spark, results):
    assert results["right"].count() == 9

def test_inner_join_columns(spark, results):
    cols = results["inner"].columns
    assert "employee_name" in cols
    assert "dept_name"     in cols