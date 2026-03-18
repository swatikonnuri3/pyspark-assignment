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
from util import reorder_columns

from pyspark.sql import SparkSession


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
    for c in ["employee_id", "employee_name", "salary",
              "state", "age", "department"]:
        assert c in result.columns