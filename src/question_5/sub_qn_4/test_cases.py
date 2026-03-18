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
from util import add_bonus

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_4").master("local").getOrCreate()

@pytest.fixture(scope="session")
def result(spark):
    return add_bonus(sub1.create_employee_df(spark))


def test_bonus_column_exists(spark, result):
    assert "bonus" in result.columns

def test_bonus_is_double_salary(spark, result):
    for row in result.collect():
        assert row["bonus"] == row["salary"] * 2

def test_row_count_unchanged(spark, result):
    assert result.count() == 7

def test_james_bonus(spark, result):
    from pyspark.sql.functions import col
    row = result.filter(col("employee_name") == "james").collect()[0]
    assert row["bonus"] == 18000