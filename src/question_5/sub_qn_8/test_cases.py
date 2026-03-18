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
s7   = load(os.path.join(base, 'sub_qn_7', 'util.py'), 's7')

sys.modules.pop('util', None)
sys.path = [p for p in sys.path if 'question_' not in p and 'sub_qn_' not in p]
sys.path.insert(0, os.path.dirname(__file__))
from util import lowercase_columns_and_load_date

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_8").master("local").getOrCreate()

@pytest.fixture(scope="session")
def result(spark):
    employee_df = sub1.create_employee_df(spark)
    country_df  = sub1.create_country_df(spark)
    df = s7.replace_state_with_country(employee_df, country_df)
    return lowercase_columns_and_load_date(df)


def test_all_columns_lowercase(spark, result):
    for col_name in result.columns:
        assert col_name == col_name.lower()

def test_load_date_exists(spark, result):
    assert "load_date" in result.columns

def test_load_date_is_date_type(spark, result):
    assert isinstance(result.schema["load_date"].dataType, DateType)

def test_row_count_unchanged(spark, result):
    assert result.count() == 7

def test_load_date_not_null(spark, result):
    from pyspark.sql.functions import col
    assert result.filter(col("load_date").isNull()).count() == 0