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
sys.path.insert(0, os.path.dirname(__file__))
from util import rename_columns

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q3_2").master("local").getOrCreate()

@pytest.fixture(scope="session")
def renamed_df(spark):
    return rename_columns(sub1.create_log_df(spark))


def test_new_column_names(spark, renamed_df):
    assert renamed_df.columns == ["log_id", "user_id", "user_activity", "time_stamp"]

def test_row_count_unchanged(spark, renamed_df):
    assert renamed_df.count() == 8

def test_old_columns_gone(spark, renamed_df):
    assert "log id"    not in renamed_df.columns
    assert "user$id"   not in renamed_df.columns
    assert "action"    not in renamed_df.columns
    assert "timestamp" not in renamed_df.columns