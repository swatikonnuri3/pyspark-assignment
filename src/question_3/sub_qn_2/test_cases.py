import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import rename_columns


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