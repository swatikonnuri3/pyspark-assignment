import pytest
import sys
import os
import importlib.util as importlib_util

sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

# Load sub_qn_3's util explicitly
_sub3_path = os.path.join(os.path.dirname(__file__), 'util.py')
_spec3 = importlib_util.spec_from_file_location("sub_qn_3_util", _sub3_path)
_sub_qn_3_util = importlib_util.module_from_spec(_spec3)
_spec3.loader.exec_module(_sub_qn_3_util)

create_log_df = _sub_qn_3_util.create_log_df
rename_columns = _sub_qn_3_util.rename_columns
actions_last_7_days = _sub_qn_3_util.actions_last_7_days

from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q3_3").master("local").getOrCreate()

@pytest.fixture(scope="session")
def renamed_df(spark):
    return rename_columns(create_log_df(spark))

@pytest.fixture(scope="session")
def result_df(spark, renamed_df):
    return actions_last_7_days(renamed_df)

def test_result_has_columns(spark, result_df):
    assert "user_id"      in result_df.columns
    assert "action_count" in result_df.columns

def test_result_not_empty(spark, result_df):
    assert result_df.count() > 0

def test_action_counts_positive(spark, result_df):
    from pyspark.sql.functions import col
    assert result_df.filter(col("action_count") <= 0).count() == 0