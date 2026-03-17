import pytest
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
from util import create_log_df


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q3_1").master("local").getOrCreate()


def test_row_count(spark):
    assert create_log_df(spark).count() == 8

def test_columns_exist(spark):
    df = create_log_df(spark)
    assert "log id"    in df.columns
    assert "user$id"   in df.columns
    assert "action"    in df.columns
    assert "timestamp" in df.columns

def test_log_id_dtype(spark):
    df = create_log_df(spark)
    assert str(df.schema["log id"].dataType) == "IntegerType()"

def test_user_id_dtype(spark):
    df = create_log_df(spark)
    assert str(df.schema["user$id"].dataType) == "IntegerType()"

def test_action_dtype(spark):
    df = create_log_df(spark)
    assert str(df.schema["action"].dataType) == "StringType()"

def test_distinct_actions(spark):
    df = create_log_df(spark)
    actions = [r.action for r in df.select("action").distinct().collect()]
    assert "login"  in actions
    assert "click"  in actions
    assert "logout" in actions