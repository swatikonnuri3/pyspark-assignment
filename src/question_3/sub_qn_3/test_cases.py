import pytest
import sys
import os


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("Test_Q3_3").master("local").getOrCreate()

@pytest.fixture(scope="session")
def renamed_df(spark):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_2'))
    import util as s1
    import util as s2
    return s2.rename_columns(s1.create_log_df(spark))

@pytest.fixture(scope="session")
def result_df(spark, renamed_df):
    sys.path.insert(0, os.path.dirname(__file__))
    from util import actions_last_7_days
    return actions_last_7_days(renamed_df)


def test_result_has_columns(spark, result_df):
    assert "user_id"      in result_df.columns
    assert "action_count" in result_df.columns

def test_result_not_empty(spark, result_df):
    assert result_df.count() > 0

def test_action_counts_positive(spark, result_df):
    from pyspark.sql.functions import col
    assert result_df.filter(col("action_count") <= 0).count() == 0