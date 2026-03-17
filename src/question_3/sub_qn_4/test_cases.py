import pytest
import sys
import os


def load(path, name):
    import importlib.util as ilu
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("Test_Q3_4").master("local").getOrCreate()

@pytest.fixture(scope="session")
def result_df(spark):
    base = os.path.join(os.path.dirname(__file__), '..')
    s1   = load(os.path.join(base, 'sub_qn_1', 'util.py'), 's1')
    s2   = load(os.path.join(base, 'sub_qn_2', 'util.py'), 's2')
    sys.path.insert(0, os.path.dirname(__file__))
    from util import convert_timestamp_to_date
    df = s2.rename_columns(s1.create_log_df(spark))
    return convert_timestamp_to_date(df)


def test_login_date_column_exists(spark, result_df):
    assert "login_date" in result_df.columns

def test_timestamp_column_removed(spark, result_df):
    assert "time_stamp" not in result_df.columns

def test_login_date_is_date_type(spark, result_df):
    from pyspark.sql.types import DateType
    assert isinstance(result_df.schema["login_date"].dataType, DateType)

def test_row_count_unchanged(spark, result_df):
    assert result_df.count() == 8