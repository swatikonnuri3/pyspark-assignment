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
    return SparkSession.builder.appName("Test_Q3_5").master("local").getOrCreate()

@pytest.fixture(scope="session")
def final_df(spark):
    base = os.path.join(os.path.dirname(__file__), '..')
    s1   = load(os.path.join(base, 'sub_qn_1', 'util.py'), 's1')
    s2   = load(os.path.join(base, 'sub_qn_2', 'util.py'), 's2')
    s4   = load(os.path.join(base, 'sub_qn_4', 'util.py'), 's4')
    return s4.convert_timestamp_to_date(s2.rename_columns(s1.create_log_df(spark)))


def test_csv_written(spark, final_df, tmp_path):
    sys.path.insert(0, os.path.dirname(__file__))
    from util import write_csv_options
    write_csv_options(final_df, str(tmp_path))
    assert os.path.exists(str(tmp_path))

def test_df_has_login_date(spark, final_df):
    assert "login_date" in final_df.columns

def test_df_row_count(spark, final_df):
    assert final_df.count() == 8