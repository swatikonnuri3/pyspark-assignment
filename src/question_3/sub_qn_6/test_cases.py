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
s1 = load(os.path.join(base, 'sub_qn_1', 'util.py'), 's1')
s2 = load(os.path.join(base, 'sub_qn_2', 'util.py'), 's2')
s4 = load(os.path.join(base, 'sub_qn_4', 'util.py'), 's4')

sys.modules.pop('util', None)
sys.path = [p for p in sys.path if 'question_' not in p and 'sub_qn_' not in p]
sys.path.insert(0, os.path.dirname(__file__))
from util import write_managed_table

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("Test_Q3_6") \
        .master("local") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .getOrCreate()

@pytest.fixture(scope="session")
def final_df(spark):
    return s4.convert_timestamp_to_date(
               s2.rename_columns(s1.create_log_df(spark))
           )


def test_table_created(spark, final_df):
    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    write_managed_table(final_df, spark)
    tables = [r.tableName for r in spark.sql("SHOW TABLES IN user").collect()]
    assert "login_details" in tables

def test_table_row_count(spark, final_df):
    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    write_managed_table(final_df, spark)
    assert spark.sql("SELECT * FROM user.login_details").count() == 8

def test_table_has_login_date(spark, final_df):
    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    write_managed_table(final_df, spark)
    df = spark.sql("SELECT * FROM user.login_details")
    assert "login_date" in df.columns