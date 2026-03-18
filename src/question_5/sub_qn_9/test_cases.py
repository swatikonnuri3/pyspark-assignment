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
s8   = load(os.path.join(base, 'sub_qn_8', 'util.py'), 's8')

sys.modules.pop('util', None)
sys.path.insert(0, os.path.dirname(__file__))
from util import write_external_tables

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("Test_Q5_9") \
        .master("local") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .getOrCreate()

@pytest.fixture(scope="session")
def final_df(spark):
    employee_df = sub1.create_employee_df(spark)
    country_df  = sub1.create_country_df(spark)
    return s8.lowercase_columns_and_load_date(
               s7.replace_state_with_country(employee_df, country_df)
           )


def test_df_has_load_date(spark, final_df):
    assert "load_date" in final_df.columns

def test_df_row_count(spark, final_df):
    assert final_df.count() == 7

def test_csv_write(spark, final_df, tmp_path):
    final_df.write.mode("overwrite") \
            .option("header", True) \
            .csv(str(tmp_path / "emp_csv"))
    assert os.path.exists(str(tmp_path / "emp_csv"))

def test_parquet_write(spark, final_df, tmp_path):
    final_df.write.mode("overwrite") \
            .parquet(str(tmp_path / "emp_parquet"))
    assert os.path.exists(str(tmp_path / "emp_parquet"))

def test_all_columns_lowercase(spark, final_df):
    for col_name in final_df.columns:
        assert col_name == col_name.lower()