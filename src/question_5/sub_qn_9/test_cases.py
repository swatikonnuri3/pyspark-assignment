import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))


def load(path, name):
    import importlib.util as ilu
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("Test_Q5_9") \
        .master("local") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .getOrCreate()

@pytest.fixture(scope="session")
def final_df(spark):
    base = os.path.join(os.path.dirname(__file__), '..')
    s7   = load(os.path.join(base, 'sub_qn_7', 'util.py'), 's7')
    s8   = load(os.path.join(base, 'sub_qn_8', 'util.py'), 's8')
    import util as sub1
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