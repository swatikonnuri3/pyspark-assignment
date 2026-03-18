import pytest
import sys
import os
import json

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
s6 = load(os.path.join(base, 'sub_qn_6', 'util.py'), 's6')
s7 = load(os.path.join(base, 'sub_qn_7', 'util.py'), 's7')
s8 = load(os.path.join(base, 'sub_qn_8', 'util.py'), 's8')

sys.modules.pop('util', None)
sys.path = [p for p in sys.path if 'question_' not in p and 'sub_qn_' not in p]
sys.path.insert(0, os.path.dirname(__file__))
from util import write_partitioned_table

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("Test_Q4_9") \
        .master("local") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .getOrCreate()

@pytest.fixture(scope="session")
def final_df(spark, tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("data")
    data = [{"id": "0001", "employeeName": "James",
             "projects": [{"projectId": "P1"}]}]
    json_file = str(tmp_path / "emp.json")
    with open(json_file, "w") as f:
        json.dump(data, f)
    return s8.add_year_month_day(
               s7.add_load_date(
                   s6.rename_camel_to_snake(
                       s2.flatten_df(
                           s1.read_json_dynamic(spark, json_file)
                       )
                   )
               )
           )


def test_has_partition_columns(spark, final_df):
    assert "year"  in final_df.columns
    assert "month" in final_df.columns
    assert "day"   in final_df.columns

def test_table_write(spark, final_df):
    spark.sql("CREATE DATABASE IF NOT EXISTS employee")
    write_partitioned_table(final_df, spark)
    result = spark.sql("SELECT * FROM employee.employee_details")
    assert result.count() >= 1