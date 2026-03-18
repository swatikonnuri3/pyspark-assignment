import pytest
import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

sys.modules.pop('util', None)
sys.path = [p for p in sys.path if 'question_' not in p and 'sub_qn_' not in p]
sys.path.insert(0, os.path.dirname(__file__))
from util import create_employee_df, create_department_df, create_country_df

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q5_1").master("local").getOrCreate()


def test_employee_row_count(spark):
    assert create_employee_df(spark).count() == 7

def test_employee_columns(spark):
    df = create_employee_df(spark)
    assert df.columns == ["employee_id", "employee_name", "department",
                          "state", "salary", "age"]

def test_employee_id_dtype(spark):
    df = create_employee_df(spark)
    assert str(df.schema["employee_id"].dataType) == "IntegerType()"

def test_employee_name_dtype(spark):
    df = create_employee_df(spark)
    assert str(df.schema["employee_name"].dataType) == "StringType()"

def test_department_row_count(spark):
    assert create_department_df(spark).count() == 5

def test_department_columns(spark):
    df = create_department_df(spark)
    assert df.columns == ["dept_id", "dept_name"]

def test_country_row_count(spark):
    assert create_country_df(spark).count() == 3

def test_country_columns(spark):
    df = create_country_df(spark)
    assert df.columns == ["country_code", "country_name"]