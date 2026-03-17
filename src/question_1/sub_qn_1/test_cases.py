import pytest
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
from util import create_purchase_data_df, create_product_data_df


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q1_1").master("local").getOrCreate()


def test_purchase_row_count(spark):
    assert create_purchase_data_df(spark).count() == 11

def test_purchase_columns(spark):
    df = create_purchase_data_df(spark)
    assert df.columns == ["customer", "product_model"]

def test_purchase_customer_dtype(spark):
    df = create_purchase_data_df(spark)
    assert str(df.schema["customer"].dataType) == "IntegerType()"

def test_purchase_product_dtype(spark):
    df = create_purchase_data_df(spark)
    assert str(df.schema["product_model"].dataType) == "StringType()"

def test_product_row_count(spark):
    assert create_product_data_df(spark).count() == 5

def test_product_columns(spark):
    df = create_product_data_df(spark)
    assert df.columns == ["product_model"]

def test_product_dtype(spark):
    df = create_product_data_df(spark)
    assert str(df.schema["product_model"].dataType) == "StringType()"

def test_purchase_iphone13_count(spark):
    df = create_purchase_data_df(spark)
    assert df.filter(df.product_model == "iphone13").count() == 4

def test_product_all_models_present(spark):
    df = create_product_data_df(spark)
    models = [r.product_model for r in df.collect()]
    for m in ["iphone13", "iphone14", "dell i5 core", "dell i3 core", "hp i5 core"]:
        assert m in models