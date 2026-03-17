import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import customers_upgraded_to_iphone14


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q1_3").master("local").getOrCreate()

@pytest.fixture(scope="session")
def purchase_df(spark):
    return sub1.create_purchase_data_df(spark)


def test_result_count(spark, purchase_df):
    assert customers_upgraded_to_iphone14(purchase_df).count() == 2

def test_customer_1_present(spark, purchase_df):
    result = [r.customer for r in customers_upgraded_to_iphone14(purchase_df).collect()]
    assert 1 in result

def test_customer_3_present(spark, purchase_df):
    result = [r.customer for r in customers_upgraded_to_iphone14(purchase_df).collect()]
    assert 3 in result

def test_customer_2_absent(spark, purchase_df):
    result = [r.customer for r in customers_upgraded_to_iphone14(purchase_df).collect()]
    assert 2 not in result

def test_customer_4_absent(spark, purchase_df):
    result = [r.customer for r in customers_upgraded_to_iphone14(purchase_df).collect()]
    assert 4 not in result

def test_output_column(spark, purchase_df):
    assert "customer" in customers_upgraded_to_iphone14(purchase_df).columns