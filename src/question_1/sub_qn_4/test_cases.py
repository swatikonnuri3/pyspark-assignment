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

# Remove cached util so THIS folder's util.py loads correctly
sys.modules.pop('util', None)
sys.path.insert(0, os.path.dirname(__file__))
from util import new_products

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q1_4").master("local").getOrCreate()

@pytest.fixture(scope="session")
def purchase_df(spark):
    return sub1.create_purchase_data_df(spark)

@pytest.fixture(scope="session")
def product_df(spark):
    return sub1.create_product_data_df(spark)


def test_result_count(spark, purchase_df, product_df):
    assert new_products(product_df, purchase_df).count() == 1

def test_customer_1_present(spark, purchase_df, product_df):
    result = [r.customer for r in new_products(product_df, purchase_df).collect()]
    assert 1 in result

def test_customer_2_absent(spark, purchase_df, product_df):
    result = [r.customer for r in new_products(product_df, purchase_df).collect()]
    assert 2 not in result

def test_customer_3_absent(spark, purchase_df, product_df):
    result = [r.customer for r in new_products(product_df, purchase_df).collect()]
    assert 3 not in result

def test_customer_4_absent(spark, purchase_df, product_df):
    result = [r.customer for r in new_products(product_df, purchase_df).collect()]
    assert 4 not in result

def test_output_column(spark, purchase_df, product_df):
    assert "customer" in new_products(product_df, purchase_df).columns