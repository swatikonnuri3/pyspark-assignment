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

sys.modules.pop('util', None)
sys.path.insert(0, os.path.dirname(__file__))
from util import apply_mask

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q2_5").master("local").getOrCreate()

@pytest.fixture(scope="session")
def masked_df(spark):
    return apply_mask(sub1.create_credit_card_df(spark))


def test_masked_column_exists(spark, masked_df):
    assert "masked_card_number" in masked_df.columns

def test_last_4_digits_correct(spark, masked_df):
    for row in masked_df.collect():
        assert row["card_number"][-4:] == row["masked_card_number"][-4:]

def test_first_12_are_stars(spark, masked_df):
    for row in masked_df.collect():
        assert row["masked_card_number"][:12] == "*" * 12

def test_masked_length_equals_original(spark, masked_df):
    for row in masked_df.collect():
        assert len(row["masked_card_number"]) == len(row["card_number"])

def test_specific_mask(spark):
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("card_number", StringType())])
    df = spark.createDataFrame([("1234567891234567",)], schema)
    result = apply_mask(df).collect()[0]["masked_card_number"]
    assert result == "************4567"