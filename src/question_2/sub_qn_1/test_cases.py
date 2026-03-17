import pytest
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
from util import (create_credit_card_df_inline, create_credit_card_df_csv,
                  create_credit_card_df_json, create_credit_card_df_parquet)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q2_1").master("local").getOrCreate()


def test_inline_row_count(spark):
    assert create_credit_card_df_inline(spark).count() == 5

def test_inline_columns(spark):
    assert create_credit_card_df_inline(spark).columns == ["card_number"]

def test_inline_schema(spark):
    df = create_credit_card_df_inline(spark)
    assert str(df.schema["card_number"].dataType) == "StringType()"

def test_csv_row_count(spark, tmp_path):
    assert create_credit_card_df_csv(spark, str(tmp_path / "cc.csv")).count() == 5

def test_json_row_count(spark, tmp_path):
    assert create_credit_card_df_json(spark, str(tmp_path / "cc.json")).count() == 5

def test_parquet_row_count(spark, tmp_path):
    assert create_credit_card_df_parquet(spark, str(tmp_path / "cc.parquet")).count() == 5

def test_card_number_length(spark):
    from pyspark.sql.functions import length, col
    df = create_credit_card_df_inline(spark)
    assert df.filter(length(col("card_number")) != 16).count() == 0