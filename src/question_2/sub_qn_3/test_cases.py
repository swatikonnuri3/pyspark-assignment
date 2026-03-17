import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import increase_partitions


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q2_3").master("local").getOrCreate()


def test_repartition_to_5(spark):
    df = sub1.create_credit_card_df(spark)
    assert increase_partitions(df, 5).rdd.getNumPartitions() == 5

def test_repartition_preserves_row_count(spark):
    df = sub1.create_credit_card_df(spark)
    assert increase_partitions(df, 5).count() == 5