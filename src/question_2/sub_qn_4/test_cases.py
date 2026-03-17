import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import decrease_partitions


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q2_4").master("local").getOrCreate()


def test_coalesce_back_to_original(spark):
    df = sub1.create_credit_card_df(spark)
    original = df.rdd.getNumPartitions()
    df_back = decrease_partitions(df.repartition(5), original)
    assert df_back.rdd.getNumPartitions() == original

def test_coalesce_preserves_row_count(spark):
    df = sub1.create_credit_card_df(spark)
    original = df.rdd.getNumPartitions()
    df_back = decrease_partitions(df.repartition(5), original)
    assert df_back.count() == 5