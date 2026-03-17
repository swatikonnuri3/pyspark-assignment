import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import get_partition_count


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q2_2").master("local").getOrCreate()


def test_partition_count_positive(spark):
    df = sub1.create_credit_card_df(spark)
    assert get_partition_count(df) >= 1

def test_partition_count_is_int(spark):
    df = sub1.create_credit_card_df(spark)
    assert isinstance(get_partition_count(df), int)