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
from util import decrease_partitions

from pyspark.sql import SparkSession


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