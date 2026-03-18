import pytest
import sys
import os
import json

sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

import importlib.util as ilu
def load(path, name):
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

base = os.path.join(os.path.dirname(__file__), '..')
s1 = load(os.path.join(base, 'sub_qn_1', 'util.py'), 's1')
s2 = load(os.path.join(base, 'sub_qn_2', 'util.py'), 's2')

sys.modules.pop('util', None)
sys.path = [p for p in sys.path if 'question_' not in p and 'sub_qn_' not in p]
sys.path.insert(0, os.path.dirname(__file__))
from util import filter_by_id

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q4_5").master("local").getOrCreate()

@pytest.fixture(scope="session")
def sample_json(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("data")
    data = [
        {"id": "0001", "name": "James", "projects": [{"pid": "P1"}]},
        {"id": "0002", "name": "Maria", "projects": [{"pid": "P2"}]}
    ]
    json_file = str(tmp_path / "emp.json")
    with open(json_file, "w") as f:
        json.dump(data, f)
    return json_file


def test_filter_returns_only_0001(spark, sample_json):
    df = s2.flatten_df(s1.read_json_dynamic(spark, sample_json))
    result = filter_by_id(df, "0001")
    assert result.count() >= 1
    for row in result.collect():
        assert row["id"] == "0001"

def test_filter_excludes_0002(spark, sample_json):
    from pyspark.sql.functions import col
    df = s2.flatten_df(s1.read_json_dynamic(spark, sample_json))
    result = filter_by_id(df, "0001")
    assert result.filter(col("id") == "0002").count() == 0