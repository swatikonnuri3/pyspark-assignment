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

sys.modules.pop('util', None)
sys.path = [p for p in sys.path if 'question_' not in p and 'sub_qn_' not in p]
sys.path.insert(0, os.path.dirname(__file__))
from util import demonstrate_explode, demonstrate_explode_outer, demonstrate_posexplode

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q4_4").master("local").getOrCreate()

@pytest.fixture(scope="session")
def sample_json(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("data")
    data = [
        {"id": "0001", "name": "James",
         "projects": [{"pid": "P1"}, {"pid": "P2"}]},
        {"id": "0002", "name": "Maria",
         "projects": [{"pid": "P3"}]},
        {"id": "0003", "name": "Scott", "projects": None}
    ]
    json_file = str(tmp_path / "emp.json")
    with open(json_file, "w") as f:
        json.dump(data, f)
    return json_file


def test_explode_removes_nulls(spark, sample_json):
    df = s1.read_json_dynamic(spark, sample_json)
    result = demonstrate_explode(df, "projects")
    assert result.count() == 3

def test_explode_outer_keeps_nulls(spark, sample_json):
    df = s1.read_json_dynamic(spark, sample_json)
    result = demonstrate_explode_outer(df, "projects")
    assert result.count() == 4

def test_posexplode_has_pos_column(spark, sample_json):
    df = s1.read_json_dynamic(spark, sample_json)
    result = demonstrate_posexplode(df, "projects")
    assert "pos" in result.columns