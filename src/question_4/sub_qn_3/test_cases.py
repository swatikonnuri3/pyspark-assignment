import pytest
import sys
import os
import json


def load(path, name):
    import importlib.util as ilu
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("Test_Q4_3").master("local").getOrCreate()


@pytest.fixture(scope="session")
def sample_json(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("data")
    data = [
        {"id": "0001", "name": "James",
         "projects": [{"pid": "P1"}, {"pid": "P2"}]},
        {"id": "0002", "name": "Maria",
         "projects": [{"pid": "P3"}]}
    ]
    json_file = str(tmp_path / "emp.json")
    with open(json_file, "w") as f:
        json.dump(data, f)
    return json_file


def test_flattened_count_greater(spark, sample_json):
    base = os.path.join(os.path.dirname(__file__), '..')
    s1   = load(os.path.join(base, 'sub_qn_1', 'util.py'), 's1')
    sys.path.insert(0, os.path.dirname(__file__))
    from util import count_comparison
    df = s1.read_json_dynamic(spark, sample_json)
    original, flattened = count_comparison(spark, df)
    assert flattened > original