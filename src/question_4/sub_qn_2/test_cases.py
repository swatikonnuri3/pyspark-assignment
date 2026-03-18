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
from util import flatten_df

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q4_2").master("local").getOrCreate()

@pytest.fixture(scope="session")
def sample_json(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("data")
    data = [
        {"id": "0001", "name": "James",
         "projects": [{"project_id": "P1", "project_name": "Alpha"},
                      {"project_id": "P2", "project_name": "Beta"}]},
        {"id": "0002", "name": "Maria",
         "projects": [{"project_id": "P3", "project_name": "Gamma"}]}
    ]
    json_file = str(tmp_path / "employee.json")
    with open(json_file, "w") as f:
        json.dump(data, f)
    return json_file

@pytest.fixture(scope="session")
def flat_df(spark, sample_json):
    return flatten_df(s1.read_json_dynamic(spark, sample_json))


def test_no_array_columns_after_flatten(spark, flat_df):
    from pyspark.sql.types import ArrayType
    array_cols = [f.name for f in flat_df.schema.fields
                  if isinstance(f.dataType, ArrayType)]
    assert len(array_cols) == 0

def test_flatten_increases_rows(spark, sample_json, flat_df):
    original_count = s1.read_json_dynamic(spark, sample_json).count()
    assert flat_df.count() >= original_count