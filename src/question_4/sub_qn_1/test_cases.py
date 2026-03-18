import pytest
import sys
import os
import json
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

sys.modules.pop('util', None)
sys.path.insert(0, os.path.dirname(__file__))
from util import read_json_dynamic

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q4_1").master("local").getOrCreate()

@pytest.fixture(scope="session")
def sample_json(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("data")
    data = [
        {"id": "0001", "name": "James",
         "projects": [{"project_id": "P1", "project_name": "Alpha"}]},
        {"id": "0002", "name": "Maria",
         "projects": [{"project_id": "P2", "project_name": "Beta"}]}
    ]
    json_file = str(tmp_path / "employee.json")
    with open(json_file, "w") as f:
        json.dump(data, f)
    return json_file


def test_json_read_not_empty(spark, sample_json):
    assert read_json_dynamic(spark, sample_json).count() > 0

def test_json_read_has_columns(spark, sample_json):
    assert len(read_json_dynamic(spark, sample_json).columns) > 0

def test_json_schema_inferred(spark, sample_json):
    df = read_json_dynamic(spark, sample_json)
    assert "id" in df.columns
    assert "name" in df.columns