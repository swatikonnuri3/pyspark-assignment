import pytest
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
from util import read_json_dynamic
import json


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("Test_Q4_1").master("local").getOrCreate()


@pytest.fixture(scope="session")
def sample_json(tmp_path_factory):
    """Create a sample nested JSON file for testing."""
    tmp_path = tmp_path_factory.mktemp("data")
    data = [
        {
            "id": "0001",
            "name": "James",
            "department": "Engineering",
            "projects": [
                {"project_id": "P1", "project_name": "Alpha"},
                {"project_id": "P2", "project_name": "Beta"}
            ]
        },
        {
            "id": "0002",
            "name": "Maria",
            "department": "Finance",
            "projects": [
                {"project_id": "P3", "project_name": "Gamma"}
            ]
        }
    ]
    json_file = str(tmp_path / "employee.json")
    with open(json_file, "w") as f:
        json.dump(data, f)
    return json_file


def test_json_read_not_empty(spark, sample_json):
    df = read_json_dynamic(spark, sample_json)
    assert df.count() > 0

def test_json_read_has_columns(spark, sample_json):
    df = read_json_dynamic(spark, sample_json)
    assert len(df.columns) > 0

def test_json_schema_inferred(spark, sample_json):
    df = read_json_dynamic(spark, sample_json)
    assert "id" in df.columns
    assert "name" in df.columns