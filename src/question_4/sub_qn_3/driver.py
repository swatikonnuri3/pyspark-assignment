import sys
import os
sys.path.insert(0, os.path.dirname(__file__))


def load(path, name):
    import importlib.util as ilu
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

base = os.path.join(os.path.dirname(__file__), '..')
s1   = load(os.path.join(base, 'sub_qn_1', 'util.py'), 's1')

from util import count_comparison
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q4_3").master("local").getOrCreate()

    json_path = os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'data', 'employee.json'
    )
    df = s1.read_json_dynamic(spark, json_path)

    print("=== Record Count Comparison ===")
    count_comparison(spark, df)


if __name__ == "__main__":
    main()