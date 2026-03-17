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
s2   = load(os.path.join(base, 'sub_qn_2', 'util.py'), 's2')

from util import filter_by_id
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q4_5").master("local").getOrCreate()

    json_path = os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'data', 'employee.json'
    )
    df       = s1.read_json_dynamic(spark, json_path)
    flat_df  = s2.flatten_df(df)

    print("=== Filtered: id == 0001 ===")
    filter_by_id(flat_df).show(truncate=False)


if __name__ == "__main__":
    main()