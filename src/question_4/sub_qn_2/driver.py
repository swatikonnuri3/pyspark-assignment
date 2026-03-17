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

from util import flatten_df
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q4_2").master("local").getOrCreate()

    json_path = os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'data', 'employee.json'
    )

    df = s1.read_json_dynamic(spark, json_path)

    print("=== Original DataFrame ===")
    df.show(truncate=False)
    df.printSchema()

    print("=== Flattened DataFrame ===")
    flat_df = flatten_df(df)
    flat_df.show(truncate=False)
    flat_df.printSchema()


if __name__ == "__main__":
    main()