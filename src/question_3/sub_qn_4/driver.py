import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_2'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import importlib, types

def load(path, name):
    import importlib.util as ilu
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

base = os.path.join(os.path.dirname(__file__), '..')
s1   = load(os.path.join(base, 'sub_qn_1', 'util.py'), 's1')
s2   = load(os.path.join(base, 'sub_qn_2', 'util.py'), 's2')

from util import convert_timestamp_to_date


def main():
    spark = SparkSession.builder.appName("Q3_4").master("local").getOrCreate()
    df = s2.rename_columns(s1.create_log_df(spark))

    print("=== Before conversion ===")
    df.show(truncate=False)
    df.printSchema()

    print("=== After timestamp → login_date ===")
    result = convert_timestamp_to_date(df)
    result.show(truncate=False)
    result.printSchema()


if __name__ == "__main__":
    main()