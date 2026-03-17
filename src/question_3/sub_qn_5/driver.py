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
s4   = load(os.path.join(base, 'sub_qn_4', 'util.py'), 's4')

from util import write_csv_options
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q3_5").master("local").getOrCreate()
    df = s4.convert_timestamp_to_date(
             s2.rename_columns(
                 s1.create_log_df(spark)
             )
         )

    print("=== Writing CSV with different options ===")
    write_csv_options(df)


if __name__ == "__main__":
    main()