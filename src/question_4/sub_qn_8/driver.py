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
s6   = load(os.path.join(base, 'sub_qn_6', 'util.py'), 's6')
s7   = load(os.path.join(base, 'sub_qn_7', 'util.py'), 's7')

from util import add_year_month_day
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q4_8").master("local").getOrCreate()

    json_path = os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'data', 'employee.json'
    )
    df = s7.add_load_date(
             s6.rename_camel_to_snake(
                 s2.flatten_df(
                     s1.read_json_dynamic(spark, json_path)
                 )
             )
         )

    print("=== DataFrame with year, month, day ===")
    result = add_year_month_day(df)
    result.show(truncate=False)


if __name__ == "__main__":
    main()