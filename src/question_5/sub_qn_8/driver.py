import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

import importlib.util as ilu

def load(path, name):
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

base = os.path.join(os.path.dirname(__file__), '..')
sub1 = load(os.path.join(base, 'sub_qn_1', 'util.py'), 'sub1')
s7   = load(os.path.join(base, 'sub_qn_7', 'util.py'), 's7')

sys.path.insert(0, os.path.dirname(__file__))
from util import lowercase_columns_and_load_date

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q5_8").master("local").getOrCreate()

    employee_df = sub1.create_employee_df(spark)
    country_df  = sub1.create_country_df(spark)

    df = s7.replace_state_with_country(employee_df, country_df)

    print("=== Lowercase columns + load_date ===")
    result = lowercase_columns_and_load_date(df)
    result.show()
    print(result.columns)


if __name__ == "__main__":
    main()