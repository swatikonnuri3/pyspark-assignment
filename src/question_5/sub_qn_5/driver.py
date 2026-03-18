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

sys.path.insert(0, os.path.dirname(__file__))
from util import reorder_columns

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q5_5").master("local").getOrCreate()

    employee_df = sub1.create_employee_df(spark)

    print("=== Original column order ===")
    print(employee_df.columns)

    print("=== Reordered columns ===")
    result = reorder_columns(employee_df)
    print(result.columns)
    result.show()


if __name__ == "__main__":
    main()