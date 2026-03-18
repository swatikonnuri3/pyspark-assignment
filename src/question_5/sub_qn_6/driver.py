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
from util import perform_joins

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q5_6").master("local").getOrCreate()

    employee_df   = sub1.create_employee_df(spark)
    department_df = sub1.create_department_df(spark)

    results = perform_joins(employee_df, department_df)

    for join_type, df in results.items():
        print(f"=== {join_type.upper()} JOIN ===")
        df.show()


if __name__ == "__main__":
    main()