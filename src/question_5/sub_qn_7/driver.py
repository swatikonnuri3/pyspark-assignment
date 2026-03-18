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
from util import replace_state_with_country

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q5_7").master("local").getOrCreate()

    employee_df = sub1.create_employee_df(spark)
    country_df  = sub1.create_country_df(spark)

    print("=== Employee with country_name instead of state code ===")
    replace_state_with_country(employee_df, country_df).show()
    # Expected: ny → newyork, ca → california, uk → russia


if __name__ == "__main__":
    main()