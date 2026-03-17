import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

def load(path, name):
    import importlib.util as ilu
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

base = os.path.join(os.path.dirname(__file__), '..')
s7   = load(os.path.join(base, 'sub_qn_7', 'util.py'), 's7')

from pyspark.sql import SparkSession
import util as sub1
from util import lowercase_columns_and_load_date


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