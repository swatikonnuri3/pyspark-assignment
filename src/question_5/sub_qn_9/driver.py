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
s8   = load(os.path.join(base, 'sub_qn_8', 'util.py'), 's8')

from pyspark.sql import SparkSession
import util as sub1
from util import write_external_tables


def main():
    spark = SparkSession.builder \
        .appName("Q5_9") \
        .master("local") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    employee_df = sub1.create_employee_df(spark)
    country_df  = sub1.create_country_df(spark)

    df = s8.lowercase_columns_and_load_date(
             s7.replace_state_with_country(employee_df, country_df)
         )

    print("=== Writing External Tables (CSV + Parquet) ===")
    write_external_tables(df, spark)

    print("=== Verify CSV Table ===")
    spark.sql("SELECT * FROM employee_ext.employee_csv").show()

    print("=== Verify Parquet Table ===")
    spark.sql("SELECT * FROM employee_ext.employee_parquet").show()


if __name__ == "__main__":
    main()