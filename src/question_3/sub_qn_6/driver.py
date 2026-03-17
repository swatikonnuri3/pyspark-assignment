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

from util import write_managed_table
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("Q3_6") \
        .master("local") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    df = s4.convert_timestamp_to_date(
             s2.rename_columns(
                 s1.create_log_df(spark)
             )
         )

    print("=== Writing managed table user.login_details ===")
    write_managed_table(df, spark)

    print("=== Verifying table ===")
    spark.sql("SELECT * FROM user.login_details").show(truncate=False)


if __name__ == "__main__":
    main()