import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import rename_columns


def main():
    spark = SparkSession.builder.appName("Q3_2").master("local").getOrCreate()
    df = sub1.create_log_df(spark)

    print("=== Before Rename ===")
    df.show(truncate=False)

    print("=== After Rename ===")
    renamed_df = rename_columns(df)
    renamed_df.show(truncate=False)
    renamed_df.printSchema()


if __name__ == "__main__":
    main()