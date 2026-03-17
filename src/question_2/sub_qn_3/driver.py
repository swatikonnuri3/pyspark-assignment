import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import increase_partitions


def main():
    spark = SparkSession.builder.appName("Q2_3").master("local").getOrCreate()
    df = sub1.create_credit_card_df(spark)
    print(f"Before repartition: {df.rdd.getNumPartitions()} partition(s)")
    df_re = increase_partitions(df, 5)
    print(f"After repartition(5): {df_re.rdd.getNumPartitions()} partition(s)")


if __name__ == "__main__":
    main()