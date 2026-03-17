import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import apply_mask


def main():
    spark = SparkSession.builder.appName("Q2_5").master("local").getOrCreate()
    df = sub1.create_credit_card_df(spark)

    print("=== Masked Card Numbers ===")
    apply_mask(df).show(truncate=False)
    # Expected: ************4567, ************1234 etc


if __name__ == "__main__":
    main()