import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import customers_only_iphone13


def main():
    spark = SparkSession.builder.appName("Q1_2").master("local").getOrCreate()

    purchase_data_df = sub1.create_purchase_data_df(spark)

    print("=== Purchase Data ===")
    purchase_data_df.show()

    print("=== Customers who bought ONLY iphone13 ===")
    result = customers_only_iphone13(purchase_data_df)
    result.show()
    # Expected: customer 4


if __name__ == "__main__":
    main()