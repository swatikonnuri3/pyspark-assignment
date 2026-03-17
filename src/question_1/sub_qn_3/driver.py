import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import customers_upgraded_to_iphone14


def main():
    spark = SparkSession.builder.appName("Q1_3").master("local").getOrCreate()

    purchase_data_df = sub1.create_purchase_data_df(spark)

    print("=== Purchase Data ===")
    purchase_data_df.show()

    print("=== Customers who upgraded iphone13 → iphone14 ===")
    result = customers_upgraded_to_iphone14(purchase_data_df)
    result.show()
    # Expected: customers 1 and 3


if __name__ == "__main__":
    main()