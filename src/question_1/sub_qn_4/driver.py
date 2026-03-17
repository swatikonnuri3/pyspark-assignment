import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import new_products


def main():
    spark = SparkSession.builder.appName("Q1_4").master("local").getOrCreate()

    purchase_data_df = sub1.create_purchase_data_df(spark)
    product_data_df  = sub1.create_product_data_df(spark)

    print("=== Purchase Data ===")
    purchase_data_df.show()

    print("=== Product Catalogue ===")
    product_data_df.show()

    print("=== Customers who bought ALL models ===")
    result = new_products(product_data_df, purchase_data_df)
    result.show()
    # Expected: customer 1


if __name__ == "__main__":
    main()