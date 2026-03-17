import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from util import get_spark, create_purchase_data_df, create_product_data_df


def main():
    spark = get_spark()

    purchase_data_df = create_purchase_data_df(spark)
    product_data_df = create_product_data_df(spark)

    print("=== Purchase Data DataFrame ===")
    purchase_data_df.show()
    purchase_data_df.printSchema()

    print("=== Product Data DataFrame ===")
    product_data_df.show()
    product_data_df.printSchema()


if __name__ == "__main__":
    main()