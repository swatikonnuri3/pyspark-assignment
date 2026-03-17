import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
from util import get_final_output


def main():
    spark = SparkSession.builder.appName("Q2_6").master("local").getOrCreate()

    print("=== Final Output: card_number | masked_card_number ===")
    get_final_output(spark).show(truncate=False)


if __name__ == "__main__":
    main()