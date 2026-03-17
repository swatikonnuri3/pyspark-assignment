import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import add_bonus


def main():
    spark = SparkSession.builder.appName("Q5_4").master("local").getOrCreate()

    employee_df = sub1.create_employee_df(spark)

    print("=== Employee DataFrame with Bonus ===")
    add_bonus(employee_df).show()


if __name__ == "__main__":
    main()