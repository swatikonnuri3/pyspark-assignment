import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import replace_state_with_country


def main():
    spark = SparkSession.builder.appName("Q5_7").master("local").getOrCreate()

    employee_df = sub1.create_employee_df(spark)
    country_df  = sub1.create_country_df(spark)

    print("=== Employee with country_name instead of state code ===")
    replace_state_with_country(employee_df, country_df).show()
    # Expected: ny → newyork, ca → california, uk → russia


if __name__ == "__main__":
    main()