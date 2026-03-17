import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import emp_dept_name_starts_with_m


def main():
    spark = SparkSession.builder.appName("Q5_3").master("local").getOrCreate()

    employee_df   = sub1.create_employee_df(spark)
    department_df = sub1.create_department_df(spark)

    print("=== Employees whose name starts with 'm' ===")
    emp_dept_name_starts_with_m(employee_df, department_df).show()
    # Expected: michel → sales, maria → sales


if __name__ == "__main__":
    main()