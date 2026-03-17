import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
from util import avg_salary_by_dept


def main():
    spark = SparkSession.builder.appName("Q5_2").master("local").getOrCreate()

    employee_df   = sub1.create_employee_df(spark)
    department_df = sub1.create_department_df(spark)

    print("=== Average Salary by Department ===")
    avg_salary_by_dept(employee_df, department_df).show()


if __name__ == "__main__":
    main()