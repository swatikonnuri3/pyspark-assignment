import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from util import get_spark, create_employee_df, create_department_df, create_country_df


def main():
    spark = get_spark()

    employee_df   = create_employee_df(spark)
    department_df = create_department_df(spark)
    country_df    = create_country_df(spark)

    print("=== Employee DataFrame ===")
    employee_df.show()
    employee_df.printSchema()

    print("=== Department DataFrame ===")
    department_df.show()
    department_df.printSchema()

    print("=== Country DataFrame ===")
    country_df.show()
    country_df.printSchema()


if __name__ == "__main__":
    main()