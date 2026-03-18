import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql.functions import avg, col, round


def avg_salary_by_dept(employee_df, department_df):
    return (
        employee_df
        .join(department_df,
              employee_df.department == department_df.dept_id,
              "inner")
        .groupBy("dept_id", "dept_name")
        .agg(round(avg(col("salary")), 2).alias("avg_salary"))
        .orderBy("dept_id")
    )