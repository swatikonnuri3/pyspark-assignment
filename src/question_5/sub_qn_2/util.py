from pyspark.sql.functions import avg, col, round


def avg_salary_by_dept(employee_df, department_df):
    """Find average salary of each department with department name."""
    return (
        employee_df
        .join(department_df,
              employee_df.department == department_df.dept_id,
              "inner")
        .groupBy("dept_id", "dept_name")
        .agg(round(avg(col("salary")), 2).alias("avg_salary"))
        .orderBy("dept_id")
    )