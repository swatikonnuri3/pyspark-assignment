from pyspark.sql.functions import col


def emp_dept_name_starts_with_m(employee_df, department_df):
    """
    Find employee name and department name
    where employee name starts with 'm'.
    """
    return (
        employee_df
        .join(department_df,
              employee_df.department == department_df.dept_id,
              "inner")
        .filter(col("employee_name").startswith("m"))
        .select("employee_name", "dept_name")
    )