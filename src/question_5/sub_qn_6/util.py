from pyspark.sql.functions import col


def perform_joins(employee_df, department_df):
    """
    Perform inner, left and right joins dynamically
    between employee_df and department_df.
    """
    join_condition = employee_df.department == department_df.dept_id

    join_types = ["inner", "left", "right"]
    results    = {}

    for join_type in join_types:
        results[join_type] = (
            employee_df.join(department_df, join_condition, join_type)
                       .select(
                           employee_df.employee_id,
                           employee_df.employee_name,
                           employee_df.department,
                           department_df.dept_name,
                           employee_df.salary
                       )
        )

    return results