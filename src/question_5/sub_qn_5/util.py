def reorder_columns(employee_df):
    """
    Reorder columns dynamically to:
    employee_id, employee_name, salary, state, age, department
    """
    new_order = ["employee_id", "employee_name", "salary",
                 "state", "age", "department"]
    return employee_df.select(new_order)