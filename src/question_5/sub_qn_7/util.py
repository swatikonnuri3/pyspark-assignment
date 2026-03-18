import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql.functions import col


def replace_state_with_country(employee_df, country_df):
    """
    Replace state code in employee_df with full country_name
    from country_df.
    """
    return (
        employee_df
        .join(country_df,
              employee_df.state == country_df.country_code,
              "left")
        .drop("state", "country_code")
        .withColumnRenamed("country_name", "state")
        .select("employee_id", "employee_name", "department",
                "state", "salary", "age")
    )