import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

import re

def camel_to_snake(name):
    """Convert a camelCase column name to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def rename_camel_to_snake(df):
    """Dynamically rename all columns from camelCase to snake_case."""
    for col_name in df.columns:
        new_name = camel_to_snake(col_name)
        if new_name != col_name:
            df = df.withColumnRenamed(col_name, new_name)
    return df