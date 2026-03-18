import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql.functions import col, explode


def flatten_df(df):
    """
    Flatten nested DataFrame by exploding array columns.
    Handles one level of nesting dynamically.
    """
    from pyspark.sql.types import ArrayType, StructType

    array_cols = [
        field.name for field in df.schema.fields
        if isinstance(field.dataType, ArrayType)
    ]

    for array_col in array_cols:
        df = df.withColumn(array_col, explode(col(array_col)))

    struct_cols = [
        field.name for field in df.schema.fields
        if isinstance(field.dataType, StructType)
    ]

    for struct_col in struct_cols:
        nested = df.select(f"{struct_col}.*").columns
        for nested_col in nested:
            df = df.withColumn(
                f"{struct_col}_{nested_col}",
                col(f"{struct_col}.{nested_col}")
            )
        df = df.drop(struct_col)

    return df