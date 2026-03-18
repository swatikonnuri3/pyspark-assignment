import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql.functions import col, explode, explode_outer, posexplode

def demonstrate_explode(df, array_col):
    """explode - ignores NULL arrays, one row per element."""
    print("=== explode ===")
    result = df.withColumn(array_col, explode(col(array_col)))
    result.show(truncate=False)
    return result

def demonstrate_explode_outer(df, array_col):
    """explode_outer - keeps rows with NULL/empty arrays as NULL."""
    print("=== explode_outer ===")
    result = df.withColumn(array_col, explode_outer(col(array_col)))
    result.show(truncate=False)
    return result

def demonstrate_posexplode(df, array_col):
    """posexplode - returns position index + element, ignores NULLs."""
    print("=== posexplode ===")
    result = df.select(
        "*",
        posexplode(col(array_col)).alias("pos", array_col + "_item")
    ).drop(array_col)
    result.show(truncate=False)
    return result