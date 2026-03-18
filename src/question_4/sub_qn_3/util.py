import sys
import os
sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

from pyspark.sql.functions import col, explode, explode_outer

def count_comparison(spark, df):
    """
    Compare record count before and after flattening.
    """
    original_count = df.count()
    print(f"Original (not flattened) count : {original_count}")
    from pyspark.sql.types import ArrayType
    array_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, ArrayType)
    ]
    if array_cols:
        exploded_df = df.withColumn(array_cols[0], explode(col(array_cols[0])))
        exploded_count = exploded_df.count()
        print(f"Flattened count               : {exploded_count}")
        print(
            f"\nDifference: {exploded_count - original_count} extra rows "
            f"because each element in the '{array_cols[0]}' array "
            f"becomes a separate row after explode."
        )
        return original_count, exploded_count
    else:
        print("No array columns found to explode.")
        return original_count, original_count