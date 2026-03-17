from pyspark.sql.functions import col


def filter_by_id(df, id_value="0001"):
    """Filter records where id equals the given value."""
    return df.filter(col("id") == id_value)