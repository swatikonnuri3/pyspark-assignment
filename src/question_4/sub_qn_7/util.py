from pyspark.sql.functions import current_date, col


def add_load_date(df):
    """Add load_date column with current date."""
    return df.withColumn("load_date", current_date())