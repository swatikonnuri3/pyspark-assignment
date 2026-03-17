from pyspark.sql.functions import current_date, col


def lowercase_columns_and_load_date(df):
    """
    Dynamically convert all column names to lowercase
    and add load_date column with current date.
    """
    # Lowercase all column names dynamically
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())

    # Add load_date
    df = df.withColumn("load_date", current_date())

    return df