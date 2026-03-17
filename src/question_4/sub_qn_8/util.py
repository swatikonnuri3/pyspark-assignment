from pyspark.sql.functions import col, year, month, dayofmonth


def add_year_month_day(df):
    """
    Add year, month, day columns from load_date column.
    """
    return df \
        .withColumn("year",  year(col("load_date"))) \
        .withColumn("month", month(col("load_date"))) \
        .withColumn("day",   dayofmonth(col("load_date")))